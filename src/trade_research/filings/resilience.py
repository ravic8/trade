from __future__ import annotations

import argparse
import json
import os
import re
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

import redis

from trade_research.config import Settings, get_settings
from trade_research.filings.models import FilingRun
from trade_research.filings.runtime import FilingRuntime, get_filing_runtime

_PROBE_ID_PATTERN = re.compile(r"^[A-Za-z0-9][A-Za-z0-9_.:-]{0,127}$")
_WORKER_PROBE_TTL_SECONDS = 86_400


def _timestamp() -> str:
    return datetime.now(UTC).isoformat().replace("+00:00", "Z")


def _validate_probe_id(probe_id: str) -> str:
    if not _PROBE_ID_PATTERN.fullmatch(probe_id):
        raise ValueError("invalid resilience probe identifier")
    return probe_id


def _worker_probe_key(probe_id: str) -> str:
    return f"filing-resilience:worker:{_validate_probe_id(probe_id)}"


def _redis_client(settings: Settings) -> redis.Redis:
    return redis.Redis.from_url(settings.redis_url, decode_responses=True)


def worker_probe_status(settings: Settings, probe_id: str) -> dict[str, Any]:
    client = _redis_client(settings)
    values = client.hgetall(_worker_probe_key(probe_id))
    if not values:
        return {
            "probe_id": probe_id,
            "state": "missing",
            "task_id": None,
            "attempt_count": 0,
            "first_worker_pid": None,
            "recovered_worker_pid": None,
            "first_process_start_ticks": None,
            "recovered_process_start_ticks": None,
            "first_hostname": None,
            "recovered_hostname": None,
            "started_at": None,
            "completed_at": None,
        }
    return {
        "probe_id": probe_id,
        "state": values.get("state", "unknown"),
        "task_id": values.get("task_id"),
        "attempt_count": int(values.get("attempt_count", "0")),
        "first_worker_pid": _optional_integer(values.get("first_worker_pid")),
        "recovered_worker_pid": _optional_integer(values.get("recovered_worker_pid")),
        "first_process_start_ticks": _optional_integer(values.get("first_process_start_ticks")),
        "recovered_process_start_ticks": _optional_integer(
            values.get("recovered_process_start_ticks")
        ),
        "first_hostname": values.get("first_hostname"),
        "recovered_hostname": values.get("recovered_hostname"),
        "started_at": values.get("started_at"),
        "completed_at": values.get("completed_at"),
    }


def start_worker_probe(
    settings: Settings,
    *,
    probe_id: str,
    hold_seconds: int,
) -> dict[str, Any]:
    if not 30 <= hold_seconds <= 300:
        raise ValueError("worker probe hold must be between 30 and 300 seconds")
    key = _worker_probe_key(probe_id)
    client = _redis_client(settings)
    task_id = f"filing-resilience-worker-{probe_id}"
    created = client.hsetnx(key, "state", "queued")
    if not created:
        raise ValueError("worker resilience probe already exists")
    client.hset(
        key,
        mapping={
            "task_id": task_id,
            "attempt_count": "0",
            "queued_at": _timestamp(),
        },
    )
    client.expire(key, _WORKER_PROBE_TTL_SECONDS)
    try:
        from trade_research.filings.tasks import worker_recovery_probe

        worker_recovery_probe.apply_async(
            args=[probe_id],
            kwargs={"hold_seconds": hold_seconds},
            queue=settings.filing_queue_name,
            task_id=task_id,
        )
    except Exception:
        client.delete(key)
        raise
    return worker_probe_status(settings, probe_id)


def begin_worker_probe_attempt(
    settings: Settings,
    *,
    probe_id: str,
    worker_pid: int,
    process_start_ticks: int,
    hostname: str,
) -> dict[str, Any]:
    key = _worker_probe_key(probe_id)
    client = _redis_client(settings)
    attempt_count = int(client.hincrby(key, "attempt_count", 1))
    mapping = {
        "state": "active" if attempt_count == 1 else "redelivered",
        "last_worker_pid": str(worker_pid),
        "last_hostname": hostname,
    }
    if attempt_count == 1:
        mapping.update(
            {
                "first_worker_pid": str(worker_pid),
                "first_process_start_ticks": str(process_start_ticks),
                "first_hostname": hostname,
                "started_at": _timestamp(),
            }
        )
    else:
        mapping.update(
            {
                "recovered_worker_pid": str(worker_pid),
                "recovered_process_start_ticks": str(process_start_ticks),
                "recovered_hostname": hostname,
            }
        )
    client.hset(key, mapping=mapping)
    client.expire(key, _WORKER_PROBE_TTL_SECONDS)
    return worker_probe_status(settings, probe_id)


def complete_worker_probe(settings: Settings, *, probe_id: str) -> dict[str, Any]:
    client = _redis_client(settings)
    key = _worker_probe_key(probe_id)
    client.hset(
        key,
        mapping={"state": "completed", "completed_at": _timestamp()},
    )
    client.expire(key, _WORKER_PROBE_TTL_SECONDS)
    return worker_probe_status(settings, probe_id)


def timeout_worker_probe(settings: Settings, *, probe_id: str) -> None:
    client = _redis_client(settings)
    key = _worker_probe_key(probe_id)
    client.hset(
        key,
        mapping={"state": "termination_not_observed", "completed_at": _timestamp()},
    )
    client.expire(key, _WORKER_PROBE_TTL_SECONDS)


def current_process_start_ticks() -> int:
    process_stat = Path(f"/proc/{os.getpid()}/stat").read_text(encoding="utf-8")
    fields_after_name = process_stat.rsplit(")", 1)[1].split()
    return int(fields_after_name[19])


def prepare_stale_lease_probe(
    runtime: FilingRuntime,
    *,
    filing_id: str,
    workspace_id: str,
    probe_id: str,
    lease_seconds: int,
) -> FilingRun:
    _validate_probe_id(probe_id)
    if not 1 <= lease_seconds <= 30:
        raise ValueError("stale lease probe duration must be between 1 and 30 seconds")
    document = runtime.store.document(filing_id, workspace_id)
    if document is None:
        raise ValueError("filing document was not found in the workspace")
    run, created = runtime.store.create_run(
        workspace_id=workspace_id,
        company_id=document.company_id,
        filing_id=document.filing_id,
        idempotency_key=f"resilience-stale-lease:{probe_id}",
        max_attempts=3,
        input_payload={
            "resilience_drill": True,
            "probe_id": probe_id,
            "submitted_by": "production-resilience-drill",
        },
    )
    if not created:
        raise ValueError("stale lease resilience run already exists")
    claimed = runtime.store.claim_run(
        run.run_id,
        worker_id=f"resilience-drill:{probe_id}",
        lease_seconds=lease_seconds,
    )
    if not claimed:
        raise RuntimeError("stale lease resilience run could not be claimed")
    latest = runtime.store.run(run.run_id, workspace_id)
    if latest is None:
        raise RuntimeError("stale lease resilience run disappeared")
    return latest


def recover_stale_lease_probe(
    runtime: FilingRuntime,
    *,
    run_id: str,
    workspace_id: str,
) -> list[str]:
    from trade_research.filings.tasks import dispatch_filing_run

    recovered = runtime.store.recover_stale_runs(
        workspace_id=workspace_id,
        limit=100,
    )
    if run_id not in recovered:
        raise RuntimeError("expected resilience run was not recovered")
    for recovered_run_id in recovered:
        dispatch_filing_run(recovered_run_id, runtime=runtime)
    return recovered


def stale_lease_probe_status(
    runtime: FilingRuntime,
    *,
    run_id: str,
    workspace_id: str,
) -> dict[str, Any]:
    run = runtime.store.run(run_id, workspace_id)
    if run is None:
        raise ValueError("stale lease resilience run was not found")
    candidates = runtime.store.candidate_facts(run_id)
    defects = runtime.store.validation_defects(run_id)
    facts = runtime.store.approved_facts(
        workspace_id=workspace_id,
        company_id=run.company_id,
        current_only=False,
        limit=2_000,
    )
    run_facts = [
        fact for fact in facts if fact.source_filing_id == run.filing_id and fact.run_id == run_id
    ]
    return {
        "run_id": run.run_id,
        "workspace_id": run.workspace_id,
        "company_id": run.company_id,
        "filing_id": run.filing_id,
        "status": run.status.value,
        "current_node": run.current_node,
        "attempt_count": run.attempt_count,
        "max_attempts": run.max_attempts,
        "worker_id": run.worker_id,
        "lease_expires_at": (run.lease_expires_at.isoformat() if run.lease_expires_at else None),
        "candidate_count": len(candidates),
        "defect_count": len(defects),
        "approved_fact_count": len(run_facts),
        "unique_approved_fact_ids": len({fact.fact_id for fact in run_facts}),
        "trace_id": run.trace_id,
    }


def _optional_integer(value: str | None) -> int | None:
    return int(value) if value else None


def _print_json(payload: Any) -> None:
    print(json.dumps(payload, separators=(",", ":"), default=str))


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Internal operations for the filing resilience drill."
    )
    subparsers = parser.add_subparsers(dest="operation", required=True)

    worker_start = subparsers.add_parser("worker-start")
    worker_start.add_argument("--probe-id", required=True)
    worker_start.add_argument("--hold-seconds", type=int, default=120)

    worker_status = subparsers.add_parser("worker-status")
    worker_status.add_argument("--probe-id", required=True)

    stale_prepare = subparsers.add_parser("stale-prepare")
    stale_prepare.add_argument("--probe-id", required=True)
    stale_prepare.add_argument("--filing-id", required=True)
    stale_prepare.add_argument("--workspace-id", default="default")
    stale_prepare.add_argument("--lease-seconds", type=int, default=2)

    stale_recover = subparsers.add_parser("stale-recover")
    stale_recover.add_argument("--run-id", required=True)
    stale_recover.add_argument("--workspace-id", default="default")

    stale_status = subparsers.add_parser("stale-status")
    stale_status.add_argument("--run-id", required=True)
    stale_status.add_argument("--workspace-id", default="default")

    arguments = parser.parse_args()
    settings = get_settings()
    if arguments.operation == "worker-start":
        _print_json(
            start_worker_probe(
                settings,
                probe_id=arguments.probe_id,
                hold_seconds=arguments.hold_seconds,
            )
        )
    elif arguments.operation == "worker-status":
        _print_json(worker_probe_status(settings, arguments.probe_id))
    elif arguments.operation == "stale-prepare":
        runtime = get_filing_runtime()
        run = prepare_stale_lease_probe(
            runtime,
            filing_id=arguments.filing_id,
            workspace_id=arguments.workspace_id,
            probe_id=arguments.probe_id,
            lease_seconds=arguments.lease_seconds,
        )
        _print_json(
            stale_lease_probe_status(
                runtime,
                run_id=run.run_id,
                workspace_id=arguments.workspace_id,
            )
        )
    elif arguments.operation == "stale-recover":
        runtime = get_filing_runtime()
        recovered = recover_stale_lease_probe(
            runtime,
            run_id=arguments.run_id,
            workspace_id=arguments.workspace_id,
        )
        _print_json({"recovered_run_ids": recovered})
    elif arguments.operation == "stale-status":
        _print_json(
            stale_lease_probe_status(
                get_filing_runtime(),
                run_id=arguments.run_id,
                workspace_id=arguments.workspace_id,
            )
        )


if __name__ == "__main__":
    main()
