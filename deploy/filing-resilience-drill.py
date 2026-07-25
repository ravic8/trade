#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import os
import re
import subprocess
import sys
import time
from collections.abc import Callable
from datetime import datetime, timezone
from pathlib import Path
from typing import Any
from uuid import uuid4

_IDENTIFIER_PATTERN = re.compile(r"^[A-Za-z0-9][A-Za-z0-9_.:-]{0,127}$")
_TERMINAL_RUN_STATUSES = {"completed", "failed", "cancelled"}


class DrillFailure(RuntimeError):
    pass


def _timestamp() -> str:
    return datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")  # noqa: UP017


def _log(message: str) -> None:
    print(f"[trade-resilience] {message}", flush=True)


def _command(
    arguments: list[str],
    *,
    label: str,
) -> str:
    completed = subprocess.run(
        arguments,
        check=False,
        capture_output=True,
        text=True,
    )
    if completed.returncode != 0:
        if completed.stderr.strip():
            print(completed.stderr.strip(), file=sys.stderr)
        raise DrillFailure(f"{label} failed")
    return completed.stdout.strip()


def _json_command(arguments: list[str], *, label: str) -> dict[str, Any]:
    output = _command(arguments, label=label)
    try:
        return json.loads(output)
    except json.JSONDecodeError as exc:
        raise DrillFailure(f"{label} returned invalid JSON") from exc


def _wait_for(
    operation: Callable[[], dict[str, Any]],
    predicate: Callable[[dict[str, Any]], bool],
    *,
    timeout_seconds: int,
    description: str,
) -> dict[str, Any]:
    deadline = time.monotonic() + timeout_seconds
    latest: dict[str, Any] = {}
    while time.monotonic() < deadline:
        latest = operation()
        if predicate(latest):
            return latest
        time.sleep(1)
    raise DrillFailure(f"timed out waiting for {description}")


def _write_report(path: Path, report: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_suffix(".json.tmp")
    temporary.write_text(json.dumps(report, indent=2) + "\n", encoding="utf-8")
    temporary.replace(path)


def _internal_command(
    compose: list[str],
    operation: str,
    *arguments: str,
) -> list[str]:
    return [
        *compose,
        "exec",
        "-T",
        "api",
        "python",
        "-m",
        "trade_research.filings.resilience",
        operation,
        *arguments,
    ]


def _worker_container(
    compose: list[str],
    *,
    expected_hostname: str,
) -> str:
    container_ids = [
        value
        for value in _command(
            [*compose, "ps", "--status", "running", "-q", "filing-worker"],
            label="filing worker discovery",
        ).splitlines()
        if value
    ]
    if not container_ids:
        raise DrillFailure("no running filing worker container was found")
    normalized = expected_hostname.rsplit("@", 1)[-1]
    for container_id in container_ids:
        hostname = _command(
            [
                "docker",
                "inspect",
                "--format",
                "{{.Config.Hostname}}",
                container_id,
            ],
            label="filing worker hostname inspection",
        )
        if hostname == normalized:
            return container_id
    raise DrillFailure("active probe worker did not match a filing-worker container")


def _terminate_probe_child(
    container_id: str,
    worker_pid: int,
    process_start_ticks: int,
) -> None:
    if worker_pid <= 1:
        raise DrillFailure("refusing to terminate an unsafe worker PID")
    if process_start_ticks <= 0:
        raise DrillFailure("refusing to terminate a worker without process identity")
    script = """
import os
from pathlib import Path
import signal
import sys

pid = int(sys.argv[1])
expected_start_ticks = int(sys.argv[2])
if pid <= 1:
    raise SystemExit("unsafe worker PID")
command = Path(f"/proc/{pid}/cmdline").read_bytes().replace(b"\\0", b" ").lower()
if b"celery" not in command:
    raise SystemExit("target process is not a Celery worker")
process_stat = Path(f"/proc/{pid}/stat").read_text(encoding="utf-8")
actual_start_ticks = int(process_stat.rsplit(")", 1)[1].split()[19])
if actual_start_ticks != expected_start_ticks:
    raise SystemExit("target Celery process identity changed")
os.kill(pid, signal.SIGKILL)
"""
    _command(
        [
            "docker",
            "exec",
            container_id,
            "python",
            "-c",
            script,
            str(worker_pid),
            str(process_start_ticks),
        ],
        label="probe worker termination",
    )


def run_drill(arguments: argparse.Namespace) -> tuple[dict[str, Any], int]:
    app_dir = Path(arguments.app_dir).expanduser().resolve()
    env_file = Path(arguments.env_file).expanduser().resolve()
    compose_file = app_dir / "docker-compose.prod.yml"
    if not env_file.is_file():
        raise DrillFailure(f"environment file is missing: {env_file}")
    if not compose_file.is_file():
        raise DrillFailure(f"production Compose file is missing: {compose_file}")
    if not _IDENTIFIER_PATTERN.fullmatch(arguments.workspace_id):
        raise DrillFailure("invalid workspace identifier")
    if not _IDENTIFIER_PATTERN.fullmatch(arguments.filing_id):
        raise DrillFailure("invalid filing identifier")

    drill_id = (
        f"{datetime.now(timezone.utc).strftime('%Y%m%dT%H%M%SZ')}-"  # noqa: UP017
        f"{uuid4().hex[:12]}"
    )
    probe_id = f"m1-{drill_id}"
    report_path = Path(arguments.report_dir).expanduser().resolve() / f"{drill_id}.json"
    compose = [
        "docker",
        "compose",
        "--env-file",
        str(env_file),
        "-f",
        str(compose_file),
    ]
    report: dict[str, Any] = {
        "schema_version": 1,
        "drill_id": drill_id,
        "status": "failed",
        "stage": "initializing",
        "exit_code": 1,
        "error": None,
        "started_at": _timestamp(),
        "finished_at": None,
        "workspace_id": arguments.workspace_id,
        "filing_id": arguments.filing_id,
        "readiness_passed": False,
        "worker_termination": {
            "probe_id": probe_id,
            "task_id": None,
            "first_worker_pid": None,
            "recovered_worker_pid": None,
            "first_process_start_ticks": None,
            "recovered_process_start_ticks": None,
            "attempt_count": 0,
            "redelivered": False,
            "worker_healthy": False,
        },
        "stale_lease_recovery": {
            "run_id": None,
            "recovered": False,
            "status": None,
            "attempt_count": 0,
            "candidate_count": 0,
            "approved_fact_count": 0,
            "unique_approved_fact_ids": 0,
            "defect_count": 0,
        },
    }
    exit_code = 1
    try:
        report["stage"] = "readiness"
        _log("verifying filing production readiness")
        readiness = _json_command(
            [
                *compose,
                "exec",
                "-T",
                "api",
                "trade-research",
                "verify-filing-production",
            ],
            label="filing production readiness",
        )
        if readiness.get("passed") is not True:
            raise DrillFailure("filing production readiness did not pass")
        report["readiness_passed"] = True

        report["stage"] = "worker_probe_dispatch"
        _log("dispatching bounded worker-termination probe")
        worker_start = _json_command(
            _internal_command(
                compose,
                "worker-start",
                "--probe-id",
                probe_id,
                "--hold-seconds",
                str(arguments.worker_hold_seconds),
            ),
            label="worker probe dispatch",
        )
        task_id = worker_start.get("task_id")
        if not isinstance(task_id, str) or not task_id:
            raise DrillFailure("worker probe dispatch returned no task identifier")
        report["worker_termination"]["task_id"] = task_id

        report["stage"] = "worker_probe_active"
        active = _wait_for(
            lambda: _json_command(
                _internal_command(
                    compose,
                    "worker-status",
                    "--probe-id",
                    probe_id,
                ),
                label="worker probe status",
            ),
            lambda value: (
                value.get("state") == "active"
                and value.get("attempt_count") == 1
                and isinstance(value.get("first_worker_pid"), int)
                and isinstance(value.get("first_process_start_ticks"), int)
                and bool(value.get("first_hostname"))
            ),
            timeout_seconds=60,
            description="the first worker probe attempt",
        )
        first_pid = int(active["first_worker_pid"])
        first_start_ticks = int(active["first_process_start_ticks"])
        report["worker_termination"]["first_worker_pid"] = first_pid
        report["worker_termination"]["first_process_start_ticks"] = first_start_ticks
        container_id = _worker_container(
            compose,
            expected_hostname=str(active["first_hostname"]),
        )

        report["stage"] = "worker_termination"
        _log(f"terminating probe execution child PID {first_pid}")
        _terminate_probe_child(container_id, first_pid, first_start_ticks)

        report["stage"] = "worker_redelivery"
        recovered_worker = _wait_for(
            lambda: _json_command(
                _internal_command(
                    compose,
                    "worker-status",
                    "--probe-id",
                    probe_id,
                ),
                label="worker probe status",
            ),
            lambda value: (
                value.get("state") == "completed"
                and int(value.get("attempt_count", 0)) >= 2
                and isinstance(value.get("recovered_worker_pid"), int)
                and isinstance(
                    value.get("recovered_process_start_ticks"),
                    int,
                )
            ),
            timeout_seconds=90,
            description="worker probe redelivery",
        )
        recovered_pid = int(recovered_worker["recovered_worker_pid"])
        worker_attempts = int(recovered_worker["attempt_count"])
        if recovered_worker.get("task_id") != task_id:
            raise DrillFailure("redelivered worker probe changed task identifier")
        if worker_attempts != 2:
            raise DrillFailure("worker probe did not complete on exactly two attempts")
        if recovered_pid == first_pid:
            raise DrillFailure("redelivered probe reused the terminated worker PID")
        report["worker_termination"].update(
            {
                "recovered_worker_pid": recovered_pid,
                "recovered_process_start_ticks": int(
                    recovered_worker["recovered_process_start_ticks"]
                ),
                "attempt_count": worker_attempts,
                "redelivered": True,
            }
        )
        ping = _command(
            [
                *compose,
                "exec",
                "-T",
                "filing-worker",
                "celery",
                "-A",
                "trade_research.filings.tasks:celery_app",
                "inspect",
                "ping",
                "--timeout",
                "10",
            ],
            label="filing worker health verification",
        )
        if "pong" not in ping.lower():
            raise DrillFailure("filing worker did not respond after child recovery")
        report["worker_termination"]["worker_healthy"] = True
        _log("worker task was redelivered and completed after termination")

        report["stage"] = "stale_lease_prepare"
        _log("preparing an expired filing execution lease")
        prepared = _json_command(
            _internal_command(
                compose,
                "stale-prepare",
                "--probe-id",
                probe_id,
                "--filing-id",
                arguments.filing_id,
                "--workspace-id",
                arguments.workspace_id,
                "--lease-seconds",
                str(arguments.lease_seconds),
            ),
            label="stale lease preparation",
        )
        stale_run_id = str(prepared["run_id"])
        report["stale_lease_recovery"]["run_id"] = stale_run_id
        time.sleep(arguments.lease_seconds + 1)

        report["stage"] = "stale_lease_recovery"
        recovered = _json_command(
            _internal_command(
                compose,
                "stale-recover",
                "--run-id",
                stale_run_id,
                "--workspace-id",
                arguments.workspace_id,
            ),
            label="stale lease recovery",
        )
        if stale_run_id not in recovered.get("recovered_run_ids", []):
            raise DrillFailure("expected stale filing run was not recovered")
        report["stale_lease_recovery"]["recovered"] = True

        report["stage"] = "stale_run_completion"
        completed_run = _wait_for(
            lambda: _json_command(
                _internal_command(
                    compose,
                    "stale-status",
                    "--run-id",
                    stale_run_id,
                    "--workspace-id",
                    arguments.workspace_id,
                ),
                label="stale filing run status",
            ),
            lambda value: value.get("status") in _TERMINAL_RUN_STATUSES,
            timeout_seconds=180,
            description="the recovered filing run",
        )
        report["stale_lease_recovery"].update(
            {
                "status": completed_run.get("status"),
                "attempt_count": int(completed_run.get("attempt_count", 0)),
                "candidate_count": int(completed_run.get("candidate_count", 0)),
                "approved_fact_count": int(completed_run.get("approved_fact_count", 0)),
                "unique_approved_fact_ids": int(completed_run.get("unique_approved_fact_ids", 0)),
                "defect_count": int(completed_run.get("defect_count", 0)),
                "trace_id": completed_run.get("trace_id"),
            }
        )
        stale = report["stale_lease_recovery"]
        if stale["status"] != "completed":
            raise DrillFailure("recovered stale filing run did not complete")
        if stale["attempt_count"] != 2:
            raise DrillFailure("recovered stale filing run has an unexpected attempt count")
        if stale["candidate_count"] < arguments.expected_facts:
            raise DrillFailure("recovered stale filing run produced too few candidates")
        if stale["approved_fact_count"] != stale["candidate_count"]:
            raise DrillFailure("recovered stale filing run did not approve every candidate")
        if stale["unique_approved_fact_ids"] != stale["approved_fact_count"]:
            raise DrillFailure("recovered stale filing run produced duplicate approved facts")
        if stale["defect_count"] != 0:
            raise DrillFailure("recovered stale filing run produced validation defects")

        report["status"] = "passed"
        report["stage"] = "completed"
        report["exit_code"] = 0
        exit_code = 0
        _log("worker-termination and stale-lease recovery drill passed")
    except (DrillFailure, KeyError, ValueError) as exc:
        report["error"] = {
            "type": type(exc).__name__,
            "message": str(exc)[:500],
        }
        print(f"[trade-resilience] {exc}", file=sys.stderr)
    except KeyboardInterrupt:
        report["stage"] = "interrupted"
        report["error"] = {
            "type": "KeyboardInterrupt",
            "message": "drill interrupted by operator",
        }
        print("[trade-resilience] drill interrupted", file=sys.stderr)
    finally:
        report["finished_at"] = _timestamp()
        _write_report(report_path, report)
        _log(f"resilience report: {report_path}")
    return report, exit_code


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description=(
            "Terminate a bounded Celery execution child, verify task redelivery, "
            "and recover an expired filing lease."
        )
    )
    parser.add_argument("filing_id", help="Registered XBRL filing used by the lease drill.")
    parser.add_argument("--workspace-id", default="default")
    parser.add_argument(
        "--expected-facts",
        type=int,
        default=59,
        help="Minimum expected candidates from the recovered filing.",
    )
    parser.add_argument(
        "--app-dir",
        default=os.environ.get("TRADE_APP_DIR", "/opt/trade/app"),
    )
    parser.add_argument(
        "--env-file",
        default=os.environ.get("TRADE_ENV_FILE", "/opt/trade/.env"),
    )
    parser.add_argument(
        "--report-dir",
        default=os.environ.get(
            "PROD_RESILIENCE_REPORT_DIR",
            "/opt/trade/resilience-reports",
        ),
    )
    parser.add_argument("--worker-hold-seconds", type=int, default=120)
    parser.add_argument("--lease-seconds", type=int, default=2)
    return parser


def main() -> None:
    arguments = _parser().parse_args()
    if arguments.expected_facts < 1:
        raise SystemExit("--expected-facts must be positive")
    if not 30 <= arguments.worker_hold_seconds <= 300:
        raise SystemExit("--worker-hold-seconds must be between 30 and 300")
    if not 1 <= arguments.lease_seconds <= 30:
        raise SystemExit("--lease-seconds must be between 1 and 30")
    try:
        _report, exit_code = run_drill(arguments)
    except DrillFailure as exc:
        print(f"[trade-resilience] {exc}", file=sys.stderr)
        raise SystemExit(1) from exc
    raise SystemExit(exit_code)


if __name__ == "__main__":
    main()
