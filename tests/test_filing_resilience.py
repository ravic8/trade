from __future__ import annotations

import argparse
import importlib.util
import json
from pathlib import Path
from types import SimpleNamespace
from typing import Any

import pytest

from trade_research.config import Settings
from trade_research.filings import resilience
from trade_research.filings.models import FilingDocument, FilingRunStatus
from trade_research.filings.store import FilingStore
from trade_research.filings.tasks import worker_recovery_probe

REPOSITORY_ROOT = Path(__file__).resolve().parents[1]
DRILL_PATH = REPOSITORY_ROOT / "deploy" / "filing-resilience-drill.py"
_DRILL_SPEC = importlib.util.spec_from_file_location(
    "filing_resilience_drill",
    DRILL_PATH,
)
assert _DRILL_SPEC is not None and _DRILL_SPEC.loader is not None
drill = importlib.util.module_from_spec(_DRILL_SPEC)
_DRILL_SPEC.loader.exec_module(drill)


class _FakeRedis:
    def __init__(self) -> None:
        self.hashes: dict[str, dict[str, str]] = {}

    def hgetall(self, key: str) -> dict[str, str]:
        return dict(self.hashes.get(key, {}))

    def hsetnx(self, key: str, field: str, value: str) -> int:
        values = self.hashes.setdefault(key, {})
        if field in values:
            return 0
        values[field] = value
        return 1

    def hset(self, key: str, *, mapping: dict[str, str]) -> None:
        self.hashes.setdefault(key, {}).update(
            {name: str(value) for name, value in mapping.items()}
        )

    def hincrby(self, key: str, field: str, amount: int) -> int:
        values = self.hashes.setdefault(key, {})
        result = int(values.get(field, "0")) + amount
        values[field] = str(result)
        return result

    def expire(self, _key: str, _seconds: int) -> bool:
        return True

    def delete(self, key: str) -> int:
        return int(self.hashes.pop(key, None) is not None)


def _settings(tmp_path: Path) -> Settings:
    return Settings(
        app_env="test",
        data_dir=tmp_path,
        database_url=f"sqlite:///{tmp_path / 'filings.sqlite3'}",
        redis_url="redis://example.test:6379/0",
        filing_index_enabled=False,
        langfuse_enabled=False,
        otel_enabled=False,
    )


def test_worker_recovery_probe_records_redelivery(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    settings = _settings(tmp_path)
    client = _FakeRedis()
    dispatched: dict[str, Any] = {}
    monkeypatch.setattr(resilience, "_redis_client", lambda _settings: client)
    monkeypatch.setattr(
        worker_recovery_probe,
        "apply_async",
        lambda *args, **kwargs: dispatched.update({"args": args, "kwargs": kwargs}),
    )

    queued = resilience.start_worker_probe(
        settings,
        probe_id="m1-probe-1",
        hold_seconds=120,
    )
    active = resilience.begin_worker_probe_attempt(
        settings,
        probe_id="m1-probe-1",
        worker_pid=42,
        process_start_ticks=1_000,
        hostname="celery@worker-a",
    )
    redelivered = resilience.begin_worker_probe_attempt(
        settings,
        probe_id="m1-probe-1",
        worker_pid=84,
        process_start_ticks=2_000,
        hostname="celery@worker-a",
    )
    completed = resilience.complete_worker_probe(
        settings,
        probe_id="m1-probe-1",
    )

    assert queued["state"] == "queued"
    assert dispatched["kwargs"]["task_id"] == "filing-resilience-worker-m1-probe-1"
    assert active["state"] == "active"
    assert active["attempt_count"] == 1
    assert active["first_worker_pid"] == 42
    assert active["first_process_start_ticks"] == 1_000
    assert redelivered["state"] == "redelivered"
    assert redelivered["attempt_count"] == 2
    assert completed["state"] == "completed"
    assert completed["recovered_worker_pid"] == 84

    with pytest.raises(ValueError, match="already exists"):
        resilience.start_worker_probe(
            settings,
            probe_id="m1-probe-1",
            hold_seconds=120,
        )


def test_stale_run_recovery_is_scoped_to_workspace(tmp_path: Path) -> None:
    store = FilingStore(f"sqlite:///{tmp_path / 'filings.sqlite3'}")
    store.initialize()
    alpha, _ = store.create_run(
        workspace_id="alpha",
        company_id="NSE:INFY",
        filing_id="alpha-filing",
        idempotency_key="alpha-stale",
        max_attempts=3,
    )
    beta, _ = store.create_run(
        workspace_id="beta",
        company_id="NSE:INFY",
        filing_id="beta-filing",
        idempotency_key="beta-stale",
        max_attempts=3,
    )
    assert store.claim_run(alpha.run_id, worker_id="dead-alpha", lease_seconds=-1)
    assert store.claim_run(beta.run_id, worker_id="dead-beta", lease_seconds=-1)

    recovered = store.recover_stale_runs(workspace_id="alpha")

    assert recovered == [alpha.run_id]
    assert store.run(alpha.run_id, "alpha").status == FilingRunStatus.RETRYING
    assert store.run(beta.run_id, "beta").status == FilingRunStatus.RUNNING


def test_stale_lease_probe_uses_a_real_filing_run(tmp_path: Path) -> None:
    store = FilingStore(f"sqlite:///{tmp_path / 'filings.sqlite3'}")
    store.initialize()
    document = FilingDocument(
        filing_id="filing-1",
        workspace_id="alpha",
        company_id="NSE:INFY",
        symbol="INFY",
        company_name="Infosys Limited",
        categories=["xbrl financial"],
        source_url="https://example.test/filing.xml",
        relative_path="filing.xml",
        object_uri="file:///filing.xml",
        filename="filing.xml",
        byte_size=1,
        sha256="a" * 64,
        content_type="application/xml",
        document_key="NSE:INFY:2026-03-31:consolidated",
    )
    store.register_document(document)
    runtime = SimpleNamespace(store=store)

    run = resilience.prepare_stale_lease_probe(
        runtime,
        filing_id=document.filing_id,
        workspace_id="alpha",
        probe_id="stale-probe-1",
        lease_seconds=2,
    )

    assert run.status == FilingRunStatus.RUNNING
    assert run.attempt_count == 1
    assert run.worker_id == "resilience-drill:stale-probe-1"
    assert run.input_payload["resilience_drill"] is True


def test_resilience_drill_emits_passing_report(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    app_dir = tmp_path / "app"
    app_dir.mkdir()
    (app_dir / "docker-compose.prod.yml").write_text("services: {}\n")
    env_file = tmp_path / "production.env"
    env_file.write_text("APP_ENV=production\n")
    report_dir = tmp_path / "reports"
    worker_status_calls = 0
    terminated: dict[str, Any] = {}

    def fake_json_command(command: list[str], *, label: str) -> dict[str, Any]:
        nonlocal worker_status_calls
        joined = " ".join(command)
        if "verify-filing-production" in joined:
            return {"passed": True}
        if "worker-start" in joined:
            return {"task_id": "filing-resilience-worker-probe"}
        if "worker-status" in joined:
            worker_status_calls += 1
            if worker_status_calls == 1:
                return {
                    "state": "active",
                    "attempt_count": 1,
                    "first_worker_pid": 42,
                    "first_process_start_ticks": 1_000,
                    "first_hostname": "celery@worker-host",
                }
            return {
                "state": "completed",
                "task_id": "filing-resilience-worker-probe",
                "attempt_count": 2,
                "first_worker_pid": 42,
                "recovered_worker_pid": 84,
                "recovered_process_start_ticks": 2_000,
            }
        if "stale-prepare" in joined:
            return {"run_id": "stale-run-id"}
        if "stale-recover" in joined:
            return {"recovered_run_ids": ["stale-run-id"]}
        if "stale-status" in joined:
            return {
                "status": "completed",
                "attempt_count": 2,
                "candidate_count": 59,
                "approved_fact_count": 59,
                "unique_approved_fact_ids": 59,
                "defect_count": 0,
                "trace_id": "safe-trace-id",
            }
        raise AssertionError(f"unexpected JSON command for {label}: {joined}")

    def fake_command(command: list[str], *, label: str) -> str:
        joined = " ".join(command)
        if " ps --status running -q filing-worker" in joined:
            return "worker-container"
        if "docker inspect" in joined:
            return "worker-host"
        if " inspect ping " in joined:
            return "worker-host: pong"
        raise AssertionError(f"unexpected command for {label}: {joined}")

    monkeypatch.setattr(drill, "_json_command", fake_json_command)
    monkeypatch.setattr(drill, "_command", fake_command)
    monkeypatch.setattr(
        drill,
        "_terminate_probe_child",
        lambda container, pid, start_ticks: terminated.update(
            {
                "container": container,
                "pid": pid,
                "start_ticks": start_ticks,
            }
        ),
    )
    monkeypatch.setattr(drill.time, "sleep", lambda _seconds: None)
    arguments = argparse.Namespace(
        filing_id="739dea02-ef41-5b20-88e5-cfdf6bcb61fc",
        workspace_id="default",
        expected_facts=59,
        app_dir=str(app_dir),
        env_file=str(env_file),
        report_dir=str(report_dir),
        worker_hold_seconds=120,
        lease_seconds=2,
    )

    report, exit_code = drill.run_drill(arguments)

    assert exit_code == 0
    assert report["status"] == "passed"
    assert report["stage"] == "completed"
    assert report["worker_termination"]["redelivered"] is True
    assert report["worker_termination"]["worker_healthy"] is True
    assert report["stale_lease_recovery"]["recovered"] is True
    assert report["stale_lease_recovery"]["unique_approved_fact_ids"] == 59
    assert terminated == {
        "container": "worker-container",
        "pid": 42,
        "start_ticks": 1_000,
    }
    reports = list(report_dir.glob("*.json"))
    assert len(reports) == 1
    assert json_load(reports[0])["status"] == "passed"


def test_resilience_drill_refuses_pid_one() -> None:
    with pytest.raises(drill.DrillFailure, match="unsafe worker PID"):
        drill._terminate_probe_child("worker-container", 1, 1_000)


def test_worker_recovery_task_has_loss_recovery_semantics() -> None:
    assert worker_recovery_probe.acks_late is True
    assert worker_recovery_probe.reject_on_worker_lost is True


def test_resilience_drill_is_executable() -> None:
    assert DRILL_PATH.stat().st_mode & 0o111


def json_load(path: Path) -> dict[str, Any]:
    return json.loads(path.read_text(encoding="utf-8"))
