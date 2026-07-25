from __future__ import annotations

import argparse
import importlib.util
import json
from pathlib import Path
from types import SimpleNamespace
from typing import Any

import pytest

from trade_research.config import Settings
from trade_research.filings.human_review_acceptance import (
    human_review_acceptance_status,
    verify_reviewer_identity,
)
from trade_research.filings.store import FilingStore

REPOSITORY_ROOT = Path(__file__).resolve().parents[1]
DRILL_PATH = REPOSITORY_ROOT / "deploy" / "filing-human-review-drill.py"
_DRILL_SPEC = importlib.util.spec_from_file_location(
    "filing_human_review_drill",
    DRILL_PATH,
)
assert _DRILL_SPEC is not None and _DRILL_SPEC.loader is not None
drill = importlib.util.module_from_spec(_DRILL_SPEC)
_DRILL_SPEC.loader.exec_module(drill)


def _settings(tmp_path: Path) -> Settings:
    return Settings(
        app_env="test",
        data_dir=tmp_path,
        database_url=f"sqlite:///{tmp_path / 'filings.sqlite3'}",
        admin_emails="reviewer@example.test,other@example.test",
        admin_email_headers=("cf-access-authenticated-user-email,x-forwarded-email"),
        filing_index_enabled=False,
        langfuse_enabled=False,
        otel_enabled=False,
    )


def test_reviewer_identity_must_use_configured_allowlist_and_header(
    tmp_path: Path,
) -> None:
    settings = _settings(tmp_path)

    verified = verify_reviewer_identity(
        settings,
        reviewer_email="Reviewer@Example.Test",
        actor_header="CF-Access-Authenticated-User-Email",
    )

    assert verified == {
        "reviewer_email": "reviewer@example.test",
        "actor_header": "cf-access-authenticated-user-email",
        "header_configured": True,
        "reviewer_allowlisted": True,
    }
    with pytest.raises(ValueError, match="allowlist"):
        verify_reviewer_identity(
            settings,
            reviewer_email="unknown@example.test",
            actor_header="cf-access-authenticated-user-email",
        )
    with pytest.raises(ValueError, match="not configured"):
        verify_reviewer_identity(
            settings,
            reviewer_email="reviewer@example.test",
            actor_header="x-untrusted-email",
        )


def test_audit_event_queries_are_workspace_and_target_scoped(
    tmp_path: Path,
) -> None:
    store = FilingStore(f"sqlite:///{tmp_path / 'filings.sqlite3'}")
    store.initialize()
    store.record_audit_event(
        workspace_id="alpha",
        actor_id="reviewer@example.test",
        action="review.approve",
        target_type="filing_review",
        target_id="review-1",
        reason="Reviewed.",
    )
    store.record_audit_event(
        workspace_id="alpha",
        actor_id="reviewer@example.test",
        action="review.reject",
        target_type="filing_review",
        target_id="review-2",
        reason="Rejected.",
    )
    store.record_audit_event(
        workspace_id="beta",
        actor_id="other@example.test",
        action="review.approve",
        target_type="filing_review",
        target_id="review-1",
        reason="Different workspace.",
    )

    events = store.audit_events(
        workspace_id="alpha",
        action="review.approve",
        target_type="filing_review",
        target_id="review-1",
    )

    assert len(events) == 1
    assert events[0]["actor_id"] == "reviewer@example.test"
    assert events[0]["reason"] == "Reviewed."


def test_human_review_acceptance_status_joins_review_facts_and_audit() -> None:
    review = SimpleNamespace(
        review_id="review-1",
        run_id="run-1",
        status=SimpleNamespace(value="approved"),
        reviewer_id="reviewer@example.test",
        reason="Reviewed against evidence.",
        decision_payload={"decision": "approve"},
        decided_at=SimpleNamespace(isoformat=lambda: "2026-07-25T00:00:00+00:00"),
        payload={
            "candidate_facts": [{"candidate_id": "candidate-1"}],
            "intelligence_objects": [],
            "evidence": [{"evidence_id": "evidence-1"}],
            "defects": [],
        },
    )
    run = SimpleNamespace(
        run_id="run-1",
        workspace_id="alpha",
        company_id="NSE:INFY",
        filing_id="filing-1",
        status=SimpleNamespace(value="completed"),
        current_node="completed",
        attempt_count=2,
        worker_id=None,
        lease_expires_at=None,
        waiting_review_at=SimpleNamespace(isoformat=lambda: "2026-07-25T00:00:00+00:00"),
        trace_id="trace-1",
        output_payload={
            "review_id": "review-1",
            "review_status": "approved",
        },
    )
    fact = SimpleNamespace(
        fact_id="fact-1",
        source_filing_id="filing-1",
        run_id="run-1",
        review_status=SimpleNamespace(value="approved"),
        approved_by="reviewer@example.test",
    )
    store = SimpleNamespace(
        review=lambda _review_id, _workspace_id: review,
        run=lambda _run_id, _workspace_id: run,
        candidate_facts=lambda _run_id: [{"candidate_id": "candidate-1"}],
        validation_defects=lambda _run_id: [],
        approved_facts=lambda **_kwargs: [fact],
        audit_events=lambda **_kwargs: [
            {
                "actor_id": "reviewer@example.test",
                "action": "review.approve",
                "reason": "Reviewed against evidence.",
            }
        ],
    )

    status = human_review_acceptance_status(
        SimpleNamespace(store=store),
        review_id="review-1",
        workspace_id="alpha",
    )

    assert status["run_status"] == "completed"
    assert status["review_status"] == "approved"
    assert status["approved_fact_count"] == 1
    assert status["reviewer_approved_fact_count"] == 1
    assert status["review_status_counts"] == {"approved": 1}
    assert status["audit_event_count"] == 1
    assert status["audit_reason_matches"] is True


def test_human_review_drill_emits_passing_report(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    app_dir = tmp_path / "app"
    app_dir.mkdir()
    (app_dir / "docker-compose.prod.yml").write_text(
        "services: {}\n",
        encoding="utf-8",
    )
    env_file = tmp_path / "production.env"
    env_file.write_text("APP_ENV=production\n", encoding="utf-8")
    report_dir = tmp_path / "reports"
    run_status_calls = 0

    def fake_json_command(command: list[str], *, label: str) -> Any:
        joined = " ".join(command)
        if "verify-filing-production" in joined:
            return {"passed": True}
        if "human_review_acceptance identity" in joined:
            return {
                "reviewer_email": "reviewer@example.test",
                "actor_header": "cf-access-authenticated-user-email",
                "header_configured": True,
                "reviewer_allowlisted": True,
            }
        if "human_review_acceptance status" in joined:
            return {
                "run_status": "completed",
                "current_node": "completed",
                "attempt_count": 2,
                "waiting_review_at": "2026-07-25T18:30:00Z",
                "worker_id": None,
                "lease_expires_at": None,
                "trace_id": "trace-1",
                "output_review_id": "review-1",
                "output_review_status": "approved",
                "review_status": "approved",
                "reviewer_id": "reviewer@example.test",
                "reason": "Reviewed against the locked golden dataset.",
                "decision": "approve",
                "decided_at": "2026-07-25T00:00:00+00:00",
                "approved_fact_count": 59,
                "unique_approved_fact_ids": 59,
                "review_status_counts": {"approved": 59},
                "reviewer_approved_fact_count": 59,
                "validation_defect_count": 0,
                "audit_event_count": 1,
                "audit_actor_ids": ["reviewer@example.test"],
                "audit_actions": ["review.approve"],
                "audit_reason_matches": True,
            }
        raise AssertionError(f"unexpected JSON command for {label}: {joined}")

    def fake_api_request(
        _compose: list[str],
        *,
        method: str,
        path: str,
        workspace_id: str,
        actor_header: str | None = None,
        actor_email: str | None = None,
        payload: dict[str, Any] | None = None,
    ) -> Any:
        nonlocal run_status_calls
        assert workspace_id == "default"
        if method == "POST" and path == "/api/filings/runs":
            assert actor_header == "cf-access-authenticated-user-email"
            assert actor_email == "reviewer@example.test"
            assert payload and payload["force_review"] is True
            return {"accepted": True, "run": {"run_id": "run-1"}}
        if method == "GET" and path == "/api/filings/runs/run-1":
            run_status_calls += 1
            if run_status_calls == 1:
                return {
                    "status": "waiting_review",
                    "current_node": "human_review",
                    "attempt_count": 1,
                    "worker_id": None,
                    "lease_expires_at": None,
                    "output_payload": {"review_id": "review-1"},
                }
            return {"status": "completed"}
        if method == "GET" and path == "/api/filings/reviews/review-1":
            return {
                "review_id": "review-1",
                "run_id": "run-1",
                "status": "pending",
                "payload": {
                    "candidate_facts": [{} for _ in range(59)],
                    "evidence": [{} for _ in range(59)],
                    "defects": [],
                },
            }
        if method == "POST" and path.endswith("/decision"):
            assert actor_email == "reviewer@example.test"
            assert payload == {
                "decision": "approve",
                "reason": "Reviewed against the locked golden dataset.",
            }
            return {"status": "queued"}
        raise AssertionError(f"unexpected API request: {method} {path}")

    monkeypatch.setattr(drill, "_json_command", fake_json_command)
    monkeypatch.setattr(drill, "_api_request", fake_api_request)
    arguments = argparse.Namespace(
        filing_id="739dea02-ef41-5b20-88e5-cfdf6bcb61fc",
        reviewer_email="reviewer@example.test",
        actor_header="cf-access-authenticated-user-email",
        workspace_id="default",
        expected_facts=59,
        reason="Reviewed against the locked golden dataset.",
        app_dir=str(app_dir),
        env_file=str(env_file),
        report_dir=str(report_dir),
    )

    report, exit_code = drill.run_drill(arguments)

    assert exit_code == 0
    assert report["status"] == "passed"
    assert report["stage"] == "completed"
    assert report["interrupt"]["worker_lease_released"] is True
    assert report["interrupt"]["packet_candidate_count"] == 59
    assert report["decision"]["audit_verified"] is True
    assert report["resume"]["attempt_count"] == 2
    assert report["resume"]["approved_fact_count"] == 59
    assert report["resume"]["worker_lease_released"] is True
    reports = list(report_dir.glob("*.json"))
    assert len(reports) == 1
    assert json.loads(reports[0].read_text(encoding="utf-8"))["status"] == "passed"


def test_human_review_drill_is_executable() -> None:
    assert DRILL_PATH.stat().st_mode & 0o111
