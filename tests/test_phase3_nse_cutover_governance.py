from __future__ import annotations

from datetime import UTC, date, datetime, timedelta
from importlib import import_module
from types import SimpleNamespace

import pytest
from fastapi.testclient import TestClient
from sqlalchemy import create_engine, select
from sqlalchemy.pool import StaticPool

from trade_research.config import Settings
from trade_research.control_plane.tables import (
    audit_events_table,
    nse_provider_comparison_evidence_table,
    nse_provider_cutover_decisions_table,
)
from trade_research.market_data.cutover import NseProviderCutoverRepository
from trade_research.pipelines import nse_cutover
from trade_research.pipelines.base import PipelineRunResult

api_module = import_module("trade_research.api.app")


def _repository() -> NseProviderCutoverRepository:
    engine = create_engine(
        "sqlite+pysqlite:///:memory:",
        connect_args={"check_same_thread": False},
        poolclass=StaticPool,
    )
    audit_events_table.create(engine)
    nse_provider_comparison_evidence_table.create(engine)
    nse_provider_cutover_decisions_table.create(engine)
    return NseProviderCutoverRepository(engine)


def _record_window(
    repository: NseProviderCutoverRepository,
    *,
    window_end: date,
    status: str = "pass",
) -> dict:
    observed_at = datetime.combine(window_end, datetime.min.time(), tzinfo=UTC)
    return repository.record_comparison(
        status=status,
        metrics={
            "window_start": (window_end - timedelta(days=30)).isoformat(),
            "window_end": window_end.isoformat(),
            "comparison_state": "ready" if status == "pass" else "close_mismatch",
            "ready": status == "pass",
            "row_overlap_ratio": 0.999,
            "close_match_ratio": 0.999,
        },
        blocking_issues=[] if status == "pass" else ["Close mismatch"],
        observed_at=observed_at,
    )


def test_comparison_evidence_is_content_addressed_and_idempotent() -> None:
    repository = _repository()
    end = date(2026, 9, 4)

    first = _record_window(repository, window_end=end)
    second = _record_window(repository, window_end=end)

    assert first["evidence_id"] == second["evidence_id"]
    assert first["evidence_sha256"] == second["evidence_sha256"]
    assert len(repository.recent_evidence()) == 1


def test_approval_requires_consecutive_distinct_passing_windows() -> None:
    repository = _repository()
    start = date(2026, 9, 1)
    for offset in range(4):
        _record_window(repository, window_end=start + timedelta(days=offset))

    eligibility = repository.eligibility(required_passing_windows=5)

    assert eligibility.eligible is False
    assert eligibility.passing_windows == 4
    with pytest.raises(ValueError, match="5 consecutive"):
        repository.approve(
            actor_email="admin@example.com",
            reason="Validated the required evidence bundle.",
            idempotency_key="approval-1",
            expected_evidence_bundle_sha256=eligibility.evidence_bundle_sha256,
            required_passing_windows=5,
        )


def test_authenticated_decision_activates_yfinance_and_rollback_restores_upstox() -> None:
    repository = _repository()
    start = date(2026, 9, 1)
    for offset in range(5):
        _record_window(repository, window_end=start + timedelta(days=offset))
    eligibility = repository.eligibility(required_passing_windows=5)

    approval = repository.approve(
        actor_email="ADMIN@example.com",
        reason="Reviewed overlap, freshness, and close-price evidence.",
        idempotency_key="approval-1",
        expected_evidence_bundle_sha256=eligibility.evidence_bundle_sha256,
        required_passing_windows=5,
        at=datetime(2026, 9, 6, 9, tzinfo=UTC),
    )
    approved = repository.status(configured_primary="yfinance", required_passing_windows=5)

    assert approval.actor_email == "admin@example.com"
    assert approved.yfinance_approved is True
    assert approved.effective_primary == "yfinance"

    rollback = repository.rollback(
        actor_email="admin@example.com",
        reason="Freshness degraded after cutover; restore the prior provider.",
        idempotency_key="rollback-1",
        expected_current_decision_sha256=approval.decision_sha256,
        at=datetime(2026, 9, 6, 10, tzinfo=UTC),
    )
    restored = repository.status(configured_primary="yfinance", required_passing_windows=5)

    assert rollback.action == "rollback_to_upstox"
    assert restored.yfinance_approved is False
    assert restored.effective_primary == "upstox"
    with repository._engine.connect() as connection:
        audit_actions = (
            connection.execute(
                select(audit_events_table.c.action).order_by(audit_events_table.c.created_at)
            )
            .scalars()
            .all()
        )
    assert audit_actions == ["approve_yfinance_primary", "rollback_to_upstox"]


def test_stale_review_digest_and_idempotency_key_reuse_are_rejected() -> None:
    repository = _repository()
    start = date(2026, 9, 1)
    for offset in range(5):
        _record_window(repository, window_end=start + timedelta(days=offset))
    eligibility = repository.eligibility(required_passing_windows=5)

    with pytest.raises(ValueError, match="evidence changed"):
        repository.approve(
            actor_email="admin@example.com",
            reason="Reviewed all required provider comparison evidence.",
            idempotency_key="approval-1",
            expected_evidence_bundle_sha256="0" * 64,
            required_passing_windows=5,
        )

    repository.approve(
        actor_email="admin@example.com",
        reason="Reviewed all required provider comparison evidence.",
        idempotency_key="approval-1",
        expected_evidence_bundle_sha256=eligibility.evidence_bundle_sha256,
        required_passing_windows=5,
    )
    with pytest.raises(ValueError, match="another decision"):
        repository.approve(
            actor_email="admin@example.com",
            reason="A different decision using the same request key.",
            idempotency_key="approval-1",
            expected_evidence_bundle_sha256=eligibility.evidence_bundle_sha256,
            required_passing_windows=5,
        )


def test_cutover_api_requires_admin_and_records_reviewed_evidence(monkeypatch) -> None:
    repository = _repository()
    for offset in range(5):
        _record_window(
            repository,
            window_end=date(2026, 9, 1) + timedelta(days=offset),
        )
    settings = SimpleNamespace(
        admin_emails="admin@example.com",
        admin_email_headers="cf-access-authenticated-user-email",
        nse_daily_primary_source="yfinance",
        nse_cutover_required_passing_windows=5,
    )
    monkeypatch.setattr(api_module, "get_settings", lambda: settings)
    monkeypatch.setattr(
        api_module,
        "_nse_provider_cutover_repository",
        lambda workspace_id: repository,
    )

    with TestClient(api_module.app) as client:
        status_response = client.get("/api/data/operations/nse-provider-cutover")
        bundle = status_response.json()["eligibility"]["evidence_bundle_sha256"]
        denied = client.post(
            "/api/admin/nse-provider-cutover/approve",
            headers={"X-Idempotency-Key": "approval-api-1"},
            json={
                "reason": "Reviewed all provider evidence for this cutover.",
                "expected_evidence_bundle_sha256": bundle,
            },
        )
        approved = client.post(
            "/api/admin/nse-provider-cutover/approve",
            headers={
                "cf-access-authenticated-user-email": "ADMIN@example.com",
                "X-Idempotency-Key": "approval-api-1",
            },
            json={
                "reason": "Reviewed all provider evidence for this cutover.",
                "expected_evidence_bundle_sha256": bundle,
            },
        )

    assert status_response.status_code == 200
    assert denied.status_code == 403
    assert approved.status_code == 200
    assert approved.json()["effective_primary"] == "yfinance"
    assert approved.json()["active_decision"]["actor_email"] == "admin@example.com"


def test_yfinance_primary_pipeline_fails_closed_without_active_approval(
    monkeypatch,
) -> None:
    repository = _repository()
    settings = Settings(
        _env_file=None,
        yfinance_daily_enabled=True,
        yfinance_nse_enabled=True,
        nse_daily_primary_source="yfinance",
    )
    monkeypatch.setattr(nse_cutover, "get_settings", lambda: settings)
    monkeypatch.setattr(
        nse_cutover,
        "run_nse_yfinance_cutover_readiness",
        lambda **_kwargs: PipelineRunResult(
            name="readiness", status="pass", metrics={"ready": True}
        ),
    )
    monkeypatch.setattr(
        nse_cutover,
        "TimescaleStore",
        lambda _url: SimpleNamespace(engine=repository._engine),
    )

    result = nse_cutover.run_nse_daily_ohlcv_primary_pipeline()

    assert result.status == "fail"
    assert result.metrics["effective_primary_source"] == "upstox"
    assert "no active authenticated" in result.blocking_issues[0]
