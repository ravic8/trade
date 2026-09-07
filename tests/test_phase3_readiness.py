from __future__ import annotations

from dataclasses import replace
from datetime import UTC, date, datetime, timedelta
from decimal import Decimal

import pytest
from pydantic import ValidationError
from sqlalchemy import create_engine

from trade_research.config import Settings
from trade_research.control_plane.tables import (
    audit_events_table,
    market_data_availability_observations_table,
    market_data_quality_outcomes_table,
    market_data_replication_checkpoints_table,
    nse_provider_comparison_evidence_table,
    nse_provider_cutover_decisions_table,
    phase3_readiness_evidence_table,
)
from trade_research.dagster.schedule_policy import desired_schedule_statuses
from trade_research.market_data.contracts import (
    CandleInterval,
    MarketCandle,
    candle_business_sha256,
)
from trade_research.market_data.cutover import NseProviderCutoverRepository
from trade_research.market_data.readiness import Phase3ReadinessRepository


def _engine():
    engine = create_engine("sqlite://")
    for table in (
        audit_events_table,
        market_data_quality_outcomes_table,
        market_data_replication_checkpoints_table,
        market_data_availability_observations_table,
        nse_provider_comparison_evidence_table,
        nse_provider_cutover_decisions_table,
        phase3_readiness_evidence_table,
    ):
        table.create(engine)
    return engine


def _record_run(engine, *, run_id: str, interval: str, business_digest: str) -> None:
    at = datetime(2026, 9, 6, 10, tzinfo=UTC)
    candle_timestamp = at if interval == "1m" else None
    with engine.begin() as connection:
        connection.execute(
            market_data_quality_outcomes_table.insert(),
            {
                "quality_outcome_id": f"quality-{run_id}",
                "workspace_id": "default",
                "source_run_id": run_id,
                "request_id": f"request-{run_id}",
                "provider": "yfinance",
                "exchange": "NSE",
                "interval": interval,
                "instrument_id": "NSE_EQ|RELIANCE",
                "provider_symbol": "RELIANCE.NS",
                "session_date": date(2026, 9, 5),
                "candle_timestamp": candle_timestamp,
                "status": "valid",
                "reason_code": "accepted",
                "severity": "info",
                "expected": True,
                "retryable": False,
                "raw_artifact_id": "artifact-1",
                "details": {},
                "observed_at": at,
                "created_at": at,
                "updated_at": at,
            },
        )
        connection.execute(
            market_data_replication_checkpoints_table.insert(),
            {
                "replication_checkpoint_id": f"replica-{run_id}",
                "workspace_id": "default",
                "source_run_id": run_id,
                "source_store": "validated_batch",
                "destination_store": "clickhouse",
                "dataset_key": "ohlcv_daily" if interval == "1d" else "ohlcv_intraday",
                "exchange": "NSE",
                "interval": interval,
                "status": "reconciled",
                "source_row_count": 1,
                "destination_row_count": 1,
                "source_digest": "a" * 64,
                "destination_digest": "a" * 64,
                "source_watermark": at,
                "destination_watermark": at,
                "watermark_lag_seconds": 0,
                "replication_latency_ms": 10,
                "error_message": None,
                "details": {
                    "business_row_count": 1,
                    "business_digest": business_digest,
                },
                "started_at": at,
                "completed_at": at,
                "created_at": at,
                "updated_at": at,
            },
        )
        if interval == "1m":
            sessions = [f"2026-09-0{day}" for day in range(1, 6)]
            connection.execute(
                market_data_availability_observations_table.insert(),
                {
                    "availability_observation_id": f"availability-{run_id}",
                    "workspace_id": "default",
                    "source_run_id": run_id,
                    "request_id": f"request-{run_id}",
                    "provider": "yfinance",
                    "exchange": "NSE",
                    "interval": "1m",
                    "instrument_id": "NSE_EQ|RELIANCE",
                    "provider_symbol": "RELIANCE.NS",
                    "requested_start": at - timedelta(days=7),
                    "requested_end": at,
                    "eligible_sessions": sessions,
                    "observed_sessions": sessions,
                    "observed_first_timestamp": at - timedelta(days=5),
                    "observed_last_timestamp": at,
                    "observed_row_count": 1,
                    "status": "observed",
                    "reason_code": "provider_returned_data",
                    "retryable": False,
                    "raw_artifact_id": "artifact-1",
                    "observed_at": at,
                    "created_at": at,
                },
            )


def _record_provider_windows(repository: NseProviderCutoverRepository) -> None:
    for offset in range(5):
        window_end = date(2026, 9, 1) + timedelta(days=offset)
        repository.record_comparison(
            status="pass",
            metrics={
                "window_start": (window_end - timedelta(days=30)).isoformat(),
                "window_end": window_end.isoformat(),
                "comparison_state": "ready",
                "ready": True,
            },
            blocking_issues=[],
            observed_at=datetime.combine(window_end, datetime.min.time(), tzinfo=UTC),
        )


def test_canary_assessment_proves_scope_quality_replica_and_rerun() -> None:
    engine = _engine()
    _record_run(engine, run_id="daily-1", interval="1d", business_digest="d" * 64)
    _record_run(engine, run_id="minute-1", interval="1m", business_digest="m" * 64)
    _record_run(engine, run_id="minute-2", interval="1m", business_digest="m" * 64)
    repository = Phase3ReadinessRepository(engine)

    evidence = repository.assess_canary(
        daily_run_id="daily-1",
        minute_run_id="minute-1",
        minute_rerun_id="minute-2",
        max_instruments=25,
        required_observed_sessions=5,
    )
    replay = repository.assess_canary(
        daily_run_id="daily-1",
        minute_run_id="minute-1",
        minute_rerun_id="minute-2",
        max_instruments=25,
        required_observed_sessions=5,
    )

    assert evidence["status"] == "pass"
    assert evidence["metrics"]["idempotence"]["equivalent"] is True
    assert len(evidence["session_dates"]) == 5
    assert replay["evidence_id"] == evidence["evidence_id"]
    assert len(repository.recent_evidence()) == 1


def test_canary_fails_closed_without_business_digest_and_availability() -> None:
    engine = _engine()
    _record_run(engine, run_id="daily-1", interval="1d", business_digest="d" * 64)
    repository = Phase3ReadinessRepository(engine)

    evidence = repository.assess_canary(
        daily_run_id="daily-1",
        minute_run_id="missing-1",
        minute_rerun_id="missing-2",
        max_instruments=25,
        required_observed_sessions=5,
    )

    assert evidence["status"] == "fail"
    assert any("no durable quality" in issue for issue in evidence["blocking_issues"])
    assert any("Independent minute reruns" in issue for issue in evidence["blocking_issues"])


def test_minute_completeness_excludes_explained_provider_unavailability() -> None:
    engine = _engine()
    _record_run(engine, run_id="daily-1", interval="1d", business_digest="d" * 64)
    _record_run(engine, run_id="minute-1", interval="1m", business_digest="m" * 64)
    _record_run(engine, run_id="minute-2", interval="1m", business_digest="m" * 64)
    with engine.begin() as connection:
        for run_id in ("minute-1", "minute-2"):
            connection.execute(
                market_data_quality_outcomes_table.insert(),
                {
                    "quality_outcome_id": f"provider-boundary-{run_id}",
                    "workspace_id": "default",
                    "source_run_id": run_id,
                    "request_id": f"request-{run_id}",
                    "provider": "yfinance",
                    "exchange": "NSE",
                    "interval": "1m",
                    "instrument_id": "NSE_EQ|RELIANCE",
                    "provider_symbol": "RELIANCE.NS",
                    "session_date": date(2026, 9, 5),
                    "candle_timestamp": datetime(2026, 9, 5, 10, tzinfo=UTC),
                    "status": "provider_unavailable",
                    "reason_code": "outside_provider_observed_session_window",
                    "severity": "warning",
                    "expected": True,
                    "retryable": False,
                    "raw_artifact_id": "artifact-1",
                    "details": {},
                    "observed_at": datetime(2026, 9, 6, 10, tzinfo=UTC),
                    "created_at": datetime(2026, 9, 6, 10, tzinfo=UTC),
                    "updated_at": datetime(2026, 9, 6, 10, tzinfo=UTC),
                },
            )

    evidence = Phase3ReadinessRepository(engine).assess_canary(
        daily_run_id="daily-1",
        minute_run_id="minute-1",
        minute_rerun_id="minute-2",
        max_instruments=25,
        required_observed_sessions=5,
    )

    assert evidence["status"] == "pass"
    assert evidence["metrics"]["runs"]["minute"]["quality"]["completeness_ratio"] == 1.0


def test_readiness_requires_canary_drill_and_provider_windows() -> None:
    engine = _engine()
    _record_run(engine, run_id="daily-1", interval="1d", business_digest="d" * 64)
    _record_run(engine, run_id="minute-1", interval="1m", business_digest="m" * 64)
    _record_run(engine, run_id="minute-2", interval="1m", business_digest="m" * 64)
    readiness_repository = Phase3ReadinessRepository(engine)
    readiness_repository.assess_canary(
        daily_run_id="daily-1",
        minute_run_id="minute-1",
        minute_rerun_id="minute-2",
        max_instruments=25,
        required_observed_sessions=5,
    )
    cutover = NseProviderCutoverRepository(engine)
    _record_provider_windows(cutover)
    eligibility = cutover.eligibility(required_passing_windows=5)
    initial = cutover.approve(
        actor_email="admin@example.com",
        reason="Initial reviewed provider cutover approval.",
        idempotency_key="initial-approval",
        expected_evidence_bundle_sha256=eligibility.evidence_bundle_sha256,
        required_passing_windows=5,
        at=datetime(2026, 9, 6, 8, tzinfo=UTC),
    )
    rollback = cutover.rollback(
        actor_email="admin@example.com",
        reason="Exercise the documented provider rollback path.",
        idempotency_key="rollback-drill",
        expected_current_decision_sha256=initial.decision_sha256,
        at=datetime(2026, 9, 6, 9, tzinfo=UTC),
    )
    restored = cutover.approve(
        actor_email="admin@example.com",
        reason="Restore only after the rollback checks completed.",
        idempotency_key="restore-approval",
        expected_evidence_bundle_sha256=eligibility.evidence_bundle_sha256,
        required_passing_windows=5,
        at=datetime(2026, 9, 6, 10, tzinfo=UTC),
    )
    readiness_repository.record_rollback_restore_drill(
        actor_email="admin@example.com",
        reason="Reviewed the rollback and explicit restore drill evidence.",
        rollback_decision_sha256=rollback.decision_sha256,
        restored_decision_sha256=restored.decision_sha256,
        checks={
            "rollback_effective_provider_upstox": True,
            "upstox_pipeline_healthy": True,
            "restore_required_explicit_approval": True,
            "post_restore_yfinance_pipeline_healthy": True,
        },
    )

    readiness = readiness_repository.readiness(required_passing_windows=5)

    assert readiness.ready_for_production is True
    assert all(gate.passed for gate in readiness.gates)


def test_business_digest_ignores_request_lineage_but_detects_value_change() -> None:
    candle = MarketCandle(
        instrument_id="NSE_EQ|RELIANCE",
        provider_symbol="RELIANCE.NS",
        symbol="RELIANCE",
        exchange="NSE",
        session_date=date(2026, 9, 5),
        timestamp=datetime(2026, 9, 5, 3, 45, tzinfo=UTC),
        interval=CandleInterval.ONE_MINUTE,
        open=Decimal("100"),
        high=Decimal("101"),
        low=Decimal("99"),
        close=Decimal("100.5"),
        volume=100,
        currency="INR",
        provider="yfinance",
        provider_timestamp=datetime(2026, 9, 6, tzinfo=UTC),
        request_id="request-1",
        raw_artifact_id="artifact-1",
        adapter_version="yfinance-v1",
    )

    rerun = replace(
        candle,
        request_id="request-2",
        raw_artifact_id="artifact-2",
        provider_timestamp=datetime(2026, 9, 7, tzinfo=UTC),
    )

    assert candle_business_sha256(rerun) == candle_business_sha256(candle)
    changed = replace(rerun, close=Decimal("100.6"))
    assert candle_business_sha256(changed) != candle_business_sha256(candle)


def test_production_schedule_has_a_separate_fail_closed_activation_gate() -> None:
    settings = Settings(_env_file=None)

    assert settings.phase3_production_activation_enabled is False
    assert desired_schedule_statuses(settings)["yfinance_nse_minute_schedule"] == "stopped"
    with pytest.raises(ValidationError, match="production activation requires"):
        Settings(_env_file=None, phase3_production_activation_enabled=True)
