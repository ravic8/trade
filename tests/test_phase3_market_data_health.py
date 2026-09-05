from __future__ import annotations

from datetime import UTC, date, datetime, timedelta
from importlib import import_module
from types import SimpleNamespace

from fastapi.testclient import TestClient
from sqlalchemy import create_engine
from sqlalchemy.pool import StaticPool

from trade_research.control_plane.tables import (
    artifact_manifests_table,
    market_data_quality_outcomes_table,
    market_data_replication_checkpoints_table,
)
from trade_research.market_data.health import MarketDataHealthRepository

NOW = datetime(2026, 9, 5, 13, 0, tzinfo=UTC)
api_module = import_module("trade_research.api.app")


def _engine():
    engine = create_engine(
        "sqlite+pysqlite:///:memory:",
        connect_args={"check_same_thread": False},
        poolclass=StaticPool,
    )
    artifact_manifests_table.create(engine)
    market_data_quality_outcomes_table.create(engine)
    market_data_replication_checkpoints_table.create(engine)
    return engine


def _quality_row(
    outcome_id: str,
    *,
    source_run_id: str,
    status: str,
    observed_at: datetime,
    instrument_id: str = "NSE_EQ|RELIANCE",
    raw_artifact_id: str | None = None,
) -> dict:
    return {
        "quality_outcome_id": outcome_id,
        "workspace_id": "workspace-1",
        "source_run_id": source_run_id,
        "request_id": f"request-{source_run_id}",
        "provider": "yfinance",
        "exchange": "NSE",
        "interval": "1m",
        "instrument_id": instrument_id,
        "provider_symbol": "RELIANCE.NS",
        "session_date": date(2026, 9, 5),
        "candle_timestamp": NOW - timedelta(minutes=5),
        "status": status,
        "reason_code": "accepted" if status == "valid" else "candle_absent",
        "severity": "info" if status == "valid" else "error",
        "expected": True,
        "retryable": status != "valid",
        "raw_artifact_id": raw_artifact_id,
        "details": {},
        "observed_at": observed_at,
        "created_at": observed_at,
        "updated_at": observed_at,
    }


def test_health_uses_latest_run_and_surfaces_quality_lineage_and_replication() -> None:
    engine = _engine()
    old = NOW - timedelta(days=1)
    with engine.begin() as connection:
        connection.execute(
            artifact_manifests_table.insert(),
            [
                {
                    "artifact_manifest_id": "artifact-1",
                    "artifact_type": "market_data_raw_response",
                    "storage_uri": "s3://private/raw-1.json",
                    "sha256": "a" * 64,
                    "size_bytes": 512,
                    "media_type": "application/json",
                    "object_version_id": "version-1",
                    "manifest_metadata": {},
                    "created_at": NOW,
                }
            ],
        )
        connection.execute(
            market_data_quality_outcomes_table.insert(),
            [
                _quality_row(
                    "old-missing",
                    source_run_id="old-run",
                    status="missing",
                    observed_at=old,
                ),
                _quality_row(
                    "valid-1",
                    source_run_id="latest-run",
                    status="valid",
                    observed_at=NOW,
                    raw_artifact_id="artifact-1",
                ),
                _quality_row(
                    "valid-2",
                    source_run_id="latest-run",
                    status="valid",
                    observed_at=NOW,
                    instrument_id="NSE_EQ|INFY",
                    raw_artifact_id="artifact-1",
                ),
                _quality_row(
                    "latest-missing",
                    source_run_id="latest-run",
                    status="missing",
                    observed_at=NOW,
                    instrument_id="NSE_EQ|TCS",
                ),
            ],
        )
        connection.execute(
            market_data_replication_checkpoints_table.insert(),
            [
                {
                    "replication_checkpoint_id": "checkpoint-1",
                    "workspace_id": "workspace-1",
                    "source_run_id": "latest-run",
                    "source_store": "timescaledb",
                    "destination_store": "clickhouse",
                    "dataset_key": "ohlcv_intraday",
                    "exchange": "NSE",
                    "interval": "1m",
                    "status": "reconciled",
                    "source_row_count": 2,
                    "destination_row_count": 2,
                    "source_digest": "b" * 64,
                    "destination_digest": "b" * 64,
                    "source_watermark": NOW,
                    "destination_watermark": NOW,
                    "watermark_lag_seconds": 0.0,
                    "replication_latency_ms": 125.0,
                    "error_message": None,
                    "details": {},
                    "started_at": NOW - timedelta(seconds=1),
                    "completed_at": NOW,
                    "created_at": NOW,
                    "updated_at": NOW,
                }
            ],
        )

    snapshot = MarketDataHealthRepository(engine).snapshot(
        workspace_id="workspace-1"
    )

    assert snapshot.health_status == "failed"
    assert len(snapshot.quality) == 1
    summary = snapshot.quality[0]
    assert summary.source_run_id == "latest-run"
    assert summary.total_outcomes == 3
    assert summary.status_counts["valid"] == 2
    assert summary.unexplained_gap_count == 1
    assert summary.completeness_ratio == 2 / 3
    assert summary.raw_artifact_count == 1
    assert snapshot.issues[0].reason_code == "candle_absent"
    assert snapshot.issues[0].occurrences == 1
    assert snapshot.raw_lineage[0].artifact_manifest_id == "artifact-1"
    assert snapshot.raw_lineage[0].sha256 == "a" * 64
    assert snapshot.raw_lineage[0].object_versioned is True
    assert snapshot.replication[0].counts_match is True
    assert snapshot.replication[0].digests_match is True


class _HealthRepository:
    def __init__(self, snapshot) -> None:
        self._snapshot = snapshot

    def snapshot(self, **filters):
        assert filters == {
            "workspace_id": "workspace-1",
            "provider": "yfinance",
            "exchange": "NSE",
            "issue_limit": 10,
            "lineage_limit": 5,
        }
        return self._snapshot


def test_market_data_health_api_is_workspace_scoped(monkeypatch) -> None:
    repository = MarketDataHealthRepository(_engine())
    snapshot = repository.snapshot(workspace_id="workspace-1")
    monkeypatch.setattr(
        "trade_research.api.app._market_data_health_repository",
        lambda: _HealthRepository(snapshot),
    )
    monkeypatch.setattr(
        "trade_research.api.app.get_settings",
        lambda: SimpleNamespace(
            phase3_market_data_enabled=True,
            clickhouse_enabled=True,
        ),
    )

    with TestClient(api_module.app) as client:
        response = client.get(
            "/api/data/operations/market-data-health",
            params={"issue_limit": 10, "lineage_limit": 5},
            headers={"X-Workspace-ID": "workspace-1"},
        )

    assert response.status_code == 200
    assert response.json() == {
        "enabled": True,
        "clickhouse_enabled": True,
        "workspace_id": "workspace-1",
        "provider": "yfinance",
        "exchange": "NSE",
        "health_status": "unknown",
        "checked_at": response.json()["checked_at"],
        "quality": [],
        "issues": [],
        "raw_lineage": [],
        "replication": [],
    }


def test_market_data_health_api_rejects_unsupported_scope() -> None:
    with TestClient(api_module.app) as client:
        exchange = client.get(
            "/api/data/operations/market-data-health?exchange=TSX"
        )
        provider = client.get(
            "/api/data/operations/market-data-health?provider=upstox"
        )
        workspace = client.get(
            "/api/data/operations/market-data-health",
            headers={"X-Workspace-ID": " "},
        )

    assert exchange.status_code == 400
    assert provider.status_code == 400
    assert workspace.status_code == 400
