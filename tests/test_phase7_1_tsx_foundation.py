from __future__ import annotations

from datetime import UTC, datetime
from pathlib import Path

import pytest
from alembic import command
from alembic.config import Config
from sqlalchemy import Column, DateTime, MetaData, String, Table, create_engine, text

from trade_research.config import Settings
from trade_research.pipelines import universe_snapshot
from trade_research.universe.persisted import (
    UniverseRefreshResult,
    UniverseValidationResult,
)


class _TSXProvider:
    exchange = "TSX"
    source = "tsx_test"

    def fetch(self):
        raise AssertionError("service.refresh is stubbed in this test")

    def diagnostics(self):
        return {"source_rows": 3, "tsx_rows": 1}


def test_disabled_tsx_flag_prevents_universe_backfill_enqueue(monkeypatch) -> None:
    settings = Settings(
        _env_file=None,
        yfinance_daily_enabled=True,
        yfinance_full_us_enabled=True,
        yfinance_full_tsx_enabled=False,
        yfinance_nse_enabled=False,
    )
    captured: dict[str, bool] = {}

    def fake_refresh(self, provider, policy, **kwargs):
        captured["enqueue_backfills"] = kwargs["enqueue_backfills"]
        return UniverseRefreshResult(
            snapshot_id="tsx-snapshot",
            exchange="TSX",
            source="tsx_test",
            status="accepted",
            symbol_count=1,
            validation=UniverseValidationResult(
                accepted=True,
                symbol_count=1,
                previous_symbol_count=None,
                change_ratio=None,
                errors=(),
            ),
            events_written=1,
            work_items_queued=0,
        )

    monkeypatch.setattr(universe_snapshot, "get_settings", lambda: settings)
    monkeypatch.setattr(
        universe_snapshot.PersistedUniverseService,
        "refresh",
        fake_refresh,
    )

    result = universe_snapshot.run_equity_universe_snapshot_pipeline(
        "TSX",
        provider=_TSXProvider(),
        repository=object(),
    )

    assert captured["enqueue_backfills"] is False
    assert result.metrics["backfill_planning_enabled"] is False
    assert result.metrics["backfill_execution_enabled"] is False
    assert result.metrics["source_diagnostics"] == {"source_rows": 3, "tsx_rows": 1}


def test_ca_migration_keeps_canonical_tsx_row_and_moves_non_conflicts(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    database_url = f"sqlite:///{tmp_path / 'tsx-canonicalization.sqlite'}"
    engine = create_engine(database_url)
    legacy = MetaData()
    symbols = Table(
        "symbols",
        legacy,
        Column("symbol", String, primary_key=True),
        Column("exchange", String, primary_key=True),
        Column("fetched_at", DateTime(timezone=True), nullable=False),
    )
    legacy.create_all(engine)
    observed_at = datetime(2026, 7, 18, tzinfo=UTC)
    with engine.begin() as connection:
        connection.execute(
            symbols.insert(),
            [
                {"symbol": "SHOP", "exchange": "CA", "fetched_at": observed_at},
                {"symbol": "SHOP", "exchange": "TSX", "fetched_at": observed_at},
                {"symbol": "RY", "exchange": "CA", "fetched_at": observed_at},
            ],
        )

    monkeypatch.setenv("DATABASE_URL", database_url)
    command.upgrade(Config("alembic.ini"), "head")

    with engine.connect() as connection:
        rows = connection.execute(
            text("SELECT symbol, exchange FROM symbols ORDER BY symbol, exchange")
        ).all()
        revision = connection.execute(text("SELECT version_num FROM alembic_version")).scalar_one()

    assert rows == [("RY", "TSX"), ("SHOP", "TSX")]
    assert revision == "20260718_0006"
