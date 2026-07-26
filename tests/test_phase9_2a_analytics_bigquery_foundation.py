from __future__ import annotations

from datetime import UTC, date, datetime
from importlib import util
from pathlib import Path
from typing import Any

import pytest
from alembic import command
from alembic.config import Config
from pydantic import ValidationError
from sqlalchemy import create_engine, inspect

from trade_research.analytics.access import analyst_role_statements
from trade_research.analytics.bigquery import (
    ENTITY_SPECS,
    MergeResult,
    ReconciliationResult,
    _source_metrics,
    run_bigquery_sync,
)
from trade_research.config import Settings
from trade_research.storage.timescale import TimescaleStore, metadata, ohlcv_daily_table


def test_phase9_2a_migration_adds_sync_state_and_declares_curated_views(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    database_url = f"sqlite:///{tmp_path / 'phase9-2a.sqlite'}"
    monkeypatch.setenv("DATABASE_URL", database_url)
    config = Config("alembic.ini")
    command.upgrade(config, "head")

    engine = create_engine(database_url)
    assert {"bigquery_sync_runs", "bigquery_sync_partitions"}.issubset(
        inspect(engine).get_table_names()
    )
    with engine.connect() as connection:
        revision = connection.exec_driver_sql(
            "SELECT version_num FROM alembic_version"
        ).scalar_one()
    assert revision == "20260726_0012"

    migration_path = Path(
        "migrations/versions/20260719_0007_phase9_2a_analytics_bigquery_foundation.py"
    )
    module_spec = util.spec_from_file_location("phase9_2a_migration", migration_path)
    assert module_spec and module_spec.loader
    module = util.module_from_spec(module_spec)
    module_spec.loader.exec_module(module)
    assert set(module.ANALYTICS_VIEWS) == {
        "ohlcv_daily",
        "symbol_state",
        "pipeline_work_state",
        "ingestion_runs",
        "provider_health",
        "universe_lifecycle",
    }


def test_analyst_role_policy_is_analytics_only_and_read_only() -> None:
    statements = analyst_role_statements("analyst_alice", "trade_research")
    policy = ";\n".join(statements)

    assert "GRANT USAGE ON SCHEMA analytics" in policy
    assert "GRANT SELECT ON ALL TABLES IN SCHEMA analytics" in policy
    assert "default_transaction_read_only = on" in policy
    assert "CONNECTION LIMIT 2" in policy
    assert "statement_timeout = '5min'" in policy
    assert "idle_in_transaction_session_timeout = '1min'" in policy
    assert "lock_timeout = '5s'" in policy
    assert "REVOKE CREATE ON SCHEMA public" in policy
    assert "GRANT SELECT ON ALL TABLES IN SCHEMA public" not in policy
    with pytest.raises(ValueError, match="Role names"):
        analyst_role_statements("analyst; DROP ROLE trade", "trade_research")


def test_source_metrics_use_sql_null_literals_for_entities_without_dates() -> None:
    store = TimescaleStore("sqlite://")
    metadata.create_all(store.engine)

    metrics = _source_metrics(
        store,
        ENTITY_SPECS["symbols"],
        exchange=None,
        start_date=date(2026, 7, 13),
        end_date=date(2026, 7, 20),
    )

    assert metrics.row_count == 0
    assert metrics.watermark is None
    assert metrics.minimum_date is None
    assert metrics.maximum_date is None


def test_bigquery_settings_are_disabled_and_validate_activation() -> None:
    assert Settings(_env_file=None).bigquery_enabled is False
    with pytest.raises(ValidationError, match="BIGQUERY_PROJECT_ID"):
        Settings(_env_file=None, bigquery_enabled=True)
    with pytest.raises(ValidationError, match="BIGQUERY_CREDENTIALS_PATH"):
        Settings(
            _env_file=None,
            bigquery_enabled=True,
            bigquery_project_id="analytics-project",
            bigquery_auth_method="service_account_file",
        )


def test_ohlcv_bigquery_contract_uses_partition_cluster_and_natural_key() -> None:
    spec = ENTITY_SPECS["ohlcv_daily"]
    assert spec.natural_keys == ("instrument_key", "source", "date")
    assert spec.partition_field == "date"
    assert spec.require_partition_filter is True
    assert spec.cluster_fields == ("exchange", "instrument_key", "source")


class FakeBigQueryGateway:
    def __init__(self) -> None:
        self.rows: dict[str, dict[tuple[Any, ...], dict[str, Any]]] = {}
        self.merge_calls = 0
        self.ensured: list[str] = []
        self.fail_next_merge = False

    def ensure_foundation(self, specs) -> None:
        self.ensured = [spec.name for spec in specs]

    def merge_rows(self, spec, rows, *, load_id: str) -> MergeResult:
        del load_id
        self.merge_calls += 1
        if self.fail_next_merge:
            self.fail_next_merge = False
            raise RuntimeError("transient fake load failure")
        table = self.rows.setdefault(spec.name, {})
        inserted = updated = 0
        for source in rows:
            row = dict(source)
            key = tuple(row[name] for name in spec.natural_keys)
            if key in table:
                updated += 1
            else:
                inserted += 1
            table[key] = row
        return MergeResult(
            job_id=f"fake-job-{self.merge_calls}",
            inserted_rows=inserted,
            updated_rows=updated,
            staging_row_count=len(rows),
            merged_row_count=len(rows),
        )

    def reconcile(
        self,
        spec,
        *,
        exchange: str | None,
        start_date: date | None,
        end_date: date | None,
    ) -> ReconciliationResult:
        rows = list(self.rows.get(spec.name, {}).values())
        if exchange:
            rows = [row for row in rows if row[spec.exchange_field] == exchange]
        if spec.date_field and start_date and end_date:
            rows = [
                row
                for row in rows
                if start_date <= row[spec.date_field] <= end_date
            ]
        watermark = (
            str(max(row[spec.watermark_field] for row in rows))
            if rows and spec.watermark_field
            else None
        )
        minimum_date = (
            min(row[spec.date_field] for row in rows)
            if rows and spec.date_field
            else None
        )
        maximum_date = (
            max(row[spec.date_field] for row in rows)
            if rows and spec.date_field
            else None
        )
        return ReconciliationResult(
            row_count=len(rows),
            watermark=watermark,
            minimum_date=minimum_date,
            maximum_date=maximum_date,
        )


def test_bigquery_sync_is_bounded_reconciled_and_resumable(tmp_path: Path) -> None:
    database_url = f"sqlite:///{tmp_path / 'sync.sqlite'}"
    store = TimescaleStore(database_url)
    metadata.create_all(store.engine)
    at = datetime(2026, 7, 19, tzinfo=UTC)
    rows = [
        {
            "instrument_key": f"YF|TEST{i}",
            "source": "yfinance",
            "date": date(2025, 1, 2),
            "symbol": f"TEST{i}",
            "exchange": "US",
            "open": 10.0,
            "high": 11.0,
            "low": 9.0,
            "close": 10.5,
            "volume": 1000,
            "open_interest": None,
            "fetched_at": at,
            "quality_status": "valid",
        }
        for i in range(101)
    ]
    with store.engine.begin() as connection:
        connection.execute(ohlcv_daily_table.insert(), rows)
    settings = Settings(
        _env_file=None,
        database_url=database_url,
        bigquery_enabled=True,
        bigquery_production_sync_enabled=True,
        bigquery_project_id="analytics-project",
        bigquery_backfill_chunk_size=100,
    )
    gateway = FakeBigQueryGateway()

    result = run_bigquery_sync(
        settings=settings,
        store=store,
        gateway=gateway,
        exchange="US",
        year=2025,
        entities=["ohlcv_daily"],
        run_id="dagster-run-1",
    )

    assert result.status == "completed"
    assert result.source_row_count == result.destination_row_count == 101
    assert result.count_difference == 0
    assert result.inserted_rows == 101
    assert gateway.merge_calls == 2
    assert store.bigquery_sync_runs(limit=1)[0]["bigquery_job_id"] == "fake-job-2"
    assert store.bigquery_sync_partitions(run_id="dagster-run-1")[0]["status"] == "completed"

    resumed = run_bigquery_sync(
        settings=settings,
        store=store,
        gateway=gateway,
        exchange="US",
        year=2025,
        entities=["ohlcv_daily"],
        run_id="dagster-run-1",
    )
    assert resumed.status == "completed"
    assert gateway.merge_calls == 2


def test_bigquery_sync_resume_preserves_partition_attempt_count(tmp_path: Path) -> None:
    database_url = f"sqlite:///{tmp_path / 'resume.sqlite'}"
    store = TimescaleStore(database_url)
    metadata.create_all(store.engine)
    with store.engine.begin() as connection:
        connection.execute(
            ohlcv_daily_table.insert(),
            {
                "instrument_key": "YF|AAPL",
                "source": "yfinance",
                "date": date(2025, 1, 2),
                "symbol": "AAPL",
                "exchange": "US",
                "open": 10.0,
                "high": 11.0,
                "low": 9.0,
                "close": 10.5,
                "volume": 1000,
                "open_interest": None,
                "fetched_at": datetime(2026, 7, 19, tzinfo=UTC),
                "quality_status": "valid",
            },
        )
    settings = Settings(
        _env_file=None,
        database_url=database_url,
        bigquery_enabled=True,
        bigquery_production_sync_enabled=True,
        bigquery_project_id="analytics-project",
        bigquery_retry_attempts=1,
    )
    gateway = FakeBigQueryGateway()
    gateway.fail_next_merge = True

    failed = run_bigquery_sync(
        settings=settings,
        store=store,
        gateway=gateway,
        exchange="US",
        year=2025,
        entities=["ohlcv_daily"],
        run_id="dagster-resume",
    )
    assert failed.status == "failed"
    assert store.bigquery_sync_partitions(run_id="dagster-resume")[0]["attempt_count"] == 1

    completed = run_bigquery_sync(
        settings=settings,
        store=store,
        gateway=gateway,
        exchange="US",
        year=2025,
        entities=["ohlcv_daily"],
        run_id="dagster-resume",
    )
    assert completed.status == "completed"
    assert store.bigquery_sync_partitions(run_id="dagster-resume")[0]["attempt_count"] == 2
