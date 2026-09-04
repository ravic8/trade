from __future__ import annotations

from pathlib import Path

import pytest
from alembic import command
from alembic.config import Config
from pydantic import ValidationError
from sqlalchemy import Column, DateTime, MetaData, String, Table, create_engine, inspect

from trade_research.config import Settings
from trade_research.exchanges import (
    CANONICAL_EQUITY_EXCHANGES,
    canonical_equity_exchange,
    is_legacy_equity_exchange_alias,
)
from trade_research.storage.timescale import metadata

FOUNDATION_TABLES = {
    "adaptive_rate_state",
    "daily_coverage_summary",
    "exchange_sessions",
    "instrument_aliases",
    "pipeline_work_items",
    "symbol_lifecycle_events",
    "universe_snapshot_members",
    "universe_snapshots",
}

SYMBOL_LIFECYCLE_COLUMNS = {
    "canonical_instrument_id",
    "first_seen_at",
    "last_seen_at",
    "inactive_at",
    "inactive_reason",
    "consecutive_missing_refreshes",
    "last_universe_snapshot_id",
    "source_identity",
    "provider_instrument_key",
    "listing_status",
    "listing_status_reason",
    "listing_status_effective_at",
    "pipeline_eligibility",
    "provider_status",
    "provider_status_reason",
    "provider_status_updated_at",
}


def test_phase1_tables_and_symbol_columns_are_in_sqlalchemy_metadata() -> None:
    assert FOUNDATION_TABLES.issubset(metadata.tables)
    assert SYMBOL_LIFECYCLE_COLUMNS.issubset(metadata.tables["symbols"].c.keys())
    assert {column.name for column in metadata.tables["pipeline_work_items"].primary_key} == {
        "work_item_id"
    }
    assert any(
        constraint.name == "uq_pipeline_work_items_idempotency_key"
        for constraint in metadata.tables["pipeline_work_items"].constraints
    )


def test_alembic_upgrade_bootstraps_an_empty_database(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    database_path = tmp_path / "empty.sqlite"
    database_url = f"sqlite:///{database_path}"
    engine = create_engine(database_url)

    monkeypatch.setenv("DATABASE_URL", database_url)
    config = Config("alembic.ini")
    command.upgrade(config, "head")

    upgraded = inspect(engine)
    assert {
        "ohlcv_daily",
        "pipeline_work_items",
        "opportunity_targets_daily",
        "workflow_requests",
    }.issubset(set(upgraded.get_table_names()))
    with engine.connect() as connection:
        revision = connection.exec_driver_sql(
            "SELECT version_num FROM alembic_version"
        ).scalar_one()
    assert revision == "20260904_0013"


def test_phase1_feature_flags_are_safe_by_default() -> None:
    settings = Settings(_env_file=None)

    assert settings.yfinance_daily_enabled is False
    assert settings.yfinance_adaptive_rate_mode == "fixed"
    assert settings.yfinance_initial_rpm == 300
    assert settings.yfinance_minimum_rpm == 30
    assert settings.yfinance_maximum_rpm == 600
    assert settings.yfinance_initial_concurrency == 4
    assert settings.yfinance_maximum_concurrency == 8
    assert settings.yfinance_full_us_enabled is False
    assert settings.yfinance_full_tsx_enabled is False
    assert settings.yfinance_nse_enabled is False
    assert settings.legacy_upstox_nse_enabled is True
    assert settings.forex_pipelines_enabled is False


@pytest.mark.parametrize(
    ("overrides", "message"),
    [
        (
            {"yfinance_minimum_rpm": 400, "yfinance_initial_rpm": 300},
            "minimum <= initial <= maximum",
        ),
        (
            {"yfinance_initial_concurrency": 9, "yfinance_maximum_concurrency": 8},
            "initial <= maximum",
        ),
    ],
)
def test_phase1_rate_and_concurrency_bounds_are_validated(
    overrides: dict[str, int],
    message: str,
) -> None:
    with pytest.raises(ValidationError, match=message):
        Settings(_env_file=None, **overrides)


@pytest.mark.parametrize(
    ("value", "expected"),
    [
        ("NSE", "NSE"),
        ("tsx", "TSX"),
        ("US", "US"),
        ("CA", "TSX"),
        ("Canada", "TSX"),
        ("USA", "US"),
        ("NASDAQ", "US"),
        ("NYSE", "US"),
    ],
)
def test_exchange_aliases_normalize_to_canonical_codes(value: str, expected: str) -> None:
    assert canonical_equity_exchange(value) == expected
    assert expected in CANONICAL_EQUITY_EXCHANGES


def test_exchange_alias_helper_distinguishes_canonical_codes() -> None:
    assert is_legacy_equity_exchange_alias("CA") is True
    assert is_legacy_equity_exchange_alias("TSX") is False
    with pytest.raises(ValueError, match="Unsupported equity exchange"):
        canonical_equity_exchange("FOREX")


def test_transition_migration_upgrades_and_downgrades_legacy_schema(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    database_path = tmp_path / "phase1.sqlite"
    database_url = f"sqlite:///{database_path}"
    engine = create_engine(database_url)
    legacy_metadata = MetaData()
    Table(
        "symbols",
        legacy_metadata,
        Column("symbol", String, primary_key=True),
        Column("exchange", String, primary_key=True),
        Column("fetched_at", DateTime(timezone=True), nullable=False),
    )
    legacy_metadata.create_all(engine)

    monkeypatch.setenv("DATABASE_URL", database_url)
    config = Config("alembic.ini")
    command.upgrade(config, "head")

    upgraded = inspect(engine)
    assert FOUNDATION_TABLES.issubset(set(upgraded.get_table_names()))
    assert SYMBOL_LIFECYCLE_COLUMNS.issubset(
        {column["name"] for column in upgraded.get_columns("symbols")}
    )
    assert command.current(config) is None

    command.downgrade(config, "base")
    downgraded = inspect(engine)
    assert FOUNDATION_TABLES.isdisjoint(set(downgraded.get_table_names()))
    assert SYMBOL_LIFECYCLE_COLUMNS.isdisjoint(
        {column["name"] for column in downgraded.get_columns("symbols")}
    )


def test_upgrade_reconciles_create_all_tables_with_legacy_symbols(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    database_path = tmp_path / "phase1-create-all.sqlite"
    database_url = f"sqlite:///{database_path}"
    engine = create_engine(database_url)
    legacy_metadata = MetaData()
    Table(
        "symbols",
        legacy_metadata,
        Column("symbol", String, primary_key=True),
        Column("exchange", String, primary_key=True),
        Column("fetched_at", DateTime(timezone=True), nullable=False),
    )
    legacy_metadata.create_all(engine)
    for table_name in FOUNDATION_TABLES:
        metadata.tables[table_name].create(engine)

    monkeypatch.setenv("DATABASE_URL", database_url)
    config = Config("alembic.ini")
    command.upgrade(config, "head")

    upgraded = inspect(engine)
    assert SYMBOL_LIFECYCLE_COLUMNS.issubset(
        {column["name"] for column in upgraded.get_columns("symbols")}
    )
    with engine.connect() as connection:
        revision = connection.exec_driver_sql(
            "SELECT version_num FROM alembic_version"
        ).scalar_one()
    assert revision == "20260904_0013"
