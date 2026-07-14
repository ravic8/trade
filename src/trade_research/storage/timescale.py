from __future__ import annotations

from collections.abc import Iterable, Mapping
from datetime import UTC, date, datetime, timedelta
from typing import Any
from uuid import UUID, uuid4

import pandas as pd
from sqlalchemy import (
    JSON,
    BigInteger,
    Boolean,
    Column,
    Date,
    DateTime,
    Float,
    MetaData,
    String,
    Table,
    Text,
    case,
    create_engine,
    func,
    select,
    text,
)
from sqlalchemy.dialects.postgresql import insert

from trade_research.features.daily_technical import FEATURE_COLUMNS_V1_0
from trade_research.schemas import Symbol
from trade_research.targets.daily_forward import DAILY_FORWARD_TARGET_COLUMNS_V1_0

metadata = MetaData()

symbols_table = Table(
    "symbols",
    metadata,
    Column("symbol", String, primary_key=True),
    Column("exchange", String, primary_key=True),
    Column("yahoo_symbol", String),
    Column("name", String),
    Column("currency", String),
    Column("source", String, nullable=False),
    Column("source_url", String),
    Column("is_active", Boolean, nullable=False, default=True),
    Column("fetched_at", DateTime(timezone=True), nullable=False),
)

ohlcv_hourly_table = Table(
    "ohlcv_hourly",
    metadata,
    Column("ticker", String, primary_key=True),
    Column("ts", DateTime(timezone=True), primary_key=True),
    Column("source", String, primary_key=True),
    Column("exchange", String, nullable=False),
    Column("open", Float, nullable=False),
    Column("high", Float, nullable=False),
    Column("low", Float, nullable=False),
    Column("close", Float, nullable=False),
    Column("volume", BigInteger, nullable=False),
    Column("fetched_at", DateTime(timezone=True), nullable=False),
    Column("quality_status", String, nullable=False),
)

ingestion_runs_table = Table(
    "ingestion_runs",
    metadata,
    Column("run_id", String, primary_key=True),
    Column("job_name", String, nullable=False),
    Column("status", String, nullable=False),
    Column("exchange", String, nullable=False),
    Column("source", String, nullable=False),
    Column("started_at", DateTime(timezone=True), nullable=False),
    Column("finished_at", DateTime(timezone=True)),
    Column("items_requested", BigInteger, nullable=False, default=0),
    Column("items_processed", BigInteger, nullable=False, default=0),
    Column("items_succeeded", BigInteger, nullable=False, default=0),
    Column("items_failed", BigInteger, nullable=False, default=0),
    Column("error_message", String),
    Column("run_metadata", JSON, nullable=False, default=dict),
)

provider_request_log_table = Table(
    "provider_request_log",
    metadata,
    Column("id", String, primary_key=True),
    Column("run_id", String),
    Column("provider", String, nullable=False),
    Column("endpoint_group", String, nullable=False),
    Column("request_key", String, nullable=False),
    Column("instrument_key", String),
    Column("symbol", String),
    Column("interval", String),
    Column("window_start", Date),
    Column("window_end", Date),
    Column("status_code", BigInteger),
    Column("status", String, nullable=False),
    Column("error_message", Text),
    Column("retry_count", BigInteger, nullable=False, default=0),
    Column("rate_limited", Boolean, nullable=False, default=False),
    Column("wait_seconds", Float, nullable=False, default=0.0),
    Column("duration_ms", Float, nullable=False, default=0.0),
    Column("created_at", DateTime(timezone=True), nullable=False),
)

exchange_holidays_table = Table(
    "exchange_holidays",
    metadata,
    Column("exchange", String, primary_key=True),
    Column("year", BigInteger, primary_key=True),
    Column("source_url", String, nullable=False),
    Column("closed_dates", JSON, nullable=False, default=list),
    Column("early_close_dates", JSON, nullable=False, default=list),
    Column("fetched_at", DateTime(timezone=True), nullable=False),
)

feed_health_table = Table(
    "feed_health",
    metadata,
    Column("symbol", String, primary_key=True),
    Column("exchange", String, primary_key=True),
    Column("source", String, primary_key=True),
    Column("yahoo_symbol", String),
    Column("status", String, nullable=False),
    Column("last_success_at", DateTime(timezone=True)),
    Column("last_failure_at", DateTime(timezone=True)),
    Column("consecutive_failures", BigInteger, nullable=False, default=0),
    Column("success_count", BigInteger, nullable=False, default=0),
    Column("failure_count", BigInteger, nullable=False, default=0),
    Column("last_error_code", String),
    Column("last_error_message", String),
    Column("latest_candle_ts", DateTime(timezone=True)),
    Column("next_retry_at", DateTime(timezone=True)),
    Column("updated_at", DateTime(timezone=True), nullable=False),
)

hourly_backlog_windows_table = Table(
    "hourly_backlog_windows",
    metadata,
    Column("exchange", String, primary_key=True),
    Column("window_start", DateTime(timezone=True), primary_key=True),
    Column("source", String, primary_key=True),
    Column("window_end", DateTime(timezone=True), nullable=False),
    Column("status", String, nullable=False),
    Column("expected_symbol_count", BigInteger, nullable=False),
    Column("observed_symbol_count", BigInteger, nullable=False),
    Column("coverage_ratio", Float, nullable=False),
    Column("attempt_count", BigInteger, nullable=False, default=0),
    Column("first_detected_at", DateTime(timezone=True), nullable=False),
    Column("last_checked_at", DateTime(timezone=True), nullable=False),
    Column("last_recovery_at", DateTime(timezone=True)),
    Column("recovery_run_id", String),
    Column("last_error", String),
)

provider_instruments_table = Table(
    "provider_instruments",
    metadata,
    Column("source", String, primary_key=True),
    Column("instrument_key", String, primary_key=True),
    Column("exchange", String),
    Column("segment", String),
    Column("asset_type", String),
    Column("trading_symbol", String),
    Column("name", String),
    Column("isin", String),
    Column("lot_size", BigInteger),
    Column("tick_size", Float),
    Column("expiry", Date),
    Column("strike", Float),
    Column("option_type", String),
    Column("underlying_symbol", String),
    Column("underlying_key", String),
    Column("exchange_token", String),
    Column("active", Boolean, nullable=False, default=True),
    Column("fetched_at", DateTime(timezone=True), nullable=False),
    Column("raw", JSON, nullable=False, default=dict),
)

provider_credentials_table = Table(
    "provider_credentials",
    metadata,
    Column("provider", String, primary_key=True),
    Column("credential_type", String, primary_key=True),
    Column("encrypted_value", Text, nullable=False),
    Column("updated_at", DateTime(timezone=True), nullable=False),
    Column("updated_by", String),
    Column("last_validated_at", DateTime(timezone=True)),
    Column("validation_status", String),
    Column("validation_message", String),
)

tradable_universes_table = Table(
    "tradable_universes",
    metadata,
    Column("universe_id", String, primary_key=True),
    Column("name", String, nullable=False),
    Column("description", String),
    Column("exchange", String, nullable=False),
    Column("source", String, nullable=False),
    Column("criteria_json", JSON, nullable=False, default=dict),
    Column("created_at", DateTime(timezone=True), nullable=False),
)

tradable_universe_members_table = Table(
    "tradable_universe_members",
    metadata,
    Column("universe_id", String, primary_key=True),
    Column("symbol", String, primary_key=True),
    Column("instrument_key", String),
    Column("rank", BigInteger),
    Column("avg_daily_volume", Float),
    Column("avg_daily_turnover", Float),
    Column("trading_days", BigInteger),
    Column("zero_volume_ratio", Float),
    Column("start_date", Date),
    Column("end_date", Date),
    Column("included_at", DateTime(timezone=True), nullable=False),
)

ohlcv_daily_table = Table(
    "ohlcv_daily",
    metadata,
    Column("instrument_key", String, primary_key=True),
    Column("source", String, primary_key=True),
    Column("date", Date, primary_key=True),
    Column("symbol", String, nullable=False),
    Column("exchange", String, nullable=False),
    Column("open", Float, nullable=False),
    Column("high", Float, nullable=False),
    Column("low", Float, nullable=False),
    Column("close", Float, nullable=False),
    Column("volume", BigInteger, nullable=False),
    Column("open_interest", BigInteger),
    Column("fetched_at", DateTime(timezone=True), nullable=False),
    Column("quality_status", String, nullable=False),
)

data_quality_audits_table = Table(
    "data_quality_audits",
    metadata,
    Column("audit_id", String, primary_key=True),
    Column("dataset_name", String, nullable=False),
    Column("source", String, nullable=False),
    Column("symbol", String),
    Column("instrument_key", String),
    Column("interval", String),
    Column("start_date", Date),
    Column("end_date", Date),
    Column("rows", BigInteger, nullable=False),
    Column("missing_dates", BigInteger, nullable=False, default=0),
    Column("null_rows", BigInteger, nullable=False, default=0),
    Column("duplicate_rows", BigInteger, nullable=False, default=0),
    Column("zero_volume_rows", BigInteger, nullable=False, default=0),
    Column("zero_or_negative_close_rows", BigInteger, nullable=False, default=0),
    Column("coverage_ratio", Float),
    Column("status", String, nullable=False),
    Column("warnings_json", JSON, nullable=False, default=list),
    Column("created_at", DateTime(timezone=True), nullable=False),
)

features_daily_table = Table(
    "features_daily",
    metadata,
    Column("instrument_key", String, primary_key=True),
    Column("date", Date, primary_key=True),
    Column("feature_version", String, primary_key=True),
    Column("source", String, nullable=False),
    Column("symbol", String, nullable=False),
    Column("exchange", String, nullable=False),
    Column("open", Float),
    Column("high", Float),
    Column("low", Float),
    Column("close", Float),
    Column("volume", BigInteger),
    Column("open_interest", BigInteger),
    Column("computed_at", DateTime(timezone=True), nullable=False),
    Column("quality_status", String, nullable=False),
    *(Column(column, Float) for column in FEATURE_COLUMNS_V1_0),
)

feature_runs_table = Table(
    "feature_runs",
    metadata,
    Column("run_id", String, primary_key=True),
    Column("dataset_name", String, nullable=False),
    Column("feature_version", String, nullable=False),
    Column("source", String, nullable=False),
    Column("status", String, nullable=False),
    Column("started_at", DateTime(timezone=True), nullable=False),
    Column("finished_at", DateTime(timezone=True), nullable=False),
    Column("rows", BigInteger, nullable=False),
    Column("symbols", BigInteger, nullable=False),
    Column("date_min", Date),
    Column("date_max", Date),
    Column("invalid_ohlcv_count", BigInteger, nullable=False, default=0),
    Column("summary_json", JSON, nullable=False, default=dict),
)

feature_audits_table = Table(
    "feature_audits",
    metadata,
    Column("audit_id", String, primary_key=True),
    Column("run_id", String),
    Column("dataset_name", String, nullable=False),
    Column("feature_version", String, nullable=False),
    Column("feature", String, nullable=False),
    Column("null_count", BigInteger, nullable=False),
    Column("null_pct", Float, nullable=False),
    Column("inf_count", BigInteger, nullable=False),
    Column("created_at", DateTime(timezone=True), nullable=False),
)

targets_daily_table = Table(
    "targets_daily",
    metadata,
    Column("instrument_key", String, primary_key=True),
    Column("date", Date, primary_key=True),
    Column("target_version", String, primary_key=True),
    Column("source", String, nullable=False),
    Column("symbol", String, nullable=False),
    Column("exchange", String, nullable=False),
    Column("computed_at", DateTime(timezone=True), nullable=False),
    Column("quality_status", String, nullable=False),
    Column("forward_ret_1d", Float),
    Column("forward_ret_5d", Float),
    Column("forward_ret_10d", Float),
    Column("forward_ret_20d", Float),
    Column("forward_ret_60d", Float),
    Column("forward_outperform_universe_20d", Float),
    Column("top_quantile_forward_return_20d", Boolean),
)

target_runs_table = Table(
    "target_runs",
    metadata,
    Column("run_id", String, primary_key=True),
    Column("dataset_name", String, nullable=False),
    Column("target_version", String, nullable=False),
    Column("source", String, nullable=False),
    Column("status", String, nullable=False),
    Column("started_at", DateTime(timezone=True), nullable=False),
    Column("finished_at", DateTime(timezone=True), nullable=False),
    Column("rows", BigInteger, nullable=False),
    Column("symbols", BigInteger, nullable=False),
    Column("date_min", Date),
    Column("date_max", Date),
    Column("invalid_ohlcv_count", BigInteger, nullable=False, default=0),
    Column("summary_json", JSON, nullable=False, default=dict),
)

target_audits_table = Table(
    "target_audits",
    metadata,
    Column("audit_id", String, primary_key=True),
    Column("run_id", String),
    Column("dataset_name", String, nullable=False),
    Column("target_version", String, nullable=False),
    Column("target", String, nullable=False),
    Column("null_count", BigInteger, nullable=False),
    Column("null_pct", Float, nullable=False),
    Column("inf_count", BigInteger, nullable=False),
    Column("created_at", DateTime(timezone=True), nullable=False),
)

stock_coverage_runs_table = Table(
    "stock_coverage_runs",
    metadata,
    Column("run_id", String, primary_key=True),
    Column("dagster_run_id", String),
    Column("source", String, nullable=False),
    Column("exchange", String, nullable=False),
    Column("as_of_date", Date, nullable=False),
    Column("generated_at", DateTime(timezone=True), nullable=False),
    Column("status", String, nullable=False),
    Column("summary_json", JSON, nullable=False, default=dict),
)

stock_coverage_by_window_table = Table(
    "stock_coverage_by_window",
    metadata,
    Column("run_id", String, primary_key=True),
    Column("window_months", BigInteger, primary_key=True),
    Column("instrument_key", String, primary_key=True),
    Column("symbol", String, nullable=False),
    Column("exchange", String, nullable=False),
    Column("source", String, nullable=False),
    Column("window_start", Date, nullable=False),
    Column("window_end", Date, nullable=False),
    Column("first_date", Date),
    Column("last_date", Date),
    Column("expected_date_count", BigInteger, nullable=False),
    Column("observed_date_count", BigInteger, nullable=False),
    Column("missing_date_count", BigInteger, nullable=False),
    Column("coverage_pct", Float, nullable=False),
    Column("has_latest_expected_date", Boolean, nullable=False),
    Column("latest_date_lag_days", BigInteger),
    Column("coverage_status", String, nullable=False),
    Column("created_at", DateTime(timezone=True), nullable=False),
)

daily_ohlcv_fetch_coverage_table = Table(
    "daily_ohlcv_fetch_coverage",
    metadata,
    Column("run_id", String, primary_key=True),
    Column("instrument_key", String, primary_key=True),
    Column("symbol", String, nullable=False),
    Column("source", String, nullable=False),
    Column("exchange", String, nullable=False),
    Column("latest_stored_date", Date),
    Column("fetch_start", Date),
    Column("fetch_end", Date, nullable=False),
    Column("should_fetch", Boolean, nullable=False),
    Column("status", String, nullable=False),
    Column("rows_fetched", BigInteger, nullable=False, default=0),
    Column("skip_reason", String),
    Column("error_message", String),
    Column("created_at", DateTime(timezone=True), nullable=False),
)


class TimescaleStore:
    def __init__(self, database_url: str) -> None:
        self.engine = create_engine(database_url, pool_pre_ping=True)

    def initialize(self) -> None:
        metadata.create_all(self.engine)
        with self.engine.begin() as connection:
            connection.execute(
                text(
                    "CREATE INDEX IF NOT EXISTS idx_ohlcv_hourly_exchange_ts "
                    "ON ohlcv_hourly (exchange, ts DESC)"
                )
            )
            connection.execute(
                text(
                    "CREATE INDEX IF NOT EXISTS idx_ingestion_runs_started_at "
                    "ON ingestion_runs (started_at DESC)"
                )
            )
            connection.execute(
                text(
                    "CREATE INDEX IF NOT EXISTS idx_provider_request_log_run "
                    "ON provider_request_log (run_id, created_at DESC)"
                )
            )
            connection.execute(
                text(
                    "CREATE INDEX IF NOT EXISTS idx_provider_request_log_provider "
                    "ON provider_request_log (provider, endpoint_group, created_at DESC)"
                )
            )
            connection.execute(
                text(
                    "CREATE INDEX IF NOT EXISTS idx_feed_health_exchange_status "
                    "ON feed_health (exchange, status, next_retry_at)"
                )
            )
            connection.execute(
                text(
                    "CREATE INDEX IF NOT EXISTS idx_hourly_backlog_exchange_status "
                    "ON hourly_backlog_windows (exchange, status, window_start)"
                )
            )
            connection.execute(
                text(
                    "CREATE INDEX IF NOT EXISTS idx_provider_instruments_symbol "
                    "ON provider_instruments (source, exchange, segment, trading_symbol)"
                )
            )
            connection.execute(
                text(
                    "CREATE INDEX IF NOT EXISTS idx_provider_credentials_updated "
                    "ON provider_credentials (provider, updated_at DESC)"
                )
            )
            connection.execute(
                text(
                    "CREATE INDEX IF NOT EXISTS idx_ohlcv_daily_symbol_date "
                    "ON ohlcv_daily (symbol, date DESC)"
                )
            )
            connection.execute(
                text(
                    "CREATE INDEX IF NOT EXISTS idx_data_quality_dataset "
                    "ON data_quality_audits (dataset_name, source, created_at DESC)"
                )
            )
            connection.execute(
                text(
                    "CREATE INDEX IF NOT EXISTS idx_features_daily_symbol_date "
                    "ON features_daily (symbol, date DESC)"
                )
            )
            connection.execute(
                text(
                    "CREATE INDEX IF NOT EXISTS idx_features_daily_version_date "
                    "ON features_daily (feature_version, date DESC)"
                )
            )
            connection.execute(
                text(
                    "CREATE INDEX IF NOT EXISTS idx_feature_runs_version "
                    "ON feature_runs (feature_version, finished_at DESC)"
                )
            )
            connection.execute(
                text(
                    "CREATE INDEX IF NOT EXISTS idx_feature_audits_run "
                    "ON feature_audits (run_id, feature)"
                )
            )
            connection.execute(
                text(
                    "CREATE INDEX IF NOT EXISTS idx_targets_daily_symbol_date "
                    "ON targets_daily (symbol, date DESC)"
                )
            )
            connection.execute(
                text(
                    "CREATE INDEX IF NOT EXISTS idx_targets_daily_version_date "
                    "ON targets_daily (target_version, date DESC)"
                )
            )
            connection.execute(
                text(
                    "CREATE INDEX IF NOT EXISTS idx_target_runs_version "
                    "ON target_runs (target_version, finished_at DESC)"
                )
            )
            connection.execute(
                text(
                    "CREATE INDEX IF NOT EXISTS idx_target_audits_run "
                    "ON target_audits (run_id, target)"
                )
            )
            connection.execute(
                text(
                    "CREATE INDEX IF NOT EXISTS idx_stock_coverage_window_status "
                    "ON stock_coverage_by_window (window_months, coverage_status)"
                )
            )
            connection.execute(
                text(
                    "CREATE INDEX IF NOT EXISTS idx_stock_coverage_symbol_window "
                    "ON stock_coverage_by_window (symbol, window_months)"
                )
            )
            connection.execute(
                text(
                    "CREATE INDEX IF NOT EXISTS idx_daily_ohlcv_fetch_coverage_status "
                    "ON daily_ohlcv_fetch_coverage (run_id, status)"
                )
            )

        with self.engine.begin() as connection:
            connection.execute(text("CREATE EXTENSION IF NOT EXISTS timescaledb"))
            connection.execute(
                text(
                    "SELECT create_hypertable("
                    "'ohlcv_hourly', 'ts', if_not_exists => TRUE, migrate_data => TRUE)"
                )
            )
            connection.execute(
                text(
                    "SELECT create_hypertable("
                    "'ohlcv_daily', 'date', if_not_exists => TRUE, migrate_data => TRUE)"
                )
            )
            connection.execute(
                text(
                    "SELECT create_hypertable("
                    "'features_daily', 'date', if_not_exists => TRUE, migrate_data => TRUE)"
                )
            )
            connection.execute(
                text(
                    "SELECT create_hypertable("
                    "'targets_daily', 'date', if_not_exists => TRUE, migrate_data => TRUE)"
                )
            )

    def insert_stock_coverage_run(
        self,
        run_id: str,
        coverage: pd.DataFrame,
        summary: Mapping[str, Any],
        as_of_date: date,
        dagster_run_id: str | None = None,
        source: str = "upstox",
        exchange: str = "NSE",
        status: str = "completed",
    ) -> int:
        generated_at = datetime.now(UTC)
        run_row = {
            "run_id": run_id,
            "dagster_run_id": dagster_run_id,
            "source": source,
            "exchange": exchange.upper(),
            "as_of_date": as_of_date,
            "generated_at": generated_at,
            "status": status,
            "summary_json": dict(summary),
        }
        rows = self._stock_coverage_rows(
            coverage,
            run_id=run_id,
            source=source,
            exchange=exchange,
            created_at=generated_at,
        )
        with self.engine.begin() as connection:
            run_statement = insert(stock_coverage_runs_table).values(run_row)
            run_update_columns = {
                column.name: getattr(run_statement.excluded, column.name)
                for column in stock_coverage_runs_table.columns
                if column.name != "run_id"
            }
            connection.execute(
                run_statement.on_conflict_do_update(
                    index_elements=["run_id"],
                    set_=run_update_columns,
                )
            )
            connection.execute(
                stock_coverage_by_window_table.delete().where(
                    stock_coverage_by_window_table.c.run_id == run_id
                )
            )
            for chunk in _chunks(rows, size=1_000):
                connection.execute(stock_coverage_by_window_table.insert().values(chunk))
        return len(rows)

    def insert_daily_ohlcv_fetch_coverage(
        self,
        run_id: str,
        coverage: pd.DataFrame,
        source: str = "upstox",
        exchange: str = "NSE",
    ) -> int:
        rows = self._daily_ohlcv_fetch_coverage_rows(
            coverage,
            run_id=run_id,
            source=source,
            exchange=exchange,
            created_at=datetime.now(UTC),
        )
        if not rows:
            return 0
        with self.engine.begin() as connection:
            connection.execute(
                daily_ohlcv_fetch_coverage_table.delete().where(
                    daily_ohlcv_fetch_coverage_table.c.run_id == run_id
                )
            )
            for chunk in _chunks(rows, size=1_000):
                connection.execute(daily_ohlcv_fetch_coverage_table.insert().values(chunk))
        return len(rows)

    def insert_provider_request_logs(self, logs: Iterable[Mapping[str, Any]]) -> int:
        rows = self._provider_request_log_rows(logs)
        if not rows:
            return 0
        with self.engine.begin() as connection:
            for chunk in _chunks(rows, size=1_000):
                connection.execute(provider_request_log_table.insert().values(chunk))
        return len(rows)

    def daily_ohlcv_fetch_retry_candidates(
        self,
        run_id: str | None = None,
        statuses: tuple[str, ...] = ("failed", "no_rows"),
        source: str = "upstox",
        exchange: str = "NSE",
        limit: int | None = None,
    ) -> pd.DataFrame:
        if not statuses:
            return pd.DataFrame()
        selected_run_id = run_id or self.latest_daily_ohlcv_fetch_coverage_run_id(
            source=source,
            exchange=exchange,
        )
        if not selected_run_id:
            return pd.DataFrame()
        query = (
            daily_ohlcv_fetch_coverage_table.select()
            .where(daily_ohlcv_fetch_coverage_table.c.run_id == selected_run_id)
            .where(daily_ohlcv_fetch_coverage_table.c.source == source)
            .where(daily_ohlcv_fetch_coverage_table.c.exchange == exchange.upper())
            .where(daily_ohlcv_fetch_coverage_table.c.status.in_(list(statuses)))
            .order_by(
                daily_ohlcv_fetch_coverage_table.c.status,
                daily_ohlcv_fetch_coverage_table.c.symbol,
            )
        )
        if limit:
            query = query.limit(limit)
        with self.engine.begin() as connection:
            rows = [dict(row) for row in connection.execute(query).mappings()]
        return pd.DataFrame(rows)

    def latest_daily_ohlcv_fetch_coverage_run_id(
        self,
        source: str = "upstox",
        exchange: str = "NSE",
    ) -> str | None:
        query = (
            select(daily_ohlcv_fetch_coverage_table.c.run_id)
            .where(daily_ohlcv_fetch_coverage_table.c.source == source)
            .where(daily_ohlcv_fetch_coverage_table.c.exchange == exchange.upper())
            .group_by(daily_ohlcv_fetch_coverage_table.c.run_id)
            .order_by(func.max(daily_ohlcv_fetch_coverage_table.c.created_at).desc())
            .limit(1)
        )
        with self.engine.begin() as connection:
            return connection.execute(query).scalar_one_or_none()

    def provider_credential(
        self,
        provider: str,
        credential_type: str = "access_token",
    ) -> dict[str, Any] | None:
        query = select(provider_credentials_table).where(
            provider_credentials_table.c.provider == provider.lower(),
            provider_credentials_table.c.credential_type == credential_type,
        )
        with self.engine.begin() as connection:
            row = connection.execute(query).mappings().first()
        return dict(row) if row else None

    def upsert_provider_credential(
        self,
        provider: str,
        credential_type: str,
        encrypted_value: str,
        updated_by: str,
        validation_status: str,
        validation_message: str | None = None,
        last_validated_at: datetime | None = None,
    ) -> None:
        row = {
            "provider": provider.lower(),
            "credential_type": credential_type,
            "encrypted_value": encrypted_value,
            "updated_at": datetime.now(UTC),
            "updated_by": updated_by,
            "last_validated_at": last_validated_at,
            "validation_status": validation_status,
            "validation_message": validation_message,
        }
        statement = insert(provider_credentials_table).values(row)
        update_columns = {
            column.name: getattr(statement.excluded, column.name)
            for column in provider_credentials_table.columns
            if column.name not in {"provider", "credential_type"}
        }
        with self.engine.begin() as connection:
            connection.execute(
                statement.on_conflict_do_update(
                    index_elements=["provider", "credential_type"],
                    set_=update_columns,
                )
            )

    def upsert_symbols(self, symbols: Iterable[Symbol], fetched_at: datetime | None = None) -> int:
        fetched = fetched_at or datetime.now(UTC)
        rows = [
            {
                **symbol.model_dump(),
                "is_active": True,
                "fetched_at": fetched,
            }
            for symbol in symbols
        ]
        if not rows:
            return 0

        statement = insert(symbols_table).values(rows)
        update_columns = {
            column.name: getattr(statement.excluded, column.name)
            for column in symbols_table.columns
            if column.name not in {"symbol", "exchange"}
        }
        with self.engine.begin() as connection:
            connection.execute(
                statement.on_conflict_do_update(
                    index_elements=["symbol", "exchange"],
                    set_=update_columns,
                )
            )
        return len(rows)

    def active_symbols(self, exchange: str, max_age_days: int | None = None) -> list[Symbol]:
        exchange_code = exchange.upper()
        with self.engine.begin() as connection:
            if max_age_days is not None:
                max_fetched_at = connection.execute(
                    text(
                        """
                        SELECT max(fetched_at)
                        FROM symbols
                        WHERE exchange = :exchange AND is_active = true
                        """
                    ),
                    {"exchange": exchange_code},
                ).scalar_one_or_none()
                if max_fetched_at is None or _as_utc(max_fetched_at) < datetime.now(
                    UTC
                ) - timedelta(days=max_age_days):
                    return []

            rows = connection.execute(
                symbols_table.select()
                .where(symbols_table.c.exchange == exchange_code)
                .where(symbols_table.c.is_active.is_(True))
                .order_by(symbols_table.c.symbol)
            ).mappings()
            return [
                Symbol(
                    symbol=row["symbol"],
                    exchange=row["exchange"],
                    yahoo_symbol=row["yahoo_symbol"],
                    name=row["name"],
                    currency=row["currency"],
                    source=row["source"],
                    source_url=row["source_url"],
                )
                for row in rows
            ]

    def upsert_exchange_holidays(
        self,
        exchange: str,
        year: int,
        closed_dates: Iterable[date],
        early_close_dates: Iterable[date],
        source_url: str,
        fetched_at: datetime | None = None,
    ) -> int:
        row = {
            "exchange": exchange.upper(),
            "year": year,
            "source_url": source_url,
            "closed_dates": sorted(item.isoformat() for item in closed_dates),
            "early_close_dates": sorted(item.isoformat() for item in early_close_dates),
            "fetched_at": fetched_at or datetime.now(UTC),
        }
        statement = insert(exchange_holidays_table).values(row)
        update_columns = {
            column.name: getattr(statement.excluded, column.name)
            for column in exchange_holidays_table.columns
            if column.name not in {"exchange", "year"}
        }
        with self.engine.begin() as connection:
            connection.execute(
                statement.on_conflict_do_update(
                    index_elements=["exchange", "year"],
                    set_=update_columns,
                )
            )
        return 1

    def exchange_holidays(
        self,
        exchange: str,
        year: int,
        max_age_days: int | None = None,
    ) -> dict[str, Any] | None:
        query = (
            exchange_holidays_table.select()
            .where(exchange_holidays_table.c.exchange == exchange.upper())
            .where(exchange_holidays_table.c.year == year)
        )
        with self.engine.begin() as connection:
            row = connection.execute(query).mappings().first()
        if row is None:
            return None
        if max_age_days is not None and _as_utc(row["fetched_at"]) < datetime.now(
            UTC
        ) - timedelta(days=max_age_days):
            return None
        return dict(row)

    def fetchable_symbols(
        self,
        symbols: list[Symbol],
        source: str = "yahoo",
        limit: int | None = None,
    ) -> list[Symbol]:
        candidates = [symbol for symbol in symbols if symbol.yahoo_symbol]
        if not candidates:
            return []

        exchange_code = candidates[0].exchange.upper()
        symbol_ids = [symbol.symbol for symbol in candidates]
        health_by_symbol = self._feed_health_by_symbol(
            exchange=exchange_code,
            source=source,
            symbols=symbol_ids,
        )
        now = datetime.now(UTC)
        fetchable: list[Symbol] = []
        for symbol in candidates:
            health = health_by_symbol.get(symbol.symbol)
            if health and not _feed_health_can_fetch(health, now):
                continue
            fetchable.append(symbol)
            if limit and len(fetchable) >= limit:
                break
        return fetchable

    def fetchable_symbol_count(self, exchange: str, source: str = "yahoo") -> int:
        return len(self.fetchable_symbols(self.active_symbols(exchange), source=source))

    def update_feed_health(
        self,
        symbols: list[Symbol],
        successful_latest_candles: Mapping[str, datetime],
        source: str = "yahoo",
        failure_threshold: int = 5,
        max_backoff_hours: int = 24,
        unsupported_retry_days: int = 7,
        failure_message: str | None = None,
    ) -> dict[str, int]:
        tracked_symbols = [symbol for symbol in symbols if symbol.yahoo_symbol]
        if not tracked_symbols:
            return {"updated": 0, "succeeded": 0, "failed": 0}

        exchange_code = tracked_symbols[0].exchange.upper()
        existing = self._feed_health_by_symbol(
            exchange=exchange_code,
            source=source,
            symbols=[symbol.symbol for symbol in tracked_symbols],
        )
        now = datetime.now(UTC)
        success_tickers = {ticker.upper(): ts for ticker, ts in successful_latest_candles.items()}
        rows: list[dict[str, Any]] = []
        succeeded = 0
        failed = 0
        for symbol in tracked_symbols:
            current = existing.get(symbol.symbol, {})
            yahoo_symbol = str(symbol.yahoo_symbol).upper()
            if yahoo_symbol in success_tickers:
                succeeded += 1
                rows.append(
                    {
                        "symbol": symbol.symbol,
                        "exchange": exchange_code,
                        "source": source,
                        "yahoo_symbol": yahoo_symbol,
                        "status": "active",
                        "last_success_at": now,
                        "last_failure_at": current.get("last_failure_at"),
                        "consecutive_failures": 0,
                        "success_count": int(current.get("success_count") or 0) + 1,
                        "failure_count": int(current.get("failure_count") or 0),
                        "last_error_code": None,
                        "last_error_message": None,
                        "latest_candle_ts": success_tickers[yahoo_symbol],
                        "next_retry_at": None,
                        "updated_at": now,
                    }
                )
                continue

            failed += 1
            consecutive_failures = int(current.get("consecutive_failures") or 0) + 1
            if consecutive_failures >= failure_threshold:
                status = "unsupported"
                next_retry_at = now + timedelta(days=unsupported_retry_days)
            else:
                status = "temporarily_failed"
                backoff_hours = min(2 ** (consecutive_failures - 1), max_backoff_hours)
                next_retry_at = now + timedelta(hours=backoff_hours)
            rows.append(
                {
                    "symbol": symbol.symbol,
                    "exchange": exchange_code,
                    "source": source,
                    "yahoo_symbol": yahoo_symbol,
                    "status": status,
                    "last_success_at": current.get("last_success_at"),
                    "last_failure_at": now,
                    "consecutive_failures": consecutive_failures,
                    "success_count": int(current.get("success_count") or 0),
                    "failure_count": int(current.get("failure_count") or 0) + 1,
                    "last_error_code": "no_hourly_data",
                    "last_error_message": failure_message or "Provider returned no hourly candles",
                    "latest_candle_ts": current.get("latest_candle_ts"),
                    "next_retry_at": next_retry_at,
                    "updated_at": now,
                }
            )

        if rows:
            statement = insert(feed_health_table).values(rows)
            update_columns = {
                column.name: getattr(statement.excluded, column.name)
                for column in feed_health_table.columns
                if column.name not in {"symbol", "exchange", "source"}
            }
            with self.engine.begin() as connection:
                connection.execute(
                    statement.on_conflict_do_update(
                        index_elements=["symbol", "exchange", "source"],
                        set_=update_columns,
                    )
                )
        return {"updated": len(rows), "succeeded": succeeded, "failed": failed}

    def feed_health_summary(self, exchange: str | None = None) -> list[dict[str, Any]]:
        where_clause = "WHERE exchange = :exchange" if exchange else ""
        query = text(
            f"""
            SELECT exchange, source, status, count(*) AS symbols
            FROM feed_health
            {where_clause}
            GROUP BY exchange, source, status
            ORDER BY exchange, source, status
            """
        )
        params = {"exchange": exchange.upper()} if exchange else {}
        with self.engine.begin() as connection:
            return [dict(row) for row in connection.execute(query, params).mappings()]

    def _feed_health_by_symbol(
        self,
        exchange: str,
        source: str,
        symbols: list[str],
    ) -> dict[str, dict[str, Any]]:
        if not symbols:
            return {}
        query = (
            feed_health_table.select()
            .where(feed_health_table.c.exchange == exchange.upper())
            .where(feed_health_table.c.source == source)
            .where(feed_health_table.c.symbol.in_(symbols))
        )
        with self.engine.begin() as connection:
            rows = connection.execute(query).mappings()
            return {row["symbol"]: dict(row) for row in rows}

    def scan_hourly_backlog_windows(
        self,
        exchange: str,
        windows: Iterable[tuple[datetime, datetime]],
        expected_symbol_count: int,
        coverage_threshold: float,
        source: str = "yahoo",
    ) -> list[dict[str, Any]]:
        pairs = [(_as_utc(start), _as_utc(end)) for start, end in windows]
        if not pairs or expected_symbol_count <= 0:
            return []

        counts = self._hourly_symbol_counts(
            exchange=exchange,
            window_starts=[start for start, _ in pairs],
            source=source,
        )
        pending: list[dict[str, Any]] = []
        for window_start, window_end in pairs:
            observed = counts.get(window_start, 0)
            coverage = observed / expected_symbol_count
            row = self.upsert_hourly_backlog_window(
                exchange=exchange,
                source=source,
                window_start=window_start,
                window_end=window_end,
                expected_symbol_count=expected_symbol_count,
                observed_symbol_count=observed,
                coverage_ratio=coverage,
                is_complete=coverage >= coverage_threshold,
            )
            if row["status"] in {"missing", "partial", "failed"}:
                pending.append(row)
        return pending

    def hourly_backlog_candidates(
        self,
        exchange: str,
        limit: int,
        max_attempts: int,
        stale_recovery_minutes: int,
        source: str = "yahoo",
    ) -> list[dict[str, Any]]:
        query = (
            hourly_backlog_windows_table.select()
            .where(hourly_backlog_windows_table.c.exchange == exchange.upper())
            .where(hourly_backlog_windows_table.c.source == source)
            .where(
                hourly_backlog_windows_table.c.status.in_(
                    ["missing", "partial", "failed", "queued", "running"]
                )
            )
            .where(hourly_backlog_windows_table.c.attempt_count < max_attempts)
            .order_by(hourly_backlog_windows_table.c.window_start)
        )
        with self.engine.begin() as connection:
            rows = [dict(row) for row in connection.execute(query).mappings()]

        stale_before = datetime.now(UTC) - timedelta(minutes=stale_recovery_minutes)
        candidates: list[dict[str, Any]] = []
        for row in rows:
            if row["status"] in {"queued", "running"}:
                last_recovery_at = row["last_recovery_at"]
                if last_recovery_at and _as_utc(last_recovery_at) >= stale_before:
                    continue
            candidates.append(row)
            if len(candidates) >= limit:
                break
        return candidates

    def upsert_hourly_backlog_window(
        self,
        exchange: str,
        source: str,
        window_start: datetime,
        window_end: datetime,
        expected_symbol_count: int,
        observed_symbol_count: int,
        coverage_ratio: float,
        is_complete: bool,
    ) -> dict[str, Any]:
        exchange_code = exchange.upper()
        now = datetime.now(UTC)
        key_filter = (
            (hourly_backlog_windows_table.c.exchange == exchange_code)
            & (hourly_backlog_windows_table.c.window_start == _as_utc(window_start))
            & (hourly_backlog_windows_table.c.source == source)
        )
        with self.engine.begin() as connection:
            existing = connection.execute(
                hourly_backlog_windows_table.select().where(key_filter)
            ).mappings().first()
            existing_status = existing["status"] if existing else None
            if is_complete:
                status = "recovered"
            elif existing_status in {"queued", "running"}:
                status = existing_status
            elif existing_status in {"partial", "failed"}:
                status = existing_status
            else:
                status = "missing"

            values = {
                "exchange": exchange_code,
                "window_start": _as_utc(window_start),
                "source": source,
                "window_end": _as_utc(window_end),
                "status": status,
                "expected_symbol_count": expected_symbol_count,
                "observed_symbol_count": observed_symbol_count,
                "coverage_ratio": coverage_ratio,
                "attempt_count": int(existing["attempt_count"]) if existing else 0,
                "first_detected_at": existing["first_detected_at"] if existing else now,
                "last_checked_at": now,
                "last_recovery_at": existing["last_recovery_at"] if existing else None,
                "recovery_run_id": existing["recovery_run_id"] if existing else None,
                "last_error": existing["last_error"] if existing else None,
            }
            statement = insert(hourly_backlog_windows_table).values(values)
            update_columns = {
                column.name: getattr(statement.excluded, column.name)
                for column in hourly_backlog_windows_table.columns
                if column.name not in {"exchange", "window_start", "source", "first_detected_at"}
            }
            connection.execute(
                statement.on_conflict_do_update(
                    index_elements=["exchange", "window_start", "source"],
                    set_=update_columns,
                )
            )
            row = connection.execute(
                hourly_backlog_windows_table.select().where(key_filter)
            ).mappings().one()
        return dict(row)

    def mark_hourly_backlog_queued(
        self,
        exchange: str,
        window_start: datetime,
        source: str = "yahoo",
    ) -> None:
        self._update_hourly_backlog_status(
            exchange=exchange,
            window_start=window_start,
            source=source,
            status="queued",
            increment_attempt=True,
        )

    def mark_hourly_backlog_running(
        self,
        exchange: str,
        window_start: datetime,
        recovery_run_id: str,
        source: str = "yahoo",
    ) -> None:
        self._update_hourly_backlog_status(
            exchange=exchange,
            window_start=window_start,
            source=source,
            status="running",
            recovery_run_id=recovery_run_id,
        )

    def finish_hourly_backlog_recovery(
        self,
        exchange: str,
        window_start: datetime,
        observed_symbol_count: int,
        expected_symbol_count: int,
        coverage_threshold: float,
        error_message: str | None = None,
        source: str = "yahoo",
    ) -> dict[str, Any] | None:
        coverage = observed_symbol_count / expected_symbol_count if expected_symbol_count else 0.0
        if error_message:
            status = "failed"
        elif coverage >= coverage_threshold:
            status = "recovered"
        else:
            status = "partial"
        values = {
            "status": status,
            "expected_symbol_count": expected_symbol_count,
            "observed_symbol_count": observed_symbol_count,
            "coverage_ratio": coverage,
            "last_checked_at": datetime.now(UTC),
            "last_recovery_at": datetime.now(UTC),
            "last_error": error_message,
        }
        query = (
            hourly_backlog_windows_table.update()
            .where(hourly_backlog_windows_table.c.exchange == exchange.upper())
            .where(hourly_backlog_windows_table.c.window_start == _as_utc(window_start))
            .where(hourly_backlog_windows_table.c.source == source)
            .values(**values)
            .returning(hourly_backlog_windows_table)
        )
        with self.engine.begin() as connection:
            row = connection.execute(query).mappings().first()
        return dict(row) if row else None

    def hourly_window_symbol_count(
        self,
        exchange: str,
        window_start: datetime,
        source: str = "yahoo",
    ) -> int:
        return self._hourly_symbol_counts(exchange, [_as_utc(window_start)], source).get(
            _as_utc(window_start),
            0,
        )

    def _hourly_symbol_counts(
        self,
        exchange: str,
        window_starts: list[datetime],
        source: str,
    ) -> dict[datetime, int]:
        if not window_starts:
            return {}
        query = (
            select(ohlcv_hourly_table.c.ts, func.count(func.distinct(ohlcv_hourly_table.c.ticker)))
            .where(ohlcv_hourly_table.c.exchange == exchange.upper())
            .where(ohlcv_hourly_table.c.source == source)
            .where(ohlcv_hourly_table.c.ts.in_(window_starts))
            .group_by(ohlcv_hourly_table.c.ts)
        )
        with self.engine.begin() as connection:
            rows = connection.execute(query).all()
        return {_as_utc(ts): int(count) for ts, count in rows}

    def _update_hourly_backlog_status(
        self,
        exchange: str,
        window_start: datetime,
        source: str,
        status: str,
        increment_attempt: bool = False,
        recovery_run_id: str | None = None,
    ) -> None:
        values: dict[str, Any] = {
            "status": status,
            "last_recovery_at": datetime.now(UTC),
        }
        if recovery_run_id:
            values["recovery_run_id"] = recovery_run_id
        if increment_attempt:
            values["attempt_count"] = hourly_backlog_windows_table.c.attempt_count + 1
        query = (
            hourly_backlog_windows_table.update()
            .where(hourly_backlog_windows_table.c.exchange == exchange.upper())
            .where(hourly_backlog_windows_table.c.window_start == _as_utc(window_start))
            .where(hourly_backlog_windows_table.c.source == source)
            .values(**values)
        )
        with self.engine.begin() as connection:
            connection.execute(query)

    def upsert_hourly_ohlcv(self, frame: pd.DataFrame, exchange: str, source: str = "yahoo") -> int:
        rows = self._ohlcv_rows(frame, exchange=exchange, source=source)
        if not rows:
            return 0

        total = 0
        with self.engine.begin() as connection:
            for chunk in _chunks(rows, size=1_000):
                statement = insert(ohlcv_hourly_table).values(chunk)
                update_columns = {
                    column.name: getattr(statement.excluded, column.name)
                    for column in ohlcv_hourly_table.columns
                    if column.name not in {"ticker", "ts", "source"}
                }
                connection.execute(
                    statement.on_conflict_do_update(
                        index_elements=["ticker", "ts", "source"],
                        set_=update_columns,
                    )
                )
                total += len(chunk)
        return total

    def upsert_provider_instruments(self, frame: pd.DataFrame, source: str = "upstox") -> int:
        rows = self._instrument_rows(frame, source=source)
        if not rows:
            return 0

        total = 0
        with self.engine.begin() as connection:
            for chunk in _chunks(rows, size=1_000):
                statement = insert(provider_instruments_table).values(chunk)
                update_columns = {
                    column.name: getattr(statement.excluded, column.name)
                    for column in provider_instruments_table.columns
                    if column.name not in {"source", "instrument_key"}
                }
                connection.execute(
                    statement.on_conflict_do_update(
                        index_elements=["source", "instrument_key"],
                        set_=update_columns,
                    )
                )
                total += len(chunk)
        return total

    def upsert_tradable_universe(
        self,
        universe_id: str,
        name: str,
        exchange: str,
        source: str,
        criteria: Mapping[str, Any],
        members: pd.DataFrame,
        description: str | None = None,
    ) -> int:
        created_at = datetime.now(UTC)
        member_rows = self._universe_member_rows(members, universe_id, created_at)
        with self.engine.begin() as connection:
            universe_statement = insert(tradable_universes_table).values(
                universe_id=universe_id,
                name=name,
                description=description,
                exchange=exchange,
                source=source,
                criteria_json=dict(criteria),
                created_at=created_at,
            )
            connection.execute(
                universe_statement.on_conflict_do_update(
                    index_elements=["universe_id"],
                    set_={
                        "name": universe_statement.excluded.name,
                        "description": universe_statement.excluded.description,
                        "exchange": universe_statement.excluded.exchange,
                        "source": universe_statement.excluded.source,
                        "criteria_json": universe_statement.excluded.criteria_json,
                        "created_at": universe_statement.excluded.created_at,
                    },
                )
            )
            connection.execute(
                tradable_universe_members_table.delete().where(
                    tradable_universe_members_table.c.universe_id == universe_id
                )
            )
            if member_rows:
                for chunk in _chunks(member_rows, size=1_000):
                    connection.execute(tradable_universe_members_table.insert().values(chunk))
        return len(member_rows)

    def upsert_daily_ohlcv(
        self,
        frame: pd.DataFrame,
        exchange: str = "NSE",
        source: str = "upstox",
    ) -> int:
        rows = self._daily_ohlcv_rows(frame, exchange=exchange, source=source)
        if not rows:
            return 0

        total = 0
        with self.engine.begin() as connection:
            for chunk in _chunks(rows, size=1_000):
                statement = insert(ohlcv_daily_table).values(chunk)
                update_columns = {
                    column.name: getattr(statement.excluded, column.name)
                    for column in ohlcv_daily_table.columns
                    if column.name not in {"instrument_key", "source", "date"}
                }
                connection.execute(
                    statement.on_conflict_do_update(
                        index_elements=["instrument_key", "source", "date"],
                        set_=update_columns,
                    )
                )
                total += len(chunk)
        return total

    def latest_daily_ohlcv_dates(
        self,
        instrument_keys: list[str],
        source: str = "upstox",
    ) -> dict[str, date]:
        if not instrument_keys:
            return {}
        with self.engine.begin() as connection:
            rows = (
                connection.execute(
                    select(
                        ohlcv_daily_table.c.instrument_key,
                        func.max(ohlcv_daily_table.c.date).label("latest_date"),
                    )
                    .where(ohlcv_daily_table.c.source == source)
                    .where(ohlcv_daily_table.c.instrument_key.in_(instrument_keys))
                    .group_by(ohlcv_daily_table.c.instrument_key)
                )
                .mappings()
                .all()
            )
        return {
            str(row["instrument_key"]): row["latest_date"]
            for row in rows
            if row["latest_date"] is not None
        }

    def resolve_provider_instruments(
        self,
        symbols: list[str],
        source: str = "upstox",
        exchange: str = "NSE",
    ) -> list[dict[str, Any]]:
        normalized_symbols = sorted(
            {symbol.strip().upper() for symbol in symbols if symbol.strip()}
        )
        if not normalized_symbols:
            return []
        exchange_upper = exchange.upper()
        query = (
            provider_instruments_table.select()
            .where(provider_instruments_table.c.source == source)
            .where(provider_instruments_table.c.active.is_(True))
            .where(provider_instruments_table.c.trading_symbol.in_(normalized_symbols))
            .where(
                (provider_instruments_table.c.exchange == exchange_upper)
                | (provider_instruments_table.c.segment == f"{exchange_upper}_EQ")
            )
            .order_by(provider_instruments_table.c.trading_symbol)
        )
        with self.engine.begin() as connection:
            return [dict(row) for row in connection.execute(query).mappings()]

    def daily_ohlcv_dates_by_instrument(
        self,
        instrument_keys: list[str],
        start_date: date,
        end_date: date,
        source: str = "upstox",
        exchange: str = "NSE",
    ) -> dict[str, set[date]]:
        if not instrument_keys:
            return {}
        query = (
            select(ohlcv_daily_table.c.instrument_key, ohlcv_daily_table.c.date)
            .where(ohlcv_daily_table.c.source == source)
            .where(ohlcv_daily_table.c.exchange == exchange.upper())
            .where(ohlcv_daily_table.c.instrument_key.in_(instrument_keys))
            .where(ohlcv_daily_table.c.date >= start_date)
            .where(ohlcv_daily_table.c.date <= end_date)
            .order_by(ohlcv_daily_table.c.instrument_key, ohlcv_daily_table.c.date)
        )
        dates_by_key: dict[str, set[date]] = {key: set() for key in instrument_keys}
        with self.engine.begin() as connection:
            rows = connection.execute(query).all()
        for instrument_key, candle_date in rows:
            if candle_date is not None:
                dates_by_key.setdefault(str(instrument_key), set()).add(candle_date)
        return dates_by_key

    def daily_ohlcv_average_turnover_by_instrument(
        self,
        instrument_keys: list[str],
        start_date: date,
        end_date: date,
        source: str = "upstox",
        exchange: str = "NSE",
    ) -> dict[str, float]:
        if not instrument_keys:
            return {}
        query = (
            select(
                ohlcv_daily_table.c.instrument_key,
                func.avg(ohlcv_daily_table.c.close * ohlcv_daily_table.c.volume).label(
                    "avg_daily_turnover"
                ),
            )
            .where(ohlcv_daily_table.c.source == source)
            .where(ohlcv_daily_table.c.exchange == exchange.upper())
            .where(ohlcv_daily_table.c.instrument_key.in_(instrument_keys))
            .where(ohlcv_daily_table.c.date >= start_date)
            .where(ohlcv_daily_table.c.date <= end_date)
            .group_by(ohlcv_daily_table.c.instrument_key)
        )
        with self.engine.begin() as connection:
            rows = connection.execute(query).mappings().all()
        return {
            str(row["instrument_key"]): float(row["avg_daily_turnover"])
            for row in rows
            if row["avg_daily_turnover"] is not None
        }

    def daily_ohlcv_availability(
        self,
        source: str = "upstox",
        exchange: str = "NSE",
        start_date: date | None = None,
        end_date: date | None = None,
        query_text: str | None = None,
        universe_id: str | None = None,
        coverage_status: str | None = None,
        expected_rows_per_symbol: int = 0,
        limit: int = 50,
        offset: int = 0,
        sort: str = "symbol",
    ) -> dict[str, Any]:
        if (start_date is None) != (end_date is None):
            raise ValueError("start_date and end_date must be supplied together.")
        if start_date and end_date and start_date > end_date:
            raise ValueError("start_date must be on or before end_date.")

        exchange_upper = exchange.upper()
        params: dict[str, Any] = {
            "source": source,
            "exchange": exchange_upper,
            "expected_rows": int(expected_rows_per_symbol),
            "limit": int(limit),
            "offset": int(offset),
        }
        ohlcv_date_filter = ""
        if start_date and end_date:
            params["start_date"] = start_date
            params["end_date"] = end_date
            ohlcv_date_filter = "AND d.date BETWEEN :start_date AND :end_date"

        filters = [
            "pi.source = :source",
            "pi.active = true",
            "(upper(coalesce(pi.segment, '')) = (:exchange || '_EQ'))",
            "(upper(coalesce(pi.asset_type, '')) IN ('EQ', 'EQUITY', ''))",
        ]
        universe_join = ""
        if universe_id:
            params["universe_id"] = universe_id
            universe_join = """
                JOIN tradable_universe_members tum
                    ON tum.instrument_key = pi.instrument_key
                   AND tum.universe_id = :universe_id
            """
        if query_text:
            params["query"] = f"%{query_text.strip().upper()}%"
            filters.append(
                "("
                "upper(pi.trading_symbol) LIKE :query OR "
                "upper(coalesce(pi.name, '')) LIKE :query OR "
                "upper(coalesce(pi.isin, '')) LIKE :query OR "
                "upper(pi.instrument_key) LIKE :query"
                ")"
            )

        status_filter = ""
        if coverage_status:
            normalized_status = coverage_status.lower()
            if normalized_status not in {"complete", "partial", "empty"}:
                raise ValueError("coverage_status must be complete, partial, or empty.")
            params["coverage_status"] = normalized_status
            status_filter = "WHERE coverage_status = :coverage_status"

        order_by = {
            "symbol": "symbol ASC",
            "-symbol": "symbol DESC",
            "coverage_pct": "coverage_pct ASC, symbol ASC",
            "-coverage_pct": "coverage_pct DESC, symbol ASC",
            "missing_rows": "missing_rows ASC, symbol ASC",
            "-missing_rows": "missing_rows DESC, symbol ASC",
            "latest_stored_date": "latest_stored_date ASC NULLS FIRST, symbol ASC",
            "-latest_stored_date": "latest_stored_date DESC NULLS LAST, symbol ASC",
        }.get(sort)
        if order_by is None:
            raise ValueError("Unsupported sort value.")

        statement = text(
            f"""
            WITH latest_fetch AS (
                SELECT DISTINCT ON (instrument_key)
                    instrument_key,
                    status AS last_fetch_status
                FROM daily_ohlcv_fetch_coverage
                WHERE source = :source
                  AND exchange = :exchange
                ORDER BY instrument_key, created_at DESC
            ),
            latest_success AS (
                SELECT DISTINCT ON (instrument_key)
                    instrument_key,
                    run_id AS last_successful_run
                FROM daily_ohlcv_fetch_coverage
                WHERE source = :source
                  AND exchange = :exchange
                  AND status = 'fetched'
                ORDER BY instrument_key, created_at DESC
            ),
            base AS (
                SELECT
                    pi.trading_symbol AS symbol,
                    pi.name,
                    pi.instrument_key,
                    pi.source AS provider,
                    :exchange AS exchange,
                    min(d.date) AS first_stored_date,
                    max(d.date) AS latest_stored_date,
                    count(d.date)::bigint AS stored_rows,
                    ls.last_successful_run,
                    lf.last_fetch_status
                FROM provider_instruments pi
                {universe_join}
                LEFT JOIN ohlcv_daily d
                    ON d.instrument_key = pi.instrument_key
                   AND d.source = pi.source
                   AND d.exchange = :exchange
                   {ohlcv_date_filter}
                LEFT JOIN latest_fetch lf
                    ON lf.instrument_key = pi.instrument_key
                LEFT JOIN latest_success ls
                    ON ls.instrument_key = pi.instrument_key
                WHERE {" AND ".join(filters)}
                GROUP BY
                    pi.trading_symbol,
                    pi.name,
                    pi.instrument_key,
                    pi.source,
                    ls.last_successful_run,
                    lf.last_fetch_status
            ),
            scored AS (
                SELECT
                    *,
                    CAST(:expected_rows AS bigint) AS expected_rows,
                    greatest(
                        CAST(:expected_rows AS bigint) - stored_rows,
                        0
                    )::bigint AS missing_rows,
                    CASE
                        WHEN CAST(:expected_rows AS bigint) <= 0 THEN 0.0
                        ELSE least(stored_rows::float / CAST(:expected_rows AS float), 1.0)
                    END AS coverage_pct,
                    CASE
                        WHEN stored_rows = 0 THEN 'empty'
                        WHEN CAST(:expected_rows AS bigint) > 0
                            AND stored_rows >= CAST(:expected_rows AS bigint) THEN 'complete'
                        ELSE 'partial'
                    END AS coverage_status
                FROM base
            ),
            filtered AS (
                SELECT * FROM scored
                {status_filter}
            ),
            summary AS (
                SELECT
                    count(*)::bigint AS symbols_total,
                    count(*) FILTER (
                        WHERE coverage_status = 'complete'
                    )::bigint AS symbols_complete,
                    count(*) FILTER (WHERE coverage_status = 'partial')::bigint AS symbols_partial,
                    count(*) FILTER (WHERE coverage_status = 'empty')::bigint AS symbols_empty,
                    coalesce(sum(expected_rows), 0)::bigint AS expected_rows,
                    coalesce(sum(stored_rows), 0)::bigint AS stored_rows,
                    coalesce(sum(missing_rows), 0)::bigint AS missing_rows,
                    count(*) FILTER (
                        WHERE missing_rows > 0
                    )::bigint AS estimated_provider_calls_for_missing
                FROM filtered
            ),
            total_count AS (
                SELECT count(*)::bigint AS total FROM filtered
            )
            SELECT
                f.*,
                tc.total,
                s.symbols_total,
                s.symbols_complete,
                s.symbols_partial,
                s.symbols_empty,
                s.expected_rows AS summary_expected_rows,
                s.stored_rows AS summary_stored_rows,
                s.missing_rows AS summary_missing_rows,
                s.estimated_provider_calls_for_missing
            FROM filtered f
            CROSS JOIN total_count tc
            CROSS JOIN summary s
            ORDER BY {order_by}
            LIMIT :limit OFFSET :offset
            """
        )
        with self.engine.begin() as connection:
            rows = [dict(row) for row in connection.execute(statement, params).mappings()]

        summary = {
            "symbols_total": 0,
            "symbols_complete": 0,
            "symbols_partial": 0,
            "symbols_empty": 0,
            "expected_rows": 0,
            "stored_rows": 0,
            "missing_rows": 0,
            "estimated_provider_calls_for_missing": 0,
        }
        total = 0
        if rows:
            first = rows[0]
            total = int(first["total"] or 0)
            summary = {
                "symbols_total": int(first["symbols_total"] or 0),
                "symbols_complete": int(first["symbols_complete"] or 0),
                "symbols_partial": int(first["symbols_partial"] or 0),
                "symbols_empty": int(first["symbols_empty"] or 0),
                "expected_rows": int(first["summary_expected_rows"] or 0),
                "stored_rows": int(first["summary_stored_rows"] or 0),
                "missing_rows": int(first["summary_missing_rows"] or 0),
                "estimated_provider_calls_for_missing": int(
                    first["estimated_provider_calls_for_missing"] or 0
                ),
            }

        clean_rows = []
        for row in rows:
            clean_rows.append(
                {
                    "symbol": row["symbol"],
                    "name": row.get("name"),
                    "instrument_key": row["instrument_key"],
                    "provider": row["provider"],
                    "exchange": row["exchange"],
                    "interval": "1d",
                    "first_stored_date": row.get("first_stored_date"),
                    "latest_stored_date": row.get("latest_stored_date"),
                    "stored_rows": int(row["stored_rows"] or 0),
                    "expected_rows": int(row["expected_rows"] or 0),
                    "coverage_pct": float(row["coverage_pct"] or 0.0),
                    "missing_rows": int(row["missing_rows"] or 0),
                    "coverage_status": row["coverage_status"],
                    "last_successful_run": row.get("last_successful_run"),
                    "last_fetch_status": row.get("last_fetch_status"),
                }
            )
        return {"total": total, "rows": clean_rows, "summary": summary}

    def seeded_daily_ohlcv_availability(
        self,
        symbols: list[dict[str, str | None]],
        source: str,
        exchange: str,
        start_date: date | None = None,
        end_date: date | None = None,
        query_text: str | None = None,
        coverage_status: str | None = None,
        expected_rows_per_symbol: int = 0,
        limit: int = 50,
        offset: int = 0,
        sort: str = "symbol",
    ) -> dict[str, Any]:
        if (start_date is None) != (end_date is None):
            raise ValueError("start_date and end_date must be supplied together.")
        if start_date and end_date and start_date > end_date:
            raise ValueError("start_date must be on or before end_date.")

        seed_rows = [
            row
            for row in symbols
            if row.get("symbol") and row.get("instrument_key")
        ]
        if not seed_rows:
            return {
                "total": 0,
                "rows": [],
                "summary": {
                    "symbols_total": 0,
                    "symbols_complete": 0,
                    "symbols_partial": 0,
                    "symbols_empty": 0,
                    "expected_rows": 0,
                    "stored_rows": 0,
                    "missing_rows": 0,
                    "estimated_provider_calls_for_missing": 0,
                },
            }

        params: dict[str, Any] = {
            "source": source,
            "exchange": exchange.upper(),
            "expected_rows": int(expected_rows_per_symbol),
            "limit": int(limit),
            "offset": int(offset),
        }
        values_sql = []
        for index, row in enumerate(seed_rows):
            symbol_key = f"symbol_{index}"
            name_key = f"name_{index}"
            instrument_key = f"instrument_key_{index}"
            values_sql.append(f"(:{symbol_key}, :{name_key}, :{instrument_key})")
            params[symbol_key] = str(row["symbol"]).upper()
            params[name_key] = row.get("name")
            params[instrument_key] = str(row["instrument_key"])

        ohlcv_date_filter = ""
        if start_date and end_date:
            params["start_date"] = start_date
            params["end_date"] = end_date
            ohlcv_date_filter = "AND d.date BETWEEN :start_date AND :end_date"

        filters = []
        if query_text:
            params["query"] = f"%{query_text.strip().upper()}%"
            filters.append(
                "("
                "upper(s.symbol) LIKE :query OR "
                "upper(coalesce(s.name, '')) LIKE :query OR "
                "upper(s.instrument_key) LIKE :query"
                ")"
            )

        status_filter = ""
        if coverage_status:
            normalized_status = coverage_status.lower()
            if normalized_status not in {"complete", "partial", "empty"}:
                raise ValueError("coverage_status must be complete, partial, or empty.")
            params["coverage_status"] = normalized_status
            status_filter = "WHERE coverage_status = :coverage_status"

        order_by = {
            "symbol": "symbol ASC",
            "-symbol": "symbol DESC",
            "coverage_pct": "coverage_pct ASC, symbol ASC",
            "-coverage_pct": "coverage_pct DESC, symbol ASC",
            "missing_rows": "missing_rows ASC, symbol ASC",
            "-missing_rows": "missing_rows DESC, symbol ASC",
            "latest_stored_date": "latest_stored_date ASC NULLS FIRST, symbol ASC",
            "-latest_stored_date": "latest_stored_date DESC NULLS LAST, symbol ASC",
        }.get(sort)
        if order_by is None:
            raise ValueError("Unsupported sort value.")

        where_clause = f"WHERE {' AND '.join(filters)}" if filters else ""
        statement = text(
            f"""
            WITH seed_symbols(symbol, name, instrument_key) AS (
                VALUES {", ".join(values_sql)}
            ),
            latest_fetch AS (
                SELECT DISTINCT ON (instrument_key)
                    instrument_key,
                    status AS last_fetch_status
                FROM daily_ohlcv_fetch_coverage
                WHERE source = :source
                  AND exchange = :exchange
                ORDER BY instrument_key, created_at DESC
            ),
            latest_success AS (
                SELECT DISTINCT ON (instrument_key)
                    instrument_key,
                    run_id AS last_successful_run
                FROM daily_ohlcv_fetch_coverage
                WHERE source = :source
                  AND exchange = :exchange
                  AND status = 'fetched'
                ORDER BY instrument_key, created_at DESC
            ),
            base AS (
                SELECT
                    s.symbol,
                    s.name,
                    s.instrument_key,
                    :source AS provider,
                    :exchange AS exchange,
                    min(d.date) AS first_stored_date,
                    max(d.date) AS latest_stored_date,
                    count(d.date)::bigint AS stored_rows,
                    ls.last_successful_run,
                    lf.last_fetch_status
                FROM seed_symbols s
                LEFT JOIN ohlcv_daily d
                    ON d.instrument_key = s.instrument_key
                   AND d.source = :source
                   AND d.exchange = :exchange
                   {ohlcv_date_filter}
                LEFT JOIN latest_fetch lf
                    ON lf.instrument_key = s.instrument_key
                LEFT JOIN latest_success ls
                    ON ls.instrument_key = s.instrument_key
                {where_clause}
                GROUP BY
                    s.symbol,
                    s.name,
                    s.instrument_key,
                    ls.last_successful_run,
                    lf.last_fetch_status
            ),
            scored AS (
                SELECT
                    *,
                    CAST(:expected_rows AS bigint) AS expected_rows,
                    greatest(
                        CAST(:expected_rows AS bigint) - stored_rows,
                        0
                    )::bigint AS missing_rows,
                    CASE
                        WHEN CAST(:expected_rows AS bigint) <= 0 THEN 0.0
                        ELSE least(stored_rows::float / CAST(:expected_rows AS float), 1.0)
                    END AS coverage_pct,
                    CASE
                        WHEN stored_rows = 0 THEN 'empty'
                        WHEN CAST(:expected_rows AS bigint) > 0
                            AND stored_rows >= CAST(:expected_rows AS bigint) THEN 'complete'
                        ELSE 'partial'
                    END AS coverage_status
                FROM base
            ),
            filtered AS (
                SELECT * FROM scored
                {status_filter}
            ),
            summary AS (
                SELECT
                    count(*)::bigint AS symbols_total,
                    count(*) FILTER (
                        WHERE coverage_status = 'complete'
                    )::bigint AS symbols_complete,
                    count(*) FILTER (WHERE coverage_status = 'partial')::bigint AS symbols_partial,
                    count(*) FILTER (WHERE coverage_status = 'empty')::bigint AS symbols_empty,
                    coalesce(sum(expected_rows), 0)::bigint AS expected_rows,
                    coalesce(sum(stored_rows), 0)::bigint AS stored_rows,
                    coalesce(sum(missing_rows), 0)::bigint AS missing_rows,
                    count(*) FILTER (
                        WHERE missing_rows > 0
                    )::bigint AS estimated_provider_calls_for_missing
                FROM filtered
            ),
            total_count AS (
                SELECT count(*)::bigint AS total FROM filtered
            )
            SELECT
                f.*,
                tc.total,
                s.symbols_total,
                s.symbols_complete,
                s.symbols_partial,
                s.symbols_empty,
                s.expected_rows AS summary_expected_rows,
                s.stored_rows AS summary_stored_rows,
                s.missing_rows AS summary_missing_rows,
                s.estimated_provider_calls_for_missing
            FROM filtered f
            CROSS JOIN total_count tc
            CROSS JOIN summary s
            ORDER BY {order_by}
            LIMIT :limit OFFSET :offset
            """
        )
        with self.engine.begin() as connection:
            rows = [dict(row) for row in connection.execute(statement, params).mappings()]

        summary = {
            "symbols_total": 0,
            "symbols_complete": 0,
            "symbols_partial": 0,
            "symbols_empty": 0,
            "expected_rows": 0,
            "stored_rows": 0,
            "missing_rows": 0,
            "estimated_provider_calls_for_missing": 0,
        }
        total = 0
        if rows:
            first = rows[0]
            total = int(first["total"] or 0)
            summary = {
                "symbols_total": int(first["symbols_total"] or 0),
                "symbols_complete": int(first["symbols_complete"] or 0),
                "symbols_partial": int(first["symbols_partial"] or 0),
                "symbols_empty": int(first["symbols_empty"] or 0),
                "expected_rows": int(first["summary_expected_rows"] or 0),
                "stored_rows": int(first["summary_stored_rows"] or 0),
                "missing_rows": int(first["summary_missing_rows"] or 0),
                "estimated_provider_calls_for_missing": int(
                    first["estimated_provider_calls_for_missing"] or 0
                ),
            }

        clean_rows = []
        for row in rows:
            clean_rows.append(
                {
                    "symbol": row["symbol"],
                    "name": row.get("name"),
                    "instrument_key": row["instrument_key"],
                    "provider": row["provider"],
                    "exchange": row["exchange"],
                    "interval": "1d",
                    "first_stored_date": row.get("first_stored_date"),
                    "latest_stored_date": row.get("latest_stored_date"),
                    "stored_rows": int(row["stored_rows"] or 0),
                    "expected_rows": int(row["expected_rows"] or 0),
                    "coverage_pct": float(row["coverage_pct"] or 0.0),
                    "missing_rows": int(row["missing_rows"] or 0),
                    "coverage_status": row["coverage_status"],
                    "last_successful_run": row.get("last_successful_run"),
                    "last_fetch_status": row.get("last_fetch_status"),
                }
            )
        return {"total": total, "rows": clean_rows, "summary": summary}

    def search_provider_instruments(
        self,
        query_text: str,
        source: str = "upstox",
        exchange: str = "NSE",
        limit: int = 20,
    ) -> list[dict[str, Any]]:
        normalized = query_text.strip().upper()
        if not normalized:
            return []
        params = {
            "source": source,
            "exchange": exchange.upper(),
            "exact": normalized,
            "prefix": f"{normalized}%",
            "query": f"%{normalized}%",
            "limit": int(limit),
        }
        statement = text(
            """
            WITH universe_rank AS (
                SELECT
                    instrument_key,
                    symbol,
                    min(rank) AS rank
                FROM tradable_universe_members
                WHERE instrument_key IS NOT NULL
                GROUP BY instrument_key, symbol
            )
            SELECT
                pi.trading_symbol AS symbol,
                pi.name,
                pi.instrument_key,
                pi.source AS provider,
                :exchange AS exchange,
                pi.isin,
                pi.segment,
                pi.asset_type
            FROM provider_instruments pi
            LEFT JOIN universe_rank ur
                ON ur.instrument_key = pi.instrument_key
               AND ur.symbol = pi.trading_symbol
            WHERE pi.source = :source
              AND pi.active = true
              AND upper(coalesce(pi.segment, '')) = (:exchange || '_EQ')
              AND upper(coalesce(pi.asset_type, '')) IN ('EQ', 'EQUITY', '')
              AND (
                    upper(pi.trading_symbol) LIKE :query OR
                    upper(coalesce(pi.name, '')) LIKE :query OR
                    upper(coalesce(pi.isin, '')) LIKE :query OR
                    upper(pi.instrument_key) LIKE :query
              )
            ORDER BY
                CASE
                    WHEN upper(pi.trading_symbol) = :exact THEN 0
                    WHEN ur.rank IS NOT NULL
                        AND upper(pi.trading_symbol) LIKE :prefix THEN 1
                    WHEN ur.rank IS NOT NULL
                        AND upper(coalesce(pi.name, '')) LIKE :prefix THEN 2
                    WHEN upper(pi.trading_symbol) LIKE :prefix THEN 3
                    WHEN upper(coalesce(pi.name, '')) LIKE :prefix THEN 4
                    ELSE 5
                END,
                ur.rank ASC NULLS LAST,
                pi.trading_symbol,
                pi.instrument_key
            LIMIT :limit
            """
        )
        with self.engine.begin() as connection:
            rows = [dict(row) for row in connection.execute(statement, params).mappings()]
        return [
            {
                "symbol": row["symbol"],
                "name": row.get("name"),
                "instrument_key": row["instrument_key"],
                "provider": row["provider"],
                "exchange": row["exchange"],
                "isin": row.get("isin"),
                "segment": row.get("segment"),
                "asset_type": row.get("asset_type"),
            }
            for row in rows
        ]

    def tradable_universes(
        self,
        exchange: str = "NSE",
        source: str | None = None,
    ) -> list[dict[str, Any]]:
        member_counts = (
            select(
                tradable_universe_members_table.c.universe_id,
                func.count(tradable_universe_members_table.c.symbol).label("member_count"),
            )
            .group_by(tradable_universe_members_table.c.universe_id)
            .subquery()
        )
        query = (
            select(
                tradable_universes_table.c.universe_id,
                tradable_universes_table.c.name,
                tradable_universes_table.c.description,
                tradable_universes_table.c.exchange,
                tradable_universes_table.c.source,
                tradable_universes_table.c.criteria_json.label("criteria"),
                tradable_universes_table.c.created_at,
                func.coalesce(member_counts.c.member_count, 0).label("member_count"),
            )
            .select_from(
                tradable_universes_table.outerjoin(
                    member_counts,
                    member_counts.c.universe_id
                    == tradable_universes_table.c.universe_id,
                )
            )
            .where(tradable_universes_table.c.exchange == exchange.upper())
            .order_by(tradable_universes_table.c.name)
        )
        if source:
            query = query.where(tradable_universes_table.c.source == source)
        with self.engine.begin() as connection:
            rows = [dict(row) for row in connection.execute(query).mappings()]
        for row in rows:
            row["criteria"] = row.get("criteria") if isinstance(row.get("criteria"), dict) else {}
            row["member_count"] = int(row.get("member_count") or 0)
        return rows

    def tradable_universe_members(
        self,
        universe_id: str,
        limit: int = 500,
        offset: int = 0,
    ) -> list[dict[str, Any]]:
        query = (
            tradable_universe_members_table.select()
            .where(tradable_universe_members_table.c.universe_id == universe_id)
            .order_by(
                tradable_universe_members_table.c.rank.asc().nulls_last(),
                tradable_universe_members_table.c.symbol,
            )
            .limit(limit)
            .offset(offset)
        )
        with self.engine.begin() as connection:
            return [dict(row) for row in connection.execute(query).mappings()]

    def daily_ohlcv_frame(
        self,
        exchange: str = "NSE",
        source: str = "upstox",
        limit: int | None = None,
        start_date: date | None = None,
        end_date: date | None = None,
    ) -> pd.DataFrame:
        query = (
            ohlcv_daily_table.select()
            .where(ohlcv_daily_table.c.exchange == exchange.upper())
            .where(ohlcv_daily_table.c.source == source)
            .order_by(ohlcv_daily_table.c.instrument_key, ohlcv_daily_table.c.date)
        )
        if start_date is not None:
            query = query.where(ohlcv_daily_table.c.date >= start_date)
        if end_date is not None:
            query = query.where(ohlcv_daily_table.c.date <= end_date)
        if limit:
            keys_query = (
                select(ohlcv_daily_table.c.instrument_key)
                .where(ohlcv_daily_table.c.exchange == exchange.upper())
                .where(ohlcv_daily_table.c.source == source)
                .group_by(ohlcv_daily_table.c.instrument_key)
                .order_by(ohlcv_daily_table.c.instrument_key)
                .limit(limit)
            )
            with self.engine.begin() as connection:
                keys = [row[0] for row in connection.execute(keys_query).all()]
            query = query.where(ohlcv_daily_table.c.instrument_key.in_(keys))

        with self.engine.begin() as connection:
            rows = [dict(row) for row in connection.execute(query).mappings()]
        return pd.DataFrame(rows)

    def latest_daily_feature_date(
        self,
        feature_version: str,
        exchange: str = "NSE",
    ) -> date | None:
        query = (
            select(func.max(features_daily_table.c.date))
            .where(features_daily_table.c.feature_version == feature_version)
            .where(features_daily_table.c.exchange == exchange.upper())
        )
        with self.engine.begin() as connection:
            return connection.execute(query).scalar_one_or_none()

    def upsert_daily_features(self, frame: pd.DataFrame) -> int:
        rows = self._daily_feature_rows(frame)
        if not rows:
            return 0

        total = 0
        with self.engine.begin() as connection:
            for chunk in _chunks(rows, size=1_000):
                statement = insert(features_daily_table).values(chunk)
                update_columns = {
                    column.name: getattr(statement.excluded, column.name)
                    for column in features_daily_table.columns
                    if column.name not in {"instrument_key", "date", "feature_version"}
                }
                connection.execute(
                    statement.on_conflict_do_update(
                        index_elements=["instrument_key", "date", "feature_version"],
                        set_=update_columns,
                    )
                )
                total += len(chunk)
        return total

    def insert_feature_run(
        self,
        summary: Mapping[str, Any],
        source: str,
        status: str = "completed",
        started_at: datetime | None = None,
        finished_at: datetime | None = None,
    ) -> str:
        run_id = str(uuid4())
        finished = finished_at or datetime.now(UTC)
        row = {
            "run_id": run_id,
            "dataset_name": str(summary["dataset_name"]),
            "feature_version": str(summary["feature_version"]),
            "source": source,
            "status": status,
            "started_at": started_at or finished,
            "finished_at": finished,
            "rows": int(summary.get("row_count") or 0),
            "symbols": int(summary.get("symbol_count") or 0),
            "date_min": _nullable_date(summary.get("date_min")),
            "date_max": _nullable_date(summary.get("date_max")),
            "invalid_ohlcv_count": int(summary.get("invalid_ohlcv_count") or 0),
            "summary_json": dict(summary),
        }
        with self.engine.begin() as connection:
            connection.execute(feature_runs_table.insert().values(row))
        return run_id

    def insert_feature_audits(
        self,
        audit: pd.DataFrame,
        dataset_name: str,
        feature_version: str,
        run_id: str | None = None,
    ) -> int:
        rows = self._feature_audit_rows(
            audit,
            dataset_name=dataset_name,
            feature_version=feature_version,
            run_id=run_id,
        )
        if not rows:
            return 0
        with self.engine.begin() as connection:
            for chunk in _chunks(rows, size=1_000):
                connection.execute(feature_audits_table.insert().values(chunk))
        return len(rows)

    def daily_feature_frame(
        self,
        feature_version: str,
        exchange: str = "NSE",
        limit: int | None = None,
        start_date: date | None = None,
        end_date: date | None = None,
    ) -> pd.DataFrame:
        query = (
            features_daily_table.select()
            .where(features_daily_table.c.feature_version == feature_version)
            .where(features_daily_table.c.exchange == exchange.upper())
            .order_by(features_daily_table.c.instrument_key, features_daily_table.c.date)
        )
        if start_date is not None:
            query = query.where(features_daily_table.c.date >= start_date)
        if end_date is not None:
            query = query.where(features_daily_table.c.date <= end_date)
        if limit:
            keys_query = (
                select(features_daily_table.c.instrument_key)
                .where(features_daily_table.c.feature_version == feature_version)
                .where(features_daily_table.c.exchange == exchange.upper())
                .group_by(features_daily_table.c.instrument_key)
                .order_by(features_daily_table.c.instrument_key)
                .limit(limit)
            )
            with self.engine.begin() as connection:
                keys = [row[0] for row in connection.execute(keys_query).all()]
            query = query.where(features_daily_table.c.instrument_key.in_(keys))

        with self.engine.begin() as connection:
            rows = [dict(row) for row in connection.execute(query).mappings()]
        return pd.DataFrame(rows)

    def latest_daily_target_date(
        self,
        target_version: str,
        exchange: str = "NSE",
    ) -> date | None:
        query = (
            select(func.max(targets_daily_table.c.date))
            .where(targets_daily_table.c.target_version == target_version)
            .where(targets_daily_table.c.exchange == exchange.upper())
        )
        with self.engine.begin() as connection:
            return connection.execute(query).scalar_one_or_none()

    def upsert_daily_targets(self, frame: pd.DataFrame) -> int:
        rows = self._daily_target_rows(frame)
        if not rows:
            return 0

        total = 0
        with self.engine.begin() as connection:
            for chunk in _chunks(rows, size=1_000):
                statement = insert(targets_daily_table).values(chunk)
                update_columns = {
                    column.name: getattr(statement.excluded, column.name)
                    for column in targets_daily_table.columns
                    if column.name not in {"instrument_key", "date", "target_version"}
                }
                connection.execute(
                    statement.on_conflict_do_update(
                        index_elements=["instrument_key", "date", "target_version"],
                        set_=update_columns,
                    )
                )
                total += len(chunk)
        return total

    def insert_target_run(
        self,
        summary: Mapping[str, Any],
        source: str,
        status: str = "completed",
        started_at: datetime | None = None,
        finished_at: datetime | None = None,
    ) -> str:
        run_id = str(uuid4())
        finished = finished_at or datetime.now(UTC)
        row = {
            "run_id": run_id,
            "dataset_name": str(summary["dataset_name"]),
            "target_version": str(summary["target_version"]),
            "source": source,
            "status": status,
            "started_at": started_at or finished,
            "finished_at": finished,
            "rows": int(summary.get("row_count") or 0),
            "symbols": int(summary.get("symbol_count") or 0),
            "date_min": _nullable_date(summary.get("date_min")),
            "date_max": _nullable_date(summary.get("date_max")),
            "invalid_ohlcv_count": int(summary.get("invalid_ohlcv_count") or 0),
            "summary_json": dict(summary),
        }
        with self.engine.begin() as connection:
            connection.execute(target_runs_table.insert().values(row))
        return run_id

    def insert_target_audits(
        self,
        audit: pd.DataFrame,
        dataset_name: str,
        target_version: str,
        run_id: str | None = None,
    ) -> int:
        rows = self._target_audit_rows(
            audit,
            dataset_name=dataset_name,
            target_version=target_version,
            run_id=run_id,
        )
        if not rows:
            return 0
        with self.engine.begin() as connection:
            for chunk in _chunks(rows, size=1_000):
                connection.execute(target_audits_table.insert().values(chunk))
        return len(rows)

    def daily_target_frame(
        self,
        target_version: str,
        exchange: str = "NSE",
        limit: int | None = None,
        start_date: date | None = None,
        end_date: date | None = None,
    ) -> pd.DataFrame:
        query = (
            targets_daily_table.select()
            .where(targets_daily_table.c.target_version == target_version)
            .where(targets_daily_table.c.exchange == exchange.upper())
            .order_by(targets_daily_table.c.instrument_key, targets_daily_table.c.date)
        )
        if start_date is not None:
            query = query.where(targets_daily_table.c.date >= start_date)
        if end_date is not None:
            query = query.where(targets_daily_table.c.date <= end_date)
        if limit:
            keys_query = (
                select(targets_daily_table.c.instrument_key)
                .where(targets_daily_table.c.target_version == target_version)
                .where(targets_daily_table.c.exchange == exchange.upper())
                .group_by(targets_daily_table.c.instrument_key)
                .order_by(targets_daily_table.c.instrument_key)
                .limit(limit)
            )
            with self.engine.begin() as connection:
                keys = [row[0] for row in connection.execute(keys_query).all()]
            query = query.where(targets_daily_table.c.instrument_key.in_(keys))

        with self.engine.begin() as connection:
            rows = [dict(row) for row in connection.execute(query).mappings()]
        return pd.DataFrame(rows)

    def insert_data_quality_audits(
        self,
        audit: pd.DataFrame,
        dataset_name: str,
        source: str,
        interval: str,
    ) -> int:
        rows = self._audit_rows(
            audit,
            dataset_name=dataset_name,
            source=source,
            interval=interval,
        )
        if not rows:
            return 0
        with self.engine.begin() as connection:
            for chunk in _chunks(rows, size=1_000):
                connection.execute(data_quality_audits_table.insert().values(chunk))
        return len(rows)

    def start_ingestion_run(
        self,
        job_name: str,
        exchange: str,
        source: str,
        items_requested: int,
        run_metadata: Mapping[str, Any] | None = None,
    ) -> UUID:
        run_id = uuid4()
        with self.engine.begin() as connection:
            connection.execute(
                ingestion_runs_table.insert().values(
                    run_id=str(run_id),
                    job_name=job_name,
                    status="running",
                    exchange=exchange,
                    source=source,
                    started_at=datetime.now(UTC),
                    items_requested=items_requested,
                    items_processed=0,
                    items_succeeded=0,
                    items_failed=0,
                    run_metadata=dict(run_metadata or {}),
                )
            )
        return run_id

    def finish_ingestion_run(
        self,
        run_id: UUID,
        status: str,
        items_processed: int,
        items_succeeded: int,
        items_failed: int,
        error_message: str | None = None,
    ) -> None:
        with self.engine.begin() as connection:
            connection.execute(
                ingestion_runs_table.update()
                .where(ingestion_runs_table.c.run_id == str(run_id))
                .values(
                    status=status,
                    finished_at=datetime.now(UTC),
                    items_processed=items_processed,
                    items_succeeded=items_succeeded,
                    items_failed=items_failed,
                    error_message=error_message,
                )
            )

    def latest_runs(
        self,
        limit: int = 20,
        source: str | None = None,
        exchange: str | None = None,
        status: str | None = None,
    ) -> list[dict[str, Any]]:
        query = (
            ingestion_runs_table.select()
            .order_by(ingestion_runs_table.c.started_at.desc())
            .limit(limit)
        )
        if source:
            query = query.where(ingestion_runs_table.c.source == source)
        if exchange:
            query = query.where(ingestion_runs_table.c.exchange == exchange.upper())
        if status:
            query = query.where(ingestion_runs_table.c.status == status)
        with self.engine.begin() as connection:
            return [dict(row) for row in connection.execute(query).mappings()]

    def ingestion_run(self, run_id: str) -> dict[str, Any] | None:
        query = ingestion_runs_table.select().where(ingestion_runs_table.c.run_id == run_id)
        with self.engine.begin() as connection:
            row = connection.execute(query).mappings().first()
        return dict(row) if row else None

    def daily_ohlcv_fetch_coverage_for_run(
        self,
        run_id: str,
        source: str | None = None,
        exchange: str | None = None,
    ) -> list[dict[str, Any]]:
        query = (
            daily_ohlcv_fetch_coverage_table.select()
            .where(daily_ohlcv_fetch_coverage_table.c.run_id == run_id)
            .order_by(
                daily_ohlcv_fetch_coverage_table.c.status,
                daily_ohlcv_fetch_coverage_table.c.symbol,
            )
        )
        if source:
            query = query.where(daily_ohlcv_fetch_coverage_table.c.source == source)
        if exchange:
            query = query.where(
                daily_ohlcv_fetch_coverage_table.c.exchange == exchange.upper()
            )
        with self.engine.begin() as connection:
            return [dict(row) for row in connection.execute(query).mappings()]

    def provider_request_logs_for_run(
        self,
        run_id: str,
        provider: str | None = None,
        endpoint_group: str | None = None,
        limit: int = 100,
    ) -> list[dict[str, Any]]:
        query = (
            provider_request_log_table.select()
            .where(provider_request_log_table.c.run_id == run_id)
            .order_by(provider_request_log_table.c.created_at.desc())
            .limit(max(limit, 1))
        )
        if provider:
            query = query.where(provider_request_log_table.c.provider == provider.lower())
        if endpoint_group:
            query = query.where(provider_request_log_table.c.endpoint_group == endpoint_group)
        with self.engine.begin() as connection:
            return [dict(row) for row in connection.execute(query).mappings()]

    def provider_request_log_summary(
        self,
        run_id: str,
        provider: str | None = None,
        endpoint_group: str | None = None,
    ) -> list[dict[str, Any]]:
        query = select(
            provider_request_log_table.c.provider,
            provider_request_log_table.c.endpoint_group,
            provider_request_log_table.c.status,
            func.count().label("requests"),
            func.sum(provider_request_log_table.c.wait_seconds).label("wait_seconds"),
            func.avg(provider_request_log_table.c.duration_ms).label("avg_duration_ms"),
            func.sum(
                case((provider_request_log_table.c.rate_limited.is_(True), 1), else_=0)
            ).label("rate_limited_requests"),
        ).where(provider_request_log_table.c.run_id == run_id)
        if provider:
            query = query.where(provider_request_log_table.c.provider == provider.lower())
        if endpoint_group:
            query = query.where(provider_request_log_table.c.endpoint_group == endpoint_group)
        query = query.group_by(
            provider_request_log_table.c.provider,
            provider_request_log_table.c.endpoint_group,
            provider_request_log_table.c.status,
        ).order_by(
            provider_request_log_table.c.provider,
            provider_request_log_table.c.endpoint_group,
            provider_request_log_table.c.status,
        )
        with self.engine.begin() as connection:
            return [dict(row) for row in connection.execute(query).mappings()]

    def market_status(self) -> list[dict[str, Any]]:
        query = text(
            """
            WITH universe AS (
                SELECT exchange, count(*) AS universe_size
                FROM symbols
                WHERE is_active = true
                GROUP BY exchange
            ),
            candles AS (
                SELECT exchange, max(ts) AS latest_candle, count(DISTINCT ticker) AS candle_symbols
                FROM ohlcv_hourly
                GROUP BY exchange
            ),
            runs AS (
                SELECT DISTINCT ON (exchange) exchange, started_at, status
                FROM ingestion_runs
                WHERE job_name LIKE '%_hourly_ohlcv'
                ORDER BY exchange, started_at DESC
            )
            SELECT
                COALESCE(universe.exchange, candles.exchange, runs.exchange) AS exchange,
                COALESCE(universe.universe_size, 0) AS universe_size,
                candles.latest_candle,
                COALESCE(candles.candle_symbols, 0) AS candle_symbols,
                runs.started_at AS last_ohlcv_run,
                runs.status AS last_ohlcv_status
            FROM universe
            FULL OUTER JOIN candles ON universe.exchange = candles.exchange
            FULL OUTER JOIN runs ON COALESCE(universe.exchange, candles.exchange) = runs.exchange
            ORDER BY exchange
            """
        )
        with self.engine.begin() as connection:
            return [dict(row) for row in connection.execute(query).mappings()]

    def candles(self, ticker: str, limit: int = 300) -> list[dict[str, Any]]:
        query = (
            ohlcv_hourly_table.select()
            .where(ohlcv_hourly_table.c.ticker == ticker.upper())
            .order_by(ohlcv_hourly_table.c.ts.desc())
            .limit(limit)
        )
        with self.engine.begin() as connection:
            rows = [dict(row) for row in connection.execute(query).mappings()]
        return list(reversed(rows))

    def latest_candles(
        self,
        exchange: str,
        symbols: list[str] | None = None,
        lookback_hours: int = 72,
        source: str = "yahoo",
    ) -> list[dict[str, Any]]:
        params: dict[str, Any] = {
            "exchange": exchange.upper(),
            "source": source,
            "lookback_start": datetime.now(UTC) - timedelta(hours=lookback_hours),
        }
        symbol_filter = ""
        if symbols:
            params["symbols"] = [symbol.upper() for symbol in symbols]
            symbol_filter = "AND ticker = ANY(:symbols)"

        query = text(
            f"""
            WITH ranked AS (
                SELECT
                    ticker,
                    ts,
                    open,
                    high,
                    low,
                    close,
                    volume,
                    quality_status,
                    fetched_at,
                    row_number() OVER (PARTITION BY ticker ORDER BY ts DESC) AS rn
                FROM ohlcv_hourly
                WHERE exchange = :exchange
                  AND source = :source
                  AND ts >= :lookback_start
                  {symbol_filter}
            )
            SELECT ticker, ts, open, high, low, close, volume, quality_status, fetched_at
            FROM ranked
            WHERE rn = 1
            ORDER BY ticker
            """
        )
        with self.engine.begin() as connection:
            return [dict(row) for row in connection.execute(query, params).mappings()]

    def symbol_timeseries(
        self,
        exchange: str,
        symbol: str,
        start_time: datetime,
        end_time: datetime,
        source: str = "yahoo",
    ) -> list[dict[str, Any]]:
        query = (
            ohlcv_hourly_table.select()
            .where(ohlcv_hourly_table.c.exchange == exchange.upper())
            .where(ohlcv_hourly_table.c.ticker == symbol.upper())
            .where(ohlcv_hourly_table.c.source == source)
            .where(ohlcv_hourly_table.c.ts >= _as_utc(start_time))
            .where(ohlcv_hourly_table.c.ts <= _as_utc(end_time))
            .order_by(ohlcv_hourly_table.c.ts)
        )
        with self.engine.begin() as connection:
            return [dict(row) for row in connection.execute(query).mappings()]

    def session_summary(
        self,
        exchange: str,
        session_date: date | None = None,
        source: str = "yahoo",
    ) -> dict[str, Any]:
        params: dict[str, Any] = {"exchange": exchange.upper(), "source": source}
        date_filter = ""
        if session_date:
            params["session_date"] = session_date
            date_filter = "WHERE session_date = :session_date"

        query = text(
            f"""
            WITH exchange_rows AS (
                SELECT
                    ticker,
                    ts,
                    close,
                    date(ts AT TIME ZONE 'UTC') AS session_date
                FROM ohlcv_hourly
                WHERE exchange = :exchange AND source = :source
            ),
            target_session AS (
                SELECT session_date
                FROM exchange_rows
                {date_filter}
                ORDER BY session_date DESC
                LIMIT 1
            ),
            session_rows AS (
                SELECT er.*
                FROM exchange_rows er
                JOIN target_session ts ON er.session_date = ts.session_date
            ),
            last_close AS (
                SELECT DISTINCT ON (ticker)
                    ticker,
                    close,
                    ts
                FROM session_rows
                ORDER BY ticker, ts DESC
            ),
            first_close AS (
                SELECT DISTINCT ON (ticker)
                    ticker,
                    close,
                    ts
                FROM session_rows
                ORDER BY ticker, ts ASC
            ),
            deltas AS (
                SELECT
                    lc.ticker,
                    fc.close AS first_close,
                    lc.close AS last_close,
                    CASE
                        WHEN fc.close > 0 THEN ((lc.close - fc.close) / fc.close) * 100.0
                        ELSE NULL
                    END AS return_pct
                FROM last_close lc
                JOIN first_close fc ON lc.ticker = fc.ticker
            )
            SELECT
                (SELECT session_date FROM target_session) AS session_date,
                count(*) AS symbol_count,
                count(*) FILTER (WHERE return_pct > 0) AS advancers,
                count(*) FILTER (WHERE return_pct < 0) AS decliners,
                count(*) FILTER (WHERE return_pct = 0) AS unchanged,
                avg(return_pct) AS avg_return_pct,
                min(return_pct) AS min_return_pct,
                max(return_pct) AS max_return_pct
            FROM deltas
            """
        )
        with self.engine.begin() as connection:
            row = connection.execute(query, params).mappings().first()
        return dict(row) if row else {}

    def data_quality_snapshot(self, exchange: str, source: str = "yahoo") -> dict[str, Any]:
        params = {"exchange": exchange.upper(), "source": source}
        query = text(
            """
            WITH latest_ts AS (
                SELECT max(ts) AS max_ts
                FROM ohlcv_hourly
                WHERE exchange = :exchange AND source = :source
            ),
            latest_counts AS (
                SELECT count(DISTINCT ticker) AS latest_candle_symbols
                FROM ohlcv_hourly o
                JOIN latest_ts l ON o.ts = l.max_ts
                WHERE o.exchange = :exchange AND o.source = :source
            ),
            active_universe AS (
                SELECT count(*) AS active_symbols
                FROM symbols
                WHERE exchange = :exchange AND is_active = true
            ),
            backlog AS (
                SELECT
                    count(*) FILTER (
                        WHERE status IN ('missing', 'partial', 'failed')
                    ) AS open_backlog_windows
                FROM hourly_backlog_windows
                WHERE exchange = :exchange AND source = :source
            )
            SELECT
                :exchange AS exchange,
                (SELECT max_ts FROM latest_ts) AS latest_candle_ts,
                coalesce(
                    (SELECT latest_candle_symbols FROM latest_counts),
                    0
                ) AS latest_candle_symbols,
                coalesce((SELECT active_symbols FROM active_universe), 0) AS active_symbols,
                coalesce((SELECT open_backlog_windows FROM backlog), 0) AS open_backlog_windows
            """
        )
        with self.engine.begin() as connection:
            row = connection.execute(query, params).mappings().first()
        if not row:
            return {}
        data = dict(row)
        active_symbols = int(data["active_symbols"] or 0)
        latest_candle_symbols = int(data["latest_candle_symbols"] or 0)
        data["completeness_ratio"] = (
            (latest_candle_symbols / active_symbols) if active_symbols > 0 else 0.0
        )
        return data

    @staticmethod
    def _ohlcv_rows(frame: pd.DataFrame, exchange: str, source: str) -> list[dict[str, Any]]:
        if frame.empty:
            return []

        column_map = {column.lower(): column for column in frame.columns}
        required = ["ticker", "datetime", "open", "high", "low", "close", "volume"]
        if missing := [column for column in required if column not in column_map]:
            raise ValueError(f"Missing OHLCV columns: {', '.join(missing)}")

        fetched_at = datetime.now(UTC)
        rows: list[dict[str, Any]] = []
        for record in frame.to_dict(orient="records"):
            row = {key.lower(): value for key, value in record.items()}
            if any(pd.isna(row[column]) for column in required):
                continue
            rows.append(
                {
                    "ticker": str(row["ticker"]).upper(),
                    "ts": pd.Timestamp(row["datetime"]).to_pydatetime(),
                    "source": source,
                    "exchange": exchange,
                    "open": float(row["open"]),
                    "high": float(row["high"]),
                    "low": float(row["low"]),
                    "close": float(row["close"]),
                    "volume": int(row["volume"]),
                    "fetched_at": fetched_at,
                    "quality_status": _quality_status(row),
                }
            )
        return rows

    @staticmethod
    def _instrument_rows(frame: pd.DataFrame, source: str) -> list[dict[str, Any]]:
        fetched_at = datetime.now(UTC)
        rows = []
        for record in frame.to_dict(orient="records"):
            instrument_key = _clean_string(record.get("instrument_key"))
            if not instrument_key:
                continue
            rows.append(
                {
                    "source": source,
                    "instrument_key": instrument_key,
                    "exchange": _clean_string(record.get("exchange")),
                    "segment": _clean_string(record.get("segment")),
                    "asset_type": _clean_string(record.get("asset_type")),
                    "trading_symbol": _clean_string(record.get("trading_symbol")),
                    "name": _clean_string(record.get("name")),
                    "isin": _clean_string(record.get("isin")),
                    "lot_size": _nullable_int(record.get("lot_size")),
                    "tick_size": _nullable_float(record.get("tick_size")),
                    "expiry": _nullable_date(record.get("expiry")),
                    "strike": _nullable_float(record.get("strike")),
                    "option_type": _clean_string(record.get("option_type")),
                    "underlying_symbol": _clean_string(record.get("underlying_symbol")),
                    "underlying_key": _clean_string(record.get("underlying_key")),
                    "exchange_token": _clean_string(record.get("exchange_token")),
                    "active": True,
                    "fetched_at": fetched_at,
                    "raw": record.get("raw") if isinstance(record.get("raw"), dict) else {},
                }
            )
        return rows

    @staticmethod
    def _universe_member_rows(
        members: pd.DataFrame,
        universe_id: str,
        included_at: datetime,
    ) -> list[dict[str, Any]]:
        rows = []
        for record in members.to_dict(orient="records"):
            rows.append(
                {
                    "universe_id": universe_id,
                    "symbol": str(record["symbol"]).upper(),
                    "instrument_key": _clean_string(record.get("instrument_key")),
                    "rank": _nullable_int(record.get("rank")),
                    "avg_daily_volume": _nullable_float(record.get("avg_daily_volume")),
                    "avg_daily_turnover": _nullable_float(record.get("avg_daily_turnover")),
                    "trading_days": _nullable_int(record.get("trading_days")),
                    "zero_volume_ratio": _nullable_float(record.get("zero_volume_ratio")),
                    "start_date": _nullable_date(record.get("first_date")),
                    "end_date": _nullable_date(record.get("last_date")),
                    "included_at": included_at,
                }
            )
        return rows

    @staticmethod
    def _daily_ohlcv_rows(frame: pd.DataFrame, exchange: str, source: str) -> list[dict[str, Any]]:
        if frame.empty:
            return []

        fetched_at = datetime.now(UTC)
        rows = []
        for record in frame.to_dict(orient="records"):
            required_columns = ["Date", "Open", "High", "Low", "Close", "Volume"]
            if any(pd.isna(record.get(column)) for column in required_columns):
                continue
            row = {
                "instrument_key": str(record["InstrumentKey"]),
                "source": source,
                "date": _nullable_date(record["Date"]),
                "symbol": str(record["Symbol"]).upper(),
                "exchange": exchange,
                "open": float(record["Open"]),
                "high": float(record["High"]),
                "low": float(record["Low"]),
                "close": float(record["Close"]),
                "volume": int(record["Volume"]),
                "open_interest": _nullable_int(record.get("OpenInterest")),
                "fetched_at": fetched_at,
            }
            row["quality_status"] = _quality_status(row)
            rows.append(row)
        return rows

    @staticmethod
    def _audit_rows(
        audit: pd.DataFrame,
        dataset_name: str,
        source: str,
        interval: str,
    ) -> list[dict[str, Any]]:
        created_at = datetime.now(UTC)
        rows = []
        for record in audit.to_dict(orient="records"):
            rows.append(
                {
                    "audit_id": str(uuid4()),
                    "dataset_name": dataset_name,
                    "source": source,
                    "symbol": _clean_string(record.get("symbol")),
                    "instrument_key": _clean_string(record.get("instrument_key")),
                    "interval": interval,
                    "start_date": _nullable_date(record.get("start_date")),
                    "end_date": _nullable_date(record.get("end_date")),
                    "rows": int(record.get("rows") or 0),
                    "missing_dates": int(record.get("missing_dates") or 0),
                    "null_rows": int(record.get("null_ohlcv_rows") or 0),
                    "duplicate_rows": int(record.get("duplicate_date_rows") or 0),
                    "zero_volume_rows": int(record.get("zero_volume_rows") or 0),
                    "zero_or_negative_close_rows": int(
                        record.get("zero_or_negative_close_rows") or 0
                    ),
                    "coverage_ratio": _nullable_float(record.get("coverage_ratio")),
                    "status": str(record.get("status") or "unknown"),
                    "warnings_json": [],
                    "created_at": created_at,
                }
            )
        return rows

    @staticmethod
    def _daily_feature_rows(frame: pd.DataFrame) -> list[dict[str, Any]]:
        if frame.empty:
            return []

        computed_at = datetime.now(UTC)
        rows = []
        for record in frame.to_dict(orient="records"):
            instrument_key = _clean_string(record.get("instrument_key"))
            feature_date = _nullable_date(record.get("date"))
            feature_version = _clean_string(record.get("feature_version"))
            symbol = _clean_string(record.get("symbol"))
            if not instrument_key or feature_date is None or not feature_version or not symbol:
                continue

            row = {
                "instrument_key": instrument_key,
                "date": feature_date,
                "feature_version": feature_version,
                "source": _clean_string(record.get("source")) or "unknown",
                "symbol": symbol.upper(),
                "exchange": (_clean_string(record.get("exchange")) or "NSE").upper(),
                "open": _nullable_float(record.get("open")),
                "high": _nullable_float(record.get("high")),
                "low": _nullable_float(record.get("low")),
                "close": _nullable_float(record.get("close")),
                "volume": _nullable_int(record.get("volume")),
                "open_interest": _nullable_int(record.get("open_interest")),
                "computed_at": computed_at,
                "quality_status": _clean_string(record.get("quality_status")) or "unknown",
            }
            for column in FEATURE_COLUMNS_V1_0:
                row[column] = _nullable_float(record.get(column))
            rows.append(row)
        return rows

    @staticmethod
    def _feature_audit_rows(
        audit: pd.DataFrame,
        dataset_name: str,
        feature_version: str,
        run_id: str | None,
    ) -> list[dict[str, Any]]:
        created_at = datetime.now(UTC)
        rows = []
        for record in audit.to_dict(orient="records"):
            feature = _clean_string(record.get("feature"))
            if not feature:
                continue
            rows.append(
                {
                    "audit_id": str(uuid4()),
                    "run_id": run_id,
                    "dataset_name": dataset_name,
                    "feature_version": feature_version,
                    "feature": feature,
                    "null_count": int(record.get("null_count") or 0),
                    "null_pct": float(record.get("null_pct") or 0.0),
                    "inf_count": int(record.get("inf_count") or 0),
                    "created_at": created_at,
                }
            )
        return rows

    @staticmethod
    def _daily_target_rows(frame: pd.DataFrame) -> list[dict[str, Any]]:
        if frame.empty:
            return []

        computed_at = datetime.now(UTC)
        rows = []
        for record in frame.to_dict(orient="records"):
            instrument_key = _clean_string(record.get("instrument_key"))
            target_date = _nullable_date(record.get("date"))
            target_version = _clean_string(record.get("target_version"))
            symbol = _clean_string(record.get("symbol"))
            if not instrument_key or target_date is None or not target_version or not symbol:
                continue

            row = {
                "instrument_key": instrument_key,
                "date": target_date,
                "target_version": target_version,
                "source": _clean_string(record.get("source")) or "unknown",
                "symbol": symbol.upper(),
                "exchange": (_clean_string(record.get("exchange")) or "NSE").upper(),
                "computed_at": computed_at,
                "quality_status": _clean_string(record.get("quality_status")) or "unknown",
            }
            for column in DAILY_FORWARD_TARGET_COLUMNS_V1_0:
                if column == "top_quantile_forward_return_20d":
                    row[column] = _nullable_bool(record.get(column))
                else:
                    row[column] = _nullable_float(record.get(column))
            rows.append(row)
        return rows

    @staticmethod
    def _target_audit_rows(
        audit: pd.DataFrame,
        dataset_name: str,
        target_version: str,
        run_id: str | None,
    ) -> list[dict[str, Any]]:
        created_at = datetime.now(UTC)
        rows = []
        for record in audit.to_dict(orient="records"):
            target = _clean_string(record.get("target"))
            if not target:
                continue
            rows.append(
                {
                    "audit_id": str(uuid4()),
                    "run_id": run_id,
                    "dataset_name": dataset_name,
                    "target_version": target_version,
                    "target": target,
                    "null_count": int(record.get("null_count") or 0),
                    "null_pct": float(record.get("null_pct") or 0.0),
                    "inf_count": int(record.get("inf_count") or 0),
                    "created_at": created_at,
                }
            )
        return rows

    @staticmethod
    def _stock_coverage_rows(
        coverage: pd.DataFrame,
        run_id: str,
        source: str,
        exchange: str,
        created_at: datetime,
    ) -> list[dict[str, Any]]:
        rows = []
        for record in coverage.to_dict(orient="records"):
            instrument_key = _clean_string(record.get("instrument_key"))
            symbol = _clean_string(record.get("symbol"))
            window_months = _nullable_int(record.get("window_months"))
            if not instrument_key or not symbol or window_months is None:
                continue
            rows.append(
                {
                    "run_id": run_id,
                    "window_months": window_months,
                    "instrument_key": instrument_key,
                    "symbol": symbol.upper(),
                    "exchange": exchange.upper(),
                    "source": source,
                    "window_start": _nullable_date(record.get("window_start")),
                    "window_end": _nullable_date(record.get("window_end")),
                    "first_date": _nullable_date(record.get("first_date")),
                    "last_date": _nullable_date(record.get("last_date")),
                    "expected_date_count": int(record.get("expected_date_count") or 0),
                    "observed_date_count": int(record.get("observed_date_count") or 0),
                    "missing_date_count": int(record.get("missing_date_count") or 0),
                    "coverage_pct": float(record.get("coverage_pct") or 0.0),
                    "has_latest_expected_date": bool(record.get("has_latest_expected_date")),
                    "latest_date_lag_days": _nullable_int(record.get("latest_date_lag_days")),
                    "coverage_status": str(record.get("coverage_status") or "unknown"),
                    "created_at": created_at,
                }
            )
        return rows

    @staticmethod
    def _daily_ohlcv_fetch_coverage_rows(
        coverage: pd.DataFrame,
        run_id: str,
        source: str,
        exchange: str,
        created_at: datetime,
    ) -> list[dict[str, Any]]:
        rows = []
        for record in coverage.to_dict(orient="records"):
            instrument_key = _clean_string(record.get("instrument_key"))
            symbol = _clean_string(record.get("symbol"))
            fetch_end = _nullable_date(record.get("fetch_end"))
            if not instrument_key or not symbol or fetch_end is None:
                continue
            rows.append(
                {
                    "run_id": run_id,
                    "instrument_key": instrument_key,
                    "symbol": symbol.upper(),
                    "source": source,
                    "exchange": exchange.upper(),
                    "latest_stored_date": _nullable_date(record.get("latest_stored_date")),
                    "fetch_start": _nullable_date(record.get("fetch_start")),
                    "fetch_end": fetch_end,
                    "should_fetch": bool(record.get("should_fetch")),
                    "status": str(record.get("fetch_status") or record.get("status") or "unknown"),
                    "rows_fetched": int(record.get("rows_fetched") or 0),
                    "skip_reason": _clean_string(record.get("skip_reason")),
                    "error_message": _clean_string(record.get("error")),
                    "created_at": created_at,
                }
            )
        return rows

    @staticmethod
    def _provider_request_log_rows(logs: Iterable[Mapping[str, Any]]) -> list[dict[str, Any]]:
        rows = []
        for record in logs:
            provider = _clean_string(record.get("provider"))
            endpoint_group = _clean_string(record.get("endpoint_group"))
            request_key = _clean_string(record.get("request_key"))
            status = _clean_string(record.get("status"))
            if not provider or not endpoint_group or not request_key or not status:
                continue
            rows.append(
                {
                    "id": _clean_string(record.get("id")) or str(uuid4()),
                    "run_id": _clean_string(record.get("run_id")),
                    "provider": provider.lower(),
                    "endpoint_group": endpoint_group,
                    "request_key": request_key,
                    "instrument_key": _clean_string(record.get("instrument_key")),
                    "symbol": _clean_string(record.get("symbol")),
                    "interval": _clean_string(record.get("interval")),
                    "window_start": _nullable_date(record.get("window_start")),
                    "window_end": _nullable_date(record.get("window_end")),
                    "status_code": _nullable_int(record.get("status_code")),
                    "status": status,
                    "error_message": _clean_string(record.get("error_message")),
                    "retry_count": int(record.get("retry_count") or 0),
                    "rate_limited": bool(record.get("rate_limited")),
                    "wait_seconds": float(record.get("wait_seconds") or 0.0),
                    "duration_ms": float(record.get("duration_ms") or 0.0),
                    "created_at": _as_utc(record.get("created_at") or datetime.now(UTC)),
                }
            )
        return rows


def make_timescale_store(database_url: str) -> TimescaleStore:
    return TimescaleStore(database_url)


def _quality_status(row: Mapping[str, Any]) -> str:
    if row["close"] <= 0 or row["high"] < row["low"]:
        return "suspicious"
    if row["volume"] < 0:
        return "suspicious"
    return "ok"


def _as_utc(value: datetime) -> datetime:
    if value.tzinfo is None:
        return value.replace(tzinfo=UTC)
    return value.astimezone(UTC)


def _chunks(rows: list[dict[str, Any]], size: int) -> Iterable[list[dict[str, Any]]]:
    for start in range(0, len(rows), size):
        yield rows[start : start + size]


def _clean_string(value: Any) -> str | None:
    if value is None or pd.isna(value):
        return None
    text = str(value).strip()
    return text or None


def _nullable_float(value: Any) -> float | None:
    if value is None or pd.isna(value):
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def _nullable_int(value: Any) -> int | None:
    if value is None or pd.isna(value):
        return None
    try:
        return int(float(value))
    except (TypeError, ValueError):
        return None


def _nullable_bool(value: Any) -> bool | None:
    if value is None or pd.isna(value):
        return None
    if isinstance(value, bool):
        return value
    if isinstance(value, (int, float)):
        return bool(value)
    text = str(value).strip().lower()
    if text in {"true", "t", "1", "yes", "y"}:
        return True
    if text in {"false", "f", "0", "no", "n"}:
        return False
    return None


def _nullable_date(value: Any) -> date | None:
    if value is None or pd.isna(value):
        return None
    if isinstance(value, datetime):
        return value.date()
    if isinstance(value, date):
        return value
    try:
        return date.fromisoformat(str(value)[:10])
    except ValueError:
        return None


def _feed_health_can_fetch(health: Mapping[str, Any], now: datetime) -> bool:
    status = health.get("status")
    next_retry_at = health.get("next_retry_at")
    if next_retry_at and _as_utc(next_retry_at) > now:
        return False
    return status not in {"quarantined"}
