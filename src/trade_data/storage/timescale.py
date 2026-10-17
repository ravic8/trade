from __future__ import annotations

from collections.abc import Iterable, Mapping, Sequence
from datetime import UTC, date, datetime, timedelta
from typing import TYPE_CHECKING, Any, TypeVar
from uuid import NAMESPACE_URL, UUID, uuid4, uuid5

import pandas as pd
from sqlalchemy import (
    JSON,
    BigInteger,
    Boolean,
    Column,
    Date,
    DateTime,
    Float,
    ForeignKey,
    Index,
    MetaData,
    String,
    Table,
    Text,
    UniqueConstraint,
    case,
    create_engine,
    func,
    select,
    text,
)
from sqlalchemy.dialects.postgresql import insert

from trade_data.schemas import Symbol
from trade_data.storage.ingestion_runs import IngestionRunRepository
from trade_data.storage.legacy_schema import (
    DAILY_OPPORTUNITY_TARGET_COLUMNS_V1_0,
    FEATURE_COLUMNS_V1_0,
)

if TYPE_CHECKING:
    from trade_data.data.daily_work import DailyWorkItem
    from trade_data.universe.persisted import UniverseReconciliationPlan

metadata = MetaData()
_ChunkItem = TypeVar("_ChunkItem")

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
    Column("canonical_instrument_id", String),
    Column("first_seen_at", DateTime(timezone=True)),
    Column("last_seen_at", DateTime(timezone=True)),
    Column("inactive_at", DateTime(timezone=True)),
    Column("inactive_reason", String),
    Column(
        "consecutive_missing_refreshes",
        BigInteger,
        nullable=False,
        default=0,
        server_default="0",
    ),
    Column("last_universe_snapshot_id", String),
    Column("source_identity", String),
    Column("provider_instrument_key", String),
    Column("listing_status", String, nullable=False, default="active", server_default="active"),
    Column("listing_status_reason", String),
    Column("listing_status_effective_at", DateTime(timezone=True)),
    Column(
        "pipeline_eligibility",
        String,
        nullable=False,
        default="incremental",
        server_default="incremental",
    ),
    Column("provider_status", String, nullable=False, default="unknown", server_default="unknown"),
    Column("provider_status_reason", String),
    Column("provider_status_updated_at", DateTime(timezone=True)),
    Column("instrument_type", String, nullable=False, default="unknown", server_default="unknown"),
    Column(
        "reconciliation_status",
        String,
        nullable=False,
        default="not_required",
        server_default="not_required",
    ),
    Column("reconciliation_reason", String),
    Column("official_sector", String),
    Column("official_security_type", String),
    Column("official_source_updated_at", DateTime(timezone=True)),
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

ohlcv_intraday_table = Table(
    "ohlcv_intraday",
    metadata,
    Column("instrument_key", String, primary_key=True),
    Column("source", String, primary_key=True),
    Column("interval", String, primary_key=True),
    Column("ts", DateTime(timezone=True), primary_key=True),
    Column("symbol", String, nullable=False),
    Column("exchange", String, nullable=False),
    Column("asset_class", String, nullable=False),
    Column("open", Float, nullable=False),
    Column("high", Float, nullable=False),
    Column("low", Float, nullable=False),
    Column("close", Float, nullable=False),
    Column("volume", Float, nullable=False),
    Column("fetched_at", DateTime(timezone=True), nullable=False),
    Column("quality_status", String, nullable=False),
)

price_adjustments_daily_table = Table(
    "price_adjustments_daily",
    metadata,
    Column("instrument_key", String, primary_key=True),
    Column("source", String, primary_key=True),
    Column("date", Date, primary_key=True),
    Column("symbol", String, nullable=False),
    Column("exchange", String, nullable=False),
    Column("raw_close", Float, nullable=False),
    Column("adjusted_close", Float, nullable=False),
    Column("adjustment_factor", Float, nullable=False),
    Column("fetched_at", DateTime(timezone=True), nullable=False),
)

corporate_actions_table = Table(
    "corporate_actions",
    metadata,
    Column("source", String, primary_key=True),
    Column("instrument_key", String, primary_key=True),
    Column("action_date", Date, primary_key=True),
    Column("action_type", String, primary_key=True),
    Column("symbol", String, nullable=False),
    Column("exchange", String, nullable=False),
    Column("value", Float),
    Column("currency", String),
    Column("raw", JSON, nullable=False, default=dict),
    Column("fetched_at", DateTime(timezone=True), nullable=False),
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

opportunity_targets_daily_table = Table(
    "opportunity_targets_daily",
    metadata,
    Column("instrument_key", String, primary_key=True),
    Column("source", String, primary_key=True),
    Column("date", Date, primary_key=True),
    Column("target_version", String, primary_key=True),
    Column("symbol", String, nullable=False),
    Column("exchange", String, nullable=False),
    Column("computed_at", DateTime(timezone=True), nullable=False),
    Column("quality_status", String, nullable=False),
    Column("open", Float, nullable=False),
    Column("high", Float, nullable=False),
    Column("low", Float, nullable=False),
    Column("close", Float, nullable=False),
    Column("previous_close", Float),
    Column("volume", BigInteger, nullable=False),
    Column("open_interest", BigInteger),
    *(Column(column, Float) for column in DAILY_OPPORTUNITY_TARGET_COLUMNS_V1_0),
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

exchange_sessions_table = Table(
    "exchange_sessions",
    metadata,
    Column("exchange", String, primary_key=True),
    Column("session_date", Date, primary_key=True),
    Column("is_trading_day", Boolean, nullable=False),
    Column("market_open_utc", DateTime(timezone=True)),
    Column("market_close_utc", DateTime(timezone=True)),
    Column("is_early_close", Boolean, nullable=False, default=False, server_default="false"),
    Column("source_url", String, nullable=False),
    Column("calendar_version", String, nullable=False),
    Column("validation_status", String, nullable=False),
    Column("generated_at", DateTime(timezone=True), nullable=False),
)

universe_snapshots_table = Table(
    "universe_snapshots",
    metadata,
    Column("snapshot_id", String, primary_key=True),
    Column("exchange", String, nullable=False),
    Column("source", String, nullable=False),
    Column("status", String, nullable=False),
    Column("fetched_at", DateTime(timezone=True), nullable=False),
    Column("symbol_count", BigInteger, nullable=False, default=0, server_default="0"),
    Column("validation_json", JSON, nullable=False, default=dict, server_default="{}"),
    Column("error_message", Text),
    Column("created_at", DateTime(timezone=True), nullable=False),
)

universe_snapshot_members_table = Table(
    "universe_snapshot_members",
    metadata,
    Column("snapshot_id", String, primary_key=True),
    Column("canonical_instrument_id", String, primary_key=True),
    Column("exchange_symbol", String, nullable=False),
    Column("provider_symbol", String),
    Column("name", String),
    Column("raw_metadata", JSON, nullable=False, default=dict, server_default="{}"),
)

instrument_aliases_table = Table(
    "instrument_aliases",
    metadata,
    Column("alias_id", String, primary_key=True),
    Column("canonical_instrument_id", String, nullable=False),
    Column("provider", String, nullable=False),
    Column("provider_symbol", String, nullable=False),
    Column("valid_from", DateTime(timezone=True), nullable=False),
    Column("valid_to", DateTime(timezone=True)),
    Column("is_current", Boolean, nullable=False, default=True, server_default="true"),
    UniqueConstraint(
        "canonical_instrument_id",
        "provider",
        "provider_symbol",
        "valid_from",
        name="uq_instrument_alias_identity",
    ),
)

symbol_lifecycle_events_table = Table(
    "symbol_lifecycle_events",
    metadata,
    Column("event_id", String, primary_key=True),
    Column("canonical_instrument_id", String, nullable=False),
    Column("exchange", String, nullable=False),
    Column("event_type", String, nullable=False),
    Column("old_value", JSON),
    Column("new_value", JSON),
    Column("snapshot_id", String),
    Column("created_at", DateTime(timezone=True), nullable=False),
)

pipeline_work_items_table = Table(
    "pipeline_work_items",
    metadata,
    Column("work_item_id", String, primary_key=True),
    Column("idempotency_key", String, nullable=False),
    Column("work_type", String, nullable=False),
    Column("provider", String, nullable=False),
    Column("exchange", String, nullable=False),
    Column("canonical_instrument_id", String, nullable=False),
    Column("provider_symbol", String, nullable=False),
    Column("interval", String, nullable=False),
    Column("window_start", Date, nullable=False),
    Column("window_end", Date, nullable=False),
    Column("priority", BigInteger, nullable=False, default=100, server_default="100"),
    Column("status", String, nullable=False),
    Column("attempt_count", BigInteger, nullable=False, default=0, server_default="0"),
    Column("max_attempts", BigInteger, nullable=False, default=9, server_default="9"),
    Column("next_attempt_at", DateTime(timezone=True)),
    Column("locked_by", String),
    Column("locked_at", DateTime(timezone=True)),
    Column("run_id", String),
    Column("parent_work_item_id", String),
    Column("last_status_code", BigInteger),
    Column("last_error_code", String),
    Column("last_error_message", Text),
    Column("created_at", DateTime(timezone=True), nullable=False),
    Column("updated_at", DateTime(timezone=True), nullable=False),
    Column("completed_at", DateTime(timezone=True)),
    UniqueConstraint("idempotency_key", name="uq_pipeline_work_items_idempotency_key"),
)

provider_daily_history_evidence_table = Table(
    "provider_daily_history_evidence",
    metadata,
    Column("evidence_id", String, primary_key=True),
    Column("provider", String, nullable=False),
    Column("instrument_key", String, nullable=False),
    Column("exchange", String, nullable=False),
    Column("canonical_instrument_id", String, nullable=False),
    Column("provider_symbol", String, nullable=False),
    Column("interval", String, nullable=False),
    Column("work_type", String, nullable=False),
    Column("requested_start", Date, nullable=False),
    Column("requested_end", Date, nullable=False),
    Column("coverage_start", Date, nullable=False),
    Column("coverage_end", Date, nullable=False),
    Column("first_available_date", Date, nullable=False),
    Column("last_available_date", Date, nullable=False),
    Column("expected_rows", BigInteger, nullable=False),
    Column("observed_rows", BigInteger, nullable=False),
    Column("missing_rows", BigInteger, nullable=False),
    Column("coverage_ratio", Float, nullable=False),
    Column("classification", String, nullable=False),
    Column("quarantine_reason", String),
    Column("status", String, nullable=False, default="active", server_default="active"),
    Column("evidence_run_id", String, nullable=False),
    Column("verified_at", DateTime(timezone=True), nullable=False),
    Column("created_at", DateTime(timezone=True), nullable=False),
    Column("updated_at", DateTime(timezone=True), nullable=False),
)

adaptive_rate_state_table = Table(
    "adaptive_rate_state",
    metadata,
    Column("provider", String, primary_key=True),
    Column("current_rpm", BigInteger, nullable=False),
    Column("last_safe_rpm", BigInteger),
    Column("minimum_rpm", BigInteger, nullable=False),
    Column("maximum_rpm", BigInteger, nullable=False),
    Column("current_concurrency", BigInteger, nullable=False),
    Column(
        "consecutive_healthy_windows",
        BigInteger,
        nullable=False,
        default=0,
        server_default="0",
    ),
    Column("circuit_state", String, nullable=False, default="closed", server_default="closed"),
    Column("cooldown_until", DateTime(timezone=True)),
    Column("last_429_at", DateTime(timezone=True)),
    Column("recent_error_rate", Float, nullable=False, default=0.0, server_default="0"),
    Column("latency_baseline_ms", Float),
    Column("updated_at", DateTime(timezone=True), nullable=False),
)

daily_coverage_summary_table = Table(
    "daily_coverage_summary",
    metadata,
    Column("source", String, primary_key=True),
    Column("exchange", String, primary_key=True),
    Column("instrument_key", String, primary_key=True),
    Column("interval", String, primary_key=True),
    Column("as_of_date", Date, primary_key=True),
    Column("symbol", String, nullable=False),
    Column("first_expected_date", Date),
    Column("first_stored_date", Date),
    Column("latest_expected_date", Date),
    Column("latest_stored_date", Date),
    Column("expected_rows", BigInteger, nullable=False, default=0, server_default="0"),
    Column("stored_rows", BigInteger, nullable=False, default=0, server_default="0"),
    Column("missing_rows", BigInteger, nullable=False, default=0, server_default="0"),
    Column("coverage_pct", Float, nullable=False, default=0.0, server_default="0"),
    Column("coverage_status", String, nullable=False),
    Column("freshness_status", String, nullable=False),
    Column("last_successful_run", String),
    Column("last_fetch_status", String),
    Column("next_retry_at", DateTime(timezone=True)),
    Column("attempt_count", BigInteger, nullable=False, default=0, server_default="0"),
    Column("lifecycle_status", String, nullable=False),
    Column("updated_at", DateTime(timezone=True), nullable=False),
)

bigquery_sync_runs_table = Table(
    "bigquery_sync_runs",
    metadata,
    Column("run_id", String, primary_key=True),
    Column("trigger", String, nullable=False),
    Column("status", String, nullable=False),
    Column("project_id", String, nullable=False),
    Column("dataset", String, nullable=False),
    Column("reporting_dataset", String),
    Column("authenticated_principal", String),
    Column("location", String, nullable=False),
    Column("exchange", String),
    Column("year", BigInteger),
    Column("entities", JSON, nullable=False, default=list),
    Column("started_at", DateTime(timezone=True), nullable=False),
    Column("finished_at", DateTime(timezone=True)),
    Column("source_row_count", BigInteger, nullable=False, default=0, server_default="0"),
    Column("destination_row_count", BigInteger, nullable=False, default=0, server_default="0"),
    Column("count_difference", BigInteger, nullable=False, default=0, server_default="0"),
    Column("inserted_rows", BigInteger, nullable=False, default=0, server_default="0"),
    Column("updated_rows", BigInteger, nullable=False, default=0, server_default="0"),
    Column("rejected_rows", BigInteger, nullable=False, default=0, server_default="0"),
    Column("staging_row_count", BigInteger, nullable=False, default=0, server_default="0"),
    Column("merged_row_count", BigInteger, nullable=False, default=0, server_default="0"),
    Column(
        "duplicate_business_key_count",
        BigInteger,
        nullable=False,
        default=0,
        server_default="0",
    ),
    Column("retry_count", BigInteger, nullable=False, default=0, server_default="0"),
    Column("duration_seconds", Float),
    Column("source_watermark", String),
    Column("destination_watermark", String),
    Column("last_successful_sync_at", DateTime(timezone=True)),
    Column("bigquery_job_id", String),
    Column("schema_drift", JSON, nullable=False, default=dict, server_default="{}"),
    Column("error_details", Text),
    Column("created_at", DateTime(timezone=True), nullable=False),
    Column("updated_at", DateTime(timezone=True), nullable=False),
)

bigquery_sync_partitions_table = Table(
    "bigquery_sync_partitions",
    metadata,
    Column("partition_id", String, primary_key=True),
    Column("run_id", String, ForeignKey("bigquery_sync_runs.run_id"), nullable=False),
    Column("entity", String, nullable=False),
    Column("exchange", String),
    Column("partition_start", Date),
    Column("partition_end", Date),
    Column("status", String, nullable=False),
    Column("attempt_count", BigInteger, nullable=False, default=0, server_default="0"),
    Column("source_row_count", BigInteger, nullable=False, default=0, server_default="0"),
    Column("destination_row_count", BigInteger, nullable=False, default=0, server_default="0"),
    Column("count_difference", BigInteger, nullable=False, default=0, server_default="0"),
    Column("inserted_rows", BigInteger, nullable=False, default=0, server_default="0"),
    Column("updated_rows", BigInteger, nullable=False, default=0, server_default="0"),
    Column("rejected_rows", BigInteger, nullable=False, default=0, server_default="0"),
    Column("staging_row_count", BigInteger, nullable=False, default=0, server_default="0"),
    Column("merged_row_count", BigInteger, nullable=False, default=0, server_default="0"),
    Column(
        "duplicate_business_key_count",
        BigInteger,
        nullable=False,
        default=0,
        server_default="0",
    ),
    Column("source_min_date", Date),
    Column("source_max_date", Date),
    Column("destination_min_date", Date),
    Column("destination_max_date", Date),
    Column("source_watermark", String),
    Column("destination_watermark", String),
    Column("bigquery_job_id", String),
    Column("duration_seconds", Float),
    Column("schema_drift", JSON, nullable=False, default=dict, server_default="{}"),
    Column("error_details", Text),
    Column("created_at", DateTime(timezone=True), nullable=False),
    Column("updated_at", DateTime(timezone=True), nullable=False),
    Column("completed_at", DateTime(timezone=True)),
    UniqueConstraint(
        "run_id",
        "entity",
        "exchange",
        "partition_start",
        "partition_end",
        name="uq_bigquery_sync_partition_scope",
    ),
)

Index(
    "idx_exchange_sessions_open_date",
    exchange_sessions_table.c.exchange,
    exchange_sessions_table.c.is_trading_day,
    exchange_sessions_table.c.session_date,
)
Index(
    "idx_opportunity_targets_exchange_date",
    opportunity_targets_daily_table.c.exchange,
    opportunity_targets_daily_table.c.date,
)
Index(
    "idx_opportunity_targets_symbol_date",
    opportunity_targets_daily_table.c.symbol,
    opportunity_targets_daily_table.c.date,
)
Index(
    "idx_universe_snapshots_exchange_fetched",
    universe_snapshots_table.c.exchange,
    universe_snapshots_table.c.fetched_at,
)
Index(
    "idx_instrument_aliases_current",
    instrument_aliases_table.c.canonical_instrument_id,
    instrument_aliases_table.c.provider,
    instrument_aliases_table.c.is_current,
)
Index(
    "idx_symbol_lifecycle_events_exchange_created",
    symbol_lifecycle_events_table.c.exchange,
    symbol_lifecycle_events_table.c.created_at,
)
Index(
    "idx_pipeline_work_items_claim",
    pipeline_work_items_table.c.status,
    pipeline_work_items_table.c.next_attempt_at,
    pipeline_work_items_table.c.priority,
    pipeline_work_items_table.c.created_at,
)
Index(
    "idx_provider_daily_history_exchange_classification",
    provider_daily_history_evidence_table.c.exchange,
    provider_daily_history_evidence_table.c.classification,
    provider_daily_history_evidence_table.c.status,
)
Index(
    "idx_provider_daily_history_instrument",
    provider_daily_history_evidence_table.c.provider,
    provider_daily_history_evidence_table.c.instrument_key,
    provider_daily_history_evidence_table.c.interval,
)
Index(
    "idx_daily_coverage_summary_exchange_status",
    daily_coverage_summary_table.c.exchange,
    daily_coverage_summary_table.c.coverage_status,
    daily_coverage_summary_table.c.as_of_date,
)
Index("idx_bigquery_sync_runs_started", bigquery_sync_runs_table.c.started_at)
Index(
    "idx_bigquery_sync_partitions_status",
    bigquery_sync_partitions_table.c.status,
    bigquery_sync_partitions_table.c.updated_at,
)


class TimescaleStore:
    def __init__(self, database_url: str) -> None:
        self.engine = create_engine(
            database_url,
            pool_pre_ping=True,
            hide_parameters=True,
        )

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
                    "CREATE INDEX IF NOT EXISTS idx_ohlcv_daily_source_exchange_date "
                    "ON ohlcv_daily (source, exchange, date)"
                )
            )
            connection.execute(
                text(
                    "CREATE INDEX IF NOT EXISTS idx_symbols_exchange_provider_instrument "
                    "ON symbols (exchange, provider_instrument_key)"
                )
            )
            connection.execute(
                text(
                    "CREATE INDEX IF NOT EXISTS idx_ohlcv_intraday_symbol_ts "
                    "ON ohlcv_intraday (symbol, interval, ts DESC)"
                )
            )
            connection.execute(
                text(
                    "CREATE INDEX IF NOT EXISTS idx_ohlcv_intraday_exchange_ts "
                    "ON ohlcv_intraday (exchange, asset_class, interval, ts DESC)"
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
                    "CREATE INDEX IF NOT EXISTS idx_opportunity_targets_exchange_date "
                    "ON opportunity_targets_daily (exchange, date DESC)"
                )
            )
            connection.execute(
                text(
                    "CREATE INDEX IF NOT EXISTS idx_opportunity_targets_symbol_date "
                    "ON opportunity_targets_daily (symbol, date DESC)"
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
                    "'ohlcv_intraday', 'ts', if_not_exists => TRUE, migrate_data => TRUE)"
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
            connection.execute(
                text(
                    "SELECT create_hypertable("
                    "'opportunity_targets_daily', 'date', "
                    "if_not_exists => TRUE, migrate_data => TRUE)"
                )
            )

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

    def adaptive_rate_state(self, provider: str) -> dict[str, Any] | None:
        query = adaptive_rate_state_table.select().where(
            adaptive_rate_state_table.c.provider == provider.strip().lower()
        )
        with self.engine.connect() as connection:
            row = connection.execute(query).mappings().first()
        return dict(row) if row is not None else None

    def upsert_adaptive_rate_state(self, state: Mapping[str, Any]) -> int:
        provider = _clean_string(state.get("provider"))
        if not provider:
            raise ValueError("Adaptive-rate state requires a provider.")
        values = {
            "provider": provider.lower(),
            "current_rpm": int(state["current_rpm"]),
            "last_safe_rpm": _nullable_int(state.get("last_safe_rpm")),
            "minimum_rpm": int(state["minimum_rpm"]),
            "maximum_rpm": int(state["maximum_rpm"]),
            "current_concurrency": int(state["current_concurrency"]),
            "consecutive_healthy_windows": int(state.get("consecutive_healthy_windows") or 0),
            "circuit_state": _clean_string(state.get("circuit_state")) or "closed",
            "cooldown_until": (
                _as_utc(state["cooldown_until"])
                if state.get("cooldown_until") is not None
                else None
            ),
            "last_429_at": (
                _as_utc(state["last_429_at"]) if state.get("last_429_at") is not None else None
            ),
            "recent_error_rate": float(state.get("recent_error_rate") or 0.0),
            "latency_baseline_ms": (
                float(state["latency_baseline_ms"])
                if state.get("latency_baseline_ms") is not None
                else None
            ),
            "updated_at": _as_utc(state.get("updated_at") or datetime.now(UTC)),
        }
        statement = insert(adaptive_rate_state_table).values(values)
        with self.engine.begin() as connection:
            connection.execute(
                statement.on_conflict_do_update(
                    index_elements=[adaptive_rate_state_table.c.provider],
                    set_={
                        column: getattr(statement.excluded, column)
                        for column in values
                        if column != "provider"
                    },
                )
            )
        return 1

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
        rows = _deduplicate_rows(
            [
                {
                    **symbol.model_dump(),
                    "is_active": True,
                    "fetched_at": fetched,
                }
                for symbol in symbols
            ],
            ("symbol", "exchange"),
        )
        if not rows:
            return 0

        statement = insert(symbols_table).values(rows)
        update_columns = {
            column.name: getattr(statement.excluded, column.name)
            for column in symbols_table.columns
            if column.name
            not in {
                "symbol",
                "exchange",
                "provider_status",
                "provider_status_reason",
                "provider_status_updated_at",
            }
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
                    source_identity=row["source_identity"],
                    listing_status=row["listing_status"],
                    listing_status_reason=row["listing_status_reason"],
                    listing_status_effective_at=row["listing_status_effective_at"],
                    pipeline_eligibility=row["pipeline_eligibility"],
                    instrument_type=row["instrument_type"],
                    reconciliation_status=row["reconciliation_status"],
                    reconciliation_reason=row["reconciliation_reason"],
                    official_sector=row["official_sector"],
                    official_security_type=row["official_security_type"],
                    official_source_updated_at=row["official_source_updated_at"],
                )
                for row in rows
            ]

    def latest_accepted_universe_snapshot(self, exchange: str) -> dict[str, Any] | None:
        query = (
            universe_snapshots_table.select()
            .where(universe_snapshots_table.c.exchange == exchange.upper())
            .where(universe_snapshots_table.c.status == "accepted")
            .order_by(
                universe_snapshots_table.c.fetched_at.desc(),
                universe_snapshots_table.c.snapshot_id.desc(),
            )
            .limit(1)
        )
        with self.engine.begin() as connection:
            row = connection.execute(query).mappings().first()
        return dict(row) if row is not None else None

    def universe_symbol_state(self, exchange: str) -> list[dict[str, Any]]:
        query = (
            symbols_table.select()
            .where(symbols_table.c.exchange == exchange.upper())
            .order_by(symbols_table.c.symbol)
        )
        with self.engine.begin() as connection:
            return [dict(row) for row in connection.execute(query).mappings()]

    def persisted_universe_symbols(self, exchange: str) -> list[Symbol]:
        latest = self.latest_accepted_universe_snapshot(exchange)
        if latest is None:
            return []
        query = (
            select(
                symbols_table.c.symbol,
                symbols_table.c.exchange,
                symbols_table.c.yahoo_symbol,
                symbols_table.c.name,
                symbols_table.c.currency,
                symbols_table.c.source,
                symbols_table.c.source_url,
                symbols_table.c.source_identity,
                symbols_table.c.listing_status,
                symbols_table.c.listing_status_reason,
                symbols_table.c.listing_status_effective_at,
                symbols_table.c.pipeline_eligibility,
            )
            .select_from(
                universe_snapshot_members_table.join(
                    symbols_table,
                    (universe_snapshot_members_table.c.exchange_symbol == symbols_table.c.symbol)
                    & (symbols_table.c.exchange == exchange.upper()),
                )
            )
            .where(universe_snapshot_members_table.c.snapshot_id == str(latest["snapshot_id"]))
            .where(symbols_table.c.is_active.is_(True))
            .order_by(symbols_table.c.symbol)
        )
        with self.engine.begin() as connection:
            rows = connection.execute(query).mappings()
            return [Symbol(**dict(row)) for row in rows]

    def persisted_universe_instruments(self, exchange: str) -> list[dict[str, Any]]:
        """Return the active members of the latest accepted universe snapshot."""
        exchange_code = exchange.upper()
        latest = self.latest_accepted_universe_snapshot(exchange_code)
        if latest is None:
            return []
        query = (
            select(
                symbols_table.c.canonical_instrument_id,
                symbols_table.c.symbol,
                symbols_table.c.yahoo_symbol.label("provider_symbol"),
                symbols_table.c.name,
                symbols_table.c.provider_instrument_key.label("instrument_key"),
                symbols_table.c.instrument_type,
            )
            .select_from(
                universe_snapshot_members_table.join(
                    symbols_table,
                    (
                        universe_snapshot_members_table.c.canonical_instrument_id
                        == symbols_table.c.canonical_instrument_id
                    )
                    & (symbols_table.c.exchange == exchange_code),
                )
            )
            .where(universe_snapshot_members_table.c.snapshot_id == str(latest["snapshot_id"]))
            .where(symbols_table.c.is_active.is_(True))
            .where(symbols_table.c.pipeline_eligibility != "none")
            .where(symbols_table.c.provider_instrument_key.is_not(None))
            .order_by(symbols_table.c.symbol)
        )
        with self.engine.begin() as connection:
            return [dict(row) for row in connection.execute(query).mappings()]

    def record_universe_snapshot(
        self,
        *,
        snapshot_id: str,
        exchange: str,
        source: str,
        status: str,
        fetched_at: datetime,
        symbol_count: int,
        validation_json: Mapping[str, Any],
        error_message: str | None,
    ) -> None:
        statement = insert(universe_snapshots_table).values(
            snapshot_id=snapshot_id,
            exchange=exchange.upper(),
            source=source,
            status=status,
            fetched_at=fetched_at,
            symbol_count=symbol_count,
            validation_json=dict(validation_json),
            error_message=error_message,
            created_at=datetime.now(UTC),
        )
        update_columns = {
            column.name: getattr(statement.excluded, column.name)
            for column in universe_snapshots_table.columns
            if column.name != "snapshot_id"
        }
        with self.engine.begin() as connection:
            connection.execute(
                statement.on_conflict_do_update(
                    index_elements=["snapshot_id"],
                    set_=update_columns,
                )
            )

    def persist_accepted_universe_snapshot(
        self,
        *,
        plan: UniverseReconciliationPlan,
        source: str,
        validation_json: Mapping[str, Any],
    ) -> None:
        snapshot_statement = insert(universe_snapshots_table).values(
            snapshot_id=plan.snapshot_id,
            exchange=plan.exchange,
            source=source,
            status="accepted",
            fetched_at=plan.fetched_at,
            symbol_count=len(plan.members),
            validation_json=dict(validation_json),
            error_message=None,
            created_at=datetime.now(UTC),
        )
        snapshot_updates = {
            column.name: getattr(snapshot_statement.excluded, column.name)
            for column in universe_snapshots_table.columns
            if column.name != "snapshot_id"
        }
        symbol_rows = [
            {
                "symbol": item.symbol,
                "exchange": item.exchange,
                "yahoo_symbol": item.provider_symbol,
                "name": item.name,
                "currency": item.currency,
                "source": item.source,
                "source_url": item.source_url,
                "is_active": item.is_active,
                "canonical_instrument_id": item.canonical_instrument_id,
                "first_seen_at": item.first_seen_at,
                "last_seen_at": item.last_seen_at,
                "inactive_at": item.inactive_at,
                "inactive_reason": item.inactive_reason,
                "consecutive_missing_refreshes": item.consecutive_missing_refreshes,
                "last_universe_snapshot_id": item.last_universe_snapshot_id,
                "source_identity": item.source_identity,
                "provider_instrument_key": item.provider_instrument_key,
                "listing_status": item.listing_status,
                "listing_status_reason": item.listing_status_reason,
                "listing_status_effective_at": item.listing_status_effective_at,
                "pipeline_eligibility": item.pipeline_eligibility,
                "instrument_type": item.instrument_type,
                "reconciliation_status": item.reconciliation_status,
                "reconciliation_reason": item.reconciliation_reason,
                "official_sector": item.official_sector,
                "official_security_type": item.official_security_type,
                "official_source_updated_at": item.official_source_updated_at,
                "fetched_at": plan.fetched_at,
            }
            for item in plan.instruments
        ]
        member_rows = [
            {
                "snapshot_id": plan.snapshot_id,
                "canonical_instrument_id": item.canonical_instrument_id,
                "exchange_symbol": item.symbol,
                "provider_symbol": item.provider_symbol,
                "name": item.name,
                "raw_metadata": {
                    "currency": item.currency,
                    "source": item.source,
                    "source_url": item.source_url,
                    "source_identity": item.source_identity,
                    "provider_instrument_key": item.provider_instrument_key,
                    "listing_status": item.listing_status,
                    "listing_status_reason": item.listing_status_reason,
                    "listing_status_effective_at": (
                        item.listing_status_effective_at.isoformat()
                        if item.listing_status_effective_at
                        else None
                    ),
                    "pipeline_eligibility": item.pipeline_eligibility,
                    "instrument_type": item.instrument_type,
                    "reconciliation_status": item.reconciliation_status,
                    "reconciliation_reason": item.reconciliation_reason,
                    "official_sector": item.official_sector,
                    "official_security_type": item.official_security_type,
                    "official_source_updated_at": (
                        item.official_source_updated_at.isoformat()
                        if item.official_source_updated_at
                        else None
                    ),
                },
            }
            for item in plan.members
        ]
        provider_rows = [
            {
                "source": "yfinance",
                "instrument_key": item.provider_instrument_key or f"YF|{item.provider_symbol}",
                "exchange": item.exchange,
                "segment": f"{item.exchange}_EQ",
                "asset_type": "EQUITY",
                "trading_symbol": item.symbol,
                "name": item.name,
                "active": item.is_active and item.pipeline_eligibility != "none",
                "fetched_at": plan.fetched_at,
                "raw": {
                    "canonical_instrument_id": item.canonical_instrument_id,
                    "provider_symbol": item.provider_symbol,
                    "snapshot_id": plan.snapshot_id,
                    "currency": item.currency,
                    "source": item.source,
                    "source_url": item.source_url,
                    "source_identity": item.source_identity,
                    "provider_instrument_key": item.provider_instrument_key,
                    "listing_status": item.listing_status,
                    "pipeline_eligibility": item.pipeline_eligibility,
                    "instrument_type": item.instrument_type,
                    "reconciliation_status": item.reconciliation_status,
                    "reconciliation_reason": item.reconciliation_reason,
                    "official_sector": item.official_sector,
                    "official_security_type": item.official_security_type,
                    "official_source_updated_at": (
                        item.official_source_updated_at.isoformat()
                        if item.official_source_updated_at
                        else None
                    ),
                },
            }
            for item in plan.instruments
            if item.provider_symbol
        ]
        event_rows = [
            {
                "event_id": item.event_id,
                "canonical_instrument_id": item.canonical_instrument_id,
                "exchange": item.exchange,
                "event_type": item.event_type,
                "old_value": item.old_value,
                "new_value": item.new_value,
                "snapshot_id": item.snapshot_id,
                "created_at": item.created_at,
            }
            for item in plan.events
        ]
        work_rows = [
            {
                "work_item_id": item.work_item_id,
                "idempotency_key": item.idempotency_key,
                "work_type": "new_symbol_backfill",
                "provider": "yfinance",
                "exchange": item.exchange,
                "canonical_instrument_id": item.canonical_instrument_id,
                "provider_symbol": item.provider_symbol,
                "interval": "1d",
                "window_start": item.window_start,
                "window_end": item.window_end,
                "priority": 30,
                "status": "queued",
                "attempt_count": 0,
                "max_attempts": 9,
                "next_attempt_at": item.created_at,
                "created_at": item.created_at,
                "updated_at": item.created_at,
            }
            for item in plan.work_items
        ]
        symbol_rows = _deduplicate_rows(symbol_rows, ("symbol", "exchange"))
        member_rows = _deduplicate_rows(
            member_rows,
            ("snapshot_id", "canonical_instrument_id"),
        )
        provider_rows = _deduplicate_rows(
            provider_rows,
            ("source", "instrument_key"),
        )

        with self.engine.begin() as connection:
            connection.execute(
                snapshot_statement.on_conflict_do_update(
                    index_elements=["snapshot_id"],
                    set_=snapshot_updates,
                )
            )
            if symbol_rows:
                for chunk in _chunks(symbol_rows, size=1_000):
                    symbol_statement = insert(symbols_table).values(chunk)
                    symbol_updates = {
                        column.name: getattr(symbol_statement.excluded, column.name)
                        for column in symbols_table.columns
                        if column.name
                        not in {
                            "symbol",
                            "exchange",
                            "provider_status",
                            "provider_status_reason",
                            "provider_status_updated_at",
                        }
                    }
                    connection.execute(
                        symbol_statement.on_conflict_do_update(
                            index_elements=["symbol", "exchange"],
                            set_=symbol_updates,
                        )
                    )
            if member_rows:
                for chunk in _chunks(member_rows, size=1_000):
                    member_statement = insert(universe_snapshot_members_table).values(chunk)
                    member_updates = {
                        column.name: getattr(member_statement.excluded, column.name)
                        for column in universe_snapshot_members_table.columns
                        if column.name not in {"snapshot_id", "canonical_instrument_id"}
                    }
                    connection.execute(
                        member_statement.on_conflict_do_update(
                            index_elements=["snapshot_id", "canonical_instrument_id"],
                            set_=member_updates,
                        )
                    )
            if provider_rows:
                for chunk in _chunks(provider_rows, size=1_000):
                    provider_statement = insert(provider_instruments_table).values(chunk)
                    provider_updates = {
                        column.name: getattr(provider_statement.excluded, column.name)
                        for column in provider_instruments_table.columns
                        if column.name not in {"source", "instrument_key"}
                    }
                    connection.execute(
                        provider_statement.on_conflict_do_update(
                            index_elements=["source", "instrument_key"],
                            set_=provider_updates,
                        )
                    )
            self._persist_yfinance_aliases(connection, plan)
            ineligible_ids = [
                item.canonical_instrument_id
                for item in plan.instruments
                if item.pipeline_eligibility == "none"
            ]
            if ineligible_ids:
                connection.execute(
                    pipeline_work_items_table.update()
                    .where(pipeline_work_items_table.c.canonical_instrument_id.in_(ineligible_ids))
                    .where(pipeline_work_items_table.c.provider == "yfinance")
                    .where(pipeline_work_items_table.c.status.in_(("queued", "retry_wait")))
                    .values(
                        status="cancelled",
                        last_error_code="lifecycle_ineligible",
                        last_error_message=("Instrument lifecycle excludes provider execution."),
                        next_attempt_at=None,
                        completed_at=plan.fetched_at,
                        updated_at=plan.fetched_at,
                    )
                )
            if event_rows:
                for chunk in _chunks(event_rows, size=1_000):
                    connection.execute(
                        insert(symbol_lifecycle_events_table)
                        .values(chunk)
                        .on_conflict_do_nothing(index_elements=["event_id"])
                    )
            if work_rows:
                for chunk in _chunks(work_rows, size=1_000):
                    connection.execute(
                        insert(pipeline_work_items_table)
                        .values(chunk)
                        .on_conflict_do_nothing(index_elements=["idempotency_key"])
                    )

    @staticmethod
    def _persist_yfinance_aliases(connection: Any, plan: UniverseReconciliationPlan) -> None:
        members = list(
            {
                item.canonical_instrument_id: item for item in plan.members if item.provider_symbol
            }.values()
        )
        if not members:
            return
        canonical_ids = [item.canonical_instrument_id for item in members]
        provider_symbols = [str(item.provider_symbol) for item in members]
        current_aliases = list(
            connection.execute(
                instrument_aliases_table.select()
                .where(instrument_aliases_table.c.provider == "yfinance")
                .where(instrument_aliases_table.c.is_current.is_(True))
                .where(
                    (instrument_aliases_table.c.canonical_instrument_id.in_(canonical_ids))
                    | instrument_aliases_table.c.provider_symbol.in_(provider_symbols)
                )
            ).mappings()
        )
        current_by_instrument: dict[str, list[Mapping[str, Any]]] = {}
        for alias in current_aliases:
            current_by_instrument.setdefault(str(alias["canonical_instrument_id"]), []).append(
                alias
            )

        changed_members = [
            item
            for item in members
            if not any(
                alias["provider_symbol"] == item.provider_symbol
                for alias in current_by_instrument.get(item.canonical_instrument_id, [])
            )
        ]
        if not changed_members:
            return
        changed_ids = [item.canonical_instrument_id for item in changed_members]
        changed_symbols = [str(item.provider_symbol) for item in changed_members]
        old_provider_symbols = sorted(
            {
                str(alias["provider_symbol"])
                for alias in current_aliases
                if str(alias["canonical_instrument_id"]) in changed_ids
                or (
                    str(alias["provider_symbol"]) in changed_symbols
                    and str(alias["canonical_instrument_id"]) not in changed_ids
                )
            }
        )
        connection.execute(
            instrument_aliases_table.update()
            .where(instrument_aliases_table.c.provider == "yfinance")
            .where(instrument_aliases_table.c.is_current.is_(True))
            .where(
                instrument_aliases_table.c.canonical_instrument_id.in_(changed_ids)
                | instrument_aliases_table.c.provider_symbol.in_(changed_symbols)
            )
            .values(valid_to=plan.fetched_at, is_current=False)
        )
        if old_provider_symbols:
            connection.execute(
                provider_instruments_table.update()
                .where(provider_instruments_table.c.source == "yfinance")
                .where(
                    provider_instruments_table.c.instrument_key.in_(
                        [f"YF|{symbol}" for symbol in old_provider_symbols]
                    )
                )
                .values(active=False, fetched_at=plan.fetched_at)
            )
        alias_rows = []
        for item in changed_members:
            alias_identity = (
                f"{plan.snapshot_id}:{item.canonical_instrument_id}:yfinance:{item.provider_symbol}"
            )
            alias_rows.append(
                {
                    "alias_id": str(uuid5(NAMESPACE_URL, alias_identity)),
                    "canonical_instrument_id": item.canonical_instrument_id,
                    "provider": "yfinance",
                    "provider_symbol": item.provider_symbol,
                    "valid_from": plan.fetched_at,
                    "valid_to": None,
                    "is_current": True,
                }
            )
        for chunk in _chunks(alias_rows, size=1_000):
            connection.execute(
                insert(instrument_aliases_table)
                .values(chunk)
                .on_conflict_do_nothing(index_elements=["alias_id"])
            )

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
        if max_age_days is not None and _as_utc(row["fetched_at"]) < datetime.now(UTC) - timedelta(
            days=max_age_days
        ):
            return None
        return dict(row)

    def upsert_exchange_sessions(
        self,
        sessions: Iterable[Mapping[str, Any]],
    ) -> int:
        rows = _deduplicate_rows(
            [dict(row) for row in sessions],
            ("exchange", "session_date"),
        )
        if not rows:
            return 0
        total = 0
        with self.engine.begin() as connection:
            for chunk in _chunks(rows, size=1_000):
                statement = insert(exchange_sessions_table).values(chunk)
                update_columns = {
                    column.name: getattr(statement.excluded, column.name)
                    for column in exchange_sessions_table.columns
                    if column.name not in {"exchange", "session_date"}
                }
                connection.execute(
                    statement.on_conflict_do_update(
                        index_elements=["exchange", "session_date"],
                        set_=update_columns,
                    )
                )
                total += len(chunk)
        return total

    def exchange_sessions(
        self,
        exchange: str,
        start_date: date,
        end_date: date,
    ) -> list[dict[str, Any]]:
        query = (
            exchange_sessions_table.select()
            .where(exchange_sessions_table.c.exchange == exchange.upper())
            .where(exchange_sessions_table.c.session_date >= start_date)
            .where(exchange_sessions_table.c.session_date <= end_date)
            .order_by(exchange_sessions_table.c.session_date)
        )
        with self.engine.begin() as connection:
            return [dict(row) for row in connection.execute(query).mappings()]

    def delete_exchange_sessions(
        self,
        exchange: str,
        start_date: date,
        end_date: date,
    ) -> int:
        statement = (
            exchange_sessions_table.delete()
            .where(exchange_sessions_table.c.exchange == exchange.upper())
            .where(exchange_sessions_table.c.session_date >= start_date)
            .where(exchange_sessions_table.c.session_date <= end_date)
        )
        with self.engine.begin() as connection:
            result = connection.execute(statement)
        return int(result.rowcount or 0)

    def latest_provider_eligible_exchange_session(
        self,
        exchange: str,
        *,
        at: datetime | None = None,
        provider_grace_minutes: int = 0,
    ) -> dict[str, Any] | None:
        eligible_before = _as_utc(at or datetime.now(UTC)) - timedelta(
            minutes=provider_grace_minutes
        )
        query = (
            exchange_sessions_table.select()
            .where(exchange_sessions_table.c.exchange == exchange.upper())
            .where(exchange_sessions_table.c.is_trading_day.is_(True))
            .where(exchange_sessions_table.c.validation_status.like("valid%"))
            .where(exchange_sessions_table.c.market_close_utc <= eligible_before)
            .order_by(exchange_sessions_table.c.session_date.desc())
            .limit(1)
        )
        with self.engine.begin() as connection:
            row = connection.execute(query).mappings().first()
        return dict(row) if row is not None else None

    def active_yfinance_daily_instruments(
        self,
        exchange: str,
    ) -> list[dict[str, Any]]:
        """Return active members of the exchange's latest accepted snapshot."""
        exchange_code = exchange.upper()
        latest = self.latest_accepted_universe_snapshot(exchange_code)
        if latest is None:
            return []
        query = (
            select(
                symbols_table.c.canonical_instrument_id,
                symbols_table.c.symbol,
                symbols_table.c.exchange,
                symbols_table.c.yahoo_symbol.label("provider_symbol"),
                symbols_table.c.provider_instrument_key,
                symbols_table.c.first_seen_at,
                symbols_table.c.listing_status,
                symbols_table.c.listing_status_reason,
                symbols_table.c.listing_status_effective_at,
                symbols_table.c.pipeline_eligibility,
                symbols_table.c.provider_status,
                symbols_table.c.instrument_type,
                symbols_table.c.reconciliation_status,
                symbols_table.c.reconciliation_reason,
                symbols_table.c.official_sector,
                symbols_table.c.official_security_type,
                symbols_table.c.official_source_updated_at,
            )
            .select_from(
                universe_snapshot_members_table.join(
                    symbols_table,
                    (universe_snapshot_members_table.c.exchange_symbol == symbols_table.c.symbol)
                    & (symbols_table.c.exchange == exchange_code),
                )
            )
            .where(universe_snapshot_members_table.c.snapshot_id == str(latest["snapshot_id"]))
            .where(symbols_table.c.is_active.is_(True))
            .where(symbols_table.c.yahoo_symbol.is_not(None))
            .where(symbols_table.c.canonical_instrument_id.is_not(None))
            .where(symbols_table.c.pipeline_eligibility != "none")
            .order_by(symbols_table.c.symbol)
        )
        with self.engine.begin() as connection:
            return [dict(row) for row in connection.execute(query).mappings()]

    def universe_reconciliation_summary(self, exchange: str) -> dict[str, Any]:
        exchange_code = exchange.upper()
        latest = self.latest_accepted_universe_snapshot(exchange_code)
        if latest is None:
            return {"snapshot": None, "groups": []}
        query = (
            select(
                symbols_table.c.reconciliation_status,
                symbols_table.c.reconciliation_reason,
                symbols_table.c.instrument_type,
                symbols_table.c.pipeline_eligibility,
                func.count().label("symbols"),
            )
            .select_from(
                universe_snapshot_members_table.join(
                    symbols_table,
                    (universe_snapshot_members_table.c.exchange_symbol == symbols_table.c.symbol)
                    & (symbols_table.c.exchange == exchange_code),
                )
            )
            .where(universe_snapshot_members_table.c.snapshot_id == str(latest["snapshot_id"]))
            .group_by(
                symbols_table.c.reconciliation_status,
                symbols_table.c.reconciliation_reason,
                symbols_table.c.instrument_type,
                symbols_table.c.pipeline_eligibility,
            )
            .order_by(
                symbols_table.c.pipeline_eligibility.desc(),
                symbols_table.c.reconciliation_status,
                symbols_table.c.instrument_type,
            )
        )
        with self.engine.begin() as connection:
            groups = [dict(row) for row in connection.execute(query).mappings()]
        return {"snapshot": latest, "groups": groups}

    def enqueue_pipeline_work_items(
        self,
        work_items: Iterable[Mapping[str, Any] | DailyWorkItem],
    ) -> int:
        rows = _deduplicate_rows(
            [dict(item) if isinstance(item, Mapping) else item.as_row() for item in work_items],
            ("idempotency_key",),
        )
        if not rows:
            return 0
        inserted = 0
        with self.engine.begin() as connection:
            for chunk in _chunks(rows, size=1_000):
                result = connection.execute(
                    insert(pipeline_work_items_table)
                    .values(chunk)
                    .on_conflict_do_nothing(index_elements=["idempotency_key"])
                    .returning(pipeline_work_items_table.c.work_item_id)
                )
                inserted += len(result.scalars().all())
        return inserted

    def upsert_provider_daily_history_evidence(
        self,
        evidence_rows: Iterable[Mapping[str, Any]],
    ) -> int:
        rows = _deduplicate_rows(
            [dict(row) for row in evidence_rows],
            ("evidence_id",),
        )
        if not rows:
            return 0
        with self.engine.begin() as connection:
            for chunk in _chunks(rows, size=1_000):
                statement = insert(provider_daily_history_evidence_table).values(chunk)
                updates = {
                    column.name: getattr(statement.excluded, column.name)
                    for column in provider_daily_history_evidence_table.columns
                    if column.name not in {"evidence_id", "created_at"}
                }
                connection.execute(
                    statement.on_conflict_do_update(
                        index_elements=["evidence_id"],
                        set_=updates,
                    )
                )
        return len(rows)

    def provider_daily_history_evidence(
        self,
        instrument_keys: Iterable[str],
        *,
        provider: str = "yfinance",
        interval: str = "1d",
        active_only: bool = True,
        chunk_size: int = 500,
    ) -> dict[str, list[dict[str, Any]]]:
        keys = list(dict.fromkeys(str(key) for key in instrument_keys))
        grouped: dict[str, list[dict[str, Any]]] = {}
        if not keys:
            return grouped
        with self.engine.begin() as connection:
            for chunk in _chunks(keys, size=max(chunk_size, 1)):
                query = (
                    provider_daily_history_evidence_table.select()
                    .where(provider_daily_history_evidence_table.c.provider == provider)
                    .where(provider_daily_history_evidence_table.c.interval == interval)
                    .where(provider_daily_history_evidence_table.c.instrument_key.in_(chunk))
                    .order_by(
                        provider_daily_history_evidence_table.c.instrument_key,
                        provider_daily_history_evidence_table.c.requested_start,
                    )
                )
                if active_only:
                    query = query.where(provider_daily_history_evidence_table.c.status == "active")
                for row in connection.execute(query).mappings():
                    item = dict(row)
                    grouped.setdefault(str(item["instrument_key"]), []).append(item)
        return grouped

    def succeeded_daily_backfill_work_items(
        self,
        canonical_instrument_ids: Iterable[str],
        *,
        provider: str = "yfinance",
    ) -> list[dict[str, Any]]:
        canonical_ids = list(dict.fromkeys(str(value) for value in canonical_instrument_ids))
        if not canonical_ids:
            return []
        rows: list[dict[str, Any]] = []
        with self.engine.begin() as connection:
            for chunk in _chunks(canonical_ids, size=500):
                query = (
                    select(
                        pipeline_work_items_table,
                        symbols_table.c.provider_instrument_key,
                        symbols_table.c.listing_status,
                        symbols_table.c.listing_status_effective_at,
                        symbols_table.c.first_seen_at,
                    )
                    .select_from(
                        pipeline_work_items_table.join(
                            symbols_table,
                            (
                                pipeline_work_items_table.c.canonical_instrument_id
                                == symbols_table.c.canonical_instrument_id
                            )
                            & (pipeline_work_items_table.c.exchange == symbols_table.c.exchange),
                        )
                    )
                    .where(pipeline_work_items_table.c.provider == provider)
                    .where(pipeline_work_items_table.c.status == "succeeded")
                    .where(symbols_table.c.is_active.is_(True))
                    .where(
                        symbols_table.c.yahoo_symbol == pipeline_work_items_table.c.provider_symbol
                    )
                    .where(
                        pipeline_work_items_table.c.work_type.in_(
                            ("initial_backfill", "new_symbol_backfill", "gap_repair")
                        )
                    )
                    .where(pipeline_work_items_table.c.canonical_instrument_id.in_(chunk))
                    .order_by(
                        pipeline_work_items_table.c.provider_symbol,
                        pipeline_work_items_table.c.window_start,
                    )
                )
                rows.extend(dict(row) for row in connection.execute(query).mappings())
        return rows

    def provider_daily_history_summary(
        self,
        exchange: str,
        *,
        provider: str = "yfinance",
    ) -> dict[str, Any]:
        filters = (
            provider_daily_history_evidence_table.c.exchange == exchange.upper(),
            provider_daily_history_evidence_table.c.provider == provider,
            provider_daily_history_evidence_table.c.status == "active",
        )
        grouped_query = (
            select(
                provider_daily_history_evidence_table.c.classification,
                func.count().label("evidence_windows"),
                func.count(
                    func.distinct(provider_daily_history_evidence_table.c.instrument_key)
                ).label("instruments"),
                func.sum(provider_daily_history_evidence_table.c.expected_rows).label(
                    "expected_rows"
                ),
                func.sum(provider_daily_history_evidence_table.c.observed_rows).label(
                    "observed_rows"
                ),
                func.sum(provider_daily_history_evidence_table.c.missing_rows).label(
                    "provider_unavailable_rows"
                ),
                func.max(provider_daily_history_evidence_table.c.verified_at).label(
                    "latest_verified_at"
                ),
            )
            .where(*filters)
            .group_by(provider_daily_history_evidence_table.c.classification)
            .order_by(provider_daily_history_evidence_table.c.classification)
        )
        quarantine_query = (
            provider_daily_history_evidence_table.select()
            .where(*filters)
            .where(provider_daily_history_evidence_table.c.classification == "quarantined_sparse")
            .order_by(provider_daily_history_evidence_table.c.provider_symbol)
            .limit(50)
        )
        with self.engine.begin() as connection:
            groups = [dict(row) for row in connection.execute(grouped_query).mappings()]
            quarantined = [dict(row) for row in connection.execute(quarantine_query).mappings()]
        return {
            "provider": provider,
            "exchange": exchange.upper(),
            "groups": groups,
            "quarantined": quarantined,
        }

    def cancel_pipeline_work_items_before_listing(
        self,
        *,
        exchange: str,
        provider: str = "yfinance",
        at: datetime | None = None,
    ) -> int:
        """Cancel pending work whose entire window predates an active listing."""
        now = _as_utc(at or datetime.now(UTC))
        listing_match = (
            select(1)
            .select_from(symbols_table)
            .where(
                symbols_table.c.canonical_instrument_id
                == pipeline_work_items_table.c.canonical_instrument_id
            )
            .where(symbols_table.c.exchange == exchange.upper())
            .where(symbols_table.c.listing_status == "active")
            .where(symbols_table.c.listing_status_effective_at.is_not(None))
            .where(
                pipeline_work_items_table.c.window_end
                < func.date(symbols_table.c.listing_status_effective_at)
            )
            .exists()
        )
        statement = (
            pipeline_work_items_table.update()
            .where(pipeline_work_items_table.c.exchange == exchange.upper())
            .where(pipeline_work_items_table.c.provider == provider)
            .where(pipeline_work_items_table.c.status.in_(("queued", "retry_wait")))
            .where(listing_match)
            .values(
                status="cancelled",
                last_error_code="outside_listing_window",
                last_error_message=(
                    "Work window ends before the active instrument listing boundary."
                ),
                next_attempt_at=None,
                completed_at=now,
                updated_at=now,
            )
        )
        with self.engine.begin() as connection:
            result = connection.execute(statement)
        return int(result.rowcount or 0)

    def cancel_pending_pipeline_work_for_instruments(
        self,
        canonical_instrument_ids: Iterable[str],
        *,
        provider: str = "yfinance",
        reason: str,
        message: str,
        at: datetime | None = None,
    ) -> int:
        canonical_ids = list(dict.fromkeys(str(value) for value in canonical_instrument_ids))
        if not canonical_ids:
            return 0
        now = _as_utc(at or datetime.now(UTC))
        statement = (
            pipeline_work_items_table.update()
            .where(pipeline_work_items_table.c.provider == provider)
            .where(pipeline_work_items_table.c.canonical_instrument_id.in_(canonical_ids))
            .where(pipeline_work_items_table.c.status.in_(("queued", "retry_wait")))
            .values(
                status="cancelled",
                last_error_code=reason,
                last_error_message=message,
                next_attempt_at=None,
                completed_at=now,
                updated_at=now,
            )
        )
        with self.engine.begin() as connection:
            result = connection.execute(statement)
        return int(result.rowcount or 0)

    def cancel_pipeline_work_items_covered_by_provider_history(
        self,
        *,
        exchange: str | None = None,
        provider: str = "yfinance",
        at: datetime | None = None,
    ) -> int:
        """Cancel pending historical work fully covered by verified evidence."""
        now = _as_utc(at or datetime.now(UTC))
        evidence_match = (
            select(1)
            .select_from(provider_daily_history_evidence_table)
            .where(
                provider_daily_history_evidence_table.c.provider
                == pipeline_work_items_table.c.provider
            )
            .where(
                provider_daily_history_evidence_table.c.exchange
                == pipeline_work_items_table.c.exchange
            )
            .where(
                provider_daily_history_evidence_table.c.canonical_instrument_id
                == pipeline_work_items_table.c.canonical_instrument_id
            )
            .where(
                provider_daily_history_evidence_table.c.interval
                == pipeline_work_items_table.c.interval
            )
            .where(provider_daily_history_evidence_table.c.status == "active")
            .where(
                provider_daily_history_evidence_table.c.classification.in_(
                    ("verified_complete", "verified_partial")
                )
            )
            .where(
                provider_daily_history_evidence_table.c.coverage_start
                <= pipeline_work_items_table.c.window_start
            )
            .where(
                provider_daily_history_evidence_table.c.coverage_end
                >= pipeline_work_items_table.c.window_end
            )
            .exists()
        )
        statement = (
            pipeline_work_items_table.update()
            .where(pipeline_work_items_table.c.provider == provider)
            .where(
                pipeline_work_items_table.c.work_type.in_(
                    ("initial_backfill", "new_symbol_backfill", "gap_repair")
                )
            )
            .where(pipeline_work_items_table.c.status.in_(("queued", "retry_wait")))
            .where(evidence_match)
            .values(
                status="cancelled",
                next_attempt_at=None,
                locked_by=None,
                locked_at=None,
                last_error_code="provider_history_verified",
                last_error_message=(
                    "Cancelled because durable provider-history evidence covers this window."
                ),
                completed_at=now,
                updated_at=now,
            )
        )
        if exchange:
            statement = statement.where(pipeline_work_items_table.c.exchange == exchange.upper())
        with self.engine.begin() as connection:
            result = connection.execute(statement)
        return int(result.rowcount or 0)

    def claim_pipeline_work_items(
        self,
        *,
        worker_id: str,
        limit: int,
        at: datetime | None = None,
        provider: str = "yfinance",
        exchange: str | None = None,
        work_type: str | None = None,
        provider_symbols: Iterable[str] | None = None,
    ) -> list[dict[str, Any]]:
        """Atomically claim ready work using PostgreSQL SKIP LOCKED."""
        if limit <= 0:
            return []
        now = _as_utc(at or datetime.now(UTC))
        ready = (pipeline_work_items_table.c.status == "queued") | (
            (pipeline_work_items_table.c.status == "retry_wait")
            & (
                (pipeline_work_items_table.c.next_attempt_at.is_(None))
                | (pipeline_work_items_table.c.next_attempt_at <= now)
            )
        )
        query = (
            pipeline_work_items_table.select()
            .where(pipeline_work_items_table.c.provider == provider)
            .where(ready)
            .where(
                pipeline_work_items_table.c.attempt_count < pipeline_work_items_table.c.max_attempts
            )
        )
        if exchange is not None:
            query = query.where(pipeline_work_items_table.c.exchange == exchange.upper())
        if work_type is not None:
            query = query.where(pipeline_work_items_table.c.work_type == work_type)
        if provider_symbols is not None:
            symbols = tuple(dict.fromkeys(value.strip().upper() for value in provider_symbols))
            if not symbols:
                return []
            query = query.where(pipeline_work_items_table.c.provider_symbol.in_(symbols))
        query = (
            query.order_by(
                pipeline_work_items_table.c.priority,
                pipeline_work_items_table.c.created_at,
                pipeline_work_items_table.c.work_item_id,
            )
            .limit(limit)
            .with_for_update(skip_locked=True)
        )
        claimed: list[dict[str, Any]] = []
        with self.engine.begin() as connection:
            rows = list(connection.execute(query).mappings())
            lifecycle_by_id: dict[str, Mapping[str, Any]] = {}
            canonical_ids = [str(row["canonical_instrument_id"]) for row in rows]
            if canonical_ids:
                lifecycle_rows = connection.execute(
                    select(
                        symbols_table.c.canonical_instrument_id,
                        symbols_table.c.first_seen_at,
                        symbols_table.c.listing_status,
                        symbols_table.c.listing_status_reason,
                        symbols_table.c.listing_status_effective_at,
                        symbols_table.c.pipeline_eligibility,
                        symbols_table.c.provider_status,
                        symbols_table.c.provider_instrument_key,
                    ).where(symbols_table.c.canonical_instrument_id.in_(canonical_ids))
                ).mappings()
                lifecycle_by_id = {
                    str(item["canonical_instrument_id"]): item for item in lifecycle_rows
                }
            for row in rows:
                values = {
                    "status": "running",
                    "attempt_count": int(row["attempt_count"]) + 1,
                    "locked_by": worker_id,
                    "locked_at": now,
                    "updated_at": now,
                    "completed_at": None,
                }
                connection.execute(
                    pipeline_work_items_table.update()
                    .where(pipeline_work_items_table.c.work_item_id == row["work_item_id"])
                    .values(**values)
                )
                lifecycle = lifecycle_by_id.get(str(row["canonical_instrument_id"]), {})
                claimed.append({**dict(row), **dict(lifecycle), **values})
        return claimed

    def heartbeat_pipeline_work_items(
        self,
        *,
        worker_id: str,
        work_item_ids: Iterable[str],
        at: datetime | None = None,
    ) -> int:
        ids = list(dict.fromkeys(str(value) for value in work_item_ids))
        if not ids:
            return 0
        now = _as_utc(at or datetime.now(UTC))
        statement = (
            pipeline_work_items_table.update()
            .where(pipeline_work_items_table.c.work_item_id.in_(ids))
            .where(pipeline_work_items_table.c.status == "running")
            .where(pipeline_work_items_table.c.locked_by == worker_id)
            .values(locked_at=now, updated_at=now)
        )
        with self.engine.begin() as connection:
            result = connection.execute(statement)
        return int(result.rowcount or 0)

    def transition_pipeline_work_item(
        self,
        *,
        work_item_id: str,
        worker_id: str,
        status: str,
        status_code: int | None = None,
        error_code: str | None = None,
        error_message: str | None = None,
        run_id: str | None = None,
        retry_delay: timedelta | None = None,
        at: datetime | None = None,
    ) -> dict[str, Any] | None:
        if status not in {"succeeded", "retry_wait", "terminal", "cancelled"}:
            raise ValueError(f"Unsupported pipeline work transition: {status}")
        from trade_data.data.daily_work import (
            WORK_PRIORITIES,
            durable_retry_delay,
        )

        now = _as_utc(at or datetime.now(UTC))
        identity = pipeline_work_items_table.c.work_item_id == work_item_id
        with self.engine.begin() as connection:
            row = (
                connection.execute(
                    pipeline_work_items_table.select()
                    .where(identity)
                    .where(pipeline_work_items_table.c.status == "running")
                    .where(pipeline_work_items_table.c.locked_by == worker_id)
                    .with_for_update()
                )
                .mappings()
                .first()
            )
            if row is None:
                return None
            resolved_status = status
            attempt_count = int(row["attempt_count"])
            if status == "retry_wait" and attempt_count >= int(row["max_attempts"]):
                resolved_status = "terminal"
                error_code = error_code or "maximum_attempts_exhausted"
            values: dict[str, Any] = {
                "status": resolved_status,
                "locked_by": None,
                "locked_at": None,
                "run_id": run_id,
                "last_status_code": status_code,
                "last_error_code": error_code,
                "last_error_message": error_message,
                "updated_at": now,
                "completed_at": (
                    now if resolved_status in {"succeeded", "terminal", "cancelled"} else None
                ),
                "next_attempt_at": (
                    now + (retry_delay or durable_retry_delay(attempt_count))
                    if resolved_status == "retry_wait"
                    else None
                ),
            }
            if resolved_status == "retry_wait" and row["work_type"] == "daily_incremental":
                values["priority"] = WORK_PRIORITIES["daily_incremental_retry"]
            connection.execute(pipeline_work_items_table.update().where(identity).values(**values))
            return {**dict(row), **values}

    def update_symbol_provider_status(
        self,
        canonical_instrument_id: str,
        *,
        status: str,
        reason: str | None = None,
        at: datetime | None = None,
    ) -> int:
        now = _as_utc(at or datetime.now(UTC))
        with self.engine.begin() as connection:
            result = connection.execute(
                symbols_table.update()
                .where(symbols_table.c.canonical_instrument_id == canonical_instrument_id)
                .values(
                    provider_status=status,
                    provider_status_reason=reason,
                    provider_status_updated_at=now,
                )
            )
        return int(result.rowcount or 0)

    def recover_stale_pipeline_work_items(
        self,
        *,
        stale_before: datetime,
        at: datetime | None = None,
        provider: str = "yfinance",
        exchange: str | None = None,
        work_type: str | None = None,
        provider_symbols: Iterable[str] | None = None,
    ) -> int:
        now = _as_utc(at or datetime.now(UTC))
        statement = (
            pipeline_work_items_table.update()
            .where(pipeline_work_items_table.c.provider == provider)
            .where(pipeline_work_items_table.c.status == "running")
            .where(pipeline_work_items_table.c.locked_at < _as_utc(stale_before))
            .values(
                status=case(
                    (
                        pipeline_work_items_table.c.attempt_count
                        >= pipeline_work_items_table.c.max_attempts,
                        "terminal",
                    ),
                    else_="queued",
                ),
                next_attempt_at=case(
                    (
                        pipeline_work_items_table.c.attempt_count
                        >= pipeline_work_items_table.c.max_attempts,
                        None,
                    ),
                    else_=now,
                ),
                locked_by=None,
                locked_at=None,
                last_error_code="stale_lock_recovered",
                last_error_message=("Worker heartbeat expired; work was recovered or finalized."),
                updated_at=now,
                completed_at=case(
                    (
                        pipeline_work_items_table.c.attempt_count
                        >= pipeline_work_items_table.c.max_attempts,
                        now,
                    ),
                    else_=None,
                ),
            )
        )
        if exchange is not None:
            statement = statement.where(pipeline_work_items_table.c.exchange == exchange.upper())
        if work_type is not None:
            statement = statement.where(pipeline_work_items_table.c.work_type == work_type)
        if provider_symbols is not None:
            symbols = tuple(dict.fromkeys(value.strip().upper() for value in provider_symbols))
            if not symbols:
                return 0
            statement = statement.where(pipeline_work_items_table.c.provider_symbol.in_(symbols))
        with self.engine.begin() as connection:
            result = connection.execute(statement)
        return int(result.rowcount or 0)

    def pipeline_work_queue_summary(
        self,
        *,
        provider: str = "yfinance",
    ) -> dict[str, int]:
        query = (
            select(
                pipeline_work_items_table.c.status,
                func.count().label("count"),
            )
            .where(pipeline_work_items_table.c.provider == provider)
            .group_by(pipeline_work_items_table.c.status)
        )
        with self.engine.begin() as connection:
            rows = connection.execute(query).all()
        return {str(status): int(count) for status, count in rows}

    def pipeline_work_queue_groups(
        self,
        *,
        provider: str | None = None,
        exchange: str | None = None,
    ) -> list[dict[str, Any]]:
        query = select(
            pipeline_work_items_table.c.provider,
            pipeline_work_items_table.c.exchange,
            pipeline_work_items_table.c.work_type,
            pipeline_work_items_table.c.status,
            func.count().label("items"),
            func.count(func.distinct(pipeline_work_items_table.c.provider_symbol)).label("symbols"),
            func.max(pipeline_work_items_table.c.attempt_count).label("maximum_attempts"),
            func.min(pipeline_work_items_table.c.created_at).label("oldest_created_at"),
            func.min(pipeline_work_items_table.c.next_attempt_at).label("earliest_next_attempt_at"),
        )
        if provider:
            query = query.where(pipeline_work_items_table.c.provider == provider.lower())
        if exchange:
            query = query.where(pipeline_work_items_table.c.exchange == exchange.upper())
        query = query.group_by(
            pipeline_work_items_table.c.provider,
            pipeline_work_items_table.c.exchange,
            pipeline_work_items_table.c.work_type,
            pipeline_work_items_table.c.status,
        ).order_by(
            pipeline_work_items_table.c.exchange,
            pipeline_work_items_table.c.work_type,
            pipeline_work_items_table.c.status,
        )
        with self.engine.begin() as connection:
            return [dict(row) for row in connection.execute(query).mappings()]

    def pipeline_work_items_page(
        self,
        *,
        provider: str | None = None,
        exchange: str | None = None,
        status: str | None = None,
        work_type: str | None = None,
        symbol: str | None = None,
        limit: int = 100,
        offset: int = 0,
    ) -> dict[str, Any]:
        filters = []
        if provider:
            filters.append(pipeline_work_items_table.c.provider == provider.lower())
        if exchange:
            filters.append(pipeline_work_items_table.c.exchange == exchange.upper())
        if status:
            filters.append(pipeline_work_items_table.c.status == status.lower())
        if work_type:
            filters.append(pipeline_work_items_table.c.work_type == work_type.lower())
        if symbol:
            filters.append(
                func.upper(pipeline_work_items_table.c.provider_symbol).contains(symbol.upper())
            )

        query = pipeline_work_items_table.select()
        count_query = select(func.count()).select_from(pipeline_work_items_table)
        for condition in filters:
            query = query.where(condition)
            count_query = count_query.where(condition)
        query = (
            query.order_by(
                pipeline_work_items_table.c.updated_at.desc(),
                pipeline_work_items_table.c.priority,
                pipeline_work_items_table.c.work_item_id,
            )
            .limit(max(limit, 1))
            .offset(max(offset, 0))
        )
        with self.engine.begin() as connection:
            total = int(connection.execute(count_query).scalar_one())
            rows = [dict(row) for row in connection.execute(query).mappings()]
        return {"total": total, "rows": rows}

    def pipeline_work_items_for_run(
        self,
        run_id: str,
        *,
        claimed_work_item_ids: Iterable[str] | None = None,
        exchange: str | None = None,
        limit: int = 500,
    ) -> list[dict[str, Any]]:
        """Return claimed work, even when a later retry replaced its run_id."""
        claimed_ids = list(dict.fromkeys(str(value) for value in (claimed_work_item_ids or ())))
        query = pipeline_work_items_table.select()
        if claimed_ids:
            query = query.where(pipeline_work_items_table.c.work_item_id.in_(claimed_ids))
        else:
            query = query.where(pipeline_work_items_table.c.run_id == run_id)
        if exchange:
            query = query.where(pipeline_work_items_table.c.exchange == exchange.upper())
        query = query.order_by(
            case(
                (pipeline_work_items_table.c.last_error_code.is_not(None), 0),
                else_=1,
            ),
            pipeline_work_items_table.c.exchange,
            pipeline_work_items_table.c.provider_symbol,
        ).limit(max(1, limit))
        with self.engine.begin() as connection:
            return [dict(row) for row in connection.execute(query).mappings()]

    def symbol_lifecycle_events_page(
        self,
        *,
        exchange: str | None = None,
        event_type: str | None = None,
        symbol: str | None = None,
        limit: int = 100,
        offset: int = 0,
    ) -> dict[str, Any]:
        joined = symbol_lifecycle_events_table.outerjoin(
            symbols_table,
            (
                symbol_lifecycle_events_table.c.canonical_instrument_id
                == symbols_table.c.canonical_instrument_id
            )
            & (symbol_lifecycle_events_table.c.exchange == symbols_table.c.exchange),
        )
        filters = []
        if exchange:
            filters.append(symbol_lifecycle_events_table.c.exchange == exchange.upper())
        if event_type:
            filters.append(symbol_lifecycle_events_table.c.event_type == event_type.lower())
        if symbol:
            filters.append(func.upper(symbols_table.c.symbol).contains(symbol.upper()))

        columns = [
            symbol_lifecycle_events_table.c.event_id,
            symbol_lifecycle_events_table.c.canonical_instrument_id,
            symbol_lifecycle_events_table.c.exchange,
            symbols_table.c.symbol,
            symbol_lifecycle_events_table.c.event_type,
            symbol_lifecycle_events_table.c.old_value,
            symbol_lifecycle_events_table.c.new_value,
            symbol_lifecycle_events_table.c.snapshot_id,
            symbol_lifecycle_events_table.c.created_at,
        ]
        query = select(*columns).select_from(joined)
        count_query = select(func.count()).select_from(joined)
        for condition in filters:
            query = query.where(condition)
            count_query = count_query.where(condition)
        query = (
            query.order_by(
                symbol_lifecycle_events_table.c.created_at.desc(),
                symbol_lifecycle_events_table.c.event_id,
            )
            .limit(max(limit, 1))
            .offset(max(offset, 0))
        )
        with self.engine.begin() as connection:
            total = int(connection.execute(count_query).scalar_one())
            rows = [dict(row) for row in connection.execute(query).mappings()]
        return {"total": total, "rows": rows}

    def adaptive_rate_states(
        self,
        *,
        provider: str | None = None,
    ) -> list[dict[str, Any]]:
        query = adaptive_rate_state_table.select()
        if provider:
            query = query.where(adaptive_rate_state_table.c.provider == provider.lower())
        query = query.order_by(adaptive_rate_state_table.c.provider)
        with self.engine.begin() as connection:
            return [dict(row) for row in connection.execute(query).mappings()]

    def provider_data_freshness(
        self,
        *,
        provider: str | None = None,
        exchange: str | None = None,
    ) -> list[dict[str, Any]]:
        query = select(
            ohlcv_daily_table.c.source.label("provider"),
            ohlcv_daily_table.c.exchange,
            func.min(ohlcv_daily_table.c.date).label("first_date"),
            func.max(ohlcv_daily_table.c.date).label("latest_date"),
            func.count().label("rows"),
            func.count(func.distinct(ohlcv_daily_table.c.instrument_key)).label("symbols"),
            func.sum(case((ohlcv_daily_table.c.quality_status == "suspicious", 1), else_=0)).label(
                "suspicious_rows"
            ),
            func.max(ohlcv_daily_table.c.fetched_at).label("latest_fetched_at"),
        )
        if provider:
            query = query.where(ohlcv_daily_table.c.source == provider.lower())
        if exchange:
            query = query.where(ohlcv_daily_table.c.exchange == exchange.upper())
        query = query.group_by(
            ohlcv_daily_table.c.source,
            ohlcv_daily_table.c.exchange,
        ).order_by(ohlcv_daily_table.c.exchange, ohlcv_daily_table.c.source)
        with self.engine.begin() as connection:
            return [dict(row) for row in connection.execute(query).mappings()]

    def latest_accepted_universe_snapshots(
        self,
        *,
        exchange: str | None = None,
    ) -> list[dict[str, Any]]:
        query = universe_snapshots_table.select().where(
            universe_snapshots_table.c.status == "accepted"
        )
        if exchange:
            query = query.where(universe_snapshots_table.c.exchange == exchange.upper())
        query = query.order_by(
            universe_snapshots_table.c.fetched_at.desc(),
            universe_snapshots_table.c.snapshot_id.desc(),
        )
        latest: dict[str, dict[str, Any]] = {}
        with self.engine.begin() as connection:
            for row in connection.execute(query).mappings():
                payload = dict(row)
                latest.setdefault(str(payload["exchange"]), payload)
        return [latest[key] for key in sorted(latest)]

    def observed_daily_session_dates(
        self,
        exchange: str,
        start_date: date,
        end_date: date,
        *,
        minimum_instruments: int = 10,
    ) -> set[date]:
        query = (
            select(ohlcv_daily_table.c.date)
            .where(ohlcv_daily_table.c.exchange == exchange.upper())
            .where(ohlcv_daily_table.c.date >= start_date)
            .where(ohlcv_daily_table.c.date <= end_date)
            .group_by(ohlcv_daily_table.c.date)
            .having(
                func.count(func.distinct(ohlcv_daily_table.c.instrument_key)) >= minimum_instruments
            )
        )
        with self.engine.begin() as connection:
            return set(connection.execute(query).scalars())

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

    def upsert_daily_price_adjustments(
        self,
        frame: pd.DataFrame,
        exchange: str = "NSE",
        source: str = "upstox",
    ) -> int:
        rows = self._daily_price_adjustment_rows(frame, exchange=exchange, source=source)
        if not rows:
            return 0

        total = 0
        with self.engine.begin() as connection:
            for chunk in _chunks(rows, size=1_000):
                statement = insert(price_adjustments_daily_table).values(chunk)
                update_columns = {
                    column.name: getattr(statement.excluded, column.name)
                    for column in price_adjustments_daily_table.columns
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

    def upsert_corporate_actions(
        self,
        frame: pd.DataFrame,
        exchange: str,
        source: str,
    ) -> int:
        rows = self._corporate_action_rows(frame, exchange=exchange, source=source)
        if not rows:
            return 0

        total = 0
        with self.engine.begin() as connection:
            for chunk in _chunks(rows, size=1_000):
                statement = insert(corporate_actions_table).values(chunk)
                update_columns = {
                    column.name: getattr(statement.excluded, column.name)
                    for column in corporate_actions_table.columns
                    if column.name not in {"source", "instrument_key", "action_date", "action_type"}
                }
                connection.execute(
                    statement.on_conflict_do_update(
                        index_elements=[
                            "source",
                            "instrument_key",
                            "action_date",
                            "action_type",
                        ],
                        set_=update_columns,
                    )
                )
                total += len(chunk)
        return total

    def latest_daily_ohlcv_dates(
        self,
        instrument_keys: list[str],
        source: str = "upstox",
        *,
        valid_only: bool = False,
        chunk_size: int = 250,
    ) -> dict[str, date]:
        normalized_keys = list(dict.fromkeys(instrument_keys))
        if not normalized_keys:
            return {}
        if chunk_size < 1:
            raise ValueError("chunk_size must be positive")
        latest_dates: dict[str, date] = {}
        # Prune old Timescale chunks for active symbols. Missing recent symbols
        # still receive an unbounded lookup, preserving old and delisted history.
        recent_cutoff = datetime.now(UTC).date() - timedelta(days=90)
        with self.engine.begin() as connection:
            for key_chunk in _chunks(normalized_keys, size=chunk_size):
                query = (
                    select(
                        ohlcv_daily_table.c.instrument_key,
                        func.max(ohlcv_daily_table.c.date).label("latest_date"),
                    )
                    .where(ohlcv_daily_table.c.source == source)
                    .where(ohlcv_daily_table.c.instrument_key.in_(key_chunk))
                    .group_by(ohlcv_daily_table.c.instrument_key)
                )
                if valid_only:
                    query = query.where(ohlcv_daily_table.c.quality_status == "ok")
                rows = (
                    connection.execute(query.where(ohlcv_daily_table.c.date >= recent_cutoff))
                    .mappings()
                    .all()
                )
                recent_dates = {
                    str(row["instrument_key"]): row["latest_date"]
                    for row in rows
                    if row["latest_date"] is not None
                }
                latest_dates.update(recent_dates)
                missing_keys = [key for key in key_chunk if key not in recent_dates]
                if missing_keys:
                    fallback = query.where(ohlcv_daily_table.c.instrument_key.in_(missing_keys))
                    rows = connection.execute(fallback).mappings().all()
                    latest_dates.update(
                        {
                            str(row["instrument_key"]): row["latest_date"]
                            for row in rows
                            if row["latest_date"] is not None
                        }
                    )
        return latest_dates

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
        source_lower = source.lower()
        if source_lower == "yfinance":
            query = (
                select(
                    symbols_table.c.provider_instrument_key.label("instrument_key"),
                    symbols_table.c.symbol.label("trading_symbol"),
                    symbols_table.c.yahoo_symbol,
                    symbols_table.c.name,
                )
                .where(symbols_table.c.exchange == exchange_upper)
                .where(symbols_table.c.is_active.is_(True))
                .where(symbols_table.c.pipeline_eligibility != "none")
                .where(symbols_table.c.provider_instrument_key.is_not(None))
                .where(
                    (symbols_table.c.symbol.in_(normalized_symbols))
                    | (symbols_table.c.yahoo_symbol.in_(normalized_symbols))
                )
                .order_by(symbols_table.c.symbol)
            )
            with self.engine.begin() as connection:
                return [dict(row) for row in connection.execute(query).mappings()]
        query = (
            provider_instruments_table.select()
            .where(provider_instruments_table.c.source == source_lower)
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
        *,
        valid_only: bool = False,
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
        if valid_only:
            query = query.where(ohlcv_daily_table.c.quality_status == "ok")
        dates_by_key: dict[str, set[date]] = {key: set() for key in instrument_keys}
        with self.engine.begin() as connection:
            rows = connection.execute(query).all()
        for instrument_key, candle_date in rows:
            if candle_date is not None:
                dates_by_key.setdefault(str(instrument_key), set()).add(candle_date)
        return dates_by_key

    def first_daily_ohlcv_dates_by_instrument(
        self,
        instrument_keys: list[str],
        source: str = "upstox",
        exchange: str = "NSE",
    ) -> dict[str, date]:
        if not instrument_keys:
            return {}
        query = (
            select(
                ohlcv_daily_table.c.instrument_key,
                func.min(ohlcv_daily_table.c.date).label("first_date"),
            )
            .where(ohlcv_daily_table.c.source == source)
            .where(ohlcv_daily_table.c.exchange == exchange.upper())
            .where(ohlcv_daily_table.c.instrument_key.in_(instrument_keys))
            .group_by(ohlcv_daily_table.c.instrument_key)
        )
        with self.engine.begin() as connection:
            rows = connection.execute(query).all()
        return {
            str(instrument_key): first_date
            for instrument_key, first_date in rows
            if first_date is not None
        }

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
        expected_session_dates: Iterable[date] | None = None,
        limit: int = 50,
        offset: int = 0,
        sort: str = "symbol",
    ) -> dict[str, Any]:
        if (start_date is None) != (end_date is None):
            raise ValueError("start_date and end_date must be supplied together.")
        if start_date and end_date and start_date > end_date:
            raise ValueError("start_date must be on or before end_date.")
        expected_dates = tuple(dict.fromkeys(expected_session_dates or ()))
        if expected_dates and len(expected_dates) != expected_rows_per_symbol:
            raise ValueError("expected_session_dates must match expected_rows_per_symbol.")

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
        expected_values_sql = []
        for index, session_date in enumerate(expected_dates):
            date_key = f"expected_date_{index}"
            expected_values_sql.append(f"(:{date_key})")
            params[date_key] = session_date
        expected_sessions_sql = (
            f"VALUES {', '.join(expected_values_sql)}"
            if expected_values_sql
            else "SELECT CAST(NULL AS date) WHERE false"
        )

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
            expected_sessions(session_date) AS (
                {expected_sessions_sql}
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
                    count(d.date)::bigint AS raw_stored_rows,
                    count(d.date) FILTER (
                        WHERE expected.session_date IS NOT NULL
                          AND d.quality_status = 'ok'
                    )::bigint AS matched_stored_rows,
                    count(d.date) FILTER (
                        WHERE expected.session_date IS NOT NULL
                          AND d.quality_status <> 'ok'
                    )::bigint AS invalid_stored_rows,
                    count(d.date) FILTER (
                        WHERE expected.session_date IS NULL
                    )::bigint AS off_calendar_stored_rows,
                    ls.last_successful_run,
                    lf.last_fetch_status
                FROM provider_instruments pi
                {universe_join}
                LEFT JOIN ohlcv_daily d
                    ON d.instrument_key = pi.instrument_key
                   AND d.source = pi.source
                   AND d.exchange = :exchange
                   {ohlcv_date_filter}
                LEFT JOIN expected_sessions expected
                    ON expected.session_date = d.date
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
                    raw_stored_rows AS stored_rows,
                    CASE
                        WHEN CAST(:expected_rows AS bigint) <= 0 THEN raw_stored_rows
                        ELSE matched_stored_rows
                    END::bigint AS calendar_matched_rows,
                    CASE
                        WHEN CAST(:expected_rows AS bigint) <= 0 THEN 0
                        ELSE invalid_stored_rows
                    END::bigint AS invalid_stored_eligible_sessions,
                    CASE
                        WHEN CAST(:expected_rows AS bigint) <= 0 THEN 0
                        ELSE off_calendar_stored_rows
                    END::bigint AS off_calendar_rows,
                    CAST(:expected_rows AS bigint) AS expected_rows,
                    greatest(
                        CAST(:expected_rows AS bigint) - CASE
                            WHEN CAST(:expected_rows AS bigint) <= 0 THEN raw_stored_rows
                            ELSE matched_stored_rows
                        END,
                        0
                    )::bigint AS missing_rows,
                    CASE
                        WHEN CAST(:expected_rows AS bigint) <= 0 THEN 0.0
                        ELSE least(matched_stored_rows::float / CAST(:expected_rows AS float), 1.0)
                    END AS coverage_pct,
                    CASE
                        WHEN (CASE
                            WHEN CAST(:expected_rows AS bigint) <= 0 THEN raw_stored_rows
                            ELSE matched_stored_rows
                        END) = 0 THEN 'empty'
                        WHEN CAST(:expected_rows AS bigint) > 0
                            AND matched_stored_rows >= CAST(:expected_rows AS bigint)
                            THEN 'complete'
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
                    coalesce(sum(calendar_matched_rows), 0)::bigint AS calendar_matched_rows,
                    coalesce(sum(invalid_stored_eligible_sessions), 0)::bigint
                        AS invalid_stored_eligible_sessions,
                    coalesce(sum(off_calendar_rows), 0)::bigint AS off_calendar_rows,
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
                s.calendar_matched_rows AS summary_calendar_matched_rows,
                s.invalid_stored_eligible_sessions
                    AS summary_invalid_stored_eligible_sessions,
                s.off_calendar_rows AS summary_off_calendar_rows,
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
            "expected_eligible_sessions": 0,
            "valid_stored_eligible_sessions": 0,
            "invalid_stored_eligible_sessions": 0,
            "coverage_ratio": 0.0,
            "calendar_matched_rows": 0,
            "off_calendar_rows": 0,
            "missing_rows": 0,
            "provider_unavailable_rows": 0,
            "actionable_missing_rows": 0,
            "symbols_provider_limited": 0,
            "symbols_actionable": 0,
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
                "expected_eligible_sessions": int(first["summary_expected_rows"] or 0),
                "valid_stored_eligible_sessions": int(first["summary_calendar_matched_rows"] or 0),
                "invalid_stored_eligible_sessions": int(
                    first["summary_invalid_stored_eligible_sessions"] or 0
                ),
                "coverage_ratio": (
                    float(first["summary_calendar_matched_rows"] or 0)
                    / float(first["summary_expected_rows"])
                    if first["summary_expected_rows"]
                    else 0.0
                ),
                "calendar_matched_rows": int(first["summary_calendar_matched_rows"] or 0),
                "off_calendar_rows": int(first["summary_off_calendar_rows"] or 0),
                "missing_rows": int(first["summary_missing_rows"] or 0),
                "provider_unavailable_rows": 0,
                "actionable_missing_rows": int(first["summary_missing_rows"] or 0),
                "symbols_provider_limited": 0,
                "symbols_actionable": int(first["estimated_provider_calls_for_missing"] or 0),
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
                    "expected_eligible_sessions": int(row["expected_rows"] or 0),
                    "valid_stored_eligible_sessions": int(row["calendar_matched_rows"] or 0),
                    "invalid_stored_eligible_sessions": int(
                        row["invalid_stored_eligible_sessions"] or 0
                    ),
                    "coverage_ratio": float(row["coverage_pct"] or 0.0),
                    "calendar_matched_rows": int(row["calendar_matched_rows"] or 0),
                    "off_calendar_rows": int(row["off_calendar_rows"] or 0),
                    "expected_rows": int(row["expected_rows"] or 0),
                    "coverage_pct": float(row["coverage_pct"] or 0.0),
                    "missing_rows": int(row["missing_rows"] or 0),
                    "provider_unavailable_rows": 0,
                    "actionable_missing_rows": int(row["missing_rows"] or 0),
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
        expected_session_dates: Iterable[date] | None = None,
        limit: int = 50,
        offset: int = 0,
        sort: str = "symbol",
    ) -> dict[str, Any]:
        if (start_date is None) != (end_date is None):
            raise ValueError("start_date and end_date must be supplied together.")
        if start_date and end_date and start_date > end_date:
            raise ValueError("start_date must be on or before end_date.")

        seed_rows = [row for row in symbols if row.get("symbol") and row.get("instrument_key")]
        expected_dates = tuple(dict.fromkeys(expected_session_dates or ()))
        if expected_dates and len(expected_dates) != expected_rows_per_symbol:
            raise ValueError("expected_session_dates must match expected_rows_per_symbol.")
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
                    "expected_eligible_sessions": 0,
                    "valid_stored_eligible_sessions": 0,
                    "invalid_stored_eligible_sessions": 0,
                    "coverage_ratio": 0.0,
                    "calendar_matched_rows": 0,
                    "off_calendar_rows": 0,
                    "missing_rows": 0,
                    "provider_unavailable_rows": 0,
                    "actionable_missing_rows": 0,
                    "symbols_provider_limited": 0,
                    "symbols_actionable": 0,
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

        expected_values_sql = []
        for index, session_date in enumerate(expected_dates):
            date_key = f"expected_date_{index}"
            expected_values_sql.append(f"(:{date_key})")
            params[date_key] = session_date
        expected_sessions_sql = (
            f"VALUES {', '.join(expected_values_sql)}"
            if expected_values_sql
            else "SELECT CAST(NULL AS date) WHERE false"
        )

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
            expected_sessions(session_date) AS (
                {expected_sessions_sql}
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
                    count(d.date)::bigint AS raw_stored_rows,
                    count(d.date) FILTER (
                        WHERE expected.session_date IS NOT NULL
                          AND d.quality_status = 'ok'
                    )::bigint AS matched_stored_rows,
                    count(d.date) FILTER (
                        WHERE expected.session_date IS NOT NULL
                          AND d.quality_status <> 'ok'
                    )::bigint AS invalid_stored_rows,
                    count(d.date) FILTER (
                        WHERE expected.session_date IS NULL
                    )::bigint AS off_calendar_stored_rows,
                    ls.last_successful_run,
                    lf.last_fetch_status
                FROM seed_symbols s
                LEFT JOIN ohlcv_daily d
                    ON d.instrument_key = s.instrument_key
                   AND d.source = :source
                   AND d.exchange = :exchange
                   {ohlcv_date_filter}
                LEFT JOIN expected_sessions expected
                    ON expected.session_date = d.date
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
            verified_evidence_sessions AS (
                SELECT DISTINCT
                    evidence.instrument_key,
                    expected.session_date
                FROM provider_daily_history_evidence evidence
                JOIN seed_symbols seeded
                    ON seeded.instrument_key = evidence.instrument_key
                JOIN expected_sessions expected
                    ON expected.session_date BETWEEN
                        evidence.coverage_start AND evidence.coverage_end
                WHERE evidence.provider = :source
                  AND evidence.exchange = :exchange
                  AND evidence.interval = '1d'
                  AND evidence.status = 'active'
                  AND evidence.classification IN (
                      'verified_complete',
                      'verified_partial'
                  )
            ),
            provider_unavailable AS (
                SELECT
                    evidence.instrument_key,
                    count(*) FILTER (WHERE stored.date IS NULL)::bigint
                        AS provider_unavailable_rows
                FROM verified_evidence_sessions evidence
                LEFT JOIN ohlcv_daily stored
                    ON stored.instrument_key = evidence.instrument_key
                   AND stored.source = :source
                   AND stored.exchange = :exchange
                   AND stored.date = evidence.session_date
                GROUP BY evidence.instrument_key
            ),
            scored AS (
                SELECT
                    base.*,
                    raw_stored_rows AS stored_rows,
                    CASE
                        WHEN CAST(:expected_rows AS bigint) <= 0
                            THEN raw_stored_rows
                        ELSE matched_stored_rows
                    END::bigint AS calendar_matched_rows,
                    CASE
                        WHEN CAST(:expected_rows AS bigint) <= 0 THEN 0
                        ELSE off_calendar_stored_rows
                    END::bigint AS off_calendar_rows,
                    CASE
                        WHEN CAST(:expected_rows AS bigint) <= 0 THEN 0
                        ELSE invalid_stored_rows
                    END::bigint AS invalid_stored_eligible_sessions,
                    CAST(:expected_rows AS bigint) AS expected_rows,
                    greatest(
                        CAST(:expected_rows AS bigint) -
                            CASE
                                WHEN CAST(:expected_rows AS bigint) <= 0
                                    THEN raw_stored_rows
                                ELSE matched_stored_rows
                            END,
                        0
                    )::bigint AS missing_rows,
                    least(
                        greatest(
                            CAST(:expected_rows AS bigint) -
                                CASE
                                    WHEN CAST(:expected_rows AS bigint) <= 0
                                        THEN raw_stored_rows
                                    ELSE matched_stored_rows
                                END,
                            0
                        ),
                        coalesce(unavailable.provider_unavailable_rows, 0)
                    )::bigint AS provider_unavailable_rows,
                    greatest(
                        greatest(
                            CAST(:expected_rows AS bigint) -
                                CASE
                                    WHEN CAST(:expected_rows AS bigint) <= 0
                                        THEN raw_stored_rows
                                    ELSE matched_stored_rows
                                END,
                            0
                        ) - coalesce(unavailable.provider_unavailable_rows, 0),
                        0
                    )::bigint AS actionable_missing_rows,
                    CASE
                        WHEN CAST(:expected_rows AS bigint) <= 0 THEN 0.0
                        ELSE least(
                            matched_stored_rows::float /
                                CAST(:expected_rows AS float),
                            1.0
                        )
                    END AS coverage_pct,
                    CASE
                        WHEN (
                            CASE
                                WHEN CAST(:expected_rows AS bigint) <= 0
                                    THEN raw_stored_rows
                                ELSE matched_stored_rows
                            END
                        ) = 0 THEN 'empty'
                        WHEN CAST(:expected_rows AS bigint) > 0
                            AND matched_stored_rows >= CAST(:expected_rows AS bigint)
                            THEN 'complete'
                        ELSE 'partial'
                    END AS coverage_status
                FROM base
                LEFT JOIN provider_unavailable unavailable
                    ON unavailable.instrument_key = base.instrument_key
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
                    coalesce(sum(calendar_matched_rows), 0)::bigint
                        AS calendar_matched_rows,
                    coalesce(sum(invalid_stored_eligible_sessions), 0)::bigint
                        AS invalid_stored_eligible_sessions,
                    coalesce(sum(off_calendar_rows), 0)::bigint AS off_calendar_rows,
                    coalesce(sum(missing_rows), 0)::bigint AS missing_rows,
                    coalesce(sum(provider_unavailable_rows), 0)::bigint
                        AS provider_unavailable_rows,
                    coalesce(sum(actionable_missing_rows), 0)::bigint
                        AS actionable_missing_rows,
                    count(*) FILTER (
                        WHERE provider_unavailable_rows > 0
                    )::bigint AS symbols_provider_limited,
                    count(*) FILTER (
                        WHERE actionable_missing_rows > 0
                    )::bigint AS symbols_actionable,
                    count(*) FILTER (
                        WHERE actionable_missing_rows > 0
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
                s.calendar_matched_rows AS summary_calendar_matched_rows,
                s.invalid_stored_eligible_sessions
                    AS summary_invalid_stored_eligible_sessions,
                s.off_calendar_rows AS summary_off_calendar_rows,
                s.missing_rows AS summary_missing_rows,
                s.provider_unavailable_rows AS summary_provider_unavailable_rows,
                s.actionable_missing_rows AS summary_actionable_missing_rows,
                s.symbols_provider_limited,
                s.symbols_actionable,
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
            "expected_eligible_sessions": 0,
            "valid_stored_eligible_sessions": 0,
            "invalid_stored_eligible_sessions": 0,
            "coverage_ratio": 0.0,
            "calendar_matched_rows": 0,
            "off_calendar_rows": 0,
            "missing_rows": 0,
            "provider_unavailable_rows": 0,
            "actionable_missing_rows": 0,
            "symbols_provider_limited": 0,
            "symbols_actionable": 0,
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
                "expected_eligible_sessions": int(first["summary_expected_rows"] or 0),
                "valid_stored_eligible_sessions": int(first["summary_calendar_matched_rows"] or 0),
                "invalid_stored_eligible_sessions": int(
                    first["summary_invalid_stored_eligible_sessions"] or 0
                ),
                "coverage_ratio": (
                    float(first["summary_calendar_matched_rows"] or 0)
                    / float(first["summary_expected_rows"])
                    if first["summary_expected_rows"]
                    else 0.0
                ),
                "calendar_matched_rows": int(first["summary_calendar_matched_rows"] or 0),
                "off_calendar_rows": int(first["summary_off_calendar_rows"] or 0),
                "missing_rows": int(first["summary_missing_rows"] or 0),
                "provider_unavailable_rows": int(first["summary_provider_unavailable_rows"] or 0),
                "actionable_missing_rows": int(first["summary_actionable_missing_rows"] or 0),
                "symbols_provider_limited": int(first["symbols_provider_limited"] or 0),
                "symbols_actionable": int(first["symbols_actionable"] or 0),
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
                    "expected_eligible_sessions": int(row["expected_rows"] or 0),
                    "valid_stored_eligible_sessions": int(row["calendar_matched_rows"] or 0),
                    "invalid_stored_eligible_sessions": int(
                        row["invalid_stored_eligible_sessions"] or 0
                    ),
                    "coverage_ratio": float(row["coverage_pct"] or 0.0),
                    "calendar_matched_rows": int(row["calendar_matched_rows"] or 0),
                    "off_calendar_rows": int(row["off_calendar_rows"] or 0),
                    "expected_rows": int(row["expected_rows"] or 0),
                    "coverage_pct": float(row["coverage_pct"] or 0.0),
                    "missing_rows": int(row["missing_rows"] or 0),
                    "provider_unavailable_rows": int(row["provider_unavailable_rows"] or 0),
                    "actionable_missing_rows": int(row["actionable_missing_rows"] or 0),
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

    def search_persisted_symbols(
        self,
        query_text: str,
        *,
        provider: str = "yfinance",
        exchange: str,
        limit: int = 10,
    ) -> list[dict[str, Any]]:
        """Search the accepted active equity universe for typeahead suggestions."""
        normalized = query_text.strip().upper()
        if not normalized:
            return []
        exchange_code = exchange.upper()
        exact = normalized
        prefix = f"{normalized}%"
        contains = f"%{normalized}%"
        ranking = case(
            (func.upper(symbols_table.c.symbol) == exact, 0),
            (func.upper(symbols_table.c.symbol).like(prefix), 1),
            (func.upper(func.coalesce(symbols_table.c.name, "")).like(prefix), 2),
            (func.upper(func.coalesce(symbols_table.c.yahoo_symbol, "")).like(prefix), 3),
            else_=4,
        )
        query = (
            select(
                symbols_table.c.symbol,
                symbols_table.c.yahoo_symbol.label("provider_symbol"),
                symbols_table.c.name,
                symbols_table.c.provider_instrument_key.label("instrument_key"),
                symbols_table.c.canonical_instrument_id,
                symbols_table.c.instrument_type.label("asset_type"),
            )
            .where(symbols_table.c.exchange == exchange_code)
            .where(symbols_table.c.is_active.is_(True))
            .where(symbols_table.c.pipeline_eligibility != "none")
            .where(symbols_table.c.provider_instrument_key.is_not(None))
            .where(
                func.upper(symbols_table.c.symbol).like(contains)
                | func.upper(func.coalesce(symbols_table.c.name, "")).like(contains)
                | func.upper(func.coalesce(symbols_table.c.yahoo_symbol, "")).like(contains)
            )
            .order_by(ranking, symbols_table.c.symbol)
            .limit(max(1, limit))
        )
        with self.engine.begin() as connection:
            rows = [dict(row) for row in connection.execute(query).mappings()]
        return [
            {
                **row,
                "provider": provider.lower(),
                "exchange": exchange_code,
                "isin": None,
                "segment": f"{exchange_code}_EQ",
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
                    member_counts.c.universe_id == tradable_universes_table.c.universe_id,
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
        instrument_keys: Sequence[str] | None = None,
    ) -> pd.DataFrame:
        query = (
            ohlcv_daily_table.select()
            .where(ohlcv_daily_table.c.exchange == exchange.upper())
            .where(ohlcv_daily_table.c.source == source.lower())
            .order_by(ohlcv_daily_table.c.instrument_key, ohlcv_daily_table.c.date)
        )
        if start_date is not None:
            query = query.where(ohlcv_daily_table.c.date >= start_date)
        if end_date is not None:
            query = query.where(ohlcv_daily_table.c.date <= end_date)
        if instrument_keys is not None:
            selected_keys = tuple(dict.fromkeys(str(key) for key in instrument_keys if key))
            if not selected_keys:
                return pd.DataFrame()
            query = query.where(ohlcv_daily_table.c.instrument_key.in_(selected_keys))
        elif limit:
            keys_query = (
                select(ohlcv_daily_table.c.instrument_key)
                .where(ohlcv_daily_table.c.exchange == exchange.upper())
                .where(ohlcv_daily_table.c.source == source.lower())
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

    def daily_ohlcv_instrument_keys(
        self,
        *,
        exchange: str,
        source: str,
        start_date: date | None = None,
        limit: int | None = None,
    ) -> list[str]:
        query = (
            select(ohlcv_daily_table.c.instrument_key)
            .where(ohlcv_daily_table.c.exchange == exchange.upper())
            .where(ohlcv_daily_table.c.source == source.lower())
            .distinct()
            .order_by(ohlcv_daily_table.c.instrument_key)
        )
        if start_date is not None:
            query = query.where(ohlcv_daily_table.c.date >= start_date)
        if limit is not None:
            query = query.limit(limit)
        with self.engine.begin() as connection:
            return [str(row[0]) for row in connection.execute(query).all()]

    def preceding_valid_daily_ohlcv_frame(
        self,
        *,
        exchange: str,
        source: str,
        instrument_keys: Sequence[str],
        before_date: date,
    ) -> pd.DataFrame:
        selected_keys = tuple(dict.fromkeys(str(key) for key in instrument_keys if key))
        if not selected_keys:
            return pd.DataFrame()
        valid = (
            ohlcv_daily_table.c.open > 0,
            ohlcv_daily_table.c.high > 0,
            ohlcv_daily_table.c.low > 0,
            ohlcv_daily_table.c.close > 0,
            ohlcv_daily_table.c.volume >= 0,
            ohlcv_daily_table.c.high >= ohlcv_daily_table.c.low,
            ohlcv_daily_table.c.high >= ohlcv_daily_table.c.open,
            ohlcv_daily_table.c.high >= ohlcv_daily_table.c.close,
            ohlcv_daily_table.c.low <= ohlcv_daily_table.c.open,
            ohlcv_daily_table.c.low <= ohlcv_daily_table.c.close,
        )
        ranked = (
            select(
                *ohlcv_daily_table.c,
                func.row_number()
                .over(
                    partition_by=ohlcv_daily_table.c.instrument_key,
                    order_by=ohlcv_daily_table.c.date.desc(),
                )
                .label("_opportunity_predecessor_rank"),
            )
            .where(ohlcv_daily_table.c.exchange == exchange.upper())
            .where(ohlcv_daily_table.c.source == source.lower())
            .where(ohlcv_daily_table.c.instrument_key.in_(selected_keys))
            .where(ohlcv_daily_table.c.date < before_date)
            .where(*valid)
            .subquery()
        )
        query = (
            select(*(ranked.c[column.name] for column in ohlcv_daily_table.columns))
            .where(ranked.c._opportunity_predecessor_rank == 1)
            .order_by(ranked.c.instrument_key, ranked.c.date)
        )
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
        return IngestionRunRepository(self.engine, ingestion_runs_table).start(
            job_name=job_name,
            exchange=exchange,
            source=source,
            items_requested=items_requested,
            run_metadata=run_metadata,
        )

    def finish_ingestion_run(
        self,
        run_id: UUID,
        status: str,
        items_processed: int,
        items_succeeded: int,
        items_failed: int,
        error_message: str | None = None,
        run_metadata_patch: Mapping[str, Any] | None = None,
    ) -> None:
        IngestionRunRepository(self.engine, ingestion_runs_table).finish(
            run_id=run_id,
            status=status,
            items_processed=items_processed,
            items_succeeded=items_succeeded,
            items_failed=items_failed,
            error_message=error_message,
            run_metadata_patch=run_metadata_patch,
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

    def provider_runs(
        self,
        limit: int = 50,
        offset: int = 0,
        source: str | None = None,
        exchange: str | None = None,
        job_name: str | None = None,
        status: str | None = None,
        start_date: date | None = None,
        end_date: date | None = None,
    ) -> list[dict[str, Any]]:
        query = ingestion_runs_table.select().order_by(ingestion_runs_table.c.started_at.desc())
        if source:
            query = query.where(ingestion_runs_table.c.source == source.lower())
        if exchange:
            exchange_code = exchange.upper()
            work_item_exchange_match = (
                select(1)
                .select_from(pipeline_work_items_table)
                .where(pipeline_work_items_table.c.run_id == ingestion_runs_table.c.run_id)
                .where(pipeline_work_items_table.c.exchange == exchange_code)
                .exists()
            )
            query = query.where(
                (ingestion_runs_table.c.exchange == exchange_code) | work_item_exchange_match
            )
        if job_name:
            query = query.where(ingestion_runs_table.c.job_name == job_name)
        if status:
            query = query.where(ingestion_runs_table.c.status == status)
        if start_date:
            query = query.where(ingestion_runs_table.c.started_at >= start_date)
        if end_date:
            query = query.where(ingestion_runs_table.c.started_at < end_date + timedelta(days=1))
        query = query.limit(max(limit, 1)).offset(max(offset, 0))
        with self.engine.begin() as connection:
            rows = [dict(row) for row in connection.execute(query).mappings()]
            run_ids = [str(row["run_id"]) for row in rows]
            work_item_exchanges: dict[str, set[str]] = {}
            if run_ids:
                exchange_rows = connection.execute(
                    select(
                        pipeline_work_items_table.c.run_id,
                        pipeline_work_items_table.c.exchange,
                    )
                    .where(pipeline_work_items_table.c.run_id.in_(run_ids))
                    .distinct()
                ).mappings()
                for exchange_row in exchange_rows:
                    work_item_exchanges.setdefault(str(exchange_row["run_id"]), set()).add(
                        str(exchange_row["exchange"])
                    )
        for row in rows:
            actual_exchanges = work_item_exchanges.get(str(row["run_id"]), set())
            if not actual_exchanges and str(row["exchange"]).upper() != "MULTI":
                actual_exchanges = {str(row["exchange"])}
            row["work_item_exchanges"] = sorted(actual_exchanges)
        return rows

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
            query = query.where(daily_ohlcv_fetch_coverage_table.c.exchange == exchange.upper())
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

    def provider_request_logs(
        self,
        limit: int = 100,
        offset: int = 0,
        run_id: str | None = None,
        provider: str | None = None,
        endpoint_group: str | None = None,
        status: str | None = None,
        exchange: str | None = None,
        job_name: str | None = None,
        start_date: date | None = None,
        end_date: date | None = None,
    ) -> list[dict[str, Any]]:
        query = provider_request_log_table.select().order_by(
            provider_request_log_table.c.created_at.desc()
        )
        if exchange or job_name:
            query = query.select_from(
                provider_request_log_table.outerjoin(
                    ingestion_runs_table,
                    provider_request_log_table.c.run_id == ingestion_runs_table.c.run_id,
                )
            )
        if run_id:
            query = query.where(provider_request_log_table.c.run_id == run_id)
        if provider:
            query = query.where(provider_request_log_table.c.provider == provider.lower())
        if endpoint_group:
            query = query.where(provider_request_log_table.c.endpoint_group == endpoint_group)
        if status:
            query = query.where(provider_request_log_table.c.status == status)
        if exchange:
            query = query.where(ingestion_runs_table.c.exchange == exchange.upper())
        if job_name:
            query = query.where(ingestion_runs_table.c.job_name == job_name)
        if start_date:
            query = query.where(provider_request_log_table.c.created_at >= start_date)
        if end_date:
            query = query.where(
                provider_request_log_table.c.created_at < end_date + timedelta(days=1)
            )
        query = query.limit(max(limit, 1)).offset(max(offset, 0))
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
            func.sum(case((provider_request_log_table.c.rate_limited.is_(True), 1), else_=0)).label(
                "rate_limited_requests"
            ),
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

    def provider_request_summary(
        self,
        run_id: str | None = None,
        provider: str | None = None,
        endpoint_group: str | None = None,
        status: str | None = None,
        exchange: str | None = None,
        job_name: str | None = None,
        start_date: date | None = None,
        end_date: date | None = None,
    ) -> list[dict[str, Any]]:
        query = select(
            provider_request_log_table.c.provider,
            provider_request_log_table.c.endpoint_group,
            provider_request_log_table.c.status,
            func.count().label("requests"),
            func.sum(provider_request_log_table.c.wait_seconds).label("wait_seconds"),
            func.avg(provider_request_log_table.c.duration_ms).label("avg_duration_ms"),
            func.sum(case((provider_request_log_table.c.rate_limited.is_(True), 1), else_=0)).label(
                "rate_limited_requests"
            ),
        )
        if exchange or job_name:
            query = query.select_from(
                provider_request_log_table.outerjoin(
                    ingestion_runs_table,
                    provider_request_log_table.c.run_id == ingestion_runs_table.c.run_id,
                )
            )
        if run_id:
            query = query.where(provider_request_log_table.c.run_id == run_id)
        if provider:
            query = query.where(provider_request_log_table.c.provider == provider.lower())
        if endpoint_group:
            query = query.where(provider_request_log_table.c.endpoint_group == endpoint_group)
        if status:
            query = query.where(provider_request_log_table.c.status == status)
        if exchange:
            query = query.where(ingestion_runs_table.c.exchange == exchange.upper())
        if job_name:
            query = query.where(ingestion_runs_table.c.job_name == job_name)
        if start_date:
            query = query.where(provider_request_log_table.c.created_at >= start_date)
        if end_date:
            query = query.where(
                provider_request_log_table.c.created_at < end_date + timedelta(days=1)
            )
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

    def market_status(self, source: str = "yfinance") -> list[dict[str, Any]]:
        """Summarize accepted daily candles for the latest accepted active universe."""
        snapshots = (
            select(
                universe_snapshots_table.c.snapshot_id,
                universe_snapshots_table.c.exchange,
                func.row_number()
                .over(
                    partition_by=universe_snapshots_table.c.exchange,
                    order_by=(
                        universe_snapshots_table.c.fetched_at.desc(),
                        universe_snapshots_table.c.snapshot_id.desc(),
                    ),
                )
                .label("rank"),
            )
            .where(universe_snapshots_table.c.status == "accepted")
            .cte("snapshots")
        )
        active = (
            select(
                symbols_table.c.exchange,
                symbols_table.c.provider_instrument_key.label("instrument_key"),
            )
            .select_from(
                symbols_table.join(
                    universe_snapshot_members_table,
                    symbols_table.c.canonical_instrument_id
                    == universe_snapshot_members_table.c.canonical_instrument_id,
                ).join(
                    snapshots,
                    (snapshots.c.snapshot_id == universe_snapshot_members_table.c.snapshot_id)
                    & (snapshots.c.exchange == symbols_table.c.exchange),
                )
            )
            .where(
                snapshots.c.rank == 1,
                symbols_table.c.is_active.is_(True),
                symbols_table.c.pipeline_eligibility != "none",
                symbols_table.c.provider_instrument_key.is_not(None),
            )
            .distinct()
            .cte("active")
        )
        universe = (
            select(active.c.exchange, func.count().label("universe_size"))
            .group_by(active.c.exchange)
            .cte("universe")
        )
        accepted = (
            select(
                ohlcv_daily_table.c.exchange,
                ohlcv_daily_table.c.instrument_key,
                ohlcv_daily_table.c.date,
            )
            .select_from(
                ohlcv_daily_table.join(
                    active,
                    (active.c.exchange == ohlcv_daily_table.c.exchange)
                    & (active.c.instrument_key == ohlcv_daily_table.c.instrument_key),
                )
            )
            .where(ohlcv_daily_table.c.source == source, ohlcv_daily_table.c.quality_status == "ok")
            .cte("accepted")
        )
        latest = (
            select(accepted.c.exchange, func.max(accepted.c.date).label("latest_candle"))
            .group_by(accepted.c.exchange)
            .cte("latest")
        )
        candles = (
            select(
                accepted.c.exchange,
                latest.c.latest_candle,
                func.count(func.distinct(accepted.c.instrument_key)).label("candle_symbols"),
            )
            .select_from(
                accepted.join(
                    latest,
                    (accepted.c.exchange == latest.c.exchange)
                    & (accepted.c.date == latest.c.latest_candle),
                )
            )
            .group_by(accepted.c.exchange, latest.c.latest_candle)
            .cte("candles")
        )
        runs = (
            select(
                ingestion_runs_table.c.exchange,
                ingestion_runs_table.c.started_at,
                ingestion_runs_table.c.status,
                func.row_number()
                .over(
                    partition_by=ingestion_runs_table.c.exchange,
                    order_by=(
                        ingestion_runs_table.c.started_at.desc(),
                        ingestion_runs_table.c.run_id.desc(),
                    ),
                )
                .label("rank"),
            )
            .where(
                ingestion_runs_table.c.source == source,
                (
                    ingestion_runs_table.c.job_name.like("%daily_ohlcv")
                    | (ingestion_runs_table.c.job_name == "yfinance_daily_work_queue")
                ),
            )
            .cte("daily_runs")
        )
        query = (
            select(
                universe.c.exchange,
                universe.c.universe_size,
                candles.c.latest_candle,
                func.coalesce(candles.c.candle_symbols, 0).label("candle_symbols"),
                runs.c.started_at.label("last_ohlcv_run"),
                runs.c.status.label("last_ohlcv_status"),
            )
            .select_from(
                universe.outerjoin(candles, universe.c.exchange == candles.c.exchange).outerjoin(
                    runs, (universe.c.exchange == runs.c.exchange) & (runs.c.rank == 1)
                )
            )
            .order_by(universe.c.exchange)
        )
        with self.engine.begin() as connection:
            return [dict(row) for row in connection.execute(query).mappings()]

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
        return _deduplicate_rows(rows, ("source", "instrument_key"))

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
        return _deduplicate_rows(rows, ("universe_id", "symbol"))

    @staticmethod
    def _daily_ohlcv_rows(frame: pd.DataFrame, exchange: str, source: str) -> list[dict[str, Any]]:
        if frame.empty:
            return []

        fetched_at = datetime.now(UTC)
        rows = []
        for record in frame.to_dict(orient="records"):
            normalized = "instrument_key" in record
            required_columns = (
                ["date", "open", "high", "low", "close", "volume"]
                if normalized
                else ["Date", "Open", "High", "Low", "Close", "Volume"]
            )
            if any(pd.isna(record.get(column)) for column in required_columns):
                continue
            row = {
                "instrument_key": str(
                    record["instrument_key"] if normalized else record["InstrumentKey"]
                ),
                "source": str(record.get("source") or source).lower(),
                "date": _nullable_date(record["date"] if normalized else record["Date"]),
                "symbol": str(record["symbol"] if normalized else record["Symbol"]).upper(),
                "exchange": str(record.get("exchange") or exchange).upper(),
                "open": float(record["open"] if normalized else record["Open"]),
                "high": float(record["high"] if normalized else record["High"]),
                "low": float(record["low"] if normalized else record["Low"]),
                "close": float(record["close"] if normalized else record["Close"]),
                "volume": int(record["volume"] if normalized else record["Volume"]),
                "open_interest": _nullable_int(
                    record.get("open_interest") if normalized else record.get("OpenInterest")
                ),
                "fetched_at": _as_utc(record.get("fetched_at") or fetched_at),
            }
            row["quality_status"] = str(record.get("quality_status") or _quality_status(row))
            rows.append(row)
        return _deduplicate_rows(rows, ("instrument_key", "source", "date"))

    @staticmethod
    def _daily_price_adjustment_rows(
        frame: pd.DataFrame,
        exchange: str,
        source: str,
    ) -> list[dict[str, Any]]:
        if frame.empty or "AdjClose" not in frame.columns:
            return []

        fetched_at = datetime.now(UTC)
        rows = []
        for record in frame.to_dict(orient="records"):
            raw_close = _nullable_float(record.get("Close"))
            adjusted_close = _nullable_float(record.get("AdjClose"))
            if raw_close is None or adjusted_close is None:
                continue
            if raw_close <= 0 or adjusted_close <= 0:
                continue
            candle_date = _nullable_date(record.get("Date"))
            instrument_key = _clean_string(record.get("InstrumentKey"))
            symbol = _clean_string(record.get("Symbol"))
            if candle_date is None or not instrument_key or not symbol:
                continue
            rows.append(
                {
                    "instrument_key": instrument_key,
                    "source": source,
                    "date": candle_date,
                    "symbol": symbol.upper(),
                    "exchange": exchange,
                    "raw_close": raw_close,
                    "adjusted_close": adjusted_close,
                    "adjustment_factor": adjusted_close / raw_close,
                    "fetched_at": fetched_at,
                }
            )
        return _deduplicate_rows(rows, ("instrument_key", "source", "date"))

    @staticmethod
    def _corporate_action_rows(
        frame: pd.DataFrame,
        exchange: str,
        source: str,
    ) -> list[dict[str, Any]]:
        if frame.empty:
            return []

        fetched_at = datetime.now(UTC)
        rows = []
        for record in frame.to_dict(orient="records"):
            instrument_key = _clean_string(record.get("InstrumentKey"))
            symbol = _clean_string(record.get("Symbol"))
            action_date = _nullable_date(record.get("ActionDate") or record.get("Date"))
            action_type = _clean_string(record.get("ActionType"))
            if not instrument_key or not symbol or action_date is None or not action_type:
                continue
            rows.append(
                {
                    "source": source,
                    "instrument_key": instrument_key,
                    "action_date": action_date,
                    "action_type": action_type.lower(),
                    "symbol": symbol.upper(),
                    "exchange": exchange,
                    "value": _nullable_float(record.get("Value")),
                    "currency": _clean_string(record.get("Currency")),
                    "raw": record.get("Raw") if isinstance(record.get("Raw"), dict) else {},
                    "fetched_at": fetched_at,
                }
            )
        return _deduplicate_rows(
            rows,
            ("source", "instrument_key", "action_date", "action_type"),
        )

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
        return _deduplicate_rows(rows, ("run_id", "instrument_key"))

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
        return _deduplicate_rows(rows, ("id",))


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


def _chunks(rows: list[_ChunkItem], size: int) -> Iterable[list[_ChunkItem]]:
    for start in range(0, len(rows), size):
        yield rows[start : start + size]


def _deduplicate_rows(
    rows: list[dict[str, Any]],
    key_columns: tuple[str, ...],
) -> list[dict[str, Any]]:
    """Return at most one row per database conflict key, with the last row winning."""
    deduplicated: dict[tuple[Any, ...], dict[str, Any]] = {}
    for row in rows:
        deduplicated[tuple(row[column] for column in key_columns)] = row
    return list(deduplicated.values())


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


def _nullable_datetime(value: Any) -> datetime | None:
    if value is None or pd.isna(value):
        return None
    if isinstance(value, datetime):
        return _as_utc(value)
    try:
        parsed = datetime.fromisoformat(str(value).replace("Z", "+00:00"))
    except ValueError:
        try:
            parsed = pd.to_datetime(value, utc=True).to_pydatetime()
        except (TypeError, ValueError):
            return None
    return _as_utc(parsed)
