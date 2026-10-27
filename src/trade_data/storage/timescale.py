from __future__ import annotations

from collections.abc import Iterable, Mapping, Sequence
from datetime import UTC, date, datetime, timedelta
from typing import Any, TypeVar

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
    create_engine,
    func,
    select,
    text,
)
from sqlalchemy.dialects.postgresql import insert

from trade_data.storage.legacy_schema import (
    DAILY_OPPORTUNITY_TARGET_COLUMNS_V1_0,
    FEATURE_COLUMNS_V1_0,
)

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
            .where(ohlcv_daily_table.c.source == "nse_official_bhavcopy")
            .where(ohlcv_daily_table.c.date >= start_date)
            .where(ohlcv_daily_table.c.date <= end_date)
            .group_by(ohlcv_daily_table.c.date)
            .having(
                func.count(func.distinct(ohlcv_daily_table.c.instrument_key)) >= minimum_instruments
            )
        )
        with self.engine.begin() as connection:
            return set(connection.execute(query).scalars())

    def upsert_daily_ohlcv(
        self,
        frame: pd.DataFrame,
        exchange: str = "NSE",
        source: str = "nse_official_bhavcopy",
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

    def daily_ohlcv_frame(
        self,
        exchange: str = "NSE",
        source: str = "nse_official_bhavcopy",
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
            row["quality_status"] = (
                "suspicious"
                if _quality_status(row) != "ok"
                else str(record.get("quality_status") or "ok")
            )
            rows.append(row)
        return _deduplicate_rows(rows, ("instrument_key", "source", "date"))


def make_timescale_store(database_url: str) -> TimescaleStore:
    return TimescaleStore(database_url)


def _quality_status(row: Mapping[str, Any]) -> str:
    from trade_data.market_data.price_quality import daily_price_errors

    return "suspicious" if daily_price_errors(row) else "ok"


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
