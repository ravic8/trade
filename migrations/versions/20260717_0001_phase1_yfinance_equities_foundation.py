"""Add the Phase 1 yfinance equities foundation.

Revision ID: 20260717_0001
Revises: None
Create Date: 2026-07-17
"""

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op

revision: str = "20260717_0001"
down_revision: str | None = None
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None

_NEW_TABLES = (
    "daily_coverage_summary",
    "adaptive_rate_state",
    "pipeline_work_items",
    "symbol_lifecycle_events",
    "instrument_aliases",
    "universe_snapshot_members",
    "universe_snapshots",
    "exchange_sessions",
)


def _table_names() -> set[str]:
    return set(sa.inspect(op.get_bind()).get_table_names())


def _column_names(table_name: str) -> set[str]:
    return {
        str(column["name"])
        for column in sa.inspect(op.get_bind()).get_columns(table_name)
    }


def upgrade() -> None:
    tables = _table_names()
    if "symbols" in tables:
        columns = _column_names("symbols")
        additions = (
            sa.Column("canonical_instrument_id", sa.String(), nullable=True),
            sa.Column("first_seen_at", sa.DateTime(timezone=True), nullable=True),
            sa.Column("last_seen_at", sa.DateTime(timezone=True), nullable=True),
            sa.Column("inactive_at", sa.DateTime(timezone=True), nullable=True),
            sa.Column("inactive_reason", sa.String(), nullable=True),
            sa.Column(
                "consecutive_missing_refreshes",
                sa.BigInteger(),
                nullable=False,
                server_default=sa.text("0"),
            ),
            sa.Column("last_universe_snapshot_id", sa.String(), nullable=True),
        )
        for column in additions:
            if column.name not in columns:
                op.add_column("symbols", column)

    if "exchange_sessions" not in tables:
        op.create_table(
            "exchange_sessions",
            sa.Column("exchange", sa.String(), nullable=False),
            sa.Column("session_date", sa.Date(), nullable=False),
            sa.Column("is_trading_day", sa.Boolean(), nullable=False),
            sa.Column("market_open_utc", sa.DateTime(timezone=True), nullable=True),
            sa.Column("market_close_utc", sa.DateTime(timezone=True), nullable=True),
            sa.Column(
                "is_early_close",
                sa.Boolean(),
                nullable=False,
                server_default=sa.text("false"),
            ),
            sa.Column("source_url", sa.String(), nullable=False),
            sa.Column("calendar_version", sa.String(), nullable=False),
            sa.Column("validation_status", sa.String(), nullable=False),
            sa.Column("generated_at", sa.DateTime(timezone=True), nullable=False),
            sa.PrimaryKeyConstraint("exchange", "session_date"),
        )
        op.create_index(
            "idx_exchange_sessions_open_date",
            "exchange_sessions",
            ["exchange", "is_trading_day", "session_date"],
        )

    if "universe_snapshots" not in tables:
        op.create_table(
            "universe_snapshots",
            sa.Column("snapshot_id", sa.String(), nullable=False),
            sa.Column("exchange", sa.String(), nullable=False),
            sa.Column("source", sa.String(), nullable=False),
            sa.Column("status", sa.String(), nullable=False),
            sa.Column("fetched_at", sa.DateTime(timezone=True), nullable=False),
            sa.Column("symbol_count", sa.BigInteger(), nullable=False, server_default="0"),
            sa.Column("validation_json", sa.JSON(), nullable=False, server_default="{}"),
            sa.Column("error_message", sa.Text(), nullable=True),
            sa.Column("created_at", sa.DateTime(timezone=True), nullable=False),
            sa.PrimaryKeyConstraint("snapshot_id"),
        )
        op.create_index(
            "idx_universe_snapshots_exchange_fetched",
            "universe_snapshots",
            ["exchange", "fetched_at"],
        )

    if "universe_snapshot_members" not in tables:
        op.create_table(
            "universe_snapshot_members",
            sa.Column("snapshot_id", sa.String(), nullable=False),
            sa.Column("canonical_instrument_id", sa.String(), nullable=False),
            sa.Column("exchange_symbol", sa.String(), nullable=False),
            sa.Column("provider_symbol", sa.String(), nullable=True),
            sa.Column("name", sa.String(), nullable=True),
            sa.Column("raw_metadata", sa.JSON(), nullable=False, server_default="{}"),
            sa.PrimaryKeyConstraint("snapshot_id", "canonical_instrument_id"),
        )

    if "instrument_aliases" not in tables:
        op.create_table(
            "instrument_aliases",
            sa.Column("alias_id", sa.String(), nullable=False),
            sa.Column("canonical_instrument_id", sa.String(), nullable=False),
            sa.Column("provider", sa.String(), nullable=False),
            sa.Column("provider_symbol", sa.String(), nullable=False),
            sa.Column("valid_from", sa.DateTime(timezone=True), nullable=False),
            sa.Column("valid_to", sa.DateTime(timezone=True), nullable=True),
            sa.Column("is_current", sa.Boolean(), nullable=False, server_default="true"),
            sa.PrimaryKeyConstraint("alias_id"),
            sa.UniqueConstraint(
                "canonical_instrument_id",
                "provider",
                "provider_symbol",
                "valid_from",
                name="uq_instrument_alias_identity",
            ),
        )
        op.create_index(
            "idx_instrument_aliases_current",
            "instrument_aliases",
            ["canonical_instrument_id", "provider", "is_current"],
        )

    if "symbol_lifecycle_events" not in tables:
        op.create_table(
            "symbol_lifecycle_events",
            sa.Column("event_id", sa.String(), nullable=False),
            sa.Column("canonical_instrument_id", sa.String(), nullable=False),
            sa.Column("exchange", sa.String(), nullable=False),
            sa.Column("event_type", sa.String(), nullable=False),
            sa.Column("old_value", sa.JSON(), nullable=True),
            sa.Column("new_value", sa.JSON(), nullable=True),
            sa.Column("snapshot_id", sa.String(), nullable=True),
            sa.Column("created_at", sa.DateTime(timezone=True), nullable=False),
            sa.PrimaryKeyConstraint("event_id"),
        )
        op.create_index(
            "idx_symbol_lifecycle_events_exchange_created",
            "symbol_lifecycle_events",
            ["exchange", "created_at"],
        )

    if "pipeline_work_items" not in tables:
        op.create_table(
            "pipeline_work_items",
            sa.Column("work_item_id", sa.String(), nullable=False),
            sa.Column("idempotency_key", sa.String(), nullable=False),
            sa.Column("work_type", sa.String(), nullable=False),
            sa.Column("provider", sa.String(), nullable=False),
            sa.Column("exchange", sa.String(), nullable=False),
            sa.Column("canonical_instrument_id", sa.String(), nullable=False),
            sa.Column("provider_symbol", sa.String(), nullable=False),
            sa.Column("interval", sa.String(), nullable=False),
            sa.Column("window_start", sa.Date(), nullable=False),
            sa.Column("window_end", sa.Date(), nullable=False),
            sa.Column("priority", sa.BigInteger(), nullable=False, server_default="100"),
            sa.Column("status", sa.String(), nullable=False),
            sa.Column("attempt_count", sa.BigInteger(), nullable=False, server_default="0"),
            sa.Column("max_attempts", sa.BigInteger(), nullable=False, server_default="9"),
            sa.Column("next_attempt_at", sa.DateTime(timezone=True), nullable=True),
            sa.Column("locked_by", sa.String(), nullable=True),
            sa.Column("locked_at", sa.DateTime(timezone=True), nullable=True),
            sa.Column("run_id", sa.String(), nullable=True),
            sa.Column("parent_work_item_id", sa.String(), nullable=True),
            sa.Column("last_status_code", sa.BigInteger(), nullable=True),
            sa.Column("last_error_code", sa.String(), nullable=True),
            sa.Column("last_error_message", sa.Text(), nullable=True),
            sa.Column("created_at", sa.DateTime(timezone=True), nullable=False),
            sa.Column("updated_at", sa.DateTime(timezone=True), nullable=False),
            sa.Column("completed_at", sa.DateTime(timezone=True), nullable=True),
            sa.PrimaryKeyConstraint("work_item_id"),
            sa.UniqueConstraint(
                "idempotency_key",
                name="uq_pipeline_work_items_idempotency_key",
            ),
        )
        op.create_index(
            "idx_pipeline_work_items_claim",
            "pipeline_work_items",
            ["status", "next_attempt_at", "priority", "created_at"],
        )

    if "adaptive_rate_state" not in tables:
        op.create_table(
            "adaptive_rate_state",
            sa.Column("provider", sa.String(), nullable=False),
            sa.Column("current_rpm", sa.BigInteger(), nullable=False),
            sa.Column("last_safe_rpm", sa.BigInteger(), nullable=True),
            sa.Column("minimum_rpm", sa.BigInteger(), nullable=False),
            sa.Column("maximum_rpm", sa.BigInteger(), nullable=False),
            sa.Column("current_concurrency", sa.BigInteger(), nullable=False),
            sa.Column(
                "consecutive_healthy_windows",
                sa.BigInteger(),
                nullable=False,
                server_default="0",
            ),
            sa.Column("circuit_state", sa.String(), nullable=False, server_default="closed"),
            sa.Column("cooldown_until", sa.DateTime(timezone=True), nullable=True),
            sa.Column("last_429_at", sa.DateTime(timezone=True), nullable=True),
            sa.Column("recent_error_rate", sa.Float(), nullable=False, server_default="0"),
            sa.Column("latency_baseline_ms", sa.Float(), nullable=True),
            sa.Column("updated_at", sa.DateTime(timezone=True), nullable=False),
            sa.PrimaryKeyConstraint("provider"),
        )

    if "daily_coverage_summary" not in tables:
        op.create_table(
            "daily_coverage_summary",
            sa.Column("source", sa.String(), nullable=False),
            sa.Column("exchange", sa.String(), nullable=False),
            sa.Column("instrument_key", sa.String(), nullable=False),
            sa.Column("interval", sa.String(), nullable=False),
            sa.Column("as_of_date", sa.Date(), nullable=False),
            sa.Column("symbol", sa.String(), nullable=False),
            sa.Column("first_expected_date", sa.Date(), nullable=True),
            sa.Column("first_stored_date", sa.Date(), nullable=True),
            sa.Column("latest_expected_date", sa.Date(), nullable=True),
            sa.Column("latest_stored_date", sa.Date(), nullable=True),
            sa.Column("expected_rows", sa.BigInteger(), nullable=False, server_default="0"),
            sa.Column("stored_rows", sa.BigInteger(), nullable=False, server_default="0"),
            sa.Column("missing_rows", sa.BigInteger(), nullable=False, server_default="0"),
            sa.Column("coverage_pct", sa.Float(), nullable=False, server_default="0"),
            sa.Column("coverage_status", sa.String(), nullable=False),
            sa.Column("freshness_status", sa.String(), nullable=False),
            sa.Column("last_successful_run", sa.String(), nullable=True),
            sa.Column("last_fetch_status", sa.String(), nullable=True),
            sa.Column("next_retry_at", sa.DateTime(timezone=True), nullable=True),
            sa.Column("attempt_count", sa.BigInteger(), nullable=False, server_default="0"),
            sa.Column("lifecycle_status", sa.String(), nullable=False),
            sa.Column("updated_at", sa.DateTime(timezone=True), nullable=False),
            sa.PrimaryKeyConstraint(
                "source",
                "exchange",
                "instrument_key",
                "interval",
                "as_of_date",
            ),
        )
        op.create_index(
            "idx_daily_coverage_summary_exchange_status",
            "daily_coverage_summary",
            ["exchange", "coverage_status", "as_of_date"],
        )


def downgrade() -> None:
    tables = _table_names()
    for table_name in _NEW_TABLES:
        if table_name in tables:
            op.drop_table(table_name)

    if "symbols" in tables:
        columns = _column_names("symbols")
        for column_name in (
            "last_universe_snapshot_id",
            "consecutive_missing_refreshes",
            "inactive_reason",
            "inactive_at",
            "last_seen_at",
            "first_seen_at",
            "canonical_instrument_id",
        ):
            if column_name in columns:
                op.drop_column("symbols", column_name)
