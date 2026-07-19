"""Add analytics views and durable BigQuery synchronization state.

Revision ID: 20260719_0007
Revises: 20260718_0006
Create Date: 2026-07-19
"""

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op

revision: str = "20260719_0007"
down_revision: str | None = "20260718_0006"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


ANALYTICS_VIEWS: dict[str, str] = {
    "ohlcv_daily": """
        SELECT instrument_key, source, date, symbol, exchange,
               open, high, low, close, volume, open_interest,
               fetched_at, quality_status
        FROM public.ohlcv_daily
    """,
    "symbol_state": """
        SELECT symbol, exchange, yahoo_symbol, name, currency, source,
               canonical_instrument_id, source_identity, provider_instrument_key,
               is_active, listing_status, listing_status_reason,
               listing_status_effective_at, pipeline_eligibility,
               provider_status, provider_status_reason, provider_status_updated_at,
               instrument_type, reconciliation_status, reconciliation_reason,
               official_sector, official_security_type, first_seen_at, last_seen_at,
               inactive_at, inactive_reason, consecutive_missing_refreshes,
               last_universe_snapshot_id, fetched_at
        FROM public.symbols
    """,
    "pipeline_work_state": """
        SELECT work_item_id, idempotency_key, work_type, provider, exchange,
               canonical_instrument_id, provider_symbol, interval,
               window_start, window_end, priority, status, attempt_count,
               max_attempts, next_attempt_at, locked_by, locked_at, run_id,
               parent_work_item_id, last_status_code, last_error_code,
               last_error_message, created_at, updated_at, completed_at
        FROM public.pipeline_work_items
    """,
    "ingestion_runs": """
        SELECT run_id, job_name, status, exchange, source, started_at, finished_at,
               items_requested, items_processed, items_succeeded, items_failed,
               error_message, run_metadata
        FROM public.ingestion_runs
    """,
    "provider_health": """
        SELECT COALESCE(rate.provider, requests.provider) AS provider,
               rate.current_rpm, rate.last_safe_rpm,
               rate.minimum_rpm, rate.maximum_rpm, rate.current_concurrency,
               rate.consecutive_healthy_windows, rate.circuit_state,
               rate.cooldown_until, rate.last_429_at, rate.recent_error_rate,
               rate.latency_baseline_ms, rate.updated_at,
               COALESCE(requests.requests_24h, 0) AS requests_24h,
               COALESCE(requests.rate_limited_24h, 0) AS rate_limited_24h,
               requests.last_request_at
        FROM public.adaptive_rate_state AS rate
        FULL OUTER JOIN (
            SELECT provider,
                   COUNT(*) FILTER (
                       WHERE created_at >= CURRENT_TIMESTAMP - INTERVAL '24 hours'
                   ) AS requests_24h,
                   COUNT(*) FILTER (
                       WHERE rate_limited
                         AND created_at >= CURRENT_TIMESTAMP - INTERVAL '24 hours'
                   ) AS rate_limited_24h,
                   MAX(created_at) AS last_request_at
            FROM public.provider_request_log
            GROUP BY provider
        ) AS requests ON requests.provider = rate.provider
    """,
    "universe_lifecycle": """
        SELECT event.event_id, event.canonical_instrument_id, event.exchange,
               symbol.symbol, symbol.yahoo_symbol AS provider_symbol,
               event.event_type, event.old_value, event.new_value,
               event.snapshot_id, event.created_at,
               symbol.is_active, symbol.listing_status,
               symbol.pipeline_eligibility, symbol.provider_status
        FROM public.symbol_lifecycle_events AS event
        LEFT JOIN public.symbols AS symbol
          ON symbol.canonical_instrument_id = event.canonical_instrument_id
         AND symbol.exchange = event.exchange
    """,
}


def upgrade() -> None:
    tables = set(sa.inspect(op.get_bind()).get_table_names())
    if "bigquery_sync_runs" not in tables:
        op.create_table(
            "bigquery_sync_runs",
            sa.Column("run_id", sa.String(), nullable=False),
            sa.Column("trigger", sa.String(), nullable=False),
            sa.Column("status", sa.String(), nullable=False),
            sa.Column("project_id", sa.String(), nullable=False),
            sa.Column("dataset", sa.String(), nullable=False),
            sa.Column("location", sa.String(), nullable=False),
            sa.Column("exchange", sa.String(), nullable=True),
            sa.Column("year", sa.BigInteger(), nullable=True),
            sa.Column("entities", sa.JSON(), nullable=False),
            sa.Column("started_at", sa.DateTime(timezone=True), nullable=False),
            sa.Column("finished_at", sa.DateTime(timezone=True), nullable=True),
            sa.Column("source_row_count", sa.BigInteger(), nullable=False, server_default="0"),
            sa.Column("destination_row_count", sa.BigInteger(), nullable=False, server_default="0"),
            sa.Column("count_difference", sa.BigInteger(), nullable=False, server_default="0"),
            sa.Column("inserted_rows", sa.BigInteger(), nullable=False, server_default="0"),
            sa.Column("updated_rows", sa.BigInteger(), nullable=False, server_default="0"),
            sa.Column("rejected_rows", sa.BigInteger(), nullable=False, server_default="0"),
            sa.Column("retry_count", sa.BigInteger(), nullable=False, server_default="0"),
            sa.Column("duration_seconds", sa.Float(), nullable=True),
            sa.Column("source_watermark", sa.String(), nullable=True),
            sa.Column("destination_watermark", sa.String(), nullable=True),
            sa.Column("last_successful_sync_at", sa.DateTime(timezone=True), nullable=True),
            sa.Column("bigquery_job_id", sa.String(), nullable=True),
            sa.Column("schema_drift", sa.JSON(), nullable=False, server_default="{}"),
            sa.Column("error_details", sa.Text(), nullable=True),
            sa.Column("created_at", sa.DateTime(timezone=True), nullable=False),
            sa.Column("updated_at", sa.DateTime(timezone=True), nullable=False),
            sa.PrimaryKeyConstraint("run_id"),
        )
        op.create_index(
            "idx_bigquery_sync_runs_started",
            "bigquery_sync_runs",
            ["started_at"],
        )

    if "bigquery_sync_partitions" not in tables:
        op.create_table(
            "bigquery_sync_partitions",
            sa.Column("partition_id", sa.String(), nullable=False),
            sa.Column("run_id", sa.String(), nullable=False),
            sa.Column("entity", sa.String(), nullable=False),
            sa.Column("exchange", sa.String(), nullable=True),
            sa.Column("partition_start", sa.Date(), nullable=True),
            sa.Column("partition_end", sa.Date(), nullable=True),
            sa.Column("status", sa.String(), nullable=False),
            sa.Column("attempt_count", sa.BigInteger(), nullable=False, server_default="0"),
            sa.Column("source_row_count", sa.BigInteger(), nullable=False, server_default="0"),
            sa.Column("destination_row_count", sa.BigInteger(), nullable=False, server_default="0"),
            sa.Column("count_difference", sa.BigInteger(), nullable=False, server_default="0"),
            sa.Column("inserted_rows", sa.BigInteger(), nullable=False, server_default="0"),
            sa.Column("updated_rows", sa.BigInteger(), nullable=False, server_default="0"),
            sa.Column("rejected_rows", sa.BigInteger(), nullable=False, server_default="0"),
            sa.Column("source_watermark", sa.String(), nullable=True),
            sa.Column("destination_watermark", sa.String(), nullable=True),
            sa.Column("bigquery_job_id", sa.String(), nullable=True),
            sa.Column("duration_seconds", sa.Float(), nullable=True),
            sa.Column("schema_drift", sa.JSON(), nullable=False, server_default="{}"),
            sa.Column("error_details", sa.Text(), nullable=True),
            sa.Column("created_at", sa.DateTime(timezone=True), nullable=False),
            sa.Column("updated_at", sa.DateTime(timezone=True), nullable=False),
            sa.Column("completed_at", sa.DateTime(timezone=True), nullable=True),
            sa.ForeignKeyConstraint(["run_id"], ["bigquery_sync_runs.run_id"]),
            sa.PrimaryKeyConstraint("partition_id"),
            sa.UniqueConstraint(
                "run_id",
                "entity",
                "exchange",
                "partition_start",
                "partition_end",
                name="uq_bigquery_sync_partition_scope",
            ),
        )
        op.create_index(
            "idx_bigquery_sync_partitions_status",
            "bigquery_sync_partitions",
            ["status", "updated_at"],
        )

    if op.get_bind().dialect.name != "postgresql":
        return
    op.execute(sa.text("CREATE SCHEMA IF NOT EXISTS analytics"))
    op.execute(sa.text("REVOKE CREATE ON SCHEMA public FROM PUBLIC"))
    for name, query in ANALYTICS_VIEWS.items():
        op.execute(
            sa.text(
                f"CREATE OR REPLACE VIEW analytics.{name} "
                "WITH (security_barrier = true) AS "
                f"{query}"
            )
        )
    op.execute(sa.text("REVOKE ALL ON SCHEMA analytics FROM PUBLIC"))
    op.execute(sa.text("REVOKE ALL ON ALL TABLES IN SCHEMA analytics FROM PUBLIC"))


def downgrade() -> None:
    if op.get_bind().dialect.name == "postgresql":
        for name in reversed(tuple(ANALYTICS_VIEWS)):
            op.execute(sa.text(f"DROP VIEW IF EXISTS analytics.{name}"))
        op.execute(sa.text("DROP SCHEMA IF EXISTS analytics"))
    tables = set(sa.inspect(op.get_bind()).get_table_names())
    if "bigquery_sync_partitions" in tables:
        op.drop_table("bigquery_sync_partitions")
    if "bigquery_sync_runs" in tables:
        op.drop_table("bigquery_sync_runs")
