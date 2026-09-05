"""Add Phase 3 market-data quality and replication ledgers.

Revision ID: 20260905_0015
Revises: 20260905_0014
Create Date: 2026-09-05
"""

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op

revision: str = "20260905_0015"
down_revision: str | None = "20260905_0014"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    op.create_table(
        "market_data_quality_outcomes",
        sa.Column("quality_outcome_id", sa.String(length=64), primary_key=True),
        sa.Column("workspace_id", sa.String(length=64), nullable=False),
        sa.Column("source_run_id", sa.String(length=255), nullable=False),
        sa.Column("request_id", sa.String(length=255), nullable=False),
        sa.Column("provider", sa.String(length=64), nullable=False),
        sa.Column("exchange", sa.String(length=32), nullable=False),
        sa.Column("interval", sa.String(length=16), nullable=False),
        sa.Column("instrument_id", sa.String(length=255), nullable=False),
        sa.Column("provider_symbol", sa.String(length=255), nullable=False),
        sa.Column("session_date", sa.Date(), nullable=False),
        sa.Column("candle_timestamp", sa.DateTime(timezone=True)),
        sa.Column("status", sa.String(length=32), nullable=False),
        sa.Column("reason_code", sa.String(length=100), nullable=False),
        sa.Column("severity", sa.String(length=16), nullable=False),
        sa.Column("expected", sa.Boolean(), nullable=False),
        sa.Column("retryable", sa.Boolean(), nullable=False),
        sa.Column("raw_artifact_id", sa.String(length=36)),
        sa.Column("details", sa.JSON(), nullable=False),
        sa.Column("observed_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("updated_at", sa.DateTime(timezone=True), nullable=False),
    )
    op.create_index(
        "idx_market_data_quality_run_status",
        "market_data_quality_outcomes",
        ["source_run_id", "status"],
    )
    op.create_index(
        "idx_market_data_quality_instrument_session",
        "market_data_quality_outcomes",
        ["exchange", "interval", "instrument_id", "session_date"],
    )
    op.create_table(
        "market_data_replication_checkpoints",
        sa.Column("replication_checkpoint_id", sa.String(length=64), primary_key=True),
        sa.Column("workspace_id", sa.String(length=64), nullable=False),
        sa.Column("source_run_id", sa.String(length=255), nullable=False),
        sa.Column("source_store", sa.String(length=64), nullable=False),
        sa.Column("destination_store", sa.String(length=64), nullable=False),
        sa.Column("dataset_key", sa.String(length=100), nullable=False),
        sa.Column("exchange", sa.String(length=32), nullable=False),
        sa.Column("interval", sa.String(length=16), nullable=False),
        sa.Column("status", sa.String(length=32), nullable=False),
        sa.Column("source_row_count", sa.BigInteger(), nullable=False),
        sa.Column("destination_row_count", sa.BigInteger()),
        sa.Column("source_digest", sa.String(length=64), nullable=False),
        sa.Column("destination_digest", sa.String(length=64)),
        sa.Column("source_watermark", sa.DateTime(timezone=True)),
        sa.Column("destination_watermark", sa.DateTime(timezone=True)),
        sa.Column("watermark_lag_seconds", sa.Float()),
        sa.Column("replication_latency_ms", sa.Float()),
        sa.Column("error_message", sa.Text()),
        sa.Column("details", sa.JSON(), nullable=False),
        sa.Column("started_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("completed_at", sa.DateTime(timezone=True)),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("updated_at", sa.DateTime(timezone=True), nullable=False),
    )
    op.create_index(
        "idx_market_data_replication_run",
        "market_data_replication_checkpoints",
        ["source_run_id", "dataset_key"],
    )
    op.create_index(
        "idx_market_data_replication_status_updated",
        "market_data_replication_checkpoints",
        ["status", "updated_at"],
    )


def downgrade() -> None:
    op.drop_index(
        "idx_market_data_replication_status_updated",
        table_name="market_data_replication_checkpoints",
    )
    op.drop_index(
        "idx_market_data_replication_run",
        table_name="market_data_replication_checkpoints",
    )
    op.drop_table("market_data_replication_checkpoints")
    op.drop_index(
        "idx_market_data_quality_instrument_session",
        table_name="market_data_quality_outcomes",
    )
    op.drop_index(
        "idx_market_data_quality_run_status",
        table_name="market_data_quality_outcomes",
    )
    op.drop_table("market_data_quality_outcomes")
