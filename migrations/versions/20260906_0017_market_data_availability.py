"""Add observed market-data availability evidence.

Revision ID: 20260906_0017
Revises: 20260906_0016
Create Date: 2026-09-06
"""

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op

revision: str = "20260906_0017"
down_revision: str | None = "20260906_0016"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    op.create_table(
        "market_data_availability_observations",
        sa.Column("availability_observation_id", sa.String(length=64), primary_key=True),
        sa.Column("workspace_id", sa.String(length=64), nullable=False),
        sa.Column("source_run_id", sa.String(length=255), nullable=False),
        sa.Column("request_id", sa.String(length=255), nullable=False),
        sa.Column("provider", sa.String(length=64), nullable=False),
        sa.Column("exchange", sa.String(length=32), nullable=False),
        sa.Column("interval", sa.String(length=16), nullable=False),
        sa.Column("instrument_id", sa.String(length=255), nullable=False),
        sa.Column("provider_symbol", sa.String(length=255), nullable=False),
        sa.Column("requested_start", sa.DateTime(timezone=True), nullable=False),
        sa.Column("requested_end", sa.DateTime(timezone=True), nullable=False),
        sa.Column("eligible_sessions", sa.JSON(), nullable=False),
        sa.Column("observed_sessions", sa.JSON(), nullable=False),
        sa.Column("observed_first_timestamp", sa.DateTime(timezone=True)),
        sa.Column("observed_last_timestamp", sa.DateTime(timezone=True)),
        sa.Column("observed_row_count", sa.BigInteger(), nullable=False),
        sa.Column("status", sa.String(length=32), nullable=False),
        sa.Column("reason_code", sa.String(length=100), nullable=False),
        sa.Column("retryable", sa.Boolean(), nullable=False),
        sa.Column("raw_artifact_id", sa.String(length=36)),
        sa.Column("observed_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False),
    )
    op.create_index(
        "idx_market_data_availability_scope_observed",
        "market_data_availability_observations",
        ["workspace_id", "provider", "exchange", "interval", "observed_at"],
    )
    op.create_index(
        "idx_market_data_availability_run_instrument",
        "market_data_availability_observations",
        ["source_run_id", "instrument_id"],
    )


def downgrade() -> None:
    op.drop_index(
        "idx_market_data_availability_run_instrument",
        table_name="market_data_availability_observations",
    )
    op.drop_index(
        "idx_market_data_availability_scope_observed",
        table_name="market_data_availability_observations",
    )
    op.drop_table("market_data_availability_observations")
