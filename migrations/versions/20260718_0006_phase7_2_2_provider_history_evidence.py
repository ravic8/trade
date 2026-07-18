"""Add provider-specific daily history evidence.

Revision ID: 20260718_0006
Revises: 20260718_0005
Create Date: 2026-07-18
"""

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op

revision: str = "20260718_0006"
down_revision: str | None = "20260718_0005"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    tables = set(sa.inspect(op.get_bind()).get_table_names())
    if "provider_daily_history_evidence" in tables:
        return
    op.create_table(
        "provider_daily_history_evidence",
        sa.Column("evidence_id", sa.String(), nullable=False),
        sa.Column("provider", sa.String(), nullable=False),
        sa.Column("instrument_key", sa.String(), nullable=False),
        sa.Column("exchange", sa.String(), nullable=False),
        sa.Column("canonical_instrument_id", sa.String(), nullable=False),
        sa.Column("provider_symbol", sa.String(), nullable=False),
        sa.Column("interval", sa.String(), nullable=False),
        sa.Column("work_type", sa.String(), nullable=False),
        sa.Column("requested_start", sa.Date(), nullable=False),
        sa.Column("requested_end", sa.Date(), nullable=False),
        sa.Column("coverage_start", sa.Date(), nullable=False),
        sa.Column("coverage_end", sa.Date(), nullable=False),
        sa.Column("first_available_date", sa.Date(), nullable=False),
        sa.Column("last_available_date", sa.Date(), nullable=False),
        sa.Column("expected_rows", sa.BigInteger(), nullable=False),
        sa.Column("observed_rows", sa.BigInteger(), nullable=False),
        sa.Column("missing_rows", sa.BigInteger(), nullable=False),
        sa.Column("coverage_ratio", sa.Float(), nullable=False),
        sa.Column("classification", sa.String(), nullable=False),
        sa.Column("quarantine_reason", sa.String(), nullable=True),
        sa.Column("status", sa.String(), nullable=False, server_default="active"),
        sa.Column("evidence_run_id", sa.String(), nullable=False),
        sa.Column("verified_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("updated_at", sa.DateTime(timezone=True), nullable=False),
        sa.PrimaryKeyConstraint("evidence_id"),
    )
    op.create_index(
        "idx_provider_daily_history_exchange_classification",
        "provider_daily_history_evidence",
        ["exchange", "classification", "status"],
    )
    op.create_index(
        "idx_provider_daily_history_instrument",
        "provider_daily_history_evidence",
        ["provider", "instrument_key", "interval"],
    )


def downgrade() -> None:
    tables = set(sa.inspect(op.get_bind()).get_table_names())
    if "provider_daily_history_evidence" not in tables:
        return
    op.drop_index(
        "idx_provider_daily_history_instrument",
        table_name="provider_daily_history_evidence",
    )
    op.drop_index(
        "idx_provider_daily_history_exchange_classification",
        table_name="provider_daily_history_evidence",
    )
    op.drop_table("provider_daily_history_evidence")
