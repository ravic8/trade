"""Add completed-session Opportunity target variables.

Revision ID: 20260720_0009
Revises: 20260720_0008
Create Date: 2026-07-20
"""

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op

revision: str = "20260720_0009"
down_revision: str | None = "20260720_0008"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    tables = set(sa.inspect(op.get_bind()).get_table_names())
    if "opportunity_targets_daily" not in tables:
        op.create_table(
            "opportunity_targets_daily",
            sa.Column("instrument_key", sa.String(), nullable=False),
            sa.Column("source", sa.String(), nullable=False),
            sa.Column("date", sa.Date(), nullable=False),
            sa.Column("target_version", sa.String(), nullable=False),
            sa.Column("symbol", sa.String(), nullable=False),
            sa.Column("exchange", sa.String(), nullable=False),
            sa.Column("computed_at", sa.DateTime(timezone=True), nullable=False),
            sa.Column("quality_status", sa.String(), nullable=False),
            sa.Column("open", sa.Float(), nullable=False),
            sa.Column("high", sa.Float(), nullable=False),
            sa.Column("low", sa.Float(), nullable=False),
            sa.Column("close", sa.Float(), nullable=False),
            sa.Column("previous_close", sa.Float(), nullable=True),
            sa.Column("volume", sa.BigInteger(), nullable=False),
            sa.Column("open_interest", sa.BigInteger(), nullable=True),
            sa.Column("session_return", sa.Float(), nullable=True),
            sa.Column("gap", sa.Float(), nullable=True),
            sa.Column("true_return", sa.Float(), nullable=True),
            sa.Column("upside", sa.Float(), nullable=True),
            sa.Column("downside", sa.Float(), nullable=True),
            sa.Column("giveback", sa.Float(), nullable=True),
            sa.Column("recovery", sa.Float(), nullable=True),
            sa.Column("session_range", sa.Float(), nullable=True),
            sa.Column("true_upside", sa.Float(), nullable=True),
            sa.Column("true_downside", sa.Float(), nullable=True),
            sa.Column("true_range", sa.Float(), nullable=True),
            sa.PrimaryKeyConstraint(
                "instrument_key",
                "source",
                "date",
                "target_version",
            ),
        )
        op.create_index(
            "idx_opportunity_targets_exchange_date",
            "opportunity_targets_daily",
            ["exchange", "date"],
        )
        op.create_index(
            "idx_opportunity_targets_symbol_date",
            "opportunity_targets_daily",
            ["symbol", "date"],
        )

    if op.get_bind().dialect.name != "postgresql":
        return
    op.execute(
        sa.text(
            "SELECT create_hypertable('opportunity_targets_daily', 'date', "
            "if_not_exists => TRUE, migrate_data => TRUE)"
        )
    )
    op.execute(sa.text("CREATE SCHEMA IF NOT EXISTS analytics"))
    op.execute(
        sa.text(
            "CREATE OR REPLACE VIEW analytics.opportunity_targets_daily "
            "WITH (security_barrier = true) AS "
            "SELECT instrument_key, source, date, target_version, symbol, exchange, "
            "computed_at, quality_status, open, high, low, close, previous_close, "
            "volume, open_interest, session_return, gap, true_return, upside, "
            "downside, giveback, recovery, session_range, true_upside, "
            "true_downside, true_range FROM public.opportunity_targets_daily"
        )
    )
    op.execute(sa.text("REVOKE ALL ON analytics.opportunity_targets_daily FROM PUBLIC"))


def downgrade() -> None:
    if op.get_bind().dialect.name == "postgresql":
        op.execute(sa.text("DROP VIEW IF EXISTS analytics.opportunity_targets_daily"))
    tables = set(sa.inspect(op.get_bind()).get_table_names())
    if "opportunity_targets_daily" in tables:
        op.drop_index(
            "idx_opportunity_targets_symbol_date",
            table_name="opportunity_targets_daily",
        )
        op.drop_index(
            "idx_opportunity_targets_exchange_date",
            table_name="opportunity_targets_daily",
        )
        op.drop_table("opportunity_targets_daily")
