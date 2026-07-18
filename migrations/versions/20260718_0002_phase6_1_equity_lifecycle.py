"""Add Phase 6.1 equity identity and lifecycle state.

Revision ID: 20260718_0002
Revises: 20260717_0001
Create Date: 2026-07-18
"""

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op

revision: str = "20260718_0002"
down_revision: str | None = "20260717_0001"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def _column_names(table_name: str) -> set[str]:
    return {str(column["name"]) for column in sa.inspect(op.get_bind()).get_columns(table_name)}


def upgrade() -> None:
    if "symbols" not in sa.inspect(op.get_bind()).get_table_names():
        return
    columns = _column_names("symbols")
    additions = (
        sa.Column("source_identity", sa.String(), nullable=True),
        sa.Column("provider_instrument_key", sa.String(), nullable=True),
        sa.Column(
            "listing_status",
            sa.String(),
            nullable=False,
            server_default="active",
        ),
        sa.Column("listing_status_reason", sa.String(), nullable=True),
        sa.Column(
            "listing_status_effective_at",
            sa.DateTime(timezone=True),
            nullable=True,
        ),
        sa.Column(
            "pipeline_eligibility",
            sa.String(),
            nullable=False,
            server_default="incremental",
        ),
        sa.Column(
            "provider_status",
            sa.String(),
            nullable=False,
            server_default="unknown",
        ),
        sa.Column("provider_status_reason", sa.String(), nullable=True),
        sa.Column(
            "provider_status_updated_at",
            sa.DateTime(timezone=True),
            nullable=True,
        ),
    )
    for column in additions:
        if column.name not in columns:
            op.add_column("symbols", column)

    eligibility = (
        "CASE WHEN is_active THEN COALESCE(pipeline_eligibility, 'incremental') ELSE 'none' END"
        if "is_active" in columns
        else "COALESCE(pipeline_eligibility, 'incremental')"
    )
    op.execute(
        sa.text(
            "UPDATE symbols "
            "SET listing_status = COALESCE(listing_status, 'active'), "
            f"pipeline_eligibility = {eligibility}, "
            "provider_status = COALESCE(provider_status, 'unknown')"
        )
    )
    if "yahoo_symbol" in columns:
        op.execute(
            sa.text(
                "UPDATE symbols "
                "SET provider_instrument_key = 'YF|' || yahoo_symbol "
                "WHERE provider_instrument_key IS NULL "
                "AND yahoo_symbol IS NOT NULL"
            )
        )


def downgrade() -> None:
    if "symbols" not in sa.inspect(op.get_bind()).get_table_names():
        return
    columns = _column_names("symbols")
    for column_name in (
        "provider_status_updated_at",
        "provider_status_reason",
        "provider_status",
        "pipeline_eligibility",
        "listing_status_effective_at",
        "listing_status_reason",
        "listing_status",
        "source_identity",
        "provider_instrument_key",
    ):
        if column_name in columns:
            op.drop_column("symbols", column_name)
