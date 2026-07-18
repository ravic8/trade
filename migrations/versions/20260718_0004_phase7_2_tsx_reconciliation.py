"""Add Phase 7.2 TSX reconciliation state.

Revision ID: 20260718_0004
Revises: 20260718_0003
Create Date: 2026-07-18
"""

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op

revision: str = "20260718_0004"
down_revision: str | None = "20260718_0003"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def _column_names(table_name: str) -> set[str]:
    return {str(column["name"]) for column in sa.inspect(op.get_bind()).get_columns(table_name)}


def upgrade() -> None:
    if "symbols" not in sa.inspect(op.get_bind()).get_table_names():
        return
    columns = _column_names("symbols")
    additions = (
        sa.Column(
            "instrument_type",
            sa.String(),
            nullable=False,
            server_default="unknown",
        ),
        sa.Column(
            "reconciliation_status",
            sa.String(),
            nullable=False,
            server_default="not_required",
        ),
        sa.Column("reconciliation_reason", sa.String(), nullable=True),
        sa.Column("official_sector", sa.String(), nullable=True),
        sa.Column("official_security_type", sa.String(), nullable=True),
        sa.Column(
            "official_source_updated_at",
            sa.DateTime(timezone=True),
            nullable=True,
        ),
    )
    for column in additions:
        if column.name not in columns:
            op.add_column("symbols", column)

    op.execute(
        sa.text(
            "UPDATE symbols "
            "SET instrument_type = COALESCE(instrument_type, 'unknown'), "
            "reconciliation_status = COALESCE(reconciliation_status, 'not_required')"
        )
    )


def downgrade() -> None:
    if "symbols" not in sa.inspect(op.get_bind()).get_table_names():
        return
    columns = _column_names("symbols")
    for column_name in (
        "official_source_updated_at",
        "official_security_type",
        "official_sector",
        "reconciliation_reason",
        "reconciliation_status",
        "instrument_type",
    ):
        if column_name in columns:
            op.drop_column("symbols", column_name)
