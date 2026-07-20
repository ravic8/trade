"""Add Phase 9.2B BigQuery canary reconciliation evidence.

Revision ID: 20260720_0008
Revises: 20260719_0007
Create Date: 2026-07-20
"""

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op

revision: str = "20260720_0008"
down_revision: str | None = "20260719_0007"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


RUN_COLUMNS: tuple[sa.Column, ...] = (
    sa.Column("reporting_dataset", sa.String(), nullable=True),
    sa.Column("authenticated_principal", sa.String(), nullable=True),
    sa.Column("staging_row_count", sa.BigInteger(), nullable=False, server_default="0"),
    sa.Column("merged_row_count", sa.BigInteger(), nullable=False, server_default="0"),
    sa.Column(
        "duplicate_business_key_count",
        sa.BigInteger(),
        nullable=False,
        server_default="0",
    ),
)

PARTITION_COLUMNS: tuple[sa.Column, ...] = (
    sa.Column("staging_row_count", sa.BigInteger(), nullable=False, server_default="0"),
    sa.Column("merged_row_count", sa.BigInteger(), nullable=False, server_default="0"),
    sa.Column(
        "duplicate_business_key_count",
        sa.BigInteger(),
        nullable=False,
        server_default="0",
    ),
    sa.Column("source_min_date", sa.Date(), nullable=True),
    sa.Column("source_max_date", sa.Date(), nullable=True),
    sa.Column("destination_min_date", sa.Date(), nullable=True),
    sa.Column("destination_max_date", sa.Date(), nullable=True),
)


def upgrade() -> None:
    inspector = sa.inspect(op.get_bind())
    tables = set(inspector.get_table_names())
    for table_name, columns in (
        ("bigquery_sync_runs", RUN_COLUMNS),
        ("bigquery_sync_partitions", PARTITION_COLUMNS),
    ):
        if table_name not in tables:
            continue
        existing = {column["name"] for column in inspector.get_columns(table_name)}
        for column in columns:
            if column.name not in existing:
                op.add_column(table_name, column)


def downgrade() -> None:
    inspector = sa.inspect(op.get_bind())
    tables = set(inspector.get_table_names())
    for table_name, columns in (
        ("bigquery_sync_partitions", reversed(PARTITION_COLUMNS)),
        ("bigquery_sync_runs", reversed(RUN_COLUMNS)),
    ):
        if table_name not in tables:
            continue
        existing = {column["name"] for column in inspector.get_columns(table_name)}
        for column in columns:
            if column.name in existing:
                op.drop_column(table_name, column.name)
