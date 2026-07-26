"""Persist Lens investigation quality evaluations.

Revision ID: 20260726_0012
Revises: 20260726_0011
Create Date: 2026-07-26
"""

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op

revision: str = "20260726_0012"
down_revision: str | None = "20260726_0011"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    op.create_table(
        "filing_investigation_evaluations",
        sa.Column("evaluation_id", sa.String(), primary_key=True),
        sa.Column("analysis_id", sa.String(), nullable=False),
        sa.Column("workspace_id", sa.String(), nullable=False),
        sa.Column("dataset_id", sa.String(), nullable=False),
        sa.Column("evaluator_version", sa.String(), nullable=False),
        sa.Column("status", sa.String(), nullable=False),
        sa.Column("score", sa.Float(), nullable=False),
        sa.Column("report_payload", sa.JSON(), nullable=False),
        sa.Column("trace_id", sa.String()),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False),
    )
    op.create_index(
        "idx_filing_investigation_evaluation_latest",
        "filing_investigation_evaluations",
        ["workspace_id", "analysis_id", "created_at"],
    )


def downgrade() -> None:
    op.drop_index(
        "idx_filing_investigation_evaluation_latest",
        table_name="filing_investigation_evaluations",
    )
    op.drop_table("filing_investigation_evaluations")
