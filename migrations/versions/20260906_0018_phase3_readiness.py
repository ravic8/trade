"""Add durable Phase 3 readiness evidence.

Revision ID: 20260906_0018
Revises: 20260906_0017
Create Date: 2026-09-06
"""

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op

revision: str = "20260906_0018"
down_revision: str | None = "20260906_0017"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    op.create_table(
        "phase3_readiness_evidence",
        sa.Column("evidence_id", sa.String(length=64), primary_key=True),
        sa.Column("workspace_id", sa.String(length=64), nullable=False),
        sa.Column("evidence_type", sa.String(length=32), nullable=False),
        sa.Column("status", sa.String(length=16), nullable=False),
        sa.Column("source_run_ids", sa.JSON(), nullable=False),
        sa.Column("session_dates", sa.JSON(), nullable=False),
        sa.Column("metrics", sa.JSON(), nullable=False),
        sa.Column("blocking_issues", sa.JSON(), nullable=False),
        sa.Column("evidence_refs", sa.JSON(), nullable=False),
        sa.Column("actor_email", sa.String(length=255)),
        sa.Column("reason", sa.Text()),
        sa.Column("evidence_sha256", sa.String(length=64), nullable=False, unique=True),
        sa.Column("observed_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False),
    )
    op.create_index(
        "idx_phase3_readiness_scope_observed",
        "phase3_readiness_evidence",
        ["workspace_id", "evidence_type", "observed_at"],
    )


def downgrade() -> None:
    op.drop_index(
        "idx_phase3_readiness_scope_observed",
        table_name="phase3_readiness_evidence",
    )
    op.drop_table("phase3_readiness_evidence")
