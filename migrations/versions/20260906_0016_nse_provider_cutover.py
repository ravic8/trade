"""Add NSE provider cutover evidence and decision ledgers.

Revision ID: 20260906_0016
Revises: 20260905_0015
Create Date: 2026-09-06
"""

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op

revision: str = "20260906_0016"
down_revision: str | None = "20260905_0015"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    op.create_table(
        "nse_provider_comparison_evidence",
        sa.Column("evidence_id", sa.String(length=64), primary_key=True),
        sa.Column("workspace_id", sa.String(length=64), nullable=False),
        sa.Column("window_start", sa.Date(), nullable=False),
        sa.Column("window_end", sa.Date(), nullable=False),
        sa.Column("status", sa.String(length=16), nullable=False),
        sa.Column("comparison_state", sa.String(length=64), nullable=False),
        sa.Column("ready", sa.Boolean(), nullable=False),
        sa.Column("metrics", sa.JSON(), nullable=False),
        sa.Column("blocking_issues", sa.JSON(), nullable=False),
        sa.Column("evidence_sha256", sa.String(length=64), nullable=False, unique=True),
        sa.Column("observed_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False),
    )
    op.create_index(
        "idx_nse_provider_evidence_scope_window",
        "nse_provider_comparison_evidence",
        ["workspace_id", "window_end", "observed_at"],
    )
    op.create_table(
        "nse_provider_cutover_decisions",
        sa.Column("decision_id", sa.String(length=64), primary_key=True),
        sa.Column("workspace_id", sa.String(length=64), nullable=False),
        sa.Column("idempotency_key", sa.String(length=200), nullable=False),
        sa.Column("action", sa.String(length=32), nullable=False),
        sa.Column("from_provider", sa.String(length=64), nullable=False),
        sa.Column("to_provider", sa.String(length=64), nullable=False),
        sa.Column("evidence_ids", sa.JSON(), nullable=False),
        sa.Column("evidence_bundle_sha256", sa.String(length=64), nullable=False),
        sa.Column("actor_email", sa.String(length=255), nullable=False),
        sa.Column("reason", sa.Text(), nullable=False),
        sa.Column("decision_sha256", sa.String(length=64), nullable=False, unique=True),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False),
        sa.UniqueConstraint(
            "workspace_id",
            "idempotency_key",
            name="uq_nse_cutover_decisions_workspace_idempotency",
        ),
    )
    op.create_index(
        "idx_nse_provider_decisions_scope_created",
        "nse_provider_cutover_decisions",
        ["workspace_id", "created_at"],
    )


def downgrade() -> None:
    op.drop_index(
        "idx_nse_provider_decisions_scope_created",
        table_name="nse_provider_cutover_decisions",
    )
    op.drop_table("nse_provider_cutover_decisions")
    op.drop_index(
        "idx_nse_provider_evidence_scope_window",
        table_name="nse_provider_comparison_evidence",
    )
    op.drop_table("nse_provider_comparison_evidence")
