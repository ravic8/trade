"""Add durable API-to-Dagster workflow requests.

Revision ID: 20260904_0013
Revises: 20260726_0012
Create Date: 2026-09-04
"""

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op

revision: str = "20260904_0013"
down_revision: str | None = "20260726_0012"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    op.create_table(
        "workflow_requests",
        sa.Column("workflow_id", sa.String(), primary_key=True),
        sa.Column("workflow_type", sa.String(), nullable=False),
        sa.Column("idempotency_key", sa.String(), nullable=False),
        sa.Column("request_hash", sa.String(), nullable=False),
        sa.Column("request_payload", sa.JSON(), nullable=False),
        sa.Column("status", sa.String(), nullable=False),
        sa.Column("requested_by", sa.String(), nullable=False),
        sa.Column("dagster_run_id", sa.String()),
        sa.Column("result_run_id", sa.String()),
        sa.Column("error_message", sa.Text()),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("started_at", sa.DateTime(timezone=True)),
        sa.Column("completed_at", sa.DateTime(timezone=True)),
        sa.Column("updated_at", sa.DateTime(timezone=True), nullable=False),
        sa.UniqueConstraint("idempotency_key", name="uq_workflow_requests_idempotency_key"),
    )
    op.create_index(
        "idx_workflow_requests_dispatch",
        "workflow_requests",
        ["workflow_type", "status", "created_at"],
    )


def downgrade() -> None:
    op.drop_index("idx_workflow_requests_dispatch", table_name="workflow_requests")
    op.drop_table("workflow_requests")
