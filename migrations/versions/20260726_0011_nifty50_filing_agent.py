"""Add Nifty 50 filing investigation state and universe snapshots.

Revision ID: 20260726_0011
Revises: 20260724_0010
Create Date: 2026-07-26
"""

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op

revision: str = "20260726_0011"
down_revision: str | None = "20260724_0010"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    op.create_table(
        "filing_universe_snapshots",
        sa.Column("snapshot_id", sa.String(), primary_key=True),
        sa.Column("workspace_id", sa.String(), nullable=False),
        sa.Column("universe_id", sa.String(), nullable=False),
        sa.Column("effective_date", sa.Date(), nullable=False),
        sa.Column("source_url", sa.Text(), nullable=False),
        sa.Column("source_hash", sa.String(length=64), nullable=False),
        sa.Column("members", sa.JSON(), nullable=False),
        sa.Column("member_count", sa.BigInteger(), nullable=False),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False),
        sa.UniqueConstraint(
            "workspace_id",
            "universe_id",
            "source_hash",
            name="uq_filing_universe_snapshot_source",
        ),
    )
    op.create_index(
        "idx_filing_universe_workspace_effective",
        "filing_universe_snapshots",
        ["workspace_id", "universe_id", "effective_date"],
    )

    op.create_table(
        "filing_investigation_runs",
        sa.Column("analysis_id", sa.String(), primary_key=True),
        sa.Column("thread_id", sa.String(), nullable=False),
        sa.Column("workspace_id", sa.String(), nullable=False),
        sa.Column("universe_id", sa.String(), nullable=False),
        sa.Column("universe_snapshot_id", sa.String()),
        sa.Column("question", sa.Text(), nullable=False),
        sa.Column("status", sa.String(), nullable=False),
        sa.Column("current_node", sa.String(), nullable=False),
        sa.Column("progress", sa.Float(), nullable=False),
        sa.Column("request_payload", sa.JSON(), nullable=False),
        sa.Column("plan_payload", sa.JSON(), nullable=False),
        sa.Column("result_payload", sa.JSON(), nullable=False),
        sa.Column("error_code", sa.String()),
        sa.Column("error_message", sa.Text()),
        sa.Column("trace_id", sa.String()),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("updated_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("finished_at", sa.DateTime(timezone=True)),
    )
    op.create_index(
        "idx_filing_investigation_workspace_status",
        "filing_investigation_runs",
        ["workspace_id", "status", "updated_at"],
    )

    op.create_table(
        "filing_investigation_events",
        sa.Column("event_id", sa.String(), primary_key=True),
        sa.Column("analysis_id", sa.String(), nullable=False),
        sa.Column("workspace_id", sa.String(), nullable=False),
        sa.Column("sequence", sa.BigInteger(), nullable=False),
        sa.Column("node", sa.String(), nullable=False),
        sa.Column("status", sa.String(), nullable=False),
        sa.Column("detail", sa.JSON(), nullable=False),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False),
        sa.UniqueConstraint(
            "analysis_id",
            "sequence",
            name="uq_filing_investigation_event_sequence",
        ),
    )
    op.create_index(
        "idx_filing_investigation_events",
        "filing_investigation_events",
        ["analysis_id", "sequence"],
    )


def downgrade() -> None:
    op.drop_index(
        "idx_filing_investigation_events",
        table_name="filing_investigation_events",
    )
    op.drop_table("filing_investigation_events")
    op.drop_index(
        "idx_filing_investigation_workspace_status",
        table_name="filing_investigation_runs",
    )
    op.drop_table("filing_investigation_runs")
    op.drop_index(
        "idx_filing_universe_workspace_effective",
        table_name="filing_universe_snapshots",
    )
    op.drop_table("filing_universe_snapshots")
