"""Cancel TSX work that predates the active listing boundary.

Revision ID: 20260718_0005
Revises: 20260718_0004
Create Date: 2026-07-18
"""

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op

revision: str = "20260718_0005"
down_revision: str | None = "20260718_0004"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    tables = set(sa.inspect(op.get_bind()).get_table_names())
    if not {"symbols", "pipeline_work_items"}.issubset(tables):
        return

    symbols = sa.table(
        "symbols",
        sa.column("canonical_instrument_id", sa.String()),
        sa.column("exchange", sa.String()),
        sa.column("listing_status", sa.String()),
        sa.column("listing_status_effective_at", sa.DateTime(timezone=True)),
    )
    work = sa.table(
        "pipeline_work_items",
        sa.column("canonical_instrument_id", sa.String()),
        sa.column("exchange", sa.String()),
        sa.column("provider", sa.String()),
        sa.column("window_end", sa.Date()),
        sa.column("status", sa.String()),
        sa.column("last_error_code", sa.String()),
        sa.column("last_error_message", sa.Text()),
        sa.column("next_attempt_at", sa.DateTime(timezone=True)),
        sa.column("completed_at", sa.DateTime(timezone=True)),
        sa.column("updated_at", sa.DateTime(timezone=True)),
    )
    before_active_listing = (
        sa.select(1)
        .select_from(symbols)
        .where(symbols.c.canonical_instrument_id == work.c.canonical_instrument_id)
        .where(symbols.c.exchange == "TSX")
        .where(symbols.c.listing_status == "active")
        .where(symbols.c.listing_status_effective_at.is_not(None))
        .where(work.c.window_end < sa.func.date(symbols.c.listing_status_effective_at))
        .exists()
    )
    op.execute(
        work.update()
        .where(work.c.exchange == "TSX")
        .where(work.c.provider == "yfinance")
        .where(work.c.status.in_(("queued", "retry_wait")))
        .where(before_active_listing)
        .values(
            status="cancelled",
            last_error_code="outside_listing_window",
            last_error_message=(
                "Work window ends before the active instrument listing boundary."
            ),
            next_attempt_at=None,
            completed_at=sa.func.now(),
            updated_at=sa.func.now(),
        )
    )


def downgrade() -> None:
    # The previous queue states and retry timestamps cannot be reconstructed safely.
    pass
