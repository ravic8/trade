"""Canonicalize legacy Canadian equity exchange values to TSX.

Revision ID: 20260718_0003
Revises: 20260718_0002
Create Date: 2026-07-18
"""

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op

revision: str = "20260718_0003"
down_revision: str | None = "20260718_0002"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None

_SIMPLE_EXCHANGE_TABLES = (
    "corporate_actions",
    "daily_ohlcv_fetch_coverage",
    "features_daily",
    "ingestion_runs",
    "ohlcv_daily",
    "ohlcv_hourly",
    "ohlcv_intraday",
    "pipeline_work_items",
    "price_adjustments_daily",
    "provider_instruments",
    "stock_coverage_by_window",
    "stock_coverage_runs",
    "symbol_lifecycle_events",
    "targets_daily",
    "tradable_universes",
    "universe_snapshots",
)

_CONFLICT_KEYS = {
    "daily_coverage_summary": (
        "source",
        "instrument_key",
        "interval",
        "as_of_date",
    ),
    "exchange_holidays": ("year",),
    "exchange_sessions": ("session_date",),
    "feed_health": ("symbol", "source"),
    "hourly_backlog_windows": ("window_start", "source"),
    "symbols": ("symbol",),
}


def _exchange_tables() -> set[str]:
    inspector = sa.inspect(op.get_bind())
    return {
        table_name
        for table_name in inspector.get_table_names()
        if "exchange" in {str(column["name"]) for column in inspector.get_columns(table_name)}
    }


def _delete_legacy_conflicts(table_name: str, keys: tuple[str, ...]) -> None:
    equality = " AND ".join(
        f'canonical."{key}" = "{table_name}"."{key}"' for key in keys
    )
    op.execute(
        sa.text(
            f'DELETE FROM "{table_name}" '
            "WHERE exchange = 'CA' "
            "AND EXISTS ("
            f'SELECT 1 FROM "{table_name}" AS canonical '
            "WHERE canonical.exchange = 'TSX' "
            f"AND {equality}"
            ")"
        )
    )


def _canonicalize_table(table_name: str) -> None:
    op.execute(
        sa.text(
            f'UPDATE "{table_name}" '
            "SET exchange = 'TSX' "
            "WHERE exchange = 'CA'"
        )
    )


def upgrade() -> None:
    tables = _exchange_tables()
    for table_name, keys in _CONFLICT_KEYS.items():
        if table_name not in tables:
            continue
        _delete_legacy_conflicts(table_name, keys)
        _canonicalize_table(table_name)
    for table_name in _SIMPLE_EXCHANGE_TABLES:
        if table_name in tables:
            _canonicalize_table(table_name)


def downgrade() -> None:
    # Canonical TSX records cannot be distinguished safely from rows that used
    # the legacy CA alias. Reintroducing CA would corrupt canonical data, so this
    # data-only migration is intentionally irreversible.
    pass
