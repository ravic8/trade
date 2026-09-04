"""Bootstrap the schema that predates Alembic ownership.

Revision ID: 20260716_0000
Revises: None
Create Date: 2026-09-04

The original service created these tables through ``TimescaleStore.initialize``
before the first Alembic revision was introduced.  Keeping the table-name set
here fixed lets a fresh database enter the historical migration chain without
also creating tables owned by later revisions.
"""

from collections.abc import Sequence

from alembic import op

revision: str = "20260716_0000"
down_revision: str | None = None
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


_PRE_ALEMBIC_TABLES = frozenset(
    {
        "corporate_actions",
        "daily_ohlcv_fetch_coverage",
        "data_quality_audits",
        "exchange_holidays",
        "feature_audits",
        "feature_runs",
        "features_daily",
        "feed_health",
        "hourly_backlog_windows",
        "ingestion_runs",
        "ohlcv_daily",
        "ohlcv_hourly",
        "ohlcv_intraday",
        "price_adjustments_daily",
        "provider_credentials",
        "provider_instruments",
        "provider_request_log",
        "stock_coverage_by_window",
        "stock_coverage_runs",
        "symbols",
        "target_audits",
        "target_runs",
        "targets_daily",
        "tradable_universe_members",
        "tradable_universes",
    }
)


def upgrade() -> None:
    # Imported lazily so Alembic can load the revision graph without importing
    # application storage code. The names above, rather than all metadata
    # tables, define the immutable pre-Alembic ownership boundary.
    from trade_research.storage.timescale import metadata

    bind = op.get_bind()
    if bind.dialect.name == "postgresql":
        op.execute("CREATE EXTENSION IF NOT EXISTS timescaledb")

    tables_by_name = {table.name: table for table in metadata.sorted_tables}
    missing_definitions = _PRE_ALEMBIC_TABLES - tables_by_name.keys()
    if missing_definitions:
        missing = ", ".join(sorted(missing_definitions))
        raise RuntimeError(f"Missing pre-Alembic table definitions: {missing}")

    for table in metadata.sorted_tables:
        if table.name in _PRE_ALEMBIC_TABLES:
            table.create(bind, checkfirst=True)


def downgrade() -> None:
    # These tables predate Alembic and may contain legacy production data.
    # Downgrading to the pre-Alembic state must preserve them, regardless of
    # whether this reconciliation revision created them on a fresh database.
    pass
