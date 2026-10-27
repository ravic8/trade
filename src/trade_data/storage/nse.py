"""Durable session coverage and source proof; existing market data is never deleted."""

from sqlalchemy import JSON, Column, Date, DateTime, Integer, String, Table

from trade_data.storage.timescale import metadata

nse_sessions = Table(
    "nse_bhavcopy_sessions",
    metadata,
    Column("session_date", Date, primary_key=True),
    Column("status", String, nullable=False),
    Column("source_url", String),
    Column("source_sha256", String),
    Column("expected_rows", Integer),
    Column("postgres_rows", Integer),
    Column("clickhouse_rows", Integer),
    Column("source_digest", String),
    Column("details", JSON, nullable=False),
    Column("updated_at", DateTime(timezone=True), nullable=False),
)
