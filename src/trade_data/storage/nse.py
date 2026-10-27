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


def latest_nse_session(store, *, provider: str, grace_minutes: int):
    """A dated official final file also proves a completed special session."""
    from sqlalchemy import func, select

    calendar = store.latest_provider_eligible_exchange_session(
        "NSE", provider_grace_minutes=grace_minutes
    )
    with store.engine.connect() as c:
        published = c.execute(
            select(func.max(nse_sessions.c.session_date))
            .where(nse_sessions.c.status == "complete")
            .where(nse_sessions.c.details["source_provider"].as_string() == provider)
        ).scalar()
    if published and (not calendar or published > calendar["session_date"]):
        return {"session_date": published, "basis": "official_final_bhavcopy"}
    return calendar
