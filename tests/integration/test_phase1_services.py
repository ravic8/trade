import os

import pytest
import redis
from sqlalchemy import create_engine, inspect, text

from trade_data.data.rate_limits import RateLimitWindow, RedisProviderRateLimiter

pytestmark = pytest.mark.skipif(
    os.getenv("PHASE1_SERVICE_TESTS") != "1",
    reason="requires the Phase 1 PostgreSQL/Timescale and Redis CI services",
)


def test_postgresql_timescale_schema_is_upgraded() -> None:
    engine = create_engine(os.environ["DATABASE_URL"])
    with engine.connect() as connection:
        assert connection.execute(text("SELECT 1")).scalar_one() == 1
        assert (
            connection.execute(
                text("SELECT count(*) FROM pg_extension WHERE extname = 'timescaledb'")
            ).scalar_one()
            == 1
        )
    tables = set(inspect(engine).get_table_names())
    assert {"ohlcv_daily", "pipeline_work_items", "opportunity_targets_daily"} <= tables


def test_redis_is_shared_rate_limit_backend() -> None:
    redis_url = os.environ["REDIS_URL"]
    client = redis.Redis.from_url(redis_url)
    client.flushdb()
    limiter = RedisProviderRateLimiter(
        redis_url,
        {("phase1", "smoke"): (RateLimitWindow("1m", 10, 60),)},
    )
    decision = limiter.acquire("phase1", "smoke")
    assert decision.backend == "redis"
    assert client.zcard("provider-rate-limit:phase1:smoke:1m") == 1


def test_quality_batch_upserts_overlapping_candle_explanations_once() -> None:
    from dataclasses import replace
    from datetime import UTC, date, datetime
    from uuid import uuid4

    from sqlalchemy import delete, select

    from trade_data.control_plane.tables import market_data_quality_outcomes_table as table
    from trade_data.market_data.quality import MarketDataQualityOutcome, MarketDataQualityRepository
    from trade_data.market_data.quality import MarketDataQualityStatus as Status

    engine = create_engine(os.environ["DATABASE_URL"])
    run_id = "quality-overlap-" + str(uuid4())
    first = MarketDataQualityOutcome(
        source_run_id=run_id,
        request_id=run_id,
        provider="yfinance",
        exchange="NSE",
        interval="1d",
        instrument_id="overlap-fixture",
        provider_symbol="FIXTURE.NS",
        session_date=date(2026, 9, 1),
        status=Status.MISSING,
        reason_code="candle_absent",
        severity="error",
        expected=True,
        retryable=True,
        observed_at=datetime.now(UTC),
        details={"window": "first"},
    )
    last = replace(first, details={"window": "last"})
    try:
        repository = MarketDataQualityRepository(engine)
        assert repository.record([first, last]) == 1
        assert repository.record([first, last]) == 1
        with engine.connect() as connection:
            rows = (
                connection.execute(select(table).where(table.c.source_run_id == run_id))
                .mappings()
                .all()
            )
        assert len(rows) == 1
        assert rows[0]["details"] == {"window": "last"}
    finally:
        with engine.begin() as connection:
            connection.execute(delete(table).where(table.c.source_run_id == run_id))
        engine.dispose()
