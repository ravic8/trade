import os

import pytest
import redis
from sqlalchemy import create_engine, inspect, text

from trade_research.data.rate_limits import RateLimitWindow, RedisProviderRateLimiter

pytestmark = pytest.mark.skipif(
    os.getenv("PHASE1_SERVICE_TESTS") != "1",
    reason="requires the Phase 1 PostgreSQL/Timescale and Redis CI services",
)


def test_postgresql_timescale_schema_is_upgraded() -> None:
    engine = create_engine(os.environ["DATABASE_URL"])
    with engine.connect() as connection:
        assert connection.execute(text("SELECT 1")).scalar_one() == 1
        assert connection.execute(
            text("SELECT count(*) FROM pg_extension WHERE extname = 'timescaledb'")
        ).scalar_one() == 1
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
