import time

import pytest

from trade_research.config import Settings
from trade_research.data.rate_limits import (
    InMemoryProviderRateLimiter,
    RateLimitWindow,
    _limit_key,
    provider_rate_limit_windows,
)


def test_in_memory_provider_rate_limiter_waits_after_window_is_full() -> None:
    limiter = InMemoryProviderRateLimiter(
        {("upstox", "historical"): (RateLimitWindow("tiny", 1, 1),)}
    )

    first = limiter.acquire("upstox", "historical")
    started = time.monotonic()
    second = limiter.acquire("upstox", "historical")
    elapsed = time.monotonic() - started

    assert first.rate_limited is False
    assert second.rate_limited is True
    assert second.wait_seconds > 0
    assert elapsed >= 0.9


def test_in_memory_provider_rate_limiter_ignores_unknown_endpoint() -> None:
    limiter = InMemoryProviderRateLimiter({})

    decision = limiter.acquire("unknown", "endpoint")

    assert decision.backend == "memory"
    assert decision.wait_seconds == 0
    assert decision.rate_limited is False


def test_provider_rate_windows_include_yfinance_intraday_download() -> None:
    windows = provider_rate_limit_windows(
        Settings(
            provider_rate_limit_backend="none",
            yfinance_initial_rpm=321,
        )
    )

    assert ("yfinance", "intraday_download") in windows
    assert windows[("yfinance", "download")][0].limit == 321
    assert windows[("yfinance", "intraday_download")][0].limit == 321


def test_yfinance_daily_and_intraday_share_one_global_budget_key() -> None:
    assert _limit_key("yfinance", "download", "1m") == (
        "provider-rate-limit:yfinance:all"
    )
    assert _limit_key("yfinance", "intraday_download", "1m") == (
        "provider-rate-limit:yfinance:all"
    )


def test_in_memory_limiter_reserves_weighted_ticker_tokens() -> None:
    limiter = InMemoryProviderRateLimiter(
        {("yfinance", "download"): (RateLimitWindow("1m", 3, 60),)}
    )

    limiter.acquire("yfinance", "download", weight=2)
    limiter.acquire("yfinance", "download", weight=1)

    assert len(limiter._hits["provider-rate-limit:yfinance:all"]) == 3


def test_weight_cannot_exceed_a_rate_window() -> None:
    limiter = InMemoryProviderRateLimiter(
        {("yfinance", "download"): (RateLimitWindow("1m", 2, 60),)}
    )

    with pytest.raises(ValueError, match="weight 3 exceeds"):
        limiter.acquire("yfinance", "download", weight=3)


def test_runtime_rate_update_applies_to_all_yfinance_endpoint_groups() -> None:
    limiter = InMemoryProviderRateLimiter(
        {
            ("yfinance", "download"): (RateLimitWindow("1m", 300, 60),),
            ("yfinance", "intraday_download"): (RateLimitWindow("1m", 300, 60),),
        }
    )

    limiter.update_rate_per_minute("yfinance", 75)

    assert limiter._limits[("yfinance", "download")][0].limit == 75
    assert limiter._limits[("yfinance", "intraday_download")][0].limit == 75
