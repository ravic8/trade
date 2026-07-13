import time

from trade_research.data.rate_limits import InMemoryProviderRateLimiter, RateLimitWindow


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
