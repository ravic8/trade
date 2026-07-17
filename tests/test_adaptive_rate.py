from __future__ import annotations

from datetime import UTC, datetime, timedelta

from trade_research.config import Settings
from trade_research.data.adaptive_rate import YahooAdaptiveRateGovernor
from trade_research.data.provider_retry import ProviderFailureClassification


class _Limiter:
    def __init__(self) -> None:
        self.updates: list[tuple[str, int]] = []

    def update_rate_per_minute(self, provider: str, limit: int) -> None:
        self.updates.append((provider, limit))


class _Store:
    def __init__(self, persisted: dict | None = None) -> None:
        self.persisted = persisted
        self.writes: list[dict] = []

    def adaptive_rate_state(self, provider: str) -> dict | None:
        assert provider == "yfinance"
        return self.persisted

    def upsert_adaptive_rate_state(self, state: dict) -> int:
        self.writes.append(state)
        self.persisted = state
        return 1


def test_observe_mode_recommends_increase_without_changing_enforced_rate() -> None:
    monotonic_now = [0.0]
    utc_now = [datetime(2026, 7, 17, tzinfo=UTC)]
    limiter = _Limiter()
    store = _Store()
    settings = Settings(
        _env_file=None,
        yfinance_adaptive_rate_mode="observe",
        yfinance_adaptive_evaluation_window_seconds=1,
        yfinance_adaptive_healthy_windows_before_increase=2,
    )
    governor = YahooAdaptiveRateGovernor(
        settings,
        limiter,
        store,
        monotonic_clock=lambda: monotonic_now[0],
        utc_clock=lambda: utc_now[0],
        sleep_fn=lambda seconds: None,
    )

    monotonic_now[0] = 1
    governor.report(None, 100)
    monotonic_now[0] = 2
    governor.report(None, 100)
    snapshot = governor.snapshot()

    assert snapshot.enforced_rpm == 300
    assert snapshot.recommended_rpm == 330
    assert snapshot.enforced_concurrency == 4
    assert snapshot.recommended_concurrency == 5
    assert limiter.updates == []
    assert store.writes[-1]["current_rpm"] == 330


def test_adaptive_mode_applies_429_reduction_and_opens_cooldown() -> None:
    now = datetime(2026, 7, 17, tzinfo=UTC)
    limiter = _Limiter()
    settings = Settings(
        _env_file=None,
        yfinance_adaptive_rate_mode="adaptive",
        yfinance_adaptive_cooldown_seconds=60,
    )
    governor = YahooAdaptiveRateGovernor(
        settings,
        limiter,
        _Store(),
        utc_clock=lambda: now,
        sleep_fn=lambda seconds: None,
    )
    rate_limit = ProviderFailureClassification(
        code="rate_limited",
        retryable=True,
        affects_provider_health=True,
        status_code=429,
        retry_after_seconds=120,
    )

    governor.report(rate_limit, 50)
    snapshot = governor.snapshot()

    assert snapshot.enforced_rpm == 75
    assert snapshot.recommended_rpm == 75
    assert snapshot.circuit_state == "open"
    assert snapshot.cooldown_until == now + timedelta(seconds=120)
    assert limiter.updates == [("yfinance", 300), ("yfinance", 75)]


def test_adaptive_startup_uses_eighty_percent_of_last_safe_rate() -> None:
    limiter = _Limiter()
    store = _Store(
        {
            "last_safe_rpm": 500,
            "current_concurrency": 6,
            "circuit_state": "closed",
        }
    )
    settings = Settings(_env_file=None, yfinance_adaptive_rate_mode="adaptive")

    governor = YahooAdaptiveRateGovernor(settings, limiter, store)

    assert governor.snapshot().enforced_rpm == 400
    assert governor.snapshot().enforced_concurrency == 6
