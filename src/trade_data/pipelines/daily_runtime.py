"""Composition point for the existing daily yfinance ingestion workflows."""

from __future__ import annotations

from collections.abc import Callable
from dataclasses import dataclass

from trade_data.config import Settings
from trade_data.data.daily_provider import YFinanceBatchProvider
from trade_data.data.rate_limits import ProviderRateLimiter, build_provider_rate_limiter
from trade_data.data.yfinance_provider import YFinanceDailyProvider
from trade_data.storage.daily_contract import DailyIngestionStore
from trade_data.storage.timescale import TimescaleStore


@dataclass(frozen=True)
class YFinanceDailyRuntime:
    """Dependencies shared by the daily planner, worker, and direct fetch path.

    ``store_factory`` implements the explicit daily storage interface. The
    default adapter is PostgreSQL/TimescaleDB; alternative backends need the
    same transaction and SQLAlchemy-engine semantics before they are supported.
    """

    store_factory: Callable[[str], DailyIngestionStore]
    provider_factory: Callable[[], YFinanceBatchProvider]
    rate_limiter_factory: Callable[[Settings], ProviderRateLimiter] = build_provider_rate_limiter

    def open_store(self, settings: Settings) -> DailyIngestionStore:
        return self.store_factory(settings.database_url)

    def open_rate_limiter(self, settings: Settings) -> ProviderRateLimiter:
        return self.rate_limiter_factory(settings)


def build_yfinance_daily_runtime(
    *,
    store_factory: Callable[[str], DailyIngestionStore] = TimescaleStore,
    rate_limiter_factory: Callable[[Settings], ProviderRateLimiter] = build_provider_rate_limiter,
) -> YFinanceDailyRuntime:
    """Wire production adapters; no connection or provider request occurs here."""

    return YFinanceDailyRuntime(
        store_factory=store_factory,
        provider_factory=lambda: YFinanceDailyProvider(auto_adjust=False),
        rate_limiter_factory=rate_limiter_factory,
    )
