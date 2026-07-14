from __future__ import annotations

from dataclasses import dataclass
from datetime import date


@dataclass(frozen=True)
class HistoricalCapability:
    unit: str
    interval_min: int
    interval_max: int
    available_from: date
    max_window: str | None
    notes: str | None = None


@dataclass(frozen=True)
class RateLimits:
    standard_api_per_second: int
    standard_api_per_minute: int
    standard_api_per_30_minutes: int


@dataclass(frozen=True)
class ProviderCapability:
    provider: str
    api_version: str
    source_url: str
    historical: tuple[HistoricalCapability, ...]
    rate_limits: RateLimits
    notes: tuple[str, ...] = ()


UPSTOX_V3_HISTORICAL_CANDLE_DOCS_URL = (
    "https://upstox.com/developer/api-documentation/v3/get-historical-candle-data/"
)
UPSTOX_RATE_LIMIT_DOCS_URL = "https://upstox.com/developer/api-documentation/rate-limiting/"
YFINANCE_SOURCE_URL = "https://pypi.org/project/yfinance/"


UPSTOX_V3_CAPABILITY = ProviderCapability(
    provider="upstox",
    api_version="v3",
    source_url=UPSTOX_V3_HISTORICAL_CANDLE_DOCS_URL,
    historical=(
        HistoricalCapability(
            unit="minutes",
            interval_min=1,
            interval_max=15,
            available_from=date(2022, 1, 1),
            max_window="1 month",
        ),
        HistoricalCapability(
            unit="minutes",
            interval_min=16,
            interval_max=300,
            available_from=date(2022, 1, 1),
            max_window="1 quarter",
        ),
        HistoricalCapability(
            unit="hours",
            interval_min=1,
            interval_max=5,
            available_from=date(2022, 1, 1),
            max_window="1 quarter",
        ),
        HistoricalCapability(
            unit="days",
            interval_min=1,
            interval_max=1,
            available_from=date(2000, 1, 1),
            max_window="10 years",
        ),
        HistoricalCapability(
            unit="weeks",
            interval_min=1,
            interval_max=1,
            available_from=date(2000, 1, 1),
            max_window=None,
            notes="Upstox documents no per-request historical retrieval limit.",
        ),
        HistoricalCapability(
            unit="months",
            interval_min=1,
            interval_max=1,
            available_from=date(2000, 1, 1),
            max_window=None,
            notes="Upstox documents no per-request historical retrieval limit.",
        ),
    ),
    rate_limits=RateLimits(
        standard_api_per_second=50,
        standard_api_per_minute=500,
        standard_api_per_30_minutes=2000,
    ),
    notes=(
        "Historical V3 candles use unit plus numeric interval.",
        "Intraday V3 candles are current-trading-day only and are not part of this MVP.",
        f"Rate limits source: {UPSTOX_RATE_LIMIT_DOCS_URL}",
    ),
)

YFINANCE_CAPABILITY = ProviderCapability(
    provider="yfinance",
    api_version="library",
    source_url=YFINANCE_SOURCE_URL,
    historical=(
        HistoricalCapability(
            unit="days",
            interval_min=1,
            interval_max=1,
            available_from=date(1900, 1, 1),
            max_window=None,
            notes="Coverage varies by ticker and Yahoo Finance availability.",
        ),
    ),
    rate_limits=RateLimits(
        standard_api_per_second=0,
        standard_api_per_minute=30,
        standard_api_per_30_minutes=900,
    ),
    notes=(
        "Unofficial Yahoo Finance library; use conservative batching and retries.",
        "Phase 3A stores raw OHLCV into ohlcv_daily; adjusted close storage is deferred.",
    ),
)


def provider_capability(provider: str) -> ProviderCapability:
    normalized = provider.strip().lower()
    if normalized == "upstox":
        return UPSTOX_V3_CAPABILITY
    if normalized == "yfinance":
        return YFINANCE_CAPABILITY
    raise ValueError(f"Unsupported provider: {provider}")
