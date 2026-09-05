"""Provider-neutral market-data contracts and validation."""

from trade_research.market_data.adapters import yfinance_frame_to_candles
from trade_research.market_data.aggregation import (
    AggregatedMarketCandle,
    IntradayAggregationRequest,
    aggregate_nse_minute_candles,
    nse_bucket_expected_minutes,
    nse_bucket_start,
)
from trade_research.market_data.contracts import (
    CandleInterval,
    MarketCandle,
    ProviderRequest,
    candle_content_sha256,
)
from trade_research.market_data.health import (
    MarketDataHealthRepository,
    MarketDataHealthSnapshot,
)
from trade_research.market_data.raw_snapshots import RawSnapshotWriter, StoredRawSnapshot
from trade_research.market_data.validation import (
    CandleValidationIssue,
    CandleValidationResult,
    MarketDataValidationError,
    validate_candle_batch,
)

__all__ = [
    "AggregatedMarketCandle",
    "CandleInterval",
    "CandleValidationIssue",
    "CandleValidationResult",
    "IntradayAggregationRequest",
    "MarketCandle",
    "MarketDataValidationError",
    "MarketDataHealthRepository",
    "MarketDataHealthSnapshot",
    "ProviderRequest",
    "RawSnapshotWriter",
    "StoredRawSnapshot",
    "aggregate_nse_minute_candles",
    "candle_content_sha256",
    "nse_bucket_expected_minutes",
    "nse_bucket_start",
    "validate_candle_batch",
    "yfinance_frame_to_candles",
]
