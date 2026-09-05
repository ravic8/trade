"""Provider-neutral market-data contracts and validation."""

from trade_research.market_data.adapters import yfinance_frame_to_candles
from trade_research.market_data.contracts import (
    CandleInterval,
    MarketCandle,
    ProviderRequest,
    candle_content_sha256,
)
from trade_research.market_data.raw_snapshots import RawSnapshotWriter, StoredRawSnapshot
from trade_research.market_data.validation import (
    CandleValidationIssue,
    CandleValidationResult,
    MarketDataValidationError,
    validate_candle_batch,
)

__all__ = [
    "CandleInterval",
    "CandleValidationIssue",
    "CandleValidationResult",
    "MarketCandle",
    "MarketDataValidationError",
    "ProviderRequest",
    "RawSnapshotWriter",
    "StoredRawSnapshot",
    "candle_content_sha256",
    "validate_candle_batch",
    "yfinance_frame_to_candles",
]
