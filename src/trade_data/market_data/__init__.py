"""Provider-neutral market-data contracts and validation."""

from trade_data.market_data.adapters import yfinance_frame_to_candles
from trade_data.market_data.contracts import (
    CandleInterval,
    MarketCandle,
    ProviderRequest,
    candle_business_sha256,
    candle_content_sha256,
)
from trade_data.market_data.health import (
    MarketDataHealthRepository,
    MarketDataHealthSnapshot,
)
from trade_data.market_data.raw_snapshots import RawSnapshotWriter, StoredRawSnapshot
from trade_data.market_data.validation import (
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
    "MarketDataHealthRepository",
    "MarketDataHealthSnapshot",
    "ProviderRequest",
    "RawSnapshotWriter",
    "StoredRawSnapshot",
    "candle_business_sha256",
    "candle_content_sha256",
    "validate_candle_batch",
    "yfinance_frame_to_candles",
]
