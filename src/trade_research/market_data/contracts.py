from __future__ import annotations

import hashlib
import json
from dataclasses import asdict, dataclass, field
from datetime import UTC, date, datetime
from decimal import Decimal
from enum import StrEnum
from typing import Any


class CandleInterval(StrEnum):
    ONE_MINUTE = "1m"
    FIVE_MINUTES = "5m"
    FIFTEEN_MINUTES = "15m"
    THIRTY_MINUTES = "30m"
    ONE_HOUR = "1h"
    ONE_DAY = "1d"

    @property
    def is_intraday(self) -> bool:
        return self is not CandleInterval.ONE_DAY


@dataclass(frozen=True)
class ProviderRequest:
    """Provider-independent identity for one bounded market-data request."""

    request_id: str
    provider: str
    exchange: str
    interval: CandleInterval
    window_start: datetime
    window_end: datetime
    provider_symbols: tuple[str, ...]
    retrieved_at: datetime
    adapter_version: str
    parameters: dict[str, Any] = field(default_factory=dict)

    def __post_init__(self) -> None:
        if not self.request_id.strip():
            raise ValueError("request_id is required")
        if not self.provider.strip() or not self.exchange.strip():
            raise ValueError("provider and exchange are required")
        if not self.provider_symbols:
            raise ValueError("provider_symbols must not be empty")
        if not self.adapter_version.strip():
            raise ValueError("adapter_version is required")
        for name, value in (
            ("window_start", self.window_start),
            ("window_end", self.window_end),
            ("retrieved_at", self.retrieved_at),
        ):
            if value.tzinfo is None:
                raise ValueError(f"{name} must be timezone-aware")
        if self.window_start >= self.window_end:
            raise ValueError("window_start must be before window_end")


@dataclass(frozen=True)
class MarketCandle:
    """Canonical candle emitted by any provider adapter before persistence."""

    instrument_id: str
    provider_symbol: str
    exchange: str
    session_date: date
    open: Decimal
    high: Decimal
    low: Decimal
    close: Decimal
    volume: int
    currency: str
    provider: str
    provider_timestamp: datetime
    request_id: str
    adapter_version: str
    interval: CandleInterval
    timestamp: datetime | None = None
    raw_artifact_id: str | None = None
    symbol: str | None = None

    def __post_init__(self) -> None:
        required = {
            "instrument_id": self.instrument_id,
            "provider_symbol": self.provider_symbol,
            "exchange": self.exchange,
            "currency": self.currency,
            "provider": self.provider,
            "request_id": self.request_id,
            "adapter_version": self.adapter_version,
        }
        missing = [name for name, value in required.items() if not value.strip()]
        if missing:
            raise ValueError("required candle fields are empty: " + ", ".join(missing))
        if self.provider_timestamp.tzinfo is None:
            raise ValueError("provider_timestamp must be timezone-aware")
        if self.interval.is_intraday:
            if self.timestamp is None or self.timestamp.tzinfo is None:
                raise ValueError("intraday candles require a timezone-aware timestamp")
        elif self.timestamp is not None:
            raise ValueError("daily candles must not set timestamp")

    @property
    def identity(self) -> tuple[str, CandleInterval, date, datetime | None]:
        return (self.instrument_id, self.interval, self.session_date, self.timestamp)

    def canonical_payload(self) -> dict[str, Any]:
        payload = asdict(self)
        payload["session_date"] = self.session_date.isoformat()
        payload["provider_timestamp"] = self.provider_timestamp.astimezone(UTC).isoformat()
        payload["timestamp"] = (
            self.timestamp.astimezone(UTC).isoformat() if self.timestamp is not None else None
        )
        payload["interval"] = self.interval.value
        for field_name in ("open", "high", "low", "close"):
            payload[field_name] = format(getattr(self, field_name), "f")
        return payload


def candle_content_sha256(candle: MarketCandle) -> str:
    encoded = json.dumps(
        candle.canonical_payload(),
        sort_keys=True,
        separators=(",", ":"),
    ).encode("utf-8")
    return hashlib.sha256(encoded).hexdigest()
