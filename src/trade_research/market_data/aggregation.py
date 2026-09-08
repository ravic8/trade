from __future__ import annotations

import hashlib
from dataclasses import dataclass
from datetime import UTC, date, datetime, time, timedelta
from decimal import Decimal
from typing import Any
from zoneinfo import ZoneInfo

from trade_research.market_data.contracts import (
    CandleInterval,
    MarketCandle,
    candle_content_sha256,
)

_NSE_TIMEZONE = ZoneInfo("Asia/Kolkata")
_NSE_OPEN = time(9, 15)
_NSE_CLOSE = time(15, 30)
_AGGREGATE_MINUTES = {
    CandleInterval.FIVE_MINUTES: 5,
    CandleInterval.FIFTEEN_MINUTES: 15,
    CandleInterval.THIRTY_MINUTES: 30,
    CandleInterval.ONE_HOUR: 60,
}


@dataclass(frozen=True)
class IntradayAggregationRequest:
    instrument_id: str
    interval: CandleInterval
    window_start: datetime
    window_end: datetime
    provider: str = "yfinance"
    exchange: str = "NSE"
    workspace_id: str = "default"
    complete_only: bool = True
    limit: int = 5_000

    def __post_init__(self) -> None:
        if not self.instrument_id.strip():
            raise ValueError("instrument_id is required")
        if not self.provider.strip() or not self.workspace_id.strip():
            raise ValueError("provider and workspace_id are required")
        if self.provider.lower() != "yfinance":
            raise ValueError("V1 intraday aggregation supports provider=yfinance only")
        if self.exchange.upper() != "NSE":
            raise ValueError("V1 intraday aggregation supports NSE only")
        if self.interval not in _AGGREGATE_MINUTES:
            supported = ", ".join(interval.value for interval in _AGGREGATE_MINUTES)
            raise ValueError(f"aggregate interval must be one of: {supported}")
        if self.window_start.tzinfo is None or self.window_end.tzinfo is None:
            raise ValueError("aggregation window must be timezone-aware")
        if self.window_start >= self.window_end:
            raise ValueError("window_start must be before window_end")
        if self.window_end - self.window_start > timedelta(days=8):
            raise ValueError("aggregation window cannot exceed 8 days")
        if not 1 <= self.limit <= 100_000:
            raise ValueError("limit must be between 1 and 100000")
        object.__setattr__(self, "provider", self.provider.lower())
        object.__setattr__(self, "exchange", self.exchange.upper())
        object.__setattr__(self, "window_start", _as_utc(self.window_start))
        object.__setattr__(self, "window_end", _as_utc(self.window_end))

    @property
    def interval_minutes(self) -> int:
        return _AGGREGATE_MINUTES[self.interval]


@dataclass(frozen=True)
class AggregatedMarketCandle:
    workspace_id: str
    instrument_id: str
    exchange: str
    symbol: str
    provider_symbol: str
    currency: str
    candle_timestamp: datetime
    session_date: date
    interval: CandleInterval
    open: Decimal
    high: Decimal
    low: Decimal
    close: Decimal
    volume: int
    provider: str
    provider_timestamp: datetime
    source_rows: int
    expected_source_rows: int
    complete: bool
    source_digest: str
    source_run_ids: tuple[str, ...] = ()
    raw_artifact_ids: tuple[str, ...] = ()

    @classmethod
    def from_mapping(cls, row: dict[str, Any]) -> AggregatedMarketCandle:
        source_digest = row.get("source_digest")
        if source_digest is None:
            source_digest = aggregate_source_digest(row["source_content_digests"])
        return cls(
            workspace_id=str(row["workspace_id"]),
            instrument_id=str(row["instrument_id"]),
            exchange=str(row["exchange"]),
            symbol=str(row["symbol"]),
            provider_symbol=str(row["provider_symbol"]),
            currency=str(row["currency"]),
            candle_timestamp=_as_utc(row["candle_timestamp"]),
            session_date=_as_date(row["session_date"]),
            interval=CandleInterval(str(row["interval"])),
            open=Decimal(str(row["open"])),
            high=Decimal(str(row["high"])),
            low=Decimal(str(row["low"])),
            close=Decimal(str(row["close"])),
            volume=int(row["volume"]),
            provider=str(row["provider"]),
            provider_timestamp=_as_utc(row["provider_timestamp"]),
            source_rows=int(row["source_rows"]),
            expected_source_rows=int(row["expected_source_rows"]),
            complete=bool(row["complete"]),
            source_digest=str(source_digest),
            source_run_ids=tuple(str(value) for value in row.get("source_run_ids", ())),
            raw_artifact_ids=tuple(
                str(value) for value in row.get("raw_artifact_ids", ()) if value
            ),
        )


def aggregate_nse_minute_candles(
    request: IntradayAggregationRequest,
    candles: list[MarketCandle] | tuple[MarketCandle, ...],
) -> tuple[AggregatedMarketCandle, ...]:
    """Deterministic Python reference for the ClickHouse aggregation query."""

    selected: dict[datetime, MarketCandle] = {}
    for candle in candles:
        _validate_source_candle(request, candle)
        assert candle.timestamp is not None
        timestamp = _as_utc(candle.timestamp)
        if not (_as_utc(request.window_start) <= timestamp < _as_utc(request.window_end)):
            continue
        previous = selected.get(timestamp)
        if previous is not None and previous.canonical_payload() != candle.canonical_payload():
            raise ValueError(f"conflicting 1m source candle at {timestamp.isoformat()}")
        selected.setdefault(timestamp, candle)

    buckets: dict[datetime, list[MarketCandle]] = {}
    for candle in selected.values():
        assert candle.timestamp is not None
        bucket = nse_bucket_start(candle.timestamp, request.interval)
        buckets.setdefault(bucket, []).append(candle)

    aggregated: list[AggregatedMarketCandle] = []
    for bucket_timestamp, bucket_candles in sorted(buckets.items()):
        bucket_candles.sort(key=_candle_timestamp)
        expected_rows = nse_bucket_expected_minutes(bucket_timestamp, request.interval)
        complete = len(bucket_candles) == expected_rows
        if request.complete_only and not complete:
            continue
        first = bucket_candles[0]
        source_digests = sorted(candle_content_sha256(candle) for candle in bucket_candles)
        aggregated.append(
            AggregatedMarketCandle(
                workspace_id=request.workspace_id,
                instrument_id=first.instrument_id,
                exchange=first.exchange,
                symbol=first.symbol or first.provider_symbol,
                provider_symbol=first.provider_symbol,
                currency=first.currency,
                candle_timestamp=bucket_timestamp,
                session_date=first.session_date,
                interval=request.interval,
                open=first.open,
                high=max(candle.high for candle in bucket_candles),
                low=min(candle.low for candle in bucket_candles),
                close=bucket_candles[-1].close,
                volume=sum(candle.volume for candle in bucket_candles),
                provider=first.provider,
                provider_timestamp=max(
                    _as_utc(candle.provider_timestamp) for candle in bucket_candles
                ),
                source_rows=len(bucket_candles),
                expected_source_rows=expected_rows,
                complete=complete,
                source_digest=aggregate_source_digest(source_digests),
                raw_artifact_ids=tuple(
                    sorted(
                        {
                            candle.raw_artifact_id
                            for candle in bucket_candles
                            if candle.raw_artifact_id
                        }
                    )
                ),
            )
        )
        if len(aggregated) >= request.limit:
            break
    return tuple(aggregated)


def nse_bucket_start(timestamp: datetime, interval: CandleInterval) -> datetime:
    interval_minutes = _aggregate_minutes(interval)
    local_timestamp = _as_utc(timestamp).astimezone(_NSE_TIMEZONE)
    session_open = datetime.combine(local_timestamp.date(), _NSE_OPEN, tzinfo=_NSE_TIMEZONE)
    session_close = datetime.combine(local_timestamp.date(), _NSE_CLOSE, tzinfo=_NSE_TIMEZONE)
    if not session_open <= local_timestamp < session_close:
        raise ValueError("source candle is outside the NSE regular session")
    elapsed_minutes = int((local_timestamp - session_open).total_seconds() // 60)
    bucket_offset = (elapsed_minutes // interval_minutes) * interval_minutes
    return (session_open + timedelta(minutes=bucket_offset)).astimezone(UTC)


def aggregate_source_digest(content_digests: Any) -> str:
    normalized = sorted(
        value.decode("ascii") if isinstance(value, bytes) else str(value)
        for value in content_digests
    )
    return hashlib.sha256("\n".join(normalized).encode()).hexdigest()


def nse_bucket_expected_minutes(
    bucket_timestamp: datetime,
    interval: CandleInterval,
) -> int:
    interval_minutes = _aggregate_minutes(interval)
    local_bucket = _as_utc(bucket_timestamp).astimezone(_NSE_TIMEZONE)
    session_open = datetime.combine(local_bucket.date(), _NSE_OPEN, tzinfo=_NSE_TIMEZONE)
    session_close = datetime.combine(local_bucket.date(), _NSE_CLOSE, tzinfo=_NSE_TIMEZONE)
    if not session_open <= local_bucket < session_close:
        raise ValueError("aggregate bucket is outside the NSE regular session")
    remaining_minutes = int((session_close - local_bucket).total_seconds() // 60)
    return min(interval_minutes, remaining_minutes)


def _validate_source_candle(
    request: IntradayAggregationRequest,
    candle: MarketCandle,
) -> None:
    if candle.interval is not CandleInterval.ONE_MINUTE:
        raise ValueError("aggregation source candles must use interval=1m")
    if candle.exchange.upper() != request.exchange.upper():
        raise ValueError("source candle exchange does not match aggregation request")
    if candle.instrument_id != request.instrument_id:
        raise ValueError("source candle instrument does not match aggregation request")
    if candle.provider.lower() != request.provider.lower():
        raise ValueError("source candle provider does not match aggregation request")
    if candle.timestamp is None:
        raise ValueError("aggregation source candle is missing its timestamp")
    if candle.timestamp.second or candle.timestamp.microsecond:
        raise ValueError("aggregation source candles must align to a whole minute")
    nse_bucket_start(candle.timestamp, request.interval)


def _candle_timestamp(candle: MarketCandle) -> datetime:
    if candle.timestamp is None:
        raise ValueError("aggregation source candle is missing its timestamp")
    return _as_utc(candle.timestamp)


def _aggregate_minutes(interval: CandleInterval) -> int:
    try:
        return _AGGREGATE_MINUTES[interval]
    except KeyError as exc:
        raise ValueError(f"unsupported aggregate interval: {interval.value}") from exc


def _as_utc(value: datetime) -> datetime:
    if value.tzinfo is None:
        raise ValueError("datetime must be timezone-aware")
    return value.astimezone(UTC)


def _as_date(value: date | datetime | str) -> date:
    if isinstance(value, datetime):
        return value.date()
    if isinstance(value, date):
        return value
    return date.fromisoformat(value)
