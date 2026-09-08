from __future__ import annotations

from collections.abc import Mapping
from datetime import UTC, datetime
from decimal import Decimal
from typing import Any
from zoneinfo import ZoneInfo

import pandas as pd

from trade_research.market_data.contracts import MarketCandle, ProviderRequest

_NSE_TIMEZONE = ZoneInfo("Asia/Kolkata")


def yfinance_frame_to_candles(
    frame: pd.DataFrame,
    request: ProviderRequest,
    *,
    currency: str,
    raw_artifact_id: str | None = None,
    canonical_instrument_ids: Mapping[str, str] | None = None,
) -> list[MarketCandle]:
    """Translate an existing normalized yfinance frame into the common contract."""

    if frame.empty:
        return []
    date_column = "Timestamp" if request.interval.is_intraday else "Date"
    required = {
        date_column,
        "InstrumentKey",
        "TradingSymbol",
        "Open",
        "High",
        "Low",
        "Close",
        "Volume",
    }
    missing = sorted(required - set(frame.columns))
    if missing:
        raise ValueError("normalized yfinance frame is missing: " + ", ".join(missing))

    candles: list[MarketCandle] = []
    for row in frame.to_dict(orient="records"):
        timestamp = _timestamp(row[date_column]) if request.interval.is_intraday else None
        session_date = (
            timestamp.astimezone(UTC).date()
            if timestamp is not None
            else pd.Timestamp(row[date_column]).date()
        )
        exchange = str(row.get("Exchange") or request.exchange).upper()
        if exchange == "NSE" and timestamp is not None:
            session_date = timestamp.astimezone(_NSE_TIMEZONE).date()
        candles.append(
            MarketCandle(
                instrument_id=str(
                    (canonical_instrument_ids or {}).get(
                        str(row["TradingSymbol"]),
                        (canonical_instrument_ids or {}).get(
                            str(row["InstrumentKey"]),
                            str(row["InstrumentKey"]),
                        ),
                    )
                ),
                provider_symbol=str(row["TradingSymbol"]),
                symbol=str(row.get("Symbol") or row["TradingSymbol"]),
                exchange=exchange,
                session_date=session_date,
                open=_decimal(row["Open"]),
                high=_decimal(row["High"]),
                low=_decimal(row["Low"]),
                close=_decimal(row["Close"]),
                volume=int(row["Volume"] or 0),
                currency=currency,
                provider=request.provider,
                provider_timestamp=request.retrieved_at,
                request_id=request.request_id,
                raw_artifact_id=raw_artifact_id,
                adapter_version=request.adapter_version,
                interval=request.interval,
                timestamp=timestamp,
            )
        )
    return candles


def _timestamp(value: Any) -> datetime:
    parsed = pd.Timestamp(value)
    if parsed.tzinfo is None:
        parsed = parsed.tz_localize(UTC)
    return parsed.to_pydatetime().astimezone(UTC)


def _decimal(value: Any) -> Decimal:
    return Decimal(str(value))
