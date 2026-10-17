from __future__ import annotations

from collections.abc import Mapping
from decimal import Decimal
from typing import Any
from zoneinfo import ZoneInfo

import pandas as pd

from trade_data.market_data.contracts import MarketCandle, ProviderRequest

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
    date_column = "Date"
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
        session_date = pd.Timestamp(row[date_column]).date()
        exchange = str(row.get("Exchange") or request.exchange).upper()
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
                timestamp=None,
            )
        )
    return candles


def _decimal(value: Any) -> Decimal:
    return Decimal(str(value))
