from __future__ import annotations

import lzma
import struct
from collections.abc import Iterable
from datetime import UTC, datetime, timedelta

import httpx
import pandas as pd

from trade_research.universe import DukascopyInstrument

DUKASCOPY_DATAFEED_BASE_URL = "https://datafeed.dukascopy.com/datafeed"
DUKASCOPY_INTERVAL_5M = "5m"


class DukascopyHistoricalProvider:
    """Fetch and normalize Dukascopy tick archives into intraday candles."""

    def __init__(
        self,
        base_url: str = DUKASCOPY_DATAFEED_BASE_URL,
        timeout_seconds: float = 15.0,
    ) -> None:
        self.base_url = base_url.rstrip("/")
        self.timeout_seconds = timeout_seconds

    def fetch_hour_ticks(
        self,
        instrument: DukascopyInstrument,
        hour_start: datetime,
    ) -> pd.DataFrame:
        if hour_start.tzinfo is None:
            hour_start = hour_start.replace(tzinfo=UTC)
        else:
            hour_start = hour_start.astimezone(UTC)
        url = dukascopy_hour_url(self.base_url, instrument, hour_start)
        with httpx.Client(timeout=self.timeout_seconds, follow_redirects=True) as client:
            response = client.get(url)
        if response.status_code == 404:
            return pd.DataFrame()
        response.raise_for_status()
        return normalize_dukascopy_ticks(
            response.content,
            instrument=instrument,
            hour_start=hour_start,
        )


def dukascopy_hour_url(
    base_url: str,
    instrument: DukascopyInstrument,
    hour_start: datetime,
) -> str:
    hour_start = hour_start.astimezone(UTC)
    return (
        f"{base_url.rstrip('/')}/{instrument.dukascopy_id.upper()}/"
        f"{hour_start.year:04d}/{hour_start.month - 1:02d}/{hour_start.day:02d}/"
        f"{hour_start.hour:02d}h_ticks.bi5"
    )


def normalize_dukascopy_ticks(
    payload: bytes,
    instrument: DukascopyInstrument,
    hour_start: datetime,
) -> pd.DataFrame:
    if not payload:
        return pd.DataFrame()
    decompressed = _decompress_bi5(payload)
    rows = []
    scale = _price_scale(instrument)
    for offset in range(0, len(decompressed), 20):
        chunk = decompressed[offset : offset + 20]
        if len(chunk) < 20:
            continue
        milliseconds, ask_raw, bid_raw, ask_volume, bid_volume = struct.unpack(
            ">iii ff".replace(" ", ""),
            chunk,
        )
        timestamp = hour_start + timedelta(milliseconds=int(milliseconds))
        ask = ask_raw / scale
        bid = bid_raw / scale
        rows.append(
            {
                "Timestamp": timestamp,
                "Ask": ask,
                "Bid": bid,
                "Price": (ask + bid) / 2,
                "Volume": float(ask_volume) + float(bid_volume),
                "InstrumentKey": instrument.instrument_key,
                "Symbol": instrument.symbol,
                "TradingSymbol": instrument.dukascopy_id.upper(),
                "Exchange": instrument.exchange,
                "AssetClass": instrument.asset_class,
                "Source": "dukascopy",
            }
        )
    return pd.DataFrame(rows)


def aggregate_ticks_to_ohlcv(
    ticks: pd.DataFrame,
    instrument: DukascopyInstrument,
    interval: str = DUKASCOPY_INTERVAL_5M,
) -> pd.DataFrame:
    if ticks.empty:
        return pd.DataFrame()
    if interval != DUKASCOPY_INTERVAL_5M:
        raise ValueError("Only 5m Dukascopy aggregation is currently supported.")

    frame = ticks.copy()
    frame["Timestamp"] = pd.to_datetime(frame["Timestamp"], utc=True)
    frame = frame.sort_values("Timestamp").set_index("Timestamp")
    grouped = frame.resample("5min", label="left", closed="left").agg(
        Open=("Price", "first"),
        High=("Price", "max"),
        Low=("Price", "min"),
        Close=("Price", "last"),
        Volume=("Volume", "sum"),
    )
    grouped = grouped.dropna(subset=["Open", "High", "Low", "Close"]).reset_index()
    if grouped.empty:
        return pd.DataFrame()
    grouped["InstrumentKey"] = instrument.instrument_key
    grouped["Symbol"] = instrument.symbol
    grouped["TradingSymbol"] = instrument.dukascopy_id.upper()
    grouped["Exchange"] = instrument.exchange
    grouped["AssetClass"] = instrument.asset_class
    grouped["Interval"] = interval
    grouped["Source"] = "dukascopy"
    return grouped[
        [
            "Timestamp",
            "Open",
            "High",
            "Low",
            "Close",
            "Volume",
            "InstrumentKey",
            "Symbol",
            "TradingSymbol",
            "Exchange",
            "AssetClass",
            "Interval",
            "Source",
        ]
    ]


def combine_tick_frames(
    frames: Iterable[pd.DataFrame],
    instrument: DukascopyInstrument,
    interval: str = DUKASCOPY_INTERVAL_5M,
) -> pd.DataFrame:
    non_empty = [frame for frame in frames if not frame.empty]
    if not non_empty:
        return pd.DataFrame()
    ticks = pd.concat(non_empty, ignore_index=True)
    return aggregate_ticks_to_ohlcv(ticks, instrument=instrument, interval=interval)


def _decompress_bi5(payload: bytes) -> bytes:
    try:
        return lzma.decompress(payload)
    except lzma.LZMAError:
        return lzma.decompress(
            payload,
            format=lzma.FORMAT_RAW,
            filters=[{"id": lzma.FILTER_LZMA1, "dict_size": 1 << 23}],
        )


def _price_scale(instrument: DukascopyInstrument) -> float:
    if instrument.asset_class == "crypto":
        return 1_000.0
    if "JPY" in instrument.symbol:
        return 1_000.0
    return 100_000.0
