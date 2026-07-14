from __future__ import annotations

from datetime import UTC, date, datetime, timedelta
from typing import Any

import pandas as pd

from trade_research.universe import YFinanceIntradayInstrument


class YFinanceDailyProvider:
    """Fetch daily OHLCV candles from yfinance."""

    def __init__(self, auto_adjust: bool = False) -> None:
        self.auto_adjust = auto_adjust

    def fetch_daily_ohlcv(
        self,
        symbols: list[dict[str, str]],
        start: date,
        end: date,
    ) -> pd.DataFrame:
        if not symbols:
            return pd.DataFrame()
        try:
            import yfinance as yf
        except ImportError as exc:  # pragma: no cover - exercised in installed runtime
            raise RuntimeError("Install yfinance to fetch yfinance daily data.") from exc

        tickers = [item["yahoo_symbol"] for item in symbols]
        raw = yf.download(
            tickers=tickers,
            start=start.isoformat(),
            end=(end + timedelta(days=1)).isoformat(),
            interval="1d",
            group_by="ticker",
            auto_adjust=self.auto_adjust,
            progress=False,
            threads=False,
        )
        return normalize_yfinance_daily(raw, symbols)


class YFinanceIntradayProvider:
    """Fetch 5-minute intraday OHLCV candles from yfinance."""

    def __init__(self, auto_adjust: bool = False) -> None:
        self.auto_adjust = auto_adjust

    def fetch_intraday_ohlcv(
        self,
        instruments: list[YFinanceIntradayInstrument],
        start: datetime,
        end: datetime,
        interval: str = "5m",
    ) -> pd.DataFrame:
        if not instruments:
            return pd.DataFrame()
        if interval != "5m":
            raise ValueError("Only interval=5m is supported for yfinance intraday.")
        try:
            import yfinance as yf
        except ImportError as exc:  # pragma: no cover - exercised in installed runtime
            raise RuntimeError("Install yfinance to fetch yfinance intraday data.") from exc

        tickers = [item.yahoo_symbol for item in instruments]
        raw = yf.download(
            tickers=tickers,
            start=start.astimezone(UTC).replace(tzinfo=None),
            end=end.astimezone(UTC).replace(tzinfo=None),
            interval=interval,
            group_by="ticker",
            auto_adjust=self.auto_adjust,
            progress=False,
            threads=False,
        )
        return normalize_yfinance_intraday(raw, instruments, interval=interval)


def normalize_yfinance_daily(frame: pd.DataFrame, symbols: list[dict[str, str]]) -> pd.DataFrame:
    if frame.empty or not symbols:
        return pd.DataFrame()

    frames: list[pd.DataFrame] = []
    if isinstance(frame.columns, pd.MultiIndex):
        top_level = set(frame.columns.get_level_values(0).astype(str))
        second_level = set(frame.columns.get_level_values(1).astype(str))
        ticker_first = any(item["yahoo_symbol"] in top_level for item in symbols)
        for item in symbols:
            ticker = item["yahoo_symbol"]
            if ticker_first:
                if ticker not in top_level:
                    continue
                ticker_frame = frame[ticker].copy()
            else:
                if ticker not in second_level:
                    continue
                ticker_frame = frame.xs(ticker, axis=1, level=1).copy()
            normalized = _normalize_single_ticker_frame(ticker_frame, item)
            if not normalized.empty:
                frames.append(normalized)
    else:
        normalized = _normalize_single_ticker_frame(frame.copy(), symbols[0])
        if not normalized.empty:
            frames.append(normalized)

    if not frames:
        return pd.DataFrame()
    return pd.concat(frames, ignore_index=True).sort_values(["Symbol", "Date"]).reset_index(
        drop=True
    )


def _normalize_single_ticker_frame(frame: pd.DataFrame, symbol: dict[str, str]) -> pd.DataFrame:
    required = ["Open", "High", "Low", "Close", "Volume"]
    if any(column not in frame.columns for column in required):
        return pd.DataFrame()

    data = frame.reset_index()
    date_column = "Date" if "Date" in data.columns else data.columns[0]
    rows: list[dict[str, Any]] = []
    for record in data.to_dict(orient="records"):
        candle_date = pd.to_datetime(record.get(date_column), errors="coerce")
        if pd.isna(candle_date):
            continue
        if any(pd.isna(record.get(column)) for column in required):
            continue
        rows.append(
            {
                "Date": candle_date.date(),
                "Open": float(record["Open"]),
                "High": float(record["High"]),
                "Low": float(record["Low"]),
                "Close": float(record["Close"]),
                "AdjClose": (
                    float(record["Adj Close"])
                    if "Adj Close" in record and not pd.isna(record.get("Adj Close"))
                    else None
                ),
                "Volume": int(record["Volume"]),
                "OpenInterest": None,
                "InstrumentKey": symbol["instrument_key"],
                "Symbol": symbol["symbol"],
                "TradingSymbol": symbol["yahoo_symbol"],
                "Source": "yfinance",
            }
        )
    return pd.DataFrame(rows)


def normalize_yfinance_intraday(
    frame: pd.DataFrame,
    instruments: list[YFinanceIntradayInstrument],
    interval: str = "5m",
) -> pd.DataFrame:
    if frame.empty or not instruments:
        return pd.DataFrame()

    frames: list[pd.DataFrame] = []
    if isinstance(frame.columns, pd.MultiIndex):
        top_level = set(frame.columns.get_level_values(0).astype(str))
        second_level = set(frame.columns.get_level_values(1).astype(str))
        ticker_first = any(item.yahoo_symbol in top_level for item in instruments)
        for item in instruments:
            ticker = item.yahoo_symbol
            if ticker_first:
                if ticker not in top_level:
                    continue
                ticker_frame = frame[ticker].copy()
            else:
                if ticker not in second_level:
                    continue
                ticker_frame = frame.xs(ticker, axis=1, level=1).copy()
            normalized = _normalize_single_intraday_frame(ticker_frame, item, interval)
            if not normalized.empty:
                frames.append(normalized)
    else:
        normalized = _normalize_single_intraday_frame(frame.copy(), instruments[0], interval)
        if not normalized.empty:
            frames.append(normalized)

    if not frames:
        return pd.DataFrame()
    return pd.concat(frames, ignore_index=True).sort_values(["Symbol", "Timestamp"]).reset_index(
        drop=True
    )


def _normalize_single_intraday_frame(
    frame: pd.DataFrame,
    instrument: YFinanceIntradayInstrument,
    interval: str,
) -> pd.DataFrame:
    required = ["Open", "High", "Low", "Close", "Volume"]
    if any(column not in frame.columns for column in required):
        return pd.DataFrame()

    data = frame.reset_index()
    ts_column = "Datetime" if "Datetime" in data.columns else data.columns[0]
    rows: list[dict[str, Any]] = []
    for record in data.to_dict(orient="records"):
        timestamp = pd.to_datetime(record.get(ts_column), errors="coerce", utc=True)
        if pd.isna(timestamp):
            continue
        if any(pd.isna(record.get(column)) for column in ["Open", "High", "Low", "Close"]):
            continue
        rows.append(
            {
                "Timestamp": timestamp.to_pydatetime(),
                "Open": float(record["Open"]),
                "High": float(record["High"]),
                "Low": float(record["Low"]),
                "Close": float(record["Close"]),
                "Volume": float(record.get("Volume") or 0.0),
                "InstrumentKey": instrument.instrument_key,
                "Symbol": instrument.symbol,
                "TradingSymbol": instrument.yahoo_symbol,
                "Exchange": instrument.exchange,
                "AssetClass": instrument.asset_class,
                "Interval": interval,
                "Source": "yfinance",
            }
        )
    return pd.DataFrame(rows)
