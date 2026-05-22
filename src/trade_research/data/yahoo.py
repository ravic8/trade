import random
import time
from collections.abc import Sequence
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import date

import pandas as pd
import yfinance as yf

from trade_research.data.base import MarketDataProvider


class YahooFinanceMarketDataProvider(MarketDataProvider):
    """Development market data provider.

    Use this for local iteration. Production deployments should add licensed vendor providers
    and use Yahoo only as a fallback.
    """

    def __init__(
        self,
        batch_size: int = 20,
        throttle_seconds: float = 1.0,
        max_workers: int = 2,
        retry_attempts: int = 3,
        retry_base_seconds: float = 1.0,
        jitter_seconds: float = 0.5,
    ) -> None:
        self.batch_size = batch_size
        self.throttle_seconds = throttle_seconds
        self.max_workers = max_workers
        self.retry_attempts = retry_attempts
        self.retry_base_seconds = retry_base_seconds
        self.jitter_seconds = jitter_seconds

    def fetch_daily_ohlcv(
        self,
        tickers: Sequence[str],
        start: date,
        end: date,
    ) -> pd.DataFrame:
        all_data: list[pd.DataFrame] = []
        ticker_list = [ticker for ticker in tickers if ticker]

        batches = list(self._batches(ticker_list))
        with ThreadPoolExecutor(max_workers=self.max_workers) as executor:
            futures = []
            for batch in batches:
                futures.append(
                    executor.submit(
                        self._download_with_retries,
                        batch,
                        {
                            "start": start.isoformat(),
                            "end": end.isoformat(),
                            "auto_adjust": True,
                            "group_by": "ticker",
                            "threads": True,
                            "progress": False,
                            "timeout": 30,
                        },
                    )
                )
                time.sleep(self.throttle_seconds)

            for future in as_completed(futures):
                data = future.result()
                if not data.empty:
                    all_data.append(data)

        if not all_data:
            return pd.DataFrame()

        wide = pd.concat(all_data, axis=1)
        return self._to_long(wide, ticker_list)

    def fetch_hourly_ohlcv(
        self,
        tickers: Sequence[str],
        period: str = "7d",
    ) -> pd.DataFrame:
        all_data: list[pd.DataFrame] = []
        ticker_list = [ticker for ticker in tickers if ticker]

        batches = list(self._batches(ticker_list))
        with ThreadPoolExecutor(max_workers=self.max_workers) as executor:
            futures = []
            for batch in batches:
                futures.append(
                    executor.submit(
                        self._download_with_retries,
                        batch,
                        {
                            "period": period,
                            "interval": "1h",
                            "auto_adjust": True,
                            "group_by": "ticker",
                            "threads": True,
                            "progress": False,
                            "timeout": 30,
                        },
                    )
                )
                time.sleep(self.throttle_seconds)

            for future in as_completed(futures):
                data = future.result()
                if not data.empty:
                    all_data.append(data)

        if not all_data:
            return pd.DataFrame()

        wide = pd.concat(all_data, axis=1)
        out = self._to_long(wide, ticker_list)
        if out.empty:
            return out

        datetime_column = "Datetime" if "Datetime" in out.columns else "Date"
        out = out.rename(columns={datetime_column: "Datetime"})
        out["Datetime"] = pd.to_datetime(out["Datetime"], utc=True)
        return out.sort_values(["Ticker", "Datetime"]).reset_index(drop=True)

    def _batches(self, tickers: Sequence[str]) -> list[list[str]]:
        return [
            list(tickers[i : i + self.batch_size])
            for i in range(0, len(tickers), self.batch_size)
        ]

    def _download_with_retries(self, batch: Sequence[str], kwargs: dict) -> pd.DataFrame:
        for attempt in range(self.retry_attempts):
            try:
                return yf.download(
                    batch,
                    **kwargs,
                )
            except Exception:
                if attempt == self.retry_attempts - 1:
                    return pd.DataFrame()
                sleep_seconds = (
                    self.retry_base_seconds * (2**attempt)
                    + random.uniform(0, self.jitter_seconds)
                )
                time.sleep(sleep_seconds)
        return pd.DataFrame()

    @staticmethod
    def _to_long(wide: pd.DataFrame, tickers: Sequence[str]) -> pd.DataFrame:
        frames: list[pd.DataFrame] = []
        if not isinstance(wide.columns, pd.MultiIndex):
            if len(tickers) == 1:
                frame = wide.copy()
                if _is_empty_ohlcv_frame(frame):
                    return pd.DataFrame()
                frame.index.name = frame.index.name or "Datetime"
                frame["Ticker"] = tickers[0]
                frames.append(frame.reset_index())
            return pd.concat(frames, ignore_index=True) if frames else pd.DataFrame()

        first_level = wide.columns.get_level_values(0)
        for ticker in tickers:
            if ticker not in first_level:
                continue
            frame = wide[ticker].copy()
            if _is_empty_ohlcv_frame(frame):
                continue
            frame.index.name = frame.index.name or "Datetime"
            frame["Ticker"] = ticker
            frames.append(frame.reset_index())

        if not frames:
            return pd.DataFrame()

        out = pd.concat(frames, ignore_index=True)
        time_column = _time_column(out)
        return out.sort_values(["Ticker", time_column]).reset_index(drop=True)


def _is_empty_ohlcv_frame(frame: pd.DataFrame) -> bool:
    ohlcv_columns = [
        column
        for column in ["Open", "High", "Low", "Close", "Volume"]
        if column in frame
    ]
    return not ohlcv_columns or frame[ohlcv_columns].dropna(how="all").empty


def _time_column(frame: pd.DataFrame) -> str:
    if "Datetime" in frame.columns:
        return "Datetime"
    if "Date" in frame.columns:
        return "Date"
    return str(frame.columns[0])
