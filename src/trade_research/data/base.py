from abc import ABC, abstractmethod
from collections.abc import Sequence
from datetime import date

import pandas as pd


class MarketDataProvider(ABC):
    """Abstract base class representing a market data provider.

    Implementations of this interface fetch historical daily or hourly market data (OHLCV).
    """

    @abstractmethod
    def fetch_daily_ohlcv(
        self,
        tickers: Sequence[str],
        start: date,
        end: date,
    ) -> pd.DataFrame:
        """Fetch daily OHLCV candles for the requested tickers between start and end dates.

        Args:
            tickers: Sequence of ticker strings (e.g., ["RELIANCE.NS", "WDO.TO"])
            start: Start date of the fetch window
            end: End date of the fetch window

        Returns:
            A pandas DataFrame with long-format columns:
            ["Datetime", "Open", "High", "Low", "Close", "Volume", "Ticker"]
        """
        pass

    @abstractmethod
    def fetch_hourly_ohlcv(
        self,
        tickers: Sequence[str],
        period: str = "7d",
    ) -> pd.DataFrame:
        """Fetch hourly OHLCV candles for the requested tickers for the lookback period.

        Args:
            tickers: Sequence of ticker strings
            period: Yahoo-style period string (e.g. "7d", "10d") or lookback code

        Returns:
            A pandas DataFrame with long-format columns:
            ["Datetime", "Open", "High", "Low", "Close", "Volume", "Ticker"]
        """
        pass
