"""Contract for the existing yfinance daily-batch execution boundary."""

from __future__ import annotations

from datetime import date
from typing import Protocol

import pandas as pd


class YFinanceBatchProvider(Protocol):
    """Return normalized daily rows for yfinance-mapped instruments.

    This is deliberately yfinance-specific: symbols contain ``yahoo_symbol``
    and the result must use the current canonical ``InstrumentKey`` column.
    ``start`` and ``end`` are inclusive exchange-session dates. Nonempty rows
    contain Date, Open, High, Low, Close, Volume, InstrumentKey, Symbol,
    TradingSymbol, and Source. Date is an exchange-local date; Source must be
    yfinance, and InstrumentKey must preserve the requested provider identity.
    AdjClose and OpenInterest are nullable. An empty frame reports no returned
    observations; it is not proof of a holiday or unsupported instrument.
    Provider exceptions propagate to the executor's failure classifier.

    A provider-independent contract needs its own provenance and symbol mapping
    design before other vendors can be plugged into this executor.
    """

    def fetch_daily_ohlcv(
        self,
        symbols: list[dict[str, str]],
        start: date,
        end: date,
    ) -> pd.DataFrame: ...
