from datetime import date

import pandas as pd

from trade_research.data.yfinance_provider import (
    normalize_yfinance_daily,
    normalize_yfinance_intraday,
)
from trade_research.universe import yfinance_intraday_universe, yfinance_seed_universe


def test_yfinance_seed_universe_returns_us_and_canada_symbols() -> None:
    us = yfinance_seed_universe("us_seed")
    canada = yfinance_seed_universe("canada_seed")

    assert len(us) == 20
    assert us[0].symbol == "AAPL"
    assert us[0].yahoo_symbol == "AAPL"
    assert us[0].currency == "USD"
    assert len(canada) == 20
    assert canada[0].symbol == "SHOP"
    assert canada[0].exchange == "TSX"
    assert canada[0].yahoo_symbol == "SHOP.TO"
    assert canada[0].currency == "CAD"


def test_yfinance_intraday_universe_maps_fx_crypto_symbols() -> None:
    universe = yfinance_intraday_universe()
    by_symbol = {item.symbol: item for item in universe}

    assert by_symbol["EUR/USD"].yahoo_symbol == "EURUSD=X"
    assert by_symbol["USD/JPY"].yahoo_symbol == "JPY=X"
    assert by_symbol["USD/CNH"].yahoo_symbol == "CNH=X"
    assert by_symbol["BTC/USD"].yahoo_symbol == "BTC-USD"


def test_normalize_yfinance_daily_handles_multi_ticker_frame() -> None:
    columns = pd.MultiIndex.from_product(
        [["AAPL", "MSFT"], ["Open", "High", "Low", "Close", "Adj Close", "Volume"]]
    )
    frame = pd.DataFrame(
        [
            [
                100.0,
                101.0,
                99.0,
                100.5,
                95.25,
                1000,
                200.0,
                202.0,
                198.0,
                201.0,
                190.95,
                2000,
            ],
        ],
        index=pd.DatetimeIndex(["2026-07-01"], name="Date"),
        columns=columns,
    )

    normalized = normalize_yfinance_daily(
        frame,
        [
            {"symbol": "AAPL", "instrument_key": "YF|AAPL", "yahoo_symbol": "AAPL"},
            {"symbol": "MSFT", "instrument_key": "YF|MSFT", "yahoo_symbol": "MSFT"},
        ],
    )

    assert normalized["InstrumentKey"].tolist() == ["YF|AAPL", "YF|MSFT"]
    assert normalized["Date"].tolist() == [date(2026, 7, 1), date(2026, 7, 1)]
    assert normalized["Close"].tolist() == [100.5, 201.0]
    assert normalized["AdjClose"].tolist() == [95.25, 190.95]
    assert normalized["Volume"].tolist() == [1000, 2000]


def test_normalize_yfinance_daily_handles_single_ticker_frame() -> None:
    frame = pd.DataFrame(
        [{"Open": 10.0, "High": 11.0, "Low": 9.0, "Close": 10.5, "Volume": 500}],
        index=pd.DatetimeIndex(["2026-07-01"], name="Date"),
    )

    normalized = normalize_yfinance_daily(
        frame,
        [{"symbol": "SHOP", "instrument_key": "YF|SHOP.TO", "yahoo_symbol": "SHOP.TO"}],
    )

    assert len(normalized) == 1
    assert normalized.iloc[0]["Symbol"] == "SHOP"
    assert normalized.iloc[0]["TradingSymbol"] == "SHOP.TO"
    assert normalized.iloc[0]["Source"] == "yfinance"


def test_normalize_yfinance_intraday_handles_multi_ticker_frame() -> None:
    columns = pd.MultiIndex.from_product(
        [["EURUSD=X", "BTC-USD"], ["Open", "High", "Low", "Close", "Volume"]]
    )
    frame = pd.DataFrame(
        [
            [1.10, 1.11, 1.09, 1.105, 0, 60000.0, 60100.0, 59900.0, 60050.0, 42],
        ],
        index=pd.DatetimeIndex(["2026-07-01T00:00:00Z"], name="Datetime"),
        columns=columns,
    )
    instruments = [
        item for item in yfinance_intraday_universe() if item.symbol in {"EUR/USD", "BTC/USD"}
    ]

    normalized = normalize_yfinance_intraday(frame, instruments)

    assert normalized["InstrumentKey"].tolist() == [
        "YF_INTRADAY|BTC-USD",
        "YF_INTRADAY|EURUSD=X",
    ]
    assert normalized["Symbol"].tolist() == ["BTC/USD", "EUR/USD"]
    assert normalized["Interval"].tolist() == ["5m", "5m"]
    assert normalized["Source"].tolist() == ["yfinance", "yfinance"]
