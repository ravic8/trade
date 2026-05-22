import pandas as pd

from trade_research.data.yahoo import YahooFinanceMarketDataProvider


def test_to_long_sorts_hourly_datetime_frames() -> None:
    index = pd.to_datetime(
        ["2026-01-01 10:00:00+00:00", "2026-01-01 09:00:00+00:00"],
        utc=True,
    )
    wide = pd.DataFrame(
        {
            ("ABC.NS", "Open"): [101.0, 100.0],
            ("ABC.NS", "High"): [102.0, 101.0],
            ("ABC.NS", "Low"): [100.5, 99.5],
            ("ABC.NS", "Close"): [101.5, 100.5],
            ("ABC.NS", "Volume"): [2_000, 1_000],
        },
        index=pd.DatetimeIndex(index, name="Datetime"),
    )

    out = YahooFinanceMarketDataProvider._to_long(wide, ["ABC.NS"])

    assert out["Ticker"].tolist() == ["ABC.NS", "ABC.NS"]
    assert out["Datetime"].tolist() == sorted(out["Datetime"].tolist())


def test_to_long_skips_failed_ticker_blocks_with_unnamed_datetime_index() -> None:
    index = pd.to_datetime(
        ["2026-01-01 09:00:00+00:00", "2026-01-01 10:00:00+00:00"],
        utc=True,
    )
    wide = pd.DataFrame(
        {
            ("AAV.TO", "Open"): [10.0, 10.1],
            ("AAV.TO", "High"): [10.2, 10.3],
            ("AAV.TO", "Low"): [9.9, 10.0],
            ("AAV.TO", "Close"): [10.1, 10.2],
            ("AAV.TO", "Volume"): [1000, 1200],
            ("AW.UN.TO", "Open"): [None, None],
            ("AW.UN.TO", "High"): [None, None],
            ("AW.UN.TO", "Low"): [None, None],
            ("AW.UN.TO", "Close"): [None, None],
            ("AW.UN.TO", "Volume"): [None, None],
        },
        index=pd.DatetimeIndex(index),
    )

    out = YahooFinanceMarketDataProvider._to_long(wide, ["AAV.TO", "AW.UN.TO"])

    assert out["Ticker"].tolist() == ["AAV.TO", "AAV.TO"]
    assert "Datetime" in out.columns
