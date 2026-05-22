from datetime import date, timedelta

import pandas as pd

from trade_research.features import RangeFeatureBuilder
from trade_research.screeners import IntradayRangeScreener


def _rows(ticker: str) -> list[dict]:
    start = date(2026, 1, 1)
    rows = []
    for i in range(5):
        rows.append(
            {
                "Date": pd.Timestamp(start + timedelta(days=i)),
                "Ticker": ticker,
                "Open": 100.0,
                "High": 101.5 if i < 3 else 100.5,
                "Low": 98.5 if i < 3 else 99.5,
                "Close": 100.5,
                "Volume": 100_000,
            }
        )
    return rows


def test_range_features_match_notebook_metric_names() -> None:
    df = pd.DataFrame(_rows("ABC.NS"))
    builder = RangeFeatureBuilder(
        windows={"5D": 5},
        thresholds=[0.01, 0.02],
        min_median_dollar_volume=0,
    )

    features = builder.build(df)

    assert features.loc[0, "Ticker"] == "ABC.NS"
    assert features.loc[0, "d5Up0100"] == 3
    assert features.loc[0, "d5Dn0100"] == 3
    assert features.loc[0, "d5ClUp0200"] == 0
    assert features.loc[0, "d5ClDn0200"] == 0


def test_intraday_range_screener_matches_expected_signal() -> None:
    df = pd.DataFrame(_rows("ABC.NS"))
    builder = RangeFeatureBuilder(min_median_dollar_volume=0)
    features = builder.build(df)

    screened = IntradayRangeScreener().run(features)

    assert screened["Ticker"].tolist() == ["ABC.NS"]
