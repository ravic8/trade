from datetime import date, timedelta

import pandas as pd
import pytest

from trade_research.features import (
    FEATURE_VERSION_V1_0,
    DailyTechnicalFeatureBuilder,
    audit_daily_features,
)


def _daily_rows(days: int = 220, close_start: float = 100.0) -> pd.DataFrame:
    rows = []
    for offset in range(days):
        close = close_start + offset
        rows.append(
            {
                "Date": date(2025, 1, 1) + timedelta(days=offset),
                "Open": close - 0.5,
                "High": close + 1.0,
                "Low": close - 1.0,
                "Close": close,
                "Volume": 100_000 + offset,
                "OpenInterest": 0,
                "InstrumentKey": "NSE_EQ|TEST",
                "Symbol": "TEST",
                "Source": "upstox",
            }
        )
    return pd.DataFrame(rows)


def test_daily_v1_returns_use_past_closes_only() -> None:
    base = _daily_rows(days=40)
    changed_future = base.copy()
    changed_future.loc[30:, "Close"] = changed_future.loc[30:, "Close"] * 10
    changed_future.loc[30:, "Open"] = changed_future.loc[30:, "Close"] - 0.5
    changed_future.loc[30:, "High"] = changed_future.loc[30:, "Close"] + 1.0
    changed_future.loc[30:, "Low"] = changed_future.loc[30:, "Close"] - 1.0

    features = DailyTechnicalFeatureBuilder().build(base)
    changed_features = DailyTechnicalFeatureBuilder().build(changed_future)

    row_20 = features[features["date"].eq(date(2025, 1, 21))].iloc[0]
    changed_row_20 = changed_features[changed_features["date"].eq(date(2025, 1, 21))].iloc[0]

    assert row_20["ret_5d"] == pytest.approx(120 / 115 - 1)
    assert row_20["ret_20d"] == pytest.approx(120 / 100 - 1)
    assert changed_row_20["ret_20d"] == row_20["ret_20d"]


def test_daily_v1_warmup_and_audit_status() -> None:
    features = DailyTechnicalFeatureBuilder().build(_daily_rows(days=220))

    first = features.iloc[0]
    row_200 = features.iloc[199]

    assert pd.isna(first["ret_1d"])
    assert pd.isna(first["sma_10"])
    assert pd.isna(first["ema_10"])
    assert pd.isna(first["true_range"])
    assert first["quality_status"] == "warning"
    assert row_200["sma_200"] == pytest.approx(sum(range(100, 300)) / 200)
    assert row_200["quality_status"] == "passed"

    audit, summary = audit_daily_features(features)
    assert summary.feature_version == FEATURE_VERSION_V1_0
    assert summary.row_count == 220
    assert summary.warning_rows > 0
    assert summary.passed_rows > 0
    assert audit[audit["feature"].eq("sma_200")]["null_count"].iloc[0] == 199


def test_daily_v1_true_range_includes_gap_from_previous_close() -> None:
    frame = pd.DataFrame(
        [
            {
                "Date": date(2025, 1, 1),
                "Open": 99.0,
                "High": 101.0,
                "Low": 98.0,
                "Close": 100.0,
                "Volume": 100_000,
                "OpenInterest": 0,
                "InstrumentKey": "NSE_EQ|TEST",
                "Symbol": "TEST",
                "Source": "upstox",
            },
            {
                "Date": date(2025, 1, 2),
                "Open": 105.0,
                "High": 110.0,
                "Low": 104.0,
                "Close": 108.0,
                "Volume": 100_000,
                "OpenInterest": 0,
                "InstrumentKey": "NSE_EQ|TEST",
                "Symbol": "TEST",
                "Source": "upstox",
            },
        ]
    )

    features = DailyTechnicalFeatureBuilder().build(frame)

    assert pd.isna(features.loc[0, "true_range"])
    assert features.loc[1, "true_range"] == pytest.approx(10.0)


def test_daily_v1_duplicate_input_rows_fail() -> None:
    frame = pd.concat([_daily_rows(days=2), _daily_rows(days=1)], ignore_index=True)

    with pytest.raises(ValueError, match="duplicate instrument/date"):
        DailyTechnicalFeatureBuilder().build(frame)


def test_daily_v1_invalid_ohlcv_rows_fail() -> None:
    frame = _daily_rows(days=2)
    frame.loc[1, "High"] = 95.0

    with pytest.raises(ValueError, match="invalid rows"):
        DailyTechnicalFeatureBuilder().build(frame)
