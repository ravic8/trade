from __future__ import annotations

from datetime import date, timedelta
from pathlib import Path

import pandas as pd
import pytest

from trade_research.modeling.ml_dataset_v1 import (
    ML_DATASET_VERSION_V1,
    STATIC_FULL_HISTORY_COVERAGE_POLICY,
    build_ml_dataset_v1,
)
from trade_research.pipelines.ml_dataset import run_ml_dataset_v1_pipeline


def test_ml_dataset_filters_to_full_history_coverage_and_records_exclusions() -> None:
    build = build_ml_dataset_v1(
        ohlcv=_ohlcv(),
        features=_features(),
        targets=_targets(),
        stock_coverage=_coverage(),
    )

    assert set(build.dataset["symbol"]) == {"AAA"}
    assert build.summary["excluded_symbol_count"] == 1
    assert build.exclusions.iloc[0]["symbol"] == "BBB"
    assert build.exclusions.iloc[0]["exclusion_reason"] == "not_full_history_coverage"
    assert build.summary["coverage_policy"] == STATIC_FULL_HISTORY_COVERAGE_POLICY


def test_ml_dataset_creates_next_day_labels_and_trainability_flags() -> None:
    build = build_ml_dataset_v1(
        ohlcv=_ohlcv(days=4),
        features=_features(days=4),
        targets=_targets(days=4),
        stock_coverage=_coverage(),
    )

    first = build.dataset[build.dataset["date"].eq(date(2026, 1, 1))].iloc[0]
    last = build.dataset[build.dataset["date"].eq(date(2026, 1, 4))].iloc[0]

    assert first["ml_dataset_version"] == ML_DATASET_VERSION_V1
    assert bool(first["is_trainable"]) is True
    assert bool(first["next_day_positive"]) is True
    assert first["daily_forward_ret_1d_rank"] == pytest.approx(1.0)
    assert bool(last["is_trainable"]) is False
    assert "target_null" in last["exclusion_reasons"]


def test_ml_dataset_feature_list_excludes_targets_and_identifiers() -> None:
    build = build_ml_dataset_v1(
        ohlcv=_ohlcv(),
        features=_features(),
        targets=_targets(),
        stock_coverage=_coverage(),
    )

    assert build.feature_columns == ["ret_1d", "sma_10"]
    assert "forward_ret_1d" not in build.feature_columns
    assert "instrument_key" not in build.feature_columns
    assert build.leakage_checks["passed"] is True


def test_ml_dataset_rejects_duplicate_keys() -> None:
    duplicate_ohlcv = pd.concat([_ohlcv(), _ohlcv().iloc[[0]]], ignore_index=True)

    with pytest.raises(ValueError, match="Duplicate instrument/date keys"):
        build_ml_dataset_v1(
            ohlcv=duplicate_ohlcv,
            features=_features(),
            targets=_targets(),
            stock_coverage=_coverage(),
        )


def test_ml_dataset_pipeline_writes_artifacts(tmp_path: Path, monkeypatch) -> None:
    data_dir = tmp_path / "data"
    _write_pipeline_inputs(data_dir)
    monkeypatch.setenv("DATA_DIR", str(data_dir))

    result = run_ml_dataset_v1_pipeline()

    assert result.status == "pass"
    assert result.metrics["trainable_row_count"] == 2
    for path in result.artifacts.values():
        assert path.exists()
        assert data_dir in path.parents


def _ohlcv(days: int = 3) -> pd.DataFrame:
    rows = []
    for symbol, key in [("AAA", "NSE_EQ|AAA"), ("BBB", "NSE_EQ|BBB")]:
        for offset in range(days):
            rows.append(
                {
                    "instrument_key": key,
                    "symbol": symbol,
                    "exchange": "NSE",
                    "source": "upstox",
                    "date": date(2026, 1, 1) + timedelta(days=offset),
                    "open": 100 + offset,
                    "high": 101 + offset,
                    "low": 99 + offset,
                    "close": 100 + offset,
                    "volume": 100_000,
                    "open_interest": 0,
                    "quality_status": "passed",
                }
            )
    return pd.DataFrame(rows)


def _features(days: int = 3) -> pd.DataFrame:
    frame = _ohlcv(days)
    frame["feature_version"] = "daily_v1_ohlcv_technical_v1_0"
    frame["quality_status"] = "passed"
    frame["ret_1d"] = 0.01
    frame["sma_10"] = 100.0
    return frame[
        [
            "instrument_key",
            "symbol",
            "exchange",
            "source",
            "date",
            "feature_version",
            "quality_status",
            "ret_1d",
            "sma_10",
        ]
    ]


def _targets(days: int = 3) -> pd.DataFrame:
    frame = _ohlcv(days)
    frame["target_version"] = "daily_v1_forward_returns_v1_0"
    frame["quality_status"] = "passed"
    frame["forward_ret_1d"] = 0.02
    max_date = frame.groupby("instrument_key")["date"].transform("max")
    frame.loc[frame["date"].eq(max_date), "forward_ret_1d"] = pd.NA
    return frame[
        [
            "instrument_key",
            "symbol",
            "exchange",
            "source",
            "date",
            "target_version",
            "quality_status",
            "forward_ret_1d",
        ]
    ]


def _coverage() -> pd.DataFrame:
    return pd.DataFrame(
        [
            {
                "instrument_key": "NSE_EQ|AAA",
                "symbol": "AAA",
                "coverage_pct": 1.0,
                "coverage_status": "pass",
                "observed_date_count": 3,
                "expected_date_count": 3,
            },
            {
                "instrument_key": "NSE_EQ|BBB",
                "symbol": "BBB",
                "coverage_pct": 2 / 3,
                "coverage_status": "fail",
                "observed_date_count": 2,
                "expected_date_count": 3,
            },
        ]
    )


def _write_pipeline_inputs(data_dir: Path) -> None:
    paths = {
        "processed/validated/ohlcv_daily_validated.parquet": _ohlcv(),
        "processed/features/daily_v1_ohlcv_technical.parquet": _features(),
        "processed/targets/daily_v1_forward_returns.parquet": _targets(),
        "processed/validation/daily_pipeline_stock_coverage.parquet": _coverage(),
    }
    for name, frame in paths.items():
        path = data_dir / name
        path.parent.mkdir(parents=True, exist_ok=True)
        frame.to_parquet(path, index=False)
