from __future__ import annotations

import json
from datetime import date, timedelta
from pathlib import Path

import pandas as pd
import pytest

from trade_research.modeling.lightgbm_models import LightGBMRunConfig, run_lightgbm_predictions
from trade_research.pipelines.lightgbm_models import run_lightgbm_predictions_v1_pipeline

pytest.importorskip("lightgbm")

FEATURE_COLUMNS = ["ret_1d", "ret_5d", "ret_20d", "volatility_20d"]


def test_lightgbm_predictions_generate_model_and_blend_outputs() -> None:
    result = run_lightgbm_predictions(
        _dataset(days=12, symbols=("AAA", "BBB", "CCC", "DDD")),
        FEATURE_COLUMNS,
        config=LightGBMRunConfig(
            min_train_days=6,
            validation_days=3,
            max_folds=1,
            n_estimators=5,
            min_child_samples=1,
        ),
    )

    assert result.metrics["prediction_row_count"] == 24
    assert result.metrics["model_count"] == 6
    assert set(result.predictions["model_id"]) == {
        "lgbm_downside_classifier",
        "lgbm_downside_classifier_momentum_blend",
        "lgbm_regressor",
        "lgbm_regressor_momentum_blend",
        "lgbm_upside_classifier",
        "lgbm_upside_classifier_momentum_blend",
    }
    assert result.predictions["rank"].min() == 1
    assert "# LightGBM Predictions v1" in result.summary_md


def test_lightgbm_pipeline_writes_artifacts(tmp_path: Path, monkeypatch) -> None:
    data_dir = tmp_path / "data"
    ml_dir = data_dir / "processed/ml"
    ml_dir.mkdir(parents=True)
    _dataset(days=12, symbols=("AAA", "BBB", "CCC", "DDD")).to_parquet(
        ml_dir / "ml_dataset_v1.parquet",
        index=False,
    )
    (ml_dir / "ml_dataset_v1_feature_columns.json").write_text(
        json.dumps(FEATURE_COLUMNS) + "\n"
    )
    monkeypatch.setenv("DATA_DIR", str(data_dir))

    result = run_lightgbm_predictions_v1_pipeline(
        config=LightGBMRunConfig(
            min_train_days=6,
            validation_days=3,
            max_folds=1,
            n_estimators=5,
            min_child_samples=1,
        ),
    )

    assert result.status == "pass"
    assert result.metrics["prediction_row_count"] == 24
    for path in result.artifacts.values():
        assert path.exists()
        assert data_dir in path.parents


def _dataset(days: int, symbols: tuple[str, ...]) -> pd.DataFrame:
    rows = []
    for offset in range(days):
        current_date = date(2026, 1, 1) + timedelta(days=offset)
        for symbol_index, symbol in enumerate(symbols):
            is_final = offset == days - 1
            signal = symbol_index + 1
            rows.append(
                {
                    "instrument_key": f"NSE_EQ|{symbol}",
                    "symbol": symbol,
                    "exchange": "NSE",
                    "source": "upstox",
                    "date": current_date,
                    "ml_dataset_version": "ml_dataset_v1_0",
                    "feature_version": "features_v1",
                    "target_version": "targets_v1",
                    "coverage_policy": "static_full_history_100pct_coverage",
                    "coverage_pct_full_history": 1.0,
                    "split": "train_seed",
                    "is_trainable": not is_final,
                    "exclusion_reasons": "target_null" if is_final else "",
                    "ret_1d": signal / 100,
                    "ret_5d": signal / 50,
                    "ret_20d": signal / 25,
                    "volatility_20d": 0.10,
                    "forward_ret_1d": pd.NA if is_final else signal / 100,
                    "next_day_positive": pd.NA if is_final else symbol_index >= 2,
                    "next_day_top_decile": pd.NA if is_final else symbol_index >= 2,
                    "next_day_bottom_decile": pd.NA if is_final else symbol_index <= 1,
                    "daily_forward_ret_1d_rank": pd.NA
                    if is_final
                    else len(symbols) - symbol_index,
                }
            )
    return pd.DataFrame(rows)
