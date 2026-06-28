from __future__ import annotations

import json
from datetime import date, timedelta
from pathlib import Path

import pandas as pd

from trade_research.modeling.baselines import BaselineRunConfig, run_baseline_predictions
from trade_research.pipelines.baselines import run_baseline_predictions_v1_pipeline

FEATURE_COLUMNS = ["ret_1d", "ret_5d", "ret_20d", "volatility_20d"]


def test_baseline_predictions_generate_scores_and_metrics() -> None:
    result = run_baseline_predictions(
        _dataset(days=14, symbols=("AAA", "BBB", "CCC")),
        FEATURE_COLUMNS,
        config=BaselineRunConfig(min_train_days=6, validation_days=3),
    )

    assert result.metrics["prediction_row_count"] > 0
    assert result.metrics["model_count"] == 5
    assert set(result.predictions["model_id"]) == {
        "mean_return",
        "momentum_1d",
        "momentum_5d",
        "reversal_1d",
        "volatility_adjusted_momentum_20d",
    }
    assert result.predictions["rank"].min() == 1
    assert "top_10_average_return" in result.metrics["models"][0]
    assert "# Baseline Predictions v1" in result.summary_md


def test_momentum_baseline_ranks_highest_recent_return_first() -> None:
    result = run_baseline_predictions(
        _dataset(days=11, symbols=("AAA", "BBB", "CCC")),
        FEATURE_COLUMNS,
        config=BaselineRunConfig(min_train_days=6, validation_days=3, max_folds=1),
    )
    momentum = result.predictions[result.predictions["model_id"].eq("momentum_1d")]
    top = momentum.sort_values("rank").iloc[0]

    assert top["symbol"] == "CCC"


def test_baseline_pipeline_writes_artifacts(tmp_path: Path, monkeypatch) -> None:
    data_dir = tmp_path / "data"
    ml_dir = data_dir / "processed/ml"
    ml_dir.mkdir(parents=True)
    _dataset(days=12, symbols=("AAA", "BBB")).to_parquet(
        ml_dir / "ml_dataset_v1.parquet",
        index=False,
    )
    (ml_dir / "ml_dataset_v1_feature_columns.json").write_text(
        json.dumps(FEATURE_COLUMNS) + "\n"
    )
    monkeypatch.setenv("DATA_DIR", str(data_dir))

    result = run_baseline_predictions_v1_pipeline(
        config=BaselineRunConfig(min_train_days=6, validation_days=3),
    )

    assert result.status == "pass"
    assert result.metrics["prediction_row_count"] > 0
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
                    "next_day_positive": pd.NA if is_final else True,
                    "next_day_top_decile": pd.NA if is_final else symbol_index == len(symbols) - 1,
                    "next_day_bottom_decile": pd.NA if is_final else symbol_index == 0,
                    "daily_forward_ret_1d_rank": pd.NA
                    if is_final
                    else len(symbols) - symbol_index,
                }
            )
    return pd.DataFrame(rows)
