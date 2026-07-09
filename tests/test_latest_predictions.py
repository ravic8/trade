from __future__ import annotations

from datetime import date, timedelta
from pathlib import Path

import pandas as pd

from trade_research.modeling.latest_predictions import (
    LatestPredictionConfig,
    run_latest_predictions,
)
from trade_research.pipelines.latest_predictions import run_latest_predictions_v1_pipeline


def test_latest_predictions_scores_latest_feature_complete_date() -> None:
    dataset = _dataset(days=8, symbols=("AAA", "BBB", "CCC"))
    feature_columns = ["ret_1d", "ret_5d", "ret_20d", "volatility_20d"]

    result = run_latest_predictions(
        dataset,
        feature_columns,
        config=LatestPredictionConfig(
            min_train_days=3,
            validation_days=2,
            include_lightgbm=False,
            top_n_values=(2,),
        ),
    )

    assert not result.predictions.empty
    assert set(result.predictions["run_id"]) == {"baselines"}
    assert result.summary["prediction_date"] == "2026-01-08"
    assert result.summary["target_session_date"] == "2026-01-09"
    assert result.candidates["target_session_date"] == "2026-01-09"
    assert result.candidates["runs"][0]["models"]


def test_latest_predictions_pipeline_writes_artifacts(tmp_path: Path) -> None:
    dataset = _dataset(days=8, symbols=("AAA", "BBB", "CCC"))
    feature_columns = ["ret_1d", "ret_5d", "ret_20d", "volatility_20d"]
    dataset_path = tmp_path / "dataset.parquet"
    feature_columns_path = tmp_path / "features.json"
    output_dir = tmp_path / "latest"
    dataset.to_parquet(dataset_path, index=False)
    feature_columns_path.write_text(pd.Series(feature_columns).to_json(orient="values"))

    result = run_latest_predictions_v1_pipeline(
        dataset_path=dataset_path,
        feature_columns_path=feature_columns_path,
        output_dir=output_dir,
        config=LatestPredictionConfig(
            min_train_days=3,
            validation_days=2,
            include_lightgbm=False,
            top_n_values=(2,),
        ),
    )

    assert result.status == "pass"
    assert result.metrics["prediction_date"] == "2026-01-08"
    assert result.metrics["target_session_date"] == "2026-01-09"
    for path in result.artifacts.values():
        assert path.exists()
        assert output_dir in path.parents


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
