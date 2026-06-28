from __future__ import annotations

import json
from datetime import date, timedelta
from pathlib import Path

import pandas as pd

from trade_research.modeling.walk_forward import (
    WalkForwardManifestConfig,
    build_walk_forward_manifest,
)
from trade_research.pipelines.walk_forward import run_walk_forward_folds_v1_pipeline

FEATURE_COLUMNS = ["ret_1d", "sma_10"]


def test_walk_forward_manifest_records_valid_folds_and_leakage_checks() -> None:
    build = build_walk_forward_manifest(
        _dataset(days=12, symbols=("AAA", "BBB")),
        FEATURE_COLUMNS,
        config=WalkForwardManifestConfig(min_train_days=6, validation_days=3),
    )

    assert build.summary["fold_count"] == 3
    assert build.summary["leakage_checks_passed"] is True
    first = build.folds.iloc[0]
    assert first["prediction_date"] == date(2026, 1, 10)
    assert first["train_end_date"] == date(2026, 1, 6)
    assert first["validation_end_date"] == date(2026, 1, 9)
    assert bool(first["leakage_check_train_before_prediction"]) is True
    assert bool(first["leakage_check_validation_before_prediction"]) is True


def test_walk_forward_manifest_respects_date_filters_and_max_folds() -> None:
    build = build_walk_forward_manifest(
        _dataset(days=15, symbols=("AAA", "BBB")),
        FEATURE_COLUMNS,
        config=WalkForwardManifestConfig(
            min_train_days=6,
            validation_days=3,
            start_date=date(2026, 1, 12),
            max_folds=2,
        ),
    )

    assert build.folds["prediction_date"].tolist() == [
        date(2026, 1, 12),
        date(2026, 1, 13),
    ]


def test_walk_forward_manifest_can_return_no_folds_when_history_is_short() -> None:
    build = build_walk_forward_manifest(
        _dataset(days=5, symbols=("AAA", "BBB")),
        FEATURE_COLUMNS,
        config=WalkForwardManifestConfig(min_train_days=6, validation_days=3),
    )

    assert build.folds.empty
    assert build.summary["fold_count"] == 0
    assert build.summary["skipped_candidate_count"] > 0


def test_walk_forward_pipeline_writes_artifacts(tmp_path: Path, monkeypatch) -> None:
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

    result = run_walk_forward_folds_v1_pipeline(
        config=WalkForwardManifestConfig(min_train_days=6, validation_days=3),
    )

    assert result.status == "pass"
    assert result.metrics["fold_count"] == 3
    assert result.artifacts["folds"].exists()
    assert result.artifacts["summary"].exists()
    assert data_dir in result.artifacts["folds"].parents


def _dataset(days: int, symbols: tuple[str, ...]) -> pd.DataFrame:
    rows = []
    for offset in range(days):
        current_date = date(2026, 1, 1) + timedelta(days=offset)
        for symbol_index, symbol in enumerate(symbols):
            is_final = offset == days - 1
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
                    "ret_1d": 0.01 * (offset + 1),
                    "sma_10": 100.0 + symbol_index,
                    "forward_ret_1d": pd.NA if is_final else (symbol_index + 1) / 100,
                    "next_day_positive": pd.NA if is_final else True,
                    "next_day_top_decile": pd.NA if is_final else symbol_index == len(symbols) - 1,
                    "next_day_bottom_decile": pd.NA if is_final else symbol_index == 0,
                    "daily_forward_ret_1d_rank": pd.NA
                    if is_final
                    else len(symbols) - symbol_index,
                }
            )
    return pd.DataFrame(rows)
