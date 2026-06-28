from __future__ import annotations

from datetime import date, timedelta

import pandas as pd
import pytest

from trade_research.modeling.datasets import (
    fold_views,
    make_classification_view,
    make_downside_risk_view,
    make_prediction_view,
    make_ranking_view,
    make_regression_view,
    make_walk_forward_fold,
    trainable_rows,
)

FEATURE_COLUMNS = ["ret_1d", "sma_10"]


def test_regression_classification_risk_and_ranking_views() -> None:
    dataset = _dataset(days=8, symbols=("AAA", "BBB", "CCC"))

    regression = make_regression_view(dataset, FEATURE_COLUMNS)
    classification = make_classification_view(dataset, FEATURE_COLUMNS)
    risk = make_downside_risk_view(dataset, FEATURE_COLUMNS)
    ranking = make_ranking_view(dataset, FEATURE_COLUMNS)

    assert regression.task == "regression"
    assert regression.X.columns.tolist() == FEATURE_COLUMNS
    assert regression.y.name == "forward_ret_1d"
    assert "symbol" in regression.metadata.columns

    assert classification.task == "classification"
    assert set(classification.y.dropna().unique()).issubset({0, 1})

    assert risk.task == "risk"
    assert risk.target_column == "next_day_bottom_decile"

    assert ranking.task == "ranking"
    assert ranking.groups == [3, 3, 3, 3, 3, 3, 3]
    assert len(ranking.X) == sum(ranking.groups)


def test_prediction_view_does_not_require_known_target() -> None:
    dataset = _dataset(days=5, symbols=("AAA", "BBB"))
    prediction_day = dataset[dataset["date"].eq(date(2026, 1, 5))]

    view = make_prediction_view(prediction_day, FEATURE_COLUMNS)

    assert view.y is None
    assert len(view.X) == 2
    assert view.metadata["date"].unique().tolist() == [date(2026, 1, 5)]


def test_walk_forward_fold_uses_only_labeled_dates_before_prediction_date() -> None:
    dataset = _dataset(days=12, symbols=("AAA", "BBB"))

    fold = make_walk_forward_fold(
        dataset,
        FEATURE_COLUMNS,
        prediction_date=date(2026, 1, 11),
        min_train_days=6,
        validation_days=3,
    )

    assert fold.train_start_date == date(2026, 1, 1)
    assert fold.train_end_date == date(2026, 1, 7)
    assert fold.validation_start_date == date(2026, 1, 8)
    assert fold.validation_end_date == date(2026, 1, 10)
    assert fold.prediction_date == date(2026, 1, 11)
    assert fold.train["date"].max() < fold.prediction_date
    assert fold.validation["date"].max() < fold.prediction_date
    assert fold.prediction["date"].unique().tolist() == [date(2026, 1, 11)]


def test_fold_views_return_task_specific_frames() -> None:
    dataset = _dataset(days=12, symbols=("AAA", "BBB", "CCC"))
    fold = make_walk_forward_fold(
        dataset,
        FEATURE_COLUMNS,
        prediction_date=date(2026, 1, 11),
        min_train_days=6,
        validation_days=3,
    )

    train, validation, prediction = fold_views(
        fold,
        FEATURE_COLUMNS,
        task="ranking",
    )

    assert train.task == "ranking"
    assert validation.task == "ranking"
    assert prediction.y is None
    assert train.groups == [3, 3, 3, 3, 3, 3, 3]
    assert validation.groups == [3, 3, 3]


def test_walk_forward_fold_rejects_insufficient_history() -> None:
    with pytest.raises(ValueError, match="Not enough labeled history"):
        make_walk_forward_fold(
            _dataset(days=4, symbols=("AAA", "BBB")),
            FEATURE_COLUMNS,
            prediction_date=date(2026, 1, 4),
            min_train_days=3,
            validation_days=1,
        )


def test_trainable_rows_exclude_non_trainable_records() -> None:
    dataset = _dataset(days=3, symbols=("AAA",))

    assert len(trainable_rows(dataset)) == 2


def _dataset(days: int, symbols: tuple[str, ...]) -> pd.DataFrame:
    rows = []
    for offset in range(days):
        current_date = date(2026, 1, 1) + timedelta(days=offset)
        for symbol_index, symbol in enumerate(symbols):
            forward_ret = (symbol_index + 1) / 100
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
                    "forward_ret_1d": pd.NA if is_final else forward_ret,
                    "next_day_positive": pd.NA if is_final else forward_ret > 0,
                    "next_day_top_decile": pd.NA if is_final else symbol_index == len(symbols) - 1,
                    "next_day_bottom_decile": pd.NA if is_final else symbol_index == 0,
                    "daily_forward_ret_1d_rank": pd.NA
                    if is_final
                    else len(symbols) - symbol_index,
                }
            )
    return pd.DataFrame(rows)
