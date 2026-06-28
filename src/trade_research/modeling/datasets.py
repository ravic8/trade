from __future__ import annotations

import json
from dataclasses import dataclass
from datetime import date
from pathlib import Path
from typing import Literal

import numpy as np
import pandas as pd

from trade_research.modeling.ml_dataset_v1 import TARGET_COLUMN_V1

DatasetTask = Literal["regression", "ranking", "classification", "risk"]

DEFAULT_ML_DATASET_PATH = Path("data/processed/ml/ml_dataset_v1.parquet")
DEFAULT_FEATURE_COLUMNS_PATH = Path("data/processed/ml/ml_dataset_v1_feature_columns.json")

MODEL_METADATA_COLUMNS = [
    "instrument_key",
    "symbol",
    "exchange",
    "source",
    "date",
    "ml_dataset_version",
    "feature_version",
    "target_version",
    "coverage_policy",
    "coverage_pct_full_history",
    "split",
]


@dataclass(frozen=True)
class ModelDatasetView:
    task: DatasetTask
    X: pd.DataFrame
    y: pd.Series | None
    metadata: pd.DataFrame
    feature_columns: list[str]
    target_column: str | None = None
    groups: list[int] | None = None


@dataclass(frozen=True)
class WalkForwardFold:
    prediction_date: date
    train: pd.DataFrame
    validation: pd.DataFrame
    prediction: pd.DataFrame
    train_start_date: date
    train_end_date: date
    validation_start_date: date
    validation_end_date: date
    min_train_days: int
    validation_days: int


def load_ml_dataset_v1(
    dataset_path: Path = DEFAULT_ML_DATASET_PATH,
    feature_columns_path: Path = DEFAULT_FEATURE_COLUMNS_PATH,
) -> tuple[pd.DataFrame, list[str]]:
    dataset = pd.read_parquet(dataset_path)
    dataset["date"] = pd.to_datetime(dataset["date"], errors="coerce").dt.date
    feature_columns = json.loads(feature_columns_path.read_text())
    return dataset, list(feature_columns)


def trainable_rows(dataset: pd.DataFrame) -> pd.DataFrame:
    return (
        dataset[dataset["is_trainable"].astype(bool)]
        .sort_values(["date", "instrument_key"])
        .reset_index(drop=True)
    )


def make_regression_view(
    dataset: pd.DataFrame,
    feature_columns: list[str],
    target_column: str = TARGET_COLUMN_V1,
) -> ModelDatasetView:
    frame = trainable_rows(dataset)
    return _model_view(
        task="regression",
        frame=frame,
        feature_columns=feature_columns,
        target_column=target_column,
    )


def make_classification_view(
    dataset: pd.DataFrame,
    feature_columns: list[str],
    label_column: str = "next_day_top_decile",
) -> ModelDatasetView:
    frame = trainable_rows(dataset)
    view = _model_view(
        task="classification",
        frame=frame,
        feature_columns=feature_columns,
        target_column=label_column,
    )
    y = view.y.astype("boolean").astype("Int64") if view.y is not None else None
    return ModelDatasetView(
        task=view.task,
        X=view.X,
        y=y,
        metadata=view.metadata,
        feature_columns=view.feature_columns,
        target_column=view.target_column,
        groups=view.groups,
    )


def make_downside_risk_view(
    dataset: pd.DataFrame,
    feature_columns: list[str],
    label_column: str = "next_day_bottom_decile",
) -> ModelDatasetView:
    view = make_classification_view(
        dataset,
        feature_columns=feature_columns,
        label_column=label_column,
    )
    return ModelDatasetView(
        task="risk",
        X=view.X,
        y=view.y,
        metadata=view.metadata,
        feature_columns=view.feature_columns,
        target_column=view.target_column,
        groups=view.groups,
    )


def make_ranking_view(
    dataset: pd.DataFrame,
    feature_columns: list[str],
    target_column: str = TARGET_COLUMN_V1,
) -> ModelDatasetView:
    frame = trainable_rows(dataset).sort_values(["date", "instrument_key"]).reset_index(drop=True)
    view = _model_view(
        task="ranking",
        frame=frame,
        feature_columns=feature_columns,
        target_column=target_column,
    )
    groups = frame.groupby("date", sort=False).size().astype(int).tolist()
    return ModelDatasetView(
        task=view.task,
        X=view.X,
        y=view.y,
        metadata=view.metadata,
        feature_columns=view.feature_columns,
        target_column=view.target_column,
        groups=groups,
    )


def make_prediction_view(
    dataset: pd.DataFrame,
    feature_columns: list[str],
) -> ModelDatasetView:
    frame = _feature_complete_rows(dataset, feature_columns)
    return ModelDatasetView(
        task="regression",
        X=frame[feature_columns].copy(),
        y=None,
        metadata=_metadata(frame),
        feature_columns=feature_columns,
    )


def make_walk_forward_fold(
    dataset: pd.DataFrame,
    feature_columns: list[str],
    prediction_date: date | str,
    min_train_days: int = 300,
    validation_days: int = 60,
) -> WalkForwardFold:
    pred_date = _parse_date(prediction_date)
    prepared = dataset.copy()
    prepared["date"] = pd.to_datetime(prepared["date"], errors="coerce").dt.date
    labeled = trainable_rows(prepared)
    labeled_dates = sorted(
        date_value for date_value in labeled["date"].unique() if date_value < pred_date
    )
    if len(labeled_dates) < min_train_days + validation_days:
        raise ValueError(
            "Not enough labeled history before prediction_date for walk-forward fold: "
            f"{len(labeled_dates)} available, {min_train_days + validation_days} required."
        )

    validation_date_set = set(labeled_dates[-validation_days:])
    train_date_set = set(labeled_dates[:-validation_days])
    if len(train_date_set) < min_train_days:
        raise ValueError(
            "Not enough train dates before validation window: "
            f"{len(train_date_set)} available, {min_train_days} required."
        )

    train = labeled[labeled["date"].isin(train_date_set)].copy()
    validation = labeled[labeled["date"].isin(validation_date_set)].copy()
    prediction = prepared[prepared["date"].eq(pred_date)].copy()
    prediction = _feature_complete_rows(prediction, feature_columns)
    if prediction.empty:
        raise ValueError(f"No feature-complete prediction rows for {pred_date.isoformat()}.")

    train_dates = sorted(train["date"].unique())
    validation_dates = sorted(validation["date"].unique())
    return WalkForwardFold(
        prediction_date=pred_date,
        train=train.reset_index(drop=True),
        validation=validation.reset_index(drop=True),
        prediction=prediction.reset_index(drop=True),
        train_start_date=train_dates[0],
        train_end_date=train_dates[-1],
        validation_start_date=validation_dates[0],
        validation_end_date=validation_dates[-1],
        min_train_days=min_train_days,
        validation_days=validation_days,
    )


def fold_views(
    fold: WalkForwardFold,
    feature_columns: list[str],
    task: DatasetTask,
    target_column: str = TARGET_COLUMN_V1,
) -> tuple[ModelDatasetView, ModelDatasetView, ModelDatasetView]:
    if task == "regression":
        train = make_regression_view(fold.train, feature_columns, target_column=target_column)
        validation = make_regression_view(
            fold.validation,
            feature_columns,
            target_column=target_column,
        )
    elif task == "ranking":
        train = make_ranking_view(fold.train, feature_columns, target_column=target_column)
        validation = make_ranking_view(
            fold.validation,
            feature_columns,
            target_column=target_column,
        )
    elif task == "classification":
        train = make_classification_view(
            fold.train,
            feature_columns,
            label_column=target_column,
        )
        validation = make_classification_view(
            fold.validation,
            feature_columns,
            label_column=target_column,
        )
    elif task == "risk":
        train = make_downside_risk_view(
            fold.train,
            feature_columns,
            label_column=target_column,
        )
        validation = make_downside_risk_view(
            fold.validation,
            feature_columns,
            label_column=target_column,
        )
    else:
        raise ValueError(f"Unsupported task: {task}")
    prediction = make_prediction_view(fold.prediction, feature_columns)
    return train, validation, prediction


def _model_view(
    task: DatasetTask,
    frame: pd.DataFrame,
    feature_columns: list[str],
    target_column: str,
) -> ModelDatasetView:
    _validate_feature_columns(frame, feature_columns)
    if target_column not in frame.columns:
        raise ValueError(f"Missing target column: {target_column}")
    return ModelDatasetView(
        task=task,
        X=frame[feature_columns].copy(),
        y=frame[target_column].copy(),
        metadata=_metadata(frame),
        feature_columns=feature_columns,
        target_column=target_column,
    )


def _metadata(frame: pd.DataFrame) -> pd.DataFrame:
    columns = [column for column in MODEL_METADATA_COLUMNS if column in frame.columns]
    return frame[columns].copy()


def _feature_complete_rows(frame: pd.DataFrame, feature_columns: list[str]) -> pd.DataFrame:
    _validate_feature_columns(frame, feature_columns)
    feature_frame = frame[feature_columns]
    numeric = feature_frame.select_dtypes(include=[np.number])
    if len(numeric.columns) != len(feature_columns):
        missing_numeric = sorted(set(feature_columns).difference(numeric.columns))
        raise ValueError(f"Feature columns must be numeric: {missing_numeric}")
    mask = feature_frame.notna().all(axis=1) & ~np.isinf(numeric).any(axis=1)
    return frame[mask].sort_values(["date", "instrument_key"]).reset_index(drop=True)


def _validate_feature_columns(frame: pd.DataFrame, feature_columns: list[str]) -> None:
    missing = sorted(set(feature_columns).difference(frame.columns))
    if missing:
        raise ValueError(f"Missing feature columns: {missing}")


def _parse_date(value: date | str) -> date:
    if isinstance(value, date):
        return value
    parsed = pd.to_datetime(value, errors="raise")
    return parsed.date()
