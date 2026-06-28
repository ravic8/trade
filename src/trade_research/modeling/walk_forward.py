from __future__ import annotations

from dataclasses import asdict, dataclass
from datetime import UTC, date, datetime
from typing import Any

import pandas as pd

from trade_research.modeling.datasets import make_prediction_view, make_walk_forward_fold
from trade_research.modeling.ml_dataset_v1 import TARGET_COLUMN_V1


@dataclass(frozen=True)
class WalkForwardManifestConfig:
    min_train_days: int = 240
    validation_days: int = 60
    prediction_step_days: int = 1
    target_column: str = TARGET_COLUMN_V1
    start_date: date | None = None
    end_date: date | None = None
    max_folds: int | None = None


@dataclass(frozen=True)
class WalkForwardManifestBuild:
    folds: pd.DataFrame
    summary: dict[str, Any]


def build_walk_forward_manifest(
    dataset: pd.DataFrame,
    feature_columns: list[str],
    config: WalkForwardManifestConfig | None = None,
) -> WalkForwardManifestBuild:
    cfg = config or WalkForwardManifestConfig()
    prepared = dataset.copy()
    prepared["date"] = pd.to_datetime(prepared["date"], errors="coerce").dt.date

    candidate_dates = _candidate_prediction_dates(prepared, feature_columns, cfg)
    rows = []
    skipped = []
    for prediction_date in candidate_dates:
        try:
            fold = make_walk_forward_fold(
                prepared,
                feature_columns,
                prediction_date=prediction_date,
                min_train_days=cfg.min_train_days,
                validation_days=cfg.validation_days,
            )
        except ValueError as exc:
            skipped.append(
                {
                    "prediction_date": prediction_date,
                    "reason": str(exc),
                }
            )
            continue

        rows.append(_fold_manifest_row(fold, prepared, feature_columns, cfg))
        if cfg.max_folds is not None and len(rows) >= cfg.max_folds:
            break

    folds = pd.DataFrame(rows)
    if not folds.empty:
        folds = folds.sort_values("prediction_date").reset_index(drop=True)

    summary = _summary(
        folds=folds,
        candidate_dates=candidate_dates,
        skipped=skipped,
        dataset=prepared,
        feature_columns=feature_columns,
        config=cfg,
    )
    return WalkForwardManifestBuild(folds=folds, summary=summary)


def _candidate_prediction_dates(
    dataset: pd.DataFrame,
    feature_columns: list[str],
    config: WalkForwardManifestConfig,
) -> list[date]:
    prediction_view = make_prediction_view(dataset, feature_columns)
    dates = sorted(prediction_view.metadata["date"].dropna().unique())
    if config.start_date is not None:
        dates = [value for value in dates if value >= config.start_date]
    if config.end_date is not None:
        dates = [value for value in dates if value <= config.end_date]
    if config.prediction_step_days < 1:
        raise ValueError("prediction_step_days must be at least 1.")
    return dates[:: config.prediction_step_days]


def _fold_manifest_row(
    fold,
    dataset: pd.DataFrame,
    feature_columns: list[str],
    config: WalkForwardManifestConfig,
) -> dict[str, Any]:
    train_dates = sorted(fold.train["date"].unique())
    validation_dates = sorted(fold.validation["date"].unique())
    dataset_row = dataset.iloc[0] if not dataset.empty else {}
    return {
        "fold_id": f"wf_{fold.prediction_date.strftime('%Y%m%d')}",
        "prediction_date": fold.prediction_date,
        "train_start_date": fold.train_start_date,
        "train_end_date": fold.train_end_date,
        "validation_start_date": fold.validation_start_date,
        "validation_end_date": fold.validation_end_date,
        "train_date_count": len(train_dates),
        "validation_date_count": len(validation_dates),
        "train_row_count": int(len(fold.train)),
        "validation_row_count": int(len(fold.validation)),
        "prediction_row_count": int(len(fold.prediction)),
        "feature_column_count": int(len(feature_columns)),
        "target_column": config.target_column,
        "min_train_days": config.min_train_days,
        "validation_days": config.validation_days,
        "prediction_step_days": config.prediction_step_days,
        "ml_dataset_version": _first_value(dataset_row, "ml_dataset_version"),
        "feature_version": _first_value(dataset_row, "feature_version"),
        "target_version": _first_value(dataset_row, "target_version"),
        "coverage_policy": _first_value(dataset_row, "coverage_policy"),
        "leakage_check_train_before_prediction": fold.train_end_date < fold.prediction_date,
        "leakage_check_validation_before_prediction": (
            fold.validation_end_date < fold.prediction_date
        ),
    }


def _summary(
    folds: pd.DataFrame,
    candidate_dates: list[date],
    skipped: list[dict[str, Any]],
    dataset: pd.DataFrame,
    feature_columns: list[str],
    config: WalkForwardManifestConfig,
) -> dict[str, Any]:
    leakage_passed = bool(
        folds.empty
        or (
            folds["leakage_check_train_before_prediction"].all()
            and folds["leakage_check_validation_before_prediction"].all()
        )
    )
    return {
        "artifact_name": "walk_forward_folds_v1",
        "generated_at": datetime.now(UTC).isoformat(),
        "fold_count": int(len(folds)),
        "candidate_date_count": int(len(candidate_dates)),
        "skipped_candidate_count": int(len(skipped)),
        "first_prediction_date": _date_iso(folds["prediction_date"].min())
        if not folds.empty
        else None,
        "last_prediction_date": _date_iso(folds["prediction_date"].max())
        if not folds.empty
        else None,
        "feature_column_count": int(len(feature_columns)),
        "target_column": config.target_column,
        "trainable_row_count": int(dataset["is_trainable"].astype(bool).sum())
        if "is_trainable" in dataset
        else 0,
        "leakage_checks_passed": leakage_passed,
        "skipped_candidate_examples": skipped[:10],
        "config": asdict(config),
    }


def _first_value(row: Any, column: str) -> Any:
    if isinstance(row, pd.Series) and column in row:
        value = row[column]
        return None if pd.isna(value) else value
    return None


def _date_iso(value: Any) -> str | None:
    if pd.isna(value):
        return None
    return value.isoformat() if hasattr(value, "isoformat") else str(value)
