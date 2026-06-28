from __future__ import annotations

from dataclasses import asdict, dataclass
from datetime import UTC, date, datetime
from typing import Any

import pandas as pd

from trade_research.modeling.baselines import (
    BaselineRunConfig,
)
from trade_research.modeling.baselines import (
    _fold_predictions as _baseline_fold_predictions,
)
from trade_research.modeling.datasets import (
    fold_views,
    make_prediction_view,
    make_walk_forward_fold,
)
from trade_research.modeling.lightgbm_models import (
    LightGBMRunConfig,
    _import_lightgbm,
)
from trade_research.modeling.lightgbm_models import (
    _fold_predictions as _lightgbm_fold_predictions,
)
from trade_research.modeling.ml_dataset_v1 import TARGET_COLUMN_V1


@dataclass(frozen=True)
class LatestPredictionConfig:
    min_train_days: int = 180
    validation_days: int = 40
    target_column: str = TARGET_COLUMN_V1
    top_n_values: tuple[int, ...] = (5, 10, 20)
    include_baselines: bool = True
    include_lightgbm: bool = True
    lightgbm_n_estimators: int = 80
    lightgbm_n_jobs: int = 1


@dataclass(frozen=True)
class LatestPredictionRun:
    predictions: pd.DataFrame
    candidates: dict[str, Any]
    summary: dict[str, Any]
    summary_md: str


def run_latest_predictions(
    dataset: pd.DataFrame,
    feature_columns: list[str],
    config: LatestPredictionConfig | None = None,
) -> LatestPredictionRun:
    cfg = config or LatestPredictionConfig()
    prediction_date = _latest_prediction_date(dataset, feature_columns)
    fold = make_walk_forward_fold(
        dataset,
        feature_columns,
        prediction_date=prediction_date,
        min_train_days=cfg.min_train_days,
        validation_days=cfg.validation_days,
    )

    frames = []
    if cfg.include_baselines:
        frames.append(_baseline_predictions(fold, feature_columns, cfg))
    if cfg.include_lightgbm:
        frames.append(_lightgbm_predictions(fold, feature_columns, cfg))

    predictions = pd.concat(frames, ignore_index=True) if frames else _empty_predictions()
    predictions = predictions.sort_values(["run_id", "model_id", "rank"]).reset_index(drop=True)
    candidates = _candidate_payload(predictions, cfg, prediction_date)
    summary = _summary(predictions, candidates, fold, cfg)
    return LatestPredictionRun(
        predictions=predictions,
        candidates=candidates,
        summary=summary,
        summary_md=_summary_markdown(summary, candidates),
    )


def _latest_prediction_date(dataset: pd.DataFrame, feature_columns: list[str]) -> date:
    view = make_prediction_view(dataset, feature_columns)
    dates = pd.to_datetime(view.metadata["date"], errors="coerce").dt.date.dropna()
    if dates.empty:
        raise ValueError("No feature-complete rows are available for latest prediction.")
    return max(dates)


def _baseline_predictions(
    fold,
    feature_columns: list[str],
    config: LatestPredictionConfig,
) -> pd.DataFrame:
    baseline_config = BaselineRunConfig(
        min_train_days=config.min_train_days,
        validation_days=config.validation_days,
        target_column=config.target_column,
        top_n_values=config.top_n_values,
    )
    train_view, validation_view, prediction_view = fold_views(
        fold,
        feature_columns,
        task="regression",
        target_column=config.target_column,
    )
    out = _baseline_fold_predictions(
        fold=fold,
        train_y=train_view.y,
        validation_y=validation_view.y,
        prediction_frame=fold.prediction,
        prediction_metadata=prediction_view.metadata,
        config=baseline_config,
    )
    out.insert(0, "run_id", "baselines")
    return out


def _lightgbm_predictions(
    fold,
    feature_columns: list[str],
    config: LatestPredictionConfig,
) -> pd.DataFrame:
    lightgbm_config = LightGBMRunConfig(
        min_train_days=config.min_train_days,
        validation_days=config.validation_days,
        target_column=config.target_column,
        top_n_values=config.top_n_values,
        n_estimators=config.lightgbm_n_estimators,
        n_jobs=config.lightgbm_n_jobs,
    )
    train_view, validation_view, prediction_view = fold_views(
        fold,
        feature_columns,
        task="regression",
        target_column=config.target_column,
    )
    out = _lightgbm_fold_predictions(
        lgb=_import_lightgbm(),
        fold=fold,
        train_view=train_view,
        validation_view=validation_view,
        prediction_view=prediction_view,
        config=lightgbm_config,
    )
    out.insert(0, "run_id", "lightgbm")
    return out


def _candidate_payload(
    predictions: pd.DataFrame,
    config: LatestPredictionConfig,
    prediction_date: date,
) -> dict[str, Any]:
    runs = []
    for run_id, run_group in predictions.groupby("run_id", sort=True):
        models = []
        for model_id, model_group in run_group.groupby("model_id", sort=True):
            ranked = model_group.sort_values("rank")
            model_payload = {
                "model_id": model_id,
                "top_n": {},
            }
            for top_n in config.top_n_values:
                selected = ranked[ranked["rank"].le(top_n)]
                model_payload["top_n"][str(top_n)] = _candidate_rows(selected)
            models.append(model_payload)
        runs.append({"run_id": run_id, "models": models})
    return {
        "artifact_name": "latest_predictions_v1_candidates",
        "generated_at": datetime.now(UTC).isoformat(),
        "prediction_date": prediction_date.isoformat(),
        "target_column": config.target_column,
        "top_n_values": list(config.top_n_values),
        "runs": runs,
        "note": (
            "Latest feature-complete model candidates. Treat these as next-session candidates "
            "only when the upstream daily pipeline is current."
        ),
    }


def _candidate_rows(frame: pd.DataFrame) -> list[dict[str, Any]]:
    columns = [
        "prediction_date",
        "symbol",
        "instrument_key",
        "model_id",
        "rank",
        "score",
        "realized_forward_ret_1d",
    ]
    output = frame[[column for column in columns if column in frame.columns]].copy()
    if "prediction_date" in output:
        output["prediction_date"] = pd.to_datetime(
            output["prediction_date"],
            errors="coerce",
        ).dt.date.astype(str)
    return output.astype(object).where(pd.notna(output), None).to_dict(orient="records")


def _summary(
    predictions: pd.DataFrame,
    candidates: dict[str, Any],
    fold,
    config: LatestPredictionConfig,
) -> dict[str, Any]:
    return {
        "artifact_name": "latest_predictions_v1",
        "generated_at": datetime.now(UTC).isoformat(),
        "prediction_date": fold.prediction_date.isoformat(),
        "target_column": config.target_column,
        "prediction_row_count": int(len(predictions)),
        "run_count": int(predictions["run_id"].nunique()) if not predictions.empty else 0,
        "model_count": int(predictions["model_id"].nunique()) if not predictions.empty else 0,
        "candidate_top_n_values": list(config.top_n_values),
        "train_start_date": fold.train_start_date.isoformat(),
        "train_end_date": fold.train_end_date.isoformat(),
        "validation_start_date": fold.validation_start_date.isoformat(),
        "validation_end_date": fold.validation_end_date.isoformat(),
        "prediction_symbol_count": int(fold.prediction["instrument_key"].nunique()),
        "runs": [
            {
                "run_id": run["run_id"],
                "model_count": len(run["models"]),
            }
            for run in candidates["runs"]
        ],
        "config": asdict(config),
        "note": candidates["note"],
    }


def _summary_markdown(summary: dict[str, Any], candidates: dict[str, Any]) -> str:
    lines = [
        "# Latest Predictions v1",
        "",
        f"Generated at: `{summary['generated_at']}`",
        f"Prediction date: `{summary['prediction_date']}`",
        f"Prediction rows: `{summary['prediction_row_count']}`",
        f"Models: `{summary['model_count']}`",
        "",
        "## Top 5 Candidates",
        "",
        "| Run | Model | Symbols |",
        "| --- | --- | --- |",
    ]
    for run in candidates["runs"]:
        for model in run["models"]:
            rows = model["top_n"].get("5", [])
            symbols = ", ".join(row["symbol"] for row in rows)
            lines.append(f"| {run['run_id']} | {model['model_id']} | {symbols} |")
    lines.append("")
    return "\n".join(lines)


def _empty_predictions() -> pd.DataFrame:
    return pd.DataFrame(
        columns=[
            "run_id",
            "instrument_key",
            "symbol",
            "date",
            "prediction_date",
            "fold_id",
            "model_id",
            "score",
            "rank",
            "realized_forward_ret_1d",
        ]
    )
