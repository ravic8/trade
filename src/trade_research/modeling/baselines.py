from __future__ import annotations

from dataclasses import asdict, dataclass
from datetime import UTC, datetime
from typing import Any

import numpy as np
import pandas as pd

from trade_research.modeling.datasets import fold_views, make_walk_forward_fold
from trade_research.modeling.ml_dataset_v1 import TARGET_COLUMN_V1
from trade_research.modeling.walk_forward import WalkForwardManifestConfig

BASELINE_MODEL_IDS = [
    "mean_return",
    "momentum_1d",
    "reversal_1d",
    "momentum_5d",
    "volatility_adjusted_momentum_20d",
]


@dataclass(frozen=True)
class BaselineRunConfig:
    min_train_days: int = 180
    validation_days: int = 40
    prediction_step_days: int = 1
    max_folds: int | None = None
    target_column: str = TARGET_COLUMN_V1
    top_n_values: tuple[int, ...] = (5, 10, 20)


@dataclass(frozen=True)
class BaselineRun:
    predictions: pd.DataFrame
    metrics: dict[str, Any]
    summary_md: str


def run_baseline_predictions(
    dataset: pd.DataFrame,
    feature_columns: list[str],
    config: BaselineRunConfig | None = None,
) -> BaselineRun:
    cfg = config or BaselineRunConfig()
    manifest_config = WalkForwardManifestConfig(
        min_train_days=cfg.min_train_days,
        validation_days=cfg.validation_days,
        prediction_step_days=cfg.prediction_step_days,
        max_folds=cfg.max_folds,
        target_column=cfg.target_column,
    )
    from trade_research.modeling.walk_forward import build_walk_forward_manifest

    manifest = build_walk_forward_manifest(dataset, feature_columns, config=manifest_config)
    prediction_frames = []
    for fold_row in manifest.folds.to_dict(orient="records"):
        fold = make_walk_forward_fold(
            dataset,
            feature_columns,
            prediction_date=fold_row["prediction_date"],
            min_train_days=cfg.min_train_days,
            validation_days=cfg.validation_days,
        )
        train_view, validation_view, prediction_view = fold_views(
            fold,
            feature_columns,
            task="regression",
            target_column=cfg.target_column,
        )
        prediction_frames.append(
            _fold_predictions(
                fold=fold,
                train_y=train_view.y,
                validation_y=validation_view.y,
                prediction_frame=fold.prediction,
                prediction_metadata=prediction_view.metadata,
                config=cfg,
            )
        )

    predictions = (
        pd.concat(prediction_frames, ignore_index=True)
        if prediction_frames
        else _empty_predictions()
    )
    metrics = _metrics(predictions, cfg, manifest.summary)
    return BaselineRun(
        predictions=predictions,
        metrics=metrics,
        summary_md=_summary_markdown(metrics),
    )


def _fold_predictions(
    fold,
    train_y: pd.Series,
    validation_y: pd.Series,
    prediction_frame: pd.DataFrame,
    prediction_metadata: pd.DataFrame,
    config: BaselineRunConfig,
) -> pd.DataFrame:
    scores = _baseline_scores(prediction_frame, train_y, validation_y)
    frames = []
    base = prediction_metadata.copy()
    base["prediction_date"] = fold.prediction_date
    base["fold_id"] = f"wf_{fold.prediction_date.strftime('%Y%m%d')}"
    base["train_start_date"] = fold.train_start_date
    base["train_end_date"] = fold.train_end_date
    base["validation_start_date"] = fold.validation_start_date
    base["validation_end_date"] = fold.validation_end_date
    base["realized_forward_ret_1d"] = prediction_frame[config.target_column].to_numpy()
    for model_id, score in scores.items():
        out = base.copy()
        out["model_id"] = model_id
        out["score"] = score
        out["rank"] = out.groupby("prediction_date")["score"].rank(
            ascending=False,
            method="first",
        )
        for top_n in config.top_n_values:
            out[f"selected_top_{top_n}"] = out["rank"].le(top_n)
        frames.append(out)
    return pd.concat(frames, ignore_index=True)


def _baseline_scores(
    frame: pd.DataFrame,
    train_y: pd.Series,
    validation_y: pd.Series,
) -> dict[str, pd.Series]:
    index = frame.index
    mean_return = float(pd.concat([train_y, validation_y]).dropna().mean())
    scores: dict[str, pd.Series] = {
        "mean_return": pd.Series(mean_return, index=index),
        "momentum_1d": frame["ret_1d"] if "ret_1d" in frame else pd.Series(0.0, index=index),
        "reversal_1d": -frame["ret_1d"] if "ret_1d" in frame else pd.Series(0.0, index=index),
    }
    if "ret_5d" in frame:
        scores["momentum_5d"] = frame["ret_5d"]
    if "ret_20d" in frame and "volatility_20d" in frame:
        denom = frame["volatility_20d"].replace(0, np.nan)
        scores["volatility_adjusted_momentum_20d"] = (frame["ret_20d"] / denom).fillna(0.0)
    return scores


def _metrics(
    predictions: pd.DataFrame,
    config: BaselineRunConfig,
    manifest_summary: dict[str, Any],
) -> dict[str, Any]:
    return evaluate_prediction_metrics(
        predictions=predictions,
        top_n_values=config.top_n_values,
        manifest_summary=manifest_summary,
        config=asdict(config),
        artifact_name="baseline_predictions_v1",
    )


def evaluate_prediction_metrics(
    predictions: pd.DataFrame,
    top_n_values: tuple[int, ...],
    manifest_summary: dict[str, Any],
    config: dict[str, Any],
    artifact_name: str,
) -> dict[str, Any]:
    model_metrics = []
    for model_id, group in predictions.groupby("model_id", sort=True):
        evaluated = group[group["realized_forward_ret_1d"].notna()].copy()
        row: dict[str, Any] = {
            "model_id": model_id,
            "prediction_rows": int(len(group)),
            "evaluated_rows": int(len(evaluated)),
            "prediction_date_count": int(group["prediction_date"].nunique()),
            "rank_ic_mean": _rank_ic_mean(evaluated),
            "average_realized_return": _nullable_float(evaluated["realized_forward_ret_1d"].mean()),
        }
        for top_n in top_n_values:
            selected = evaluated[evaluated["rank"].le(top_n)]
            row[f"top_{top_n}_average_return"] = _nullable_float(
                selected["realized_forward_ret_1d"].mean()
            )
            row[f"top_{top_n}_hit_rate"] = _nullable_float(
                selected["realized_forward_ret_1d"].gt(0).mean()
            )
            row[f"top_{top_n}_date_count"] = int(selected["prediction_date"].nunique())
        model_metrics.append(row)

    return {
        "artifact_name": artifact_name,
        "generated_at": datetime.now(UTC).isoformat(),
        "config": config,
        "manifest": manifest_summary,
        "prediction_row_count": int(len(predictions)),
        "model_count": int(predictions["model_id"].nunique()) if not predictions.empty else 0,
        "models": model_metrics,
    }


def _rank_ic_mean(frame: pd.DataFrame) -> float | None:
    if frame.empty:
        return None
    values = []
    for _, group in frame.groupby("prediction_date"):
        if group["score"].nunique() < 2 or group["realized_forward_ret_1d"].nunique() < 2:
            continue
        score_rank = group["score"].rank(method="average")
        realized_rank = group["realized_forward_ret_1d"].rank(method="average")
        corr = score_rank.corr(realized_rank)
        if pd.notna(corr):
            values.append(float(corr))
    return float(np.mean(values)) if values else None


def _summary_markdown(metrics: dict[str, Any]) -> str:
    lines = [
        "# Baseline Predictions v1",
        "",
        f"Generated at: `{metrics['generated_at']}`",
        f"Prediction rows: `{metrics['prediction_row_count']}`",
        f"Models: `{metrics['model_count']}`",
        "",
        "## Model Metrics",
        "",
        "| Model | Evaluated Rows | Rank IC Mean | Top 10 Avg Return | Top 10 Hit Rate |",
        "| --- | ---: | ---: | ---: | ---: |",
    ]
    for row in metrics["models"]:
        lines.append(
            "| {model_id} | {evaluated_rows} | {rank_ic_mean} | "
            "{top_10_average_return} | {top_10_hit_rate} |".format(
                model_id=row["model_id"],
                evaluated_rows=row["evaluated_rows"],
                rank_ic_mean=_format_metric(row["rank_ic_mean"]),
                top_10_average_return=_format_metric(row.get("top_10_average_return")),
                top_10_hit_rate=_format_metric(row.get("top_10_hit_rate")),
            )
        )
    lines.append("")
    return "\n".join(lines)


def _format_metric(value: object) -> str:
    if value is None or pd.isna(value):
        return ""
    return f"{float(value):.6f}"


def _nullable_float(value: object) -> float | None:
    return None if pd.isna(value) else float(value)


def _empty_predictions() -> pd.DataFrame:
    return pd.DataFrame(
        columns=[
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
