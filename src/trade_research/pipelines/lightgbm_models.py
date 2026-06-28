from __future__ import annotations

import json
from pathlib import Path
from typing import Any

import pandas as pd

from trade_research.config import get_settings
from trade_research.modeling.datasets import load_ml_dataset_v1
from trade_research.modeling.lightgbm_models import (
    LightGBMRunConfig,
    run_lightgbm_predictions,
)
from trade_research.pipelines.base import PipelineRunResult


def run_lightgbm_predictions_v1_pipeline(
    dataset_path: Path | None = None,
    feature_columns_path: Path | None = None,
    predictions_output: Path | None = None,
    metrics_output: Path | None = None,
    summary_output: Path | None = None,
    config: LightGBMRunConfig | None = None,
) -> PipelineRunResult:
    settings = get_settings()
    output_dir = settings.data_dir / "processed/ml/lightgbm_v1"
    dataset_file = dataset_path or settings.data_dir / "processed/ml/ml_dataset_v1.parquet"
    feature_columns_file = (
        feature_columns_path
        or settings.data_dir / "processed/ml/ml_dataset_v1_feature_columns.json"
    )
    predictions_path = predictions_output or output_dir / "lightgbm_predictions.parquet"
    metrics_path = metrics_output or output_dir / "lightgbm_metrics.json"
    summary_path = summary_output or output_dir / "lightgbm_summary.md"

    dataset, feature_columns = load_ml_dataset_v1(
        dataset_path=dataset_file,
        feature_columns_path=feature_columns_file,
    )
    result = run_lightgbm_predictions(dataset, feature_columns, config=config)

    predictions_path.parent.mkdir(parents=True, exist_ok=True)
    metrics_path.parent.mkdir(parents=True, exist_ok=True)
    summary_path.parent.mkdir(parents=True, exist_ok=True)
    result.predictions.to_parquet(predictions_path, index=False)
    metrics_path.write_text(json.dumps(result.metrics, indent=2, default=_json_default) + "\n")
    summary_path.write_text(result.summary_md)

    status = "pass" if result.metrics["prediction_row_count"] > 0 else "warn"
    warnings = [] if status == "pass" else ["No LightGBM prediction rows generated."]
    return PipelineRunResult(
        name="lightgbm_predictions_v1",
        status=status,
        rows=len(result.predictions),
        artifacts={
            "predictions": predictions_path,
            "metrics": metrics_path,
            "summary": summary_path,
        },
        metrics={
            "prediction_row_count": result.metrics["prediction_row_count"],
            "model_count": result.metrics["model_count"],
            "fold_count": result.metrics["manifest"]["fold_count"],
            "first_prediction_date": result.metrics["manifest"]["first_prediction_date"],
            "last_prediction_date": result.metrics["manifest"]["last_prediction_date"],
        },
        warnings=warnings,
    )


def _json_default(value: Any) -> str:
    if isinstance(value, pd.Timestamp):
        return value.date().isoformat()
    if hasattr(value, "isoformat"):
        return value.isoformat()
    return str(value)
