from __future__ import annotations

import json
from pathlib import Path
from typing import Any

import pandas as pd

from trade_research.config import get_settings
from trade_research.modeling.datasets import load_ml_dataset_v1
from trade_research.modeling.latest_predictions import (
    LatestPredictionConfig,
    run_latest_predictions,
)
from trade_research.pipelines.base import PipelineRunResult


def run_latest_predictions_v1_pipeline(
    dataset_path: Path | None = None,
    feature_columns_path: Path | None = None,
    output_dir: Path | None = None,
    predictions_output: Path | None = None,
    candidates_output: Path | None = None,
    summary_output: Path | None = None,
    report_output: Path | None = None,
    config: LatestPredictionConfig | None = None,
) -> PipelineRunResult:
    settings = get_settings()
    root = output_dir or settings.data_dir / "processed/ml/latest_predictions_v1"
    dataset_file = dataset_path or settings.data_dir / "processed/ml/ml_dataset_v1.parquet"
    feature_columns_file = (
        feature_columns_path
        or settings.data_dir / "processed/ml/ml_dataset_v1_feature_columns.json"
    )
    predictions_path = predictions_output or root / "latest_predictions.parquet"
    candidates_path = candidates_output or root / "latest_candidates.json"
    summary_path = summary_output or root / "latest_predictions_summary.json"
    report_path = report_output or root / "latest_predictions_report.md"

    dataset, feature_columns = load_ml_dataset_v1(
        dataset_path=dataset_file,
        feature_columns_path=feature_columns_file,
    )
    result = run_latest_predictions(dataset, feature_columns, config=config)

    predictions_path.parent.mkdir(parents=True, exist_ok=True)
    candidates_path.parent.mkdir(parents=True, exist_ok=True)
    summary_path.parent.mkdir(parents=True, exist_ok=True)
    report_path.parent.mkdir(parents=True, exist_ok=True)
    result.predictions.to_parquet(predictions_path, index=False)
    candidates_path.write_text(
        json.dumps(result.candidates, indent=2, default=_json_default) + "\n"
    )
    summary_path.write_text(json.dumps(result.summary, indent=2, default=_json_default) + "\n")
    report_path.write_text(result.summary_md)

    status = "pass" if result.summary["prediction_row_count"] > 0 else "warn"
    warnings = [] if status == "pass" else ["No latest prediction rows generated."]
    return PipelineRunResult(
        name="latest_predictions_v1",
        status=status,
        rows=len(result.predictions),
        artifacts={
            "predictions": predictions_path,
            "candidates": candidates_path,
            "summary": summary_path,
            "report": report_path,
        },
        metrics={
            "prediction_row_count": result.summary["prediction_row_count"],
            "model_count": result.summary["model_count"],
            "run_count": result.summary["run_count"],
            "prediction_date": result.summary["prediction_date"],
            "target_session_date": result.summary["target_session_date"],
        },
        warnings=warnings,
    )


def _json_default(value: Any) -> str:
    if isinstance(value, pd.Timestamp):
        return value.date().isoformat()
    if hasattr(value, "isoformat"):
        return value.isoformat()
    return str(value)
