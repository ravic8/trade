from __future__ import annotations

import json
from pathlib import Path
from typing import Any

import pandas as pd

from trade_research.config import get_settings
from trade_research.modeling.datasets import load_ml_dataset_v1
from trade_research.modeling.walk_forward import (
    WalkForwardManifestConfig,
    build_walk_forward_manifest,
)
from trade_research.pipelines.base import PipelineRunResult


def run_walk_forward_folds_v1_pipeline(
    dataset_path: Path | None = None,
    feature_columns_path: Path | None = None,
    folds_output: Path | None = None,
    summary_output: Path | None = None,
    config: WalkForwardManifestConfig | None = None,
) -> PipelineRunResult:
    settings = get_settings()
    output_dir = settings.data_dir / "processed/ml/walk_forward_v1"
    dataset_file = dataset_path or settings.data_dir / "processed/ml/ml_dataset_v1.parquet"
    feature_columns_file = (
        feature_columns_path
        or settings.data_dir / "processed/ml/ml_dataset_v1_feature_columns.json"
    )
    folds_path = folds_output or output_dir / "walk_forward_folds.parquet"
    summary_path = summary_output or output_dir / "walk_forward_summary.json"

    dataset, feature_columns = load_ml_dataset_v1(
        dataset_path=dataset_file,
        feature_columns_path=feature_columns_file,
    )
    build = build_walk_forward_manifest(
        dataset,
        feature_columns,
        config=config,
    )

    folds_path.parent.mkdir(parents=True, exist_ok=True)
    summary_path.parent.mkdir(parents=True, exist_ok=True)
    build.folds.to_parquet(folds_path, index=False)
    summary_path.write_text(json.dumps(build.summary, indent=2, default=_json_default) + "\n")

    status = "pass" if build.summary["fold_count"] > 0 else "warn"
    warnings = [] if status == "pass" else ["No valid walk-forward folds generated."]
    return PipelineRunResult(
        name="walk_forward_folds_v1",
        status=status,
        rows=len(build.folds),
        artifacts={
            "folds": folds_path,
            "summary": summary_path,
        },
        metrics={
            "fold_count": build.summary["fold_count"],
            "candidate_date_count": build.summary["candidate_date_count"],
            "skipped_candidate_count": build.summary["skipped_candidate_count"],
            "first_prediction_date": build.summary["first_prediction_date"],
            "last_prediction_date": build.summary["last_prediction_date"],
            "feature_column_count": build.summary["feature_column_count"],
            "target_column": build.summary["target_column"],
            "leakage_checks_passed": build.summary["leakage_checks_passed"],
        },
        warnings=warnings,
    )


def _json_default(value: Any) -> str:
    if isinstance(value, pd.Timestamp):
        return value.date().isoformat()
    if hasattr(value, "isoformat"):
        return value.isoformat()
    return str(value)
