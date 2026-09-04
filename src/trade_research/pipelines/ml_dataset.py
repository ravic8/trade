from __future__ import annotations

import json
from pathlib import Path

import pandas as pd

from trade_research.config import get_settings
from trade_research.modeling.ml_dataset_v1 import (
    MLDatasetV1Config,
    build_ml_dataset_v1,
)
from trade_research.pipelines.base import PipelineRunResult
from trade_research.storage import ParquetStore


def run_ml_dataset_v1_pipeline(
    ohlcv_name: str = "processed/validated/ohlcv_daily_validated",
    features_name: str = "processed/features/daily_v1_ohlcv_technical",
    targets_name: str = "processed/targets/daily_v1_forward_returns",
    stock_coverage_name: str = "processed/validation/daily_pipeline_stock_coverage",
    stock_coverage_path: Path | None = None,
    output_name: str = "processed/ml/ml_dataset_v1",
    summary_output: Path | None = None,
    exclusions_output: Path | None = None,
    feature_columns_output: Path | None = None,
    leakage_checks_output: Path | None = None,
    config: MLDatasetV1Config | None = None,
) -> PipelineRunResult:
    settings = get_settings()
    store = ParquetStore(settings.data_dir)
    output_dir = settings.data_dir / "processed/ml"
    summary_path = summary_output or output_dir / "ml_dataset_v1_summary.json"
    exclusions_path = exclusions_output or output_dir / "ml_dataset_v1_exclusions.csv"
    feature_columns_path = (
        feature_columns_output or output_dir / "ml_dataset_v1_feature_columns.json"
    )
    leakage_checks_path = (
        leakage_checks_output or output_dir / "ml_dataset_v1_leakage_checks.json"
    )

    stock_coverage = (
        pd.read_parquet(stock_coverage_path)
        if stock_coverage_path is not None
        else store.read_frame(stock_coverage_name)
    )
    build = build_ml_dataset_v1(
        ohlcv=store.read_frame(ohlcv_name),
        features=store.read_frame(features_name),
        targets=store.read_frame(targets_name),
        stock_coverage=stock_coverage,
        config=config,
    )

    dataset_path = store.write_frame(output_name, build.dataset)
    exclusions_path.parent.mkdir(parents=True, exist_ok=True)
    summary_path.parent.mkdir(parents=True, exist_ok=True)
    feature_columns_path.parent.mkdir(parents=True, exist_ok=True)
    leakage_checks_path.parent.mkdir(parents=True, exist_ok=True)

    build.exclusions.to_csv(exclusions_path, index=False)
    summary_path.write_text(json.dumps(build.summary, indent=2) + "\n")
    feature_columns_path.write_text(
        json.dumps(build.feature_columns, indent=2) + "\n"
    )
    leakage_checks_path.write_text(
        json.dumps(build.leakage_checks, indent=2) + "\n"
    )

    status = "pass" if build.summary["trainable_row_count"] > 0 else "warn"
    warnings = [] if status == "pass" else ["No trainable rows in ml_dataset_v1."]
    return PipelineRunResult(
        name="ml_dataset_v1",
        status=status,
        rows=len(build.dataset),
        artifacts={
            "dataset": dataset_path,
            "summary": summary_path,
            "exclusions": exclusions_path,
            "feature_columns": feature_columns_path,
            "leakage_checks": leakage_checks_path,
        },
        metrics={
            "row_count": build.summary["row_count"],
            "trainable_row_count": build.summary["trainable_row_count"],
            "symbol_count": build.summary["symbol_count"],
            "trainable_symbol_count": build.summary["trainable_symbol_count"],
            "excluded_symbol_count": build.summary["excluded_symbol_count"],
            "feature_column_count": build.summary["feature_column_count"],
            "date_min": build.summary["date_min"],
            "date_max": build.summary["date_max"],
            "coverage_policy": build.summary["coverage_policy"],
            "leakage_checks_passed": build.summary["leakage_checks_passed"],
        },
        warnings=warnings,
    )
