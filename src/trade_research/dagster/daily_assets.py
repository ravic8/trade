from __future__ import annotations

from pathlib import Path
from typing import Any

from dagster import AssetIn, MetadataValue, asset

from trade_research.config import get_settings
from trade_research.pipelines import (
    PipelineRunResult,
    run_daily_feature_pipeline,
    run_daily_pipeline_health_pipeline,
    run_daily_target_pipeline,
    run_factor_research_pipeline,
    run_ml_dataset_v1_pipeline,
    run_processed_dataset_validation_pipeline,
    run_upstox_daily_ohlcv_pipeline,
)
from trade_research.validation import resolve_latest_expected_trading_date


@asset(
    group_name="daily_research",
    compute_kind="upstox",
    description="Incrementally fetch Upstox NSE daily OHLCV and upsert it into TimescaleDB.",
)
def upstox_daily_ohlcv(context) -> PipelineRunResult:
    settings = get_settings()
    latest = resolve_latest_expected_trading_date()
    result = run_upstox_daily_ohlcv_pipeline(
        to_date=latest.latest_expected_trading_date.isoformat(),
        store_db=True,
        export_db_snapshot=True,
        trigger="dagster",
        max_concurrent_fetches=settings.upstox_historical_concurrency,
    )
    context.add_output_metadata(_result_metadata(result))
    return result


@asset(
    group_name="daily_research",
    compute_kind="python",
    ins={
        "daily_ohlcv": AssetIn("upstox_daily_ohlcv"),
        "daily_features": AssetIn("daily_features_v1"),
        "daily_targets": AssetIn("daily_targets_v1"),
    },
    description="Validate processed OHLCV, feature, target, and alignment artifacts.",
)
def processed_dataset_validation(
    context,
    daily_ohlcv: PipelineRunResult,
    daily_features: PipelineRunResult,
    daily_targets: PipelineRunResult,
) -> PipelineRunResult:
    _assert_upstream_not_failed(daily_ohlcv)
    _assert_upstream_not_failed(daily_features)
    _assert_upstream_not_failed(daily_targets)
    result = run_processed_dataset_validation_pipeline()
    context.add_output_metadata(_result_metadata(result))
    return result


@asset(
    group_name="daily_research",
    compute_kind="python",
    ins={"daily_ohlcv": AssetIn("upstox_daily_ohlcv")},
    description="Build the frozen daily technical feature layer from daily OHLCV.",
)
def daily_features_v1(
    context,
    daily_ohlcv: PipelineRunResult,
) -> PipelineRunResult:
    _assert_upstream_not_failed(daily_ohlcv)
    result = run_daily_feature_pipeline(
        input_source="timescale",
        store_db=True,
        incremental=True,
        lookback_days=320,
        export_db_snapshot=True,
    )
    context.add_output_metadata(_result_metadata(result))
    return result


@asset(
    group_name="daily_research",
    compute_kind="python",
    ins={"daily_ohlcv": AssetIn("upstox_daily_ohlcv")},
    description="Build the frozen daily forward-return target layer from daily OHLCV.",
)
def daily_targets_v1(
    context,
    daily_ohlcv: PipelineRunResult,
) -> PipelineRunResult:
    _assert_upstream_not_failed(daily_ohlcv)
    result = run_daily_target_pipeline(
        input_source="timescale",
        store_db=True,
        incremental=True,
        recompute_lookback_days=90,
        export_db_snapshot=True,
    )
    context.add_output_metadata(_result_metadata(result))
    return result


@asset(
    group_name="daily_research",
    compute_kind="python",
    ins={
        "ml_dataset": AssetIn("ml_dataset_v1"),
        "daily_features": AssetIn("daily_features_v1"),
        "daily_targets": AssetIn("daily_targets_v1"),
    },
    description="Build daily factor-research evidence from frozen feature and target layers.",
)
def factor_research_v1(
    context,
    ml_dataset: PipelineRunResult,
    daily_features: PipelineRunResult,
    daily_targets: PipelineRunResult,
) -> PipelineRunResult:
    _assert_upstream_not_failed(ml_dataset)
    _assert_upstream_not_failed(daily_features)
    _assert_upstream_not_failed(daily_targets)
    result = run_factor_research_pipeline()
    context.add_output_metadata(_result_metadata(result))
    return result


@asset(
    group_name="daily_research",
    compute_kind="python",
    ins={
        "processed_validation": AssetIn("processed_dataset_validation"),
        "daily_features": AssetIn("daily_features_v1"),
        "daily_targets": AssetIn("daily_targets_v1"),
    },
    description="Build the leakage-aware v1 ML dataset for next-day return models.",
)
def ml_dataset_v1(
    context,
    processed_validation: PipelineRunResult,
    daily_features: PipelineRunResult,
    daily_targets: PipelineRunResult,
) -> PipelineRunResult:
    _assert_upstream_not_failed(processed_validation)
    _assert_upstream_not_failed(daily_features)
    _assert_upstream_not_failed(daily_targets)
    result = run_ml_dataset_v1_pipeline()
    context.add_output_metadata(_result_metadata(result))
    return result


@asset(
    group_name="daily_research",
    compute_kind="python",
    ins={
        "processed_validation": AssetIn("processed_dataset_validation"),
        "ml_dataset": AssetIn("ml_dataset_v1"),
        "daily_features": AssetIn("daily_features_v1"),
        "daily_targets": AssetIn("daily_targets_v1"),
        "factor_research": AssetIn("factor_research_v1"),
    },
    description="Generate the end-to-end daily research pipeline health report.",
)
def daily_pipeline_health(
    context,
    processed_validation: PipelineRunResult,
    ml_dataset: PipelineRunResult,
    daily_features: PipelineRunResult,
    daily_targets: PipelineRunResult,
    factor_research: PipelineRunResult,
) -> PipelineRunResult:
    _assert_upstream_not_failed(processed_validation)
    _assert_upstream_not_failed(ml_dataset)
    _assert_upstream_not_failed(daily_features)
    _assert_upstream_not_failed(daily_targets)
    _assert_upstream_not_failed(factor_research)
    result = run_daily_pipeline_health_pipeline(
        run_factor_research=False,
        rebuild_artifacts=False,
        coverage_run_id=context.run_id,
        store_coverage_db=True,
        coverage_windows_months=[6, 9, 12, 15, 18, 24],
    )
    context.add_output_metadata(_result_metadata(result))
    return result


def _assert_upstream_not_failed(result: PipelineRunResult) -> None:
    if result.status == "fail":
        raise RuntimeError(f"Upstream pipeline failed: {result.name}")


def _result_metadata(result: PipelineRunResult) -> dict[str, Any]:
    metadata: dict[str, Any] = {
        "pipeline": result.name,
        "status": result.status,
        "rows": result.rows,
    }
    for name, path in result.artifacts.items():
        metadata[f"artifact_{name}"] = MetadataValue.path(str(path))
    if result.warnings:
        metadata["warnings"] = MetadataValue.json(result.warnings)
    if result.blocking_issues:
        metadata["blocking_issues"] = MetadataValue.json(result.blocking_issues)
    for key, value in result.metrics.items():
        if _is_simple_metadata_value(value):
            metadata[f"metric_{key}"] = value
    return metadata


def _is_simple_metadata_value(value: object) -> bool:
    return value is None or isinstance(value, str | int | float | bool | Path)
