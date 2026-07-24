from __future__ import annotations

from pathlib import Path
from typing import Any

from dagster import Array, AssetIn, Field, Int, MetadataValue, String, asset

from trade_research.analytics.bigquery import (
    DEFAULT_BIGQUERY_ENTITIES,
    BigQuerySyncResult,
    run_bigquery_sync,
    run_bigquery_tsx_canary,
)
from trade_research.config import get_settings
from trade_research.pipelines import (
    PipelineRunResult,
    run_completed_session_opportunity_target_pipeline,
    run_daily_feature_pipeline,
    run_daily_pipeline_health_pipeline,
    run_daily_target_pipeline,
    run_dukascopy_intraday_gap_validation_pipeline,
    run_dukascopy_intraday_ohlcv_pipeline,
    run_equity_universe_snapshot_pipeline,
    run_exchange_session_materialization_pipeline,
    run_factor_research_pipeline,
    run_ml_dataset_v1_pipeline,
    run_nse_daily_ohlcv_primary_pipeline,
    run_opportunity_target_pipeline,
    run_processed_dataset_validation_pipeline,
    run_upstox_daily_ohlcv_pipeline,
    run_yfinance_daily_ohlcv_pipeline,
    run_yfinance_daily_work_planner,
    run_yfinance_daily_work_queue,
    run_yfinance_intraday_ohlcv_pipeline,
)
from trade_research.validation import resolve_latest_expected_trading_date


@asset(
    group_name="analytics_exports",
    compute_kind="bigquery",
    config_schema={
        "exchange": Field(String, is_required=False),
        "year": Field(Int, is_required=False),
        "entities": Field(Array(String), default_value=list(DEFAULT_BIGQUERY_ENTITIES)),
    },
    description=(
        "Feature-gated, bounded PostgreSQL export through BigQuery staging, "
        "idempotent MERGE, and reconciliation."
    ),
)
def bigquery_export_sync(context) -> BigQuerySyncResult:
    config = context.op_config
    result = run_bigquery_sync(
        exchange=config.get("exchange"),
        year=config.get("year"),
        entities=config.get("entities", list(DEFAULT_BIGQUERY_ENTITIES)),
        trigger="dagster",
        run_id=context.run_id,
    )
    context.add_output_metadata(
        {
            "status": result.status,
            "source_row_count": result.source_row_count,
            "destination_row_count": result.destination_row_count,
            "count_difference": result.count_difference,
            "inserted_rows": result.inserted_rows,
            "updated_rows": result.updated_rows,
            "rejected_rows": result.rejected_rows,
            "staging_row_count": result.staging_row_count,
            "merged_row_count": result.merged_row_count,
            "duplicate_business_key_count": result.duplicate_business_key_count,
            "retry_count": result.retry_count,
            "bigquery_job_id": result.bigquery_job_id or "",
            "partition_statuses": MetadataValue.json(result.partition_statuses),
            "error_details": result.error_details or "",
        }
    )
    if result.status == "failed":
        raise RuntimeError(result.error_details or "BigQuery synchronization failed.")
    return result


@asset(
    group_name="analytics_exports",
    compute_kind="bigquery",
    description=(
        "Manually launched TSX latest-completed-year OHLCV canary. It remains "
        "independently gated and has no schedule."
    ),
)
def bigquery_tsx_ohlcv_canary(context) -> BigQuerySyncResult:
    result = run_bigquery_tsx_canary()
    context.add_output_metadata(
        {
            "status": result.status,
            "source_row_count": result.source_row_count,
            "destination_row_count": result.destination_row_count,
            "count_difference": result.count_difference,
            "staging_row_count": result.staging_row_count,
            "merged_row_count": result.merged_row_count,
            "inserted_rows": result.inserted_rows,
            "updated_rows": result.updated_rows,
            "rejected_rows": result.rejected_rows,
            "duplicate_business_key_count": result.duplicate_business_key_count,
            "retry_count": result.retry_count,
            "bigquery_job_id": result.bigquery_job_id or "",
            "partition_statuses": MetadataValue.json(result.partition_statuses),
            "error_details": result.error_details or "",
        }
    )
    if result.status in {"failed", "gated"}:
        raise RuntimeError(
            result.error_details
            or "BigQuery TSX canary is gated or failed reconciliation."
        )
    return result


@asset(
    group_name="exchange_calendars",
    compute_kind="python",
    description="Materialize validated NSE sessions for history, current year, and next year.",
)
def nse_exchange_sessions(context) -> PipelineRunResult:
    result = run_exchange_session_materialization_pipeline("NSE", trigger="dagster")
    context.add_output_metadata(_result_metadata(result))
    _assert_pipeline_not_failed(result)
    return result


@asset(
    group_name="exchange_calendars",
    compute_kind="python",
    description="Materialize validated TSX sessions for history, current year, and next year.",
)
def tsx_exchange_sessions(context) -> PipelineRunResult:
    result = run_exchange_session_materialization_pipeline("TSX", trigger="dagster")
    context.add_output_metadata(_result_metadata(result))
    _assert_pipeline_not_failed(result)
    return result


@asset(
    group_name="exchange_calendars",
    compute_kind="python",
    description="Materialize validated US sessions for history, current year, and next year.",
)
def us_exchange_sessions(context) -> PipelineRunResult:
    result = run_exchange_session_materialization_pipeline("US", trigger="dagster")
    context.add_output_metadata(_result_metadata(result))
    _assert_pipeline_not_failed(result)
    return result


@asset(
    group_name="equity_universes",
    compute_kind="http",
    description="Validate, persist, and reconcile the official NSE equity universe.",
)
def nse_universe_snapshot(context) -> PipelineRunResult:
    result = run_equity_universe_snapshot_pipeline("NSE", trigger="dagster")
    context.add_output_metadata(_result_metadata(result))
    return result


@asset(
    group_name="equity_universes",
    compute_kind="http",
    description="Validate, persist, and reconcile the TSX equity universe.",
)
def tsx_universe_snapshot(context) -> PipelineRunResult:
    result = run_equity_universe_snapshot_pipeline("TSX", trigger="dagster")
    context.add_output_metadata(_result_metadata(result))
    return result


@asset(
    group_name="equity_universes",
    compute_kind="http",
    description="Validate, persist, and reconcile the US equity universe.",
)
def us_universe_snapshot(context) -> PipelineRunResult:
    result = run_equity_universe_snapshot_pipeline("US", trigger="dagster")
    context.add_output_metadata(_result_metadata(result))
    return result


@asset(
    group_name="yfinance_daily_queue",
    compute_kind="python",
    description="Plan prioritized incremental Yahoo daily work for enabled exchanges.",
)
def yfinance_daily_work_plan(context) -> PipelineRunResult:
    result = run_yfinance_daily_work_planner(
        include_initial_backfill=False,
        trigger="dagster",
    )
    context.add_output_metadata(_result_metadata(result))
    return result


@asset(
    group_name="yfinance_daily_queue",
    compute_kind="python",
    description="Plan NSE Yahoo work after the provider grace period for the completed session.",
)
def yfinance_nse_completed_session_work_plan(context) -> PipelineRunResult:
    result = run_yfinance_daily_work_planner(
        exchanges=("NSE",),
        include_initial_backfill=False,
        trigger="dagster",
    )
    context.add_output_metadata(_result_metadata(result))
    return result


@asset(
    group_name="yfinance_daily_queue",
    compute_kind="yfinance",
    description="Claim and execute one bounded batch of durable Yahoo daily work.",
)
def yfinance_daily_work_worker(context) -> PipelineRunResult:
    result = run_yfinance_daily_work_queue(trigger="dagster")
    context.add_output_metadata(_result_metadata(result))
    if result.status != "pass":
        raise RuntimeError(
            "Yfinance durable worker completed with business or operational failures: "
            "; ".join(result.warnings or [f"pipeline status={result.status}"])
        )
    return result


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
    description=(
        "Provider-neutral NSE daily OHLCV boundary. It uses Upstox by default and "
        "requires the Yahoo overlap gate before cutover."
    ),
)
def nse_daily_ohlcv(context) -> PipelineRunResult:
    latest = resolve_latest_expected_trading_date()
    result = run_nse_daily_ohlcv_primary_pipeline(
        to_date=latest.latest_expected_trading_date.isoformat(),
        trigger="dagster",
    )
    context.add_output_metadata(_result_metadata(result))
    _assert_pipeline_not_failed(result)
    return result


@asset(
    group_name="north_america_daily",
    compute_kind="yfinance",
    description="Incrementally fetch seeded US daily OHLCV from yfinance into TimescaleDB.",
)
def yfinance_us_daily_ohlcv(context) -> PipelineRunResult:
    result = run_yfinance_daily_ohlcv_pipeline(
        universe="us_seed",
        store_db=True,
        export_db_snapshot=True,
        trigger="dagster",
    )
    context.add_output_metadata(_result_metadata(result))
    return result


@asset(
    group_name="north_america_daily",
    compute_kind="yfinance",
    description="Incrementally fetch seeded Canada daily OHLCV from yfinance into TimescaleDB.",
)
def yfinance_canada_daily_ohlcv(context) -> PipelineRunResult:
    result = run_yfinance_daily_ohlcv_pipeline(
        universe="canada_seed",
        store_db=True,
        export_db_snapshot=True,
        trigger="dagster",
    )
    context.add_output_metadata(_result_metadata(result))
    return result


@asset(
    group_name="opportunity_analytics",
    compute_kind="python",
    ins={"daily_ohlcv": AssetIn("nse_daily_ohlcv")},
    description=(
        "Compute completed-session NSE Opportunity variables from OHLC and previous close."
    ),
)
def nse_opportunity_targets_v1(
    context,
    daily_ohlcv: PipelineRunResult,
) -> PipelineRunResult:
    _assert_upstream_not_failed(daily_ohlcv)
    result = run_opportunity_target_pipeline(
        exchange="NSE",
        ohlcv_source="yfinance",
    )
    context.add_output_metadata(_result_metadata(result))
    return result


@asset(
    group_name="opportunity_analytics",
    compute_kind="python",
    description=(
        "Coverage-gated incremental NSE Opportunity refresh for the latest completed session."
    ),
)
def nse_completed_session_opportunity_targets(context) -> PipelineRunResult:
    result = run_completed_session_opportunity_target_pipeline(
        exchange="NSE",
        ohlcv_source="yfinance",
    )
    context.add_output_metadata(_result_metadata(result))
    return result


@asset(
    group_name="opportunity_analytics",
    compute_kind="python",
    description=(
        "Coverage-gated incremental TSX Opportunity refresh for the latest completed session."
    ),
)
def tsx_completed_session_opportunity_targets(context) -> PipelineRunResult:
    result = run_completed_session_opportunity_target_pipeline(
        exchange="TSX",
        ohlcv_source="yfinance",
    )
    context.add_output_metadata(_result_metadata(result))
    return result


@asset(
    group_name="opportunity_analytics",
    compute_kind="python",
    description=(
        "Coverage-gated incremental US Opportunity refresh for the latest completed session."
    ),
)
def us_completed_session_opportunity_targets(context) -> PipelineRunResult:
    result = run_completed_session_opportunity_target_pipeline(
        exchange="US",
        ohlcv_source="yfinance",
    )
    context.add_output_metadata(_result_metadata(result))
    return result


@asset(
    group_name="opportunity_analytics",
    compute_kind="python",
    ins={"daily_ohlcv": AssetIn("yfinance_canada_daily_ohlcv")},
    description=(
        "Compute completed-session TSX Opportunity variables from OHLC and previous close."
    ),
)
def tsx_opportunity_targets_v1(
    context,
    daily_ohlcv: PipelineRunResult,
) -> PipelineRunResult:
    _assert_upstream_not_failed(daily_ohlcv)
    result = run_opportunity_target_pipeline(exchange="TSX", ohlcv_source="yfinance")
    context.add_output_metadata(_result_metadata(result))
    return result


@asset(
    group_name="opportunity_analytics",
    compute_kind="python",
    ins={"daily_ohlcv": AssetIn("yfinance_us_daily_ohlcv")},
    description=(
        "Compute completed-session US Opportunity variables from OHLC and previous close."
    ),
)
def us_opportunity_targets_v1(
    context,
    daily_ohlcv: PipelineRunResult,
) -> PipelineRunResult:
    _assert_upstream_not_failed(daily_ohlcv)
    result = run_opportunity_target_pipeline(exchange="US", ohlcv_source="yfinance")
    context.add_output_metadata(_result_metadata(result))
    return result


@asset(
    group_name="fx_intraday",
    compute_kind="dukascopy",
    description="Fetch Dukascopy 5-minute FX/crypto OHLCV into TimescaleDB.",
)
def dukascopy_fx_intraday_ohlcv(context) -> PipelineRunResult:
    result = run_dukascopy_intraday_ohlcv_pipeline(
        store_db=True,
        trigger="dagster",
    )
    context.add_output_metadata(_result_metadata(result))
    return result


@asset(
    group_name="fx_intraday",
    compute_kind="yfinance",
    description="Fetch yfinance 5-minute FX/crypto OHLCV into TimescaleDB.",
)
def yfinance_fx_crypto_intraday_ohlcv(context) -> PipelineRunResult:
    result = run_yfinance_intraday_ohlcv_pipeline(
        store_db=True,
        trigger="dagster",
    )
    context.add_output_metadata(_result_metadata(result))
    return result


@asset(
    group_name="fx_intraday",
    compute_kind="python",
    ins={"intraday_ohlcv": AssetIn("dukascopy_fx_intraday_ohlcv")},
    description="Validate Dukascopy 5-minute intraday gaps by instrument.",
)
def fx_intraday_gap_validation(
    context,
    intraday_ohlcv: PipelineRunResult,
) -> PipelineRunResult:
    _assert_upstream_not_failed(intraday_ohlcv)
    input_path = intraday_ohlcv.artifacts.get("ohlcv")
    if input_path is None:
        result = PipelineRunResult(
            name="dukascopy_fx_crypto_5m_gap_validation",
            status="fail",
            blocking_issues=["Dukascopy fetch produced no OHLCV artifact to validate."],
        )
        context.add_output_metadata(_result_metadata(result))
        return result
    result = run_dukascopy_intraday_gap_validation_pipeline(
        input_path=input_path,
    )
    context.add_output_metadata(_result_metadata(result))
    return result


@asset(
    group_name="fx_intraday",
    compute_kind="python",
    ins={"intraday_ohlcv": AssetIn("yfinance_fx_crypto_intraday_ohlcv")},
    description="Validate yfinance 5-minute intraday gaps by instrument.",
)
def yfinance_fx_intraday_gap_validation(
    context,
    intraday_ohlcv: PipelineRunResult,
) -> PipelineRunResult:
    _assert_upstream_not_failed(intraday_ohlcv)
    input_path = intraday_ohlcv.artifacts.get("ohlcv")
    if input_path is None:
        result = PipelineRunResult(
            name="yfinance_fx_crypto_5m_gap_validation",
            status="fail",
            blocking_issues=["yfinance fetch produced no OHLCV artifact to validate."],
        )
        context.add_output_metadata(_result_metadata(result))
        return result
    result = run_dukascopy_intraday_gap_validation_pipeline(
        input_path=input_path,
        input_name="processed/intraday/yfinance_fx_crypto_5m_5m_ohlcv",
        dataset_name="yfinance_fx_crypto_5m_gap_validation",
        provider_label="yfinance",
        output_path=Path("data/processed/intraday/yfinance_fx_crypto_5m_gap_validation.csv"),
        summary_output=Path(
            "data/processed/intraday/yfinance_fx_crypto_5m_gap_validation_summary.json"
        ),
    )
    context.add_output_metadata(_result_metadata(result))
    return result


@asset(
    group_name="daily_research",
    compute_kind="python",
    ins={
        "daily_ohlcv": AssetIn("nse_daily_ohlcv"),
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
    result = run_processed_dataset_validation_pipeline(coverage_run_id=context.run_id)
    context.add_output_metadata(_result_metadata(result))
    return result


@asset(
    group_name="daily_research",
    compute_kind="python",
    ins={"daily_ohlcv": AssetIn("nse_daily_ohlcv")},
    description="Build the frozen daily technical feature layer from daily OHLCV.",
)
def daily_features_v1(
    context,
    daily_ohlcv: PipelineRunResult,
) -> PipelineRunResult:
    _assert_upstream_not_failed(daily_ohlcv)
    result = run_daily_feature_pipeline(
        input_source="timescale",
        ohlcv_source=get_settings().nse_daily_primary_source,
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
    ins={"daily_ohlcv": AssetIn("nse_daily_ohlcv")},
    description="Build the frozen daily forward-return target layer from daily OHLCV.",
)
def daily_targets_v1(
    context,
    daily_ohlcv: PipelineRunResult,
) -> PipelineRunResult:
    _assert_upstream_not_failed(daily_ohlcv)
    result = run_daily_target_pipeline(
        input_source="timescale",
        ohlcv_source=get_settings().nse_daily_primary_source,
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


def _assert_pipeline_not_failed(result: PipelineRunResult) -> None:
    if result.status == "fail":
        raise RuntimeError(f"Pipeline failed: {result.name}")


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
