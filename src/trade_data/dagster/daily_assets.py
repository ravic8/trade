from __future__ import annotations

from pathlib import Path
from typing import Any

from dagster import Field, Int, MetadataValue, String, asset

from trade_data.config import get_settings
from trade_data.pipelines import (
    PipelineRunResult,
    run_equity_universe_snapshot_pipeline,
    run_exchange_session_materialization_pipeline,
    run_nse_daily_ohlcv_primary_pipeline,
    run_upstox_daily_ohlcv_pipeline,
    run_yfinance_daily_ohlcv_pipeline,
    run_yfinance_daily_work_planner,
    run_yfinance_daily_work_queue,
    run_yfinance_nse_canary_planner,
)
from trade_data.validation import resolve_latest_expected_trading_date


@asset(
    group_name="exchange_calendars",
    compute_kind="python",
    description="Materialize validated NSE sessions for history, current year, and next year.",
)
def nse_exchange_sessions(context) -> PipelineRunResult:
    result = run_exchange_session_materialization_pipeline("NSE", trigger="dagster")
    _record_pipeline_result(context, result)
    return result


@asset(
    group_name="exchange_calendars",
    compute_kind="python",
    description="Materialize validated TSX sessions for history, current year, and next year.",
)
def tsx_exchange_sessions(context) -> PipelineRunResult:
    result = run_exchange_session_materialization_pipeline("TSX", trigger="dagster")
    _record_pipeline_result(context, result)
    return result


@asset(
    group_name="exchange_calendars",
    compute_kind="python",
    description="Materialize validated US sessions for history, current year, and next year.",
)
def us_exchange_sessions(context) -> PipelineRunResult:
    result = run_exchange_session_materialization_pipeline("US", trigger="dagster")
    _record_pipeline_result(context, result)
    return result


@asset(
    group_name="equity_universes",
    compute_kind="http",
    description="Validate, persist, and reconcile the official NSE equity universe.",
)
def nse_universe_snapshot(context) -> PipelineRunResult:
    result = run_equity_universe_snapshot_pipeline("NSE", trigger="dagster")
    _record_pipeline_result(context, result)
    return result


@asset(
    group_name="equity_universes",
    compute_kind="http",
    description="Validate, persist, and reconcile the TSX equity universe.",
)
def tsx_universe_snapshot(context) -> PipelineRunResult:
    result = run_equity_universe_snapshot_pipeline("TSX", trigger="dagster")
    _record_pipeline_result(context, result)
    return result


@asset(
    group_name="equity_universes",
    compute_kind="http",
    description="Validate, persist, and reconcile the US equity universe.",
)
def us_universe_snapshot(context) -> PipelineRunResult:
    result = run_equity_universe_snapshot_pipeline("US", trigger="dagster")
    _record_pipeline_result(context, result)
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
    _record_pipeline_result(context, result)
    return result


@asset(
    group_name="yfinance_daily_queue",
    compute_kind="python",
    description="Plan NSE Yahoo work after the provider grace period for the completed session.",
)
def yfinance_nse_completed_session_work_plan(context) -> PipelineRunResult:
    settings = get_settings()
    result = run_yfinance_daily_work_planner(
        exchanges=("NSE",),
        include_initial_backfill=False,
        include_gap_repair=True,
        gap_repair_session_count=settings.nse_provider_comparison_sessions,
        trigger="dagster",
    )
    _record_pipeline_result(context, result)
    return result


@asset(
    group_name="nse_market_data",
    compute_kind="yfinance",
    config_schema={
        "symbol_limit": Field(Int, default_value=25),
        "symbols": Field(String, is_required=False),
    },
    description=(
        "Run one bounded NSE Yahoo daily refresh with durable quality and "
        "ClickHouse evidence for Phase 3 readiness."
    ),
)
def yfinance_nse_daily_canary(context) -> PipelineRunResult:
    settings = get_settings()
    symbol_limit = int(context.op_config["symbol_limit"])
    if symbol_limit > settings.phase3_canary_max_instruments:
        raise ValueError(
            "Daily canary symbol limit exceeds the Phase 3 readiness maximum: "
            f"{symbol_limit}>{settings.phase3_canary_max_instruments}."
        )
    symbols = context.op_config.get("symbols")
    provider_symbols = (
        [value.strip() for value in symbols.split(",") if value.strip()]
        if symbols
        else None
    )
    plan = run_yfinance_nse_canary_planner(
        symbol_limit=symbol_limit,
        provider_symbols=provider_symbols,
        enqueue=True,
        force_refresh=True,
        session_count=settings.nse_provider_comparison_sessions,
        run_key=context.run_id,
        trigger="dagster",
    )
    result = run_yfinance_daily_work_queue(
        claim_size=symbol_limit,
        exchange="NSE",
        work_type="bounded_canary",
        trigger="dagster",
    )
    result.metrics.update(
        {
            "canary": True,
            "canary_symbol_limit": symbol_limit,
            "canary_work_generated": plan.metrics.get("work_generated", 0),
            "canary_work_inserted": plan.metrics.get("work_inserted", 0),
        }
    )
    if result.metrics.get("run_id"):
        result.metrics["run_id"] = str(result.metrics["run_id"])
    if not result.metrics.get("run_id"):
        raise RuntimeError(
            "No new bounded daily canary work was available for this session window."
        )
    _record_pipeline_result(context, result, fail_on_degraded=True)
    return result


@asset(
    group_name="yfinance_daily_queue",
    compute_kind="python",
    description="Plan TSX Yahoo work after the provider grace period for the completed session.",
)
def yfinance_tsx_completed_session_work_plan(context) -> PipelineRunResult:
    result = run_yfinance_daily_work_planner(
        exchanges=("TSX",),
        include_initial_backfill=False,
        trigger="dagster",
    )
    _record_pipeline_result(context, result)
    return result


@asset(
    group_name="yfinance_daily_queue",
    compute_kind="python",
    description="Plan US Yahoo work after the provider grace period for the completed session.",
)
def yfinance_us_completed_session_work_plan(context) -> PipelineRunResult:
    result = run_yfinance_daily_work_planner(
        exchanges=("US",),
        include_initial_backfill=False,
        trigger="dagster",
    )
    _record_pipeline_result(context, result)
    return result


@asset(
    group_name="yfinance_daily_queue",
    compute_kind="yfinance",
    description="Claim and execute one bounded batch of durable Yahoo daily work.",
)
def yfinance_daily_work_worker(context) -> PipelineRunResult:
    exchange = get_settings().yfinance_daily_worker_exchange
    result = run_yfinance_daily_work_queue(
        trigger="dagster", **({"exchange": exchange} if exchange else {})
    )
    _record_pipeline_result(context, result, fail_on_degraded=True)
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
    _record_pipeline_result(context, result, fail_on_degraded=True)
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
    _record_pipeline_result(context, result, fail_on_degraded=True)
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
    _record_pipeline_result(context, result, fail_on_degraded=True)
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
    _record_pipeline_result(context, result, fail_on_degraded=True)
    return result


def _record_pipeline_result(
    context: Any,
    result: PipelineRunResult,
    *,
    fail_on_degraded: bool = False,
) -> None:
    context.add_output_metadata(_result_metadata(result))
    outcome = result.business_outcome
    details = result.blocking_issues or result.warnings
    suffix = ": " + "; ".join(details) if details else ""
    if outcome == "failed":
        context.log.error("Pipeline failed: %s%s", result.name, suffix)
        raise RuntimeError(f"Pipeline failed: {result.name}{suffix}")
    if outcome == "degraded" and fail_on_degraded:
        context.log.error("Pipeline degraded: %s%s", result.name, suffix)
        raise RuntimeError(f"Pipeline degraded: {result.name}{suffix}")
    if outcome == "degraded":
        details = result.warnings or ["Pipeline completed with warnings."]
        context.log.warning(
            "Pipeline %s finished with degraded business outcome: %s",
            result.name,
            "; ".join(details),
        )


def _result_metadata(result: PipelineRunResult) -> dict[str, Any]:
    metadata: dict[str, Any] = {
        "pipeline": result.name,
        "status": result.status,
        "business_outcome": result.business_outcome,
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
