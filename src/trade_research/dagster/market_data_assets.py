from __future__ import annotations

from typing import Any

from dagster import Bool, Field, Int, MetadataValue, String, asset

from trade_research.config import get_settings
from trade_research.market_data.partition_reconciliation import (
    run_nse_daily_partition_reconciliation,
)
from trade_research.market_data.readiness import Phase3ReadinessRepository
from trade_research.pipelines import (
    PipelineRunResult,
    run_nse_yfinance_cutover_readiness,
    run_yfinance_nse_minute_pipeline,
)
from trade_research.storage import TimescaleStore


@asset(
    group_name="nse_market_data",
    compute_kind="yfinance",
    config_schema={
        "symbol_limit": Field(Int, is_required=False),
        "symbols": Field(String, is_required=False),
        "from_datetime": Field(String, is_required=False),
        "to_datetime": Field(String, is_required=False),
    },
    description=(
        "Fetch bounded NSE 1m data, retain immutable raw evidence, validate completed "
        "sessions, and replicate accepted candles to ClickHouse."
    ),
)
def yfinance_nse_minute_ohlcv(context) -> PipelineRunResult:
    symbols = context.op_config.get("symbols")
    result = run_yfinance_nse_minute_pipeline(
        from_datetime=context.op_config.get("from_datetime"),
        to_datetime=context.op_config.get("to_datetime"),
        symbol_limit=context.op_config.get("symbol_limit"),
        provider_symbols=(
            [value.strip() for value in symbols.split(",") if value.strip()] if symbols else None
        ),
        trigger="dagster",
        at=getattr(context, "scheduled_execution_time", None),
    )
    context.add_output_metadata(
        {
            "status": result.status,
            "rows": result.rows,
            "eligible_sessions": result.metrics["eligible_sessions"],
            "selected_symbols": result.metrics["selected_symbols"],
            "raw_rows": result.metrics["raw_rows"],
            "validated_rows": result.metrics["validated_rows"],
            "clickhouse_rows": result.metrics["clickhouse_rows"],
            "failure_rows": result.metrics["failure_rows"],
            "window_start": result.metrics["window_start"],
            "window_end": result.metrics["window_end"],
            "missing_rows": result.metrics.get("missing_rows", 0),
            "missing_rows_by_symbol": MetadataValue.json(
                result.metrics.get("missing_rows_by_symbol", {})
            ),
            "provider_unavailable_rows": result.metrics.get(
                "provider_unavailable_rows", 0
            ),
            "run_id": str(result.metrics.get("run_id") or ""),
            "raw_snapshot_uri": result.metrics["raw_snapshot_uri"] or "",
        }
    )
    if result.business_outcome != "succeeded":
        raise RuntimeError("NSE minute ingestion completed with degraded or failed status.")
    return result


@asset(
    group_name="nse_market_data",
    compute_kind="readiness",
    config_schema={
        "daily_run_id": Field(String),
        "minute_run_id": Field(String),
        "minute_rerun_id": Field(String),
    },
    description=(
        "Assess bounded production daily and minute canaries from durable source-run "
        "evidence without changing Phase 3 activation."
    ),
)
def phase3_bounded_canary_assessment(context) -> dict[str, Any]:
    settings = get_settings()
    store = TimescaleStore(settings.database_url)
    store.initialize()
    evidence = Phase3ReadinessRepository(store.engine).assess_canary(
        daily_run_id=context.op_config["daily_run_id"],
        minute_run_id=context.op_config["minute_run_id"],
        minute_rerun_id=context.op_config["minute_rerun_id"],
        max_instruments=settings.phase3_canary_max_instruments,
        minimum_completeness=settings.phase3_minimum_completeness,
        required_observed_sessions=settings.phase3_required_observed_sessions,
    )
    context.add_output_metadata(
        {
            "status": evidence["status"],
            "evidence_id": evidence["evidence_id"],
            "source_run_ids": MetadataValue.json(evidence["source_run_ids"]),
            "session_dates": MetadataValue.json(evidence["session_dates"]),
            "blocking_issues": MetadataValue.json(evidence["blocking_issues"]),
            "evidence_refs": MetadataValue.json(evidence["evidence_refs"]),
        }
    )
    if evidence["status"] != "pass":
        details = "; ".join(evidence["blocking_issues"]) or "no blocking details returned"
        raise RuntimeError(f"Phase 3 bounded canary assessment failed: {details}")
    return evidence


@asset(
    group_name="nse_market_data",
    compute_kind="clickhouse",
    config_schema={
        "month": Field(String),
        "repair": Field(Bool, default_value=False),
        "max_rows": Field(Int, default_value=100_000),
    },
    description=(
        "Manually audit one monthly NSE yfinance daily partition against "
        "PostgreSQL authority and optionally upsert missing or divergent rows."
    ),
)
def nse_daily_clickhouse_partition_reconciliation(context):
    config = context.op_config
    result = run_nse_daily_partition_reconciliation(
        month=config["month"],
        repair=config["repair"],
        max_rows=config["max_rows"],
        source_run_id=context.run_id,
        at=context.scheduled_execution_time,
    )
    context.add_output_metadata(
        {
            "partition": result.partition,
            "status": result.status,
            "repair_requested": result.repair_requested,
            "repaired_rows": result.repaired_rows,
            "source_rows": result.final.source_row_count,
            "destination_rows": result.final.destination_row_count,
            "missing_rows": len(result.final.missing_identities),
            "divergent_rows": len(result.final.divergent_identities),
            "unexpected_rows": len(result.final.unexpected_identities),
            "replication_checkpoint_id": result.replication_checkpoint_id,
        }
    )
    if result.status != "reconciled":
        raise RuntimeError(
            "NSE daily ClickHouse partition remains mismatched; review the durable "
            "replication checkpoint before retrying."
        )
    return result


@asset(
    group_name="nse_market_data",
    compute_kind="provider_comparison",
    description=(
        "Record one audited NSE Upstox-vs-yfinance provider comparison window for "
        "Phase 3 production readiness."
    ),
)
def nse_yfinance_provider_comparison(context) -> PipelineRunResult:
    result = run_nse_yfinance_cutover_readiness(
        trigger="dagster",
        at=getattr(context, "scheduled_execution_time", None),
    )
    metrics = result.metrics
    context.add_output_metadata(
        {
            "status": result.status,
            "rows": result.rows,
            "comparison_state": metrics.get("comparison_state", "unavailable"),
            "window_start": metrics.get("window_start", ""),
            "window_end": metrics.get("window_end", ""),
            "overlapping_symbols": metrics.get("overlapping_symbols", 0),
            "row_overlap_ratio": metrics.get("row_overlap_ratio", 0),
            "close_match_ratio": metrics.get("close_match_ratio", 0),
            "missing_yfinance_rows": metrics.get("missing_yfinance_rows", 0),
            "missing_upstox_rows": metrics.get("missing_upstox_rows", 0),
            "missing_yfinance_symbols": metrics.get("missing_yfinance_symbols", 0),
            "missing_upstox_symbols": metrics.get("missing_upstox_symbols", 0),
            "missing_yfinance_rows_by_session": MetadataValue.json(
                metrics.get("missing_yfinance_rows_by_session", {})
            ),
            "missing_upstox_rows_by_session": MetadataValue.json(
                metrics.get("missing_upstox_rows_by_session", {})
            ),
            "missing_yfinance_row_samples": MetadataValue.json(
                metrics.get("missing_yfinance_row_samples", [])
            ),
            "missing_upstox_row_samples": MetadataValue.json(
                metrics.get("missing_upstox_row_samples", [])
            ),
            "evidence_id": metrics.get("evidence_id", ""),
            "blocking_issues": "\n".join(result.blocking_issues),
        }
    )
    if result.status != "pass":
        details = (
            "; ".join(result.blocking_issues).rstrip(".") or "no blocking details were returned"
        )
        raise RuntimeError(
            "NSE provider comparison did not pass: "
            f"{details}. Review the recorded evidence before retrying."
        )
    return result
