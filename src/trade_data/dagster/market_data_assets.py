from __future__ import annotations

from dagster import Bool, Field, Int, MetadataValue, String, asset

from trade_data.market_data.partition_reconciliation import (
    run_nse_daily_partition_reconciliation,
)
from trade_data.pipelines import (
    PipelineRunResult,
    run_nse_yfinance_cutover_readiness,
)


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
