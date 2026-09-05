from __future__ import annotations

from dagster import Field, Int, asset

from trade_research.pipelines import PipelineRunResult, run_yfinance_nse_minute_pipeline


@asset(
    group_name="nse_market_data",
    compute_kind="yfinance",
    config_schema={
        "symbol_limit": Field(Int, is_required=False),
    },
    description=(
        "Fetch bounded NSE 1m data, retain immutable raw evidence, validate completed "
        "sessions, and replicate accepted candles to ClickHouse."
    ),
)
def yfinance_nse_minute_ohlcv(context) -> PipelineRunResult:
    result = run_yfinance_nse_minute_pipeline(
        symbol_limit=context.op_config.get("symbol_limit"),
        trigger="dagster",
        at=context.scheduled_execution_time,
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
            "raw_snapshot_uri": result.metrics["raw_snapshot_uri"] or "",
        }
    )
    if result.business_outcome != "succeeded":
        raise RuntimeError("NSE minute ingestion completed with degraded or failed status.")
    return result
