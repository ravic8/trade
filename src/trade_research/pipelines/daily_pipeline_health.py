from __future__ import annotations

from pathlib import Path
from typing import Any

from trade_research.config import get_settings
from trade_research.pipelines.base import PipelineRunResult
from trade_research.pipelines.nse_cutover import run_nse_yfinance_cutover_readiness
from trade_research.pipelines.processed_validation import (
    run_processed_dataset_validation_pipeline,
)
from trade_research.validation import validate_daily_pipeline_health


def run_daily_pipeline_health_pipeline(
    run_live_fetch: bool = False,
    run_factor_research: bool = True,
    rebuild_artifacts: bool = True,
    coverage_run_id: str | None = None,
    store_coverage_db: bool = False,
    coverage_windows_months: list[int] | None = None,
    data_dir: Path | str | None = None,
) -> PipelineRunResult:
    settings = get_settings()
    root = Path(data_dir or settings.data_dir)
    if settings.nse_daily_primary_source == "yfinance":
        readiness = run_nse_yfinance_cutover_readiness(trigger="daily_health")
        processed = run_processed_dataset_validation_pipeline(data_dir=root)
        blocking = [*readiness.blocking_issues, *processed.blocking_issues]
        warnings = [*readiness.warnings, *processed.warnings]
        return PipelineRunResult(
            name="daily_pipeline_health",
            status="fail" if blocking else ("warn" if warnings else "pass"),
            rows=processed.rows,
            artifacts={
                "health_report": processed.artifacts["summary_md"],
                "health_json": processed.artifacts["summary_json"],
            },
            metrics={
                "primary_source": "yfinance",
                "cutover_readiness": readiness.metrics,
                "processed_validation": processed.metrics,
                "blocking_issues": blocking,
                "warnings": warnings,
            },
            warnings=warnings,
            blocking_issues=blocking,
        )
    result = validate_daily_pipeline_health(
        data_dir=root,
        run_live_fetch=run_live_fetch,
        run_factor_research=run_factor_research,
        rebuild_artifacts=rebuild_artifacts,
        coverage_run_id=coverage_run_id,
        store_coverage_db=store_coverage_db,
        coverage_windows_months=coverage_windows_months,
    )
    summary: dict[str, Any] = result.summary
    return PipelineRunResult(
        name="daily_pipeline_health",
        status=str(summary["overall_status"]),
        rows=int(summary.get("row_counts", {}).get("cleaned_ohlcv", 0) or 0),
        artifacts={
            "health_report": result.report_path,
            "health_json": result.json_path,
        },
        metrics=summary,
        warnings=list(summary.get("warnings", [])),
        blocking_issues=list(summary.get("blocking_issues", [])),
    )
