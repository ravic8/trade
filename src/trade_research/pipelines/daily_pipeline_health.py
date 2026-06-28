from __future__ import annotations

from pathlib import Path
from typing import Any

from trade_research.config import get_settings
from trade_research.pipelines.base import PipelineRunResult
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
