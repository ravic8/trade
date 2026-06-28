from __future__ import annotations

from pathlib import Path
from typing import Any

from trade_research.config import get_settings
from trade_research.pipelines.base import PipelineRunResult
from trade_research.validation import validate_processed_datasets


def run_processed_dataset_validation_pipeline(
    pass_coverage_threshold: float = 0.90,
    warn_coverage_threshold: float = 0.70,
    data_dir: Path | str | None = None,
) -> PipelineRunResult:
    settings = get_settings()
    root = Path(data_dir or settings.data_dir)
    result = validate_processed_datasets(
        data_dir=root,
        pass_coverage_threshold=pass_coverage_threshold,
        warn_coverage_threshold=warn_coverage_threshold,
    )
    summary: dict[str, Any] = result.summary
    return PipelineRunResult(
        name="processed_dataset_validation",
        status=str(summary["overall_status"]),
        rows=int(summary.get("row_counts", {}).get("cleaned_ohlcv", 0) or 0),
        artifacts={
            "summary_md": root / "processed/validation/processed_dataset_validation_summary.md",
            "summary_json": root / "processed/validation/processed_dataset_validation_summary.json",
        },
        metrics=summary,
        warnings=list(summary.get("warnings", [])),
        blocking_issues=list(summary.get("blocking_issues", [])),
    )

