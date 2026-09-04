from __future__ import annotations

from pathlib import Path
from typing import Any

import pandas as pd

from trade_research.config import get_settings
from trade_research.pipelines.base import PipelineRunResult
from trade_research.validation import validate_processed_datasets
from trade_research.validation.daily_pipeline import write_stock_coverage
from trade_research.validation.processed_datasets import (
    CLEANED_OHLCV,
    VALIDATION_RESULTS,
    normalize_ohlcv,
)


def run_processed_dataset_validation_pipeline(
    pass_coverage_threshold: float = 0.90,
    warn_coverage_threshold: float = 0.70,
    data_dir: Path | str | None = None,
    coverage_run_id: str | None = None,
) -> PipelineRunResult:
    settings = get_settings()
    root = Path(data_dir or settings.data_dir)
    result = validate_processed_datasets(
        data_dir=root,
        pass_coverage_threshold=pass_coverage_threshold,
        warn_coverage_threshold=warn_coverage_threshold,
        processed_ohlcv=(
            "processed/equities/nse_daily_ohlcv_yfinance.parquet"
            if settings.nse_daily_primary_source == "yfinance"
            else "processed/equities/nse_daily_ohlcv_upstox.parquet"
        ),
        run_id=coverage_run_id,
    )
    summary: dict[str, Any] = result.summary
    coverage = _write_ml_stock_coverage(
        root,
        coverage_run_id=coverage_run_id,
    )
    return PipelineRunResult(
        name="processed_dataset_validation",
        status=str(summary["overall_status"]),
        rows=int(summary.get("row_counts", {}).get("cleaned_ohlcv", 0) or 0),
        artifacts={
            "summary_md": root / "processed/validation/processed_dataset_validation_summary.md",
            "summary_json": root / "processed/validation/processed_dataset_validation_summary.json",
            "validation_results": root / VALIDATION_RESULTS,
            "stock_coverage": Path(coverage["path"]),
            "stock_coverage_windows": Path(coverage["windows_path"]),
        },
        metrics={**summary, "stock_coverage": coverage["summary"]},
        warnings=[
            *list(summary.get("warnings", [])),
            *list(coverage.get("warnings", [])),
        ],
        blocking_issues=list(summary.get("blocking_issues", [])),
    )


def _write_ml_stock_coverage(
    root: Path,
    *,
    coverage_run_id: str | None,
) -> dict[str, Any]:
    cleaned_path = root / CLEANED_OHLCV
    if not cleaned_path.exists():
        raise FileNotFoundError(
            "Processed validation did not produce the cleaned OHLCV artifact required "
            f"for stock coverage: {cleaned_path}"
        )
    cleaned = normalize_ohlcv(pd.read_parquet(cleaned_path))
    latest_stored_date = cleaned["date"].dropna().max() if not cleaned.empty else None
    if latest_stored_date is None:
        raise ValueError("Cleaned OHLCV is empty; stock coverage cannot be materialized.")
    return write_stock_coverage(
        root,
        cleaned,
        latest_stored_date,
        coverage_run_id=coverage_run_id,
        store_coverage_db=False,
        coverage_windows_months=[6, 9, 12, 15, 18, 24],
    )
