from __future__ import annotations

from dataclasses import asdict
from pathlib import Path
from typing import Any

from trade_research.config import get_settings
from trade_research.features import FEATURE_VERSION_V1_0
from trade_research.pipelines.base import PipelineRunResult
from trade_research.research import DailyFactorResearchBuilder, write_factor_research_outputs
from trade_research.storage import ParquetStore
from trade_research.targets import DAILY_FORWARD_TARGET_VERSION_V1_0


def run_factor_research_pipeline(
    feature_name: str = "processed/features/daily_v1_ohlcv_technical",
    target_name: str = "processed/targets/daily_v1_forward_returns",
    output_dir: Path = Path("data/processed/research/factors"),
    feature_version: str = FEATURE_VERSION_V1_0,
    target_version: str = DAILY_FORWARD_TARGET_VERSION_V1_0,
    quantiles: int = 5,
    min_date_rows: int = 5,
    min_month_rows: int = 20,
) -> PipelineRunResult:
    settings = get_settings()
    store = ParquetStore(settings.data_dir)
    features = store.read_frame(feature_name)
    targets = store.read_frame(target_name)
    builder = DailyFactorResearchBuilder(
        feature_version=feature_version,
        target_version=target_version,
        quantiles=quantiles,
        min_date_rows=min_date_rows,
        min_month_rows=min_month_rows,
    )
    ic, quantile_table, hit_rates, monthly, summary = builder.build(features, targets)
    paths = write_factor_research_outputs(
        ic,
        quantile_table,
        hit_rates,
        monthly,
        summary,
        output_dir,
    )
    summary_dict: dict[str, Any] = asdict(summary)
    return PipelineRunResult(
        name="factor_research",
        status="pass",
        rows=summary.row_count,
        artifacts={name: Path(path) for name, path in paths.items()},
        metrics=summary_dict,
    )

