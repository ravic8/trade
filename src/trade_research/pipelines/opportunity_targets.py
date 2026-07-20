from __future__ import annotations

from dataclasses import asdict
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any

from trade_research.config import get_settings
from trade_research.features import invalid_daily_ohlcv_mask, normalize_daily_ohlcv
from trade_research.pipelines.base import PipelineRunResult
from trade_research.storage import ParquetStore, TimescaleStore
from trade_research.targets import (
    DAILY_OPPORTUNITY_TARGET_VERSION_V1_0,
    DailyOpportunityTargetBuilder,
    audit_daily_opportunity_targets,
    write_opportunity_target_audit_outputs,
)


def run_opportunity_target_pipeline(
    *,
    exchange: str,
    ohlcv_source: str = "yfinance",
    target_version: str = DAILY_OPPORTUNITY_TARGET_VERSION_V1_0,
    store_db: bool = True,
    incremental: bool = True,
    replace_exchange: bool = False,
    recompute_lookback_days: int = 90,
    limit: int | None = None,
) -> PipelineRunResult:
    exchange_code = exchange.upper()
    if exchange_code not in {"NSE", "TSX", "US"}:
        raise ValueError("exchange must be NSE, TSX, or US")
    if replace_exchange and incremental:
        raise ValueError("replace_exchange requires a non-incremental run")

    settings = get_settings()
    started_at = datetime.now(UTC)
    db = TimescaleStore(settings.database_url)
    db.initialize()
    recompute_start = None
    if incremental:
        latest = db.latest_daily_opportunity_target_date(
            target_version,
            exchange=exchange_code,
            source=ohlcv_source,
        )
        if latest is not None:
            recompute_start = latest - timedelta(days=max(recompute_lookback_days, 7))

    source_frame = db.daily_ohlcv_frame(
        exchange=exchange_code,
        source=ohlcv_source,
        limit=limit,
        start_date=recompute_start,
    )
    if source_frame.empty:
        raise ValueError(
            f"No {ohlcv_source} daily OHLCV rows found for {exchange_code}."
        )

    normalized = normalize_daily_ohlcv(source_frame)
    invalid_ohlcv_count = int(invalid_daily_ohlcv_mask(normalized).sum())
    targets = DailyOpportunityTargetBuilder(
        target_version=target_version,
        drop_invalid_rows=True,
    ).build(source_frame)
    audit, summary = audit_daily_opportunity_targets(
        targets,
        target_version=target_version,
        invalid_ohlcv_count=invalid_ohlcv_count,
    )

    deleted_rows = 0
    if store_db:
        if replace_exchange:
            deleted_rows, db_rows = db.replace_daily_opportunity_targets(
                targets,
                target_version=target_version,
                exchange=exchange_code,
                source=ohlcv_source,
            )
        else:
            db_rows = db.upsert_daily_opportunity_targets(targets)
        run_id = db.insert_target_run(
            asdict(summary),
            source=ohlcv_source,
            started_at=started_at,
        )
        audit_rows = db.insert_target_audits(
            audit,
            dataset_name=summary.dataset_name,
            target_version=target_version,
            run_id=run_id,
        )
    else:
        db_rows = 0
        run_id = None
        audit_rows = 0

    output_prefix = f"processed/opportunities/{exchange_code.lower()}_daily_targets"
    output_path = ParquetStore(settings.data_dir).write_frame(output_prefix, targets)
    audit_output = Path(
        f"data/processed/opportunities/{exchange_code.lower()}_daily_targets_audit.csv"
    )
    summary_output = Path(
        f"data/processed/opportunities/{exchange_code.lower()}_daily_targets_summary.json"
    )
    write_opportunity_target_audit_outputs(
        audit,
        summary,
        audit_output,
        summary_output,
    )

    metrics: dict[str, Any] = {
        **asdict(summary),
        "exchange": exchange_code,
        "ohlcv_source": ohlcv_source,
        "timescale_rows": db_rows,
        "timescale_deleted_rows": deleted_rows,
        "timescale_audit_rows": audit_rows,
        "timescale_run_id": run_id,
        "incremental": incremental,
        "recompute_start": recompute_start.isoformat() if recompute_start else None,
    }
    return PipelineRunResult(
        name=f"{exchange_code.lower()}_opportunity_targets",
        status="pass" if summary.failed_rows == 0 else "warn",
        rows=len(targets),
        artifacts={
            "targets": output_path,
            "target_audit": audit_output,
            "target_summary": summary_output,
        },
        metrics=metrics,
        warnings=(
            [f"Excluded invalid OHLCV rows: {invalid_ohlcv_count}"]
            if invalid_ohlcv_count
            else []
        ),
    )
