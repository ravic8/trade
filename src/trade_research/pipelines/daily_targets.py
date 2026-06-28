from __future__ import annotations

from dataclasses import asdict
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any

from trade_research.config import get_settings
from trade_research.features import invalid_daily_ohlcv_mask, normalize_daily_ohlcv
from trade_research.pipelines.base import PipelineRunResult
from trade_research.pipelines.common import limit_daily_symbols
from trade_research.storage import ParquetStore, TimescaleStore
from trade_research.targets import (
    DAILY_FORWARD_TARGET_VERSION_V1_0,
    DailyForwardTargetBuilder,
    audit_daily_forward_targets,
    write_target_audit_outputs,
)


def run_daily_target_pipeline(
    input_source: str = "parquet",
    input_name: str = "processed/equities/nse_daily_ohlcv_upstox",
    output_name: str = "processed/targets/daily_v1_forward_returns",
    target_version: str = DAILY_FORWARD_TARGET_VERSION_V1_0,
    audit_output: Path = Path("data/processed/targets/daily_v1_forward_returns_audit.csv"),
    summary_output: Path = Path("data/processed/targets/daily_v1_forward_returns_summary.json"),
    limit: int | None = None,
    strict_invalid_rows: bool = False,
    store_db: bool = False,
    incremental: bool = False,
    recompute_lookback_days: int = 90,
    export_db_snapshot: bool = True,
) -> PipelineRunResult:
    settings = get_settings()
    started_at = datetime.now(UTC)
    normalized_source = input_source.lower()
    if normalized_source not in {"parquet", "timescale"}:
        raise ValueError("input_source must be parquet or timescale.")

    db: TimescaleStore | None = None
    recompute_start = None
    if normalized_source == "parquet":
        source_frame = ParquetStore(settings.data_dir).read_frame(input_name)
        source_frame = limit_daily_symbols(source_frame, limit)
    else:
        db = TimescaleStore(settings.database_url)
        if incremental:
            latest_target_date = db.latest_daily_target_date(target_version)
            if latest_target_date is not None:
                recompute_start = latest_target_date - timedelta(days=recompute_lookback_days)
        source_frame = db.daily_ohlcv_frame(limit=limit, start_date=recompute_start)

    if source_frame.empty:
        raise ValueError("No daily OHLCV rows found for target generation.")

    invalid_ohlcv_count = int(invalid_daily_ohlcv_mask(normalize_daily_ohlcv(source_frame)).sum())
    targets = DailyForwardTargetBuilder(
        target_version=target_version,
        drop_invalid_rows=not strict_invalid_rows,
    ).build(source_frame)

    db_rows = 0
    audit_rows = 0
    run_id: str | None = None
    if store_db:
        db = db or TimescaleStore(settings.database_url)
        db.initialize()
        db_rows = db.upsert_daily_targets(targets)
        if export_db_snapshot:
            targets_for_artifact = db.daily_target_frame(target_version=target_version, limit=limit)
        else:
            targets_for_artifact = targets
    else:
        targets_for_artifact = targets

    audit, summary = audit_daily_forward_targets(
        targets_for_artifact,
        target_version=target_version,
        invalid_ohlcv_count=invalid_ohlcv_count,
    )
    output_path = ParquetStore(settings.data_dir).write_frame(output_name, targets_for_artifact)
    write_target_audit_outputs(audit, summary, audit_output, summary_output)

    if store_db:
        assert db is not None
        run_id = db.insert_target_run(
            asdict(summary),
            source="upstox",
            started_at=started_at,
        )
        audit_rows = db.insert_target_audits(
            audit,
            dataset_name=summary.dataset_name,
            target_version=summary.target_version,
            run_id=run_id,
        )

    summary_dict: dict[str, Any] = asdict(summary)
    warnings = (
        [f"Excluded invalid OHLCV rows: {invalid_ohlcv_count}"]
        if invalid_ohlcv_count
        else []
    )
    return PipelineRunResult(
        name="daily_targets",
        status="pass" if summary.failed_rows == 0 else "warn",
        rows=len(targets),
        artifacts={
            "targets": output_path,
            "target_audit": audit_output,
            "target_summary": summary_output,
        },
        metrics={
            **summary_dict,
            "invalid_ohlcv_count": invalid_ohlcv_count,
            "timescale_rows": db_rows,
            "timescale_audit_rows": audit_rows,
            "timescale_run_id": run_id,
            "incremental": bool(incremental),
            "recompute_start": recompute_start.isoformat() if recompute_start else None,
            "computed_rows": int(len(targets)),
            "artifact_rows": int(len(targets_for_artifact)),
            "export_db_snapshot": bool(export_db_snapshot),
        },
        warnings=warnings,
    )
