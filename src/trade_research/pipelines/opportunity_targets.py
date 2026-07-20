from __future__ import annotations

import json
from dataclasses import asdict
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any

import pandas as pd

from trade_research.config import get_settings
from trade_research.features import invalid_daily_ohlcv_mask, normalize_daily_ohlcv
from trade_research.pipelines.base import PipelineRunResult
from trade_research.storage import ParquetStore, TimescaleStore
from trade_research.targets import (
    DAILY_OPPORTUNITY_TARGET_COLUMNS_V1_0,
    DAILY_OPPORTUNITY_TARGET_VERSION_V1_0,
    DailyOpportunityTargetBuilder,
    OpportunityTargetAuditSummary,
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
    batch_size: int = 50,
) -> PipelineRunResult:
    exchange_code = exchange.upper()
    source_code = ohlcv_source.lower()
    if exchange_code not in {"NSE", "TSX", "US"}:
        raise ValueError("exchange must be NSE, TSX, or US")
    if replace_exchange and incremental:
        raise ValueError("replace_exchange requires a non-incremental run")
    if limit is not None and limit < 1:
        raise ValueError("limit must be at least 1 instrument")
    if not 1 <= batch_size <= 500:
        raise ValueError("batch_size must be between 1 and 500 instruments")

    settings = get_settings()
    started_at = datetime.now(UTC)
    db = TimescaleStore(settings.database_url)
    db.initialize()
    recompute_start = None
    if incremental:
        latest = db.latest_daily_opportunity_target_date(
            target_version,
            exchange=exchange_code,
            source=source_code,
        )
        if latest is not None:
            recompute_start = latest - timedelta(days=max(recompute_lookback_days, 7))

    instrument_keys = db.daily_ohlcv_instrument_keys(
        exchange=exchange_code,
        source=source_code,
        limit=limit,
        start_date=recompute_start,
    )
    if not instrument_keys:
        raise ValueError(
            f"No {source_code} daily OHLCV rows found for {exchange_code}."
        )
    batches = list(_batches(instrument_keys, batch_size))
    builder = DailyOpportunityTargetBuilder(
        target_version=target_version,
        computed_at=started_at,
        drop_invalid_rows=True,
    )
    accumulator = _OpportunityAuditAccumulator(target_version=target_version)
    initial_progress = accumulator.progress(
        exchange=exchange_code,
        source=source_code,
        batch_count=len(batches),
        completed_batches=0,
        batch_size=batch_size,
    )
    run_id = (
        db.insert_target_run(
            initial_progress,
            source=source_code,
            status="running",
            started_at=started_at,
        )
        if store_db
        else None
    )
    parquet = ParquetStore(settings.data_dir)
    run_token = run_id or started_at.strftime("%Y%m%dT%H%M%S%fZ")
    batch_artifacts: list[dict[str, Any]] = []
    db_rows = 0
    deleted_rows = 0
    max_source_rows_in_batch = 0
    try:
        if store_db and replace_exchange:
            deleted_rows = db.delete_daily_opportunity_targets(
                target_version=target_version,
                exchange=exchange_code,
                source=source_code,
            )
        for batch_number, keys in enumerate(batches, start=1):
            source_frame = db.daily_ohlcv_frame(
                exchange=exchange_code,
                source=source_code,
                start_date=recompute_start,
                instrument_keys=keys,
            )
            if source_frame.empty:
                raise RuntimeError(
                    f"No source rows remained for Opportunity batch {batch_number}."
                )
            max_source_rows_in_batch = max(max_source_rows_in_batch, len(source_frame))
            normalized_source = normalize_daily_ohlcv(source_frame)
            invalid_ohlcv_count = int(
                invalid_daily_ohlcv_mask(normalized_source).sum()
            )
            build_frame = source_frame
            if recompute_start is not None:
                predecessor = db.preceding_valid_daily_ohlcv_frame(
                    exchange=exchange_code,
                    source=source_code,
                    instrument_keys=keys,
                    before_date=recompute_start,
                )
                if not predecessor.empty:
                    build_frame = pd.concat(
                        [predecessor, source_frame],
                        ignore_index=True,
                    )
            targets = builder.build(build_frame)
            if recompute_start is not None:
                targets = targets[targets["date"] >= recompute_start].reset_index(drop=True)
            audit, batch_summary = audit_daily_opportunity_targets(
                targets,
                target_version=target_version,
                invalid_ohlcv_count=invalid_ohlcv_count,
            )
            if store_db:
                db_rows += db.upsert_daily_opportunity_targets(targets)
            batch_path = parquet.write_frame(
                "processed/opportunities/"
                f"{exchange_code.lower()}_daily_targets/{run_token}/"
                f"batch_{batch_number:05d}",
                targets,
            )
            batch_artifacts.append(
                {
                    "batch": batch_number,
                    "instrument_count": len(keys),
                    "source_rows": len(source_frame),
                    "target_rows": len(targets),
                    "path": str(batch_path),
                }
            )
            accumulator.add(audit, batch_summary)
            if store_db and run_id:
                db.update_target_run(
                    run_id,
                    accumulator.progress(
                        exchange=exchange_code,
                        source=source_code,
                        batch_count=len(batches),
                        completed_batches=batch_number,
                        batch_size=batch_size,
                    ),
                    status="running",
                )
    except Exception as exc:
        if store_db and run_id:
            failed_progress = accumulator.progress(
                exchange=exchange_code,
                source=source_code,
                batch_count=len(batches),
                completed_batches=len(batch_artifacts),
                batch_size=batch_size,
            )
            failed_progress["error_type"] = type(exc).__name__
            failed_progress["error_details"] = str(exc)[:500]
            try:
                db.update_target_run(run_id, failed_progress, status="failed")
            except Exception as progress_exc:
                exc.add_note(
                    "Additionally failed to persist Opportunity run failure state: "
                    f"{type(progress_exc).__name__}: {progress_exc}"
                )
        raise

    audit, summary = accumulator.result()
    output_directory = (
        Path(settings.data_dir) / "processed" / "opportunities"
    )
    manifest_path = output_directory / f"{exchange_code.lower()}_daily_targets_manifest.json"
    audit_output = output_directory / f"{exchange_code.lower()}_daily_targets_audit.csv"
    summary_output = (
        output_directory / f"{exchange_code.lower()}_daily_targets_summary.json"
    )
    completed_progress = {
        **asdict(summary),
        "exchange": exchange_code,
        "ohlcv_source": source_code,
        "batch_count": len(batches),
        "completed_batches": len(batches),
        "batch_size": batch_size,
    }
    try:
        manifest_path.parent.mkdir(parents=True, exist_ok=True)
        manifest_path.write_text(
            json.dumps(
                {
                    "run_id": run_id,
                    "exchange": exchange_code,
                    "source": source_code,
                    "target_version": target_version,
                    "batch_size": batch_size,
                    "batch_count": len(batches),
                    "completed_batches": len(batch_artifacts),
                    "rows": summary.row_count,
                    "batches": batch_artifacts,
                },
                indent=2,
            )
            + "\n"
        )
        write_opportunity_target_audit_outputs(
            audit,
            summary,
            audit_output,
            summary_output,
        )
        if store_db and run_id:
            audit_rows = db.insert_target_audits(
                audit,
                dataset_name=summary.dataset_name,
                target_version=target_version,
                run_id=run_id,
            )
            db.update_target_run(run_id, completed_progress, status="completed")
        else:
            audit_rows = 0
    except Exception as exc:
        if store_db and run_id:
            failed_progress = {
                **completed_progress,
                "error_type": type(exc).__name__,
                "error_details": str(exc)[:500],
            }
            try:
                db.update_target_run(run_id, failed_progress, status="failed")
            except Exception as progress_exc:
                exc.add_note(
                    "Additionally failed to persist Opportunity finalization failure: "
                    f"{type(progress_exc).__name__}: {progress_exc}"
                )
        raise

    metrics: dict[str, Any] = {
        **asdict(summary),
        "exchange": exchange_code,
        "ohlcv_source": source_code,
        "timescale_rows": db_rows,
        "timescale_deleted_rows": deleted_rows,
        "timescale_audit_rows": audit_rows,
        "timescale_run_id": run_id,
        "incremental": incremental,
        "recompute_start": recompute_start.isoformat() if recompute_start else None,
        "instrument_count": len(instrument_keys),
        "batch_size": batch_size,
        "batch_count": len(batches),
        "completed_batches": len(batch_artifacts),
        "max_source_rows_in_batch": max_source_rows_in_batch,
    }
    return PipelineRunResult(
        name=f"{exchange_code.lower()}_opportunity_targets",
        status="pass" if summary.failed_rows == 0 else "warn",
        rows=summary.row_count,
        artifacts={
            "targets": manifest_path,
            "target_audit": audit_output,
            "target_summary": summary_output,
        },
        metrics=metrics,
        warnings=(
            [f"Excluded invalid OHLCV rows: {summary.invalid_ohlcv_count}"]
            if summary.invalid_ohlcv_count
            else []
        ),
    )


class _OpportunityAuditAccumulator:
    def __init__(self, *, target_version: str) -> None:
        self.target_version = target_version
        self.row_count = 0
        self.symbol_count = 0
        self.date_min: str | None = None
        self.date_max: str | None = None
        self.duplicate_key_count = 0
        self.invalid_ohlcv_count = 0
        self.inf_value_count = 0
        self.passed_rows = 0
        self.warning_rows = 0
        self.failed_rows = 0
        self.null_counts = {
            column: 0 for column in DAILY_OPPORTUNITY_TARGET_COLUMNS_V1_0
        }
        self.inf_counts = {
            column: 0 for column in DAILY_OPPORTUNITY_TARGET_COLUMNS_V1_0
        }

    def add(
        self,
        audit: pd.DataFrame,
        summary: OpportunityTargetAuditSummary,
    ) -> None:
        self.row_count += summary.row_count
        self.symbol_count += summary.symbol_count
        self.date_min = _minimum_iso_date(self.date_min, summary.date_min)
        self.date_max = _maximum_iso_date(self.date_max, summary.date_max)
        self.duplicate_key_count += summary.duplicate_key_count
        self.invalid_ohlcv_count += summary.invalid_ohlcv_count
        self.inf_value_count += summary.inf_value_count
        self.passed_rows += summary.passed_rows
        self.warning_rows += summary.warning_rows
        self.failed_rows += summary.failed_rows
        for row in audit.to_dict(orient="records"):
            target = str(row["target"])
            self.null_counts[target] += int(row["null_count"])
            self.inf_counts[target] += int(row["inf_count"])

    def result(self) -> tuple[pd.DataFrame, OpportunityTargetAuditSummary]:
        audit = pd.DataFrame(
            [
                {
                    "target": column,
                    "null_count": self.null_counts[column],
                    "null_pct": round(
                        self.null_counts[column] * 100 / self.row_count,
                        4,
                    )
                    if self.row_count
                    else 0.0,
                    "inf_count": self.inf_counts[column],
                }
                for column in DAILY_OPPORTUNITY_TARGET_COLUMNS_V1_0
            ]
        )
        summary = OpportunityTargetAuditSummary(
            dataset_name="daily_opportunity_outcomes",
            target_version=self.target_version,
            generated_at=datetime.now(UTC).isoformat(),
            row_count=self.row_count,
            symbol_count=self.symbol_count,
            date_min=self.date_min,
            date_max=self.date_max,
            duplicate_key_count=self.duplicate_key_count,
            invalid_ohlcv_count=self.invalid_ohlcv_count,
            inf_value_count=self.inf_value_count,
            passed_rows=self.passed_rows,
            warning_rows=self.warning_rows,
            failed_rows=self.failed_rows,
        )
        return audit, summary

    def progress(
        self,
        *,
        exchange: str,
        source: str,
        batch_count: int,
        completed_batches: int,
        batch_size: int,
    ) -> dict[str, Any]:
        _, summary = self.result()
        return {
            **asdict(summary),
            "exchange": exchange,
            "ohlcv_source": source,
            "batch_count": batch_count,
            "completed_batches": completed_batches,
            "batch_size": batch_size,
        }


def _batches(values: list[str], size: int) -> list[list[str]]:
    return [values[index : index + size] for index in range(0, len(values), size)]


def _minimum_iso_date(current: str | None, candidate: str | None) -> str | None:
    values = [value for value in (current, candidate) if value is not None]
    return min(values) if values else None


def _maximum_iso_date(current: str | None, candidate: str | None) -> str | None:
    values = [value for value in (current, candidate) if value is not None]
    return max(values) if values else None
