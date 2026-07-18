from __future__ import annotations

from dataclasses import asdict
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any

import pandas as pd

from trade_research.config import get_settings
from trade_research.features import (
    FEATURE_VERSION_V1_0,
    DailyTechnicalFeatureBuilder,
    audit_daily_features,
    invalid_daily_ohlcv_mask,
    normalize_daily_ohlcv,
    write_feature_audit_outputs,
)
from trade_research.pipelines.base import PipelineRunResult
from trade_research.pipelines.common import limit_daily_symbols
from trade_research.storage import ParquetStore, TimescaleStore


def run_daily_feature_pipeline(
    input_source: str = "parquet",
    input_name: str = "processed/equities/nse_daily_ohlcv_upstox",
    ohlcv_source: str = "upstox",
    output_name: str = "processed/features/daily_v1_ohlcv_technical",
    feature_version: str = FEATURE_VERSION_V1_0,
    audit_output: Path = Path("data/processed/features/daily_v1_ohlcv_technical_audit.csv"),
    summary_output: Path = Path("data/processed/features/daily_v1_ohlcv_technical_summary.json"),
    limit: int | None = None,
    strict_invalid_rows: bool = False,
    store_db: bool = False,
    incremental: bool = False,
    replace_exchange: bool = False,
    lookback_days: int = 320,
    export_db_snapshot: bool = True,
) -> PipelineRunResult:
    settings = get_settings()
    started_at = datetime.now(UTC)
    normalized_source = input_source.lower()
    if normalized_source not in {"parquet", "timescale"}:
        raise ValueError("input_source must be parquet or timescale.")
    if ohlcv_source not in {"upstox", "yfinance"}:
        raise ValueError("ohlcv_source must be upstox or yfinance.")
    if replace_exchange and (not store_db or incremental):
        raise ValueError(
            "replace_exchange requires store_db=True and a full rebuild."
        )

    db: TimescaleStore | None = None
    recompute_start = None
    source_start = None
    if normalized_source == "parquet":
        source_frame = ParquetStore(settings.data_dir).read_frame(input_name)
        source_frame = limit_daily_symbols(source_frame, limit)
    else:
        db = TimescaleStore(settings.database_url)
        if incremental:
            latest_feature_date = db.latest_daily_feature_date(feature_version)
            if latest_feature_date is not None:
                recompute_start = latest_feature_date + timedelta(days=1)
                source_start = recompute_start - timedelta(days=lookback_days)
        source_frame = db.daily_ohlcv_frame(
            source=ohlcv_source,
            limit=limit,
            start_date=source_start,
        )

    if source_frame.empty:
        raise ValueError("No daily OHLCV rows found for feature generation.")

    invalid_ohlcv_count = int(invalid_daily_ohlcv_mask(normalize_daily_ohlcv(source_frame)).sum())
    features = DailyTechnicalFeatureBuilder(
        feature_version=feature_version,
        drop_invalid_rows=not strict_invalid_rows,
    ).build(source_frame)
    if recompute_start is not None:
        features_to_store = features[
            pd.to_datetime(features["date"], errors="coerce").dt.date >= recompute_start
        ].copy()
    else:
        features_to_store = features

    db_rows = 0
    deleted_rows = 0
    audit_rows = 0
    run_id: str | None = None
    if store_db:
        db = db or TimescaleStore(settings.database_url)
        db.initialize()
        if replace_exchange:
            deleted_rows, db_rows = db.replace_daily_features(
                features_to_store,
                feature_version,
                exchange="NSE",
            )
        else:
            db_rows = db.upsert_daily_features(features_to_store)
        if export_db_snapshot:
            features_for_artifact = db.daily_feature_frame(
                feature_version=feature_version,
                limit=limit,
            )
        else:
            features_for_artifact = features_to_store
    else:
        features_for_artifact = features_to_store

    audit, summary = audit_daily_features(
        features_for_artifact,
        feature_version=feature_version,
        invalid_ohlcv_count=invalid_ohlcv_count,
    )
    output_path = ParquetStore(settings.data_dir).write_frame(output_name, features_for_artifact)
    write_feature_audit_outputs(audit, summary, audit_output, summary_output)

    if store_db:
        assert db is not None
        run_id = db.insert_feature_run(
            asdict(summary),
            source=ohlcv_source,
            started_at=started_at,
        )
        audit_rows = db.insert_feature_audits(
            audit,
            dataset_name=summary.dataset_name,
            feature_version=summary.feature_version,
            run_id=run_id,
        )

    summary_dict: dict[str, Any] = asdict(summary)
    warnings = (
        [f"Excluded invalid OHLCV rows: {invalid_ohlcv_count}"]
        if invalid_ohlcv_count
        else []
    )
    return PipelineRunResult(
        name="daily_features",
        status="pass" if summary.failed_rows == 0 else "warn",
        rows=len(features_to_store),
        artifacts={
            "features": output_path,
            "feature_audit": audit_output,
            "feature_summary": summary_output,
        },
        metrics={
            **summary_dict,
            "invalid_ohlcv_count": invalid_ohlcv_count,
            "timescale_rows": db_rows,
            "timescale_deleted_rows": deleted_rows,
            "timescale_audit_rows": audit_rows,
            "timescale_run_id": run_id,
            "incremental": bool(incremental),
            "replace_exchange": bool(replace_exchange),
            "ohlcv_source": ohlcv_source,
            "source_start": source_start.isoformat() if source_start else None,
            "recompute_start": recompute_start.isoformat() if recompute_start else None,
            "computed_rows": int(len(features)),
            "stored_rows": int(len(features_to_store)),
            "artifact_rows": int(len(features_for_artifact)),
            "lookback_days": int(lookback_days),
            "export_db_snapshot": bool(export_db_snapshot),
        },
        warnings=warnings,
    )
