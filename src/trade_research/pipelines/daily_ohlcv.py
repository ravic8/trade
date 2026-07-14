from __future__ import annotations

import time
from datetime import UTC, date, datetime, timedelta
from pathlib import Path
from time import perf_counter
from typing import Any

import pandas as pd

from trade_research.config import get_settings
from trade_research.credentials import resolve_provider_token
from trade_research.data import UpstoxHistoricalDataProvider, audit_daily_ohlcv
from trade_research.data.rate_limits import ProviderRateLimiter, build_provider_rate_limiter
from trade_research.pipelines.base import PipelineRunResult
from trade_research.storage import ParquetStore, TimescaleStore


def run_upstox_daily_ohlcv_pipeline(
    mapping_csv: Path = Path("data/processed/universe/liquid_nse_upstox_mapping.csv"),
    years: int = 2,
    from_date: str | None = None,
    to_date: str | None = None,
    settlement_lag_days: int = 2,
    limit: int | None = None,
    throttle_seconds: float = 0.25,
    output_name: str = "processed/equities/nse_daily_ohlcv_upstox",
    incremental_output_name: str = "processed/equities/nse_daily_ohlcv_upstox_incremental",
    audit_output: Path = Path("data/processed/equities/nse_daily_ohlcv_upstox_audit.csv"),
    failures_output: Path = Path("data/processed/equities/nse_daily_ohlcv_upstox_failures.csv"),
    skipped_output: Path = Path("data/processed/equities/nse_daily_ohlcv_upstox_skipped.csv"),
    fetch_coverage_output: Path = Path(
        "data/processed/equities/nse_daily_ohlcv_upstox_fetch_coverage.csv"
    ),
    access_token: str | None = None,
    full_refresh: bool = False,
    store_db: bool = True,
    export_db_snapshot: bool = True,
    trigger: str = "pipeline",
) -> PipelineRunResult:
    settings = get_settings()

    end = (
        _parse_pipeline_date(to_date, "to_date")
        if to_date
        else date.today() - timedelta(days=settlement_lag_days)
    )
    base_start = (
        _parse_pipeline_date(from_date, "from_date") if from_date else _subtract_years(end, years)
    )
    is_full_window = full_refresh or from_date is not None
    db = TimescaleStore(settings.database_url) if store_db else None
    if db is not None:
        db.initialize()
    mapping = _load_upstox_mapping(mapping_csv, db=db)
    if limit:
        mapping = mapping.head(limit)
    token = access_token or (
        resolve_provider_token(
            db,
            provider="upstox",
            fallback_token=settings.upstox_access_token,
            app_secret_key=settings.app_secret_key,
        )
        if db is not None
        else settings.upstox_access_token
    )
    if not token:
        raise ValueError("Set UPSTOX_ACCESS_TOKEN or pass access_token.")
    latest_dates = (
        {}
        if db is None or full_refresh or from_date
        else db.latest_daily_ohlcv_dates(
            [str(key) for key in mapping["instrument_key"].dropna().tolist()],
            source="upstox",
        )
    )
    planned = plan_daily_fetch_windows(
        mapping,
        base_start=base_start,
        end=end,
        latest_dates=latest_dates,
    )
    fetch_plan = planned[planned["should_fetch"]].copy()
    skipped_plan = planned[~planned["should_fetch"]].copy()

    skipped_output.parent.mkdir(parents=True, exist_ok=True)
    skipped_plan.to_csv(skipped_output, index=False)

    run_id = None
    if db is not None:
        run_id = db.start_ingestion_run(
            job_name="upstox_nse_daily_ohlcv",
            exchange="NSE",
            source="upstox",
            items_requested=len(fetch_plan),
            run_metadata={
                "trigger": trigger,
                "mode": "full_refresh" if full_refresh or from_date else "incremental",
                "base_start": base_start.isoformat(),
                "end": end.isoformat(),
                "settlement_lag_days": settlement_lag_days if to_date is None else None,
                "mapped_symbols": len(mapping),
                "skipped_current_symbols": len(skipped_plan),
                "export_db_snapshot": bool(export_db_snapshot),
            },
        )

    frames: list[pd.DataFrame] = []
    failures: list[dict[str, str]] = []
    limiter = build_provider_rate_limiter(settings)
    try:
        with UpstoxHistoricalDataProvider(token) as provider:
            for row in fetch_plan.to_dict(orient="records"):
                try:
                    frame = _fetch_upstox_daily_with_controls(
                        provider=provider,
                        limiter=limiter,
                        db=db,
                        run_id=str(run_id) if run_id is not None else None,
                        row=row,
                        start=_parse_pipeline_date(str(row["fetch_start"]), "fetch_start"),
                        end=end,
                    )
                    if not frame.empty:
                        frames.append(frame)
                except Exception as exc:
                    failures.append(
                        {
                            "symbol": str(row["symbol"]),
                            "instrument_key": str(row["instrument_key"]),
                            "error": str(exc),
                        }
                    )
                if throttle_seconds:
                    time.sleep(throttle_seconds)

        ohlcv = pd.concat(frames, ignore_index=True) if frames else pd.DataFrame()
        audit = (
            audit_daily_ohlcv(ohlcv, fetch_plan)
            if not fetch_plan.empty
            else _empty_daily_audit_frame()
        )

        store = ParquetStore(settings.data_dir)
        output_path = None
        if not ohlcv.empty:
            output_path = store.write_frame(
                output_name if is_full_window else incremental_output_name,
                ohlcv,
            )

        audit_output.parent.mkdir(parents=True, exist_ok=True)
        failures_output.parent.mkdir(parents=True, exist_ok=True)
        fetch_coverage_output.parent.mkdir(parents=True, exist_ok=True)
        audit.to_csv(audit_output, index=False)
        failures_frame = pd.DataFrame(failures, columns=["symbol", "instrument_key", "error"])
        failures_frame.to_csv(failures_output, index=False)
        fetch_coverage = build_daily_fetch_coverage(planned, ohlcv, failures_frame)
        fetch_coverage.to_csv(fetch_coverage_output, index=False)

        rows_written = db.upsert_daily_ohlcv(ohlcv) if db is not None and not ohlcv.empty else 0
        audits_written = (
            db.insert_data_quality_audits(
                audit,
                dataset_name="nse_daily_ohlcv",
                source="upstox",
                interval="1d",
            )
            if db is not None and not audit.empty
            else 0
        )
        fetch_coverage_rows = (
            db.insert_daily_ohlcv_fetch_coverage(str(run_id), fetch_coverage)
            if db is not None and run_id is not None and not fetch_coverage.empty
            else 0
        )
        db_snapshot_rows = 0
        if db is not None and export_db_snapshot:
            snapshot = db.daily_ohlcv_frame(exchange="NSE", source="upstox")
            if not snapshot.empty:
                output_path = store.write_frame(output_name, snapshot)
                db_snapshot_rows = int(len(snapshot))

        if db is not None and run_id is not None:
            succeeded = (
                int(audit["status"].isin(["passed", "warning"]).sum()) if not audit.empty else 0
            )
            db.finish_ingestion_run(
                run_id,
                status="completed" if rows_written else "completed_empty",
                items_processed=len(fetch_plan),
                items_succeeded=succeeded,
                items_failed=len(fetch_plan) - succeeded,
            )
    except Exception as exc:
        if db is not None and run_id is not None:
            db.finish_ingestion_run(
                run_id,
                status="failed",
                items_processed=0,
                items_succeeded=0,
                items_failed=len(fetch_plan),
                error_message=str(exc),
            )
        raise

    warnings = []
    if failures:
        warnings.append(f"Upstox daily fetch recorded {len(failures)} failures.")
    return PipelineRunResult(
        name="upstox_daily_ohlcv",
        status="warn" if failures else "pass",
        rows=int(len(ohlcv)),
        artifacts={
            **({"ohlcv": output_path} if output_path is not None else {}),
            "daily_audit": audit_output,
            "fetch_failures": failures_output,
            "skipped_symbols": skipped_output,
            "fetch_coverage": fetch_coverage_output,
        },
        metrics={
            "run_id": run_id,
            "mode": "full_refresh" if full_refresh or from_date else "incremental",
            "base_start": base_start.isoformat(),
            "end": end.isoformat(),
            "mapped_symbols": int(len(mapping)),
            "planned_symbols": int(len(planned)),
            "fetch_symbols": int(len(fetch_plan)),
            "skipped_current_symbols": int(len(skipped_plan)),
            "fetched_rows": int(len(ohlcv)),
            "failure_rows": int(len(failures)),
            "fetch_coverage_rows": int(len(fetch_coverage)),
            "timescale_fetch_coverage_rows": int(fetch_coverage_rows),
            "timescale_rows": int(rows_written),
            "timescale_audit_rows": int(audits_written),
            "db_snapshot_rows": int(db_snapshot_rows),
            "store_db": bool(store_db),
        },
        warnings=warnings,
    )


def run_upstox_daily_ohlcv_retry_pipeline(
    coverage_run_id: str | None = None,
    statuses: tuple[str, ...] = ("failed", "no_rows"),
    limit: int | None = None,
    throttle_seconds: float = 0.25,
    retry_output_name: str = "processed/equities/nse_daily_ohlcv_upstox_retry",
    retry_coverage_output: Path = Path(
        "data/processed/equities/nse_daily_ohlcv_upstox_retry_coverage.csv"
    ),
    retry_failures_output: Path = Path(
        "data/processed/equities/nse_daily_ohlcv_upstox_retry_failures.csv"
    ),
    access_token: str | None = None,
    export_db_snapshot: bool = True,
    trigger: str = "pipeline",
) -> PipelineRunResult:
    settings = get_settings()
    db = TimescaleStore(settings.database_url)
    db.initialize()
    candidates = db.daily_ohlcv_fetch_retry_candidates(
        run_id=coverage_run_id,
        statuses=statuses,
        limit=limit,
    )
    token = access_token or resolve_provider_token(
        db,
        provider="upstox",
        fallback_token=settings.upstox_access_token,
        app_secret_key=settings.app_secret_key,
    )
    if not candidates.empty and not token:
        raise ValueError("Set UPSTOX_ACCESS_TOKEN or pass access_token.")
    source_run_id = (
        coverage_run_id
        or db.latest_daily_ohlcv_fetch_coverage_run_id()
        or "unknown"
    )
    run_id = db.start_ingestion_run(
        job_name="upstox_nse_daily_ohlcv_retry",
        exchange="NSE",
        source="upstox",
        items_requested=len(candidates),
        run_metadata={
            "trigger": trigger,
            "source_coverage_run_id": source_run_id,
            "retry_statuses": list(statuses),
            "export_db_snapshot": bool(export_db_snapshot),
        },
    )

    frames: list[pd.DataFrame] = []
    failures: list[dict[str, str]] = []
    limiter = build_provider_rate_limiter(settings)
    try:
        if not candidates.empty:
            with UpstoxHistoricalDataProvider(token) as provider:
                for row in candidates.to_dict(orient="records"):
                    try:
                        frame = _fetch_upstox_daily_with_controls(
                            provider=provider,
                            limiter=limiter,
                            db=db,
                            run_id=str(run_id),
                            row=row,
                            start=_parse_pipeline_date(str(row["fetch_start"]), "fetch_start"),
                            end=_parse_pipeline_date(str(row["fetch_end"]), "fetch_end"),
                        )
                        if not frame.empty:
                            frames.append(frame)
                    except Exception as exc:
                        failures.append(
                            {
                                "symbol": str(row["symbol"]),
                                "instrument_key": str(row["instrument_key"]),
                                "error": str(exc),
                            }
                        )
                    if throttle_seconds:
                        time.sleep(throttle_seconds)

        ohlcv = pd.concat(frames, ignore_index=True) if frames else pd.DataFrame()
        failures_frame = pd.DataFrame(failures, columns=["symbol", "instrument_key", "error"])
        retry_plan = _retry_candidates_to_fetch_plan(candidates)
        retry_coverage = build_daily_fetch_coverage(retry_plan, ohlcv, failures_frame)

        store = ParquetStore(settings.data_dir)
        output_path = None
        if not ohlcv.empty:
            output_path = store.write_frame(retry_output_name, ohlcv)

        retry_coverage_output.parent.mkdir(parents=True, exist_ok=True)
        retry_failures_output.parent.mkdir(parents=True, exist_ok=True)
        retry_coverage.to_csv(retry_coverage_output, index=False)
        failures_frame.to_csv(retry_failures_output, index=False)

        rows_written = db.upsert_daily_ohlcv(ohlcv) if not ohlcv.empty else 0
        retry_coverage_rows = (
            db.insert_daily_ohlcv_fetch_coverage(str(run_id), retry_coverage)
            if not retry_coverage.empty
            else 0
        )
        db_snapshot_rows = 0
        if export_db_snapshot:
            snapshot = db.daily_ohlcv_frame(exchange="NSE", source="upstox")
            if not snapshot.empty:
                store.write_frame("processed/equities/nse_daily_ohlcv_upstox", snapshot)
                db_snapshot_rows = int(len(snapshot))

        succeeded = (
            int(retry_coverage["fetch_status"].eq("fetched").sum())
            if not retry_coverage.empty
            else 0
        )
        db.finish_ingestion_run(
            run_id,
            status="completed" if rows_written else "completed_empty",
            items_processed=len(candidates),
            items_succeeded=succeeded,
            items_failed=len(candidates) - succeeded,
        )
    except Exception as exc:
        db.finish_ingestion_run(
            run_id,
            status="failed",
            items_processed=0,
            items_succeeded=0,
            items_failed=len(candidates),
            error_message=str(exc),
        )
        raise

    warnings = []
    if failures:
        warnings.append(f"Upstox daily retry recorded {len(failures)} failures.")
    if candidates.empty:
        warnings.append("No retry candidates found.")
    return PipelineRunResult(
        name="upstox_daily_ohlcv_retry",
        status="warn" if warnings else "pass",
        rows=int(len(ohlcv)),
        artifacts={
            **({"ohlcv_retry": output_path} if output_path is not None else {}),
            "retry_coverage": retry_coverage_output,
            "retry_failures": retry_failures_output,
        },
        metrics={
            "run_id": run_id,
            "source_coverage_run_id": source_run_id,
            "candidate_rows": int(len(candidates)),
            "retry_statuses": list(statuses),
            "fetched_rows": int(len(ohlcv)),
            "failure_rows": int(len(failures)),
            "retry_coverage_rows": int(len(retry_coverage)),
            "timescale_fetch_coverage_rows": int(retry_coverage_rows),
            "timescale_rows": int(rows_written),
            "db_snapshot_rows": int(db_snapshot_rows),
        },
        warnings=warnings,
    )


def _retry_candidates_to_fetch_plan(candidates: pd.DataFrame) -> pd.DataFrame:
    if candidates.empty:
        return pd.DataFrame(
            columns=[
                "symbol",
                "instrument_key",
                "latest_stored_date",
                "fetch_start",
                "fetch_end",
                "should_fetch",
                "skip_reason",
            ]
        )
    return pd.DataFrame(
        [
            {
                "symbol": record["symbol"],
                "instrument_key": record["instrument_key"],
                "latest_stored_date": record.get("latest_stored_date"),
                "fetch_start": record.get("fetch_start"),
                "fetch_end": record.get("fetch_end"),
                "should_fetch": True,
                "skip_reason": "",
            }
            for record in candidates.to_dict(orient="records")
        ]
    )


def _load_upstox_mapping(mapping_csv: Path, db: TimescaleStore | None) -> pd.DataFrame:
    if mapping_csv.exists():
        return pd.read_csv(mapping_csv)
    if db is None:
        raise FileNotFoundError(
            f"Upstox mapping file not found: {mapping_csv}. "
            "Run map-liquid-nse-upstox or enable database storage so the mapping can be "
            "rebuilt from tradable_universe_members."
        )

    members = db.tradable_universe_members("nse_liquid_adt_100cr", limit=10_000)
    rows = [
        {
            "symbol": str(member["symbol"]).upper(),
            "instrument_key": member["instrument_key"],
            "trading_symbol": str(member.get("trading_symbol") or member["symbol"]).upper(),
        }
        for member in members
        if member.get("instrument_key") and member.get("symbol")
    ]
    if not rows:
        raise FileNotFoundError(
            f"Upstox mapping file not found: {mapping_csv}, and no mapped members were found "
            "in tradable_universe_members for universe nse_liquid_adt_100cr."
        )

    mapping = pd.DataFrame(rows)
    mapping_csv.parent.mkdir(parents=True, exist_ok=True)
    mapping.to_csv(mapping_csv, index=False)
    return mapping


def _fetch_upstox_daily_with_controls(
    provider: UpstoxHistoricalDataProvider,
    limiter: ProviderRateLimiter,
    db: TimescaleStore | None,
    run_id: str | None,
    row: dict[str, Any],
    start: date,
    end: date,
) -> pd.DataFrame:
    instrument_key = str(row["instrument_key"])
    symbol = str(row["symbol"])
    trading_symbol = str(row.get("trading_symbol") or symbol)
    decision = limiter.acquire("upstox", "historical")
    started = perf_counter()
    status = "success"
    error_message = ""
    try:
        return provider.fetch_daily_candles(
            instrument_key=instrument_key,
            start=start,
            end=end,
            symbol=symbol,
            trading_symbol=trading_symbol,
        )
    except Exception as exc:
        status = "error"
        error_message = str(exc)
        raise
    finally:
        duration_ms = (perf_counter() - started) * 1000
        _record_provider_request(
            db=db,
            run_id=run_id,
            provider="upstox",
            endpoint_group="historical",
            request_key=f"{instrument_key}:1d:{start.isoformat()}:{end.isoformat()}",
            instrument_key=instrument_key,
            symbol=symbol,
            interval="1d",
            window_start=start,
            window_end=end,
            status=status,
            error_message=error_message,
            rate_limited=decision.rate_limited,
            wait_seconds=decision.wait_seconds,
            duration_ms=duration_ms,
        )


def _record_provider_request(
    db: TimescaleStore | None,
    run_id: str | None,
    provider: str,
    endpoint_group: str,
    request_key: str,
    instrument_key: str,
    symbol: str,
    interval: str,
    window_start: date,
    window_end: date,
    status: str,
    error_message: str,
    rate_limited: bool,
    wait_seconds: float,
    duration_ms: float,
) -> None:
    if db is None:
        return
    try:
        db.insert_provider_request_logs(
            [
                {
                    "run_id": run_id,
                    "provider": provider,
                    "endpoint_group": endpoint_group,
                    "request_key": request_key,
                    "instrument_key": instrument_key,
                    "symbol": symbol,
                    "interval": interval,
                    "window_start": window_start,
                    "window_end": window_end,
                    "status": status,
                    "error_message": error_message,
                    "retry_count": 0,
                    "rate_limited": rate_limited,
                    "wait_seconds": wait_seconds,
                    "duration_ms": duration_ms,
                    "created_at": datetime.now(UTC),
                }
            ]
        )
    except Exception:
        return


def build_daily_fetch_coverage(
    planned: pd.DataFrame,
    fetched_ohlcv: pd.DataFrame,
    failures: pd.DataFrame,
) -> pd.DataFrame:
    if planned.empty:
        return pd.DataFrame(
            columns=[
                "symbol",
                "instrument_key",
                "latest_stored_date",
                "fetch_start",
                "fetch_end",
                "should_fetch",
                "fetch_status",
                "rows_fetched",
                "skip_reason",
                "error",
            ]
        )

    rows_by_key: dict[str, int] = {}
    if not fetched_ohlcv.empty and "InstrumentKey" in fetched_ohlcv.columns:
        rows_by_key = (
            fetched_ohlcv.groupby("InstrumentKey").size().astype(int).to_dict()
        )
    errors_by_key = {}
    if not failures.empty:
        errors_by_key = {
            str(row["instrument_key"]): str(row["error"])
            for row in failures.to_dict(orient="records")
        }

    rows = []
    for record in planned.to_dict(orient="records"):
        instrument_key = str(record["instrument_key"])
        should_fetch = bool(record["should_fetch"])
        error = errors_by_key.get(instrument_key)
        rows_fetched = int(rows_by_key.get(instrument_key, 0))
        if not should_fetch:
            status = "skipped_current"
        elif error:
            status = "failed"
        elif rows_fetched > 0:
            status = "fetched"
        else:
            status = "no_rows"
        rows.append(
            {
                "symbol": str(record["symbol"]),
                "instrument_key": instrument_key,
                "latest_stored_date": record.get("latest_stored_date"),
                "fetch_start": record.get("fetch_start"),
                "fetch_end": record.get("fetch_end"),
                "should_fetch": should_fetch,
                "fetch_status": status,
                "rows_fetched": rows_fetched,
                "skip_reason": record.get("skip_reason") or "",
                "error": error or "",
            }
        )
    return pd.DataFrame(rows)


def plan_daily_fetch_windows(
    mapping: pd.DataFrame,
    base_start: date,
    end: date,
    latest_dates: dict[str, date],
) -> pd.DataFrame:
    rows = []
    for record in mapping.to_dict(orient="records"):
        instrument_key = str(record["instrument_key"])
        latest_date = latest_dates.get(instrument_key)
        fetch_start = base_start
        if latest_date is not None:
            fetch_start = max(base_start, latest_date + timedelta(days=1))
        should_fetch = fetch_start <= end
        rows.append(
            {
                **record,
                "latest_stored_date": latest_date.isoformat() if latest_date else None,
                "fetch_start": fetch_start.isoformat(),
                "fetch_end": end.isoformat(),
                "should_fetch": should_fetch,
                "skip_reason": "" if should_fetch else "already_current",
            }
        )
    return pd.DataFrame(rows)


def _empty_daily_audit_frame() -> pd.DataFrame:
    return pd.DataFrame(
        columns=[
            "symbol",
            "instrument_key",
            "rows",
            "start_date",
            "end_date",
            "missing_dates",
            "null_ohlcv_rows",
            "duplicate_date_rows",
            "zero_volume_rows",
            "zero_or_negative_close_rows",
            "status",
        ]
    )


def _parse_pipeline_date(value: str, field_name: str) -> date:
    try:
        return date.fromisoformat(value)
    except ValueError as exc:
        raise ValueError(f"{field_name} must use YYYY-MM-DD format, got {value!r}.") from exc


def _subtract_years(value: date, years: int) -> date:
    try:
        return value.replace(year=value.year - years)
    except ValueError:
        return value.replace(year=value.year - years, day=28)
