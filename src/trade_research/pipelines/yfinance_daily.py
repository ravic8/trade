from __future__ import annotations

from datetime import UTC, date, datetime, timedelta
from pathlib import Path
from time import perf_counter
from typing import Any, Protocol

import pandas as pd

from trade_research.config import get_settings
from trade_research.data.rate_limits import ProviderRateLimiter, build_provider_rate_limiter
from trade_research.data.upstox import audit_daily_ohlcv
from trade_research.data.yfinance_provider import YFinanceDailyProvider
from trade_research.pipelines.base import PipelineRunResult
from trade_research.pipelines.daily_ohlcv import (
    build_daily_fetch_coverage,
    plan_daily_fetch_windows,
)
from trade_research.storage import ParquetStore, TimescaleStore
from trade_research.universe import yfinance_seed_universe


class YFinanceBatchProvider(Protocol):
    def fetch_daily_ohlcv(
        self,
        symbols: list[dict[str, str]],
        start: date,
        end: date,
    ) -> pd.DataFrame:
        ...


def run_yfinance_daily_ohlcv_pipeline(
    universe: str,
    years: int = 2,
    from_date: str | None = None,
    to_date: str | None = None,
    settlement_lag_days: int = 1,
    limit: int | None = None,
    batch_size: int = 25,
    store_db: bool = True,
    export_db_snapshot: bool = True,
    output_name: str | None = None,
    incremental_output_name: str | None = None,
    audit_output: Path | None = None,
    failures_output: Path | None = None,
    skipped_output: Path | None = None,
    fetch_coverage_output: Path | None = None,
    trigger: str = "pipeline",
    provider: YFinanceBatchProvider | None = None,
) -> PipelineRunResult:
    settings = get_settings()
    mapping = _yfinance_mapping(universe)
    if limit:
        mapping = mapping.head(limit)
    exchange = _exchange_for_universe(universe)
    universe_id = _universe_id(universe)

    output_name = output_name or f"processed/equities/yfinance_{universe_id}_daily_ohlcv"
    incremental_output_name = (
        incremental_output_name
        or f"processed/equities/yfinance_{universe_id}_daily_ohlcv_incremental"
    )
    audit_output = audit_output or Path(
        f"data/processed/equities/yfinance_{universe_id}_daily_ohlcv_audit.csv"
    )
    failures_output = failures_output or Path(
        f"data/processed/equities/yfinance_{universe_id}_daily_ohlcv_failures.csv"
    )
    skipped_output = skipped_output or Path(
        f"data/processed/equities/yfinance_{universe_id}_daily_ohlcv_skipped.csv"
    )
    fetch_coverage_output = fetch_coverage_output or Path(
        f"data/processed/equities/yfinance_{universe_id}_daily_ohlcv_fetch_coverage.csv"
    )

    end = (
        _parse_pipeline_date(to_date, "to_date")
        if to_date
        else date.today() - timedelta(days=settlement_lag_days)
    )
    base_start = (
        _parse_pipeline_date(from_date, "from_date") if from_date else _subtract_years(end, years)
    )
    is_full_window = from_date is not None

    db = TimescaleStore(settings.database_url) if store_db else None
    if db is not None:
        db.initialize()
    latest_dates = (
        {}
        if db is None or from_date
        else db.latest_daily_ohlcv_dates(
            [str(key) for key in mapping["instrument_key"].dropna().tolist()],
            source="yfinance",
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
            job_name=f"yfinance_{universe_id}_daily_ohlcv",
            exchange=exchange,
            source="yfinance",
            items_requested=len(fetch_plan),
            run_metadata={
                "trigger": trigger,
                "universe": universe_id,
                "mode": "full_window" if is_full_window else "incremental",
                "base_start": base_start.isoformat(),
                "end": end.isoformat(),
                "settlement_lag_days": settlement_lag_days if to_date is None else None,
                "mapped_symbols": len(mapping),
                "batch_size": max(batch_size, 1),
                "export_db_snapshot": bool(export_db_snapshot),
                "adjusted_close_storage": "deferred",
            },
        )

    limiter = build_provider_rate_limiter(settings)
    candle_provider = provider or YFinanceDailyProvider(auto_adjust=False)
    frames, failures = _fetch_yfinance_daily_batches_with_controls(
        provider=candle_provider,
        rows=fetch_plan.to_dict(orient="records"),
        limiter=limiter,
        db=db,
        run_id=str(run_id) if run_id is not None else None,
        batch_size=max(batch_size, 1),
    )

    ohlcv = pd.concat(frames, ignore_index=True) if frames else pd.DataFrame()
    failures_frame = pd.DataFrame(failures, columns=["symbol", "instrument_key", "error"])
    audit = (
        audit_daily_ohlcv(ohlcv, fetch_plan)
        if not fetch_plan.empty
        else _empty_daily_audit_frame()
    )
    fetch_coverage = build_daily_fetch_coverage(planned, ohlcv, failures_frame)

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
    failures_frame.to_csv(failures_output, index=False)
    fetch_coverage.to_csv(fetch_coverage_output, index=False)

    rows_written = (
        db.upsert_daily_ohlcv(ohlcv, exchange=exchange, source="yfinance")
        if db is not None and not ohlcv.empty
        else 0
    )
    audits_written = (
        db.insert_data_quality_audits(
            audit,
            dataset_name=f"yfinance_{universe_id}_daily_ohlcv",
            source="yfinance",
            interval="1d",
        )
        if db is not None and not audit.empty
        else 0
    )
    fetch_coverage_rows = (
        db.insert_daily_ohlcv_fetch_coverage(
            str(run_id),
            fetch_coverage,
            source="yfinance",
            exchange=exchange,
        )
        if db is not None and run_id is not None and not fetch_coverage.empty
        else 0
    )
    db_snapshot_rows = 0
    if db is not None and export_db_snapshot:
        snapshot = db.daily_ohlcv_frame(exchange=exchange, source="yfinance")
        if not snapshot.empty:
            output_path = store.write_frame(output_name, snapshot)
            db_snapshot_rows = int(len(snapshot))

    if db is not None and run_id is not None:
        succeeded = (
            int(fetch_coverage["fetch_status"].eq("fetched").sum())
            if not fetch_coverage.empty
            else 0
        )
        db.finish_ingestion_run(
            run_id,
            status="completed" if rows_written else "completed_empty",
            items_processed=len(fetch_plan),
            items_succeeded=succeeded,
            items_failed=max(len(fetch_plan) - succeeded, 0),
        )

    warnings = []
    if failures:
        warnings.append(f"yfinance daily fetch recorded {len(failures)} failures.")
    return PipelineRunResult(
        name=f"yfinance_{universe_id}_daily_ohlcv",
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
            "universe": universe_id,
            "exchange": exchange,
            "base_start": base_start.isoformat(),
            "end": end.isoformat(),
            "mapped_symbols": int(len(mapping)),
            "fetch_symbols": int(len(fetch_plan)),
            "skipped_current_symbols": int(len(skipped_plan)),
            "fetched_rows": int(len(ohlcv)),
            "failure_rows": int(len(failures)),
            "batch_size": int(max(batch_size, 1)),
            "timescale_rows": int(rows_written),
            "timescale_audit_rows": int(audits_written),
            "timescale_fetch_coverage_rows": int(fetch_coverage_rows),
            "db_snapshot_rows": int(db_snapshot_rows),
            "store_db": bool(store_db),
            "adjusted_close_storage": "deferred",
        },
        warnings=warnings,
    )


def _fetch_yfinance_daily_batches_with_controls(
    provider: YFinanceBatchProvider,
    rows: list[dict[str, Any]],
    limiter: ProviderRateLimiter,
    db: TimescaleStore | None,
    run_id: str | None,
    batch_size: int,
) -> tuple[list[pd.DataFrame], list[dict[str, str]]]:
    frames: list[pd.DataFrame] = []
    failures: list[dict[str, str]] = []
    for batch in _yfinance_batches(rows, batch_size=max(batch_size, 1)):
        start = _parse_pipeline_date(str(batch[0]["fetch_start"]), "fetch_start")
        end = _parse_pipeline_date(str(batch[0]["fetch_end"]), "fetch_end")
        symbols = [
            {
                "symbol": str(row["symbol"]),
                "instrument_key": str(row["instrument_key"]),
                "yahoo_symbol": str(row["yahoo_symbol"]),
            }
            for row in batch
        ]
        decision = limiter.acquire("yfinance", "download")
        started = perf_counter()
        status = "success"
        error_message = ""
        try:
            frame = provider.fetch_daily_ohlcv(symbols, start=start, end=end)
            if not frame.empty:
                frames.append(frame)
        except Exception as exc:
            status = "error"
            error_message = str(exc)
            for row in batch:
                failures.append(
                    {
                        "symbol": str(row["symbol"]),
                        "instrument_key": str(row["instrument_key"]),
                        "error": error_message,
                    }
                )
        finally:
            duration_ms = (perf_counter() - started) * 1000
            _record_yfinance_request(
                db=db,
                run_id=run_id,
                batch=batch,
                start=start,
                end=end,
                status=status,
                error_message=error_message,
                rate_limited=decision.rate_limited,
                wait_seconds=decision.wait_seconds,
                duration_ms=duration_ms,
            )
    return frames, failures


def _record_yfinance_request(
    db: TimescaleStore | None,
    run_id: str | None,
    batch: list[dict[str, Any]],
    start: date,
    end: date,
    status: str,
    error_message: str,
    rate_limited: bool,
    wait_seconds: float,
    duration_ms: float,
) -> None:
    if db is None:
        return
    symbols = [str(row["yahoo_symbol"]) for row in batch]
    try:
        db.insert_provider_request_logs(
            [
                {
                    "run_id": run_id,
                    "provider": "yfinance",
                    "endpoint_group": "download",
                    "request_key": f"{','.join(symbols)}:1d:{start.isoformat()}:{end.isoformat()}",
                    "instrument_key": ",".join(str(row["instrument_key"]) for row in batch),
                    "symbol": ",".join(str(row["symbol"]) for row in batch),
                    "interval": "1d",
                    "window_start": start,
                    "window_end": end,
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


def _yfinance_mapping(universe: str) -> pd.DataFrame:
    symbols = yfinance_seed_universe(universe)
    return pd.DataFrame(
        [
            {
                "symbol": item.symbol.upper(),
                "instrument_key": f"YF|{item.yahoo_symbol}",
                "trading_symbol": item.yahoo_symbol,
                "yahoo_symbol": item.yahoo_symbol,
                "currency": item.currency,
            }
            for item in symbols
            if item.yahoo_symbol
        ]
    )


def _yfinance_batches(rows: list[dict[str, Any]], batch_size: int) -> list[list[dict[str, Any]]]:
    grouped: dict[tuple[str, str], list[dict[str, Any]]] = {}
    for row in rows:
        key = (str(row["fetch_start"]), str(row["fetch_end"]))
        grouped.setdefault(key, []).append(row)
    batches: list[list[dict[str, Any]]] = []
    for key in sorted(grouped):
        group = grouped[key]
        for index in range(0, len(group), batch_size):
            batches.append(group[index : index + batch_size])
    return batches


def _exchange_for_universe(universe: str) -> str:
    universe_id = _universe_id(universe)
    if universe_id == "us_seed":
        return "US"
    if universe_id == "canada_seed":
        return "CA"
    raise ValueError(f"Unsupported yfinance universe: {universe}")


def _universe_id(universe: str) -> str:
    normalized = universe.strip().lower().replace("-", "_")
    if normalized in {"us", "usa", "united_states", "us_seed"}:
        return "us_seed"
    if normalized in {"ca", "canada", "canada_seed", "tsx_seed"}:
        return "canada_seed"
    raise ValueError(f"Unsupported yfinance universe: {universe}")


def _parse_pipeline_date(value: str, field_name: str) -> date:
    try:
        return date.fromisoformat(value)
    except ValueError as exc:
        raise ValueError(f"{field_name} must be YYYY-MM-DD: {value}") from exc


def _subtract_years(value: date, years: int) -> date:
    try:
        return value.replace(year=value.year - years)
    except ValueError:
        return value.replace(month=2, day=28, year=value.year - years)


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
