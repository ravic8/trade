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
from trade_research.market_calendar import (
    ExchangeHolidays,
    expected_trading_dates,
    fetch_exchange_holidays,
)
from trade_research.pipelines.base import PipelineRunResult
from trade_research.pipelines.daily_ohlcv import (
    build_daily_fetch_coverage,
    plan_daily_fetch_windows,
)
from trade_research.storage import ParquetStore, TimescaleStore
from trade_research.universe import (
    yfinance_exchange_for_universe,
    yfinance_universe,
    yfinance_universe_id,
)


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
                "adjusted_close_storage": "price_adjustments_daily",
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
    price_adjustment_rows = (
        db.upsert_daily_price_adjustments(ohlcv, exchange=exchange, source="yfinance")
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
            "timescale_price_adjustment_rows": int(price_adjustment_rows),
            "timescale_audit_rows": int(audits_written),
            "timescale_fetch_coverage_rows": int(fetch_coverage_rows),
            "db_snapshot_rows": int(db_snapshot_rows),
            "store_db": bool(store_db),
            "adjusted_close_storage": "price_adjustments_daily",
        },
        warnings=warnings,
    )


def run_yfinance_missing_ohlcv_pipeline(
    universe: str,
    from_date: str,
    to_date: str,
    coverage_status: str | None = None,
    min_avg_daily_turnover: float | None = None,
    min_coverage_pct: float | None = None,
    limit: int | None = None,
    batch_size: int = 25,
    export_db_snapshot: bool = True,
    provider: YFinanceBatchProvider | None = None,
) -> PipelineRunResult:
    settings = get_settings()
    exchange = _exchange_for_universe(universe)
    universe_id = _universe_id(universe)
    start = _parse_pipeline_date(from_date, "from_date")
    end = _parse_pipeline_date(to_date, "to_date")
    if start > end:
        raise ValueError("from_date must be on or before to_date.")
    if coverage_status and coverage_status.lower() not in {"complete", "partial", "empty"}:
        raise ValueError("coverage_status must be complete, partial, or empty.")

    mapping = _yfinance_mapping(universe)
    db = TimescaleStore(settings.database_url)
    db.initialize()
    _ensure_exchange_holidays(db, exchange, start, end)
    expected_dates = _expected_daily_dates(db, exchange, start, end)
    expected_set = set(expected_dates)
    expected_rows = len(expected_dates)
    instrument_keys = [str(key) for key in mapping["instrument_key"].dropna().tolist()]
    stored_dates = db.daily_ohlcv_dates_by_instrument(
        instrument_keys,
        start,
        end,
        source="yfinance",
        exchange=exchange,
    )
    avg_turnover = db.daily_ohlcv_average_turnover_by_instrument(
        instrument_keys,
        start,
        end,
        source="yfinance",
        exchange=exchange,
    )
    fetch_plan = _build_yfinance_missing_fetch_plan(
        mapping=mapping,
        expected_set=expected_set,
        expected_rows=expected_rows,
        stored_dates=stored_dates,
        avg_turnover=avg_turnover,
        coverage_status=coverage_status,
        min_avg_daily_turnover=min_avg_daily_turnover,
        min_coverage_pct=min_coverage_pct,
        limit=limit,
    )
    planned = fetch_plan.copy()
    if not planned.empty:
        planned["should_fetch"] = True
        planned["skip_reason"] = ""
        planned["latest_stored_date"] = None

    output_name = f"processed/equities/yfinance_{universe_id}_daily_ohlcv"
    incremental_output_name = (
        f"processed/equities/yfinance_{universe_id}_missing_daily_ohlcv_incremental"
    )
    failures_output = Path(
        f"data/processed/equities/yfinance_{universe_id}_missing_daily_ohlcv_failures.csv"
    )
    fetch_coverage_output = Path(
        f"data/processed/equities/yfinance_{universe_id}_missing_daily_ohlcv_fetch_coverage.csv"
    )
    audit_output = Path(
        f"data/processed/equities/yfinance_{universe_id}_missing_daily_ohlcv_audit.csv"
    )

    run_id = db.start_ingestion_run(
        job_name=f"yfinance_{universe_id}_missing_daily_ohlcv",
        exchange=exchange,
        source="yfinance",
        items_requested=len(fetch_plan),
        run_metadata={
            "trigger": "cli",
            "universe": universe_id,
            "mode": "filtered_missing_windows",
            "base_start": start.isoformat(),
            "end": end.isoformat(),
            "coverage_status": coverage_status,
            "min_avg_daily_turnover": min_avg_daily_turnover,
            "min_coverage_pct": min_coverage_pct,
            "batch_size": max(batch_size, 1),
            "adjusted_close_storage": "price_adjustments_daily",
        },
    )
    limiter = build_provider_rate_limiter(settings)
    candle_provider = provider or YFinanceDailyProvider(auto_adjust=False)
    frames, failures = _fetch_yfinance_daily_batches_with_controls(
        provider=candle_provider,
        rows=fetch_plan.to_dict(orient="records"),
        limiter=limiter,
        db=db,
        run_id=str(run_id),
        batch_size=max(batch_size, 1),
    )
    ohlcv = pd.concat(frames, ignore_index=True) if frames else pd.DataFrame()
    failures_frame = pd.DataFrame(failures, columns=["symbol", "instrument_key", "error"])
    fetch_coverage = build_daily_fetch_coverage(planned, ohlcv, failures_frame)
    audit = (
        audit_daily_ohlcv(ohlcv, fetch_plan)
        if not fetch_plan.empty
        else _empty_daily_audit_frame()
    )

    store = ParquetStore(settings.data_dir)
    output_path = None
    if not ohlcv.empty:
        output_path = store.write_frame(incremental_output_name, ohlcv)

    audit_output.parent.mkdir(parents=True, exist_ok=True)
    failures_output.parent.mkdir(parents=True, exist_ok=True)
    fetch_coverage_output.parent.mkdir(parents=True, exist_ok=True)
    audit.to_csv(audit_output, index=False)
    failures_frame.to_csv(failures_output, index=False)
    fetch_coverage.to_csv(fetch_coverage_output, index=False)

    rows_written = (
        db.upsert_daily_ohlcv(ohlcv, exchange=exchange, source="yfinance")
        if not ohlcv.empty
        else 0
    )
    price_adjustment_rows = (
        db.upsert_daily_price_adjustments(ohlcv, exchange=exchange, source="yfinance")
        if not ohlcv.empty
        else 0
    )
    audits_written = (
        db.insert_data_quality_audits(
            audit,
            dataset_name=f"yfinance_{universe_id}_missing_daily_ohlcv",
            source="yfinance",
            interval="1d",
        )
        if not audit.empty
        else 0
    )
    fetch_coverage_rows = (
        db.insert_daily_ohlcv_fetch_coverage(
            str(run_id),
            fetch_coverage,
            source="yfinance",
            exchange=exchange,
        )
        if not fetch_coverage.empty
        else 0
    )
    db_snapshot_rows = 0
    if export_db_snapshot:
        snapshot = db.daily_ohlcv_frame(exchange=exchange, source="yfinance")
        if not snapshot.empty:
            output_path = store.write_frame(output_name, snapshot)
            db_snapshot_rows = int(len(snapshot))

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
        warnings.append(f"yfinance missing fetch recorded {len(failures)} failures.")
    return PipelineRunResult(
        name=f"yfinance_{universe_id}_missing_daily_ohlcv",
        status="warn" if failures else "pass",
        rows=int(len(ohlcv)),
        artifacts={
            **({"ohlcv": output_path} if output_path is not None else {}),
            "daily_audit": audit_output,
            "fetch_failures": failures_output,
            "fetch_coverage": fetch_coverage_output,
        },
        metrics={
            "run_id": run_id,
            "universe": universe_id,
            "exchange": exchange,
            "base_start": start.isoformat(),
            "end": end.isoformat(),
            "mapped_symbols": int(len(mapping)),
            "fetch_symbols": (
                int(fetch_plan["instrument_key"].nunique()) if not fetch_plan.empty else 0
            ),
            "fetch_windows": int(len(fetch_plan)),
            "fetched_rows": int(len(ohlcv)),
            "failure_rows": int(len(failures)),
            "batch_size": int(max(batch_size, 1)),
            "timescale_rows": int(rows_written),
            "timescale_price_adjustment_rows": int(price_adjustment_rows),
            "timescale_audit_rows": int(audits_written),
            "timescale_fetch_coverage_rows": int(fetch_coverage_rows),
            "db_snapshot_rows": int(db_snapshot_rows),
            "store_db": True,
            "coverage_status": coverage_status,
            "min_avg_daily_turnover": min_avg_daily_turnover,
            "min_coverage_pct": min_coverage_pct,
            "adjusted_close_storage": "price_adjustments_daily",
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
    symbols = yfinance_universe(universe)
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
    return yfinance_exchange_for_universe(universe)


def _universe_id(universe: str) -> str:
    return yfinance_universe_id(universe)


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


def _build_yfinance_missing_fetch_plan(
    mapping: pd.DataFrame,
    expected_set: set[date],
    expected_rows: int,
    stored_dates: dict[str, set[date]],
    avg_turnover: dict[str, float],
    coverage_status: str | None,
    min_avg_daily_turnover: float | None,
    min_coverage_pct: float | None,
    limit: int | None,
) -> pd.DataFrame:
    normalized_status = coverage_status.lower() if coverage_status else None
    rows: list[dict[str, Any]] = []
    for item in mapping.to_dict(orient="records"):
        key = str(item["instrument_key"])
        present_dates = expected_set.intersection(stored_dates.get(key, set()))
        missing_dates = sorted(expected_set.difference(present_dates))
        stored_count = len(present_dates)
        coverage_pct = min(stored_count / expected_rows, 1.0) if expected_rows else 0.0
        status = _coverage_status(stored_count, expected_rows)
        turnover = avg_turnover.get(key)

        if normalized_status and status != normalized_status:
            continue
        if min_avg_daily_turnover is not None and (
            turnover is None or turnover < min_avg_daily_turnover
        ):
            continue
        if min_coverage_pct is not None and coverage_pct < min_coverage_pct:
            continue
        if not missing_dates:
            continue

        for window_start, window_end, window_dates in _contiguous_date_windows(
            missing_dates
        ):
            rows.append(
                {
                    "symbol": str(item["symbol"]),
                    "instrument_key": key,
                    "trading_symbol": str(item["trading_symbol"]),
                    "yahoo_symbol": str(item["yahoo_symbol"]),
                    "fetch_start": window_start.isoformat(),
                    "fetch_end": window_end.isoformat(),
                    "missing_rows": len(window_dates),
                    "stored_rows": stored_count,
                    "expected_rows": expected_rows,
                    "coverage_pct": coverage_pct,
                    "coverage_status": status,
                    "avg_daily_turnover": turnover,
                }
            )
            if limit is not None and len(rows) >= limit:
                return pd.DataFrame(rows)
    return pd.DataFrame(rows)


def _coverage_status(stored_rows: int, expected_rows: int) -> str:
    if stored_rows == 0:
        return "empty"
    if expected_rows > 0 and stored_rows >= expected_rows:
        return "complete"
    return "partial"


def _contiguous_date_windows(missing_dates: list[date]) -> list[tuple[date, date, list[date]]]:
    if not missing_dates:
        return []
    windows = []
    current = [missing_dates[0]]
    for candle_date in missing_dates[1:]:
        if candle_date == current[-1] + timedelta(days=1):
            current.append(candle_date)
            continue
        windows.append((current[0], current[-1], current))
        current = [candle_date]
    windows.append((current[0], current[-1], current))
    return windows


def _ensure_exchange_holidays(
    store: TimescaleStore,
    exchange: str,
    start_date: date,
    end_date: date,
) -> None:
    if exchange.upper() not in {"NSE", "TSX", "US", "CA"}:
        return

    for year in range(start_date.year, end_date.year + 1):
        cached = store.exchange_holidays(exchange, year)
        if cached is not None and (
            cached.get("closed_dates")
            or cached.get("early_close_dates")
            or cached.get("source_url")
        ):
            continue
        holidays = fetch_exchange_holidays(exchange, year)
        store.upsert_exchange_holidays(
            exchange=exchange,
            year=year,
            closed_dates=holidays.closed_dates,
            early_close_dates=holidays.early_close_dates,
            source_url=holidays.source_url,
        )


def _expected_daily_dates(
    store: TimescaleStore,
    exchange: str,
    start_date: date,
    end_date: date,
) -> list[date]:
    if exchange.upper() not in {"NSE", "TSX", "US", "CA"}:
        trading_dates = []
        current = start_date
        while current <= end_date:
            if current.weekday() < 5:
                trading_dates.append(current)
            current += timedelta(days=1)
        return trading_dates
    holidays = _stored_exchange_holidays(store, exchange, start_date, end_date)
    return expected_trading_dates(exchange, start_date, end_date, holidays=holidays)


def _stored_exchange_holidays(
    store: TimescaleStore,
    exchange: str,
    start_date: date,
    end_date: date,
) -> ExchangeHolidays | None:
    closed_dates: set[date] = set()
    early_close_dates: set[date] = set()
    source_url = ""
    for year in range(start_date.year, end_date.year + 1):
        row = store.exchange_holidays(exchange, year)
        if row is None:
            return None
        source_url = str(row.get("source_url") or source_url)
        closed_dates.update(
            date.fromisoformat(value) for value in row.get("closed_dates", [])
        )
        early_close_dates.update(
            date.fromisoformat(value) for value in row.get("early_close_dates", [])
        )
    return ExchangeHolidays(
        closed_dates=frozenset(closed_dates),
        early_close_dates=frozenset(early_close_dates),
        source_url=source_url,
    )
