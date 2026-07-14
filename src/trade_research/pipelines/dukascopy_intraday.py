from __future__ import annotations

import json
from datetime import UTC, date, datetime, time, timedelta
from pathlib import Path
from time import perf_counter
from typing import Protocol

import pandas as pd

from trade_research.config import get_settings
from trade_research.data.dukascopy_provider import (
    DUKASCOPY_INTERVAL_5M,
    DukascopyHistoricalProvider,
    combine_tick_frames,
)
from trade_research.data.rate_limits import ProviderRateLimiter, build_provider_rate_limiter
from trade_research.pipelines.base import PipelineRunResult
from trade_research.storage import ParquetStore, TimescaleStore
from trade_research.universe import (
    DUKASCOPY_INTRADAY_UNIVERSE_ID,
    DukascopyInstrument,
    dukascopy_intraday_universe,
)


class DukascopyHourProvider(Protocol):
    def fetch_hour_ticks(
        self,
        instrument: DukascopyInstrument,
        hour_start: datetime,
    ) -> pd.DataFrame:
        ...


def run_dukascopy_intraday_ohlcv_pipeline(
    universe: str = DUKASCOPY_INTRADAY_UNIVERSE_ID,
    interval: str = DUKASCOPY_INTERVAL_5M,
    from_date: str | None = None,
    to_date: str | None = None,
    instrument: str | None = None,
    limit: int | None = None,
    max_hours: int | None = None,
    timeout_seconds: float = 15.0,
    store_db: bool = True,
    output_name: str | None = None,
    failures_output: Path | None = None,
    trigger: str = "pipeline",
    provider: DukascopyHourProvider | None = None,
) -> PipelineRunResult:
    if interval != DUKASCOPY_INTERVAL_5M:
        raise ValueError("Only interval=5m is supported for Dukascopy Phase 4.")
    end_date = _parse_pipeline_date(to_date, "to_date") if to_date else date.today()
    start_date = (
        _parse_pipeline_date(from_date, "from_date") if from_date else end_date - timedelta(days=1)
    )
    if start_date > end_date:
        raise ValueError("from_date must be on or before to_date.")

    instruments = _filter_instruments(dukascopy_intraday_universe(universe), instrument)
    if limit:
        instruments = instruments[:limit]
    hours = _hourly_windows(start_date, end_date, max_hours=max_hours)
    output_name = output_name or f"processed/intraday/{universe}_{interval}_ohlcv"
    failures_output = failures_output or Path(
        f"data/processed/intraday/{universe}_{interval}_failures.csv"
    )

    settings = get_settings()
    db = TimescaleStore(settings.database_url) if store_db else None
    if db is not None:
        db.initialize()
    run_id = None
    request_count = len(instruments) * len(hours)
    if db is not None:
        run_id = db.start_ingestion_run(
            job_name=f"{universe}_{interval}_ohlcv",
            exchange="GLOBAL",
            source="dukascopy",
            items_requested=request_count,
            run_metadata={
                "trigger": trigger,
                "universe": universe,
                "interval": interval,
                "from_date": start_date.isoformat(),
                "to_date": end_date.isoformat(),
                "instrument": instrument,
                "mapped_symbols": len(instruments),
                "max_hours": max_hours,
                "timeout_seconds": timeout_seconds,
            },
        )

    limiter = build_provider_rate_limiter(settings)
    candle_provider = provider or DukascopyHistoricalProvider(
        timeout_seconds=timeout_seconds
    )
    frames: list[pd.DataFrame] = []
    failures: list[dict[str, str]] = []
    try:
        for dukascopy_instrument in instruments:
            tick_frames = _fetch_dukascopy_tick_hours(
                instrument=dukascopy_instrument,
                hours=hours,
                provider=candle_provider,
                limiter=limiter,
                db=db,
                run_id=str(run_id) if run_id is not None else None,
                interval=interval,
                failures=failures,
            )
            candles = combine_tick_frames(
                tick_frames,
                instrument=dukascopy_instrument,
                interval=interval,
            )
            if not candles.empty:
                frames.append(candles)

        ohlcv = pd.concat(frames, ignore_index=True) if frames else pd.DataFrame()
        store = ParquetStore(settings.data_dir)
        output_path = None
        if not ohlcv.empty:
            output_path = store.write_frame(output_name, ohlcv)

        failures_frame = pd.DataFrame(
            failures,
            columns=["symbol", "instrument_key", "hour_start", "error"],
        )
        failures_output.parent.mkdir(parents=True, exist_ok=True)
        failures_frame.to_csv(failures_output, index=False)

        rows_written = (
            db.upsert_intraday_ohlcv(ohlcv, exchange="GLOBAL", source="dukascopy")
            if db is not None and not ohlcv.empty
            else 0
        )
        if db is not None and run_id is not None:
            succeeded = request_count - len(failures)
            final_status = "completed" if rows_written else "completed_empty"
            db.finish_ingestion_run(
                run_id,
                status=final_status,
                items_processed=request_count,
                items_succeeded=max(succeeded, 0),
                items_failed=len(failures),
            )
    except Exception as exc:
        if db is not None and run_id is not None:
            db.finish_ingestion_run(
                run_id,
                status="failed",
                items_processed=0,
                items_succeeded=0,
                items_failed=request_count,
                error_message=str(exc),
            )
        raise

    warnings = []
    if failures:
        warnings.append(f"Dukascopy intraday fetch recorded {len(failures)} failures.")
    return PipelineRunResult(
        name=f"{universe}_{interval}_ohlcv",
        status="warn" if failures else "pass",
        rows=int(len(ohlcv)),
        artifacts={
            **({"ohlcv": output_path} if output_path is not None else {}),
            "fetch_failures": failures_output,
        },
        metrics={
            "run_id": run_id,
            "universe": universe,
            "interval": interval,
            "mapped_symbols": int(len(instruments)),
            "from_date": start_date.isoformat(),
            "to_date": end_date.isoformat(),
            "instrument": instrument,
            "requested_hours": int(request_count),
            "max_hours": max_hours,
            "timeout_seconds": float(timeout_seconds),
            "fetched_rows": int(len(ohlcv)),
            "failure_rows": int(len(failures)),
            "timescale_rows": int(rows_written),
            "store_db": bool(store_db),
        },
        warnings=warnings,
    )


def run_dukascopy_intraday_gap_validation_pipeline(
    input_path: Path | None = None,
    input_name: str = f"processed/intraday/{DUKASCOPY_INTRADAY_UNIVERSE_ID}_5m_ohlcv",
    interval: str = DUKASCOPY_INTERVAL_5M,
    expected_start: str | None = None,
    expected_end: str | None = None,
    output_path: Path = Path(
        f"data/processed/intraday/{DUKASCOPY_INTRADAY_UNIVERSE_ID}_5m_gap_validation.csv"
    ),
    summary_output: Path = Path(
        f"data/processed/intraday/{DUKASCOPY_INTRADAY_UNIVERSE_ID}_5m_gap_validation_summary.json"
    ),
) -> PipelineRunResult:
    if interval != DUKASCOPY_INTERVAL_5M:
        raise ValueError("Only interval=5m is supported for Dukascopy gap validation.")

    settings = get_settings()
    store = ParquetStore(settings.data_dir)
    if input_path is not None:
        frame = pd.read_parquet(input_path)
    else:
        frame = store.read_frame(input_name)

    validation = build_dukascopy_intraday_gap_validation(
        frame,
        interval=interval,
        expected_start=_parse_optional_datetime(expected_start, "expected_start"),
        expected_end=_parse_optional_datetime(expected_end, "expected_end"),
    )
    summary = _intraday_gap_validation_summary(validation)

    output_path.parent.mkdir(parents=True, exist_ok=True)
    summary_output.parent.mkdir(parents=True, exist_ok=True)
    validation.to_csv(output_path, index=False)
    summary_output.write_text(json.dumps(summary, indent=2, sort_keys=True), encoding="utf-8")

    warnings = []
    blocking = []
    if summary["symbols_with_missing_rows"]:
        warnings.append(
            f"{summary['symbols_with_missing_rows']} Dukascopy symbols have missing 5m rows."
        )
    if summary["invalid_ohlcv_rows"]:
        blocking.append(
            f"{summary['invalid_ohlcv_rows']} Dukascopy rows have invalid OHLC values."
        )
    status = "fail" if blocking else "warn" if warnings else "pass"
    return PipelineRunResult(
        name=f"{DUKASCOPY_INTRADAY_UNIVERSE_ID}_{interval}_gap_validation",
        status=status,
        rows=int(len(validation)),
        artifacts={
            "gap_validation": output_path,
            "summary": summary_output,
        },
        metrics=summary,
        warnings=warnings,
        blocking_issues=blocking,
    )


def build_dukascopy_intraday_gap_validation(
    frame: pd.DataFrame,
    interval: str = DUKASCOPY_INTERVAL_5M,
    expected_start: datetime | None = None,
    expected_end: datetime | None = None,
) -> pd.DataFrame:
    if frame.empty:
        return pd.DataFrame(
            columns=[
                "instrument_key",
                "symbol",
                "exchange",
                "asset_class",
                "interval",
                "start_ts",
                "end_ts",
                "expected_rows",
                "observed_rows",
                "missing_rows",
                "duplicate_rows",
                "invalid_ohlcv_rows",
                "coverage_pct",
                "status",
            ]
        )
    required = {"InstrumentKey", "Symbol", "Timestamp", "Open", "High", "Low", "Close"}
    missing_columns = required.difference(frame.columns)
    if missing_columns:
        raise ValueError(f"Missing Dukascopy intraday columns: {sorted(missing_columns)}")

    delta = _interval_delta(interval)
    data = frame.copy()
    data["Timestamp"] = pd.to_datetime(data["Timestamp"], utc=True)
    rows = []
    for instrument_key, group in data.groupby("InstrumentKey"):
        group = group.sort_values("Timestamp")
        symbol = str(group["Symbol"].iloc[0])
        exchange = str(group.get("Exchange", pd.Series([""])).iloc[0] or "")
        asset_class = str(group.get("AssetClass", pd.Series([""])).iloc[0] or "")
        start = expected_start or group["Timestamp"].min().to_pydatetime()
        end_exclusive = expected_end or (group["Timestamp"].max().to_pydatetime() + delta)
        expected_index = pd.date_range(
            start=pd.Timestamp(start).tz_convert("UTC"),
            end=pd.Timestamp(end_exclusive - delta).tz_convert("UTC"),
            freq=delta,
        )
        observed = set(group["Timestamp"])
        expected = set(expected_index)
        duplicate_rows = int(group.duplicated(subset=["Timestamp"]).sum())
        invalid_rows = int(
            (
                (group["Close"] <= 0)
                | (group["High"] < group["Low"])
                | group[["Open", "High", "Low", "Close"]].isna().any(axis=1)
            ).sum()
        )
        expected_rows = len(expected)
        observed_rows = len(observed.intersection(expected))
        missing_rows = max(expected_rows - observed_rows, 0)
        coverage_pct = min(observed_rows / expected_rows, 1.0) if expected_rows else 0.0
        status = "pass"
        if invalid_rows:
            status = "fail"
        elif missing_rows or duplicate_rows:
            status = "warn"
        rows.append(
            {
                "instrument_key": str(instrument_key),
                "symbol": symbol,
                "exchange": exchange,
                "asset_class": asset_class,
                "interval": interval,
                "start_ts": pd.Timestamp(start).isoformat(),
                "end_ts": pd.Timestamp(end_exclusive - delta).isoformat(),
                "expected_rows": expected_rows,
                "observed_rows": observed_rows,
                "missing_rows": missing_rows,
                "duplicate_rows": duplicate_rows,
                "invalid_ohlcv_rows": invalid_rows,
                "coverage_pct": coverage_pct,
                "status": status,
            }
        )
    return pd.DataFrame(rows).sort_values("instrument_key").reset_index(drop=True)


def _fetch_dukascopy_tick_hours(
    instrument: DukascopyInstrument,
    hours: list[datetime],
    provider: DukascopyHourProvider,
    limiter: ProviderRateLimiter,
    db: TimescaleStore | None,
    run_id: str | None,
    interval: str,
    failures: list[dict[str, str]],
) -> list[pd.DataFrame]:
    frames = []
    for hour_start in hours:
        decision = limiter.acquire("dukascopy", "historical")
        started = perf_counter()
        status = "success"
        error_message = ""
        try:
            frame = provider.fetch_hour_ticks(instrument, hour_start)
            if not frame.empty:
                frames.append(frame)
        except Exception as exc:
            status = "error"
            error_message = str(exc)
            failures.append(
                {
                    "symbol": instrument.symbol,
                    "instrument_key": instrument.instrument_key,
                    "hour_start": hour_start.isoformat(),
                    "error": error_message,
                }
            )
        finally:
            duration_ms = (perf_counter() - started) * 1000
            _record_dukascopy_request(
                db=db,
                run_id=run_id,
                instrument=instrument,
                hour_start=hour_start,
                interval=interval,
                status=status,
                error_message=error_message,
                rate_limited=decision.rate_limited,
                wait_seconds=decision.wait_seconds,
                duration_ms=duration_ms,
            )
    return frames


def _record_dukascopy_request(
    db: TimescaleStore | None,
    run_id: str | None,
    instrument: DukascopyInstrument,
    hour_start: datetime,
    interval: str,
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
                    "provider": "dukascopy",
                    "endpoint_group": "historical",
                    "request_key": (
                        f"{instrument.dukascopy_id}:{interval}:{hour_start.isoformat()}"
                    ),
                    "instrument_key": instrument.instrument_key,
                    "symbol": instrument.symbol,
                    "interval": interval,
                    "window_start": hour_start.date(),
                    "window_end": hour_start.date(),
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


def _filter_instruments(
    instruments: list[DukascopyInstrument],
    instrument: str | None,
) -> list[DukascopyInstrument]:
    if not instrument:
        return instruments
    needle = instrument.strip().upper().replace("_", "/")
    normalized = needle.replace("/", "")
    selected = [
        item
        for item in instruments
        if item.symbol.upper() == needle
        or item.symbol.upper().replace("/", "") == normalized
        or item.dukascopy_id.upper() == normalized
        or item.instrument_key.upper().endswith(f"|{normalized}")
    ]
    if not selected:
        supported = ", ".join(item.symbol for item in instruments)
        raise ValueError(f"Unsupported Dukascopy instrument {instrument!r}. Supported: {supported}")
    return selected


def _hourly_windows(
    start_date: date,
    end_date: date,
    max_hours: int | None = None,
) -> list[datetime]:
    current = datetime.combine(start_date, time.min, tzinfo=UTC)
    end = datetime.combine(end_date + timedelta(days=1), time.min, tzinfo=UTC)
    hours = []
    while current < end:
        hours.append(current)
        if max_hours is not None and len(hours) >= max_hours:
            break
        current += timedelta(hours=1)
    return hours


def _parse_pipeline_date(value: str, field_name: str) -> date:
    try:
        return date.fromisoformat(value)
    except ValueError as exc:
        raise ValueError(f"{field_name} must be YYYY-MM-DD: {value}") from exc


def _parse_optional_datetime(value: str | None, field_name: str) -> datetime | None:
    if value is None:
        return None
    try:
        parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError as exc:
        raise ValueError(f"{field_name} must be ISO datetime: {value}") from exc
    if parsed.tzinfo is None:
        return parsed.replace(tzinfo=UTC)
    return parsed.astimezone(UTC)


def _interval_delta(interval: str) -> timedelta:
    if interval == DUKASCOPY_INTERVAL_5M:
        return timedelta(minutes=5)
    raise ValueError(f"Unsupported Dukascopy interval: {interval}")


def _intraday_gap_validation_summary(validation: pd.DataFrame) -> dict[str, int | float]:
    if validation.empty:
        return {
            "symbols_total": 0,
            "symbols_pass": 0,
            "symbols_warn": 0,
            "symbols_fail": 0,
            "expected_rows": 0,
            "observed_rows": 0,
            "missing_rows": 0,
            "duplicate_rows": 0,
            "invalid_ohlcv_rows": 0,
            "symbols_with_missing_rows": 0,
            "coverage_pct": 0.0,
        }
    expected_rows = int(validation["expected_rows"].sum())
    observed_rows = int(validation["observed_rows"].sum())
    return {
        "symbols_total": int(len(validation)),
        "symbols_pass": int(validation["status"].eq("pass").sum()),
        "symbols_warn": int(validation["status"].eq("warn").sum()),
        "symbols_fail": int(validation["status"].eq("fail").sum()),
        "expected_rows": expected_rows,
        "observed_rows": observed_rows,
        "missing_rows": int(validation["missing_rows"].sum()),
        "duplicate_rows": int(validation["duplicate_rows"].sum()),
        "invalid_ohlcv_rows": int(validation["invalid_ohlcv_rows"].sum()),
        "symbols_with_missing_rows": int((validation["missing_rows"] > 0).sum()),
        "coverage_pct": min(observed_rows / expected_rows, 1.0) if expected_rows else 0.0,
    }
