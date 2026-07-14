from __future__ import annotations

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
    limit: int | None = None,
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

    instruments = dukascopy_intraday_universe(universe)
    if limit:
        instruments = instruments[:limit]
    output_name = output_name or f"processed/intraday/{universe}_{interval}_ohlcv"
    failures_output = failures_output or Path(
        f"data/processed/intraday/{universe}_{interval}_failures.csv"
    )

    settings = get_settings()
    db = TimescaleStore(settings.database_url) if store_db else None
    if db is not None:
        db.initialize()
    run_id = None
    request_count = len(instruments) * len(_hourly_windows(start_date, end_date))
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
                "mapped_symbols": len(instruments),
            },
        )

    limiter = build_provider_rate_limiter(settings)
    candle_provider = provider or DukascopyHistoricalProvider()
    frames: list[pd.DataFrame] = []
    failures: list[dict[str, str]] = []
    try:
        for instrument in instruments:
            tick_frames = _fetch_dukascopy_tick_hours(
                instrument=instrument,
                hours=_hourly_windows(start_date, end_date),
                provider=candle_provider,
                limiter=limiter,
                db=db,
                run_id=str(run_id) if run_id is not None else None,
                interval=interval,
                failures=failures,
            )
            candles = combine_tick_frames(tick_frames, instrument=instrument, interval=interval)
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
            "requested_hours": int(request_count),
            "fetched_rows": int(len(ohlcv)),
            "failure_rows": int(len(failures)),
            "timescale_rows": int(rows_written),
            "store_db": bool(store_db),
        },
        warnings=warnings,
    )


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


def _hourly_windows(start_date: date, end_date: date) -> list[datetime]:
    current = datetime.combine(start_date, time.min, tzinfo=UTC)
    end = datetime.combine(end_date + timedelta(days=1), time.min, tzinfo=UTC)
    hours = []
    while current < end:
        hours.append(current)
        current += timedelta(hours=1)
    return hours


def _parse_pipeline_date(value: str, field_name: str) -> date:
    try:
        return date.fromisoformat(value)
    except ValueError as exc:
        raise ValueError(f"{field_name} must be YYYY-MM-DD: {value}") from exc
