from __future__ import annotations

from datetime import UTC, datetime, timedelta
from pathlib import Path
from time import perf_counter
from typing import Protocol

import pandas as pd

from trade_research.config import get_settings
from trade_research.data.rate_limits import ProviderRateLimiter, build_provider_rate_limiter
from trade_research.data.yfinance_provider import YFinanceIntradayProvider
from trade_research.pipelines.base import PipelineRunResult
from trade_research.storage import ParquetStore, TimescaleStore
from trade_research.universe import (
    YFINANCE_INTRADAY_UNIVERSE_ID,
    YFinanceIntradayInstrument,
    yfinance_intraday_universe,
)


class YFinanceIntradayBatchProvider(Protocol):
    def fetch_intraday_ohlcv(
        self,
        instruments: list[YFinanceIntradayInstrument],
        start: datetime,
        end: datetime,
        interval: str = "5m",
    ) -> pd.DataFrame:
        ...


def run_yfinance_intraday_ohlcv_pipeline(
    universe: str = YFINANCE_INTRADAY_UNIVERSE_ID,
    interval: str = "5m",
    from_datetime: str | None = None,
    to_datetime: str | None = None,
    instrument: str | None = None,
    limit: int | None = None,
    store_db: bool = True,
    output_name: str | None = None,
    failures_output: Path | None = None,
    trigger: str = "pipeline",
    provider: YFinanceIntradayBatchProvider | None = None,
) -> PipelineRunResult:
    if interval != "5m":
        raise ValueError("Only interval=5m is supported for yfinance intraday.")
    end = _parse_pipeline_datetime(to_datetime, "to_datetime") if to_datetime else _utc_now_floor()
    start = (
        _parse_pipeline_datetime(from_datetime, "from_datetime")
        if from_datetime
        else end - timedelta(days=5)
    )
    if start >= end:
        raise ValueError("from_datetime must be before to_datetime.")

    instruments = _filter_instruments(yfinance_intraday_universe(universe), instrument)
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
    if db is not None:
        run_id = db.start_ingestion_run(
            job_name=f"{universe}_{interval}_ohlcv",
            exchange="GLOBAL",
            source="yfinance",
            items_requested=len(instruments),
            run_metadata={
                "trigger": trigger,
                "universe": universe,
                "interval": interval,
                "from_datetime": start.isoformat(),
                "to_datetime": end.isoformat(),
                "instrument": instrument,
                "mapped_symbols": len(instruments),
                "provider_note": "Yahoo intraday history is limited and best-effort.",
            },
        )

    limiter = build_provider_rate_limiter(settings)
    candle_provider = provider or YFinanceIntradayProvider(auto_adjust=False)
    failures: list[dict[str, str]] = []
    ohlcv = _fetch_yfinance_intraday_with_controls(
        provider=candle_provider,
        instruments=instruments,
        start=start,
        end=end,
        interval=interval,
        limiter=limiter,
        db=db,
        run_id=str(run_id) if run_id is not None else None,
        failures=failures,
    )

    store = ParquetStore(settings.data_dir)
    output_path = None
    if not ohlcv.empty:
        output_path = store.write_frame(output_name, ohlcv)

    failures_frame = pd.DataFrame(failures, columns=["symbol", "instrument_key", "error"])
    failures_output.parent.mkdir(parents=True, exist_ok=True)
    failures_frame.to_csv(failures_output, index=False)

    rows_written = (
        db.upsert_intraday_ohlcv(ohlcv, exchange="GLOBAL", source="yfinance")
        if db is not None and not ohlcv.empty
        else 0
    )
    if db is not None and run_id is not None:
        status = "completed" if rows_written else "completed_empty"
        db.finish_ingestion_run(
            run_id,
            status=status,
            items_processed=len(instruments),
            items_succeeded=0 if failures else len(instruments),
            items_failed=len(failures),
        )

    warnings = []
    if failures:
        warnings.append(f"yfinance intraday fetch recorded {len(failures)} failures.")
    if ohlcv.empty:
        warnings.append("No yfinance intraday rows were returned.")
    return PipelineRunResult(
        name=f"{universe}_{interval}_ohlcv",
        status="warn" if warnings else "pass",
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
            "from_datetime": start.isoformat(),
            "to_datetime": end.isoformat(),
            "instrument": instrument,
            "fetched_rows": int(len(ohlcv)),
            "failure_rows": int(len(failures)),
            "timescale_rows": int(rows_written),
            "store_db": bool(store_db),
            "source": "yfinance",
        },
        warnings=warnings,
    )


def _fetch_yfinance_intraday_with_controls(
    provider: YFinanceIntradayBatchProvider,
    instruments: list[YFinanceIntradayInstrument],
    start: datetime,
    end: datetime,
    interval: str,
    limiter: ProviderRateLimiter,
    db: TimescaleStore | None,
    run_id: str | None,
    failures: list[dict[str, str]],
) -> pd.DataFrame:
    decision = limiter.acquire("yfinance", "intraday_download")
    started = perf_counter()
    status = "success"
    error_message = ""
    try:
        return provider.fetch_intraday_ohlcv(
            instruments,
            start=start,
            end=end,
            interval=interval,
        )
    except Exception as exc:
        status = "error"
        error_message = str(exc)
        for instrument in instruments:
            failures.append(
                {
                    "symbol": instrument.symbol,
                    "instrument_key": instrument.instrument_key,
                    "error": error_message,
                }
            )
        return pd.DataFrame()
    finally:
        duration_ms = (perf_counter() - started) * 1000
        _record_yfinance_intraday_request(
            db=db,
            run_id=run_id,
            instruments=instruments,
            start=start,
            end=end,
            interval=interval,
            status=status,
            error_message=error_message,
            rate_limited=decision.rate_limited,
            wait_seconds=decision.wait_seconds,
            duration_ms=duration_ms,
        )


def _record_yfinance_intraday_request(
    db: TimescaleStore | None,
    run_id: str | None,
    instruments: list[YFinanceIntradayInstrument],
    start: datetime,
    end: datetime,
    interval: str,
    status: str,
    error_message: str,
    rate_limited: bool,
    wait_seconds: float,
    duration_ms: float,
) -> None:
    if db is None:
        return
    symbols = [item.symbol for item in instruments]
    try:
        db.insert_provider_request_logs(
            [
                {
                    "run_id": run_id,
                    "provider": "yfinance",
                    "endpoint_group": "intraday_download",
                    "request_key": (
                        f"{','.join(item.yahoo_symbol for item in instruments)}:"
                        f"{interval}:{start.isoformat()}:{end.isoformat()}"
                    ),
                    "instrument_key": ",".join(item.instrument_key for item in instruments),
                    "symbol": ",".join(symbols),
                    "interval": interval,
                    "window_start": start.date(),
                    "window_end": end.date(),
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
    instruments: list[YFinanceIntradayInstrument],
    instrument: str | None,
) -> list[YFinanceIntradayInstrument]:
    if not instrument:
        return instruments
    needle = instrument.strip().upper().replace("_", "/")
    normalized = needle.replace("/", "")
    selected = [
        item
        for item in instruments
        if item.symbol.upper() == needle
        or item.symbol.upper().replace("/", "") == normalized
        or item.yahoo_symbol.upper() == instrument.strip().upper()
        or item.instrument_key.upper().endswith(f"|{instrument.strip().upper()}")
    ]
    if not selected:
        supported = ", ".join(item.symbol for item in instruments)
        raise ValueError(
            f"Unsupported yfinance intraday instrument {instrument!r}. "
            f"Supported: {supported}"
        )
    return selected


def _parse_pipeline_datetime(value: str, field_name: str) -> datetime:
    try:
        parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError as exc:
        raise ValueError(f"{field_name} must be ISO datetime: {value}") from exc
    if parsed.tzinfo is None:
        return parsed.replace(tzinfo=UTC)
    return parsed.astimezone(UTC)


def _utc_now_floor() -> datetime:
    now = datetime.now(UTC)
    return now.replace(second=0, microsecond=0)
