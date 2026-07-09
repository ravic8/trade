from __future__ import annotations

import time
from concurrent.futures import ThreadPoolExecutor
from contextlib import nullcontext
from dataclasses import dataclass
from datetime import date
from typing import Any, Protocol
from uuid import UUID

import pandas as pd

from trade_research.data.coverage import (
    CoveragePreviewInput,
    DailyCoverageStore,
    build_daily_coverage_preview,
)
from trade_research.data.upstox import UpstoxHistoricalDataProvider, audit_daily_ohlcv
from trade_research.pipelines.daily_ohlcv import build_daily_fetch_coverage


class DailyOhlcvExecutionStore(DailyCoverageStore, Protocol):
    def start_ingestion_run(
        self,
        job_name: str,
        exchange: str,
        source: str,
        items_requested: int,
        run_metadata: dict[str, Any] | None = None,
    ) -> UUID:
        ...

    def finish_ingestion_run(
        self,
        run_id: UUID,
        status: str,
        items_processed: int,
        items_succeeded: int,
        items_failed: int,
        error_message: str | None = None,
    ) -> None:
        ...

    def upsert_daily_ohlcv(
        self,
        frame: pd.DataFrame,
        exchange: str = "NSE",
        source: str = "upstox",
    ) -> int:
        ...

    def insert_daily_ohlcv_fetch_coverage(
        self,
        run_id: str,
        coverage: pd.DataFrame,
        source: str = "upstox",
        exchange: str = "NSE",
    ) -> int:
        ...

    def insert_data_quality_audits(
        self,
        audit: pd.DataFrame,
        dataset_name: str,
        source: str,
        interval: str,
    ) -> int:
        ...


class DailyCandleProvider(Protocol):
    def fetch_daily_candles(
        self,
        instrument_key: str,
        start: date,
        end: date,
        symbol: str,
        trading_symbol: str | None = None,
    ) -> pd.DataFrame:
        ...


@dataclass(frozen=True)
class DailyOhlcvExecutionResult:
    run_id: str
    preview: dict[str, Any]
    status: str
    rows_fetched: int
    rows_upserted: int
    fetch_coverage_rows: int
    audit_rows: int
    failures: list[dict[str, str]]
    max_concurrent_fetches: int


def run_daily_ohlcv_request(
    request: CoveragePreviewInput,
    store: DailyOhlcvExecutionStore,
    access_token: str | None,
    provider: DailyCandleProvider | None = None,
    throttle_seconds: float = 0.0,
    max_concurrent_fetches: int = 1,
) -> DailyOhlcvExecutionResult:
    preview = build_daily_coverage_preview(request, store)
    tasks = _consolidate_tasks_by_instrument(preview["tasks"])
    if tasks and not access_token and provider is None:
        raise ValueError("Set UPSTOX_ACCESS_TOKEN before running a data pipeline request.")

    run_id = store.start_ingestion_run(
        job_name="upstox_nse_daily_ohlcv",
        exchange=request.exchange,
        source=request.provider,
        items_requested=len(tasks),
        run_metadata={
            "trigger": "ui",
            "mode": "incremental_missing_only",
            "unit": request.unit,
            "interval": request.interval,
            "start_date": request.start_date.isoformat(),
            "end_date": request.end_date.isoformat(),
            "symbols": list(request.symbols),
            "symbols_resolved": preview["symbols_resolved"],
            "expected_rows": preview["expected_rows"],
            "already_present_rows": preview["already_present_rows"],
            "missing_rows": preview["missing_rows"],
            "estimated_provider_calls": preview["estimated_provider_calls"],
            "max_concurrent_fetches": max(max_concurrent_fetches, 1),
            "warnings": preview["warnings"],
        },
    )

    frames: list[pd.DataFrame] = []
    failures: list[dict[str, str]] = []
    rows_upserted = 0
    try:
        provider_context = (
            nullcontext(provider)
            if provider is not None
            else UpstoxHistoricalDataProvider(access_token or "")
        )
        with provider_context as candle_provider:
            fetch_results = _fetch_daily_tasks(
                tasks,
                candle_provider,
                throttle_seconds=throttle_seconds,
                max_concurrent_fetches=max_concurrent_fetches,
            )
            for result in fetch_results:
                if result["error"]:
                    failures.append(
                        {
                            "symbol": str(result["task"]["symbol"]),
                            "instrument_key": str(result["task"]["instrument_key"]),
                            "error": str(result["error"]),
                        }
                    )
                    continue
                frame = result["frame"]
                if not frame.empty:
                    frames.append(frame)
                    rows_upserted += store.upsert_daily_ohlcv(
                        frame,
                        exchange=request.exchange,
                        source=request.provider,
                    )

        fetched = pd.concat(frames, ignore_index=True) if frames else pd.DataFrame()
        planned = _tasks_to_plan(tasks)
        failures_frame = pd.DataFrame(failures, columns=["symbol", "instrument_key", "error"])
        fetch_coverage = build_daily_fetch_coverage(planned, fetched, failures_frame)
        audit = (
            audit_daily_ohlcv(fetched, planned)
            if not planned.empty
            else _empty_daily_audit_frame()
        )
        fetch_coverage_rows = (
            store.insert_daily_ohlcv_fetch_coverage(
                str(run_id),
                fetch_coverage,
                source=request.provider,
                exchange=request.exchange,
            )
            if not fetch_coverage.empty
            else 0
        )
        audit_rows = (
            store.insert_data_quality_audits(
                audit,
                dataset_name="nse_daily_ohlcv",
                source=request.provider,
                interval="1d",
            )
            if not audit.empty
            else 0
        )

        status = _run_status(tasks, fetch_coverage, failures)
        succeeded = (
            int(fetch_coverage["fetch_status"].eq("fetched").sum())
            if not fetch_coverage.empty
            else 0
        )
        store.finish_ingestion_run(
            run_id,
            status=status,
            items_processed=len(tasks),
            items_succeeded=succeeded,
            items_failed=max(len(tasks) - succeeded, 0),
        )
        return DailyOhlcvExecutionResult(
            run_id=str(run_id),
            preview=preview,
            status=status,
            rows_fetched=int(len(fetched)),
            rows_upserted=int(rows_upserted),
            fetch_coverage_rows=int(fetch_coverage_rows),
            audit_rows=int(audit_rows),
            failures=failures,
            max_concurrent_fetches=max(max_concurrent_fetches, 1),
        )
    except Exception as exc:
        store.finish_ingestion_run(
            run_id,
            status="failed",
            items_processed=0,
            items_succeeded=0,
            items_failed=len(tasks),
            error_message=str(exc),
        )
        raise


def _fetch_daily_tasks(
    tasks: list[dict[str, Any]],
    provider: DailyCandleProvider,
    throttle_seconds: float,
    max_concurrent_fetches: int,
) -> list[dict[str, Any]]:
    if not tasks:
        return []
    workers = max(max_concurrent_fetches, 1)
    if workers == 1 or len(tasks) == 1:
        results = []
        for task in tasks:
            results.append(_fetch_daily_task(provider, task))
            if throttle_seconds:
                time.sleep(throttle_seconds)
        return results

    with ThreadPoolExecutor(max_workers=min(workers, len(tasks))) as executor:
        return list(executor.map(lambda task: _fetch_daily_task(provider, task), tasks))


def _fetch_daily_task(
    provider: DailyCandleProvider,
    task: dict[str, Any],
) -> dict[str, Any]:
    try:
        frame = provider.fetch_daily_candles(
            instrument_key=str(task["instrument_key"]),
            start=task["fetch_start"],
            end=task["fetch_end"],
            symbol=str(task["symbol"]),
            trading_symbol=str(task["trading_symbol"]),
        )
        return {"task": task, "frame": frame, "error": ""}
    except Exception as exc:
        return {"task": task, "frame": pd.DataFrame(), "error": str(exc)}


def _tasks_to_plan(tasks: list[dict[str, Any]]) -> pd.DataFrame:
    if not tasks:
        return pd.DataFrame(
            columns=[
                "symbol",
                "instrument_key",
                "trading_symbol",
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
                "symbol": task["symbol"],
                "instrument_key": task["instrument_key"],
                "trading_symbol": task["trading_symbol"],
                "latest_stored_date": None,
                "fetch_start": task["fetch_start"],
                "fetch_end": task["fetch_end"],
                "should_fetch": True,
                "skip_reason": "",
            }
            for task in tasks
        ]
    )


def _consolidate_tasks_by_instrument(tasks: list[dict[str, Any]]) -> list[dict[str, Any]]:
    by_key: dict[str, dict[str, Any]] = {}
    for task in tasks:
        key = str(task["instrument_key"])
        existing = by_key.get(key)
        if existing is None:
            by_key[key] = dict(task)
            continue
        existing["fetch_start"] = min(existing["fetch_start"], task["fetch_start"])
        existing["fetch_end"] = max(existing["fetch_end"], task["fetch_end"])
        existing["missing_rows"] = int(existing.get("missing_rows") or 0) + int(
            task.get("missing_rows") or 0
        )
    return sorted(by_key.values(), key=lambda item: (str(item["symbol"]), item["fetch_start"]))


def _run_status(
    tasks: list[dict[str, Any]],
    fetch_coverage: pd.DataFrame,
    failures: list[dict[str, str]],
) -> str:
    if not tasks:
        return "completed_empty"
    if failures:
        return "completed_with_warnings"
    if (
        not fetch_coverage.empty
        and fetch_coverage["fetch_status"].isin(["no_rows", "failed"]).any()
    ):
        return "completed_with_warnings"
    return "completed"


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
