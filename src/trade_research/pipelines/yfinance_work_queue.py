from __future__ import annotations

from collections.abc import Iterable
from datetime import UTC, date, datetime, timedelta
from threading import Event, Thread
from typing import Any
from uuid import uuid4

import pandas as pd

from trade_research.config import Settings, get_settings
from trade_research.data.daily_work import DailyInstrument, DailyWorkPlanner
from trade_research.data.rate_limits import build_provider_rate_limiter
from trade_research.pipelines.base import PipelineRunResult
from trade_research.pipelines.yfinance_daily import (
    YFinanceBatchProvider,
    _execute_yfinance_daily_batches_with_controls,
    _retry_database_write,
)
from trade_research.storage import TimescaleStore

SUPPORTED_EQUITY_EXCHANGES = ("NSE", "TSX", "US")


def run_yfinance_daily_work_planner(
    *,
    exchanges: Iterable[str] = SUPPORTED_EQUITY_EXCHANGES,
    include_incremental: bool = True,
    include_initial_backfill: bool = True,
    include_gap_repair: bool = False,
    trigger: str = "pipeline",
    at: datetime | None = None,
) -> PipelineRunResult:
    settings = get_settings()
    db = TimescaleStore(settings.database_url)
    db.initialize()
    observed_at = _as_utc(at or datetime.now(UTC))
    planner = DailyWorkPlanner(max_attempts=settings.yfinance_work_max_attempts)
    generated = inserted = active_count = 0
    exchange_metrics: dict[str, Any] = {}

    for raw_exchange in exchanges:
        exchange = raw_exchange.upper()
        if exchange not in SUPPORTED_EQUITY_EXCHANGES:
            raise ValueError(f"Unsupported daily equity exchange: {raw_exchange}")
        eligible = db.latest_provider_eligible_exchange_session(
            exchange,
            at=observed_at,
            provider_grace_minutes=settings.yfinance_provider_grace_minutes,
        )
        if eligible is None:
            raise ValueError(
                f"No provider-eligible materialized exchange session is available for {exchange}."
            )
        end = eligible["session_date"]
        start = _subtract_years(end, settings.yfinance_backfill_years)
        sessions = [
            row["session_date"]
            for row in db.exchange_sessions(exchange, start, end)
            if row["is_trading_day"] and str(row["validation_status"]).startswith("valid")
        ]
        if not sessions:
            raise ValueError(f"No valid materialized trading sessions found for {exchange}.")
        instruments = [
            DailyInstrument(
                canonical_instrument_id=str(row["canonical_instrument_id"]),
                provider_symbol=str(row["provider_symbol"]),
                exchange=exchange,
            )
            for row in db.active_yfinance_daily_instruments(exchange)
        ]
        if not instruments:
            raise ValueError(
                f"No active yfinance instruments are available from the latest {exchange} snapshot."
            )
        active_count += len(instruments)
        exchange_generated = exchange_inserted = 0
        latest_dates = db.latest_daily_ohlcv_dates(
            [instrument.instrument_key for instrument in instruments],
            source="yfinance",
            valid_only=True,
        )
        if include_incremental:
            work = planner.plan_incremental(
                instruments,
                sessions,
                latest_dates,
                overlap_sessions=settings.yfinance_incremental_overlap_sessions,
                now=observed_at,
            )
            exchange_generated += len(work)
            exchange_inserted += db.enqueue_pipeline_work_items(work)

        if include_initial_backfill or include_gap_repair:
            for chunk in _chunks(instruments, settings.yfinance_work_planner_chunk_size):
                keys = [instrument.instrument_key for instrument in chunk]
                stored_dates = db.daily_ohlcv_dates_by_instrument(
                    keys,
                    start,
                    end,
                    source="yfinance",
                    exchange=exchange,
                    valid_only=True,
                )
                if include_initial_backfill:
                    work = planner.plan_initial_backfill(
                        chunk, sessions, stored_dates, now=observed_at
                    )
                    exchange_generated += len(work)
                    exchange_inserted += db.enqueue_pipeline_work_items(work)
                if include_gap_repair:
                    work = planner.plan_gap_repair(chunk, sessions, stored_dates, now=observed_at)
                    exchange_generated += len(work)
                    exchange_inserted += db.enqueue_pipeline_work_items(work)

        generated += exchange_generated
        inserted += exchange_inserted
        exchange_metrics[exchange] = {
            "active_symbols": len(instruments),
            "window_start": start.isoformat(),
            "window_end": end.isoformat(),
            "valid_sessions": len(sessions),
            "work_generated": exchange_generated,
            "work_inserted": exchange_inserted,
        }

    return PipelineRunResult(
        name="yfinance_daily_work_planner",
        status="pass",
        rows=inserted,
        metrics={
            "trigger": trigger,
            "active_symbols": active_count,
            "work_generated": generated,
            "work_inserted": inserted,
            "duplicates_reused": generated - inserted,
            "include_incremental": include_incremental,
            "include_initial_backfill": include_initial_backfill,
            "include_gap_repair": include_gap_repair,
            "exchanges": exchange_metrics,
            "queue": db.pipeline_work_queue_summary(),
        },
    )


def run_yfinance_daily_work_queue(
    *,
    worker_id: str | None = None,
    claim_size: int | None = None,
    trigger: str = "pipeline",
    provider: YFinanceBatchProvider | None = None,
    at: datetime | None = None,
) -> PipelineRunResult:
    settings = get_settings()
    db = TimescaleStore(settings.database_url)
    db.initialize()
    if not settings.yfinance_daily_enabled:
        return PipelineRunResult(
            name="yfinance_daily_work_queue",
            status="pass",
            rows=0,
            metrics={
                "trigger": trigger,
                "execution_enabled": False,
                "claimed": 0,
                "succeeded": 0,
                "retry_wait": 0,
                "terminal": 0,
                "queue": db.pipeline_work_queue_summary(),
            },
            warnings=["Yfinance daily execution is disabled by configuration."],
        )
    observed_at = _as_utc(at or datetime.now(UTC))
    resolved_worker_id = worker_id or f"yfinance-worker-{uuid4()}"
    stale_before = observed_at - timedelta(minutes=settings.yfinance_work_stale_minutes)
    recovered = db.recover_stale_pipeline_work_items(stale_before=stale_before, at=observed_at)
    claimed = db.claim_pipeline_work_items(
        worker_id=resolved_worker_id,
        limit=claim_size or settings.yfinance_work_claim_size,
        at=observed_at,
    )
    if not claimed:
        return PipelineRunResult(
            name="yfinance_daily_work_queue",
            status="pass",
            rows=0,
            metrics={
                "trigger": trigger,
                "worker_id": resolved_worker_id,
                "stale_locks_recovered": recovered,
                "claimed": 0,
                "succeeded": 0,
                "retry_wait": 0,
                "terminal": 0,
                "queue": db.pipeline_work_queue_summary(),
            },
        )

    run_id = db.start_ingestion_run(
        job_name="yfinance_daily_work_queue",
        exchange="MULTI",
        source="yfinance",
        items_requested=len(claimed),
        run_metadata={
            "trigger": trigger,
            "worker_id": resolved_worker_id,
            "claimed_work_item_ids": [row["work_item_id"] for row in claimed],
        },
    )
    succeeded = retry_wait = terminal = lost_claims = rows_written = adjustment_rows = 0
    heartbeat = _WorkHeartbeat(
        db,
        resolved_worker_id,
        [str(row["work_item_id"]) for row in claimed],
        settings.yfinance_work_heartbeat_seconds,
    )
    heartbeat.start()
    try:
        for exchange, exchange_work in _group_by_exchange(claimed).items():
            outcomes, written, adjustments = _execute_claimed_exchange_work(
                db=db,
                settings=settings,
                exchange=exchange,
                work_items=exchange_work,
                run_id=str(run_id),
                provider=provider,
            )
            rows_written += written
            adjustment_rows += adjustments
            by_id = {
                str(outcome["work_item_id"]): outcome
                for outcome in outcomes
                if outcome.get("work_item_id")
            }
            for item in exchange_work:
                work_item_id = str(item["work_item_id"])
                outcome = by_id.get(work_item_id)
                status: str
                error_code: str | None
                error_message: str | None
                status_code: int | None
                if outcome is None:
                    status = "retry_wait"
                    error_code = "missing_executor_outcome"
                    error_message = "Yahoo executor returned no ticker outcome."
                    status_code = None
                elif outcome["status"] == "success":
                    status = "succeeded"
                    error_code = None
                    error_message = None
                    status_code = outcome.get("status_code")
                elif bool(outcome.get("retryable")):
                    status = "retry_wait"
                    error_code = str(outcome["status"])
                    error_message = str(outcome.get("error_message") or "")
                    status_code = outcome.get("status_code")
                else:
                    status = "terminal"
                    error_code = str(outcome["status"])
                    error_message = str(outcome.get("error_message") or "")
                    status_code = outcome.get("status_code")
                transitioned = db.transition_pipeline_work_item(
                    work_item_id=work_item_id,
                    worker_id=resolved_worker_id,
                    status=status,
                    status_code=status_code,
                    error_code=error_code,
                    error_message=error_message,
                    run_id=str(run_id),
                )
                if transitioned is None:
                    lost_claims += 1
                    continue
                resolved = str(transitioned["status"])
                succeeded += int(resolved == "succeeded")
                retry_wait += int(resolved == "retry_wait")
                terminal += int(resolved == "terminal")
    except Exception as exc:
        for item in claimed:
            transitioned = db.transition_pipeline_work_item(
                work_item_id=str(item["work_item_id"]),
                worker_id=resolved_worker_id,
                status="retry_wait",
                error_code="worker_failure",
                error_message=str(exc),
                run_id=str(run_id),
            )
            if transitioned:
                retry_wait += int(transitioned["status"] == "retry_wait")
                terminal += int(transitioned["status"] == "terminal")
    finally:
        heartbeat.stop()

    db.finish_ingestion_run(
        run_id,
        status=(
            "completed"
            if not (retry_wait or terminal or lost_claims)
            else "completed_with_failures"
        ),
        items_processed=len(claimed),
        items_succeeded=succeeded,
        items_failed=retry_wait + terminal + lost_claims,
    )
    return PipelineRunResult(
        name="yfinance_daily_work_queue",
        status="warn" if retry_wait or terminal or lost_claims else "pass",
        rows=rows_written,
        metrics={
            "trigger": trigger,
            "run_id": run_id,
            "worker_id": resolved_worker_id,
            "stale_locks_recovered": recovered,
            "claimed": len(claimed),
            "succeeded": succeeded,
            "retry_wait": retry_wait,
            "terminal": terminal,
            "lost_claims": lost_claims,
            "heartbeat_failures": heartbeat.failure_count,
            "ohlcv_rows_written": rows_written,
            "adjustment_rows_written": adjustment_rows,
            "queue": db.pipeline_work_queue_summary(),
        },
        warnings=[
            *(
                [f"{retry_wait} work items are waiting for durable retry."]
                if retry_wait
                else []
            ),
            *(
                [f"{lost_claims} work item claims changed ownership before acknowledgement."]
                if lost_claims
                else []
            ),
            *(
                [f"Work-item heartbeat failed {heartbeat.failure_count} times."]
                if heartbeat.failure_count
                else []
            ),
        ],
    )


def _execute_claimed_exchange_work(
    *,
    db: TimescaleStore,
    settings: Settings,
    exchange: str,
    work_items: list[dict[str, Any]],
    run_id: str,
    provider: YFinanceBatchProvider | None,
) -> tuple[list[dict[str, Any]], int, int]:
    rows = [
        {
            "work_item_id": str(item["work_item_id"]),
            "symbol": str(item["provider_symbol"]),
            "instrument_key": f"YF|{item['provider_symbol']}",
            "yahoo_symbol": str(item["provider_symbol"]),
            "fetch_start": item["window_start"].isoformat(),
            "fetch_end": item["window_end"].isoformat(),
        }
        for item in work_items
    ]
    execution = _execute_yfinance_daily_batches_with_controls(
        provider=provider,
        rows=rows,
        limiter=build_provider_rate_limiter(settings),
        db=db,
        run_id=run_id,
        batch_size=min(25, settings.yfinance_work_claim_size),
        settings=settings,
    )
    frame = pd.concat(execution.frames, ignore_index=True) if execution.frames else pd.DataFrame()
    written = (
        _retry_database_write(
            lambda: db.upsert_daily_ohlcv(frame, exchange=exchange, source="yfinance")
        )
        if not frame.empty
        else 0
    )
    adjustments = (
        _retry_database_write(
            lambda: db.upsert_daily_price_adjustments(frame, exchange=exchange, source="yfinance")
        )
        if not frame.empty
        else 0
    )
    return execution.ticker_outcomes, written, adjustments


class _WorkHeartbeat:
    def __init__(
        self,
        db: TimescaleStore,
        worker_id: str,
        work_item_ids: list[str],
        interval_seconds: int,
    ) -> None:
        self._db = db
        self._worker_id = worker_id
        self._work_item_ids = work_item_ids
        self._interval_seconds = interval_seconds
        self._stop = Event()
        self.failure_count = 0
        self.last_error: str | None = None
        self._thread = Thread(target=self._run, name="yfinance-work-heartbeat", daemon=True)

    def start(self) -> None:
        self._thread.start()

    def stop(self) -> None:
        self._stop.set()
        self._thread.join(timeout=self._interval_seconds + 1)

    def _run(self) -> None:
        while not self._stop.wait(self._interval_seconds):
            try:
                self._db.heartbeat_pipeline_work_items(
                    worker_id=self._worker_id,
                    work_item_ids=self._work_item_ids,
                )
            except Exception as exc:
                self.failure_count += 1
                self.last_error = str(exc)


def _group_by_exchange(
    work_items: list[dict[str, Any]],
) -> dict[str, list[dict[str, Any]]]:
    grouped: dict[str, list[dict[str, Any]]] = {}
    for item in work_items:
        grouped.setdefault(str(item["exchange"]), []).append(item)
    return grouped


def _chunks(values: list[DailyInstrument], size: int) -> list[list[DailyInstrument]]:
    return [values[index : index + size] for index in range(0, len(values), size)]


def _subtract_years(value: date, years: int) -> date:
    try:
        return value.replace(year=value.year - years)
    except ValueError:
        return value.replace(month=2, day=28, year=value.year - years)


def _as_utc(value: datetime) -> datetime:
    if value.tzinfo is None:
        return value.replace(tzinfo=UTC)
    return value.astimezone(UTC)
