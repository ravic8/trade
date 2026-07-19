from __future__ import annotations

from collections.abc import Iterable
from datetime import UTC, date, datetime, timedelta
from threading import Event, Thread
from typing import Any
from uuid import uuid4

import pandas as pd

from trade_research.config import Settings, get_settings
from trade_research.data.daily_work import DailyInstrument, DailyWorkPlanner
from trade_research.data.provider_history import (
    build_provider_daily_history_evidence,
    expected_sessions_for_work_item,
    provider_history_is_quarantined,
    verified_provider_coverage_windows,
    verified_provider_history_start,
)
from trade_research.data.rate_limits import build_provider_rate_limiter
from trade_research.pipelines.base import PipelineRunResult
from trade_research.pipelines.yfinance_daily import (
    YFinanceBatchProvider,
    _execute_yfinance_daily_batches_with_controls,
    _retry_database_write,
)
from trade_research.storage import TimescaleStore

SUPPORTED_EQUITY_EXCHANGES = ("NSE", "TSX", "US")


def enabled_yfinance_daily_exchanges(settings: Settings) -> tuple[str, ...]:
    enabled = (
        ("NSE", settings.yfinance_nse_enabled),
        ("TSX", settings.yfinance_full_tsx_enabled),
        ("US", settings.yfinance_full_us_enabled),
    )
    return tuple(exchange for exchange, is_enabled in enabled if is_enabled)


def run_yfinance_daily_work_planner(
    *,
    exchanges: Iterable[str] | None = None,
    include_incremental: bool = True,
    include_initial_backfill: bool = True,
    include_gap_repair: bool = False,
    enqueue: bool = True,
    instrument_limit_per_exchange: int | None = None,
    allow_disabled_exchanges: bool = False,
    trigger: str = "pipeline",
    at: datetime | None = None,
) -> PipelineRunResult:
    settings = get_settings()
    enabled_exchanges = enabled_yfinance_daily_exchanges(settings)
    resolved_exchanges = tuple(exchanges) if exchanges is not None else enabled_exchanges
    if not resolved_exchanges:
        raise ValueError(
            "No yfinance daily exchanges are enabled. Enable an exchange-specific "
            "feature flag."
        )
    if instrument_limit_per_exchange is not None and instrument_limit_per_exchange < 1:
        raise ValueError("instrument_limit_per_exchange must be positive when provided.")
    disabled = sorted(
        {
            value.upper()
            for value in resolved_exchanges
            if value.upper() not in enabled_exchanges
        }
    )
    if disabled and not allow_disabled_exchanges:
        raise ValueError(
            "Yfinance daily exchange flags are disabled for: " + ", ".join(disabled)
        )
    db = TimescaleStore(settings.database_url)
    db.initialize()
    observed_at = _as_utc(at or datetime.now(UTC))
    planner = DailyWorkPlanner(max_attempts=settings.yfinance_work_max_attempts)
    generated = inserted = active_count = cancelled_before_listing = 0
    cancelled_by_provider_history = 0
    quarantined_count = evidence_window_count = cancelled_quarantined_work = 0
    exchange_metrics: dict[str, Any] = {}

    for raw_exchange in resolved_exchanges:
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
        exchange_cancelled_before_listing = (
            db.cancel_pipeline_work_items_before_listing(
                exchange=exchange,
                at=observed_at,
            )
            if enqueue
            else 0
        )
        cancelled_before_listing += exchange_cancelled_before_listing
        exchange_cancelled_by_provider_history = (
            db.cancel_pipeline_work_items_covered_by_provider_history(
                exchange=exchange,
                at=observed_at,
            )
            if enqueue and settings.yfinance_provider_history_evidence_enabled
            else 0
        )
        cancelled_by_provider_history += exchange_cancelled_by_provider_history
        instrument_rows = db.active_yfinance_daily_instruments(exchange)
        if exchange == "TSX":
            instrument_rows = [
                row
                for row in instrument_rows
                if row.get("reconciliation_status") == "official_eligible"
            ]
        eligible_symbol_count = len(instrument_rows)
        if instrument_limit_per_exchange is not None:
            instrument_rows = instrument_rows[:instrument_limit_per_exchange]
        evidence_by_key: dict[str, list[dict[str, Any]]] = {}
        quarantined_symbols: list[str] = []
        quarantined_ids: list[str] = []
        covered_windows: dict[str, list[tuple[date, date]]] = {}
        if settings.yfinance_provider_history_evidence_enabled:
            selected_keys = [
                str(
                    row.get("provider_instrument_key")
                    or f"YF|{row['provider_symbol']}"
                )
                for row in instrument_rows
            ]
            evidence_by_key = db.provider_daily_history_evidence(
                selected_keys,
                provider="yfinance",
                interval="1d",
                chunk_size=settings.yfinance_work_planner_chunk_size,
            )
            retained_rows: list[dict[str, Any]] = []
            for row in instrument_rows:
                instrument_key = str(
                    row.get("provider_instrument_key")
                    or f"YF|{row['provider_symbol']}"
                )
                evidence = evidence_by_key.get(instrument_key, [])
                if provider_history_is_quarantined(evidence):
                    quarantined_symbols.append(str(row["provider_symbol"]))
                    quarantined_ids.append(str(row["canonical_instrument_id"]))
                    continue
                retained_rows.append(row)
                windows = verified_provider_coverage_windows(evidence)
                if windows:
                    covered_windows[instrument_key] = windows
            instrument_rows = retained_rows
            quarantined_count += len(quarantined_symbols)
            evidence_window_count += sum(len(value) for value in covered_windows.values())
        exchange_cancelled_quarantined_work = (
            db.cancel_pending_pipeline_work_for_instruments(
                quarantined_ids,
                reason="provider_history_quarantined",
                message=(
                    "Pending Yahoo work was cancelled because provider history "
                    "evidence quarantined the instrument."
                ),
                at=observed_at,
            )
            if enqueue and quarantined_ids
            else 0
        )
        cancelled_quarantined_work += exchange_cancelled_quarantined_work
        instruments = [
            DailyInstrument(
                canonical_instrument_id=str(row["canonical_instrument_id"]),
                provider_symbol=str(row["provider_symbol"]),
                exchange=exchange,
                listing_status=str(row.get("listing_status") or "active"),
                pipeline_eligibility=str(row.get("pipeline_eligibility") or "incremental"),
                listing_status_effective_at=row.get("listing_status_effective_at"),
                provider_instrument_key=row.get("provider_instrument_key"),
                provider_history_start_date=verified_provider_history_start(
                    evidence_by_key.get(
                        str(
                            row.get("provider_instrument_key")
                            or f"YF|{row['provider_symbol']}"
                        ),
                        [],
                    )
                ),
            )
            for row in instrument_rows
        ]
        if not instruments and not quarantined_symbols:
            raise ValueError(
                f"No active yfinance instruments are available from the latest {exchange} snapshot."
            )
        active_count += len(instruments)
        exchange_generated = exchange_inserted = 0
        latest_dates = db.latest_daily_ohlcv_dates(
            [instrument.instrument_key for instrument in instruments],
            source="yfinance",
            valid_only=True,
            chunk_size=settings.yfinance_work_planner_chunk_size,
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
            if enqueue:
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
                        chunk,
                        sessions,
                        stored_dates,
                        covered_windows=covered_windows,
                        now=observed_at,
                    )
                    exchange_generated += len(work)
                    if enqueue:
                        exchange_inserted += db.enqueue_pipeline_work_items(work)
                if include_gap_repair:
                    work = planner.plan_gap_repair(
                        chunk,
                        sessions,
                        stored_dates,
                        covered_windows=covered_windows,
                        now=observed_at,
                    )
                    exchange_generated += len(work)
                    if enqueue:
                        exchange_inserted += db.enqueue_pipeline_work_items(work)

        generated += exchange_generated
        inserted += exchange_inserted
        exchange_metrics[exchange] = {
            "active_symbols": len(instruments),
            "eligible_symbols_before_limit": eligible_symbol_count,
            "instrument_limit": instrument_limit_per_exchange,
            "window_start": start.isoformat(),
            "window_end": end.isoformat(),
            "valid_sessions": len(sessions),
            "work_generated": exchange_generated,
            "work_inserted": exchange_inserted,
            "work_cancelled_before_listing": exchange_cancelled_before_listing,
            "work_cancelled_by_provider_history": (
                exchange_cancelled_by_provider_history
            ),
            "provider_history_evidence_enabled": (
                settings.yfinance_provider_history_evidence_enabled
            ),
            "provider_history_evidence_windows": sum(
                len(value) for value in covered_windows.values()
            ),
            "provider_quarantined_symbols": quarantined_symbols,
            "provider_quarantined_work_cancelled": (
                exchange_cancelled_quarantined_work
            ),
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
            "work_cancelled_before_listing": cancelled_before_listing,
            "work_cancelled_by_provider_history": cancelled_by_provider_history,
            "provider_history_evidence_enabled": (
                settings.yfinance_provider_history_evidence_enabled
            ),
            "provider_history_evidence_windows": evidence_window_count,
            "provider_quarantined_symbols": quarantined_count,
            "provider_quarantined_work_cancelled": cancelled_quarantined_work,
            "duplicates_reused": generated - inserted if enqueue else 0,
            "enqueue": enqueue,
            "work_not_enqueued": generated if not enqueue else 0,
            "include_incremental": include_incremental,
            "include_initial_backfill": include_initial_backfill,
            "include_gap_repair": include_gap_repair,
            "exchanges": exchange_metrics,
            "queue": db.pipeline_work_queue_summary(),
        },
    )


def run_yfinance_tsx_canary_planner(
    *,
    symbol_limit: int,
    enqueue: bool = False,
    trigger: str = "pipeline",
    at: datetime | None = None,
) -> PipelineRunResult:
    settings = get_settings()
    if symbol_limit < 1:
        raise ValueError("TSX canary symbol_limit must be positive.")
    if symbol_limit > settings.yfinance_tsx_canary_max_symbols:
        raise ValueError(
            "TSX canary symbol limit exceeds configured maximum: "
            f"{symbol_limit}>{settings.yfinance_tsx_canary_max_symbols}"
        )
    if enqueue and not (
        settings.yfinance_tsx_canary_enabled or settings.yfinance_full_tsx_enabled
    ):
        raise ValueError(
            "TSX canary enqueue is disabled. Enable YFINANCE_TSX_CANARY_ENABLED "
            "for bounded canaries; do not enable the full TSX flag yet."
        )
    result = run_yfinance_daily_work_planner(
        exchanges=("TSX",),
        include_incremental=False,
        include_initial_backfill=True,
        include_gap_repair=False,
        enqueue=enqueue,
        instrument_limit_per_exchange=symbol_limit,
        allow_disabled_exchanges=True,
        trigger=trigger,
        at=at,
    )
    result.metrics["canary"] = True
    result.metrics["canary_symbol_limit"] = symbol_limit
    result.metrics["canary_execution_enabled"] = bool(
        settings.yfinance_tsx_canary_enabled or settings.yfinance_full_tsx_enabled
    )
    return result


def run_yfinance_nse_canary_planner(
    *,
    symbol_limit: int,
    enqueue: bool = False,
    trigger: str = "pipeline",
    at: datetime | None = None,
) -> PipelineRunResult:
    """Plan a deterministic, bounded NSE backfill without enabling full NSE."""
    settings = get_settings()
    if symbol_limit < 1:
        raise ValueError("NSE canary symbol_limit must be positive.")
    if symbol_limit > settings.yfinance_nse_canary_max_symbols:
        raise ValueError(
            "NSE canary symbol limit exceeds configured maximum: "
            f"{symbol_limit}>{settings.yfinance_nse_canary_max_symbols}"
        )
    if enqueue and not (
        settings.yfinance_nse_canary_enabled or settings.yfinance_nse_enabled
    ):
        raise ValueError(
            "NSE canary enqueue is disabled. Enable YFINANCE_NSE_CANARY_ENABLED "
            "for bounded canaries; do not enable the full NSE flag yet."
        )
    result = run_yfinance_daily_work_planner(
        exchanges=("NSE",),
        include_incremental=False,
        include_initial_backfill=True,
        include_gap_repair=False,
        enqueue=enqueue,
        instrument_limit_per_exchange=symbol_limit,
        allow_disabled_exchanges=True,
        trigger=trigger,
        at=at,
    )
    result.metrics["canary"] = True
    result.metrics["canary_symbol_limit"] = symbol_limit
    result.metrics["canary_execution_enabled"] = bool(
        settings.yfinance_nse_canary_enabled or settings.yfinance_nse_enabled
    )
    return result


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
    succeeded = retry_wait = terminal = cancelled = lost_claims = rows_written = 0
    adjustment_rows = 0
    heartbeat = _WorkHeartbeat(
        db,
        resolved_worker_id,
        [str(row["work_item_id"]) for row in claimed],
        settings.yfinance_work_heartbeat_seconds,
    )
    heartbeat.start()
    try:
        executable: list[dict[str, Any]] = []
        for item in claimed:
            if not _work_item_precedes_active_listing(item):
                executable.append(item)
                continue
            transitioned = db.transition_pipeline_work_item(
                work_item_id=str(item["work_item_id"]),
                worker_id=resolved_worker_id,
                status="cancelled",
                error_code="outside_listing_window",
                error_message=(
                    "Work window ends before the active instrument listing boundary."
                ),
                run_id=str(run_id),
            )
            if transitioned is None:
                lost_claims += 1
            else:
                cancelled += int(transitioned["status"] == "cancelled")

        for exchange, exchange_work in _group_by_exchange(executable).items():
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
                retry_delay = None
                provider_status = "available" if status == "succeeded" else "unavailable"
                if error_code == "empty_response":
                    first_seen_at = item.get("first_seen_at")
                    listing_status = str(item.get("listing_status") or "active")
                    if listing_status in {"halted", "suspended", "delisted"}:
                        status = "terminal"
                        error_code = "lifecycle_provider_empty"
                        provider_status = "not_expected"
                    elif first_seen_at and observed_at - _as_utc(first_seen_at) <= timedelta(
                        hours=settings.yfinance_new_listing_grace_hours
                    ):
                        status = "retry_wait"
                        error_code = "new_listing_provider_lag"
                        provider_status = "lagging"
                        retry_delay = timedelta(hours=settings.yfinance_new_listing_retry_hours)
                transitioned = db.transition_pipeline_work_item(
                    work_item_id=work_item_id,
                    worker_id=resolved_worker_id,
                    status=status,
                    status_code=status_code,
                    error_code=error_code,
                    error_message=error_message,
                    run_id=str(run_id),
                    retry_delay=retry_delay,
                )
                if transitioned is None:
                    lost_claims += 1
                    continue
                resolved = str(transitioned["status"])
                if hasattr(db, "update_symbol_provider_status"):
                    db.update_symbol_provider_status(
                        str(item["canonical_instrument_id"]),
                        status=provider_status,
                        reason=error_code,
                        at=observed_at,
                    )
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
            "cancelled": cancelled,
            "lost_claims": lost_claims,
            "heartbeat_failures": heartbeat.failure_count,
            "ohlcv_rows_written": rows_written,
            "adjustment_rows_written": adjustment_rows,
            "queue": db.pipeline_work_queue_summary(),
        },
        warnings=[
            *([f"{retry_wait} work items are waiting for durable retry."] if retry_wait else []),
            *(
                [f"{cancelled} work items were cancelled outside their listing window."]
                if cancelled
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


def _work_item_precedes_active_listing(item: dict[str, Any]) -> bool:
    effective_at = item.get("listing_status_effective_at")
    return bool(
        str(item.get("listing_status") or "active") == "active"
        and effective_at is not None
        and item["window_end"] < _as_utc(effective_at).date()
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
            "instrument_key": str(
                item.get("provider_instrument_key") or f"YF|{item['provider_symbol']}"
            ),
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
    _record_successful_provider_history_evidence(
        db=db,
        settings=settings,
        exchange=exchange,
        work_items=work_items,
        ticker_outcomes=execution.ticker_outcomes,
        run_id=run_id,
    )
    return execution.ticker_outcomes, written, adjustments


def _record_successful_provider_history_evidence(
    *,
    db: TimescaleStore,
    settings: Settings,
    exchange: str,
    work_items: list[dict[str, Any]],
    ticker_outcomes: list[dict[str, Any]],
    run_id: str,
) -> int:
    successful_ids = {
        str(outcome["work_item_id"])
        for outcome in ticker_outcomes
        if outcome.get("work_item_id") and outcome.get("status") == "success"
    }
    successful_work = [
        item
        for item in work_items
        if str(item["work_item_id"]) in successful_ids
        and item.get("work_type")
        in {"initial_backfill", "new_symbol_backfill", "gap_repair"}
    ]
    if not successful_work:
        return 0
    minimum_start = min(item["window_start"] for item in successful_work)
    maximum_end = max(item["window_end"] for item in successful_work)
    sessions = [
        row["session_date"]
        for row in db.exchange_sessions(exchange, minimum_start, maximum_end)
        if row["is_trading_day"]
        and str(row["validation_status"]).startswith("valid")
    ]
    instrument_keys = [
        str(
            item.get("provider_instrument_key")
            or f"YF|{item['provider_symbol']}"
        )
        for item in successful_work
    ]
    observed_by_key = db.daily_ohlcv_dates_by_instrument(
        list(dict.fromkeys(instrument_keys)),
        minimum_start,
        maximum_end,
        source="yfinance",
        exchange=exchange,
        valid_only=True,
    )

    evidence_rows: list[dict[str, Any]] = []
    for work_item in successful_work:
        instrument_key = str(
            work_item.get("provider_instrument_key")
            or f"YF|{work_item['provider_symbol']}"
        )
        evidence = build_provider_daily_history_evidence(
            work_item,
            expected_sessions=expected_sessions_for_work_item(work_item, sessions),
            observed_dates=sorted(observed_by_key.get(instrument_key, set())),
            run_id=run_id,
            sparse_minimum_expected_rows=(
                settings.yfinance_sparse_history_minimum_expected_rows
            ),
            sparse_maximum_observed_rows=(
                settings.yfinance_sparse_history_maximum_observed_rows
            ),
        )
        if evidence is not None:
            evidence_rows.append(evidence.as_row())
    return db.upsert_provider_daily_history_evidence(evidence_rows)


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
