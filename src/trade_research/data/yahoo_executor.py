from __future__ import annotations

from collections.abc import Callable
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import asdict, dataclass, field
from datetime import UTC, date, datetime
from threading import Condition, local
from time import perf_counter
from typing import Any, Protocol

import pandas as pd
from tenacity import Retrying, retry_if_exception_type, stop_after_attempt

from trade_research.data.adaptive_rate import YahooAdaptiveRateGovernor
from trade_research.data.provider_retry import (
    ProviderFailureClassification,
    RetryableProviderFailure,
    RetryAfterOrExponentialWait,
    classify_provider_failure,
    empty_response_classification,
)
from trade_research.data.rate_limits import ProviderRateLimiter, RateLimitDecision


class YahooDailyBatchProvider(Protocol):
    def fetch_daily_ohlcv(
        self,
        symbols: list[dict[str, str]],
        start: date,
        end: date,
    ) -> pd.DataFrame:
        ...


@dataclass(frozen=True)
class YahooDailyBatchTask:
    index: int
    rows: list[dict[str, Any]]
    start: date
    end: date
    run_id: str | None = None


@dataclass
class YahooExecutionSummary:
    frames: list[pd.DataFrame] = field(default_factory=list)
    failures: list[dict[str, str]] = field(default_factory=list)
    request_logs: list[dict[str, Any]] = field(default_factory=list)
    ticker_outcomes: list[dict[str, Any]] = field(default_factory=list)
    attempts: int = 0
    retried_tickers: int = 0
    partial_batches: int = 0
    max_workers: int = 1
    rate_state: dict[str, Any] = field(default_factory=dict)


@dataclass
class _BatchState:
    task: YahooDailyBatchTask
    pending: list[dict[str, Any]]
    frames: list[pd.DataFrame] = field(default_factory=list)
    logs: list[dict[str, Any]] = field(default_factory=list)
    outcomes: list[dict[str, Any]] = field(default_factory=list)
    attempt_count: int = 0
    retried_tickers: int = 0
    partial_batch: bool = False
    last_failure: ProviderFailureClassification | None = None
    last_error_message: str = ""


class YahooDailyExecutor:
    """Bounded, weighted, retrying executor for daily Yahoo batches."""

    def __init__(
        self,
        provider_factory: Callable[[], YahooDailyBatchProvider],
        limiter: ProviderRateLimiter,
        governor: YahooAdaptiveRateGovernor,
        *,
        max_workers: int,
        worker_capacity: int | None = None,
        maximum_attempts: int,
        retry_wait_multiplier_seconds: float,
        retry_wait_max_seconds: float,
    ) -> None:
        self._provider_factory = provider_factory
        self._limiter = limiter
        self._governor = governor
        self._max_workers = max(int(max_workers), 1)
        self._worker_capacity = max(int(worker_capacity or max_workers), self._max_workers)
        self._maximum_attempts = max(int(maximum_attempts), 1)
        self._wait = RetryAfterOrExponentialWait(
            multiplier=retry_wait_multiplier_seconds,
            maximum=retry_wait_max_seconds,
        )
        self._worker_local = local()
        self._concurrency_condition = Condition()
        self._active_requests = 0

    def execute(self, tasks: list[YahooDailyBatchTask]) -> YahooExecutionSummary:
        if not tasks:
            return YahooExecutionSummary(
                max_workers=self._max_workers,
                rate_state=asdict(self._governor.snapshot()),
            )
        results: dict[int, _BatchState] = {}
        with ThreadPoolExecutor(
            max_workers=min(self._worker_capacity, len(tasks)),
            thread_name_prefix="yahoo-daily",
        ) as pool:
            futures = {pool.submit(self._execute_batch, task): task.index for task in tasks}
            for future in as_completed(futures):
                state = future.result()
                results[state.task.index] = state

        summary = YahooExecutionSummary(max_workers=self._max_workers)
        for index in sorted(results):
            state = results[index]
            summary.frames.extend(state.frames)
            summary.request_logs.extend(state.logs)
            summary.ticker_outcomes.extend(state.outcomes)
            summary.attempts += state.attempt_count
            summary.retried_tickers += state.retried_tickers
            summary.partial_batches += int(state.partial_batch)
            for outcome in state.outcomes:
                if outcome["status"] == "success":
                    continue
                summary.failures.append(
                    {
                        "symbol": str(outcome["symbol"]),
                        "instrument_key": str(outcome["instrument_key"]),
                        "error": str(outcome["error_message"]),
                    }
                )
        summary.rate_state = asdict(self._governor.snapshot())
        return summary

    def _execute_batch(self, task: YahooDailyBatchTask) -> _BatchState:
        state = _BatchState(task=task, pending=list(task.rows))
        retryer = Retrying(
            stop=stop_after_attempt(self._maximum_attempts),
            wait=self._wait,
            retry=retry_if_exception_type(RetryableProviderFailure),
            reraise=True,
        )
        try:
            retryer(self._fetch_attempt, state)
        except RetryableProviderFailure:
            self._finalize_pending_failures(state)
        return state

    def _fetch_attempt(self, state: _BatchState) -> None:
        state.attempt_count += 1
        if state.attempt_count > 1:
            state.retried_tickers += len(state.pending)
        symbols = [_provider_symbol(row) for row in state.pending]
        self._governor.wait_for_availability()
        decision = self._limiter.acquire(
            "yfinance",
            "download",
            weight=len(symbols),
        )
        started = perf_counter()
        try:
            self._acquire_worker_slot()
            try:
                frame = self._provider().fetch_daily_ohlcv(
                    symbols,
                    start=state.task.start,
                    end=state.task.end,
                )
            finally:
                self._release_worker_slot()
        except Exception as exc:
            duration_ms = (perf_counter() - started) * 1000
            classification = classify_provider_failure(exc)
            self._governor.report(classification, duration_ms)
            state.last_failure = classification
            state.last_error_message = str(exc)
            state.logs.extend(
                _request_logs(
                    state,
                    decision,
                    duration_ms,
                    classification.code,
                    str(exc),
                    classification.status_code,
                )
            )
            if classification.retryable:
                raise RetryableProviderFailure(str(exc), classification) from exc
            self._finalize_pending_failures(state)
            return

        duration_ms = (perf_counter() - started) * 1000
        present_keys = _present_instrument_keys(frame)
        successful = [
            row for row in state.pending if str(row["instrument_key"]) in present_keys
        ]
        missing = [
            row for row in state.pending if str(row["instrument_key"]) not in present_keys
        ]
        if not frame.empty:
            state.frames.append(frame)
        if successful:
            state.outcomes.extend(
                _ticker_outcomes(
                    successful,
                    status="success",
                    attempt_count=state.attempt_count,
                )
            )
        if missing:
            classification = empty_response_classification()
            state.last_failure = classification
            state.last_error_message = "Yahoo returned no valid daily candles for the ticker."
            state.partial_batch = state.partial_batch or bool(successful)
        else:
            classification = None
            state.last_failure = None
            state.last_error_message = ""

        self._governor.report(classification, duration_ms)
        state.logs.extend(
            _mixed_response_logs(
                state,
                successful,
                missing,
                decision,
                duration_ms,
            )
        )
        state.pending = missing
        if missing:
            raise RetryableProviderFailure(state.last_error_message, classification)

    def _finalize_pending_failures(self, state: _BatchState) -> None:
        if not state.pending:
            return
        classification = state.last_failure or empty_response_classification()
        message = state.last_error_message or "Yahoo request failed."
        state.outcomes.extend(
            _ticker_outcomes(
                state.pending,
                status=classification.code,
                attempt_count=state.attempt_count,
                error_message=message,
                status_code=classification.status_code,
                retryable=classification.retryable,
            )
        )
        state.pending = []

    def _provider(self) -> YahooDailyBatchProvider:
        provider = getattr(self._worker_local, "provider", None)
        if provider is None:
            provider = self._provider_factory()
            self._worker_local.provider = provider
        return provider

    def _acquire_worker_slot(self) -> None:
        with self._concurrency_condition:
            while self._active_requests >= self._governor.concurrency:
                self._concurrency_condition.wait(timeout=0.5)
            self._active_requests += 1

    def _release_worker_slot(self) -> None:
        with self._concurrency_condition:
            self._active_requests -= 1
            self._concurrency_condition.notify_all()


def _provider_symbol(row: dict[str, Any]) -> dict[str, str]:
    return {
        "symbol": str(row["symbol"]),
        "instrument_key": str(row["instrument_key"]),
        "yahoo_symbol": str(row["yahoo_symbol"]),
    }


def _present_instrument_keys(frame: pd.DataFrame) -> set[str]:
    if frame.empty or "InstrumentKey" not in frame.columns:
        return set()
    return {str(value) for value in frame["InstrumentKey"].dropna().unique().tolist()}


def _ticker_outcomes(
    rows: list[dict[str, Any]],
    *,
    status: str,
    attempt_count: int,
    error_message: str = "",
    status_code: int | None = None,
    retryable: bool = False,
) -> list[dict[str, Any]]:
    return [
        {
            "symbol": str(row["symbol"]),
            "instrument_key": str(row["instrument_key"]),
            "yahoo_symbol": str(row["yahoo_symbol"]),
            "status": status,
            "attempt_count": attempt_count,
            "status_code": status_code,
            "retryable": retryable,
            "error_message": error_message,
        }
        for row in rows
    ]


def _mixed_response_logs(
    state: _BatchState,
    successful: list[dict[str, Any]],
    missing: list[dict[str, Any]],
    decision: RateLimitDecision,
    duration_ms: float,
) -> list[dict[str, Any]]:
    logs = []
    for row in successful:
        logs.append(
            _request_log(
                state,
                row,
                decision,
                duration_ms,
                status="success",
                error_message="",
                status_code=None,
            )
        )
    for row in missing:
        logs.append(
            _request_log(
                state,
                row,
                decision,
                duration_ms,
                status="empty_response",
                error_message="Yahoo returned no valid daily candles for the ticker.",
                status_code=None,
            )
        )
    return logs


def _request_logs(
    state: _BatchState,
    decision: RateLimitDecision,
    duration_ms: float,
    status: str,
    error_message: str,
    status_code: int | None,
) -> list[dict[str, Any]]:
    return [
        _request_log(
            state,
            row,
            decision,
            duration_ms,
            status=status,
            error_message=error_message,
            status_code=status_code,
        )
        for row in state.pending
    ]


def _request_log(
    state: _BatchState,
    row: dict[str, Any],
    decision: RateLimitDecision,
    duration_ms: float,
    *,
    status: str,
    error_message: str,
    status_code: int | None,
) -> dict[str, Any]:
    yahoo_symbol = str(row["yahoo_symbol"])
    return {
        "run_id": state.task.run_id,
        "provider": "yfinance",
        "endpoint_group": "download",
        "request_key": (
            f"{yahoo_symbol}:1d:{state.task.start.isoformat()}:{state.task.end.isoformat()}"
        ),
        "instrument_key": str(row["instrument_key"]),
        "symbol": str(row["symbol"]),
        "interval": "1d",
        "window_start": state.task.start,
        "window_end": state.task.end,
        "status_code": status_code,
        "status": status,
        "error_message": error_message,
        "retry_count": state.attempt_count - 1,
        "rate_limited": decision.rate_limited,
        # Allocate one batch-level wait across its ticker-level records so
        # aggregate observability reports wall-clock wait without multiplying it.
        "wait_seconds": decision.wait_seconds / max(len(state.pending), 1),
        "duration_ms": duration_ms,
        "created_at": datetime.now(UTC),
    }
