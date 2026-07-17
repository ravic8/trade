from __future__ import annotations

from datetime import UTC, date, datetime, timedelta

from trade_research.config import Settings
from trade_research.data.daily_work import (
    WORK_PRIORITIES,
    DailyInstrument,
    DailyWorkPlanner,
    durable_retry_delay,
)
from trade_research.pipelines import yfinance_work_queue

NOW = datetime(2026, 7, 17, 12, tzinfo=UTC)
INSTRUMENT = DailyInstrument(
    canonical_instrument_id="eq_aapl",
    provider_symbol="AAPL",
    exchange="US",
)


def test_phase5_queue_defaults_are_bounded_and_execution_is_disabled() -> None:
    settings = Settings(_env_file=None)

    assert settings.yfinance_daily_enabled is False
    assert settings.yfinance_backfill_years == 10
    assert settings.yfinance_work_claim_size == 100
    assert settings.yfinance_work_heartbeat_seconds == 30
    assert settings.yfinance_work_stale_minutes == 10
    assert settings.yfinance_work_max_attempts == 9


def test_initial_backfill_emits_only_contiguous_missing_session_windows() -> None:
    sessions = [date(2026, 7, day) for day in (13, 14, 15, 16, 17)]

    work = DailyWorkPlanner().plan_initial_backfill(
        [INSTRUMENT],
        sessions,
        {INSTRUMENT.instrument_key: {sessions[0], sessions[2]}},
        now=NOW,
    )

    assert [(item.window_start, item.window_end) for item in work] == [
        (sessions[1], sessions[1]),
        (sessions[3], sessions[4]),
    ]
    assert all(item.work_type == "initial_backfill" for item in work)
    assert all(item.priority == WORK_PRIORITIES["initial_backfill"] for item in work)


def test_complete_backfill_coverage_generates_no_work() -> None:
    sessions = [date(2026, 7, 16), date(2026, 7, 17)]

    work = DailyWorkPlanner().plan_initial_backfill(
        [INSTRUMENT],
        sessions,
        {INSTRUMENT.instrument_key: set(sessions)},
        now=NOW,
    )

    assert work == []


def test_incremental_planning_uses_five_session_overlap_and_higher_priority() -> None:
    sessions = [date(2026, 7, day) for day in range(1, 18)]
    latest = sessions[10]

    work = DailyWorkPlanner().plan_incremental(
        [INSTRUMENT],
        sessions,
        {INSTRUMENT.instrument_key: latest},
        overlap_sessions=5,
        now=NOW,
    )

    assert len(work) == 1
    assert work[0].window_start == sessions[5]
    assert work[0].window_end == sessions[-1]
    assert work[0].priority < WORK_PRIORITIES["initial_backfill"]


def test_work_item_identity_is_stable_across_repeated_planning() -> None:
    sessions = [date(2026, 7, 16), date(2026, 7, 17)]
    planner = DailyWorkPlanner()

    first = planner.plan_initial_backfill([INSTRUMENT], sessions, {}, now=NOW)[0]
    second = planner.plan_initial_backfill(
        [INSTRUMENT], sessions, {}, now=NOW + timedelta(hours=1)
    )[0]

    assert first.work_item_id == second.work_item_id
    assert first.idempotency_key == second.idempotency_key


def test_durable_retry_ladder_caps_at_twenty_four_hours() -> None:
    assert durable_retry_delay(1) == timedelta(minutes=5)
    assert durable_retry_delay(2) == timedelta(minutes=15)
    assert durable_retry_delay(3) == timedelta(hours=1)
    assert durable_retry_delay(6) == timedelta(hours=24)
    assert durable_retry_delay(99) == timedelta(hours=24)


class _MemoryQueueStore:
    def __init__(self, claimed: list[dict[str, object]]) -> None:
        self.claimed = claimed
        self.transitions: list[dict[str, object]] = []
        self.finished: dict[str, object] = {}

    def initialize(self) -> None:
        pass

    def recover_stale_pipeline_work_items(self, **kwargs) -> int:
        return 1

    def claim_pipeline_work_items(self, **kwargs):
        return self.claimed

    def start_ingestion_run(self, **kwargs) -> str:
        return "queue-run"

    def transition_pipeline_work_item(self, **kwargs):
        self.transitions.append(kwargs)
        return {"status": kwargs["status"]}

    def finish_ingestion_run(self, run_id: str, **kwargs) -> None:
        self.finished = {"run_id": run_id, **kwargs}

    def pipeline_work_queue_summary(self):
        return {"succeeded": 1, "retry_wait": 1}

    def heartbeat_pipeline_work_items(self, **kwargs) -> int:
        return len(kwargs["work_item_ids"])


def _claimed(work_item_id: str, symbol: str) -> dict[str, object]:
    return {
        "work_item_id": work_item_id,
        "exchange": "US",
        "provider_symbol": symbol,
        "window_start": date(2026, 7, 1),
        "window_end": date(2026, 7, 17),
    }


def test_worker_acknowledges_success_and_schedules_retryable_failure(monkeypatch) -> None:
    store = _MemoryQueueStore([_claimed("success", "AAPL"), _claimed("retry", "MSFT")])
    settings = Settings(
        _env_file=None,
        yfinance_daily_enabled=True,
        yfinance_work_heartbeat_seconds=5,
        yfinance_work_stale_minutes=1,
    )
    monkeypatch.setattr(yfinance_work_queue, "get_settings", lambda: settings)
    monkeypatch.setattr(yfinance_work_queue, "TimescaleStore", lambda _: store)
    monkeypatch.setattr(
        yfinance_work_queue,
        "_execute_claimed_exchange_work",
        lambda **kwargs: (
            [
                {"work_item_id": "success", "status": "success", "retryable": False},
                {
                    "work_item_id": "retry",
                    "status": "timeout",
                    "retryable": True,
                    "error_message": "timed out",
                },
            ],
            42,
            7,
        ),
    )

    result = yfinance_work_queue.run_yfinance_daily_work_queue(worker_id="worker-1", at=NOW)

    assert [transition["status"] for transition in store.transitions] == [
        "succeeded",
        "retry_wait",
    ]
    assert result.metrics["stale_locks_recovered"] == 1
    assert result.metrics["succeeded"] == 1
    assert result.metrics["retry_wait"] == 1
    assert result.metrics["ohlcv_rows_written"] == 42
    assert store.finished["items_processed"] == 2


def test_worker_does_not_claim_when_daily_execution_flag_is_disabled(monkeypatch) -> None:
    store = _MemoryQueueStore([_claimed("should-not-run", "AAPL")])
    monkeypatch.setattr(
        yfinance_work_queue,
        "get_settings",
        lambda: Settings(_env_file=None, yfinance_daily_enabled=False),
    )
    monkeypatch.setattr(yfinance_work_queue, "TimescaleStore", lambda _: store)

    result = yfinance_work_queue.run_yfinance_daily_work_queue(at=NOW)

    assert result.metrics["execution_enabled"] is False
    assert result.metrics["claimed"] == 0
    assert store.transitions == []


def test_executor_outcomes_preserve_work_item_identity() -> None:
    from trade_research.data.yahoo_executor import _ticker_outcomes

    outcomes = _ticker_outcomes(
        [
            {
                "work_item_id": "work-123",
                "symbol": "AAPL",
                "instrument_key": "YF|AAPL",
                "yahoo_symbol": "AAPL",
            }
        ],
        status="success",
        attempt_count=1,
    )

    assert outcomes[0]["work_item_id"] == "work-123"
