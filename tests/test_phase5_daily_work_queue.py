from __future__ import annotations

from datetime import UTC, date, datetime, timedelta

import pandas as pd
import pytest

from trade_research.config import Settings
from trade_research.data.daily_work import (
    WORK_PRIORITIES,
    DailyInstrument,
    DailyWorkPlanner,
    durable_retry_delay,
)
from trade_research.data.yahoo_executor import YahooExecutionSummary
from trade_research.pipelines import yfinance_work_queue
from trade_research.storage.timescale import TimescaleStore

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


def test_enabled_exchanges_are_resolved_from_cutover_flags() -> None:
    settings = Settings(
        _env_file=None,
        yfinance_full_us_enabled=True,
        yfinance_full_tsx_enabled=False,
        yfinance_nse_enabled=False,
    )

    assert yfinance_work_queue.enabled_yfinance_daily_exchanges(settings) == ("US",)


class _EmptyMappingsResult:
    def mappings(self):
        return self

    def all(self):
        return []


class _RecordingConnection:
    def __init__(self) -> None:
        self.instrument_key_counts: list[int] = []

    def execute(self, statement):
        parameter_values = statement.compile().params.values()
        key_values = next(
            value
            for value in parameter_values
            if isinstance(value, list) and value and str(value[0]).startswith("YF|")
        )
        self.instrument_key_counts.append(len(key_values))
        return _EmptyMappingsResult()


class _RecordingBegin:
    def __init__(self, connection: _RecordingConnection) -> None:
        self.connection = connection

    def __enter__(self):
        return self.connection

    def __exit__(self, *_args):
        return False


class _RecordingEngine:
    def __init__(self) -> None:
        self.connection = _RecordingConnection()

    def begin(self):
        return _RecordingBegin(self.connection)


def test_latest_date_lookup_chunks_a_full_us_universe() -> None:
    store = object.__new__(TimescaleStore)
    store.engine = _RecordingEngine()

    result = store.latest_daily_ohlcv_dates(
        [f"YF|SYMBOL{index}" for index in range(5_586)],
        source="yfinance",
        valid_only=True,
        chunk_size=250,
    )

    assert result == {}
    assert len(store.engine.connection.instrument_key_counts) == 23
    assert max(store.engine.connection.instrument_key_counts) == 250
    assert store.engine.connection.instrument_key_counts[-1] == 86


def test_timescale_engine_hides_sql_parameters() -> None:
    store = TimescaleStore("sqlite://")

    assert store.engine.hide_parameters is True


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


def test_active_listing_date_is_a_lower_bound_for_backfill_and_incremental_work() -> None:
    listed = DailyInstrument(
        canonical_instrument_id="eq_aauc",
        provider_symbol="AAUC.TO",
        exchange="TSX",
        listing_status="active",
        listing_status_effective_at=datetime(2026, 7, 15, tzinfo=UTC),
    )
    sessions = [date(2026, 7, day) for day in (13, 14, 15, 16, 17)]
    planner = DailyWorkPlanner()

    backfill = planner.plan_initial_backfill([listed], sessions, {}, now=NOW)
    incremental = planner.plan_incremental(
        [listed],
        sessions,
        {listed.instrument_key: date(2026, 7, 15)},
        overlap_sessions=5,
        now=NOW,
    )

    assert [(item.window_start, item.window_end) for item in backfill] == [
        (date(2026, 7, 15), date(2026, 7, 17))
    ]
    assert [(item.window_start, item.window_end) for item in incremental] == [
        (date(2026, 7, 15), date(2026, 7, 17))
    ]


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


def test_lifecycle_ineligible_instrument_generates_no_work() -> None:
    halted = DailyInstrument(
        canonical_instrument_id="eq_sva",
        provider_symbol="SVA",
        exchange="US",
        listing_status="halted",
        pipeline_eligibility="none",
        listing_status_effective_at=datetime(2019, 2, 22, tzinfo=UTC),
    )
    sessions = [date(2026, 7, 16), date(2026, 7, 17)]
    planner = DailyWorkPlanner()

    assert (
        planner.plan_incremental(
            [halted], sessions, {halted.instrument_key: date(2019, 2, 22)}, overlap_sessions=5
        )
        == []
    )
    assert planner.plan_initial_backfill([halted], sessions, {}) == []


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
    assert result.status == "warn"
    assert result.metrics["ohlcv_rows_written"] == 42
    assert store.finished["items_processed"] == 2
    assert store.finished["run_metadata_patch"] == {
        "exchange_results": [
            {
                "exchange": "US",
                "items_requested": 2,
                "items_processed": 2,
                "items_succeeded": 1,
                "items_failed": 1,
                "items_retry_wait": 1,
                "items_terminal": 0,
                "items_cancelled": 0,
                "lost_claims": 0,
            }
        ]
    }


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


def test_worker_records_exchange_scoped_results_for_multi_exchange_run(monkeypatch) -> None:
    nse_claim = _claimed("nse-success", "RELIANCE.NS")
    nse_claim["exchange"] = "NSE"
    us_claim = _claimed("us-retry", "AAPL")
    store = _MemoryQueueStore([nse_claim, us_claim])
    settings = Settings(
        _env_file=None,
        yfinance_daily_enabled=True,
        yfinance_work_heartbeat_seconds=5,
        yfinance_work_stale_minutes=1,
    )
    monkeypatch.setattr(yfinance_work_queue, "get_settings", lambda: settings)
    monkeypatch.setattr(yfinance_work_queue, "TimescaleStore", lambda _: store)

    def execute(**kwargs):
        item = kwargs["work_items"][0]
        if kwargs["exchange"] == "NSE":
            return ([{"work_item_id": item["work_item_id"], "status": "success"}], 1, 0)
        return (
            [
                {
                    "work_item_id": item["work_item_id"],
                    "status": "timeout",
                    "retryable": True,
                    "error_message": "timed out",
                }
            ],
            0,
            0,
        )

    monkeypatch.setattr(yfinance_work_queue, "_execute_claimed_exchange_work", execute)

    yfinance_work_queue.run_yfinance_daily_work_queue(worker_id="multi", at=NOW)

    results = {
        row["exchange"]: row for row in store.finished["run_metadata_patch"]["exchange_results"]
    }
    assert results["NSE"]["items_succeeded"] == 1
    assert results["NSE"]["items_failed"] == 0
    assert results["US"]["items_succeeded"] == 0
    assert results["US"]["items_failed"] == 1


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


def test_new_listing_empty_response_uses_long_provider_grace_retry(monkeypatch) -> None:
    claim = _claimed("new-listing", "SHOT")
    claim.update(
        {
            "canonical_instrument_id": "eq_new_shot",
            "first_seen_at": NOW - timedelta(hours=2),
            "listing_status": "active",
        }
    )
    store = _MemoryQueueStore([claim])
    settings = Settings(
        _env_file=None,
        yfinance_daily_enabled=True,
        yfinance_new_listing_grace_hours=72,
        yfinance_new_listing_retry_hours=6,
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
                {
                    "work_item_id": "new-listing",
                    "status": "empty_response",
                    "retryable": True,
                    "error_message": "no candles",
                }
            ],
            0,
            0,
        ),
    )

    result = yfinance_work_queue.run_yfinance_daily_work_queue(
        worker_id="worker-new-listing", at=NOW
    )

    assert result.metrics["retry_wait"] == 1
    assert store.transitions[0]["error_code"] == "new_listing_provider_lag"
    assert store.transitions[0]["retry_delay"] == timedelta(hours=6)


def test_worker_cancels_prelisting_claim_without_calling_yahoo(monkeypatch) -> None:
    claim = _claimed("before-listing", "AAUC.TO")
    claim.update(
        {
            "exchange": "TSX",
            "canonical_instrument_id": "eq_aauc",
            "listing_status": "active",
            "listing_status_effective_at": datetime(2026, 7, 15, tzinfo=UTC),
            "window_start": date(2026, 7, 1),
            "window_end": date(2026, 7, 14),
        }
    )
    store = _MemoryQueueStore([claim])
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
        lambda **_kwargs: pytest.fail("pre-listing work must not call Yahoo"),
    )

    result = yfinance_work_queue.run_yfinance_daily_work_queue(
        worker_id="worker-before-listing",
        at=NOW,
    )

    assert result.metrics["claimed"] == 1
    assert result.metrics["cancelled"] == 1
    assert result.metrics["succeeded"] == 0
    assert store.transitions[0]["status"] == "cancelled"
    assert store.transitions[0]["error_code"] == "outside_listing_window"


def test_worker_clamps_fetch_and_persisted_rows_to_completed_session(monkeypatch) -> None:
    written_frames: list[pd.DataFrame] = []

    class Store:
        def latest_provider_eligible_exchange_session(self, *_args, **_kwargs):
            return {"session_date": date(2026, 7, 17)}

        def upsert_daily_ohlcv(self, frame, **_kwargs):
            written_frames.append(frame.copy())
            return len(frame)

        def upsert_daily_price_adjustments(self, frame, **_kwargs):
            return len(frame)

    captured_rows: list[dict[str, object]] = []

    def execute(**kwargs):
        captured_rows.extend(kwargs["rows"])
        return YahooExecutionSummary(
            frames=[
                pd.DataFrame(
                    [
                        {
                            "InstrumentKey": "YF|RELIANCE.NS",
                            "Date": date(2026, 7, 17),
                            "Close": 100.0,
                        },
                        {
                            "InstrumentKey": "YF|RELIANCE.NS",
                            "Date": date(2026, 7, 20),
                            "Close": 101.0,
                        },
                    ]
                )
            ],
            ticker_outcomes=[{"work_item_id": "nse-work", "status": "success", "retryable": False}],
        )

    monkeypatch.setattr(
        yfinance_work_queue,
        "_execute_yfinance_daily_batches_with_controls",
        execute,
    )
    monkeypatch.setattr(yfinance_work_queue, "build_provider_rate_limiter", lambda _s: None)
    settings = Settings(_env_file=None, yfinance_provider_grace_minutes=120)
    work = {
        "work_item_id": "nse-work",
        "work_type": "daily_incremental",
        "provider_symbol": "RELIANCE.NS",
        "provider_instrument_key": "YF|RELIANCE.NS",
        "window_start": date(2026, 7, 15),
        "window_end": date(2026, 7, 20),
    }

    outcomes, written, adjustments = yfinance_work_queue._execute_claimed_exchange_work(
        db=Store(),
        settings=settings,
        exchange="NSE",
        work_items=[work],
        run_id="run-1",
        provider=None,
        at=datetime(2026, 7, 20, 8, 40, tzinfo=UTC),
    )

    assert captured_rows[0]["fetch_end"] == "2026-07-17"
    assert written == adjustments == 1
    assert written_frames[0]["Date"].tolist() == [date(2026, 7, 17)]
    assert outcomes[0]["status"] == "success"


def test_worker_retries_incremental_item_missing_its_target_session(monkeypatch) -> None:
    class Store:
        def latest_provider_eligible_exchange_session(self, *_args, **_kwargs):
            return {"session_date": date(2026, 7, 20)}

        def upsert_daily_ohlcv(self, frame, **_kwargs):
            return len(frame)

        def upsert_daily_price_adjustments(self, frame, **_kwargs):
            return len(frame)

    def execute(**_kwargs):
        return YahooExecutionSummary(
            frames=[
                pd.DataFrame(
                    [
                        {
                            "InstrumentKey": "YF|RELIANCE.NS",
                            "Date": date(2026, 7, 17),
                            "Close": 100.0,
                        }
                    ]
                )
            ],
            ticker_outcomes=[
                {
                    "work_item_id": "nse-work",
                    "status": "success",
                    "retryable": False,
                }
            ],
        )

    monkeypatch.setattr(
        yfinance_work_queue,
        "_execute_yfinance_daily_batches_with_controls",
        execute,
    )
    monkeypatch.setattr(yfinance_work_queue, "build_provider_rate_limiter", lambda _s: None)
    work = {
        "work_item_id": "nse-work",
        "work_type": "daily_incremental",
        "provider_symbol": "RELIANCE.NS",
        "provider_instrument_key": "YF|RELIANCE.NS",
        "window_start": date(2026, 7, 9),
        "window_end": date(2026, 7, 20),
    }

    outcomes, written, adjustments = yfinance_work_queue._execute_claimed_exchange_work(
        db=Store(),
        settings=Settings(_env_file=None),
        exchange="NSE",
        work_items=[work],
        run_id="run-1",
        provider=None,
        at=datetime(2026, 7, 21, 6, tzinfo=UTC),
    )

    assert written == adjustments == 1
    assert outcomes == [
        {
            "work_item_id": "nse-work",
            "status": "incomplete_session",
            "retryable": True,
            "error_message": (
                "Yahoo latest daily candle is 2026-07-17; "
                "expected completed session 2026-07-20."
            ),
        }
    ]


def test_worker_defers_window_that_has_no_completed_session(monkeypatch) -> None:
    class Store:
        def latest_provider_eligible_exchange_session(self, *_args, **_kwargs):
            return {"session_date": date(2026, 7, 17)}

    monkeypatch.setattr(
        yfinance_work_queue,
        "_execute_yfinance_daily_batches_with_controls",
        lambda **_kwargs: pytest.fail("future-only work must not call Yahoo"),
    )
    work = {
        "work_item_id": "future-work",
        "work_type": "daily_incremental",
        "provider_symbol": "RELIANCE.NS",
        "window_start": date(2026, 7, 20),
        "window_end": date(2026, 7, 20),
    }

    outcomes, written, adjustments = yfinance_work_queue._execute_claimed_exchange_work(
        db=Store(),
        settings=Settings(_env_file=None),
        exchange="NSE",
        work_items=[work],
        run_id="run-1",
        provider=None,
        at=datetime(2026, 7, 20, 8, 40, tzinfo=UTC),
    )

    assert written == adjustments == 0
    assert outcomes == [
        {
            "work_item_id": "future-work",
            "status": "session_not_complete",
            "retryable": True,
            "error_message": "No completed NSE session is available in the requested window.",
        }
    ]
