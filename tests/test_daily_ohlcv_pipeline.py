from datetime import date

import pandas as pd

from trade_research.data.rate_limits import RateLimitDecision
from trade_research.pipelines.daily_ohlcv import (
    _fetch_upstox_daily_with_controls,
    _retry_candidates_to_fetch_plan,
    build_daily_fetch_coverage,
    plan_daily_fetch_windows,
)


class _RecordingLimiter:
    def __init__(self) -> None:
        self.calls: list[tuple[str, str]] = []

    def acquire(self, provider: str, endpoint_group: str) -> RateLimitDecision:
        self.calls.append((provider, endpoint_group))
        return RateLimitDecision(backend="memory", wait_seconds=0.1, rate_limited=True)


class _RecordingStore:
    def __init__(self) -> None:
        self.logs: list[dict] = []

    def insert_provider_request_logs(self, logs) -> int:
        self.logs.extend(logs)
        return len(logs)


class _DailyProvider:
    def fetch_daily_candles(
        self,
        instrument_key: str,
        start: date,
        end: date,
        symbol: str,
        trading_symbol: str | None = None,
    ) -> pd.DataFrame:
        return pd.DataFrame(
            [
                {
                    "InstrumentKey": instrument_key,
                    "Symbol": symbol,
                    "TradingSymbol": trading_symbol,
                    "Date": start,
                    "Open": 100.0,
                    "High": 101.0,
                    "Low": 99.0,
                    "Close": 100.5,
                    "Volume": 1000,
                }
            ]
        )


def test_plan_daily_fetch_windows_skips_current_symbols() -> None:
    mapping = pd.DataFrame(
        {
            "symbol": ["AAA", "BBB"],
            "instrument_key": ["A", "B"],
            "trading_symbol": ["AAA", "BBB"],
        }
    )

    planned = plan_daily_fetch_windows(
        mapping,
        base_start=date(2026, 6, 1),
        end=date(2026, 6, 25),
        latest_dates={"A": date(2026, 6, 25), "B": date(2026, 6, 20)},
    )

    aaa = planned[planned["instrument_key"].eq("A")].iloc[0]
    bbb = planned[planned["instrument_key"].eq("B")].iloc[0]
    assert bool(aaa["should_fetch"]) is False
    assert aaa["skip_reason"] == "already_current"
    assert bool(bbb["should_fetch"]) is True
    assert bbb["fetch_start"] == "2026-06-21"


def test_fetch_upstox_daily_with_controls_limits_and_logs_request() -> None:
    limiter = _RecordingLimiter()
    store = _RecordingStore()

    frame = _fetch_upstox_daily_with_controls(
        provider=_DailyProvider(),
        limiter=limiter,
        db=store,
        run_id="run-1",
        row={
            "instrument_key": "NSE_EQ|TEST",
            "symbol": "TEST",
            "trading_symbol": "TEST",
        },
        start=date(2026, 6, 21),
        end=date(2026, 6, 25),
    )

    assert len(frame) == 1
    assert limiter.calls == [("upstox", "historical")]
    assert len(store.logs) == 1
    log = store.logs[0]
    assert log["run_id"] == "run-1"
    assert log["provider"] == "upstox"
    assert log["endpoint_group"] == "historical"
    assert log["request_key"] == "NSE_EQ|TEST:1d:2026-06-21:2026-06-25"
    assert log["rate_limited"] is True
    assert log["wait_seconds"] == 0.1
    assert log["status"] == "success"


def test_build_daily_fetch_coverage_classifies_retry_candidates() -> None:
    planned = pd.DataFrame(
        [
            {
                "symbol": "AAA",
                "instrument_key": "A",
                "latest_stored_date": "2026-06-25",
                "fetch_start": "2026-06-26",
                "fetch_end": "2026-06-26",
                "should_fetch": False,
                "skip_reason": "already_current",
            },
            {
                "symbol": "BBB",
                "instrument_key": "B",
                "latest_stored_date": "2026-06-20",
                "fetch_start": "2026-06-21",
                "fetch_end": "2026-06-26",
                "should_fetch": True,
                "skip_reason": "",
            },
            {
                "symbol": "CCC",
                "instrument_key": "C",
                "latest_stored_date": "2026-06-20",
                "fetch_start": "2026-06-21",
                "fetch_end": "2026-06-26",
                "should_fetch": True,
                "skip_reason": "",
            },
            {
                "symbol": "DDD",
                "instrument_key": "D",
                "latest_stored_date": "2026-06-20",
                "fetch_start": "2026-06-21",
                "fetch_end": "2026-06-26",
                "should_fetch": True,
                "skip_reason": "",
            },
        ]
    )
    fetched = pd.DataFrame(
        [
            {"InstrumentKey": "B", "Date": date(2026, 6, 21)},
            {"InstrumentKey": "B", "Date": date(2026, 6, 22)},
        ]
    )
    failures = pd.DataFrame(
        [{"symbol": "C", "instrument_key": "C", "error": "rate limited"}]
    )

    coverage = build_daily_fetch_coverage(planned, fetched, failures)
    statuses = dict(zip(coverage["instrument_key"], coverage["fetch_status"], strict=True))

    assert statuses == {
        "A": "skipped_current",
        "B": "fetched",
        "C": "failed",
        "D": "no_rows",
    }
    assert int(coverage[coverage["instrument_key"].eq("B")]["rows_fetched"].iloc[0]) == 2


def test_retry_candidates_become_fetch_plan_rows() -> None:
    candidates = pd.DataFrame(
        [
            {
                "symbol": "AAA",
                "instrument_key": "A",
                "latest_stored_date": date(2026, 6, 20),
                "fetch_start": date(2026, 6, 21),
                "fetch_end": date(2026, 6, 25),
            }
        ]
    )

    plan = _retry_candidates_to_fetch_plan(candidates)

    assert len(plan) == 1
    row = plan.iloc[0]
    assert row["symbol"] == "AAA"
    assert row["instrument_key"] == "A"
    assert bool(row["should_fetch"]) is True
    assert row["skip_reason"] == ""
