import asyncio
from datetime import date

import pandas as pd

import trade_research.pipelines.daily_ohlcv as daily_ohlcv_module
from trade_research.data.rate_limits import RateLimitDecision
from trade_research.pipelines.daily_ohlcv import (
    _fetch_upstox_daily_batch_with_controls,
    _fetch_upstox_daily_with_controls,
    _load_upstox_mapping,
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


class _UniverseStore:
    def __init__(self, members: list[dict]) -> None:
        self.members = members
        self.calls: list[tuple[str, int]] = []

    def tradable_universe_members(self, universe_id: str, limit: int = 500) -> list[dict]:
        self.calls.append((universe_id, limit))
        return self.members


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


class _AsyncDailyProvider:
    def __init__(self, _token: str) -> None:
        self.active = 0
        self.max_active = 0

    async def __aenter__(self) -> "_AsyncDailyProvider":
        return self

    async def __aexit__(self, *_: object) -> None:
        return None

    async def fetch_daily_candles(
        self,
        instrument_key: str,
        start: date,
        end: date,
        symbol: str,
        trading_symbol: str | None = None,
    ) -> pd.DataFrame:
        self.active += 1
        self.max_active = max(self.max_active, self.active)
        await asyncio.sleep(0.01)
        self.active -= 1
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


def test_fetch_upstox_daily_batch_uses_bounded_async_concurrency(monkeypatch) -> None:
    limiter = _RecordingLimiter()
    store = _RecordingStore()
    provider = _AsyncDailyProvider("token")
    rows = [
        {
            "instrument_key": f"NSE_EQ|TEST{index}",
            "symbol": f"TEST{index}",
            "trading_symbol": f"TEST{index}",
            "fetch_start": "2026-06-21",
        }
        for index in range(4)
    ]

    monkeypatch.setattr(
        daily_ohlcv_module,
        "AsyncUpstoxHistoricalDataProvider",
        lambda _token: provider,
    )

    frames, failures = _fetch_upstox_daily_batch_with_controls(
        rows=rows,
        token="token",
        limiter=limiter,
        db=store,
        run_id="run-1",
        end=date(2026, 6, 25),
        concurrency=2,
        throttle_seconds=0,
    )

    assert len(frames) == 4
    assert failures == []
    assert provider.max_active == 2
    assert limiter.calls == [("upstox", "historical")] * 4
    assert len(store.logs) == 4
    assert {log["status"] for log in store.logs} == {"success"}


def test_load_upstox_mapping_rebuilds_missing_csv_from_db(tmp_path) -> None:
    mapping_path = tmp_path / "data/processed/universe/liquid_nse_upstox_mapping.csv"
    store = _UniverseStore(
        [
            {
                "symbol": "hdfcbank",
                "instrument_key": "NSE_EQ|HDFC",
            },
            {
                "symbol": "RELIANCE",
                "instrument_key": "NSE_EQ|RELIANCE",
                "trading_symbol": "RELIANCE",
            },
        ]
    )

    mapping = _load_upstox_mapping(mapping_path, db=store)

    assert store.calls == [("nse_liquid_adt_100cr", 10_000)]
    assert mapping_path.exists()
    assert mapping["symbol"].tolist() == ["HDFCBANK", "RELIANCE"]
    assert mapping["instrument_key"].tolist() == ["NSE_EQ|HDFC", "NSE_EQ|RELIANCE"]
    assert mapping["trading_symbol"].tolist() == ["HDFCBANK", "RELIANCE"]


def test_load_upstox_mapping_raises_clear_error_without_file_or_db(tmp_path) -> None:
    mapping_path = tmp_path / "missing.csv"

    try:
        _load_upstox_mapping(mapping_path, db=None)
    except FileNotFoundError as exc:
        assert "Run map-liquid-nse-upstox" in str(exc)
    else:
        raise AssertionError("Expected missing mapping to raise FileNotFoundError")


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
