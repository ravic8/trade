from datetime import date

import pandas as pd

from trade_research.pipelines.daily_ohlcv import (
    _retry_candidates_to_fetch_plan,
    build_daily_fetch_coverage,
    plan_daily_fetch_windows,
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
