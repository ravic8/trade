from datetime import UTC, date, datetime

import pandas as pd

from trade_research.storage import TimescaleStore
from trade_research.storage.timescale import metadata
from trade_research.targets import (
    DAILY_OPPORTUNITY_TARGET_VERSION_V1_0,
    DailyOpportunityTargetBuilder,
)


def _targets() -> pd.DataFrame:
    rows = []
    for symbol, key, offset in (("AAA", "NSE_EQ|AAA", 0), ("BBB", "NSE_EQ|BBB", 10)):
        rows.extend(
            [
                {
                    "Date": date(2026, 1, 2),
                    "Open": 98 + offset,
                    "High": 102 + offset,
                    "Low": 97 + offset,
                    "Close": 100 + offset,
                    "Volume": 100_000,
                    "OpenInterest": 0,
                    "InstrumentKey": key,
                    "Symbol": symbol,
                    "Exchange": "NSE",
                    "Source": "yfinance",
                },
                {
                    "Date": date(2026, 1, 5),
                    "Open": 101 + offset,
                    "High": 105 + offset,
                    "Low": 99 + offset,
                    "Close": 104 + offset,
                    "Volume": 120_000,
                    "OpenInterest": 0,
                    "InstrumentKey": key,
                    "Symbol": symbol,
                    "Exchange": "NSE",
                    "Source": "yfinance",
                },
            ]
        )
    return DailyOpportunityTargetBuilder(
        computed_at=datetime(2026, 1, 6, tzinfo=UTC)
    ).build(pd.DataFrame(rows))


def test_opportunity_targets_are_idempotent_and_queryable(tmp_path) -> None:
    store = TimescaleStore(f"sqlite:///{tmp_path / 'opportunities.sqlite'}")
    metadata.create_all(store.engine)
    targets = _targets()

    assert store.upsert_daily_opportunity_targets(targets) == 4
    assert store.upsert_daily_opportunity_targets(targets) == 4

    stored = store.daily_opportunity_target_frame(
        target_version=DAILY_OPPORTUNITY_TARGET_VERSION_V1_0,
        exchange="NSE",
        source="yfinance",
    )
    assert len(stored) == 4

    page = store.opportunity_targets_page(
        target_version=DAILY_OPPORTUNITY_TARGET_VERSION_V1_0,
        exchange="NSE",
        source="yfinance",
        sort_by="true_range",
    )
    assert page["session_date"] == date(2026, 1, 5)
    assert page["total"] == 2
    assert len(page["rows"]) == 2
    assert page["summary"]["positive_sessions"] == 2
    assert page["summary"]["positive_session_ratio"] == 1.0


def test_opportunity_page_supports_symbol_filter_and_explicit_date(tmp_path) -> None:
    store = TimescaleStore(f"sqlite:///{tmp_path / 'opportunities.sqlite'}")
    metadata.create_all(store.engine)
    store.upsert_daily_opportunity_targets(_targets())

    page = store.opportunity_targets_page(
        target_version=DAILY_OPPORTUNITY_TARGET_VERSION_V1_0,
        exchange="NSE",
        source="yfinance",
        session_date=date(2026, 1, 5),
        symbol="aa",
        sort_by="symbol",
        direction="asc",
    )

    assert page["total"] == 1
    assert page["rows"][0]["symbol"] == "AAA"
