from datetime import UTC, date, datetime

import pandas as pd
import pytest

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
    assert page["selection_mode"] == "automatic"
    assert page["requested_session_date"] is None
    assert page["session_exists"] is True
    assert page["total"] == 2
    assert page["session_total"] == 2
    assert len(page["rows"]) == 2
    assert page["summary"]["positive_sessions"] == 2
    assert page["summary"]["positive_session_ratio"] == 1.0
    assert page["summary"]["return_band_sessions"] == 2
    assert page["summary"]["return_band_eligible_sessions"] == 2
    assert page["summary"]["return_band_ratio"] == 1.0
    assert page["summary"]["median_upside"] == page["distributions"]["upside"][
        "percentiles"
    ]["p50"]
    assert page["summary"]["median_true_range"] == page["distributions"][
        "true_range"
    ]["percentiles"]["p50"]
    assert page["distributions"]["upside"]["count"] == 2
    assert page["distributions"]["upside"]["percentiles"]["p50"] > 0
    assert sum(
        item["count"] for item in page["distributions"]["upside"]["bins"]
    ) == 2
    assert page["rows"][0]["percentiles"]["upside"] is not None
    assert page["available_sessions"][0]["date"] == date(2026, 1, 5)
    assert page["available_sessions"][0]["coverage_status"] == "complete"


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
    assert page["selection_mode"] == "explicit"
    assert page["requested_session_date"] == date(2026, 1, 5)
    assert page["rows"][0]["symbol"] == "AAA"

    first_session = store.opportunity_targets_page(
        target_version=DAILY_OPPORTUNITY_TARGET_VERSION_V1_0,
        exchange="NSE",
        source="yfinance",
        session_date=date(2026, 1, 2),
    )
    assert first_session["summary"]["average_gap"] is None

    unavailable = store.opportunity_targets_page(
        target_version=DAILY_OPPORTUNITY_TARGET_VERSION_V1_0,
        exchange="NSE",
        source="yfinance",
        session_date=date(2026, 1, 7),
    )
    assert unavailable["session_date"] == date(2026, 1, 7)
    assert unavailable["session_exists"] is False
    assert unavailable["coverage_status"] == "unavailable"
    assert unavailable["latest_available_date"] == date(2026, 1, 5)
    assert unavailable["latest_complete_date"] == date(2026, 1, 5)
    assert unavailable["session_instruments"] == 0
    assert unavailable["total"] == 0
    assert unavailable["distributions"] == {}


def test_opportunity_page_combines_percentile_filters_before_pagination(tmp_path) -> None:
    store = TimescaleStore(f"sqlite:///{tmp_path / 'opportunities.sqlite'}")
    metadata.create_all(store.engine)
    store.upsert_daily_opportunity_targets(_targets())

    page = store.opportunity_targets_page(
        target_version=DAILY_OPPORTUNITY_TARGET_VERSION_V1_0,
        exchange="NSE",
        source="yfinance",
        session_date=date(2026, 1, 5),
        percentile_filters={
            "upside": (75, 100),
            "recovery": (50, 100),
        },
    )

    assert page["session_total"] == 2
    assert page["total"] == 1
    assert page["rows"][0]["symbol"] == "AAA"
    assert page["summary"]["median_upside"] == page["rows"][0]["upside"]
    assert page["summary"]["median_true_range"] == page["rows"][0]["true_range"]
    assert page["percentile_filters"] == {
        "upside": {"minimum": 75, "maximum": 100},
        "recovery": {"minimum": 50, "maximum": 100},
    }


def test_opportunity_page_counts_inclusive_return_band_before_pagination(tmp_path) -> None:
    store = TimescaleStore(f"sqlite:///{tmp_path / 'opportunities.sqlite'}")
    metadata.create_all(store.engine)
    targets = _targets()
    targets.loc[
        (targets["symbol"] == "BBB") & (targets["date"] == date(2026, 1, 5)),
        "session_return",
    ] = -0.01
    store.upsert_daily_opportunity_targets(targets)

    aaa_return = 3 / 101
    page = store.opportunity_targets_page(
        target_version=DAILY_OPPORTUNITY_TARGET_VERSION_V1_0,
        exchange="NSE",
        source="yfinance",
        session_date=date(2026, 1, 5),
        limit=1,
        session_return_bounds=(aaa_return, aaa_return),
    )

    assert page["total"] == 2
    assert len(page["rows"]) == 1
    assert page["summary"]["return_band_sessions"] == 1
    assert page["summary"]["return_band_eligible_sessions"] == 2
    assert page["summary"]["return_band_ratio"] == 0.5

    losses = store.opportunity_targets_page(
        target_version=DAILY_OPPORTUNITY_TARGET_VERSION_V1_0,
        exchange="NSE",
        source="yfinance",
        session_date=date(2026, 1, 5),
        session_return_bounds=(None, -0.01),
    )
    assert losses["summary"]["return_band_sessions"] == 1

    with pytest.raises(ValueError, match="minimum cannot exceed maximum"):
        store.opportunity_targets_page(
            target_version=DAILY_OPPORTUNITY_TARGET_VERSION_V1_0,
            exchange="NSE",
            source="yfinance",
            session_return_bounds=(0.02, 0.01),
        )


def test_opportunity_page_defaults_to_latest_session_with_sufficient_coverage(tmp_path) -> None:
    store = TimescaleStore(f"sqlite:///{tmp_path / 'opportunities.sqlite'}")
    metadata.create_all(store.engine)
    targets = _targets()
    partial = targets.iloc[[1]].copy()
    partial["date"] = date(2026, 1, 6)
    store.upsert_daily_opportunity_targets(pd.concat([targets, partial], ignore_index=True))

    page = store.opportunity_targets_page(
        target_version=DAILY_OPPORTUNITY_TARGET_VERSION_V1_0,
        exchange="NSE",
        source="yfinance",
        minimum_session_coverage=0.95,
    )

    assert page["session_date"] == date(2026, 1, 5)
    assert page["latest_available_date"] == date(2026, 1, 6)
    assert page["latest_complete_date"] == date(2026, 1, 5)
    assert page["session_instruments"] == 2
    assert page["expected_instruments"] == 2
    assert page["coverage_ratio"] == 1.0
    assert page["coverage_status"] == "complete"
    assert page["available_sessions"][0] == {
        "date": date(2026, 1, 6),
        "instruments": 1,
        "expected_instruments": 2,
        "coverage_ratio": 0.5,
        "coverage_status": "partial",
    }

    explicit_partial = store.opportunity_targets_page(
        target_version=DAILY_OPPORTUNITY_TARGET_VERSION_V1_0,
        exchange="NSE",
        source="yfinance",
        session_date=date(2026, 1, 6),
    )
    assert explicit_partial["session_date"] == date(2026, 1, 6)
    assert explicit_partial["session_exists"] is True
    assert explicit_partial["expected_instruments"] == 2
    assert explicit_partial["coverage_ratio"] == 0.5
    assert explicit_partial["coverage_status"] == "partial"


def test_opportunity_histograms_keep_outliers_without_flattening_the_scale(tmp_path) -> None:
    store = TimescaleStore(f"sqlite:///{tmp_path / 'opportunities.sqlite'}")
    metadata.create_all(store.engine)
    rows = []
    for index in range(101):
        symbol = f"S{index:03d}"
        instrument_key = f"NSE_EQ|{symbol}"
        rows.append(
            {
                "Date": date(2026, 1, 2),
                "Open": 100,
                "High": 101,
                "Low": 99,
                "Close": 100,
                "Volume": 100_000,
                "OpenInterest": 0,
                "InstrumentKey": instrument_key,
                "Symbol": symbol,
                "Exchange": "NSE",
                "Source": "yfinance",
            }
        )
        close = 50 if index == 0 else 100 + (((index % 11) - 5) / 10)
        rows.append(
            {
                "Date": date(2026, 1, 5),
                "Open": 100,
                "High": max(101, close + 1),
                "Low": min(99, close - 1),
                "Close": close,
                "Volume": 100_000,
                "OpenInterest": 0,
                "InstrumentKey": instrument_key,
                "Symbol": symbol,
                "Exchange": "NSE",
                "Source": "yfinance",
            }
        )
    targets = DailyOpportunityTargetBuilder(
        computed_at=datetime(2026, 1, 6, tzinfo=UTC)
    ).build(pd.DataFrame(rows))
    store.upsert_daily_opportunity_targets(targets)

    page = store.opportunity_targets_page(
        target_version=DAILY_OPPORTUNITY_TARGET_VERSION_V1_0,
        exchange="NSE",
        source="yfinance",
        session_date=date(2026, 1, 5),
    )
    distribution = page["distributions"]["session_return"]

    assert distribution["minimum"] == -0.5
    assert distribution["display_minimum"] > distribution["minimum"]
    assert distribution["bins"][0]["lower_overflow"] is True
    assert sum(item["count"] for item in distribution["bins"]) == 101
