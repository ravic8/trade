from __future__ import annotations

from datetime import UTC, date, datetime

import pandas as pd
import pytest

from trade_research.config import Settings
from trade_research.exchange_sessions import (
    ExchangeSessionError,
    build_materialized_exchange_sessions,
    classify_exchange_session,
    expected_dates_for_instrument,
    resolve_expected_session_dates,
    shadow_compare_exchange_sessions,
    validate_materialized_exchange_sessions,
)
from trade_research.market_calendar import ExchangeHolidays
from trade_research.pipelines import exchange_sessions as exchange_sessions_pipeline
from trade_research.pipelines.exchange_sessions import (
    run_exchange_session_materialization_pipeline,
)
from trade_research.pipelines.yfinance_daily import _build_yfinance_missing_fetch_plan


class MemorySessionStore:
    def __init__(self, sessions=None) -> None:
        self.sessions = sessions or []
        self.holidays: dict[tuple[str, int], dict] = {}

    def exchange_sessions(self, exchange: str, start_date: date, end_date: date):
        return [
            row
            for row in self.sessions
            if row["exchange"] == exchange and start_date <= row["session_date"] <= end_date
        ]

    def exchange_holidays(
        self,
        exchange: str,
        year: int,
        max_age_days: int | None = None,
    ):
        return self.holidays.get((exchange, year))

    def upsert_exchange_holidays(
        self,
        exchange: str,
        year: int,
        closed_dates,
        early_close_dates,
        source_url: str,
        fetched_at=None,
    ) -> int:
        self.holidays[(exchange, year)] = {
            "closed_dates": sorted(value.isoformat() for value in closed_dates),
            "early_close_dates": sorted(value.isoformat() for value in early_close_dates),
            "source_url": source_url,
        }
        return 1

    def upsert_exchange_sessions(self, sessions) -> int:
        incoming = list(sessions)
        rows_by_key = {
            (row["exchange"], row["session_date"]): row
            for row in self.sessions
        }
        rows_by_key.update(
            {
                (row["exchange"], row["session_date"]): row
                for row in incoming
            }
        )
        self.sessions = list(rows_by_key.values())
        return len(incoming)

    def delete_exchange_sessions(
        self,
        exchange: str,
        start_date: date,
        end_date: date,
    ) -> int:
        retained = [
            row
            for row in self.sessions
            if not (
                row["exchange"] == exchange
                and start_date <= row["session_date"] <= end_date
            )
        ]
        deleted = len(self.sessions) - len(retained)
        self.sessions = retained
        return deleted


def test_phase3_defaults_keep_planning_in_shadow_mode() -> None:
    settings = Settings(_env_file=None)

    assert settings.materialized_exchange_sessions_enabled is False
    assert settings.exchange_session_history_years == 10
    assert settings.exchange_session_future_years == 1
    assert settings.exchange_session_shadow_max_discrepancies == 5


def test_us_materialization_includes_closed_dates_and_early_closes() -> None:
    sessions = build_materialized_exchange_sessions(
        "US",
        date(2026, 7, 1),
        date(2026, 11, 28),
        generated_at=datetime(2026, 1, 1, tzinfo=UTC),
    )
    by_date = {row.session_date: row for row in sessions}

    assert by_date[date(2026, 7, 2)].is_trading_day is True
    assert by_date[date(2026, 7, 3)].is_trading_day is False
    assert by_date[date(2026, 7, 4)].is_trading_day is False
    assert by_date[date(2026, 11, 27)].is_early_close is True
    assert by_date[date(2026, 11, 27)].market_close_utc == datetime(
        2026, 11, 27, 18, 0, tzinfo=UTC
    )


def test_tsx_materialization_represents_christmas_eve_early_close() -> None:
    sessions = build_materialized_exchange_sessions(
        "TSX",
        date(2026, 12, 24),
        date(2026, 12, 25),
    )

    assert sessions[0].is_trading_day is True
    assert sessions[0].is_early_close is True
    assert sessions[1].is_trading_day is False


def test_official_holiday_override_closes_library_session() -> None:
    sessions = build_materialized_exchange_sessions(
        "NSE",
        date(2026, 1, 14),
        date(2026, 1, 16),
        holiday_overrides={
            2026: ExchangeHolidays(
                closed_dates=frozenset({date(2026, 1, 15)}),
                early_close_dates=frozenset(),
                source_url="official",
            )
        },
    )

    assert [row.is_trading_day for row in sessions] == [True, False, True]


def test_observed_special_session_reopens_weekend_with_auditable_status() -> None:
    sessions = build_materialized_exchange_sessions(
        "NSE",
        date(2025, 2, 1),
        date(2025, 2, 1),
        observed_special_open_dates={date(2025, 2, 1)},
    )

    assert sessions[0].is_trading_day is True
    assert sessions[0].validation_status == "valid_observed_special_session"
    assert sessions[0].source_url == "stored_ohlcv_observation"


def test_observed_special_session_can_override_an_official_closure() -> None:
    special_date = date(2026, 1, 15)
    sessions = build_materialized_exchange_sessions(
        "NSE",
        special_date,
        special_date,
        holiday_overrides={
            2026: ExchangeHolidays(
                closed_dates=frozenset({special_date}),
                early_close_dates=frozenset(),
                source_url="official",
            )
        },
        observed_special_open_dates={special_date},
    )

    assert sessions[0].is_trading_day is True
    assert sessions[0].validation_status == "valid_observed_special_session"


def test_full_year_session_validation_checks_completeness_and_open_day_bounds() -> None:
    start = date(2026, 1, 1)
    end = date(2026, 12, 31)
    sessions = build_materialized_exchange_sessions("US", start, end)

    validation = validate_materialized_exchange_sessions(sessions, start, end)
    incomplete = validate_materialized_exchange_sessions(sessions[:-1], start, end)

    assert validation.accepted is True
    assert validation.row_count == 365
    assert 220 <= validation.open_days_by_year[2026] <= 260
    assert validation.early_close_count > 0
    assert incomplete.accepted is False
    assert incomplete.errors[0].startswith("incomplete_date_range")


def test_shadow_comparison_reports_date_level_differences() -> None:
    sessions = build_materialized_exchange_sessions(
        "US",
        date(2026, 1, 1),
        date(2026, 12, 31),
    )
    holidays = ExchangeHolidays(
        closed_dates=frozenset(),
        early_close_dates=frozenset(),
        source_url="weekday-only",
    )

    comparison = shadow_compare_exchange_sessions(sessions, {2026: holidays})

    assert comparison.compared_years == (2026,)
    assert comparison.discrepancy_count > 0
    assert date(2026, 7, 3) in comparison.legacy_only_dates


def test_materialized_resolution_requires_every_calendar_date() -> None:
    rows = [
        item.as_row()
        for item in build_materialized_exchange_sessions(
            "US",
            date(2026, 7, 1),
            date(2026, 7, 5),
        )
    ]
    store = MemorySessionStore(rows)

    resolution = resolve_expected_session_dates(
        store,
        "US",
        date(2026, 7, 1),
        date(2026, 7, 5),
        use_materialized_sessions=True,
    )

    assert resolution.dates == (date(2026, 7, 1), date(2026, 7, 2))
    store.sessions.pop()
    with pytest.raises(ExchangeSessionError, match="incomplete or invalid"):
        resolve_expected_session_dates(
            store,
            "US",
            date(2026, 7, 1),
            date(2026, 7, 5),
            use_materialized_sessions=True,
        )


def test_session_classification_respects_close_and_provider_grace() -> None:
    session = next(
        item
        for item in build_materialized_exchange_sessions(
            "US",
            date(2026, 7, 2),
            date(2026, 7, 2),
        )
    )

    assert classify_exchange_session(
        session,
        at=datetime(2026, 7, 2, 19, 0, tzinfo=UTC),
        provider_grace_minutes=120,
    ) == "market_not_closed"
    assert classify_exchange_session(
        session,
        at=datetime(2026, 7, 2, 20, 30, tzinfo=UTC),
        provider_grace_minutes=120,
    ) == "provider_pending"
    assert classify_exchange_session(
        session,
        at=datetime(2026, 7, 2, 22, 1, tzinfo=UTC),
        provider_grace_minutes=120,
    ) == "expected"


def test_first_trade_date_excludes_prelisting_sessions_from_coverage() -> None:
    expected = (date(2026, 7, 6), date(2026, 7, 7), date(2026, 7, 8))

    clipped = expected_dates_for_instrument(
        expected,
        coverage_start=date(2026, 7, 6),
        first_trade_date=date(2026, 7, 7),
    )

    assert clipped == (date(2026, 7, 7), date(2026, 7, 8))


def test_yfinance_missing_plan_does_not_queue_prelisting_dates() -> None:
    plan = _build_yfinance_missing_fetch_plan(
        mapping=pd.DataFrame(
            [
                {
                    "symbol": "NEW",
                    "instrument_key": "YF|NEW",
                    "trading_symbol": "NEW",
                    "yahoo_symbol": "NEW",
                }
            ]
        ),
        expected_set={date(2026, 7, 6), date(2026, 7, 7), date(2026, 7, 8)},
        stored_dates={"YF|NEW": {date(2026, 7, 7)}},
        first_trade_dates={"YF|NEW": date(2026, 7, 7)},
        avg_turnover={},
        coverage_status=None,
        min_avg_daily_turnover=None,
        min_coverage_pct=None,
        limit=None,
    )

    assert plan[["fetch_start", "fetch_end", "expected_rows"]].to_dict(
        orient="records"
    ) == [
        {
            "fetch_start": "2026-07-08",
            "fetch_end": "2026-07-08",
            "expected_rows": 2,
        }
    ]


def test_yfinance_missing_plan_preserves_a_real_gap_at_window_start() -> None:
    plan = _build_yfinance_missing_fetch_plan(
        mapping=pd.DataFrame(
            [
                {
                    "symbol": "OLD",
                    "instrument_key": "YF|OLD",
                    "trading_symbol": "OLD",
                    "yahoo_symbol": "OLD",
                }
            ]
        ),
        expected_set={date(2026, 7, 6), date(2026, 7, 7), date(2026, 7, 8)},
        stored_dates={"YF|OLD": {date(2026, 7, 7)}},
        first_trade_dates={"YF|OLD": date(2020, 1, 2)},
        avg_turnover={},
        coverage_status=None,
        min_avg_daily_turnover=None,
        min_coverage_pct=None,
        limit=None,
    )

    assert plan[["fetch_start", "fetch_end"]].to_dict(orient="records") == [
        {"fetch_start": "2026-07-06", "fetch_end": "2026-07-06"},
        {"fetch_start": "2026-07-08", "fetch_end": "2026-07-08"},
    ]


def test_materialization_pipeline_persists_valid_full_year(monkeypatch) -> None:
    store = MemorySessionStore()
    monkeypatch.setattr(
        exchange_sessions_pipeline,
        "get_settings",
        lambda: Settings(_env_file=None),
    )

    result = run_exchange_session_materialization_pipeline(
        "US",
        repository=store,
        as_of_date=date(2026, 7, 17),
        history_years=0,
        future_years=0,
        holiday_loader=lambda exchange, year: ExchangeHolidays(
            closed_dates=frozenset(
                {
                    date(2026, 1, 1),
                    date(2026, 1, 19),
                    date(2026, 2, 16),
                    date(2026, 4, 3),
                    date(2026, 5, 25),
                    date(2026, 6, 19),
                    date(2026, 7, 3),
                    date(2026, 9, 7),
                    date(2026, 11, 26),
                    date(2026, 12, 25),
                }
            ),
            early_close_dates=frozenset(
                {date(2026, 11, 27), date(2026, 12, 24)}
            ),
            source_url="official-test",
        ),
        generated_at=datetime(2026, 7, 17, tzinfo=UTC),
        trigger="test",
    )

    assert result.status == "pass"
    assert result.rows == 365
    assert result.metrics["shadow_discrepancy_count"] == 0
    assert result.metrics["planning_enabled"] is False
    assert len(store.sessions) == 365


def test_materialization_pipeline_skips_an_incomplete_future_year(monkeypatch) -> None:
    stale_future_row = build_materialized_exchange_sessions(
        "NSE",
        date(2027, 1, 4),
        date(2027, 1, 4),
    )[0].as_row()
    store = MemorySessionStore([stale_future_row])
    monkeypatch.setattr(
        exchange_sessions_pipeline,
        "get_settings",
        lambda: Settings(
            _env_file=None,
            exchange_session_shadow_max_discrepancies=30,
        ),
    )

    result = run_exchange_session_materialization_pipeline(
        "NSE",
        repository=store,
        as_of_date=date(2026, 7, 17),
        history_years=0,
        future_years=1,
        holiday_loader=lambda exchange, year: ExchangeHolidays(
            closed_dates=frozenset(),
            early_close_dates=frozenset(),
            source_url="official-test",
        ),
        generated_at=datetime(2026, 7, 17, tzinfo=UTC),
        trigger="test",
    )

    assert result.status == "warn"
    assert result.rows == 365
    assert result.metrics["end_date"] == "2026-12-31"
    assert result.metrics["requested_end_date"] == "2027-12-31"
    assert result.metrics["future_years_skipped"] == {
        2027: ["open_day_count_out_of_range:2027:261:220-260"]
    }
    assert result.metrics["future_rows_deleted"] == 1
    assert all(row["session_date"].year == 2026 for row in store.sessions)
    assert any(
        "Skipped incomplete future calendar year 2027" in warning
        for warning in result.warnings
    )
