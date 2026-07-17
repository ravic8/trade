from datetime import date

import trade_research.data.coverage as coverage_module
from trade_research.config import Settings
from trade_research.data.coverage import CoveragePreviewInput, build_daily_coverage_preview


class FakeCoverageStore:
    def __init__(self) -> None:
        self.instruments = [
            {
                "instrument_key": "NSE_EQ|AAA",
                "trading_symbol": "AAA",
                "name": "AAA Limited",
                "isin": "INE000AAA",
            },
            {
                "instrument_key": "NSE_EQ|BBB",
                "trading_symbol": "BBB",
                "name": "BBB Limited",
                "isin": "INE000BBB",
            },
        ]
        self.stored_dates = {
            "NSE_EQ|AAA": {date(2026, 1, 1), date(2026, 1, 2)},
            "NSE_EQ|BBB": {date(2026, 1, 1)},
        }

    def resolve_provider_instruments(
        self,
        symbols: list[str],
        source: str = "upstox",
        exchange: str = "NSE",
    ) -> list[dict]:
        assert source == "upstox"
        assert exchange == "NSE"
        requested = set(symbols)
        return [
            row
            for row in self.instruments
            if str(row["trading_symbol"]).upper() in requested
        ]

    def daily_ohlcv_dates_by_instrument(
        self,
        instrument_keys: list[str],
        start_date: date,
        end_date: date,
        source: str = "upstox",
        exchange: str = "NSE",
    ) -> dict[str, set[date]]:
        return {
            key: {
                value
                for value in self.stored_dates.get(key, set())
                if start_date <= value <= end_date
            }
            for key in instrument_keys
        }

    def first_daily_ohlcv_dates_by_instrument(
        self,
        instrument_keys: list[str],
        source: str = "upstox",
        exchange: str = "NSE",
    ) -> dict[str, date]:
        return {
            key: min(self.stored_dates[key])
            for key in instrument_keys
            if self.stored_dates.get(key)
        }

    def exchange_holidays(
        self,
        exchange: str,
        year: int,
        max_age_days: int | None = None,
    ) -> dict | None:
        assert exchange == "NSE"
        assert max_age_days is None
        return {
            "source_url": "test",
            "closed_dates": ["2026-01-05"],
            "early_close_dates": [],
            "year": year,
        }


class NoHolidayCoverageStore(FakeCoverageStore):
    def exchange_holidays(
        self,
        exchange: str,
        year: int,
        max_age_days: int | None = None,
    ) -> dict | None:
        return None


class AmbiguousCoverageStore(FakeCoverageStore):
    def __init__(self) -> None:
        super().__init__()
        self.instruments.extend(
            [
                {
                    "instrument_key": "NSE_EQ|AAA_DUP",
                    "trading_symbol": "AAA",
                    "name": "AAA Duplicate",
                    "isin": "INE000AAD",
                }
            ]
        )


class MaterializedCoverageStore(FakeCoverageStore):
    def exchange_sessions(self, exchange: str, start_date: date, end_date: date):
        open_dates = {date(2026, 1, 1), date(2026, 1, 2), date(2026, 1, 6)}
        rows = []
        current = start_date
        while current <= end_date:
            rows.append(
                {
                    "exchange": exchange,
                    "session_date": current,
                    "is_trading_day": current in open_dates,
                    "validation_status": "valid",
                }
            )
            current = date.fromordinal(current.toordinal() + 1)
        return rows


def test_daily_coverage_preview_returns_missing_windows_without_fetching() -> None:
    preview = build_daily_coverage_preview(
        CoveragePreviewInput(
            provider="upstox",
            exchange="NSE",
            symbols=("AAA", "BBB", "MISSING"),
            unit="days",
            interval=1,
            start_date=date(2026, 1, 1),
            end_date=date(2026, 1, 6),
        ),
        store=FakeCoverageStore(),
    )

    assert preview["symbols_requested"] == 3
    assert preview["symbols_resolved"] == 2
    assert preview["unresolved_symbols"] == ["MISSING"]
    assert preview["expected_rows"] == 6
    assert preview["already_present_rows"] == 3
    assert preview["missing_rows"] == 3
    assert preview["estimated_provider_calls"] == 3
    assert preview["tasks"] == [
        {
            "symbol": "AAA",
            "trading_symbol": "AAA",
            "instrument_key": "NSE_EQ|AAA",
            "fetch_start": date(2026, 1, 6),
            "fetch_end": date(2026, 1, 6),
            "missing_rows": 1,
            "status": "queued",
        },
        {
            "symbol": "BBB",
            "trading_symbol": "BBB",
            "instrument_key": "NSE_EQ|BBB",
            "fetch_start": date(2026, 1, 2),
            "fetch_end": date(2026, 1, 2),
            "missing_rows": 1,
            "status": "queued",
        },
        {
            "symbol": "BBB",
            "trading_symbol": "BBB",
            "instrument_key": "NSE_EQ|BBB",
            "fetch_start": date(2026, 1, 6),
            "fetch_end": date(2026, 1, 6),
            "missing_rows": 1,
            "status": "queued",
        },
    ]
    assert preview["warnings"] == ["Unresolved symbols: MISSING"]


def test_daily_coverage_preview_flags_ambiguous_symbols() -> None:
    preview = build_daily_coverage_preview(
        CoveragePreviewInput(
            provider="upstox",
            exchange="NSE",
            symbols=("AAA", "BBB"),
            unit="days",
            interval=1,
            start_date=date(2026, 1, 1),
            end_date=date(2026, 1, 2),
        ),
        store=AmbiguousCoverageStore(),
    )

    assert preview["symbols_requested"] == 2
    assert preview["symbols_resolved"] == 1
    assert preview["ambiguous_symbols"] == ["AAA"]
    assert preview["warnings"] == ["Ambiguous symbols: AAA"]


def test_daily_coverage_preview_warns_when_holidays_are_not_stored() -> None:
    preview = build_daily_coverage_preview(
        CoveragePreviewInput(
            provider="upstox",
            exchange="NSE",
            symbols=("AAA",),
            unit="days",
            interval=1,
            start_date=date(2026, 1, 3),
            end_date=date(2026, 1, 4),
        ),
        store=NoHolidayCoverageStore(),
    )

    assert preview["expected_rows"] == 0
    assert preview["tasks"] == []
    assert preview["warnings"] == [
        "No expected trading sessions in the requested date range.",
        "No stored exchange holiday calendar found; preview uses weekdays only.",
    ]


def test_daily_coverage_preview_rejects_non_daily_request() -> None:
    try:
        build_daily_coverage_preview(
            CoveragePreviewInput(
                provider="upstox",
                exchange="NSE",
                symbols=("AAA",),
                unit="minutes",
                interval=1,
                start_date=date(2026, 1, 1),
                end_date=date(2026, 1, 2),
            ),
            store=FakeCoverageStore(),
        )
    except ValueError as exc:
        assert "Only daily candles" in str(exc)
    else:
        raise AssertionError("expected ValueError")


def test_daily_coverage_preview_switches_to_materialized_sessions(monkeypatch) -> None:
    monkeypatch.setattr(
        coverage_module,
        "get_settings",
        lambda: Settings(_env_file=None, materialized_exchange_sessions_enabled=True),
    )

    preview = build_daily_coverage_preview(
        CoveragePreviewInput(
            provider="upstox",
            exchange="NSE",
            symbols=("AAA",),
            unit="days",
            interval=1,
            start_date=date(2026, 1, 1),
            end_date=date(2026, 1, 6),
        ),
        store=MaterializedCoverageStore(),
    )

    assert preview["calendar_source"] == "materialized_exchange_sessions"
    assert preview["expected_rows"] == 3
    assert preview["missing_rows"] == 1
