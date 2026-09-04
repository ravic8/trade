from datetime import date
from uuid import UUID

import pandas as pd
import pytest

from trade_research.data.coverage import CoveragePreviewInput
from trade_research.data.on_demand import run_daily_ohlcv_request


class FakeExecutionStore:
    def __init__(self, expected_items_requested: int = 1) -> None:
        self.finished: list[dict] = []
        self.upserted_rows = 0
        self.coverage_rows = 0
        self.audit_rows = 0
        self.expected_items_requested = expected_items_requested

    def resolve_provider_instruments(
        self,
        symbols: list[str],
        source: str = "upstox",
        exchange: str = "NSE",
    ) -> list[dict]:
        return [
            {
                "instrument_key": f"NSE_EQ|{symbol}",
                "trading_symbol": symbol,
                "name": f"{symbol} Limited",
                "isin": f"INE{symbol}",
            }
            for symbol in symbols
        ]

    def daily_ohlcv_dates_by_instrument(
        self,
        instrument_keys: list[str],
        start_date: date,
        end_date: date,
        source: str = "upstox",
        exchange: str = "NSE",
        *,
        valid_only: bool = False,
    ) -> dict[str, set[date]]:
        return {key: {start_date} for key in instrument_keys}

    def exchange_holidays(
        self,
        exchange: str,
        year: int,
        max_age_days: int | None = None,
    ) -> dict:
        return {
            "source_url": "test",
            "closed_dates": [],
            "early_close_dates": [],
            "year": year,
        }

    def start_ingestion_run(
        self,
        job_name: str,
        exchange: str,
        source: str,
        items_requested: int,
        run_metadata: dict | None = None,
    ) -> UUID:
        assert job_name == "upstox_nse_daily_ohlcv"
        assert exchange == "NSE"
        assert source == "upstox"
        assert items_requested == self.expected_items_requested
        assert run_metadata is not None
        assert run_metadata["trigger"] == "ui"
        return UUID("00000000-0000-0000-0000-000000000001")

    def finish_ingestion_run(
        self,
        run_id: UUID,
        status: str,
        items_processed: int,
        items_succeeded: int,
        items_failed: int,
        error_message: str | None = None,
    ) -> None:
        self.finished.append(
            {
                "run_id": str(run_id),
                "status": status,
                "items_processed": items_processed,
                "items_succeeded": items_succeeded,
                "items_failed": items_failed,
                "error_message": error_message,
            }
        )

    def upsert_daily_ohlcv(
        self,
        frame: pd.DataFrame,
        exchange: str = "NSE",
        source: str = "upstox",
    ) -> int:
        self.upserted_rows += len(frame)
        return len(frame)

    def insert_daily_ohlcv_fetch_coverage(
        self,
        run_id: str,
        coverage: pd.DataFrame,
        source: str = "upstox",
        exchange: str = "NSE",
    ) -> int:
        assert run_id == "00000000-0000-0000-0000-000000000001"
        assert coverage["instrument_key"].tolist() == ["NSE_EQ|AAA"]
        assert coverage["fetch_status"].tolist() == ["fetched"]
        self.coverage_rows += len(coverage)
        return len(coverage)

    def insert_data_quality_audits(
        self,
        audit: pd.DataFrame,
        dataset_name: str,
        source: str,
        interval: str,
    ) -> int:
        assert dataset_name == "nse_daily_ohlcv"
        assert interval == "1d"
        self.audit_rows += len(audit)
        return len(audit)


class FakeDailyProvider:
    def fetch_daily_candles(
        self,
        instrument_key: str,
        start: date,
        end: date,
        symbol: str,
        trading_symbol: str | None = None,
    ) -> pd.DataFrame:
        assert instrument_key == "NSE_EQ|AAA"
        assert start == date(2026, 1, 2)
        assert end == date(2026, 1, 2)
        return pd.DataFrame(
            [
                {
                    "Date": date(2026, 1, 2),
                    "Open": 100.0,
                    "High": 110.0,
                    "Low": 95.0,
                    "Close": 105.0,
                    "Volume": 1234,
                    "OpenInterest": 0,
                    "InstrumentKey": instrument_key,
                    "Symbol": symbol,
                    "TradingSymbol": trading_symbol or symbol,
                    "Source": "upstox",
                }
            ]
        )


class FailingDailyProvider:
    def fetch_daily_candles(
        self,
        instrument_key: str,
        start: date,
        end: date,
        symbol: str,
        trading_symbol: str | None = None,
    ) -> pd.DataFrame:
        raise RuntimeError("provider unavailable")


class LenientExecutionStore(FakeExecutionStore):
    def insert_daily_ohlcv_fetch_coverage(
        self,
        run_id: str,
        coverage: pd.DataFrame,
        source: str = "upstox",
        exchange: str = "NSE",
    ) -> int:
        self.last_coverage = coverage.copy()
        self.coverage_rows += len(coverage)
        return len(coverage)


class MultiSymbolExecutionStore(LenientExecutionStore):
    def __init__(self) -> None:
        super().__init__(expected_items_requested=2)


class MultiSymbolProvider:
    def __init__(self) -> None:
        self.calls: list[tuple[str, date, date]] = []

    def fetch_daily_candles(
        self,
        instrument_key: str,
        start: date,
        end: date,
        symbol: str,
        trading_symbol: str | None = None,
    ) -> pd.DataFrame:
        self.calls.append((instrument_key, start, end))
        return pd.DataFrame(
            [
                {
                    "Date": end,
                    "Open": 100.0,
                    "High": 110.0,
                    "Low": 95.0,
                    "Close": 105.0,
                    "Volume": 1234,
                    "OpenInterest": 0,
                    "InstrumentKey": instrument_key,
                    "Symbol": symbol,
                    "TradingSymbol": trading_symbol or symbol,
                    "Source": "upstox",
                }
            ]
        )


def test_run_daily_ohlcv_request_executes_sequential_fetch_and_records_state() -> None:
    store = FakeExecutionStore()

    result = run_daily_ohlcv_request(
        CoveragePreviewInput(
            provider="upstox",
            exchange="NSE",
            symbols=("AAA",),
            unit="days",
            interval=1,
            start_date=date(2026, 1, 1),
            end_date=date(2026, 1, 2),
        ),
        store=store,
        access_token=None,
        provider=FakeDailyProvider(),
    )

    assert result.run_id == "00000000-0000-0000-0000-000000000001"
    assert result.status == "completed"
    assert result.rows_fetched == 1
    assert result.rows_upserted == 1
    assert result.fetch_coverage_rows == 1
    assert result.audit_rows == 1
    assert store.finished == [
        {
            "run_id": "00000000-0000-0000-0000-000000000001",
            "status": "completed",
            "items_processed": 1,
            "items_succeeded": 1,
            "items_failed": 0,
            "error_message": None,
        }
    ]


def test_run_daily_ohlcv_request_supports_bounded_parallel_fetches() -> None:
    store = MultiSymbolExecutionStore()
    provider = MultiSymbolProvider()

    result = run_daily_ohlcv_request(
        CoveragePreviewInput(
            provider="upstox",
            exchange="NSE",
            symbols=("AAA", "BBB"),
            unit="days",
            interval=1,
            start_date=date(2026, 1, 1),
            end_date=date(2026, 1, 2),
        ),
        store=store,
        access_token=None,
        provider=provider,
        max_concurrent_fetches=2,
    )

    assert result.status == "completed"
    assert result.max_concurrent_fetches == 2
    assert result.rows_fetched == 2
    assert result.rows_upserted == 2
    assert sorted(provider.calls) == [
        ("NSE_EQ|AAA", date(2026, 1, 2), date(2026, 1, 2)),
        ("NSE_EQ|BBB", date(2026, 1, 2), date(2026, 1, 2)),
    ]
    assert store.finished[-1]["items_processed"] == 2
    assert store.finished[-1]["items_succeeded"] == 2


def test_run_daily_ohlcv_request_records_provider_failures() -> None:
    store = LenientExecutionStore()

    result = run_daily_ohlcv_request(
        CoveragePreviewInput(
            provider="upstox",
            exchange="NSE",
            symbols=("AAA",),
            unit="days",
            interval=1,
            start_date=date(2026, 1, 1),
            end_date=date(2026, 1, 2),
        ),
        store=store,
        access_token=None,
        provider=FailingDailyProvider(),
    )

    assert result.status == "completed_with_warnings"
    assert result.rows_fetched == 0
    assert result.rows_upserted == 0
    assert result.failures == [
        {"symbol": "AAA", "instrument_key": "NSE_EQ|AAA", "error": "provider unavailable"}
    ]
    assert store.last_coverage["fetch_status"].tolist() == ["failed"]
    assert store.finished[-1] == {
        "run_id": "00000000-0000-0000-0000-000000000001",
        "status": "completed_with_warnings",
        "items_processed": 1,
        "items_succeeded": 0,
        "items_failed": 1,
        "error_message": None,
    }


def test_run_daily_ohlcv_request_requires_token_when_fetching_without_provider() -> None:
    with pytest.raises(ValueError, match="UPSTOX_ACCESS_TOKEN"):
        run_daily_ohlcv_request(
            CoveragePreviewInput(
                provider="upstox",
                exchange="NSE",
                symbols=("AAA",),
                unit="days",
                interval=1,
                start_date=date(2026, 1, 1),
                end_date=date(2026, 1, 2),
            ),
            store=FakeExecutionStore(),
            access_token=None,
        )
