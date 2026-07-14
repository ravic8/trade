from datetime import UTC, date, datetime
from types import SimpleNamespace

from fastapi.testclient import TestClient

from trade_research.api.app import app


class FakeCoverageStore:
    def __init__(self) -> None:
        self.credentials: dict[tuple[str, str], dict] = {}

    def provider_credential(
        self,
        provider: str,
        credential_type: str = "access_token",
    ) -> dict | None:
        return self.credentials.get((provider, credential_type))

    def upsert_provider_credential(
        self,
        provider: str,
        credential_type: str,
        encrypted_value: str,
        updated_by: str,
        validation_status: str,
        validation_message: str | None = None,
        last_validated_at: datetime | None = None,
    ) -> None:
        self.credentials[(provider, credential_type)] = {
            "provider": provider,
            "credential_type": credential_type,
            "encrypted_value": encrypted_value,
            "updated_at": datetime(2026, 1, 6, 11, tzinfo=UTC),
            "updated_by": updated_by,
            "last_validated_at": last_validated_at,
            "validation_status": validation_status,
            "validation_message": validation_message,
        }

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
    ) -> dict:
        if source == "yfinance":
            assert exchange == "US"
            return {
                "YF|AAPL": {start_date, start_date.replace(day=2)},
                "YF|MSFT": {start_date, start_date.replace(day=2), start_date.replace(day=6)},
            }
        return {
            "NSE_EQ|AAA": {start_date, start_date.replace(day=2)},
            "NSE_EQ|BBB": {start_date},
        }

    def daily_ohlcv_average_turnover_by_instrument(
        self,
        instrument_keys: list[str],
        start_date: date,
        end_date: date,
        source: str = "upstox",
        exchange: str = "NSE",
    ) -> dict:
        assert start_date <= end_date
        if source == "yfinance":
            assert exchange == "US"
            return {
                "YF|AAPL": 100.0,
                "YF|MSFT": 500.0,
            }
        return {}

    def exchange_holidays(
        self,
        exchange: str,
        year: int,
        max_age_days: int | None = None,
    ) -> dict:
        return {
            "source_url": "test",
            "closed_dates": ["2026-01-05"],
            "early_close_dates": [],
            "year": year,
        }

    def latest_runs(
        self,
        limit: int = 20,
        source: str | None = None,
        exchange: str | None = None,
        status: str | None = None,
    ) -> list[dict]:
        assert source == "upstox"
        assert exchange == "NSE"
        rows = [_run_row()]
        if status:
            rows = [row for row in rows if row["status"] == status]
        return rows[:limit]

    def ingestion_run(self, run_id: str) -> dict | None:
        return _run_row() if run_id == "run-1" else None

    def daily_ohlcv_fetch_coverage_for_run(
        self,
        run_id: str,
        source: str | None = None,
        exchange: str | None = None,
    ) -> list[dict]:
        assert run_id == "run-1"
        assert source == "upstox"
        assert exchange == "NSE"
        return [
            {
                "run_id": "run-1",
                "instrument_key": "NSE_EQ|AAA",
                "symbol": "AAA",
                "source": "upstox",
                "exchange": "NSE",
                "latest_stored_date": date(2026, 1, 1),
                "fetch_start": date(2026, 1, 2),
                "fetch_end": date(2026, 1, 6),
                "should_fetch": True,
                "status": "fetched",
                "rows_fetched": 2,
                "skip_reason": "",
                "error_message": "",
                "created_at": datetime(2026, 1, 6, 10, tzinfo=UTC),
            }
        ]

    def daily_ohlcv_availability(
        self,
        source: str = "upstox",
        exchange: str = "NSE",
        start_date: date | None = None,
        end_date: date | None = None,
        query_text: str | None = None,
        universe_id: str | None = None,
        coverage_status: str | None = None,
        expected_rows_per_symbol: int = 0,
        limit: int = 50,
        offset: int = 0,
        sort: str = "symbol",
    ) -> dict:
        assert source == "upstox"
        assert exchange == "NSE"
        assert start_date == date(2026, 1, 1)
        assert end_date == date(2026, 1, 6)
        assert query_text == "AAA"
        assert universe_id is None
        assert coverage_status is None
        assert expected_rows_per_symbol == 3
        assert limit == 25
        assert offset == 0
        assert sort == "-coverage_pct"
        return {
            "total": 1,
            "rows": [
                {
                    "symbol": "AAA",
                    "name": "AAA Limited",
                    "instrument_key": "NSE_EQ|AAA",
                    "provider": "upstox",
                    "exchange": "NSE",
                    "interval": "1d",
                    "first_stored_date": date(2026, 1, 1),
                    "latest_stored_date": date(2026, 1, 2),
                    "stored_rows": 2,
                    "expected_rows": 3,
                    "coverage_pct": 2 / 3,
                    "missing_rows": 1,
                    "coverage_status": "partial",
                    "last_successful_run": "run-1",
                    "last_fetch_status": "fetched",
                }
            ],
            "summary": {
                "symbols_total": 1,
                "symbols_complete": 0,
                "symbols_partial": 1,
                "symbols_empty": 0,
                "expected_rows": 3,
                "stored_rows": 2,
                "missing_rows": 1,
                "estimated_provider_calls_for_missing": 1,
            },
        }

    def seeded_daily_ohlcv_availability(
        self,
        symbols: list[dict],
        source: str,
        exchange: str,
        start_date: date | None = None,
        end_date: date | None = None,
        query_text: str | None = None,
        coverage_status: str | None = None,
        expected_rows_per_symbol: int = 0,
        limit: int = 50,
        offset: int = 0,
        sort: str = "symbol",
    ) -> dict:
        assert source == "yfinance"
        assert exchange == "US"
        assert symbols[0] == {
            "symbol": "AAPL",
            "name": "Apple",
            "instrument_key": "YF|AAPL",
        }
        assert start_date == date(2026, 1, 1)
        assert end_date == date(2026, 1, 6)
        assert query_text == "AAPL"
        assert coverage_status == "partial"
        assert expected_rows_per_symbol == 4
        assert limit == 10
        assert offset == 0
        assert sort == "symbol"
        return {
            "total": 1,
            "rows": [
                {
                    "symbol": "AAPL",
                    "name": "Apple",
                    "instrument_key": "YF|AAPL",
                    "provider": "yfinance",
                    "exchange": "US",
                    "interval": "1d",
                    "first_stored_date": date(2026, 1, 1),
                    "latest_stored_date": date(2026, 1, 2),
                    "stored_rows": 2,
                    "expected_rows": 4,
                    "coverage_pct": 0.5,
                    "missing_rows": 2,
                    "coverage_status": "partial",
                    "last_successful_run": "run-yf",
                    "last_fetch_status": "fetched",
                }
            ],
            "summary": {
                "symbols_total": 1,
                "symbols_complete": 0,
                "symbols_partial": 1,
                "symbols_empty": 0,
                "expected_rows": 4,
                "stored_rows": 2,
                "missing_rows": 2,
                "estimated_provider_calls_for_missing": 1,
            },
        }

    def search_provider_instruments(
        self,
        query_text: str,
        source: str = "upstox",
        exchange: str = "NSE",
        limit: int = 20,
    ) -> list[dict]:
        assert query_text == "rel"
        assert source == "upstox"
        assert exchange == "NSE"
        assert limit == 5
        return [
            {
                "symbol": "RELIANCE",
                "name": "Reliance Industries Limited",
                "instrument_key": "NSE_EQ|INE002A01018",
                "provider": "upstox",
                "exchange": "NSE",
                "isin": "INE002A01018",
                "segment": "NSE_EQ",
                "asset_type": "EQ",
            }
        ]

    def tradable_universes(
        self,
        exchange: str = "NSE",
        source: str | None = None,
    ) -> list[dict]:
        assert exchange == "NSE"
        assert source is None
        return [
            {
                "universe_id": "nse_liquid_adt_100cr",
                "name": "NSE liquid equities ADT >= Rs 100 crore",
                "description": "Liquid mapped universe",
                "exchange": "NSE",
                "source": "liquidity_plus_upstox_mapping",
                "criteria": {"lookback": "6 months"},
                "created_at": datetime(2026, 1, 1, 9, tzinfo=UTC),
                "member_count": 2,
            }
        ]

    def tradable_universe_members(
        self,
        universe_id: str,
        limit: int = 500,
        offset: int = 0,
    ) -> list[dict]:
        assert universe_id == "nse_liquid_adt_100cr"
        assert limit == 100
        assert offset == 0
        return [
            {
                "universe_id": universe_id,
                "symbol": "AAA",
                "instrument_key": "NSE_EQ|AAA",
                "rank": 1,
                "avg_daily_volume": 1000.0,
                "avg_daily_turnover": 1_000_000_000.0,
                "trading_days": 120,
                "zero_volume_ratio": 0.0,
                "start_date": date(2025, 1, 1),
                "end_date": date(2025, 6, 30),
                "included_at": datetime(2026, 1, 1, 9, tzinfo=UTC),
            }
        ]


class ColdHolidayStore(FakeCoverageStore):
    def __init__(self) -> None:
        super().__init__()
        self.upserted_years: list[int] = []

    def exchange_holidays(
        self,
        exchange: str,
        year: int,
        max_age_days: int | None = None,
    ) -> dict | None:
        if year in self.upserted_years:
            return {
                "source_url": "fetched",
                "closed_dates": ["2026-01-05"],
                "early_close_dates": [],
                "year": year,
            }
        return None

    def upsert_exchange_holidays(
        self,
        exchange: str,
        year: int,
        closed_dates,
        early_close_dates,
        source_url: str,
    ) -> int:
        assert exchange == "NSE"
        assert date(2026, 1, 5) in closed_dates
        assert source_url == "fetched"
        self.upserted_years.append(year)
        return 1


def _run_row() -> dict:
    return {
        "run_id": "run-1",
        "job_name": "upstox_nse_daily_ohlcv",
        "status": "completed",
        "exchange": "NSE",
        "source": "upstox",
        "started_at": datetime(2026, 1, 6, 9, 0, tzinfo=UTC),
        "finished_at": datetime(2026, 1, 6, 9, 2, 5, tzinfo=UTC),
        "items_requested": 2,
        "items_processed": 2,
        "items_succeeded": 2,
        "items_failed": 0,
        "error_message": None,
        "run_metadata": {"trigger": "ui"},
    }


def test_upstox_provider_capabilities_endpoint() -> None:
    with TestClient(app) as client:
        response = client.get("/api/data/provider-capabilities/upstox")

    assert response.status_code == 200
    payload = response.json()
    assert payload["provider"] == "upstox"
    assert payload["api_version"] == "v3"
    assert payload["rate_limits"]["standard_api_per_second"] == 50
    daily = [
        item
        for item in payload["historical"]
        if item["unit"] == "days" and item["interval_min"] == 1
    ][0]
    assert daily["available_from"] == "2000-01-01"
    assert daily["max_window"] == "10 years"


def test_admin_provider_credential_status_requires_admin(monkeypatch) -> None:
    monkeypatch.setattr(
        "trade_research.api.app.get_settings",
        lambda: SimpleNamespace(
            admin_emails="admin@example.com",
            admin_email_headers="cf-access-authenticated-user-email",
            upstox_access_token=None,
        ),
    )

    with TestClient(app) as client:
        response = client.get("/api/admin/provider-credentials/upstox/status")

    assert response.status_code == 403


def test_admin_provider_credential_status_reports_env_fallback(monkeypatch) -> None:
    monkeypatch.setattr("trade_research.api.app._store", lambda: FakeCoverageStore())
    monkeypatch.setattr(
        "trade_research.api.app.get_settings",
        lambda: SimpleNamespace(
            admin_emails="admin@example.com",
            admin_email_headers="cf-access-authenticated-user-email",
            upstox_access_token="env-token",
        ),
    )

    with TestClient(app) as client:
        response = client.get(
            "/api/admin/provider-credentials/upstox/status",
            headers={"cf-access-authenticated-user-email": "admin@example.com"},
        )

    assert response.status_code == 200
    payload = response.json()
    assert payload["configured"] is True
    assert payload["source"] == "env"
    assert "access_token" not in payload


def test_admin_upstox_token_save_encrypts_and_masks_secret(monkeypatch) -> None:
    store = FakeCoverageStore()
    monkeypatch.setattr("trade_research.api.app._store", lambda: store)
    monkeypatch.setattr(
        "trade_research.api.app.get_settings",
        lambda: SimpleNamespace(
            admin_emails="admin@example.com",
            admin_email_headers="cf-access-authenticated-user-email",
            upstox_access_token=None,
            app_secret_key="test-secret-key",
        ),
    )
    monkeypatch.setattr(
        "trade_research.api.app.validate_upstox_access_token",
        lambda token: (True, "ok"),
    )

    with TestClient(app) as client:
        response = client.post(
            "/api/admin/provider-credentials/upstox/token",
            headers={"cf-access-authenticated-user-email": "ADMIN@example.com"},
            json={"access_token": "x" * 40, "validate": True},
        )

    assert response.status_code == 200
    payload = response.json()
    assert payload["configured"] is True
    assert payload["source"] == "database"
    assert payload["updated_by"] == "admin@example.com"
    saved = store.provider_credential("upstox", "access_token")
    assert saved is not None
    assert saved["encrypted_value"] != "x" * 40


def test_admin_upstox_token_test_uses_supplied_token(monkeypatch) -> None:
    monkeypatch.setattr(
        "trade_research.api.app.get_settings",
        lambda: SimpleNamespace(
            admin_emails="admin@example.com",
            admin_email_headers="cf-access-authenticated-user-email",
            upstox_access_token=None,
            app_secret_key="test-secret-key",
        ),
    )
    seen = {}

    def fake_validate(token: str) -> tuple[bool, str]:
        seen["token"] = token
        return True, "ok"

    monkeypatch.setattr("trade_research.api.app.validate_upstox_access_token", fake_validate)

    with TestClient(app) as client:
        response = client.post(
            "/api/admin/provider-credentials/upstox/test",
            headers={"cf-access-authenticated-user-email": "admin@example.com"},
            json={"access_token": "y" * 40},
        )

    assert response.status_code == 200
    assert response.json()["valid"] is True
    assert seen["token"] == "y" * 40


def test_data_coverage_preview_endpoint(monkeypatch) -> None:
    monkeypatch.setattr("trade_research.api.app._store", lambda: FakeCoverageStore())

    with TestClient(app) as client:
        response = client.post(
            "/api/data/coverage/preview",
            json={
                "provider": "upstox",
                "exchange": "NSE",
                "symbols": ["AAA", "BBB"],
                "unit": "days",
                "interval": 1,
                "start_date": "2026-01-01",
                "end_date": "2026-01-06",
            },
        )

    assert response.status_code == 200
    payload = response.json()
    assert payload["symbols_requested"] == 2
    assert payload["symbols_resolved"] == 2
    assert payload["expected_rows"] == 6
    assert payload["already_present_rows"] == 3
    assert payload["missing_rows"] == 3
    assert payload["estimated_provider_calls"] == 3
    assert payload["tasks"][0]["fetch_start"] == "2026-01-06"


def test_data_coverage_preview_fetches_missing_holiday_calendar(monkeypatch) -> None:
    store = ColdHolidayStore()
    monkeypatch.setattr("trade_research.api.app._store", lambda: store)
    monkeypatch.setattr(
        "trade_research.api.app.fetch_exchange_holidays",
        lambda exchange, year: SimpleNamespace(
            closed_dates=frozenset({date(2026, 1, 5)}),
            early_close_dates=frozenset(),
            source_url="fetched",
        ),
    )

    with TestClient(app) as client:
        response = client.post(
            "/api/data/coverage/preview",
            json={
                "provider": "upstox",
                "exchange": "NSE",
                "symbols": ["AAA"],
                "unit": "days",
                "interval": 1,
                "start_date": "2026-01-01",
                "end_date": "2026-01-06",
            },
        )

    assert response.status_code == 200
    payload = response.json()
    assert payload["expected_rows"] == 3
    assert "No stored exchange holiday calendar found" not in " ".join(payload["warnings"])
    assert store.upserted_years == [2026]


def test_data_coverage_get_endpoint(monkeypatch) -> None:
    monkeypatch.setattr("trade_research.api.app._store", lambda: FakeCoverageStore())

    with TestClient(app) as client:
        response = client.get(
            "/api/data/coverage"
            "?symbols=AAA&symbols=BBB&start_date=2026-01-01&end_date=2026-01-06"
        )

    assert response.status_code == 200
    payload = response.json()
    assert payload["symbols_requested"] == 2
    assert payload["symbols_resolved"] == 2
    assert payload["missing_rows"] == 3


def test_data_coverage_get_requires_dates(monkeypatch) -> None:
    monkeypatch.setattr("trade_research.api.app._store", lambda: FakeCoverageStore())

    with TestClient(app) as client:
        response = client.get("/api/data/coverage?symbols=AAA")

    assert response.status_code == 400
    assert response.json()["detail"] == "start_date and end_date are required"


def test_data_availability_endpoint(monkeypatch) -> None:
    monkeypatch.setattr("trade_research.api.app._store", lambda: FakeCoverageStore())

    with TestClient(app) as client:
        response = client.get(
            "/api/data/availability"
            "?start_date=2026-01-01&end_date=2026-01-06"
            "&query=AAA&limit=25&sort=-coverage_pct"
        )

    assert response.status_code == 200
    payload = response.json()
    assert payload["total"] == 1
    assert payload["summary"]["missing_rows"] == 1
    assert payload["summary"]["estimated_provider_calls_for_missing"] == 1
    assert payload["rows"][0]["symbol"] == "AAA"
    assert payload["rows"][0]["stored_rows"] == 2
    assert payload["rows"][0]["expected_rows"] == 3
    assert payload["rows"][0]["coverage_status"] == "partial"
    assert payload["rows"][0]["last_successful_run"] == "run-1"


def test_data_availability_supports_yfinance_seeded_universe(monkeypatch) -> None:
    monkeypatch.setattr("trade_research.api.app._store", lambda: FakeCoverageStore())

    with TestClient(app) as client:
        response = client.get(
            "/api/data/availability"
            "?provider=yfinance&exchange=US"
            "&start_date=2026-01-01&end_date=2026-01-06"
            "&query=AAPL&coverage_status=partial&limit=10"
        )

    assert response.status_code == 200
    payload = response.json()
    assert payload["provider"] == "yfinance"
    assert payload["exchange"] == "US"
    assert payload["total"] == 1
    assert payload["summary"]["missing_rows"] == 2
    assert payload["rows"][0]["symbol"] == "AAPL"
    assert payload["rows"][0]["instrument_key"] == "YF|AAPL"
    assert payload["rows"][0]["last_successful_run"] == "run-yf"


def test_data_availability_rejects_yfinance_exchange_mismatch() -> None:
    with TestClient(app) as client:
        response = client.get(
            "/api/data/availability"
            "?provider=yfinance&exchange=CA&universe_id=us_seed"
        )

    assert response.status_code == 400
    assert response.json()["detail"] == "universe_id=us_seed does not match exchange=CA."


def test_data_bulk_fetch_preview_supports_yfinance_filters(monkeypatch) -> None:
    monkeypatch.setattr("trade_research.api.app._store", lambda: FakeCoverageStore())

    with TestClient(app) as client:
        response = client.get(
            "/api/data/bulk-fetch-preview"
            "?provider=yfinance&exchange=US"
            "&start_date=2026-01-01&end_date=2026-01-06"
            "&query=AAPL&coverage_status=partial&limit=10"
        )

    assert response.status_code == 200
    payload = response.json()
    assert payload["provider"] == "yfinance"
    assert payload["exchange"] == "US"
    assert payload["universe_id"] == "us_seed"
    assert payload["total"] == 1
    assert payload["summary"]["symbols_partial"] == 1
    assert payload["summary"]["missing_rows"] == 2
    assert payload["rows"][0]["symbol"] == "AAPL"
    assert payload["rows"][0]["stored_rows"] == 2
    assert payload["rows"][0]["expected_rows"] == 4
    assert payload["rows"][0]["coverage_status"] == "partial"
    assert payload["tasks"] == payload["rows"][0]["tasks"]
    assert payload["tasks"][0]["fetch_start"] == "2026-01-05"
    assert payload["tasks"][0]["fetch_end"] == "2026-01-06"


def test_data_bulk_fetch_preview_filters_liquidity_and_coverage(monkeypatch) -> None:
    monkeypatch.setattr("trade_research.api.app._store", lambda: FakeCoverageStore())

    with TestClient(app) as client:
        response = client.get(
            "/api/data/bulk-fetch-preview"
            "?provider=yfinance&exchange=US"
            "&start_date=2026-01-01&end_date=2026-01-06"
            "&coverage_status=partial"
            "&min_avg_daily_turnover=200"
            "&min_coverage_pct=0.75"
            "&sort=-avg_daily_turnover"
        )

    assert response.status_code == 200
    payload = response.json()
    assert payload["min_avg_daily_turnover"] == 200.0
    assert payload["min_coverage_pct"] == 0.75
    assert payload["total"] == 1
    assert payload["summary"]["symbols_partial"] == 1
    assert payload["summary"]["missing_rows"] == 1
    assert payload["rows"][0]["symbol"] == "MSFT"
    assert payload["rows"][0]["avg_daily_turnover"] == 500.0
    assert payload["rows"][0]["coverage_pct"] == 0.75
    assert payload["tasks"][0]["fetch_start"] == "2026-01-05"
    assert payload["tasks"][0]["fetch_end"] == "2026-01-05"


def test_data_bulk_fetch_preview_rejects_non_yfinance() -> None:
    with TestClient(app) as client:
        response = client.get(
            "/api/data/bulk-fetch-preview"
            "?provider=upstox&exchange=NSE"
            "&start_date=2026-01-01&end_date=2026-01-06"
        )

    assert response.status_code == 400
    assert response.json()["detail"] == (
        "Only provider=yfinance is supported for bulk fetch preview."
    )


def test_data_availability_requires_date_pair() -> None:
    with TestClient(app) as client:
        response = client.get("/api/data/availability?start_date=2026-01-01")

    assert response.status_code == 400
    assert response.json()["detail"] == "start_date and end_date must be supplied together."


def test_data_instruments_search_endpoint(monkeypatch) -> None:
    monkeypatch.setattr("trade_research.api.app._store", lambda: FakeCoverageStore())

    with TestClient(app) as client:
        response = client.get("/api/data/instruments/search?query=rel&limit=5")

    assert response.status_code == 200
    payload = response.json()
    assert payload == [
        {
            "symbol": "RELIANCE",
            "name": "Reliance Industries Limited",
            "instrument_key": "NSE_EQ|INE002A01018",
            "provider": "upstox",
            "exchange": "NSE",
            "isin": "INE002A01018",
            "segment": "NSE_EQ",
            "asset_type": "EQ",
        }
    ]


def test_data_universes_endpoint(monkeypatch) -> None:
    monkeypatch.setattr("trade_research.api.app._store", lambda: FakeCoverageStore())

    with TestClient(app) as client:
        response = client.get("/api/data/universes")

    assert response.status_code == 200
    payload = response.json()
    assert payload[0]["universe_id"] == "nse_liquid_adt_100cr"
    assert payload[0]["member_count"] == 2
    assert payload[0]["criteria"] == {"lookback": "6 months"}


def test_data_universe_members_endpoint(monkeypatch) -> None:
    monkeypatch.setattr("trade_research.api.app._store", lambda: FakeCoverageStore())

    with TestClient(app) as client:
        response = client.get(
            "/api/data/universes/nse_liquid_adt_100cr/members?limit=100"
        )

    assert response.status_code == 200
    payload = response.json()
    assert payload[0]["symbol"] == "AAA"
    assert payload[0]["rank"] == 1
    assert payload[0]["avg_daily_turnover"] == 1_000_000_000.0


def test_data_pipeline_health_endpoint(monkeypatch) -> None:
    monkeypatch.setattr("trade_research.api.app._store", lambda: FakeCoverageStore())
    monkeypatch.setattr(
        "trade_research.api.app.get_settings",
        lambda: SimpleNamespace(
            upstox_access_token="token",
            data_pipeline_max_concurrent_fetches=4,
        ),
    )

    with TestClient(app) as client:
        response = client.get("/api/data/pipeline-health")

    assert response.status_code == 200
    payload = response.json()
    assert payload["provider"] == "upstox"
    assert payload["exchange"] == "NSE"
    assert payload["daily_ohlcv_enabled"] is True
    assert payload["upstox_access_token_configured"] is True
    assert payload["max_concurrent_fetches"] == 4


def test_data_pipeline_runs_endpoint(monkeypatch) -> None:
    monkeypatch.setattr("trade_research.api.app._store", lambda: FakeCoverageStore())

    with TestClient(app) as client:
        response = client.get("/api/data/pipeline-runs")

    assert response.status_code == 200
    payload = response.json()
    assert payload[0]["id"] == "run-1"
    assert payload[0]["name"] == "upstox_nse_daily_ohlcv"
    assert payload[0]["duration_seconds"] == 125
    assert payload[0]["items_requested"] == 2
    assert payload[0]["run_metadata"] == {"trigger": "ui"}


def test_data_pipeline_run_detail_endpoint(monkeypatch) -> None:
    monkeypatch.setattr("trade_research.api.app._store", lambda: FakeCoverageStore())

    with TestClient(app) as client:
        response = client.get("/api/data/pipeline-runs/run-1")

    assert response.status_code == 200
    payload = response.json()
    assert payload["run"]["id"] == "run-1"
    assert payload["fetch_coverage"][0]["instrument_key"] == "NSE_EQ|AAA"
    assert payload["fetch_coverage"][0]["rows_fetched"] == 2


def test_data_pipeline_run_detail_returns_404(monkeypatch) -> None:
    monkeypatch.setattr("trade_research.api.app._store", lambda: FakeCoverageStore())

    with TestClient(app) as client:
        response = client.get("/api/data/pipeline-runs/missing")

    assert response.status_code == 404


def test_create_data_pipeline_request_endpoint(monkeypatch) -> None:
    monkeypatch.setattr("trade_research.api.app._store", lambda: FakeCoverageStore())

    def fake_run_daily_ohlcv_request(*args, **kwargs):
        return SimpleNamespace(run_id="run-1")

    monkeypatch.setattr(
        "trade_research.api.app.run_daily_ohlcv_request",
        fake_run_daily_ohlcv_request,
    )

    with TestClient(app) as client:
        response = client.post(
            "/api/data/pipeline-requests",
            json={
                "provider": "upstox",
                "exchange": "NSE",
                "symbols": ["AAA"],
                "unit": "days",
                "interval": 1,
                "start_date": "2026-01-01",
                "end_date": "2026-01-06",
                "steps": ["fetch_ohlcv", "validate_ohlcv"],
                "mode": "incremental_missing_only",
            },
        )

    assert response.status_code == 200
    payload = response.json()
    assert payload["run"]["id"] == "run-1"
    assert payload["fetch_coverage"][0]["status"] == "fetched"


def test_create_data_pipeline_request_rejects_partial_steps(monkeypatch) -> None:
    monkeypatch.setattr("trade_research.api.app._store", lambda: FakeCoverageStore())

    with TestClient(app) as client:
        response = client.post(
            "/api/data/pipeline-requests",
            json={
                "provider": "upstox",
                "exchange": "NSE",
                "symbols": ["AAA"],
                "unit": "days",
                "interval": 1,
                "start_date": "2026-01-01",
                "end_date": "2026-01-06",
                "steps": ["fetch_ohlcv"],
                "mode": "incremental_missing_only",
            },
        )

    assert response.status_code == 400
    assert "fetch_ohlcv and validate_ohlcv" in response.json()["detail"]


def test_research_progress_endpoint() -> None:
    with TestClient(app) as client:
        response = client.get("/api/research/progress")

    assert response.status_code == 200
    payload = response.json()
    assert "steps" in payload
    assert payload["step_count"] >= 7


def test_research_factor_ic_rejects_bad_direction() -> None:
    with TestClient(app) as client:
        response = client.get("/api/research/factors/ic?direction=sideways")

    assert response.status_code == 400


def test_research_ml_summary_endpoint() -> None:
    with TestClient(app) as client:
        response = client.get("/api/research/ml/summary")

    assert response.status_code == 200
    payload = response.json()
    assert "assumptions" in payload
    assert "model_runs" in payload


def test_research_ml_model_metrics_rejects_bad_run() -> None:
    with TestClient(app) as client:
        response = client.get("/api/research/ml/model-metrics?run=xgboost")

    assert response.status_code == 400


def test_research_ml_latest_candidates_endpoint() -> None:
    with TestClient(app) as client:
        response = client.get("/api/research/ml/latest-candidates?run=baselines&top_n=5")

    assert response.status_code == 200
    payload = response.json()
    assert "models" in payload
    assert payload["run"] == "baselines"


def test_research_ml_robustness_rejects_bad_group() -> None:
    with TestClient(app) as client:
        response = client.get("/api/research/ml/robustness?group=xgboost")

    assert response.status_code == 400
