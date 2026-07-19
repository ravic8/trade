from datetime import UTC, date, datetime

from fastapi.testclient import TestClient

from trade_research.api.app import app

NOW = datetime(2026, 7, 19, 6, 30, tzinfo=UTC)


class FakeOperationsStore:
    def pipeline_work_queue_groups(self, *, provider=None, exchange=None):
        assert provider == "yfinance"
        assert exchange == "TSX"
        return [
            {
                "provider": "yfinance",
                "exchange": "TSX",
                "work_item_exchanges": ["TSX"],
                "work_type": "daily_incremental",
                "status": "retry_wait",
                "items": 2,
                "symbols": 2,
                "maximum_attempts": 3,
                "oldest_created_at": NOW,
                "earliest_next_attempt_at": NOW,
            }
        ]

    def pipeline_work_items_page(self, **filters):
        assert filters == {
            "provider": "yfinance",
            "exchange": "TSX",
            "status": "retry_wait",
            "work_type": "daily_incremental",
            "symbol": "RY",
            "limit": 25,
            "offset": 5,
        }
        return {"total": 1, "rows": [_work_item_row()]}

    def symbol_lifecycle_events_page(self, **filters):
        if "event_type" in filters:
            assert filters == {
                "exchange": "US",
                "event_type": "added",
                "symbol": "AAPL",
                "limit": 10,
                "offset": 0,
            }
        else:
            assert filters == {"exchange": "TSX", "limit": 5}
        return {"total": 1, "rows": [_lifecycle_row()]}

    def adaptive_rate_states(self, *, provider=None):
        assert provider in {None, "yfinance"}
        return [_rate_row()]

    def provider_data_freshness(self, *, provider=None, exchange=None):
        assert provider == "yfinance"
        assert exchange == "TSX"
        return [
            {
                "provider": "yfinance",
                "exchange": "TSX",
                "first_date": date(2016, 7, 18),
                "latest_date": date(2026, 7, 17),
                "rows": 1_200_000,
                "symbols": 644,
                "suspicious_rows": 0,
                "latest_fetched_at": NOW,
            }
        ]

    def latest_accepted_universe_snapshots(self, *, exchange=None):
        assert exchange == "TSX"
        return [
            {
                "snapshot_id": "snapshot-tsx",
                "exchange": "TSX",
                "source": "tmx_symbol_directory",
                "status": "accepted",
                "fetched_at": NOW,
                "symbol_count": 645,
                "validation_json": {"accepted": True},
                "error_message": None,
            }
        ]

    def provider_runs(self, **filters):
        assert filters == {
            "limit": 10,
            "source": "yfinance",
            "exchange": "TSX",
        }
        return [
            {
                "run_id": "run-tsx",
                "job_name": "yfinance_daily_work_queue",
                "status": "completed",
                "exchange": "TSX",
                "source": "yfinance",
                "started_at": NOW,
                "finished_at": NOW,
                "items_requested": 25,
                "items_processed": 25,
                "items_succeeded": 25,
                "items_failed": 0,
                "error_message": None,
                "run_metadata": {"trigger": "dagster"},
            }
        ]


def _work_item_row() -> dict:
    return {
        "work_item_id": "work-1",
        "idempotency_key": "ignored-by-schema",
        "work_type": "daily_incremental",
        "provider": "yfinance",
        "exchange": "TSX",
        "canonical_instrument_id": "eq-ry",
        "provider_symbol": "RY.TO",
        "interval": "1d",
        "window_start": date(2026, 7, 17),
        "window_end": date(2026, 7, 17),
        "priority": 10,
        "status": "retry_wait",
        "attempt_count": 3,
        "max_attempts": 9,
        "next_attempt_at": NOW,
        "locked_by": None,
        "locked_at": None,
        "run_id": "run-tsx",
        "parent_work_item_id": None,
        "last_status_code": 429,
        "last_error_code": "rate_limited",
        "last_error_message": "Yahoo rate limited the request.",
        "created_at": NOW,
        "updated_at": NOW,
        "completed_at": None,
    }


def _lifecycle_row() -> dict:
    return {
        "event_id": "event-1",
        "canonical_instrument_id": "eq-aapl",
        "exchange": "US",
        "symbol": "AAPL",
        "event_type": "added",
        "old_value": None,
        "new_value": {"is_active": True},
        "snapshot_id": "snapshot-us",
        "created_at": NOW,
    }


def _rate_row() -> dict:
    return {
        "provider": "yfinance",
        "current_rpm": 300,
        "last_safe_rpm": 300,
        "minimum_rpm": 30,
        "maximum_rpm": 600,
        "current_concurrency": 4,
        "consecutive_healthy_windows": 5,
        "circuit_state": "closed",
        "cooldown_until": None,
        "last_429_at": None,
        "recent_error_rate": 0.0,
        "latency_baseline_ms": 7500.0,
        "updated_at": NOW,
    }


def test_operations_overview_assembles_console_state(monkeypatch) -> None:
    monkeypatch.setattr("trade_research.api.app._store", lambda: FakeOperationsStore())

    with TestClient(app) as client:
        response = client.get(
            "/api/data/operations/overview"
            "?provider=yfinance&exchange=CA&recent_run_limit=10&lifecycle_limit=5"
        )

    assert response.status_code == 200
    payload = response.json()
    assert payload["exchange"] == "TSX"
    assert payload["queue"][0]["status"] == "retry_wait"
    assert payload["freshness"][0]["symbols"] == 644
    assert payload["adaptive_rates"][0]["current_concurrency"] == 4
    assert payload["latest_universes"][0]["symbol_count"] == 645
    assert payload["recent_runs"][0]["items_succeeded"] == 25
    assert payload["recent_runs"][0]["work_item_exchanges"] == ["TSX"]
    assert payload["recent_lifecycle_events"][0]["event_id"] == "event-1"


def test_operations_work_items_support_filters_and_pagination(monkeypatch) -> None:
    monkeypatch.setattr("trade_research.api.app._store", lambda: FakeOperationsStore())

    with TestClient(app) as client:
        response = client.get(
            "/api/data/operations/work-items"
            "?provider=yfinance&exchange=CA&status=retry_wait"
            "&work_type=daily_incremental&symbol=RY&limit=25&offset=5"
        )

    assert response.status_code == 200
    payload = response.json()
    assert payload["total"] == 1
    assert payload["limit"] == 25
    assert payload["offset"] == 5
    assert payload["rows"][0]["provider_symbol"] == "RY.TO"
    assert payload["rows"][0]["last_error_code"] == "rate_limited"


def test_operations_lifecycle_events_and_rate_limits(monkeypatch) -> None:
    monkeypatch.setattr("trade_research.api.app._store", lambda: FakeOperationsStore())

    with TestClient(app) as client:
        events = client.get(
            "/api/data/operations/lifecycle-events"
            "?exchange=US&event_type=added&symbol=AAPL&limit=10"
        )
        rates = client.get("/api/data/operations/rate-limits?provider=YFINANCE")

    assert events.status_code == 200
    assert events.json()["rows"][0]["new_value"] == {"is_active": True}
    assert rates.status_code == 200
    assert rates.json()[0]["circuit_state"] == "closed"


def test_operations_endpoints_reject_unsupported_exchange() -> None:
    with TestClient(app) as client:
        response = client.get("/api/data/operations/overview?exchange=FOREX")

    assert response.status_code == 400
    assert response.json()["detail"] == "exchange must be NSE, TSX, or US"
