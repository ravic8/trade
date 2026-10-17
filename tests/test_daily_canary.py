from datetime import UTC, date, datetime
from typing import Any

import pytest
from typer.testing import CliRunner

from trade_data import cli
from trade_data.operations.daily_canary import daily_canary_run_report, daily_canary_snapshot
from trade_data.pipelines.base import PipelineRunResult


class CanaryStore:
    def __init__(self) -> None:
        self.coverage: dict[str, set[date]] = {}
        self.instruments: list[dict[str, Any]] = [
            {
                "provider_symbol": "ABC.NS",
                "provider_instrument_key": "YF|ABC.NS",
                "listing_status": "active",
                "listing_status_effective_at": None,
            }
        ]
        self.coverage_query: tuple[Any, ...] | None = None

    def latest_provider_eligible_exchange_session(
        self, exchange: str, *, provider_grace_minutes: int
    ) -> dict[str, date]:
        assert exchange == "NSE"
        assert provider_grace_minutes >= 0
        return {"session_date": date(2026, 10, 1)}

    def active_yfinance_daily_instruments(self, exchange: str) -> list[dict[str, Any]]:
        assert exchange == "NSE"
        return self.instruments

    def exchange_sessions(
        self, exchange: str, start_date: date, end_date: date
    ) -> list[dict[str, Any]]:
        assert (exchange, start_date, end_date) == (
            "NSE", date(2026, 9, 29), date(2026, 10, 1)
        )
        return [
            {"session_date": date(2026, 9, 29), "is_trading_day": True,
             "validation_status": "valid"},
            {"session_date": date(2026, 9, 30), "is_trading_day": False,
             "validation_status": "valid"},
            {"session_date": date(2026, 10, 1), "is_trading_day": True,
             "validation_status": "valid"},
        ]

    def daily_ohlcv_dates_by_instrument(
        self,
        instrument_keys: list[str],
        start_date: date,
        end_date: date,
        source: str,
        exchange: str,
        *,
        valid_only: bool,
    ) -> dict[str, set[date]]:
        self.coverage_query = (
            instrument_keys, start_date, end_date, source, exchange, valid_only
        )
        return self.coverage


def _snapshot(store: CanaryStore) -> dict[str, Any]:
    return daily_canary_snapshot(
        store,
        exchange="nse",
        provider_symbols=["abc.ns"],
        start_date=date(2026, 9, 29),
        end_date=date(2026, 10, 1),
    )


def test_snapshot_reports_only_valid_source_coverage() -> None:
    store = CanaryStore()
    store.coverage = {"YF|ABC.NS": {date(2026, 9, 29)}}

    report = _snapshot(store)

    assert report["complete"] is False
    assert report["validated_sessions"] == ["2026-09-29", "2026-10-01"]
    assert report["instruments"][0]["missing_sessions"] == ["2026-10-01"]
    assert store.coverage_query == (
        ["YF|ABC.NS"], date(2026, 9, 29), date(2026, 10, 1),
        "yfinance", "NSE", True,
    )


def test_snapshot_complete_when_each_eligible_session_is_stored() -> None:
    store = CanaryStore()
    store.coverage = {"YF|ABC.NS": {date(2026, 9, 29), date(2026, 10, 1)}}

    report = _snapshot(store)

    assert report["complete"] is True
    assert report["instruments"][0]["valid_session_count"] == 2


def test_snapshot_respects_listing_effective_date() -> None:
    store = CanaryStore()
    store.instruments[0]["listing_status_effective_at"] = datetime(
        2026, 10, 1, tzinfo=UTC
    )

    report = _snapshot(store)

    assert report["instruments"][0]["eligible_sessions"] == ["2026-10-01"]
    assert report["instruments"][0]["missing_sessions"] == ["2026-10-01"]


@pytest.mark.parametrize(
    ("symbols", "start", "end", "error"),
    [
        (["ABC.NS", "ABC.NS"], date(2026, 9, 29), date(2026, 10, 1), "unique"),
        (["UNKNOWN.NS"], date(2026, 9, 29), date(2026, 10, 1), "absent"),
        (["ABC.NS"], date(2026, 10, 2), date(2026, 10, 2), "latest eligible"),
        (["ABC.NS"], date(2026, 8, 1), date(2026, 10, 1), "31 calendar days"),
    ],
)
def test_snapshot_rejects_unsafe_or_unavailable_scope(
    symbols: list[str], start: date, end: date, error: str
) -> None:
    with pytest.raises(ValueError, match=error):
        daily_canary_snapshot(
            CanaryStore(), exchange="NSE", provider_symbols=symbols,
            start_date=start, end_date=end,
        )


def test_cli_snapshot_uses_read_only_store(monkeypatch: pytest.MonkeyPatch) -> None:
    store = CanaryStore()
    monkeypatch.setattr(cli, "TimescaleStore", lambda _database_url: store)

    result = CliRunner().invoke(
        cli.app,
        [
            "daily-canary-snapshot", "NSE", "--symbols", "ABC.NS",
            "--from-date", "2026-09-29", "--to-date", "2026-10-01",
        ],
    )

    assert result.exit_code == 0
    assert '"missing_sessions"' in result.stdout
    assert '"2026-10-01"' in result.stdout
    assert '"provider_grace_minutes": 120' in result.stdout


def test_nse_canary_cli_plans_only_recent_bounded_sessions(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    captured: dict[str, Any] = {}

    def fake_planner(**kwargs: Any) -> PipelineRunResult:
        captured.update(kwargs)
        return PipelineRunResult(
            name="nse_canary",
            status="pass",
            metrics={
                "exchanges": {
                    "NSE": {
                        "active_symbols": 1,
                        "eligible_symbols_before_limit": 1,
                        "selected_provider_symbols": ["ABC.NS"],
                        "window_start": "2026-09-29",
                        "window_end": "2026-10-01",
                        "bounded_canary_window_start": "2026-09-29",
                        "bounded_canary_window_end": "2026-10-01",
                    }
                },
                "work_generated": 1,
                "work_inserted": 0,
                "queue": {},
            },
        )

    monkeypatch.setattr(cli, "run_yfinance_nse_canary_planner", fake_planner)
    result = CliRunner().invoke(
        cli.app,
        ["plan-yfinance-nse-canary", "--symbols", "ABC.NS", "--session-count", "3"],
    )

    assert result.exit_code == 0
    assert captured["force_refresh"] is True
    assert captured["session_count"] == 3
    assert captured["enqueue"] is False
    assert "Canary window: 2026-09-29 to 2026-10-01" in result.stdout


def test_run_report_combines_scoped_work_requests_and_coverage() -> None:
    class RunStore(CanaryStore):
        def ingestion_run(self, _run_id: str) -> dict[str, Any]:
            return {
                "job_name": "yfinance_daily_work_queue",
                "exchange": "NSE",
                "source": "yfinance",
                "status": "completed",
                "items_requested": 1,
                "items_succeeded": 1,
                "items_failed": 0,
                "run_metadata": {
                    "work_type_filter": "bounded_canary",
                    "claimed_work_item_ids": ["item-1"],
                },
            }

        def pipeline_work_items_for_run(
            self, _run_id: str, *, claimed_work_item_ids: list[str], exchange: str
        ) -> list[dict[str, Any]]:
            assert claimed_work_item_ids == ["item-1"]
            assert exchange == "NSE"
            return [{
                "work_item_id": "item-1",
                "provider": "yfinance",
                "provider_symbol": "ABC.NS",
                "work_type": "bounded_canary",
                "window_start": date(2026, 9, 29),
                "window_end": date(2026, 10, 1),
                "status": "succeeded",
                "attempt_count": 1,
            }]

        def provider_request_log_summary(
            self, _run_id: str, provider: str
        ) -> list[dict[str, Any]]:
            assert provider == "yfinance"
            return [{
                "endpoint_group": "download", "status": "success", "requests": 1,
                "rate_limited_requests": 0,
            }]

    store = RunStore()
    store.coverage = {"YF|ABC.NS": {date(2026, 9, 29), date(2026, 10, 1)}}
    report = daily_canary_run_report(store, run_id="run-1", provider_grace_minutes=120)

    assert report["complete"] is True
    assert report["provider_requests"][0]["requests"] == 1
    assert report["coverage"]["complete"] is True

    store.provider_request_log_summary = lambda *_args, **_kwargs: []  # type: ignore[method-assign]
    assert daily_canary_run_report(
        store, run_id="run-1", provider_grace_minutes=120
    )["complete"] is False


def test_run_report_rejects_unscoped_worker_run() -> None:
    class RunStore(CanaryStore):
        def ingestion_run(self, _run_id: str) -> dict[str, Any]:
            return {
                "job_name": "yfinance_daily_work_queue",
                "exchange": "NSE",
                "source": "yfinance",
                "run_metadata": {"work_type_filter": None},
            }

    with pytest.raises(ValueError, match="bounded NSE canary"):
        daily_canary_run_report(RunStore(), run_id="run-1", provider_grace_minutes=120)
