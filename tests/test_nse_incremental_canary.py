from datetime import UTC, date, datetime
from typing import Any

import pytest
from sqlalchemy.dialects import postgresql
from typer.testing import CliRunner

from trade_data import cli
from trade_data.config import Settings
from trade_data.pipelines import yfinance_work_queue
from trade_data.pipelines.base import PipelineRunResult
from trade_data.pipelines.daily_runtime import YFinanceDailyRuntime
from trade_data.storage.timescale import TimescaleStore

NOW = datetime(2026, 10, 5, 12, tzinfo=UTC)
SYMBOLS = ["RELIANCE.NS", "COALINDIA.NS", "ONGC.NS"]


def test_incremental_planner_requires_explicit_small_nse_scope(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    captured: dict[str, Any] = {}

    def fake_planner(**kwargs: Any) -> PipelineRunResult:
        captured.update(kwargs)
        return PipelineRunResult(name="planner", status="pass", metrics={})

    monkeypatch.setattr(yfinance_work_queue, "get_settings", lambda: Settings(_env_file=None))
    monkeypatch.setattr(yfinance_work_queue, "run_yfinance_daily_work_planner", fake_planner)

    yfinance_work_queue.run_yfinance_nse_incremental_canary_planner(
        provider_symbols=["reliance", "coalindia", "ongc"], at=NOW
    )

    assert captured["exchanges"] == ("NSE",)
    assert captured["provider_symbols"] == tuple(SYMBOLS)
    assert captured["include_incremental"] is True
    assert captured["include_initial_backfill"] is False
    assert captured["include_gap_repair"] is False
    assert captured["enqueue"] is False
    assert captured["instrument_limit_per_exchange"] == 3
    assert captured["maximum_incremental_window_days"] == 31
    assert captured["require_existing_daily_coverage"] is True

    with pytest.raises(ValueError, match="one to five"):
        yfinance_work_queue.run_yfinance_nse_incremental_canary_planner(
            provider_symbols=[]
        )
    with pytest.raises(ValueError, match="Duplicate"):
        yfinance_work_queue.run_yfinance_nse_incremental_canary_planner(
            provider_symbols=["reliance", "RELIANCE.NS"]
        )
    with pytest.raises(ValueError, match="requires YFINANCE_NSE_CANARY_ENABLED"):
        yfinance_work_queue.run_yfinance_nse_incremental_canary_planner(
            provider_symbols=SYMBOLS, enqueue=True
        )


def test_incremental_worker_requires_both_flags_and_forwards_scope(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    captured: dict[str, Any] = {}

    def fake_worker(**kwargs: Any) -> PipelineRunResult:
        captured.update(kwargs)
        return PipelineRunResult(name="worker", status="pass", metrics={})

    monkeypatch.setattr(yfinance_work_queue, "run_yfinance_daily_work_queue", fake_worker)
    monkeypatch.setattr(yfinance_work_queue, "get_settings", lambda: Settings(_env_file=None))
    with pytest.raises(ValueError, match="requires YFINANCE_NSE_CANARY_ENABLED"):
        yfinance_work_queue.run_yfinance_nse_incremental_canary_worker(
            provider_symbols=SYMBOLS
        )
    assert not captured

    monkeypatch.setattr(
        yfinance_work_queue,
        "get_settings",
        lambda: Settings(
            _env_file=None, yfinance_nse_canary_enabled=True, yfinance_daily_enabled=True
        ),
    )
    yfinance_work_queue.run_yfinance_nse_incremental_canary_worker(
        provider_symbols=SYMBOLS, at=NOW
    )
    assert captured["exchange"] == "NSE"
    assert captured["work_type"] == "daily_incremental"
    assert captured["provider_symbols"] == tuple(SYMBOLS)
    assert captured["claim_size"] == 3


@pytest.mark.parametrize("selected", [SYMBOLS, None])
def test_generic_worker_scopes_claim_and_recovery_without_provider_calls(
    monkeypatch: pytest.MonkeyPatch,
    selected: list[str] | None,
) -> None:
    calls: dict[str, dict[str, Any]] = {}

    class Store:
        def initialize(self) -> None:
            pass

        def recover_stale_pipeline_work_items(self, **kwargs: Any) -> int:
            calls["recover"] = kwargs
            return 0

        def claim_pipeline_work_items(self, **kwargs: Any) -> list[dict[str, Any]]:
            calls["claim"] = kwargs
            return []

        def pipeline_work_queue_summary(self) -> dict[str, int]:
            return {}

    def unexpected_provider() -> None:
        pytest.fail("No claim must mean no provider call")

    monkeypatch.setattr(
        yfinance_work_queue,
        "get_settings",
        lambda: Settings(_env_file=None, yfinance_daily_enabled=True),
    )
    runtime = YFinanceDailyRuntime(lambda _url: Store(), unexpected_provider)
    result = yfinance_work_queue.run_yfinance_daily_work_queue(
        exchange="NSE", work_type="daily_incremental", provider_symbols=selected,
        claim_size=3, at=NOW, runtime=runtime,
    )

    assert result.metrics["claimed"] == 0
    if selected is not None:
        assert calls["recover"]["provider_symbols"] == tuple(selected)
        assert calls["claim"]["provider_symbols"] == tuple(selected)
    else:
        assert "provider_symbols" not in calls["recover"]
        assert "provider_symbols" not in calls["claim"]
    assert calls["recover"]["exchange"] == "NSE"
    assert calls["recover"]["work_type"] == "daily_incremental"
    assert calls["claim"]["limit"] == 3


def test_incremental_cli_is_dry_run_by_default(monkeypatch: pytest.MonkeyPatch) -> None:
    captured: dict[str, Any] = {}

    def fake_planner(**kwargs: Any) -> PipelineRunResult:
        captured.update(kwargs)
        return PipelineRunResult(
            name="planner",
            status="pass",
            metrics={
                "exchanges": {
                    "NSE": {
                        "selected_provider_symbols": SYMBOLS,
                        "window_end": date(2026, 10, 5).isoformat(),
                        "planned_windows": [],
                    }
                },
                "work_generated": 0,
                "work_inserted": 0,
                "queue": {},
            },
        )

    monkeypatch.setattr(cli, "run_yfinance_nse_incremental_canary_planner", fake_planner)
    result = CliRunner().invoke(
        cli.app,
        ["plan-yfinance-nse-incremental-canary", "--symbols", ",".join(SYMBOLS)],
    )

    assert result.exit_code == 0
    assert captured["enqueue"] is False
    assert '"planned_windows": []' in result.stdout


def test_store_claim_and_recovery_queries_restrict_all_scope_fields() -> None:
    statements: list[Any] = []

    class Result:
        rowcount = 0

        def mappings(self) -> "Result":
            return self

        def __iter__(self):
            return iter(())

    class Connection:
        def __enter__(self) -> "Connection":
            return self

        def __exit__(self, *_args: Any) -> None:
            pass

        def execute(self, statement: Any) -> Result:
            statements.append(statement)
            return Result()

    class Engine:
        def begin(self) -> Connection:
            return Connection()

    store = object.__new__(TimescaleStore)
    store.engine = Engine()  # type: ignore[assignment]
    store.claim_pipeline_work_items(
        worker_id="nse-canary", limit=3, at=NOW, exchange="NSE",
        work_type="daily_incremental", provider_symbols=SYMBOLS,
    )
    store.recover_stale_pipeline_work_items(
        stale_before=NOW, at=NOW, exchange="NSE",
        work_type="daily_incremental", provider_symbols=SYMBOLS,
    )

    assert len(statements) == 2
    for statement in statements:
        sql = str(statement.compile(dialect=postgresql.dialect(), compile_kwargs={
            "literal_binds": True,
        }))
        assert "exchange = 'NSE'" in sql
        assert "work_type = 'daily_incremental'" in sql
        for symbol in SYMBOLS:
            assert f"'{symbol}'" in sql


def test_selected_symbol_plan_does_not_cancel_exchange_wide_work(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    sessions = [date(2026, 10, 1), date(2026, 10, 5)]
    latest_date: date | None = sessions[0]
    enqueued: list[Any] = []

    class Store:
        def initialize(self) -> None:
            pass

        def latest_provider_eligible_exchange_session(self, *_args: Any, **_kwargs: Any):
            return {"session_date": sessions[-1]}

        def exchange_sessions(self, *_args: Any, **_kwargs: Any):
            return [
                {"session_date": session, "is_trading_day": True,
                 "validation_status": "valid"}
                for session in sessions
            ]

        def cancel_pipeline_work_items_before_listing(self, **_kwargs: Any) -> None:
            pytest.fail("Selected-symbol planner must not cancel exchange-wide work")

        def cancel_pipeline_work_items_covered_by_provider_history(
            self, **_kwargs: Any
        ) -> None:
            pytest.fail("Selected-symbol planner must not cancel exchange-wide work")

        def active_yfinance_daily_instruments(self, _exchange: str):
            return [{
                "canonical_instrument_id": "eq_reliance",
                "provider_symbol": "RELIANCE.NS",
                "provider_instrument_key": "YF|RELIANCE.NS",
                "listing_status": "active",
                "pipeline_eligibility": "incremental",
            }]

        def latest_daily_ohlcv_dates(self, *_args: Any, **_kwargs: Any):
            return {"YF|RELIANCE.NS": latest_date} if latest_date else {}

        def enqueue_pipeline_work_items(self, work: list[Any]) -> int:
            enqueued.extend(work)
            return len(work)

        def pipeline_work_queue_summary(self) -> dict[str, int]:
            return {"queued": len(enqueued)}

    monkeypatch.setattr(
        yfinance_work_queue, "get_settings",
        lambda: Settings(_env_file=None, yfinance_nse_canary_enabled=True),
    )
    monkeypatch.setattr(yfinance_work_queue, "TimescaleStore", lambda _url: Store())
    result = yfinance_work_queue.run_yfinance_nse_incremental_canary_planner(
        provider_symbols=["RELIANCE.NS"], enqueue=True, at=NOW
    )

    assert result.metrics["work_inserted"] == 1
    assert result.metrics["exchanges"]["NSE"]["planned_windows"][0]["window_end"] == (
        "2026-10-05"
    )

    latest_date = None
    with pytest.raises(ValueError, match="requires valid seeded daily coverage"):
        yfinance_work_queue.run_yfinance_nse_incremental_canary_planner(
            provider_symbols=["RELIANCE.NS"], enqueue=True, at=NOW
        )
    assert len(enqueued) == 1

    sessions[:] = [date(2026, 9, 1), date(2026, 10, 5)]
    latest_date = sessions[0]
    with pytest.raises(ValueError, match="exceeds the bounded calendar window"):
        yfinance_work_queue.run_yfinance_nse_incremental_canary_planner(
            provider_symbols=["RELIANCE.NS"], enqueue=True, at=NOW
        )
    assert len(enqueued) == 1
