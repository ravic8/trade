from datetime import UTC, date, datetime
from importlib import import_module
from pathlib import Path
from types import SimpleNamespace

import pytest

from trade_data.validation.calendar import LatestTradingDate

dagster = pytest.importorskip("dagster")
daily_assets = import_module("trade_data.dagster.daily_assets")


def _result(
    name: str,
    status: str = "pass",
    artifacts: dict[str, Path] | None = None,
) -> object:
    return daily_assets.PipelineRunResult(
        name=name,
        status=status,
        rows=3,
        artifacts=artifacts or {},
        metrics={"row_count": 3, "nested": {"ignored": True}},
    )


def test_upstox_daily_asset_stores_to_timescale(monkeypatch) -> None:
    captured: dict[str, object] = {}

    def fake_pipeline(**kwargs) -> object:
        captured.update(kwargs)
        return _result("upstox_daily_ohlcv")

    monkeypatch.setattr(daily_assets, "run_upstox_daily_ohlcv_pipeline", fake_pipeline)
    monkeypatch.setattr(
        daily_assets,
        "get_settings",
        lambda: SimpleNamespace(upstox_historical_concurrency=7),
    )
    monkeypatch.setattr(
        daily_assets,
        "resolve_latest_expected_trading_date",
        lambda: LatestTradingDate(
            current_local_time=datetime(2026, 6, 28, tzinfo=UTC),
            latest_expected_trading_date=date(2026, 6, 25),
            reason="test",
            calendar_source="test",
        ),
    )

    result = daily_assets.upstox_daily_ohlcv(dagster.build_op_context())

    assert result.name == "upstox_daily_ohlcv"
    assert captured == {
        "to_date": "2026-06-25",
        "store_db": True,
        "export_db_snapshot": True,
        "trigger": "dagster",
        "max_concurrent_fetches": 7,
    }


def test_yfinance_daily_assets_store_seed_universes(monkeypatch) -> None:
    calls: list[dict[str, object]] = []

    def fake_pipeline(**kwargs) -> object:
        calls.append(kwargs)
        return _result(f"yfinance_{kwargs['universe']}_daily_ohlcv")

    monkeypatch.setattr(daily_assets, "run_yfinance_daily_ohlcv_pipeline", fake_pipeline)

    us_result = daily_assets.yfinance_us_daily_ohlcv(dagster.build_op_context())
    canada_result = daily_assets.yfinance_canada_daily_ohlcv(dagster.build_op_context())

    assert us_result.name == "yfinance_us_seed_daily_ohlcv"
    assert canada_result.name == "yfinance_canada_seed_daily_ohlcv"
    assert calls == [
        {
            "universe": "us_seed",
            "store_db": True,
            "export_db_snapshot": True,
            "trigger": "dagster",
        },
        {
            "universe": "canada_seed",
            "store_db": True,
            "export_db_snapshot": True,
            "trigger": "dagster",
        },
    ]


def test_equity_universe_assets_refresh_canonical_exchanges(monkeypatch) -> None:
    calls: list[tuple[str, str]] = []

    def fake_pipeline(exchange: str, *, trigger: str) -> object:
        calls.append((exchange, trigger))
        return _result(f"{exchange.lower()}_universe_snapshot")

    monkeypatch.setattr(
        daily_assets,
        "run_equity_universe_snapshot_pipeline",
        fake_pipeline,
    )

    daily_assets.nse_universe_snapshot(dagster.build_op_context())
    daily_assets.tsx_universe_snapshot(dagster.build_op_context())
    daily_assets.us_universe_snapshot(dagster.build_op_context())

    assert calls == [
        ("NSE", "dagster"),
        ("TSX", "dagster"),
        ("US", "dagster"),
    ]


def test_equity_universe_asset_fails_on_rejected_snapshot(monkeypatch) -> None:
    monkeypatch.setattr(
        daily_assets,
        "run_equity_universe_snapshot_pipeline",
        lambda *_args, **_kwargs: _result("nse_universe_snapshot", status="fail"),
    )

    with pytest.raises(RuntimeError, match="Pipeline failed: nse_universe_snapshot"):
        daily_assets.nse_universe_snapshot(dagster.build_op_context())


def test_exchange_session_assets_materialize_canonical_exchanges(monkeypatch) -> None:
    calls: list[tuple[str, str]] = []

    def fake_pipeline(exchange: str, *, trigger: str) -> object:
        calls.append((exchange, trigger))
        return _result(f"{exchange.lower()}_exchange_sessions")

    monkeypatch.setattr(
        daily_assets,
        "run_exchange_session_materialization_pipeline",
        fake_pipeline,
    )

    daily_assets.nse_exchange_sessions(dagster.build_op_context())
    daily_assets.tsx_exchange_sessions(dagster.build_op_context())
    daily_assets.us_exchange_sessions(dagster.build_op_context())

    assert calls == [
        ("NSE", "dagster"),
        ("TSX", "dagster"),
        ("US", "dagster"),
    ]


def test_phase5_queue_assets_call_durable_pipelines(monkeypatch) -> None:
    calls: list[tuple[str, object]] = []

    def fake_planner(**kwargs) -> object:
        calls.append(("planner", kwargs))
        return _result("yfinance_daily_work_planner")

    def fake_worker(*, trigger: str) -> object:
        calls.append(("worker", trigger))
        return _result("yfinance_daily_work_queue")

    monkeypatch.setattr(daily_assets, "run_yfinance_daily_work_planner", fake_planner)
    monkeypatch.setattr(daily_assets, "run_yfinance_daily_work_queue", fake_worker)

    daily_assets.yfinance_daily_work_plan(dagster.build_op_context())
    daily_assets.yfinance_daily_work_worker(dagster.build_op_context())

    assert calls == [
        (
            "planner",
            {"include_initial_backfill": False, "trigger": "dagster"},
        ),
        ("worker", "dagster"),
    ]


def test_scheduled_worker_restricts_claims_to_configured_exchange(monkeypatch) -> None:
    calls = []
    monkeypatch.setattr(
        daily_assets, "get_settings",
        lambda: SimpleNamespace(yfinance_daily_worker_exchange="NSE"),
    )
    monkeypatch.setattr(
        daily_assets, "run_yfinance_daily_work_queue",
        lambda **kwargs: calls.append(kwargs) or _result("worker"),
    )
    daily_assets.yfinance_daily_work_worker(dagster.build_op_context())
    assert calls == [{"trigger": "dagster", "exchange": "NSE"}]


def test_phase3_daily_canary_plans_and_claims_only_bounded_nse_work(
    monkeypatch,
) -> None:
    calls: list[tuple[str, object]] = []

    def fake_plan(**kwargs):
        calls.append(("plan", kwargs))
        return daily_assets.PipelineRunResult(
            name="planner",
            status="pass",
            rows=2,
            metrics={"work_generated": 2, "work_inserted": 2},
        )

    def fake_worker(**kwargs):
        calls.append(("worker", kwargs))
        return daily_assets.PipelineRunResult(
            name="worker",
            status="pass",
            rows=40,
            metrics={"run_id": "daily-canary-run"},
        )

    monkeypatch.setattr(daily_assets, "run_yfinance_nse_canary_planner", fake_plan)
    monkeypatch.setattr(daily_assets, "run_yfinance_daily_work_queue", fake_worker)

    context = dagster.build_op_context(
        op_config={"symbol_limit": 2, "symbols": "RELIANCE,TCS"}
    )
    result = daily_assets.yfinance_nse_daily_canary(context)

    assert result.metrics["run_id"] == "daily-canary-run"
    assert calls == [
        (
            "plan",
            {
                "symbol_limit": 2,
                "provider_symbols": ["RELIANCE", "TCS"],
                "enqueue": True,
                "force_refresh": True,
                "session_count": 20,
                "run_key": context.run_id,
                "trigger": "dagster",
            },
        ),
        (
            "worker",
            {
                "claim_size": 2,
                "exchange": "NSE",
                "work_type": "bounded_canary",
                "trigger": "dagster",
            },
        ),
    ]


def test_yfinance_worker_asset_fails_dagster_run_on_partial_business_outcome(
    monkeypatch,
) -> None:
    monkeypatch.setattr(
        daily_assets,
        "run_yfinance_daily_work_queue",
        lambda **_kwargs: daily_assets.PipelineRunResult(
            name="yfinance_daily_work_queue",
            status="warn",
            warnings=["1 work item is waiting for durable retry."],
        ),
    )

    with pytest.raises(RuntimeError, match="durable retry"):
        daily_assets.yfinance_daily_work_worker(dagster.build_op_context())


def test_result_metadata_exposes_canonical_business_outcome() -> None:
    metadata = daily_assets._result_metadata(
        daily_assets.PipelineRunResult(name="pipeline", status="warn")
    )

    assert metadata["business_outcome"] == "degraded"


def test_nse_completed_session_asset_uses_scoped_planner(
    monkeypatch,
) -> None:
    calls: list[tuple[str, object]] = []

    def fake_planner(**kwargs):
        calls.append(("planner", kwargs))
        return _result("yfinance_daily_work_planner")

    monkeypatch.setattr(daily_assets, "run_yfinance_daily_work_planner", fake_planner)

    daily_assets.yfinance_nse_completed_session_work_plan(dagster.build_op_context())

    assert calls == [
        (
            "planner",
            {
                "exchanges": ("NSE",),
                "include_initial_backfill": False,
                "include_gap_repair": True,
                "gap_repair_session_count": 20,
                "trigger": "dagster",
            },
        ),
    ]


def test_north_america_completed_session_assets_use_scoped_planners(
    monkeypatch,
) -> None:
    calls: list[tuple[str, object]] = []

    def fake_planner(*, exchanges, include_initial_backfill, trigger):
        calls.append(("planner", (exchanges, include_initial_backfill, trigger)))
        return _result("yfinance_daily_work_planner")

    monkeypatch.setattr(daily_assets, "run_yfinance_daily_work_planner", fake_planner)
    context = dagster.build_op_context()

    daily_assets.yfinance_tsx_completed_session_work_plan(context)
    daily_assets.yfinance_us_completed_session_work_plan(context)

    assert calls == [
        ("planner", (("TSX",), False, "dagster")),
        ("planner", (("US",), False, "dagster")),
    ]


@pytest.mark.parametrize(
    ("asset_name", "exchange"),
    [
        ("nse_exchange_sessions", "NSE"),
        ("tsx_exchange_sessions", "TSX"),
        ("us_exchange_sessions", "US"),
    ],
)
def test_exchange_session_assets_raise_when_materialization_fails(
    monkeypatch,
    asset_name: str,
    exchange: str,
) -> None:
    monkeypatch.setattr(
        daily_assets,
        "run_exchange_session_materialization_pipeline",
        lambda actual_exchange, *, trigger: _result(
            f"{actual_exchange.lower()}_exchange_sessions",
            status="fail",
        ),
    )

    with pytest.raises(RuntimeError, match=f"Pipeline failed: {exchange.lower()}"):
        getattr(daily_assets, asset_name)(dagster.build_op_context())
