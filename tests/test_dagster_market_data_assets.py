from importlib import import_module

import pytest

dagster = pytest.importorskip("dagster")
market_data_assets = import_module("trade_research.dagster.market_data_assets")


def _comparison_result(status: str = "pass") -> object:
    return market_data_assets.PipelineRunResult(
        name="nse_yfinance_cutover_readiness",
        status=status,
        rows=42,
        metrics={
            "comparison_state": "overlap_available",
            "window_start": "2026-09-01",
            "window_end": "2026-09-08",
            "overlapping_symbols": 25,
            "row_overlap_ratio": 1.0,
            "close_match_ratio": 1.0,
            "evidence_id": "e" * 64,
        },
        blocking_issues=[] if status == "pass" else ["comparison failed"],
    )


def test_provider_comparison_asset_records_evidence_metadata(monkeypatch) -> None:
    captured: dict[str, object] = {}

    def fake_comparison(**kwargs) -> object:
        captured.update(kwargs)
        return _comparison_result()

    monkeypatch.setattr(
        market_data_assets,
        "run_nse_yfinance_cutover_readiness",
        fake_comparison,
    )

    result = market_data_assets.nse_yfinance_provider_comparison(
        dagster.build_op_context()
    )

    assert result.status == "pass"
    assert captured == {
        "trigger": "dagster",
        "at": None,
    }


def test_provider_comparison_asset_fails_dagster_run_on_blocked_window(
    monkeypatch,
) -> None:
    monkeypatch.setattr(
        market_data_assets,
        "run_nse_yfinance_cutover_readiness",
        lambda **_kwargs: _comparison_result(status="fail"),
    )

    with pytest.raises(RuntimeError, match="comparison failed"):
        market_data_assets.nse_yfinance_provider_comparison(dagster.build_op_context())


def test_minute_asset_supports_manual_launch_context(monkeypatch) -> None:
    captured: dict[str, object] = {}

    def fake_minute(**kwargs):
        captured.update(kwargs)
        return market_data_assets.PipelineRunResult(
            name="yfinance_nse_1m_ohlcv",
            status="pass",
            rows=100,
            metrics={
                "eligible_sessions": 5,
                "selected_symbols": 2,
                "raw_rows": 100,
                "validated_rows": 100,
                "clickhouse_rows": 100,
                "failure_rows": 0,
                "run_id": "minute-canary-run",
                "raw_snapshot_uri": "s3://redacted",
            },
        )

    monkeypatch.setattr(
        market_data_assets,
        "run_yfinance_nse_minute_pipeline",
        fake_minute,
    )

    result = market_data_assets.yfinance_nse_minute_ohlcv(
        dagster.build_op_context(op_config={"symbol_limit": 2})
    )

    assert result.status == "pass"
    assert captured["at"] is None
    assert captured["symbol_limit"] == 2


def test_bounded_canary_assessment_uses_production_thresholds(monkeypatch) -> None:
    captured: dict[str, object] = {}
    settings = type(
        "Settings",
        (),
        {
            "database_url": "postgresql://test/test",
            "phase3_canary_max_instruments": 25,
            "phase3_minimum_completeness": 0.995,
            "phase3_required_observed_sessions": 5,
        },
    )()
    store = type("Store", (), {"engine": object(), "initialize": lambda self: None})()

    class Repository:
        def __init__(self, engine) -> None:
            assert engine is store.engine

        def assess_canary(self, **kwargs):
            captured.update(kwargs)
            return {
                "status": "pass",
                "evidence_id": "e" * 64,
                "source_run_ids": {
                    "daily": "daily-run",
                    "minute": "minute-run",
                    "minute_rerun": "minute-rerun",
                },
                "session_dates": ["2026-09-01"],
                "blocking_issues": [],
                "evidence_refs": {"replication_checkpoint_ids": ["checkpoint"]},
            }

    monkeypatch.setattr(market_data_assets, "get_settings", lambda: settings)
    monkeypatch.setattr(market_data_assets, "TimescaleStore", lambda _url: store)
    monkeypatch.setattr(market_data_assets, "Phase3ReadinessRepository", Repository)

    result = market_data_assets.phase3_bounded_canary_assessment(
        dagster.build_op_context(
            op_config={
                "daily_run_id": "daily-run",
                "minute_run_id": "minute-run",
                "minute_rerun_id": "minute-rerun",
            }
        )
    )

    assert result["status"] == "pass"
    assert captured == {
        "daily_run_id": "daily-run",
        "minute_run_id": "minute-run",
        "minute_rerun_id": "minute-rerun",
        "max_instruments": 25,
        "minimum_completeness": 0.995,
        "required_observed_sessions": 5,
    }
