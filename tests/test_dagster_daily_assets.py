from datetime import UTC, date, datetime
from importlib import import_module
from types import SimpleNamespace

import pytest

from trade_research.validation.daily_pipeline import LatestTradingDate

dagster = pytest.importorskip("dagster")
daily_assets = import_module("trade_research.dagster.daily_assets")


def _result(name: str, status: str = "pass") -> object:
    return daily_assets.PipelineRunResult(
        name=name,
        status=status,
        rows=3,
        metrics={"row_count": 3, "nested": {"ignored": True}},
    )


def test_daily_feature_asset_calls_pipeline_after_validation(monkeypatch) -> None:
    captured: dict[str, object] = {}

    def fake_pipeline(**kwargs) -> object:
        captured.update(kwargs)
        return _result("daily_features")

    monkeypatch.setattr(daily_assets, "run_daily_feature_pipeline", fake_pipeline)

    result = daily_assets.daily_features_v1(
        dagster.build_op_context(),
        daily_ohlcv=_result("upstox_daily_ohlcv"),
    )

    assert result.name == "daily_features"
    assert captured == {
        "input_source": "timescale",
        "store_db": True,
        "incremental": True,
        "lookback_days": 320,
        "export_db_snapshot": True,
    }


def test_daily_feature_asset_stops_on_failed_validation(monkeypatch) -> None:
    monkeypatch.setattr(
        daily_assets,
        "run_daily_feature_pipeline",
        lambda **kwargs: pytest.fail("feature pipeline should not run"),
    )

    with pytest.raises(RuntimeError, match="Upstream pipeline failed"):
        daily_assets.daily_features_v1(
            dagster.build_op_context(),
            daily_ohlcv=_result("upstox_daily_ohlcv", status="fail"),
        )


def test_daily_target_asset_uses_timescale(monkeypatch) -> None:
    captured: dict[str, object] = {}

    def fake_pipeline(**kwargs) -> object:
        captured.update(kwargs)
        return _result("daily_targets")

    monkeypatch.setattr(daily_assets, "run_daily_target_pipeline", fake_pipeline)

    result = daily_assets.daily_targets_v1(
        dagster.build_op_context(),
        daily_ohlcv=_result("upstox_daily_ohlcv"),
    )

    assert result.name == "daily_targets"
    assert captured == {
        "input_source": "timescale",
        "store_db": True,
        "incremental": True,
        "recompute_lookback_days": 90,
        "export_db_snapshot": True,
    }


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


def test_dukascopy_intraday_asset_stores_to_timescale(monkeypatch) -> None:
    captured: dict[str, object] = {}

    def fake_pipeline(**kwargs) -> object:
        captured.update(kwargs)
        return _result("dukascopy_fx_crypto_5m_ohlcv")

    monkeypatch.setattr(
        daily_assets,
        "run_dukascopy_intraday_ohlcv_pipeline",
        fake_pipeline,
    )

    result = daily_assets.dukascopy_fx_intraday_ohlcv(dagster.build_op_context())

    assert result.name == "dukascopy_fx_crypto_5m_ohlcv"
    assert captured == {
        "store_db": True,
        "trigger": "dagster",
    }


def test_fx_intraday_gap_validation_uses_fetch_artifact(monkeypatch, tmp_path) -> None:
    captured: dict[str, object] = {}
    artifact = tmp_path / "dukas.parquet"
    artifact.write_text("placeholder", encoding="utf-8")

    def fake_pipeline(**kwargs) -> object:
        captured.update(kwargs)
        return _result("dukascopy_fx_crypto_5m_gap_validation")

    monkeypatch.setattr(
        daily_assets,
        "run_dukascopy_intraday_gap_validation_pipeline",
        fake_pipeline,
    )

    result = daily_assets.fx_intraday_gap_validation(
        dagster.build_op_context(),
        intraday_ohlcv=daily_assets.PipelineRunResult(
            name="dukascopy_fx_crypto_5m_ohlcv",
            status="pass",
            artifacts={"ohlcv": artifact},
        ),
    )

    assert result.name == "dukascopy_fx_crypto_5m_gap_validation"
    assert captured == {"input_path": artifact}


def test_fx_intraday_gap_validation_fails_without_artifact() -> None:
    result = daily_assets.fx_intraday_gap_validation(
        dagster.build_op_context(),
        intraday_ohlcv=daily_assets.PipelineRunResult(
            name="dukascopy_fx_crypto_5m_ohlcv",
            status="pass",
        ),
    )

    assert result.status == "fail"
    assert result.blocking_issues == [
        "Dukascopy fetch produced no OHLCV artifact to validate."
    ]


def test_daily_pipeline_health_skips_factor_research_rebuild(monkeypatch) -> None:
    captured: dict[str, object] = {}

    def fake_pipeline(**kwargs) -> object:
        captured.update(kwargs)
        return _result("daily_pipeline_health", status="warn")

    monkeypatch.setattr(daily_assets, "run_daily_pipeline_health_pipeline", fake_pipeline)

    context = dagster.build_op_context()
    result = daily_assets.daily_pipeline_health(
        context,
        processed_validation=_result("processed_dataset_validation"),
        ml_dataset=_result("ml_dataset_v1"),
        daily_features=_result("daily_features"),
        daily_targets=_result("daily_targets"),
        factor_research=_result("factor_research"),
    )

    assert result.status == "warn"
    assert captured == {
        "run_factor_research": False,
        "rebuild_artifacts": False,
        "coverage_run_id": context.run_id,
        "store_coverage_db": True,
        "coverage_windows_months": [6, 9, 12, 15, 18, 24],
    }


def test_ml_dataset_asset_depends_on_processed_validation(monkeypatch) -> None:
    called = False

    def fake_pipeline() -> object:
        nonlocal called
        called = True
        return _result("ml_dataset_v1")

    monkeypatch.setattr(daily_assets, "run_ml_dataset_v1_pipeline", fake_pipeline)

    result = daily_assets.ml_dataset_v1(
        dagster.build_op_context(),
        processed_validation=_result("processed_dataset_validation"),
        daily_features=_result("daily_features"),
        daily_targets=_result("daily_targets"),
    )

    assert called is True
    assert result.name == "ml_dataset_v1"


def test_ml_dataset_asset_stops_on_failed_processed_validation(monkeypatch) -> None:
    monkeypatch.setattr(
        daily_assets,
        "run_ml_dataset_v1_pipeline",
        lambda: pytest.fail("ml dataset pipeline should not run"),
    )

    with pytest.raises(RuntimeError, match="Upstream pipeline failed"):
        daily_assets.ml_dataset_v1(
            dagster.build_op_context(),
            processed_validation=_result("processed_dataset_validation", status="fail"),
            daily_features=_result("daily_features"),
            daily_targets=_result("daily_targets"),
        )
