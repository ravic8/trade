from __future__ import annotations

from datetime import UTC, datetime
from pathlib import Path

import pandas as pd
import pytest

from trade_research.market_calendar import ExchangeHolidays
from trade_research.validation.daily_pipeline import (
    resolve_latest_expected_trading_date,
    validate_daily_pipeline_health,
)


def test_latest_expected_trading_date_resolution_handles_weekend() -> None:
    resolved = resolve_latest_expected_trading_date(
        at=datetime(2026, 6, 28, 6, 0, tzinfo=UTC),
        holidays=ExchangeHolidays(frozenset(), frozenset(), "test"),
    )

    assert resolved.latest_expected_trading_date.isoformat() == "2026-06-26"
    assert "weekend" in resolved.reason


def test_low_coverage_latest_date_is_warning_not_silent_pass(tmp_path: Path, monkeypatch) -> None:
    import trade_research.validation.daily_pipeline as daily_pipeline

    data_dir = _write_pipeline_inputs(tmp_path, latest_partial=True)
    monkeypatch.setattr(
        daily_pipeline,
        "fetch_exchange_holidays",
        lambda exchange, year: ExchangeHolidays(frozenset(), frozenset(), "test"),
    )

    result = validate_daily_pipeline_health(
        data_dir=data_dir,
        run_live_fetch=False,
        run_factor_research=False,
        at=datetime(2026, 6, 28, 6, 0, tzinfo=UTC),
    )

    assert result.summary["overall_status"] == "warn"
    assert result.summary["baseline_ml_ready"] is True
    assert result.summary["low_coverage_dates"]
    stock_coverage = pd.read_parquet(
        data_dir / "processed/validation/daily_pipeline_stock_coverage.parquet"
    )
    stock_coverage_windows = pd.read_parquet(
        data_dir / "processed/validation/daily_pipeline_stock_coverage_windows.parquet"
    )
    assert "coverage_pct" in stock_coverage.columns
    assert "has_latest_expected_date" in stock_coverage.columns
    assert set(stock_coverage_windows["window_months"]) == {6, 9, 12, 15, 18, 24}
    assert int((~stock_coverage["has_latest_expected_date"]).sum()) == 1
    assert result.summary["stock_coverage"]["stocks_missing_latest_expected_date"] == 1
    assert "6m" in result.summary["stock_coverage_windows"]


def test_invalid_cleaned_ohlcv_rows_are_blocking(tmp_path: Path) -> None:
    data_dir = _write_pipeline_inputs(tmp_path, latest_partial=False)
    cleaned = pd.read_parquet(data_dir / "processed/validated/ohlcv_daily_validated.parquet")
    cleaned.loc[0, "volume"] = -1
    cleaned.to_parquet(data_dir / "processed/validated/ohlcv_daily_validated.parquet", index=False)

    result = validate_daily_pipeline_health(
        data_dir=data_dir,
        run_live_fetch=False,
        run_factor_research=False,
        at=datetime(2026, 6, 28, 6, 0, tzinfo=UTC),
    )

    assert result.summary["overall_status"] == "fail"
    assert any("Invalid OHLCV rows remain" in issue for issue in result.summary["blocking_issues"])


def test_feature_target_alignment_failure_is_blocking(tmp_path: Path, monkeypatch) -> None:
    import trade_research.validation.daily_pipeline as daily_pipeline

    data_dir = _write_pipeline_inputs(tmp_path, latest_partial=False)
    features = pd.read_parquet(data_dir / "processed/features/daily_v1_ohlcv_technical.parquet")
    features = features[features["instrument_key"].ne("B")].copy()
    features.to_parquet(
        data_dir / "processed/features/daily_v1_ohlcv_technical.parquet",
        index=False,
    )
    monkeypatch.setattr(
        daily_pipeline,
        "_rebuild_features",
        lambda data_root, commands_run: {
            "status": "pass",
            "blocking_issues": [],
            "warnings": ["feature rebuild mocked"],
        },
    )
    monkeypatch.setattr(
        daily_pipeline,
        "_rebuild_targets",
        lambda data_root, commands_run: {
            "status": "pass",
            "blocking_issues": [],
            "warnings": ["target rebuild mocked"],
        },
    )

    result = validate_daily_pipeline_health(
        data_dir=data_dir,
        run_live_fetch=False,
        run_factor_research=False,
        at=datetime(2026, 6, 28, 6, 0, tzinfo=UTC),
    )

    assert result.summary["overall_status"] == "fail"
    assert any(
        "Feature keys" in issue or "Alignment issue" in issue
        for issue in result.summary["blocking_issues"]
    )


def test_health_report_json_has_required_fields(tmp_path: Path) -> None:
    data_dir = _write_pipeline_inputs(tmp_path, latest_partial=False)

    result = validate_daily_pipeline_health(
        data_dir=data_dir,
        run_live_fetch=False,
        run_factor_research=False,
        at=datetime(2026, 6, 28, 6, 0, tzinfo=UTC),
    )

    for field in [
        "current_local_time",
        "latest_expected_trading_date",
        "commands_run",
        "stages",
        "row_counts",
        "date_ranges",
        "invalid_rows",
        "duplicate_key_counts",
        "stock_coverage",
        "lowest_stock_coverage",
        "blocking_issues",
        "warnings",
        "baseline_ml_ready",
    ]:
        assert field in result.summary
    assert result.json_path.exists()
    assert result.report_path.exists()


def test_read_only_health_does_not_rebuild_artifacts(tmp_path: Path, monkeypatch) -> None:
    import trade_research.validation.daily_pipeline as daily_pipeline

    data_dir = _write_pipeline_inputs(tmp_path, latest_partial=False)
    validate_daily_pipeline_health(
        data_dir=data_dir,
        run_live_fetch=False,
        run_factor_research=False,
        at=datetime(2026, 6, 28, 6, 0, tzinfo=UTC),
    )

    monkeypatch.setattr(
        daily_pipeline,
        "_rebuild_features",
        lambda data_root, commands_run: pytest.fail("features should not rebuild"),
    )
    monkeypatch.setattr(
        daily_pipeline,
        "_rebuild_targets",
        lambda data_root, commands_run: pytest.fail("targets should not rebuild"),
    )
    monkeypatch.setattr(
        daily_pipeline,
        "_ensure_cleaned_validation",
        lambda data_root, commands_run: pytest.fail("validation should not rewrite"),
    )
    monkeypatch.setattr(
        daily_pipeline,
        "_rebuild_factor_research",
        lambda data_root, commands_run: pytest.fail("factor research should not rebuild"),
    )

    result = validate_daily_pipeline_health(
        data_dir=data_dir,
        run_live_fetch=False,
        run_factor_research=True,
        rebuild_artifacts=False,
        at=datetime(2026, 6, 28, 6, 0, tzinfo=UTC),
    )

    assert result.summary["rebuild_artifacts"] is False
    assert result.summary["commands_run"] == []
    assert result.summary["stages"]["feature_rebuild"]["mode"] == "read_only"
    assert result.summary["stages"]["target_rebuild"]["mode"] == "read_only"
    assert result.summary["stages"]["factor_research"]["mode"] == "read_only"


def _write_pipeline_inputs(tmp_path: Path, latest_partial: bool) -> Path:
    data_dir = tmp_path / "data"
    for relative in [
        "processed/equities",
        "processed/validated",
        "processed/features",
        "processed/targets",
        "processed/instruments",
        "processed/universe",
        "processed/validation",
    ]:
        (data_dir / relative).mkdir(parents=True, exist_ok=True)

    processed = _ohlcv(latest_partial=latest_partial, include_invalid=True)
    cleaned = processed[processed["Volume"].ge(0)].rename(
        columns={
            "InstrumentKey": "instrument_key",
            "Date": "date",
            "Symbol": "symbol",
            "Open": "open",
            "High": "high",
            "Low": "low",
            "Close": "close",
            "Volume": "volume",
            "OpenInterest": "open_interest",
            "Source": "source",
        }
    )
    processed.to_parquet(
        data_dir / "processed/equities/nse_daily_ohlcv_upstox.parquet",
        index=False,
    )
    cleaned.to_parquet(data_dir / "processed/validated/ohlcv_daily_validated.parquet", index=False)

    keys = cleaned[["instrument_key", "symbol", "date"]].copy()
    features = keys.copy()
    features["exchange"] = "NSE"
    features["source"] = "upstox"
    features["feature_version"] = "daily_v1_ohlcv_technical_v1_0"
    features["computed_at"] = "2026-06-28T00:00:00+00:00"
    features["quality_status"] = "passed"
    features["open"] = cleaned["open"].to_numpy()
    features["high"] = cleaned["high"].to_numpy()
    features["low"] = cleaned["low"].to_numpy()
    features["close"] = cleaned["close"].to_numpy()
    features["volume"] = cleaned["volume"].to_numpy()
    features["feature_x"] = 1.0
    features.to_parquet(
        data_dir / "processed/features/daily_v1_ohlcv_technical.parquet",
        index=False,
    )

    targets = keys.copy()
    targets["exchange"] = "NSE"
    targets["source"] = "upstox"
    targets["target_version"] = "daily_v1_forward_returns_v1_0"
    targets["computed_at"] = "2026-06-28T00:00:00+00:00"
    targets["quality_status"] = "passed"
    for column in [
        "forward_ret_1d",
        "forward_ret_5d",
        "forward_ret_10d",
        "forward_ret_20d",
        "forward_ret_60d",
    ]:
        targets[column] = 0.01
    targets["top_quantile_forward_return_20d"] = False
    targets.to_parquet(data_dir / "processed/targets/daily_v1_forward_returns.parquet", index=False)

    pd.DataFrame({"instrument_key": ["A", "B"], "trading_symbol": ["AAA", "BBB"]}).to_parquet(
        data_dir / "processed/instruments/upstox_instruments.parquet", index=False
    )
    pd.DataFrame({"symbol": ["AAA", "BBB"], "instrument_key": ["A", "B"]}).to_csv(
        data_dir / "processed/universe/liquid_nse_upstox_mapping.csv", index=False
    )
    pd.DataFrame(columns=["symbol"]).to_csv(
        data_dir / "processed/universe/liquid_nse_upstox_unmatched.csv", index=False
    )
    (data_dir / "processed/validation/raw_to_processed_metadata.json").write_text(
        '{"full_raw_replay_possible": false, "invalid_rows": 1}\n'
    )
    return data_dir


def _ohlcv(latest_partial: bool, include_invalid: bool) -> pd.DataFrame:
    rows = [
        _row("A", "AAA", "2026-06-25", 100),
        _row("B", "BBB", "2026-06-25", 100),
        _row("A", "AAA", "2026-06-26", 100),
    ]
    if not latest_partial:
        rows.append(_row("B", "BBB", "2026-06-26", 100))
    if include_invalid:
        rows.append(_row("B", "BBB", "2026-06-24", -10))
    return pd.DataFrame(rows)


def _row(instrument_key: str, symbol: str, day: str, volume: int) -> dict[str, object]:
    return {
        "InstrumentKey": instrument_key,
        "Symbol": symbol,
        "Date": day,
        "Open": 10.0,
        "High": 11.0,
        "Low": 9.0,
        "Close": 10.5,
        "Volume": volume,
        "OpenInterest": 0,
        "Source": "upstox",
    }
