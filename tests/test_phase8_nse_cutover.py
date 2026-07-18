from __future__ import annotations

from datetime import UTC, date, datetime, timedelta
from pathlib import Path

import pandas as pd
import pytest
from pydantic import ValidationError

from trade_research.config import Settings
from trade_research.pipelines import daily_pipeline_health, nse_cutover, yfinance_work_queue
from trade_research.pipelines.base import PipelineRunResult


def _candles(
    symbols: list[str],
    sessions: list[date],
    *,
    close_shift: float = 0,
) -> pd.DataFrame:
    return pd.DataFrame(
        [
            {
                "symbol": symbol,
                "date": session,
                "close": 100 + index + close_shift,
            }
            for symbol in symbols
            for index, session in enumerate(sessions)
        ]
    )


def test_phase8_flags_are_fail_closed_by_default() -> None:
    settings = Settings(_env_file=None)

    assert settings.yfinance_nse_canary_enabled is False
    assert settings.yfinance_nse_enabled is False
    assert settings.nse_daily_primary_source == "upstox"
    assert settings.legacy_upstox_nse_enabled is True


def test_yfinance_primary_requires_both_nse_execution_flags() -> None:
    with pytest.raises(ValidationError, match="NSE yfinance primary requires"):
        Settings(
            _env_file=None,
            nse_daily_primary_source="yfinance",
            yfinance_daily_enabled=True,
            yfinance_nse_enabled=False,
        )


def test_provider_comparison_matches_shared_symbol_sessions_only() -> None:
    sessions = [date(2026, 7, day) for day in (13, 14, 15)]
    upstox = _candles(["AAA", "BBB"], sessions)
    yfinance = _candles(["AAA.NS", "BBB.NS", "YFONLY.NS"], sessions)

    result = nse_cutover.compare_nse_provider_frames(
        upstox,
        yfinance,
        sessions=sessions,
        close_tolerance=0.01,
    )

    assert result["overlapping_symbols"] == 2
    assert result["overlap_rows"] == 6
    assert result["row_overlap_ratio"] == 1
    assert result["close_match_ratio"] == 1
    assert result["upstox_latest_date"] == "2026-07-15"
    assert result["yfinance_window_symbols"] == 3
    assert result["overlap_state"] == "overlap_available"


def test_provider_comparison_detects_close_divergence_and_missing_rows() -> None:
    sessions = [date(2026, 7, day) for day in (13, 14, 15)]
    upstox = _candles(["AAA"], sessions)
    yfinance = _candles(["AAA"], sessions[:-1], close_shift=10)

    result = nse_cutover.compare_nse_provider_frames(
        upstox,
        yfinance,
        sessions=sessions,
        close_tolerance=0.01,
    )

    assert result["row_overlap_ratio"] == pytest.approx(2 / 3)
    assert result["close_match_ratio"] == 0
    assert result["yfinance_latest_date"] == "2026-07-14"


def test_provider_freshness_is_independent_when_symbols_do_not_overlap() -> None:
    sessions = [date(2026, 7, day) for day in (13, 14, 15)]
    upstox = _candles(["AAA"], sessions[:-1])
    yfinance = _candles(["BBB.NS"], sessions)

    result = nse_cutover.compare_nse_provider_frames(
        upstox,
        yfinance,
        sessions=sessions,
        close_tolerance=0.01,
    )

    assert result["overlapping_symbols"] == 0
    assert result["overlap_state"] == "no_symbol_overlap"
    assert result["upstox_latest_date"] == "2026-07-14"
    assert result["yfinance_latest_date"] == "2026-07-15"
    assert result["upstox_window_rows"] == 2
    assert result["yfinance_window_rows"] == 3


def test_nse_canary_is_independent_from_full_nse_flag(monkeypatch) -> None:
    settings = Settings(
        _env_file=None,
        yfinance_nse_enabled=False,
        yfinance_nse_canary_enabled=True,
        yfinance_nse_canary_max_symbols=25,
    )
    captured: dict[str, object] = {}

    def fake_planner(**kwargs) -> PipelineRunResult:
        captured.update(kwargs)
        return PipelineRunResult(name="planner", status="pass", metrics={})

    monkeypatch.setattr(yfinance_work_queue, "get_settings", lambda: settings)
    monkeypatch.setattr(
        yfinance_work_queue,
        "run_yfinance_daily_work_planner",
        fake_planner,
    )

    result = yfinance_work_queue.run_yfinance_nse_canary_planner(
        symbol_limit=10,
        enqueue=True,
    )

    assert captured["exchanges"] == ("NSE",)
    assert captured["allow_disabled_exchanges"] is True
    assert captured["instrument_limit_per_exchange"] == 10
    assert result.metrics["canary_execution_enabled"] is True


class _ReadinessStore:
    def __init__(self, sessions: list[date], upstox: pd.DataFrame, yahoo: pd.DataFrame):
        self.sessions = sessions
        self.frames = {"upstox": upstox, "yfinance": yahoo}

    def latest_provider_eligible_exchange_session(self, *_args, **_kwargs):
        return {"session_date": self.sessions[-1]}

    def exchange_sessions(self, *_args, **_kwargs):
        return [
            {
                "session_date": session,
                "is_trading_day": True,
                "validation_status": "valid",
            }
            for session in self.sessions
        ]

    def daily_ohlcv_frame(self, *, source: str, **_kwargs):
        return self.frames[source]


def test_cutover_readiness_passes_only_with_overlap_and_freshness(monkeypatch) -> None:
    end = date(2026, 7, 17)
    sessions = [end - timedelta(days=value) for value in range(19, -1, -1)]
    settings = Settings(
        _env_file=None,
        nse_provider_comparison_sessions=20,
        nse_provider_comparison_minimum_symbols=2,
    )
    frame = _candles(["AAA", "BBB"], sessions)
    store = _ReadinessStore(sessions, frame, frame.copy())
    monkeypatch.setattr(nse_cutover, "get_settings", lambda: settings)

    result = nse_cutover.run_nse_yfinance_cutover_readiness(
        at=datetime(2026, 7, 18, tzinfo=UTC),
        store=store,
    )

    assert result.status == "pass"
    assert result.metrics["ready"] is True
    assert result.metrics["upstox_session_lag"] == 0
    assert result.metrics["yfinance_session_lag"] == 0
    assert result.metrics["comparison_state"] == "ready"


def test_cutover_readiness_distinguishes_no_overlap_from_freshness(monkeypatch) -> None:
    end = date(2026, 7, 17)
    sessions = [end - timedelta(days=value) for value in range(19, -1, -1)]
    settings = Settings(
        _env_file=None,
        nse_provider_comparison_sessions=20,
        nse_provider_comparison_minimum_symbols=1,
    )
    store = _ReadinessStore(
        sessions,
        _candles(["AAA"], sessions),
        _candles(["BBB.NS"], sessions),
    )
    monkeypatch.setattr(nse_cutover, "get_settings", lambda: settings)

    result = nse_cutover.run_nse_yfinance_cutover_readiness(store=store)

    assert result.status == "fail"
    assert result.metrics["comparison_state"] == "no_symbol_overlap"
    assert result.metrics["upstox_session_lag"] == 0
    assert result.metrics["yfinance_session_lag"] == 0
    assert result.metrics["stale_providers"] == []
    assert len(result.blocking_issues) == 1
    assert "no comparable symbols" in result.blocking_issues[0]


def test_cutover_readiness_distinguishes_missing_provider_data(monkeypatch) -> None:
    end = date(2026, 7, 17)
    sessions = [end - timedelta(days=value) for value in range(19, -1, -1)]
    settings = Settings(
        _env_file=None,
        nse_provider_comparison_sessions=20,
        nse_provider_comparison_minimum_symbols=1,
    )
    store = _ReadinessStore(
        sessions,
        pd.DataFrame(),
        _candles(["AAA.NS"], sessions),
    )
    monkeypatch.setattr(nse_cutover, "get_settings", lambda: settings)

    result = nse_cutover.run_nse_yfinance_cutover_readiness(store=store)

    assert result.status == "fail"
    assert result.metrics["comparison_state"] == "provider_data_missing"
    assert result.metrics["missing_providers"] == ["upstox"]
    assert result.metrics["stale_providers"] == []
    assert result.metrics["yfinance_session_lag"] == 0
    assert len(result.blocking_issues) == 1
    assert "no data for: upstox" in result.blocking_issues[0]


def test_cutover_readiness_reports_stale_provider_independently(monkeypatch) -> None:
    end = date(2026, 7, 17)
    sessions = [end - timedelta(days=value) for value in range(19, -1, -1)]
    settings = Settings(
        _env_file=None,
        nse_provider_comparison_sessions=20,
        nse_provider_comparison_minimum_symbols=1,
        nse_provider_comparison_maximum_session_lag=1,
    )
    store = _ReadinessStore(
        sessions,
        _candles(["AAA"], sessions[:-2]),
        _candles(["AAA.NS"], sessions),
    )
    monkeypatch.setattr(nse_cutover, "get_settings", lambda: settings)

    result = nse_cutover.run_nse_yfinance_cutover_readiness(store=store)

    assert result.status == "fail"
    assert result.metrics["comparison_state"] == "provider_stale"
    assert result.metrics["upstox_session_lag"] == 2
    assert result.metrics["yfinance_session_lag"] == 0
    assert result.metrics["stale_providers"] == ["upstox"]


def test_yfinance_daily_health_uses_provider_neutral_validation(monkeypatch) -> None:
    settings = Settings(
        _env_file=None,
        yfinance_daily_enabled=True,
        yfinance_nse_enabled=True,
        nse_daily_primary_source="yfinance",
    )
    readiness = PipelineRunResult(
        name="readiness",
        status="pass",
        metrics={"ready": True},
    )
    processed = PipelineRunResult(
        name="processed",
        status="pass",
        rows=42,
        artifacts={
            "summary_md": Path("summary.md"),
            "summary_json": Path("summary.json"),
        },
        metrics={"overall_status": "pass"},
    )
    monkeypatch.setattr(daily_pipeline_health, "get_settings", lambda: settings)
    monkeypatch.setattr(
        daily_pipeline_health,
        "run_nse_yfinance_cutover_readiness",
        lambda **_kwargs: readiness,
    )
    monkeypatch.setattr(
        daily_pipeline_health,
        "run_processed_dataset_validation_pipeline",
        lambda **_kwargs: processed,
    )
    monkeypatch.setattr(
        daily_pipeline_health,
        "validate_daily_pipeline_health",
        lambda **_kwargs: pytest.fail("legacy Upstox health path must not run"),
    )

    result = daily_pipeline_health.run_daily_pipeline_health_pipeline()

    assert result.status == "pass"
    assert result.rows == 42
    assert result.metrics["primary_source"] == "yfinance"
