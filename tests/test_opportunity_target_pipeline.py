from __future__ import annotations

from datetime import UTC, date, datetime
from types import SimpleNamespace

import pytest
from sqlalchemy import select
from typer.testing import CliRunner

from trade_research import cli
from trade_research.pipelines import opportunity_targets
from trade_research.pipelines.base import PipelineRunResult
from trade_research.storage import TimescaleStore
from trade_research.storage.timescale import (
    metadata,
    ohlcv_daily_table,
    opportunity_targets_daily_table,
    target_runs_table,
)


def _source_row(
    instrument_key: str,
    session_date: date,
    *,
    close: float,
    valid: bool = True,
) -> dict:
    return {
        "instrument_key": instrument_key,
        "source": "yfinance",
        "date": session_date,
        "symbol": instrument_key.rsplit("|", 1)[-1],
        "exchange": "US",
        "open": close - 1,
        "high": close + 1 if valid else close - 3,
        "low": close - 2 if valid else close + 2,
        "close": close,
        "volume": 100_000,
        "open_interest": 0,
        "fetched_at": datetime(2026, 1, 10, tzinfo=UTC),
        "quality_status": "passed" if valid else "failed",
    }


def _pipeline_store(tmp_path, monkeypatch, rows: list[dict]) -> TimescaleStore:
    store = TimescaleStore(f"sqlite:///{tmp_path / 'opportunity-pipeline.sqlite'}")
    metadata.create_all(store.engine)
    with store.engine.begin() as connection:
        connection.execute(ohlcv_daily_table.insert(), rows)
    store.initialize = lambda: None  # type: ignore[method-assign]
    monkeypatch.setattr(opportunity_targets, "TimescaleStore", lambda _url: store)
    monkeypatch.setattr(
        opportunity_targets,
        "get_settings",
        lambda: SimpleNamespace(database_url="unused", data_dir=tmp_path),
    )
    return store


def test_opportunity_pipeline_bounds_batches_and_persists_progress(
    tmp_path,
    monkeypatch,
) -> None:
    rows = [
        _source_row(f"US_EQ|S{number}", session_date, close=100 + day)
        for number in range(5)
        for day, session_date in enumerate(
            (date(2026, 1, 2), date(2026, 1, 5), date(2026, 1, 6))
        )
    ]
    store = _pipeline_store(tmp_path, monkeypatch, rows)
    requested_batches: list[tuple[str, ...]] = []
    original_daily_frame = store.daily_ohlcv_frame

    def observed_daily_frame(**kwargs):
        requested_batches.append(tuple(kwargs["instrument_keys"]))
        return original_daily_frame(**kwargs)

    store.daily_ohlcv_frame = observed_daily_frame  # type: ignore[method-assign]

    result = opportunity_targets.run_opportunity_target_pipeline(
        exchange="US",
        incremental=False,
        batch_size=2,
    )

    assert result.rows == 15
    assert result.metrics["batch_count"] == 3
    assert result.metrics["completed_batches"] == 3
    assert result.metrics["max_source_rows_in_batch"] == 6
    assert [len(batch) for batch in requested_batches] == [2, 2, 1]
    assert result.artifacts["targets"].is_file()
    with store.engine.begin() as connection:
        assert connection.execute(select(opportunity_targets_daily_table)).all()
        run = connection.execute(
            select(target_runs_table).where(
                target_runs_table.c.run_id == result.metrics["timescale_run_id"]
            )
        ).mappings().one()
    assert run["status"] == "completed"
    assert run["finished_at"] is not None
    assert run["summary_json"]["completed_batches"] == 3


def test_incremental_pipeline_uses_last_valid_predecessor(tmp_path, monkeypatch) -> None:
    key = "US_EQ|AAA"
    rows = [
        _source_row(key, date(2025, 12, 26), close=100),
        _source_row(key, date(2025, 12, 27), close=999, valid=False),
        _source_row(key, date(2025, 12, 29), close=103),
        _source_row(key, date(2025, 12, 30), close=104),
    ]
    store = _pipeline_store(tmp_path, monkeypatch, rows)
    store.latest_daily_opportunity_target_date = (  # type: ignore[method-assign]
        lambda *_args, **_kwargs: date(2026, 1, 5)
    )

    result = opportunity_targets.run_opportunity_target_pipeline(
        exchange="US",
        incremental=True,
        recompute_lookback_days=7,
        batch_size=1,
    )

    stored = store.daily_opportunity_target_frame(
        target_version="daily_opportunity_outcomes_v1_0",
        exchange="US",
        source="yfinance",
    )
    first_dirty_row = stored.loc[stored["date"] == date(2025, 12, 29)].iloc[0]
    assert result.rows == 2
    assert first_dirty_row["previous_close"] == 100
    assert first_dirty_row["quality_status"] == "passed"


def test_opportunity_pipeline_records_failed_batch_progress(tmp_path, monkeypatch) -> None:
    rows = [
        _source_row(f"US_EQ|S{number}", date(2026, 1, 2), close=100)
        for number in range(3)
    ]
    store = _pipeline_store(tmp_path, monkeypatch, rows)
    original_upsert = store.upsert_daily_opportunity_targets
    calls = 0

    def fail_second_batch(frame):
        nonlocal calls
        calls += 1
        if calls == 2:
            raise RuntimeError("injected batch failure")
        return original_upsert(frame)

    store.upsert_daily_opportunity_targets = fail_second_batch  # type: ignore[method-assign]

    with pytest.raises(RuntimeError, match="injected batch failure"):
        opportunity_targets.run_opportunity_target_pipeline(
            exchange="US",
            incremental=False,
            batch_size=1,
        )

    with store.engine.begin() as connection:
        run = connection.execute(
            select(target_runs_table).order_by(target_runs_table.c.started_at.desc())
        ).mappings().first()
    assert run is not None
    assert run["status"] == "failed"
    assert run["summary_json"]["completed_batches"] == 1
    assert run["summary_json"]["error_type"] == "RuntimeError"


def test_opportunity_cli_forwards_bounded_batch_size(monkeypatch) -> None:
    captured: dict = {}

    def fake_pipeline(**kwargs):
        captured.update(kwargs)
        return PipelineRunResult(
            name="us_opportunity_targets",
            status="pass",
            rows=10,
            artifacts={},
            metrics={"timescale_run_id": "run-1"},
        )

    monkeypatch.setattr(cli, "run_opportunity_target_pipeline", fake_pipeline)

    result = CliRunner().invoke(
        cli.app,
        [
            "build-opportunity-targets",
            "--exchange",
            "US",
            "--full-rebuild",
            "--batch-size",
            "25",
        ],
    )

    assert result.exit_code == 0
    assert captured["incremental"] is False
    assert captured["batch_size"] == 25
