from __future__ import annotations

from pathlib import Path

import pytest
from sqlalchemy.exc import OperationalError
from typer.testing import CliRunner

from trade_research import cli
from trade_research.config import Settings
from trade_research.pipelines import yfinance_work_queue


def test_scheduled_planner_fails_closed_when_no_exchange_is_enabled(monkeypatch) -> None:
    monkeypatch.setattr(
        yfinance_work_queue,
        "get_settings",
        lambda: Settings(_env_file=None),
    )
    monkeypatch.setattr(
        yfinance_work_queue,
        "TimescaleStore",
        lambda _url: pytest.fail("database must not be opened without an enabled exchange"),
    )

    with pytest.raises(ValueError, match="No yfinance daily exchanges are enabled"):
        yfinance_work_queue.run_yfinance_daily_work_planner()


def test_planner_cli_hides_sql_and_parameters_on_database_failure(monkeypatch) -> None:
    def fail_planner(**_kwargs):
        raise OperationalError(
            "SELECT * FROM ohlcv_daily WHERE instrument_key IN (...) ",
            {"instrument_key": "sensitive-parameter"},
            RuntimeError("could not resize shared memory segment"),
        )

    monkeypatch.setattr(cli, "run_yfinance_daily_work_planner", fail_planner)

    result = CliRunner().invoke(
        cli.app,
        [
            "plan-yfinance-daily-work",
            "--exchanges",
            "US",
            "--no-initial-backfill",
            "--no-gap-repair",
        ],
    )

    assert result.exit_code == 1
    normalized_output = " ".join(result.output.split())
    assert "could not resize shared memory segment" in normalized_output
    assert "SELECT * FROM" not in result.output
    assert "sensitive-parameter" not in result.output
    assert "Traceback" not in result.output


def test_production_compose_bounds_postgres_shared_memory_and_passes_flags() -> None:
    repository_root = Path(__file__).resolve().parents[1]
    compose = (repository_root / "docker-compose.prod.yml").read_text(encoding="utf-8")

    assert "shm_size: ${PROD_POSTGRES_SHM_SIZE:-512mb}" in compose
    assert compose.count("\n      YFINANCE_FULL_US_ENABLED:") == 3
    assert compose.count("\n      YFINANCE_FULL_TSX_ENABLED:") == 3
    assert compose.count("\n      YFINANCE_NSE_ENABLED:") == 3
