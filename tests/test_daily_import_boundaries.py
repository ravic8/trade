"""Daily startup must work when research and agent dependencies are unavailable."""

from __future__ import annotations

import os
import subprocess
import sys
import textwrap
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]


def test_daily_entrypoints_import_without_research_or_agent_dependencies(tmp_path: Path) -> None:
    script = textwrap.dedent("""
        import importlib
        import importlib.abc
        import sys

        blocked = (
            "trade_data.analytics.bigquery", "trade_data.features",
            "trade_data.targets", "trade_data.modeling",
            "trade_data.research", "trade_data.filings", "trade_data.chat",
            "trade_data.scraping", "trade_data.screeners",
            "trade_data.data.dukascopy_provider", "trade_data.universe.dukascopy",
            "trade_data.universe.yfinance_intraday", "trade_data.storage.vector",
            "trade_data.pipelines.backtest", "trade_data.pipelines.daily_features",
            "trade_data.pipelines.yfinance_intraday",
            "trade_data.pipelines.yfinance_nse_minute",
            "trade_data.validation.daily_pipeline",
            "trade_data.validation.processed_datasets",
            "trade_data.contracts.catalog",
            "trade_data.market_data.aggregation", "trade_data.market_data.availability",
            "trade_data.backlog",
            "openai", "langgraph", "qdrant_client", "celery", "lightgbm", "sklearn",
        )
        attempts = []

        class Boundary(importlib.abc.MetaPathFinder):
            def find_spec(self, fullname, path=None, target=None):
                if any(fullname == name or fullname.startswith(name + ".") for name in blocked):
                    attempts.append(fullname)
                    raise ModuleNotFoundError("Forbidden daily dependency: " + fullname)

        sys.meta_path.insert(0, Boundary())
        for module in (
            "trade_data.pipelines.yfinance_work_queue",
            "trade_data.pipelines.yfinance_daily",
            "trade_data.pipelines.daily_ohlcv",
            "trade_data.pipelines.nse_cutover",
            "trade_data.pipelines.universe_snapshot",
            "trade_data.pipelines.exchange_sessions",
            "trade_data.operations.daily_canary",
            "trade_data.dagster.definitions",
            "trade_data.cli",
            "trade_data.api.app",
        ):
            importlib.import_module(module)
        from trade_data.pipelines import run_yfinance_daily_work_planner
        from trade_data.storage import TimescaleStore
        from trade_data.contracts import DAILY_DATA_CONTRACT_REGISTRY, get_data_contract
        from trade_data.validation import resolve_latest_expected_trading_date
        from trade_data.market_calendar import ExchangeHolidays
        from datetime import UTC, datetime

        assert callable(run_yfinance_daily_work_planner)
        assert TimescaleStore.__name__ == "TimescaleStore"
        assert len(DAILY_DATA_CONTRACT_REGISTRY.contract_ids) == 5
        assert get_data_contract("market_data.ohlcv_daily.v1").storage_name == "ohlcv_daily"
        try:
            get_data_contract("unknown.contract")
        except KeyError:
            pass
        else:
            raise AssertionError("Unknown contracts must fail")
        session = resolve_latest_expected_trading_date(
            at=datetime(2026, 10, 18, 12, tzinfo=UTC),
            holidays=ExchangeHolidays(frozenset(), frozenset(), "test"),
        )
        assert session.latest_expected_trading_date.isoformat() == "2026-10-16"
        assert not attempts, attempts
    """)
    env = {**os.environ, "PYTHONPATH": str(ROOT / "src")}
    result = subprocess.run(
        [sys.executable, "-c", script],
        cwd=tmp_path,
        env=env,
        capture_output=True,
        text=True,
        timeout=45,
    )
    assert result.returncode == 0, result.stdout + result.stderr
