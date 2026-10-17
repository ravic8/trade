from datetime import date

import pandas as pd
import pytest

from trade_data.config import Settings
from trade_data.data.rate_limits import RateLimitDecision
from trade_data.pipelines import yfinance_work_queue
from trade_data.pipelines.daily_runtime import YFinanceDailyRuntime
from trade_data.pipelines.yfinance_daily import (
    _execute_yfinance_daily_batches_with_controls,
)


class EmptyProvider:
    def __init__(self) -> None:
        self.calls = 0

    def fetch_daily_ohlcv(
        self, symbols: list[dict[str, str]], start: date, end: date
    ) -> pd.DataFrame:
        self.calls += 1
        return pd.DataFrame()


class NoWaitLimiter:
    def acquire(self, provider: str, endpoint_group: str, weight: int = 1) -> RateLimitDecision:
        return RateLimitDecision(backend="memory", wait_seconds=0.0, rate_limited=False)

    def update_rate_per_minute(self, provider: str, limit: int) -> None:
        pass


def test_daily_worker_uses_injected_store_without_contacting_provider(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    class Store:
        def initialize(self) -> None:
            pass

        def pipeline_work_queue_summary(self) -> dict[str, int]:
            return {"pending": 0}

    store = Store()
    opened_urls: list[str] = []

    def open_store(url: str) -> Store:
        opened_urls.append(url)
        return store

    def unexpected_provider() -> EmptyProvider:
        pytest.fail("Disabled daily ingestion must not construct a provider")

    runtime = YFinanceDailyRuntime(open_store, unexpected_provider)
    settings = Settings(
        _env_file=None,
        database_url="postgresql+psycopg://trade:trade@localhost:5432/trade_research",
        yfinance_daily_enabled=False,
    )
    monkeypatch.setattr(yfinance_work_queue, "get_settings", lambda: settings)

    result = yfinance_work_queue.run_yfinance_daily_work_queue(runtime=runtime)

    assert opened_urls == [settings.database_url]
    assert result.metrics["queue"] == {"pending": 0}
    assert result.metrics["execution_enabled"] is False


def test_daily_executor_uses_injected_provider_factory_without_market_fixture() -> None:
    provider = EmptyProvider()

    def unexpected_store(_url: str) -> None:
        pytest.fail("Executor must not open a database")

    runtime = YFinanceDailyRuntime(unexpected_store, lambda: provider)
    execution = _execute_yfinance_daily_batches_with_controls(
        provider=None,
        rows=[
            {
                "symbol": "AAPL",
                "instrument_key": "YF|AAPL",
                "yahoo_symbol": "AAPL",
                "fetch_start": "2026-07-01",
                "fetch_end": "2026-07-01",
            }
        ],
        limiter=NoWaitLimiter(),
        db=None,
        run_id=None,
        batch_size=1,
        settings=Settings(
            _env_file=None,
            yfinance_immediate_retry_attempts=1,
            yfinance_retry_wait_multiplier_seconds=0,
            yfinance_retry_wait_max_seconds=0,
        ),
        runtime=runtime,
    )

    assert provider.calls == 1
    assert execution.frames == []
    assert execution.failures[0]["instrument_key"] == "YF|AAPL"


def test_claimed_worker_uses_injected_rate_limiter_without_real_market_data(monkeypatch):
    from trade_data.pipelines.yfinance_work_queue import _execute_claimed_exchange_work

    class Store:
        def latest_provider_eligible_exchange_session(self, *_args, **_kwargs):
            return {"session_date": date(2026, 10, 1)}

        def adaptive_rate_state(self, _provider):
            return None

        def insert_provider_request_logs(self, logs):
            return len(logs)

    provider = EmptyProvider()
    seen = []

    def limiter_factory(settings):
        seen.append(settings.redis_url)
        return NoWaitLimiter()

    def forbidden_default(_settings):
        pytest.fail("Injected limiter must bypass default Redis construction")

    monkeypatch.setattr(yfinance_work_queue, "build_provider_rate_limiter", forbidden_default)
    runtime = YFinanceDailyRuntime(lambda _url: Store(), lambda: provider, limiter_factory)
    settings = Settings(
        _env_file=None,
        redis_url="rediss://operator-redis.example:6380/1",
        yfinance_immediate_retry_attempts=1,
        yfinance_retry_wait_multiplier_seconds=0,
        yfinance_retry_wait_max_seconds=0,
    )
    outcomes, written, adjustments = _execute_claimed_exchange_work(
        db=Store(),
        settings=settings,
        exchange="NSE",
        run_id="contract-check",
        provider=None,
        runtime=runtime,
        work_items=[
            {
                "work_item_id": "contract-check",
                "work_type": "bounded_canary",
                "provider_symbol": "RELIANCE.NS",
                "window_start": date(2026, 10, 1),
                "window_end": date(2026, 10, 1),
            }
        ],
    )
    assert seen == [settings.redis_url]
    assert provider.calls == 1
    assert written == adjustments == 0
    assert outcomes and outcomes[0]["status"] != "success"


def test_default_runtime_preserves_configured_database_endpoint_without_connecting(monkeypatch):
    from sqlalchemy.engine import Engine

    from trade_data.pipelines.daily_runtime import build_yfinance_daily_runtime

    def forbidden_connection(*_args, **_kwargs):
        pytest.fail("Composition and engine construction must not connect")

    monkeypatch.setattr(Engine, "connect", forbidden_connection)
    settings = Settings(
        _env_file=None,
        database_url="postgresql+psycopg://operator:private-marker@db.example:15432/research",
    )
    runtime = build_yfinance_daily_runtime()
    store = runtime.open_store(settings)
    assert store.engine.url.host == "db.example"
    assert store.engine.url.port == 15432
    assert store.engine.url.database == "research"
    store.engine.dispose()
