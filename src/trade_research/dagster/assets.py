from datetime import UTC, datetime
from typing import Any
from zoneinfo import ZoneInfo

from dagster import AssetIn, ResourceParam, asset

from trade_research.config import get_settings
from trade_research.data import YahooFinanceMarketDataProvider
from trade_research.market_calendar import (
    EXCHANGE_CONFIGS,
    ExchangeHolidays,
    fetch_exchange_holidays,
    session_decision,
)
from trade_research.schemas import Symbol
from trade_research.storage.timescale import TimescaleStore
from trade_research.universe import NSEUniverseProvider, TSXUniverseProvider


@asset(
    group_name="nse_market_data",
    compute_kind="http",
    description="Official NSE equity universe from NSE CSV archives.",
)
def nse_universe(
    context,
    timescale_store: ResourceParam[TimescaleStore],
) -> list[Symbol]:
    settings = get_settings()
    cached_symbols = timescale_store.active_symbols(
        "NSE",
        max_age_days=settings.universe_refresh_days,
    )
    if cached_symbols:
        context.add_output_metadata(
            {
                "exchange": "NSE",
                "symbols_fetched": 0,
                "symbols_returned": len(cached_symbols),
                "source": "timescale_cache",
                "cache_max_age_days": settings.universe_refresh_days,
            }
        )
        return cached_symbols

    try:
        symbols = NSEUniverseProvider().fetch()
    except Exception:
        stale_symbols = timescale_store.active_symbols("NSE")
        if stale_symbols:
            context.add_output_metadata(
                {
                    "exchange": "NSE",
                    "symbols_fetched": 0,
                    "symbols_returned": len(stale_symbols),
                    "source": "timescale_stale_cache",
                    "cache_max_age_days": settings.universe_refresh_days,
                }
            )
            return stale_symbols
        raise
    stored_count = timescale_store.upsert_symbols(symbols)

    context.add_output_metadata(
        {
            "exchange": "NSE",
            "symbols_fetched": len(symbols),
            "symbols_stored": stored_count,
            "source": "nse_equity_list",
            "cache_refreshed": True,
        }
    )
    return symbols


@asset(
    group_name="tsx_market_data",
    compute_kind="http",
    description="TSX equity universe from the configured CSV source.",
)
def tsx_universe(
    context,
    timescale_store: ResourceParam[TimescaleStore],
) -> list[Symbol]:
    settings = get_settings()
    cached_symbols = timescale_store.active_symbols(
        "TSX",
        max_age_days=settings.universe_refresh_days,
    )
    if cached_symbols:
        context.add_output_metadata(
            {
                "exchange": "TSX",
                "symbols_fetched": 0,
                "symbols_returned": len(cached_symbols),
                "source": "timescale_cache",
                "cache_max_age_days": settings.universe_refresh_days,
            }
        )
        return cached_symbols

    try:
        symbols = TSXUniverseProvider().fetch()
    except Exception:
        stale_symbols = timescale_store.active_symbols("TSX")
        if stale_symbols:
            context.add_output_metadata(
                {
                    "exchange": "TSX",
                    "symbols_fetched": 0,
                    "symbols_returned": len(stale_symbols),
                    "source": "timescale_stale_cache",
                    "cache_max_age_days": settings.universe_refresh_days,
                }
            )
            return stale_symbols
        raise
    stored_count = timescale_store.upsert_symbols(symbols)

    context.add_output_metadata(
        {
            "exchange": "TSX",
            "symbols_fetched": len(symbols),
            "symbols_stored": stored_count,
            "source": "tsx_google_sheet",
            "cache_refreshed": True,
        }
    )
    return symbols


@asset(
    group_name="nse_market_data",
    compute_kind="yfinance",
    ins={"symbols": AssetIn("nse_universe")},
    description="Hourly NSE OHLCV candles fetched from Yahoo Finance and stored in TimescaleDB.",
)
def nse_hourly_ohlcv(
    context,
    timescale_store: ResourceParam[TimescaleStore],
    symbols: list[Symbol],
) -> dict[str, Any]:
    return _ingest_hourly_ohlcv(
        context=context,
        timescale_store=timescale_store,
        symbols=symbols,
        exchange="NSE",
        configured_symbol_limit=get_settings().nse_ingest_limit,
    )


@asset(
    group_name="tsx_market_data",
    compute_kind="yfinance",
    ins={"symbols": AssetIn("tsx_universe")},
    description="Hourly TSX OHLCV candles fetched from Yahoo Finance and stored in TimescaleDB.",
)
def tsx_hourly_ohlcv(
    context,
    timescale_store: ResourceParam[TimescaleStore],
    symbols: list[Symbol],
) -> dict[str, Any]:
    return _ingest_hourly_ohlcv(
        context=context,
        timescale_store=timescale_store,
        symbols=symbols,
        exchange="TSX",
        configured_symbol_limit=get_settings().tsx_ingest_limit,
    )


@asset(
    group_name="nse_market_data",
    compute_kind="yfinance",
    ins={"symbols": AssetIn("nse_universe")},
    config_schema={"window_start": str, "window_end": str},
    description="Recover a detected missing or partial NSE hourly Yahoo candle window.",
)
def nse_hourly_backlog_ohlcv(
    context,
    timescale_store: ResourceParam[TimescaleStore],
    symbols: list[Symbol],
) -> dict[str, Any]:
    return _recover_hourly_backlog_window(context, timescale_store, symbols, "NSE")


@asset(
    group_name="tsx_market_data",
    compute_kind="yfinance",
    ins={"symbols": AssetIn("tsx_universe")},
    config_schema={"window_start": str, "window_end": str},
    description="Recover a detected missing or partial TSX hourly Yahoo candle window.",
)
def tsx_hourly_backlog_ohlcv(
    context,
    timescale_store: ResourceParam[TimescaleStore],
    symbols: list[Symbol],
) -> dict[str, Any]:
    return _recover_hourly_backlog_window(context, timescale_store, symbols, "TSX")


def _recover_hourly_backlog_window(
    context,
    timescale_store: TimescaleStore,
    symbols: list[Symbol],
    exchange: str,
) -> dict[str, Any]:
    settings = get_settings()
    window_start = datetime.fromisoformat(context.op_config["window_start"])
    window_end = datetime.fromisoformat(context.op_config["window_end"])
    timescale_store.mark_hourly_backlog_running(exchange, window_start, context.run_id)
    try:
        result = _ingest_hourly_ohlcv(
            context=context,
            timescale_store=timescale_store,
            symbols=symbols,
            exchange=exchange,
            configured_symbol_limit=(
                settings.nse_ingest_limit if exchange == "NSE" else settings.tsx_ingest_limit
            ),
            fetch_mode="backlog_recovery",
            lookback_days=settings.hourly_history_lookback_days,
            respect_market_session=False,
        )
    except Exception as exc:
        timescale_store.finish_hourly_backlog_recovery(
            exchange=exchange,
            window_start=window_start,
            observed_symbol_count=timescale_store.hourly_window_symbol_count(
                exchange,
                window_start,
            ),
            expected_symbol_count=timescale_store.fetchable_symbol_count(exchange),
            coverage_threshold=settings.hourly_backlog_coverage_threshold,
            error_message=str(exc),
        )
        raise

    expected = timescale_store.fetchable_symbol_count(exchange)
    observed = timescale_store.hourly_window_symbol_count(exchange, window_start)
    backlog_row = timescale_store.finish_hourly_backlog_recovery(
        exchange=exchange,
        window_start=window_start,
        observed_symbol_count=observed,
        expected_symbol_count=expected,
        coverage_threshold=settings.hourly_backlog_coverage_threshold,
    )
    recovery = {
        "backlog_window_start": window_start.isoformat(),
        "backlog_window_end": window_end.isoformat(),
        "backlog_expected_symbols": expected,
        "backlog_observed_symbols": observed,
        "backlog_status": backlog_row["status"] if backlog_row else "untracked",
    }
    context.add_output_metadata(recovery)
    return {**result, **recovery}


def _ingest_hourly_ohlcv(
    context,
    timescale_store: TimescaleStore,
    symbols: list[Symbol],
    exchange: str,
    configured_symbol_limit: int | None,
    fetch_mode: str = "realtime",
    lookback_days: int | None = None,
    respect_market_session: bool = True,
) -> dict[str, Any]:
    settings = get_settings()
    fetch_lookback_days = lookback_days or settings.hourly_realtime_lookback_days
    if respect_market_session:
        try:
            holidays = _cached_or_fetch_holidays(
                timescale_store,
                exchange=exchange,
                max_age_days=settings.calendar_refresh_days,
            )
            session = session_decision(exchange, holidays=holidays)
        except Exception as exc:
            if settings.bypass_calendar:
                session = _bypassed_session(f"bypass_calendar_unavailable:{exc}")
            else:
                context.add_output_metadata(
                    {
                        "exchange": exchange,
                        "status": "skipped_calendar_unavailable",
                        "reason": str(exc),
                        "calendar_cache_max_age_days": settings.calendar_refresh_days,
                    }
                )
                return {
                    "exchange": exchange,
                    "status": "skipped_calendar_unavailable",
                    "reason": str(exc),
                    "tickers_requested": 0,
                    "tickers_with_data": 0,
                    "rows_written": 0,
                }
    else:
        session = _bypassed_session("backlog_recovery")

    if respect_market_session and not session.should_fetch and not settings.bypass_calendar:
        context.add_output_metadata(
            {
                "exchange": exchange,
                "status": "skipped_closed",
                "reason": session.reason,
                "local_time": session.local_time.isoformat(),
                "calendar_source": session.source_url,
                "calendar_cache_max_age_days": settings.calendar_refresh_days,
            }
        )
        return {
            "exchange": exchange,
            "status": "skipped_closed",
            "reason": session.reason,
            "tickers_requested": 0,
            "tickers_with_data": 0,
            "rows_written": 0,
        }

    candidate_symbols = (
        symbols[:configured_symbol_limit] if configured_symbol_limit else symbols
    )
    selected_symbols = timescale_store.fetchable_symbols(
        candidate_symbols,
        source="yahoo",
        limit=configured_symbol_limit,
    )
    tickers = [symbol.yahoo_symbol for symbol in selected_symbols if symbol.yahoo_symbol]
    run_id = timescale_store.start_ingestion_run(
        job_name=f"{exchange.lower()}_hourly_ohlcv",
        exchange=exchange,
        source="yahoo",
        items_requested=len(tickers),
        run_metadata={
            "fetch_mode": fetch_mode,
            "lookback_days": fetch_lookback_days,
            "batch_size": settings.yfinance_batch_size,
            "configured_symbol_limit": configured_symbol_limit,
            "candidate_symbols": len(candidate_symbols),
            "skipped_by_feed_health": len(candidate_symbols) - len(selected_symbols),
            "session_local_time": session.local_time.isoformat(),
            "session_market_open": session.market_open.isoformat() if session.market_open else None,
            "session_market_close": session.market_close.isoformat()
            if session.market_close
            else None,
            "calendar_source": session.source_url,
            "calendar_cache_max_age_days": settings.calendar_refresh_days,
        },
    )

    try:
        provider = YahooFinanceMarketDataProvider(
            batch_size=settings.yfinance_batch_size,
            throttle_seconds=settings.yfinance_throttle_seconds,
            max_workers=settings.yfinance_max_workers,
            retry_attempts=settings.yfinance_retry_attempts,
            retry_base_seconds=settings.yfinance_retry_base_seconds,
            jitter_seconds=settings.yfinance_jitter_seconds,
        )
        ohlcv = provider.fetch_hourly_ohlcv(
            tickers,
            period=f"{fetch_lookback_days}d",
        )
        rows_written = timescale_store.upsert_hourly_ohlcv(
            ohlcv,
            exchange=exchange,
            source="yahoo",
        )
        successful_latest_candles = _successful_latest_candles(ohlcv)
        feed_health = timescale_store.update_feed_health(
            selected_symbols,
            successful_latest_candles=successful_latest_candles,
            source="yahoo",
            failure_threshold=settings.feed_health_failure_threshold,
            max_backoff_hours=settings.feed_health_max_backoff_hours,
            unsupported_retry_days=settings.feed_health_unsupported_retry_days,
        )
        fetched_tickers = len(successful_latest_candles)
        failed = max(len(tickers) - fetched_tickers, 0)
        status = "completed" if rows_written else "completed_empty"
        timescale_store.finish_ingestion_run(
            run_id=run_id,
            status=status,
            items_processed=len(tickers),
            items_succeeded=fetched_tickers,
            items_failed=failed,
        )
    except Exception as exc:
        timescale_store.finish_ingestion_run(
            run_id=run_id,
            status="failed",
            items_processed=0,
            items_succeeded=0,
            items_failed=len(tickers),
            error_message=str(exc),
        )
        raise

    result = {
        "run_id": str(run_id),
        "tickers_requested": len(tickers),
        "tickers_with_data": fetched_tickers,
        "feed_health_succeeded": feed_health["succeeded"],
        "feed_health_failed": feed_health["failed"],
        "skipped_by_feed_health": len(candidate_symbols) - len(selected_symbols),
        "rows_written": rows_written,
        "fetch_mode": fetch_mode,
        "lookback_days": fetch_lookback_days,
        "status": status,
        "session_reason": session.reason,
    }
    context.add_output_metadata(result)
    return result


def _bypassed_session(reason: str):
    from types import SimpleNamespace

    return SimpleNamespace(
        should_fetch=True,
        reason=reason,
        local_time=datetime.now(UTC),
        market_open=None,
        market_close=None,
        source_url="bypass",
    )


def _successful_latest_candles(frame) -> dict[str, datetime]:
    if frame.empty:
        return {}
    latest = frame.groupby("Ticker")["Datetime"].max()
    return {
        str(ticker).upper(): datetime.fromisoformat(str(timestamp))
        if not hasattr(timestamp, "to_pydatetime")
        else timestamp.to_pydatetime()
        for ticker, timestamp in latest.items()
    }


def _cached_or_fetch_holidays(
    timescale_store: TimescaleStore,
    exchange: str,
    max_age_days: int,
) -> ExchangeHolidays:
    exchange_code = exchange.upper()
    config = EXCHANGE_CONFIGS[exchange_code]
    local_year = datetime.now(UTC).astimezone(ZoneInfo(config.timezone)).year
    cached = timescale_store.exchange_holidays(
        exchange_code,
        year=local_year,
        max_age_days=max_age_days,
    )
    if cached:
        return ExchangeHolidays(
            closed_dates=frozenset(
                datetime.fromisoformat(item).date() for item in cached["closed_dates"]
            ),
            early_close_dates=frozenset(
                datetime.fromisoformat(item).date() for item in cached["early_close_dates"]
            ),
            source_url=cached["source_url"],
        )

    holidays = fetch_exchange_holidays(exchange_code, local_year)
    timescale_store.upsert_exchange_holidays(
        exchange=exchange_code,
        year=local_year,
        closed_dates=holidays.closed_dates,
        early_close_dates=holidays.early_close_dates,
        source_url=holidays.source_url,
    )
    return holidays
