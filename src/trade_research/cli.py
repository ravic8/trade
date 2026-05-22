from datetime import date, datetime, timedelta
from pathlib import Path
from typing import Annotated

import pandas as pd
import typer
from rich.console import Console
from rich.table import Table

from trade_research.config import get_settings
from trade_research.data import YahooFinanceMarketDataProvider, validate_ohlcv
from trade_research.features import RangeFeatureBuilder
from trade_research.market_calendar import session_decision
from trade_research.screeners import IntradayRangeScreener
from trade_research.storage import ParquetStore, TimescaleStore
from trade_research.universe import NSEUniverseProvider, TSXUniverseProvider

app = typer.Typer(help="Market research agent CLI.")
console = Console()


def _universe_provider(exchange: str):
    normalized = exchange.upper()
    if normalized == "NSE":
        return NSEUniverseProvider()
    if normalized == "TSX":
        return TSXUniverseProvider()
    raise typer.BadParameter("exchange must be NSE or TSX")


@app.command("universe")
def universe(
    exchange: Annotated[str, typer.Argument()],
    limit: Annotated[int | None, typer.Option()] = None,
) -> None:
    provider = _universe_provider(exchange)
    symbols = provider.fetch()
    if limit:
        symbols = symbols[:limit]

    table = Table(title=f"{provider.exchange} Universe")
    table.add_column("Symbol")
    table.add_column("Yahoo")
    table.add_column("Name")
    for item in symbols:
        table.add_row(item.symbol, item.yahoo_symbol or "", item.name or "")
    console.print(table)
    console.print(f"{len(symbols)} symbols")


@app.command("market-session")
def market_session(
    exchange: Annotated[str, typer.Argument(help="Exchange to inspect: NSE or TSX.")],
) -> None:
    decision = session_decision(exchange)
    table = Table(title=f"{decision.exchange} Market Session")
    table.add_column("Field")
    table.add_column("Value")
    table.add_row("Local time", decision.local_time.isoformat())
    table.add_row("Trading day", str(decision.is_trading_day))
    table.add_row("Should fetch", str(decision.should_fetch))
    table.add_row("Reason", decision.reason)
    table.add_row("Market open", decision.market_open.isoformat() if decision.market_open else "")
    table.add_row(
        "Market close",
        decision.market_close.isoformat() if decision.market_close else "",
    )
    table.add_row("Calendar source", decision.source_url)
    console.print(table)


@app.command("init-db")
def init_db() -> None:
    settings = get_settings()
    TimescaleStore(settings.database_url).initialize()
    console.print("Initialized TimescaleDB schema")


@app.command("feed-health")
def feed_health(
    exchange: Annotated[str | None, typer.Argument(help="Optional exchange: NSE or TSX.")] = None,
) -> None:
    settings = get_settings()
    rows = TimescaleStore(settings.database_url).feed_health_summary(exchange)
    table = Table(title="Feed Health")
    table.add_column("Exchange")
    table.add_column("Source")
    table.add_column("Status")
    table.add_column("Symbols", justify="right")
    for row in rows:
        table.add_row(
            str(row["exchange"]),
            str(row["source"]),
            str(row["status"]),
            str(row["symbols"]),
        )
    console.print(table)


@app.command("ingest-nse-hourly")
def ingest_nse_hourly(
    limit: Annotated[
        int | None,
        typer.Option(help="Override NSE_INGEST_LIMIT for a one-off smoke run."),
    ] = None,
) -> None:
    ingest_hourly("NSE", limit)


@app.command("ingest-hourly")
def ingest_hourly(
    exchange: Annotated[str, typer.Argument(help="Exchange to ingest: NSE or TSX.")],
    limit: Annotated[
        int | None,
        typer.Option(help="Override the configured exchange ingest limit for this run."),
    ] = None,
    lookback_days: Annotated[
        int | None,
        typer.Option(
            min=1,
            max=60,
            help=(
                "Hourly Yahoo lookback for this run. Defaults to the realtime window; "
                "use a larger value such as 10 for an explicit historical refresh."
            ),
        ),
    ] = None,
) -> None:
    settings = get_settings()
    store = TimescaleStore(settings.database_url)
    store.initialize()

    provider = _universe_provider(exchange)
    exchange_code = provider.exchange
    symbols = provider.fetch()
    configured_limit = (
        settings.nse_ingest_limit if exchange_code == "NSE" else settings.tsx_ingest_limit
    )
    fetch_lookback_days = lookback_days or settings.hourly_realtime_lookback_days
    symbol_limit = limit or configured_limit
    stored_symbols = store.upsert_symbols(symbols)
    candidate_symbols = symbols[:symbol_limit] if symbol_limit else symbols
    selected_symbols = store.fetchable_symbols(
        candidate_symbols,
        source="yahoo",
        limit=symbol_limit,
    )
    tickers = [symbol.yahoo_symbol for symbol in selected_symbols if symbol.yahoo_symbol]
    run_id = store.start_ingestion_run(
        job_name=f"{exchange_code.lower()}_hourly_ohlcv",
        exchange=exchange_code,
        source="yahoo",
        items_requested=len(tickers),
        run_metadata={
            "trigger": "cli",
            "fetch_mode": "historical_refresh"
            if fetch_lookback_days > settings.hourly_realtime_lookback_days
            else "realtime",
            "lookback_days": fetch_lookback_days,
            "candidate_symbols": len(candidate_symbols),
            "skipped_by_feed_health": len(candidate_symbols) - len(selected_symbols),
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
        ohlcv = provider.fetch_hourly_ohlcv(tickers, period=f"{fetch_lookback_days}d")
        rows_written = store.upsert_hourly_ohlcv(
            ohlcv,
            exchange=exchange_code,
            source="yahoo",
        )
        successful_latest_candles = _successful_latest_candles(ohlcv)
        feed_health_result = store.update_feed_health(
            selected_symbols,
            successful_latest_candles=successful_latest_candles,
            source="yahoo",
            failure_threshold=settings.feed_health_failure_threshold,
            max_backoff_hours=settings.feed_health_max_backoff_hours,
            unsupported_retry_days=settings.feed_health_unsupported_retry_days,
        )
        tickers_with_data = len(successful_latest_candles)
        store.finish_ingestion_run(
            run_id,
            status="completed" if rows_written else "completed_empty",
            items_processed=len(tickers),
            items_succeeded=tickers_with_data,
            items_failed=max(len(tickers) - tickers_with_data, 0),
        )
    except Exception as exc:
        store.finish_ingestion_run(
            run_id,
            status="failed",
            items_processed=0,
            items_succeeded=0,
            items_failed=len(tickers),
            error_message=str(exc),
        )
        raise

    console.print(f"Stored {stored_symbols} {exchange_code} symbols")
    console.print(
        f"Selected {len(selected_symbols)}/{len(candidate_symbols)} candidates "
        f"after feed-health filtering"
    )
    console.print(f"Fetched hourly candles for {tickers_with_data}/{len(tickers)} tickers")
    console.print(
        f"Feed health updated: {feed_health_result['succeeded']} succeeded, "
        f"{feed_health_result['failed']} failed"
    )
    console.print(f"Upserted {rows_written} hourly rows")


@app.command("backfill-hourly")
def backfill_hourly(
    exchange: Annotated[
        str,
        typer.Argument(help="Exchange to refresh historically: NSE or TSX."),
    ],
    limit: Annotated[
        int | None,
        typer.Option(help="Override the configured exchange ingest limit for this run."),
    ] = None,
    lookback_days: Annotated[
        int | None,
        typer.Option(
            min=1,
            max=60,
            help="Override HOURLY_HISTORY_LOOKBACK_DAYS for this historical refresh.",
        ),
    ] = None,
) -> None:
    history_days = lookback_days or get_settings().hourly_history_lookback_days
    ingest_hourly(exchange=exchange, limit=limit, lookback_days=history_days)


@app.command("run-screener")
def run_screener(
    exchange: Annotated[str, typer.Argument()],
    days: Annotated[int, typer.Option(help="Calendar days of historical data to fetch.")] = 800,
    limit: Annotated[int | None, typer.Option(help="Limit symbols for smoke tests.")] = None,
    output_prefix: Annotated[
        str | None,
        typer.Option(help="Output prefix under DATA_DIR."),
    ] = None,
) -> None:
    settings = get_settings()
    output = output_prefix or exchange.lower()
    store = ParquetStore(settings.data_dir)

    provider = _universe_provider(exchange)
    symbols = provider.fetch()
    if limit:
        symbols = symbols[:limit]

    tickers = [item.yahoo_symbol for item in symbols if item.yahoo_symbol]
    end = date.today()
    start = end - timedelta(days=days)

    market_data = YahooFinanceMarketDataProvider(
        batch_size=settings.yfinance_batch_size,
        throttle_seconds=settings.yfinance_throttle_seconds,
        max_workers=settings.yfinance_max_workers,
        retry_attempts=settings.yfinance_retry_attempts,
        retry_base_seconds=settings.yfinance_retry_base_seconds,
        jitter_seconds=settings.yfinance_jitter_seconds,
    )
    ohlcv = market_data.fetch_daily_ohlcv(tickers, start=start, end=end)
    if ohlcv.empty:
        raise typer.Exit("No market data returned")

    quality_reports = validate_ohlcv(ohlcv)
    feature_df = RangeFeatureBuilder(
        min_median_dollar_volume=settings.min_median_dollar_volume
    ).build(ohlcv)
    screened_df = IntradayRangeScreener().run(feature_df)

    ohlcv_path = store.write_frame(f"{output}/ohlcv", ohlcv)
    features_path = store.write_frame(f"{output}/features", feature_df)
    screened_path = store.write_frame(f"{output}/screened_intraday_range_v1", screened_df)

    quality_df = pd.DataFrame([item.model_dump() for item in quality_reports])
    quality_path = store.write_frame(f"{output}/quality", quality_df)

    console.print(f"Wrote OHLCV: {ohlcv_path}")
    console.print(f"Wrote features: {features_path}")
    console.print(f"Wrote screened results: {screened_path}")
    console.print(f"Wrote quality reports: {quality_path}")
    console.print(f"Matched {len(screened_df)} symbols")


def _successful_latest_candles(frame: pd.DataFrame) -> dict[str, datetime]:
    if frame.empty:
        return {}
    latest = frame.groupby("Ticker")["Datetime"].max()
    return {
        str(ticker).upper(): timestamp.to_pydatetime()
        if hasattr(timestamp, "to_pydatetime")
        else datetime.fromisoformat(str(timestamp))
        for ticker, timestamp in latest.items()
    }


@app.command("features-from-parquet")
def features_from_parquet(
    input_path: Annotated[Path, typer.Argument()],
    output_path: Annotated[Path, typer.Argument()],
) -> None:
    ohlcv = pd.read_parquet(input_path)
    feature_df = RangeFeatureBuilder().build(ohlcv)
    output_path.parent.mkdir(parents=True, exist_ok=True)
    feature_df.to_parquet(output_path, index=False)
    console.print(f"Wrote {output_path}")


if __name__ == "__main__":
    app()
