import json
from datetime import date, datetime, timedelta
from pathlib import Path
from typing import Annotated

import pandas as pd
import typer
from rich.console import Console
from rich.table import Table

from trade_research.config import get_settings
from trade_research.data import (
    UpstoxHistoricalDataProvider,
    UpstoxInstrumentMasterProvider,
    UpstoxNiftyFuturesHistoryProvider,
    YahooFinanceMarketDataProvider,
    audit_daily_ohlcv,
    instrument_master_audit,
    map_liquid_universe_to_upstox,
    validate_ohlcv,
)
from trade_research.features import (
    FEATURE_VERSION_V1_0,
    DailyTechnicalFeatureBuilder,
    RangeFeatureBuilder,
    audit_daily_features,
    invalid_daily_ohlcv_mask,
    normalize_daily_ohlcv,
    write_feature_audit_outputs,
)
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


@app.command("fetch-nifty-futures-history")
def fetch_nifty_futures_history(
    from_date: Annotated[
        str | None,
        typer.Option(help="Start date. Defaults to --months before --to-date."),
    ] = None,
    to_date: Annotated[
        str | None,
        typer.Option(help="End date. Defaults to yesterday."),
    ] = None,
    months: Annotated[
        int,
        typer.Option(min=1, max=6, help="Lookback months when --from-date is omitted."),
    ] = 4,
    interval: Annotated[
        str,
        typer.Option(help="Upstox interval: 1minute, 3minute, 5minute, 15minute, 30minute, day."),
    ] = "1minute",
    active_instrument_key: Annotated[
        str | None,
        typer.Option(
            help=(
                "Optional current NIFTY futures instrument key from the Upstox BOD "
                "instruments file, e.g. NSE_FO|12345."
            ),
        ),
    ] = None,
    output_name: Annotated[
        str,
        typer.Option(help="Parquet path prefix under DATA_DIR."),
    ] = "nifty/futures_history",
    access_token: Annotated[
        str | None,
        typer.Option(help="Override UPSTOX_ACCESS_TOKEN for this run."),
    ] = None,
) -> None:
    settings = get_settings()
    token = access_token or settings.upstox_access_token
    if not token:
        raise typer.BadParameter("Set UPSTOX_ACCESS_TOKEN or pass --access-token.")

    end = _parse_cli_date(to_date, "--to-date") if to_date else date.today() - timedelta(days=1)
    start = (
        _parse_cli_date(from_date, "--from-date")
        if from_date
        else _subtract_months(end, months)
    )

    with UpstoxNiftyFuturesHistoryProvider(token) as provider:
        frame = provider.fetch_nifty50_futures_history(
            start=start,
            end=end,
            interval=interval,
            active_instrument_key=active_instrument_key,
        )

    if frame.empty:
        raise typer.Exit("No NIFTY futures data returned from Upstox.")

    path = ParquetStore(settings.data_dir).write_frame(output_name, frame)
    console.print(f"Wrote {len(frame)} NIFTY futures rows: {path}")
    console.print(f"Window: {start.isoformat()} to {end.isoformat()}")
    console.print(
        f"Contracts: {frame['TradingSymbol'].nunique()} | interval: {interval}"
    )


@app.command("fetch-upstox-instruments")
def fetch_upstox_instruments(
    output_name: Annotated[
        str,
        typer.Option(help="Processed Parquet path prefix under DATA_DIR."),
    ] = "processed/instruments/upstox_instruments",
    audit_output: Annotated[
        Path,
        typer.Option(help="Instrument audit CSV path."),
    ] = Path("data/processed/instruments/upstox_instruments_audit.csv"),
    store_db: Annotated[
        bool,
        typer.Option(help="Also upsert instruments into Timescale/Postgres."),
    ] = True,
) -> None:
    settings = get_settings()
    with UpstoxInstrumentMasterProvider() as provider:
        instruments = provider.fetch()

    audit = instrument_master_audit(instruments)
    store = ParquetStore(settings.data_dir)
    output_path = store.write_frame(output_name, instruments.drop(columns=["raw"]))
    audit_output.parent.mkdir(parents=True, exist_ok=True)
    pd.DataFrame([audit.__dict__]).to_csv(audit_output, index=False)

    rows_written = 0
    if store_db:
        db = TimescaleStore(settings.database_url)
        db.initialize()
        rows_written = db.upsert_provider_instruments(instruments, source="upstox")

    console.print(f"Wrote Upstox instruments: {output_path} ({len(instruments)} rows)")
    console.print(f"Wrote instrument audit: {audit_output}")
    if store_db:
        console.print(f"Upserted provider_instruments rows: {rows_written}")


@app.command("map-liquid-nse-upstox")
def map_liquid_nse_upstox(
    universe_csv: Annotated[
        Path,
        typer.Option(help="Liquid NSE universe CSV from Step 0."),
    ] = Path("data/processed/universe/liquid_nse_stocks.csv"),
    instruments_name: Annotated[
        str,
        typer.Option(help="Processed Upstox instruments Parquet prefix under DATA_DIR."),
    ] = "processed/instruments/upstox_instruments",
    mapping_output: Annotated[
        Path,
        typer.Option(help="Matched liquid-universe mapping CSV."),
    ] = Path("data/processed/universe/liquid_nse_upstox_mapping.csv"),
    unmatched_output: Annotated[
        Path,
        typer.Option(help="Unmatched liquid symbols CSV."),
    ] = Path("data/processed/universe/liquid_nse_upstox_unmatched.csv"),
    universe_id: Annotated[
        str,
        typer.Option(help="Canonical local universe id."),
    ] = "nse_liquid_adt_100cr",
    store_db: Annotated[
        bool,
        typer.Option(help="Also upsert universe metadata and members into Timescale/Postgres."),
    ] = True,
) -> None:
    settings = get_settings()
    liquid = pd.read_csv(universe_csv)
    instruments = ParquetStore(settings.data_dir).read_frame(instruments_name)
    matched, unmatched = map_liquid_universe_to_upstox(liquid, instruments)

    mapping_output.parent.mkdir(parents=True, exist_ok=True)
    unmatched_output.parent.mkdir(parents=True, exist_ok=True)
    matched.to_csv(mapping_output, index=False)
    unmatched.to_csv(unmatched_output, index=False)

    rows_written = 0
    if store_db:
        db = TimescaleStore(settings.database_url)
        db.initialize()
        rows_written = db.upsert_tradable_universe(
            universe_id=universe_id,
            name="NSE liquid equities ADT >= Rs 100 crore",
            exchange="NSE",
            source="yfinance_liquidity_plus_upstox_mapping",
            criteria={
                "avg_daily_turnover_min": 1_000_000_000,
                "turnover_currency": "INR",
                "lookback": "6 months",
                "zero_volume_ratio_max": 0.03,
            },
            members=matched,
            description=(
                "Step 1 universe mapped to Upstox instrument keys for batch "
                "historical ingestion."
            ),
        )

    if not unmatched.empty:
        console.print(f"[yellow]Unmatched liquid symbols: {len(unmatched)}[/yellow]")
    console.print(f"Wrote Upstox mapping: {mapping_output} ({len(matched)} rows)")
    console.print(f"Wrote unmatched symbols: {unmatched_output}")
    if store_db:
        console.print(f"Upserted tradable universe members: {rows_written}")


@app.command("fetch-upstox-nse-daily")
def fetch_upstox_nse_daily(
    mapping_csv: Annotated[
        Path,
        typer.Option(help="Matched liquid NSE Upstox mapping CSV."),
    ] = Path("data/processed/universe/liquid_nse_upstox_mapping.csv"),
    years: Annotated[
        int,
        typer.Option(min=1, max=10, help="Daily candle lookback in years."),
    ] = 2,
    from_date: Annotated[
        str | None,
        typer.Option(help="Optional start date in YYYY-MM-DD format."),
    ] = None,
    to_date: Annotated[
        str | None,
        typer.Option(help="Optional end date in YYYY-MM-DD format."),
    ] = None,
    settlement_lag_days: Annotated[
        int,
        typer.Option(
            min=1,
            max=7,
            help=(
                "Default calendar-day lag for completed daily candles when --to-date "
                "is omitted."
            ),
        ),
    ] = 2,
    limit: Annotated[
        int | None,
        typer.Option(help="Optional smoke-test symbol limit."),
    ] = None,
    throttle_seconds: Annotated[
        float,
        typer.Option(min=0, help="Pause between Upstox candle requests."),
    ] = 0.25,
    output_name: Annotated[
        str,
        typer.Option(help="Canonical full-refresh Parquet path prefix under DATA_DIR."),
    ] = "processed/equities/nse_daily_ohlcv_upstox",
    incremental_output_name: Annotated[
        str,
        typer.Option(help="Incremental-run Parquet path prefix under DATA_DIR."),
    ] = "processed/equities/nse_daily_ohlcv_upstox_incremental",
    audit_output: Annotated[
        Path,
        typer.Option(help="Daily OHLCV audit CSV path."),
    ] = Path("data/processed/equities/nse_daily_ohlcv_upstox_audit.csv"),
    failures_output: Annotated[
        Path,
        typer.Option(help="Per-symbol fetch failures CSV path."),
    ] = Path("data/processed/equities/nse_daily_ohlcv_upstox_failures.csv"),
    skipped_output: Annotated[
        Path,
        typer.Option(help="Per-symbol skipped/current CSV path for incremental runs."),
    ] = Path("data/processed/equities/nse_daily_ohlcv_upstox_skipped.csv"),
    access_token: Annotated[
        str | None,
        typer.Option(help="Override UPSTOX_ACCESS_TOKEN for this run."),
    ] = None,
    full_refresh: Annotated[
        bool,
        typer.Option(help="Fetch the full requested history instead of only missing dates."),
    ] = False,
    store_db: Annotated[
        bool,
        typer.Option(help="Also upsert daily candles and audits into Timescale/Postgres."),
    ] = True,
) -> None:
    settings = get_settings()
    token = access_token or settings.upstox_access_token
    if not token:
        raise typer.BadParameter("Set UPSTOX_ACCESS_TOKEN or pass --access-token.")

    end = (
        _parse_cli_date(to_date, "--to-date")
        if to_date
        else date.today() - timedelta(days=settlement_lag_days)
    )
    base_start = (
        _parse_cli_date(from_date, "--from-date") if from_date else _subtract_years(end, years)
    )
    is_full_window = full_refresh or from_date is not None
    mapping = pd.read_csv(mapping_csv)
    if limit:
        mapping = mapping.head(limit)

    run_id = None
    db = TimescaleStore(settings.database_url) if store_db else None
    if db is not None:
        db.initialize()
    latest_dates = (
        {}
        if db is None or full_refresh or from_date
        else db.latest_daily_ohlcv_dates(
            [str(key) for key in mapping["instrument_key"].dropna().tolist()],
            source="upstox",
        )
    )
    planned = _plan_daily_fetch_windows(
        mapping,
        base_start=base_start,
        end=end,
        latest_dates=latest_dates,
    )
    fetch_plan = planned[planned["should_fetch"]].copy()
    skipped_plan = planned[~planned["should_fetch"]].copy()

    skipped_output.parent.mkdir(parents=True, exist_ok=True)
    skipped_plan.to_csv(skipped_output, index=False)

    if db is not None:
        run_id = db.start_ingestion_run(
            job_name="upstox_nse_daily_ohlcv",
            exchange="NSE",
            source="upstox",
            items_requested=len(fetch_plan),
            run_metadata={
                "trigger": "cli",
                "mode": "full_refresh" if full_refresh or from_date else "incremental",
                "base_start": base_start.isoformat(),
                "end": end.isoformat(),
                "settlement_lag_days": settlement_lag_days if to_date is None else None,
                "mapped_symbols": len(mapping),
                "skipped_current_symbols": len(skipped_plan),
            },
        )

    frames: list[pd.DataFrame] = []
    failures: list[dict[str, str]] = []
    try:
        with UpstoxHistoricalDataProvider(token) as provider:
            for row in fetch_plan.to_dict(orient="records"):
                try:
                    frame = provider.fetch_daily_candles(
                        instrument_key=str(row["instrument_key"]),
                        start=_parse_cli_date(str(row["fetch_start"]), "fetch_start"),
                        end=end,
                        symbol=str(row["symbol"]),
                        trading_symbol=str(row.get("trading_symbol") or row["symbol"]),
                    )
                    if not frame.empty:
                        frames.append(frame)
                except Exception as exc:
                    failures.append(
                        {
                            "symbol": str(row["symbol"]),
                            "instrument_key": str(row["instrument_key"]),
                            "error": str(exc),
                        }
                    )
                if throttle_seconds:
                    import time

                    time.sleep(throttle_seconds)

        ohlcv = pd.concat(frames, ignore_index=True) if frames else pd.DataFrame()
        audit = (
            audit_daily_ohlcv(ohlcv, fetch_plan)
            if not fetch_plan.empty
            else pd.DataFrame(
                columns=[
                    "symbol",
                    "instrument_key",
                    "rows",
                    "start_date",
                    "end_date",
                    "missing_dates",
                    "null_ohlcv_rows",
                    "duplicate_date_rows",
                    "zero_volume_rows",
                    "zero_or_negative_close_rows",
                    "status",
                ]
            )
        )

        store = ParquetStore(settings.data_dir)
        output_path = None
        if not ohlcv.empty:
            output_path = store.write_frame(
                output_name if is_full_window else incremental_output_name,
                ohlcv,
            )
        audit_output.parent.mkdir(parents=True, exist_ok=True)
        failures_output.parent.mkdir(parents=True, exist_ok=True)
        audit.to_csv(audit_output, index=False)
        pd.DataFrame(
            failures,
            columns=["symbol", "instrument_key", "error"],
        ).to_csv(failures_output, index=False)

        rows_written = db.upsert_daily_ohlcv(ohlcv) if db is not None and not ohlcv.empty else 0
        audits_written = (
            db.insert_data_quality_audits(
                audit,
                dataset_name="nse_daily_ohlcv",
                source="upstox",
                interval="1d",
            )
            if db is not None and not audit.empty
            else 0
        )
        if db is not None and run_id is not None:
            succeeded = (
                int(audit["status"].isin(["passed", "warning"]).sum())
                if not audit.empty
                else 0
            )
            db.finish_ingestion_run(
                run_id,
                status="completed" if rows_written else "completed_empty",
                items_processed=len(fetch_plan),
                items_succeeded=succeeded,
                items_failed=len(fetch_plan) - succeeded,
            )
    except Exception as exc:
        if db is not None and run_id is not None:
            db.finish_ingestion_run(
                run_id,
                status="failed",
                items_processed=0,
                items_succeeded=0,
                items_failed=len(fetch_plan),
                error_message=str(exc),
            )
        raise

    if output_path is None:
        console.print("No new Upstox NSE daily OHLCV rows fetched; existing Parquet left unchanged")
    else:
        console.print(f"Wrote Upstox NSE daily OHLCV: {output_path} ({len(ohlcv)} rows)")
    console.print(f"Wrote daily audit: {audit_output}")
    console.print(f"Wrote fetch failures: {failures_output} ({len(failures)} rows)")
    console.print(f"Wrote skipped/current symbols: {skipped_output} ({len(skipped_plan)} rows)")
    if db is not None:
        console.print(f"Upserted ohlcv_daily rows: {rows_written}")
        console.print(f"Inserted data_quality_audits rows: {audits_written}")


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


def _plan_daily_fetch_windows(
    mapping: pd.DataFrame,
    base_start: date,
    end: date,
    latest_dates: dict[str, date],
) -> pd.DataFrame:
    rows = []
    for record in mapping.to_dict(orient="records"):
        instrument_key = str(record["instrument_key"])
        latest_date = latest_dates.get(instrument_key)
        fetch_start = base_start
        if latest_date is not None:
            fetch_start = max(base_start, latest_date + timedelta(days=1))
        should_fetch = fetch_start <= end
        rows.append(
            {
                **record,
                "latest_stored_date": latest_date.isoformat() if latest_date else None,
                "fetch_start": fetch_start.isoformat(),
                "fetch_end": end.isoformat(),
                "should_fetch": should_fetch,
                "skip_reason": "" if should_fetch else "already_current",
            }
        )
    return pd.DataFrame(rows)


def _subtract_months(value: date, months: int) -> date:
    year = value.year
    month = value.month - months
    while month <= 0:
        month += 12
        year -= 1

    import calendar

    day = min(value.day, calendar.monthrange(year, month)[1])
    return date(year, month, day)


def _subtract_years(value: date, years: int) -> date:
    try:
        return value.replace(year=value.year - years)
    except ValueError:
        return value.replace(year=value.year - years, day=28)


def _parse_cli_date(value: str, option_name: str) -> date:
    try:
        return date.fromisoformat(value)
    except ValueError as exc:
        raise typer.BadParameter(
            f"{option_name} must use YYYY-MM-DD format, got {value!r}."
        ) from exc


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


@app.command("build-daily-features")
def build_daily_features(
    input_source: Annotated[
        str,
        typer.Option(help="Input source: parquet or timescale."),
    ] = "parquet",
    input_name: Annotated[
        str,
        typer.Option(help="Input Parquet path prefix under DATA_DIR."),
    ] = "processed/equities/nse_daily_ohlcv_upstox",
    output_name: Annotated[
        str,
        typer.Option(help="Feature Parquet path prefix under DATA_DIR."),
    ] = "processed/features/daily_v1_ohlcv_technical",
    feature_version: Annotated[
        str,
        typer.Option(help="Feature version stored in the output rows."),
    ] = FEATURE_VERSION_V1_0,
    audit_output: Annotated[
        Path,
        typer.Option(help="Feature audit CSV path."),
    ] = Path("data/processed/features/daily_v1_ohlcv_technical_audit.csv"),
    summary_output: Annotated[
        Path,
        typer.Option(help="Feature summary JSON path."),
    ] = Path("data/processed/features/daily_v1_ohlcv_technical_summary.json"),
    limit: Annotated[
        int | None,
        typer.Option(help="Optional symbol limit for smoke tests."),
    ] = None,
    strict_invalid_rows: Annotated[
        bool,
        typer.Option(help="Fail instead of excluding invalid OHLCV rows before feature build."),
    ] = False,
) -> None:
    settings = get_settings()
    normalized_source = input_source.lower()
    if normalized_source not in {"parquet", "timescale"}:
        raise typer.BadParameter("--input-source must be parquet or timescale.")

    if normalized_source == "parquet":
        source_frame = ParquetStore(settings.data_dir).read_frame(input_name)
        source_frame = _limit_daily_feature_symbols(source_frame, limit)
    else:
        db = TimescaleStore(settings.database_url)
        source_frame = db.daily_ohlcv_frame(limit=limit)

    if source_frame.empty:
        raise typer.Exit("No daily OHLCV rows found for feature generation.")

    invalid_ohlcv_count = int(invalid_daily_ohlcv_mask(normalize_daily_ohlcv(source_frame)).sum())
    features = DailyTechnicalFeatureBuilder(
        feature_version=feature_version,
        drop_invalid_rows=not strict_invalid_rows,
    ).build(source_frame)
    audit, summary = audit_daily_features(
        features,
        feature_version=feature_version,
        invalid_ohlcv_count=invalid_ohlcv_count,
    )
    output_path = ParquetStore(settings.data_dir).write_frame(output_name, features)
    write_feature_audit_outputs(audit, summary, audit_output, summary_output)

    console.print(f"Wrote daily features: {output_path} ({len(features)} rows)")
    console.print(f"Wrote feature audit: {audit_output}")
    console.print(f"Wrote feature summary: {summary_output}")
    if invalid_ohlcv_count:
        console.print(f"[yellow]Excluded invalid OHLCV rows: {invalid_ohlcv_count}[/yellow]")
    console.print(json.dumps(summary.__dict__, indent=2))


def _limit_daily_feature_symbols(frame: pd.DataFrame, limit: int | None) -> pd.DataFrame:
    if limit is None:
        return frame
    key_column = "InstrumentKey" if "InstrumentKey" in frame.columns else "instrument_key"
    if key_column not in frame.columns:
        return frame.head(0)
    keys = sorted(frame[key_column].dropna().astype(str).unique())[:limit]
    return frame[frame[key_column].astype(str).isin(keys)].copy()


if __name__ == "__main__":
    app()
