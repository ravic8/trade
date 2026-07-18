import json
from datetime import date, timedelta
from pathlib import Path
from typing import Annotated

import pandas as pd
import typer
from rich.console import Console
from rich.table import Table
from sqlalchemy.exc import SQLAlchemyError

from trade_research.config import get_settings
from trade_research.data import (
    UpstoxInstrumentMasterProvider,
    UpstoxNiftyFuturesHistoryProvider,
    instrument_master_audit,
    map_liquid_universe_to_upstox,
)
from trade_research.features import FEATURE_VERSION_V1_0
from trade_research.market_calendar import session_decision
from trade_research.modeling.backtest import BacktestConfig
from trade_research.modeling.baselines import BaselineRunConfig
from trade_research.modeling.latest_predictions import LatestPredictionConfig
from trade_research.modeling.lightgbm_models import LightGBMRunConfig
from trade_research.modeling.walk_forward import WalkForwardManifestConfig
from trade_research.pipelines import (
    PipelineRunResult,
    run_baseline_predictions_v1_pipeline,
    run_daily_feature_pipeline,
    run_daily_pipeline_health_pipeline,
    run_daily_target_pipeline,
    run_dukascopy_intraday_ohlcv_pipeline,
    run_equity_universe_snapshot_pipeline,
    run_exchange_session_materialization_pipeline,
    run_factor_research_pipeline,
    run_latest_predictions_v1_pipeline,
    run_lightgbm_predictions_v1_pipeline,
    run_ml_dataset_v1_pipeline,
    run_nse_yfinance_cutover_readiness,
    run_prediction_backtest_v1_pipeline,
    run_processed_dataset_validation_pipeline,
    run_upstox_daily_ohlcv_pipeline,
    run_upstox_daily_ohlcv_retry_pipeline,
    run_walk_forward_folds_v1_pipeline,
    run_yfinance_daily_ohlcv_pipeline,
    run_yfinance_daily_work_planner,
    run_yfinance_daily_work_queue,
    run_yfinance_intraday_ohlcv_pipeline,
    run_yfinance_missing_ohlcv_pipeline,
    run_yfinance_nse_canary_planner,
    run_yfinance_provider_history_evidence_bootstrap,
    run_yfinance_tsx_canary_planner,
)
from trade_research.storage import ParquetStore, TimescaleStore
from trade_research.targets import (
    DAILY_FORWARD_TARGET_VERSION_V1_0,
)
from trade_research.universe import (
    NSEUniverseProvider,
    TSXUniverseProvider,
    YFinanceCanadaUniverseProvider,
    YFinanceUSUniverseProvider,
)

app = typer.Typer(help="Market research agent CLI.")
console = Console()


def _universe_provider(exchange: str):
    normalized = exchange.upper()
    if normalized == "NSE":
        return NSEUniverseProvider()
    if normalized == "TSX":
        return TSXUniverseProvider()
    if normalized == "US":
        return YFinanceUSUniverseProvider()
    if normalized == "CA":
        return YFinanceCanadaUniverseProvider()
    raise typer.BadParameter("exchange must be NSE, TSX, US, or CA")


@app.command("universe")
def universe(
    exchange: Annotated[str, typer.Argument(help="Exchange to inspect: NSE, TSX, US, or CA.")],
    limit: Annotated[int | None, typer.Option()] = None,
) -> None:
    provider = _universe_provider(exchange)
    symbols = provider.fetch()
    if limit:
        symbols = symbols[:limit]

    table = Table(title=f"{provider.exchange} Universe")
    table.add_column("Symbol")
    table.add_column("Provider Symbol")
    table.add_column("Name")
    for item in symbols:
        table.add_row(item.symbol, item.yahoo_symbol or "", item.name or "")
    console.print(table)
    console.print(f"{len(symbols)} symbols")


@app.command("refresh-equity-universe")
def refresh_equity_universe(
    exchange: Annotated[
        str,
        typer.Argument(help="Canonical equity exchange: NSE, TSX, or US."),
    ],
    allow_large_change: Annotated[
        bool,
        typer.Option(
            help=(
                "Accept a source count change above the configured safety threshold. "
                "Schema, duplicate, minimum-count, and mapping checks still apply."
            )
        ),
    ] = False,
) -> None:
    result = run_equity_universe_snapshot_pipeline(
        exchange,
        allow_large_change=allow_large_change,
        trigger="cli",
    )
    table = Table(title=f"{str(result.metrics['exchange'])} Universe Snapshot")
    table.add_column("Field")
    table.add_column("Value")
    table.add_row("Snapshot", str(result.metrics["snapshot_id"]))
    table.add_row("Status", str(result.metrics["snapshot_status"]))
    table.add_row("Source", str(result.metrics["source"]))
    table.add_row("Symbols", str(result.metrics["symbol_count"]))
    source_diagnostics = result.metrics.get("source_diagnostics") or {}
    if source_diagnostics:
        table.add_row("Source rows", str(source_diagnostics.get("source_rows", "")))
        table.add_row("TSX rows", str(source_diagnostics.get("tsx_rows", "")))
        table.add_row(
            "Excluded TSXV",
            str(source_diagnostics.get("excluded_tsxv_rows", "")),
        )
        table.add_row(
            "Excluded Cboe Canada",
            str(source_diagnostics.get("excluded_cboe_canada_rows", "")),
        )
        if source_diagnostics.get("reconciliation_version"):
            table.add_row(
                "Official issuer rows",
                str(source_diagnostics.get("official_rows", "")),
            )
            table.add_row(
                "Pipeline eligible",
                str(source_diagnostics.get("eligible_symbols", "")),
            )
            table.add_row(
                "Policy excluded",
                str(source_diagnostics.get("excluded_symbols", "")),
            )
            table.add_row(
                "Official provider-unmapped",
                str(source_diagnostics.get("provider_unmapped_official_issuers", "")),
            )
    table.add_row("Lifecycle events", str(result.metrics["events_written"]))
    table.add_row("Backfills queued", str(result.metrics["work_items_queued"]))
    table.add_row(
        "Backfill planning enabled",
        str(result.metrics["backfill_planning_enabled"]),
    )
    table.add_row(
        "Backfill execution enabled",
        str(result.metrics["backfill_execution_enabled"]),
    )
    console.print(table)
    for warning in result.warnings:
        console.print(f"[yellow]Warning: {warning}[/yellow]")
    if result.status != "pass":
        for issue in result.blocking_issues:
            console.print(f"[red]Blocked: {issue}[/red]")
        raise typer.Exit(code=1)


@app.command("tsx-reconciliation-status")
def tsx_reconciliation_status() -> None:
    settings = get_settings()
    summary = TimescaleStore(settings.database_url).universe_reconciliation_summary("TSX")
    snapshot = summary["snapshot"]
    if snapshot is None:
        raise typer.Exit("No accepted TSX universe snapshot is available.")

    console.print(
        f"Snapshot: {snapshot['snapshot_id']} | fetched={snapshot['fetched_at'].isoformat()} | "
        f"symbols={snapshot['symbol_count']}"
    )
    diagnostics = dict(snapshot.get("validation_json") or {}).get("source_diagnostics") or {}
    console.print(
        "Official reconciliation: "
        f"{diagnostics.get('official_rows', 0)} issuer rows, "
        f"{diagnostics.get('eligible_symbols', 0)} eligible symbols, "
        f"{diagnostics.get('provider_unmapped_official_issuers', 0)} provider-unmapped issuers"
    )

    table = Table(title="TSX Reconciliation Outcomes")
    table.add_column("Status")
    table.add_column("Instrument type")
    table.add_column("Eligibility")
    table.add_column("Reason")
    table.add_column("Symbols", justify="right")
    for row in summary["groups"]:
        table.add_row(
            str(row["reconciliation_status"]),
            str(row["instrument_type"]),
            str(row["pipeline_eligibility"]),
            str(row.get("reconciliation_reason") or ""),
            str(row["symbols"]),
        )
    console.print(table)


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


@app.command("refresh-exchange-sessions")
def refresh_exchange_sessions(
    exchange: Annotated[
        str,
        typer.Argument(help="Canonical equity exchange: NSE, TSX, or US."),
    ],
    as_of_date: Annotated[
        str | None,
        typer.Option(help="Reference date in YYYY-MM-DD format. Defaults to today."),
    ] = None,
) -> None:
    result = run_exchange_session_materialization_pipeline(
        exchange,
        as_of_date=(_parse_cli_date(as_of_date, "--as-of-date") if as_of_date else None),
        trigger="cli",
    )
    table = Table(title=f"{str(result.metrics['exchange'])} Materialized Sessions")
    table.add_column("Field")
    table.add_column("Value")
    table.add_row("Status", result.status)
    table.add_row("Window", f"{result.metrics['start_date']} to {result.metrics['end_date']}")
    table.add_row("Rows", str(result.metrics["session_rows"]))
    table.add_row("Trading days", str(result.metrics["trading_days"]))
    table.add_row("Closed days", str(result.metrics["closed_days"]))
    table.add_row("Early closes", str(result.metrics["early_closes"]))
    table.add_row(
        "Shadow discrepancies",
        str(result.metrics["shadow_discrepancy_count"]),
    )
    table.add_row("Planning enabled", str(result.metrics["planning_enabled"]))
    console.print(table)
    for warning in result.warnings:
        console.print(f"[yellow]Warning: {warning}[/yellow]")
    if result.status == "fail":
        for issue in result.blocking_issues:
            console.print(f"[red]Blocked: {issue}[/red]")
        raise typer.Exit(code=1)


@app.command("init-db")
def init_db() -> None:
    settings = get_settings()
    TimescaleStore(settings.database_url).initialize()
    console.print("Initialized TimescaleDB schema")


@app.command("provider-request-log")
def provider_request_log(
    run_id: Annotated[
        str | None,
        typer.Option(help="Ingestion run id to inspect. Defaults to the latest matching run."),
    ] = None,
    provider: Annotated[
        str,
        typer.Option(help="Provider/source filter."),
    ] = "upstox",
    exchange: Annotated[
        str,
        typer.Option(help="Exchange filter when resolving the latest run."),
    ] = "NSE",
    endpoint_group: Annotated[
        str | None,
        typer.Option(help="Optional endpoint group filter, for example historical."),
    ] = None,
    limit: Annotated[
        int,
        typer.Option(min=1, max=500, help="Recent request rows to show."),
    ] = 20,
) -> None:
    settings = get_settings()
    db = TimescaleStore(settings.database_url)
    selected_run_id = run_id
    if selected_run_id is None:
        runs = db.latest_runs(limit=1, source=provider, exchange=exchange)
        if not runs:
            raise typer.Exit(
                f"No ingestion runs found for provider={provider} exchange={exchange}."
            )
        selected_run_id = str(runs[0]["run_id"])

    run = db.ingestion_run(selected_run_id)
    if run is None:
        raise typer.Exit(f"Ingestion run not found: {selected_run_id}")

    summary = db.provider_request_log_summary(
        selected_run_id,
        provider=provider,
        endpoint_group=endpoint_group,
    )
    logs = db.provider_request_logs_for_run(
        selected_run_id,
        provider=provider,
        endpoint_group=endpoint_group,
        limit=limit,
    )

    console.print(f"Run: {selected_run_id}")
    console.print(
        "Job: "
        f"{run['job_name']} | status={run['status']} | "
        f"source={run['source']} | exchange={run['exchange']}"
    )

    summary_table = Table(title="Provider Request Summary")
    summary_table.add_column("Provider")
    summary_table.add_column("Endpoint")
    summary_table.add_column("Status")
    summary_table.add_column("Requests", justify="right")
    summary_table.add_column("Rate limited", justify="right")
    summary_table.add_column("Wait seconds", justify="right")
    summary_table.add_column("Avg ms", justify="right")
    for row in summary:
        summary_table.add_row(
            str(row["provider"]),
            str(row["endpoint_group"]),
            str(row["status"]),
            str(int(row["requests"] or 0)),
            str(int(row["rate_limited_requests"] or 0)),
            f"{float(row['wait_seconds'] or 0.0):.3f}",
            f"{float(row['avg_duration_ms'] or 0.0):.1f}",
        )
    console.print(summary_table)

    recent_table = Table(title=f"Recent Provider Requests (latest {limit})")
    recent_table.add_column("Created")
    recent_table.add_column("Status")
    recent_table.add_column("Symbol")
    recent_table.add_column("Window")
    recent_table.add_column("Wait", justify="right")
    recent_table.add_column("Ms", justify="right")
    recent_table.add_column("Error")
    for row in logs:
        recent_table.add_row(
            row["created_at"].isoformat() if row.get("created_at") else "",
            str(row["status"]),
            str(row.get("symbol") or ""),
            f"{row.get('window_start') or ''} -> {row.get('window_end') or ''}",
            f"{float(row.get('wait_seconds') or 0.0):.3f}",
            f"{float(row.get('duration_ms') or 0.0):.1f}",
            str(row.get("error_message") or "")[:80],
        )
    console.print(recent_table)
    if not summary:
        console.print("[yellow]No provider request logs found for this run.[/yellow]")


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
        _parse_cli_date(from_date, "--from-date") if from_date else _subtract_months(end, months)
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
    console.print(f"Contracts: {frame['TradingSymbol'].nunique()} | interval: {interval}")


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
            source="liquidity_plus_upstox_mapping",
            criteria={
                "avg_daily_turnover_min": 1_000_000_000,
                "turnover_currency": "INR",
                "lookback": "6 months",
                "zero_volume_ratio_max": 0.03,
            },
            members=matched,
            description=(
                "Step 1 universe mapped to Upstox instrument keys for batch historical ingestion."
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
                "Default calendar-day lag for completed daily candles when --to-date is omitted."
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
    max_concurrent_fetches: Annotated[
        int | None,
        typer.Option(
            min=1,
            max=32,
            help="Override UPSTOX_HISTORICAL_CONCURRENCY for this run.",
        ),
    ] = None,
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
    fetch_coverage_output: Annotated[
        Path,
        typer.Option(help="Per-run fetch coverage CSV path for retry planning."),
    ] = Path("data/processed/equities/nse_daily_ohlcv_upstox_fetch_coverage.csv"),
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
    try:
        result = run_upstox_daily_ohlcv_pipeline(
            mapping_csv=mapping_csv,
            years=years,
            from_date=from_date,
            to_date=to_date,
            settlement_lag_days=settlement_lag_days,
            limit=limit,
            throttle_seconds=throttle_seconds,
            max_concurrent_fetches=max_concurrent_fetches,
            output_name=output_name,
            incremental_output_name=incremental_output_name,
            audit_output=audit_output,
            failures_output=failures_output,
            skipped_output=skipped_output,
            fetch_coverage_output=fetch_coverage_output,
            access_token=access_token,
            full_refresh=full_refresh,
            store_db=store_db,
            trigger="cli",
        )
    except ValueError as exc:
        raise typer.BadParameter(str(exc)) from exc
    output_path = result.artifacts.get("ohlcv")
    if output_path is None:
        console.print("No new Upstox NSE daily OHLCV rows fetched; existing Parquet left unchanged")
    else:
        console.print(
            f"Wrote Upstox NSE daily OHLCV: {output_path} "
            f"({result.metrics['db_snapshot_rows'] or result.rows} rows)"
        )
    console.print(f"Wrote daily audit: {audit_output}")
    console.print(
        f"Wrote fetch failures: {failures_output} ({result.metrics['failure_rows']} rows)"
    )
    console.print(
        f"Wrote skipped/current symbols: "
        f"{skipped_output} ({result.metrics['skipped_current_symbols']} rows)"
    )
    console.print(
        f"Wrote fetch coverage: "
        f"{fetch_coverage_output} ({result.metrics['fetch_coverage_rows']} rows)"
    )
    console.print(f"Upstox historical concurrency: {result.metrics['max_concurrent_fetches']}")
    if store_db:
        console.print(f"Upserted ohlcv_daily rows: {result.metrics['timescale_rows']}")
        console.print(
            f"Inserted data_quality_audits rows: {result.metrics['timescale_audit_rows']}"
        )
        console.print(f"Exported DB snapshot rows: {result.metrics['db_snapshot_rows']}")
        console.print(
            f"Stored fetch coverage rows: {result.metrics['timescale_fetch_coverage_rows']}"
        )


@app.command("retry-upstox-nse-daily")
def retry_upstox_nse_daily(
    coverage_run_id: Annotated[
        str | None,
        typer.Option(help="Coverage run id to retry. Defaults to latest fetch coverage run."),
    ] = None,
    statuses: Annotated[
        str,
        typer.Option(help="Comma-separated fetch statuses to retry."),
    ] = "failed,no_rows",
    limit: Annotated[
        int | None,
        typer.Option(help="Optional retry candidate limit for smoke tests."),
    ] = None,
    throttle_seconds: Annotated[
        float,
        typer.Option(min=0, help="Pause between Upstox retry requests."),
    ] = 0.25,
    max_concurrent_fetches: Annotated[
        int | None,
        typer.Option(
            min=1,
            max=32,
            help="Override UPSTOX_HISTORICAL_CONCURRENCY for this retry run.",
        ),
    ] = None,
    retry_output_name: Annotated[
        str,
        typer.Option(help="Retry Parquet path prefix under DATA_DIR."),
    ] = "processed/equities/nse_daily_ohlcv_upstox_retry",
    retry_coverage_output: Annotated[
        Path,
        typer.Option(help="Retry fetch coverage CSV path."),
    ] = Path("data/processed/equities/nse_daily_ohlcv_upstox_retry_coverage.csv"),
    retry_failures_output: Annotated[
        Path,
        typer.Option(help="Retry fetch failures CSV path."),
    ] = Path("data/processed/equities/nse_daily_ohlcv_upstox_retry_failures.csv"),
    access_token: Annotated[
        str | None,
        typer.Option(help="Override UPSTOX_ACCESS_TOKEN for this run."),
    ] = None,
) -> None:
    retry_statuses = tuple(item.strip() for item in statuses.split(",") if item.strip())
    if not retry_statuses:
        raise typer.BadParameter("--statuses must include at least one status.")
    try:
        result = run_upstox_daily_ohlcv_retry_pipeline(
            coverage_run_id=coverage_run_id,
            statuses=retry_statuses,
            limit=limit,
            throttle_seconds=throttle_seconds,
            max_concurrent_fetches=max_concurrent_fetches,
            retry_output_name=retry_output_name,
            retry_coverage_output=retry_coverage_output,
            retry_failures_output=retry_failures_output,
            access_token=access_token,
            trigger="cli",
        )
    except ValueError as exc:
        raise typer.BadParameter(str(exc)) from exc

    console.print(
        "Retry candidates: "
        f"{result.metrics['candidate_rows']} from {result.metrics['source_coverage_run_id']}"
    )
    console.print(f"Fetched retry rows: {result.metrics['fetched_rows']}")
    console.print(f"Upstox historical concurrency: {result.metrics['max_concurrent_fetches']}")
    console.print(f"Wrote retry coverage: {retry_coverage_output}")
    console.print(f"Wrote retry failures: {retry_failures_output}")
    console.print(f"Stored retry coverage rows: {result.metrics['timescale_fetch_coverage_rows']}")
    if result.warnings:
        for warning in result.warnings:
            console.print(f"[yellow]{warning}[/yellow]")


@app.command("fetch-yfinance-daily")
def fetch_yfinance_daily(
    universe: Annotated[
        str,
        typer.Option(help="Universe to fetch: us_seed, canada_seed, us_all, or canada_all."),
    ] = "us_seed",
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
    limit: Annotated[
        int | None,
        typer.Option(help="Optional smoke-test symbol limit."),
    ] = None,
    batch_size: Annotated[
        int,
        typer.Option(min=1, max=100, help="Symbols per yfinance download batch."),
    ] = 25,
    store_db: Annotated[
        bool,
        typer.Option(help="Also upsert daily candles and audits into Timescale/Postgres."),
    ] = True,
) -> None:
    try:
        result = run_yfinance_daily_ohlcv_pipeline(
            universe=universe,
            years=years,
            from_date=from_date,
            to_date=to_date,
            limit=limit,
            batch_size=batch_size,
            store_db=store_db,
            trigger="cli",
        )
    except ValueError as exc:
        raise typer.BadParameter(str(exc)) from exc

    output_path = result.artifacts.get("ohlcv")
    if output_path is None:
        console.print("No new yfinance daily OHLCV rows fetched; existing Parquet left unchanged")
    else:
        console.print(
            f"Wrote yfinance daily OHLCV: {output_path} "
            f"({result.metrics['db_snapshot_rows'] or result.rows} rows)"
        )
    console.print(f"Universe: {result.metrics['universe']} / exchange {result.metrics['exchange']}")
    console.print(f"Fetched rows: {result.metrics['fetched_rows']}")
    console.print(f"Batch size: {result.metrics['batch_size']}")
    _print_yahoo_execution_controls(result)
    console.print(f"Skipped/current symbols: {result.metrics['skipped_current_symbols']}")
    console.print(f"Fetch failures: {result.metrics['failure_rows']}")
    if store_db:
        console.print(f"Upserted ohlcv_daily rows: {result.metrics['timescale_rows']}")
        console.print(
            f"Stored price adjustment rows: {result.metrics['timescale_price_adjustment_rows']}"
        )
        console.print(
            f"Stored fetch coverage rows: {result.metrics['timescale_fetch_coverage_rows']}"
        )
    for warning in result.warnings:
        console.print(f"[yellow]{warning}[/yellow]")


@app.command("plan-yfinance-daily-work")
def plan_yfinance_daily_work(
    exchanges: Annotated[
        str,
        typer.Option(help="Comma-separated exchanges: NSE, TSX, US."),
    ] = "NSE,TSX,US",
    incremental: Annotated[
        bool,
        typer.Option(help="Plan current daily incremental work."),
    ] = True,
    initial_backfill: Annotated[
        bool,
        typer.Option(help="Plan missing ten-year active-symbol work."),
    ] = True,
    gap_repair: Annotated[
        bool,
        typer.Option(help="Also plan explicit missing-gap repair work."),
    ] = False,
) -> None:
    exchange_list = [value.strip().upper() for value in exchanges.split(",") if value.strip()]
    try:
        result = run_yfinance_daily_work_planner(
            exchanges=exchange_list,
            include_incremental=incremental,
            include_initial_backfill=initial_backfill,
            include_gap_repair=gap_repair,
            trigger="cli",
        )
    except ValueError as exc:
        raise typer.BadParameter(str(exc)) from exc
    except SQLAlchemyError as exc:
        detail = str(getattr(exc, "orig", exc)).strip().splitlines()[0]
        console.print(
            f"[red]Error: Database query failed while planning daily work: {detail}[/red]"
        )
        raise typer.Exit(code=1) from None
    console.print(f"Active symbols: {result.metrics['active_symbols']}")
    console.print(
        "Durable work: "
        f"{result.metrics['work_inserted']} inserted, "
        f"{result.metrics['duplicates_reused']} existing idempotent items reused"
    )
    console.print(f"Queue state: {result.metrics['queue']}")


@app.command("run-yfinance-daily-worker")
def run_yfinance_daily_worker(
    claim_size: Annotated[
        int | None,
        typer.Option(min=1, max=1000, help="Maximum durable items to claim."),
    ] = None,
    worker_id: Annotated[
        str | None,
        typer.Option(help="Optional stable worker identity for diagnostics."),
    ] = None,
) -> None:
    result = run_yfinance_daily_work_queue(
        worker_id=worker_id,
        claim_size=claim_size,
        trigger="cli",
    )
    console.print(
        "Yahoo durable worker: "
        f"{result.metrics['claimed']} claimed, "
        f"{result.metrics['succeeded']} succeeded, "
        f"{result.metrics['retry_wait']} retry-wait, "
        f"{result.metrics['terminal']} terminal, "
        f"{result.metrics.get('cancelled', 0)} cancelled"
    )
    console.print(f"OHLCV rows written: {result.metrics.get('ohlcv_rows_written', 0)}")
    console.print(f"Queue state: {result.metrics['queue']}")
    for warning in result.warnings:
        console.print(f"[yellow]{warning}[/yellow]")


@app.command("plan-yfinance-tsx-canary")
def plan_yfinance_tsx_canary(
    symbol_limit: Annotated[
        int,
        typer.Option(min=1, max=500, help="Maximum reconciled TSX symbols to plan."),
    ] = 1,
    enqueue: Annotated[
        bool,
        typer.Option(
            "--enqueue/--dry-run",
            help="Persist durable work only when the guarded canary flag is enabled.",
        ),
    ] = False,
) -> None:
    try:
        result = run_yfinance_tsx_canary_planner(
            symbol_limit=symbol_limit,
            enqueue=enqueue,
            trigger="cli",
        )
    except ValueError as exc:
        raise typer.BadParameter(str(exc)) from exc
    except SQLAlchemyError as exc:
        detail = str(getattr(exc, "orig", exc)).strip().splitlines()[0]
        console.print(f"[red]Error: TSX canary planning failed: {detail}[/red]")
        raise typer.Exit(code=1) from None

    tsx = result.metrics["exchanges"]["TSX"]
    mode = "ENQUEUED" if enqueue else "DRY RUN"
    console.print(f"TSX canary: {mode}")
    console.print(
        f"Symbols: {tsx['active_symbols']} selected from "
        f"{tsx['eligible_symbols_before_limit']} officially eligible"
    )
    quarantined = tsx.get("provider_quarantined_symbols", [])
    if quarantined:
        console.print(
            "[yellow]Provider-history quarantine: "
            + ", ".join(quarantined)
            + "[/yellow]"
        )
    console.print(
        f"Durable work: {result.metrics['work_generated']} generated, "
        f"{result.metrics['work_inserted']} inserted"
    )
    console.print(f"Window: {tsx['window_start']} to {tsx['window_end']}")
    console.print(f"Queue state: {result.metrics['queue']}")
    if not enqueue:
        console.print(
            "[yellow]Dry run only. Enable the bounded TSX canary flag before using "
            "--enqueue.[/yellow]"
        )


@app.command("plan-yfinance-nse-canary")
def plan_yfinance_nse_canary(
    symbol_limit: Annotated[
        int,
        typer.Option(min=1, max=5_000, help="Maximum NSE symbols to plan."),
    ] = 1,
    enqueue: Annotated[
        bool,
        typer.Option(
            "--enqueue/--dry-run",
            help="Persist work only when the bounded NSE canary flag is enabled.",
        ),
    ] = False,
) -> None:
    try:
        result = run_yfinance_nse_canary_planner(
            symbol_limit=symbol_limit,
            enqueue=enqueue,
            trigger="cli",
        )
    except ValueError as exc:
        raise typer.BadParameter(str(exc)) from exc
    nse = result.metrics["exchanges"]["NSE"]
    console.print(f"NSE canary: {'ENQUEUED' if enqueue else 'DRY RUN'}")
    console.print(
        f"Symbols: {nse['active_symbols']} selected from "
        f"{nse['eligible_symbols_before_limit']} eligible"
    )
    quarantined = nse.get("provider_quarantined_symbols", [])
    if quarantined:
        console.print(
            "[yellow]Provider-history quarantine: "
            + ", ".join(quarantined)
            + "[/yellow]"
        )
    console.print(
        f"Durable work: {result.metrics['work_generated']} generated, "
        f"{result.metrics['work_inserted']} inserted"
    )
    console.print(f"Window: {nse['window_start']} to {nse['window_end']}")
    console.print(f"Queue state: {result.metrics['queue']}")
    if not enqueue:
        console.print(
            "[yellow]Dry run only. Enable the bounded NSE canary flag before using "
            "--enqueue.[/yellow]"
        )


@app.command("check-nse-yfinance-cutover")
def check_nse_yfinance_cutover() -> None:
    result = run_nse_yfinance_cutover_readiness(trigger="cli")
    metrics = result.metrics
    console.print(f"NSE Yahoo cutover: {result.status.upper()}")
    console.print(f"Comparison state: {metrics.get('comparison_state', 'unavailable')}")
    console.print(
        "Provider windows: "
        f"Upstox={metrics.get('upstox_window_symbols', 0)} symbols through "
        f"{metrics.get('upstox_latest_date') or 'none'}, "
        f"Yahoo={metrics.get('yfinance_window_symbols', 0)} symbols through "
        f"{metrics.get('yfinance_latest_date') or 'none'}"
    )
    console.print(
        "Overlap: "
        f"{metrics.get('overlapping_symbols', 0)} symbols, "
        f"{metrics.get('row_overlap_ratio', 0):.2%} rows"
    )
    console.print(
        "Raw close match: "
        f"{metrics.get('close_match_ratio', 0):.2%} "
        f"at {metrics.get('close_tolerance', 0):.2%} tolerance"
    )
    console.print(
        "Freshness lag: "
        f"Upstox={metrics.get('upstox_session_lag', 'n/a')} sessions, "
        f"Yahoo={metrics.get('yfinance_session_lag', 'n/a')} sessions"
    )
    for issue in result.blocking_issues:
        console.print(f"[red]Blocked: {issue}[/red]")
    if result.status == "fail":
        raise typer.Exit(code=1)


@app.command("refresh-yfinance-history-evidence")
def refresh_yfinance_history_evidence(
    exchange: Annotated[
        str,
        typer.Argument(help="Canonical equity exchange: NSE, TSX, or US."),
    ],
    symbol_limit: Annotated[
        int | None,
        typer.Option(min=1, help="Optional deterministic symbol limit."),
    ] = None,
    symbols: Annotated[
        str | None,
        typer.Option(help="Optional comma-separated Yahoo symbols."),
    ] = None,
) -> None:
    selected_symbols = (
        [value.strip() for value in symbols.split(",") if value.strip()]
        if symbols
        else None
    )
    try:
        result = run_yfinance_provider_history_evidence_bootstrap(
            exchange,
            symbol_limit=symbol_limit,
            provider_symbols=selected_symbols,
            trigger="cli",
        )
    except ValueError as exc:
        raise typer.BadParameter(str(exc)) from exc
    except SQLAlchemyError as exc:
        detail = str(getattr(exc, "orig", exc)).strip().splitlines()[0]
        console.print(f"[red]Error: Provider-history refresh failed: {detail}[/red]")
        raise typer.Exit(code=1) from None

    table = Table(title=f"{exchange.upper()} Yahoo Provider History Evidence")
    table.add_column("Classification")
    table.add_column("Windows", justify="right")
    for classification, count in result.metrics["classifications"].items():
        table.add_row(str(classification), str(count))
    console.print(table)
    console.print(
        f"Evidence: {result.metrics['evidence_rows_written']} rows from "
        f"{result.metrics['successful_backfill_windows']} successful backfill windows"
    )
    console.print(f"Pending work cancelled: {result.metrics['pending_work_cancelled']}")
    quarantined = result.metrics["quarantined_symbols"]
    if quarantined:
        console.print("[yellow]Quarantined: " + ", ".join(quarantined) + "[/yellow]")
    for warning in result.warnings:
        console.print(f"[yellow]{warning}[/yellow]")


@app.command("provider-history-status")
def provider_history_status(
    exchange: Annotated[
        str,
        typer.Argument(help="Canonical equity exchange: NSE, TSX, or US."),
    ],
) -> None:
    exchange_code = exchange.upper()
    if exchange_code not in {"NSE", "TSX", "US"}:
        raise typer.BadParameter("exchange must be NSE, TSX, or US")
    try:
        store = TimescaleStore(get_settings().database_url)
        store.initialize()
        summary = store.provider_daily_history_summary(exchange_code)
    except SQLAlchemyError as exc:
        detail = str(getattr(exc, "orig", exc)).strip().splitlines()[0]
        console.print(f"[red]Error: Provider-history query failed: {detail}[/red]")
        raise typer.Exit(code=1) from None

    table = Table(title=f"{exchange_code} Yahoo Provider History Status")
    table.add_column("Classification")
    table.add_column("Instruments", justify="right")
    table.add_column("Windows", justify="right")
    table.add_column("Expected", justify="right")
    table.add_column("Observed", justify="right")
    table.add_column("Unavailable", justify="right")
    for row in summary["groups"]:
        table.add_row(
            str(row["classification"]),
            str(row["instruments"]),
            str(row["evidence_windows"]),
            str(row["expected_rows"] or 0),
            str(row["observed_rows"] or 0),
            str(row["provider_unavailable_rows"] or 0),
        )
    console.print(table)
    quarantined = [str(row["provider_symbol"]) for row in summary["quarantined"]]
    if quarantined:
        console.print("[yellow]Quarantined: " + ", ".join(quarantined) + "[/yellow]")


@app.command("fetch-yfinance-missing")
def fetch_yfinance_missing(
    universe: Annotated[
        str,
        typer.Option(
            help="Universe to inspect and fill: us_seed, canada_seed, us_all, or canada_all."
        ),
    ] = "us_all",
    from_date: Annotated[
        str,
        typer.Option(help="Start date in YYYY-MM-DD format."),
    ] = ...,
    to_date: Annotated[
        str,
        typer.Option(help="End date in YYYY-MM-DD format."),
    ] = ...,
    coverage_status: Annotated[
        str | None,
        typer.Option(help="Optional filter: complete, partial, or empty."),
    ] = None,
    min_avg_daily_turnover: Annotated[
        float | None,
        typer.Option(min=0, help="Only fetch symbols at or above this stored turnover."),
    ] = None,
    min_coverage_pct: Annotated[
        float | None,
        typer.Option(min=0, max=1, help="Only fetch symbols with at least this coverage ratio."),
    ] = None,
    limit: Annotated[
        int | None,
        typer.Option(min=1, help="Maximum missing windows to fetch."),
    ] = None,
    batch_size: Annotated[
        int,
        typer.Option(min=1, max=100, help="Symbols per yfinance download batch."),
    ] = 25,
    export_db_snapshot: Annotated[
        bool,
        typer.Option(help="Export a full DB snapshot after applying the missing rows."),
    ] = True,
) -> None:
    try:
        result = run_yfinance_missing_ohlcv_pipeline(
            universe=universe,
            from_date=from_date,
            to_date=to_date,
            coverage_status=coverage_status,
            min_avg_daily_turnover=min_avg_daily_turnover,
            min_coverage_pct=min_coverage_pct,
            limit=limit,
            batch_size=batch_size,
            export_db_snapshot=export_db_snapshot,
        )
    except ValueError as exc:
        raise typer.BadParameter(str(exc)) from exc

    output_path = result.artifacts.get("ohlcv")
    if output_path is None:
        console.print("No yfinance missing-window rows fetched; existing Parquet left unchanged")
    else:
        console.print(
            f"Wrote yfinance missing-window OHLCV: {output_path} "
            f"({result.metrics['db_snapshot_rows'] or result.rows} rows)"
        )
    console.print(f"Universe: {result.metrics['universe']} / exchange {result.metrics['exchange']}")
    console.print(f"Fetch symbols: {result.metrics['fetch_symbols']}")
    console.print(f"Fetch windows: {result.metrics['fetch_windows']}")
    console.print(f"Fetched rows: {result.metrics['fetched_rows']}")
    console.print(f"Batch size: {result.metrics['batch_size']}")
    _print_yahoo_execution_controls(result)
    console.print(f"Fetch failures: {result.metrics['failure_rows']}")
    console.print(f"Upserted ohlcv_daily rows: {result.metrics['timescale_rows']}")
    console.print(
        f"Stored price adjustment rows: {result.metrics['timescale_price_adjustment_rows']}"
    )
    console.print(f"Stored fetch coverage rows: {result.metrics['timescale_fetch_coverage_rows']}")
    for warning in result.warnings:
        console.print(f"[yellow]{warning}[/yellow]")


def _print_yahoo_execution_controls(result: PipelineRunResult) -> None:
    console.print(
        "Yahoo execution: "
        f"{result.metrics['adaptive_rate_mode']} mode, "
        f"{result.metrics['enforced_rpm']} enforced RPM, "
        f"{result.metrics['recommended_rpm']} recommended RPM, "
        f"{result.metrics['yfinance_concurrency']} enforced workers, "
        f"{result.metrics['recommended_concurrency']} recommended workers"
    )
    console.print(
        "Yahoo outcomes: "
        f"{result.metrics['yahoo_attempts']} attempts, "
        f"{result.metrics['retried_tickers']} retried tickers, "
        f"{result.metrics['partial_batches']} partial batches"
    )


@app.command("fetch-dukascopy-intraday")
def fetch_dukascopy_intraday(
    universe: Annotated[
        str,
        typer.Option(help="Dukascopy intraday universe id."),
    ] = "dukascopy_fx_crypto_5m",
    interval: Annotated[
        str,
        typer.Option(help="Intraday candle interval. Phase 4 supports 5m only."),
    ] = "5m",
    from_date: Annotated[
        str,
        typer.Option(help="Start date in YYYY-MM-DD format."),
    ] = ...,
    to_date: Annotated[
        str,
        typer.Option(help="End date in YYYY-MM-DD format."),
    ] = ...,
    instrument: Annotated[
        str | None,
        typer.Option(help="Optional single instrument, e.g. EUR/USD, EURUSD, or BTC/USD."),
    ] = None,
    limit: Annotated[
        int | None,
        typer.Option(min=1, help="Optional instrument limit for smoke tests."),
    ] = None,
    max_hours: Annotated[
        int | None,
        typer.Option(min=1, help="Maximum hourly Dukascopy archive files to request."),
    ] = None,
    timeout_seconds: Annotated[
        float,
        typer.Option(min=1, max=60, help="Per-request Dukascopy HTTP timeout."),
    ] = 15.0,
    store_db: Annotated[
        bool,
        typer.Option(help="Also upsert 5-minute candles into Timescale/Postgres."),
    ] = True,
) -> None:
    try:
        result = run_dukascopy_intraday_ohlcv_pipeline(
            universe=universe,
            interval=interval,
            from_date=from_date,
            to_date=to_date,
            instrument=instrument,
            limit=limit,
            max_hours=max_hours,
            timeout_seconds=timeout_seconds,
            store_db=store_db,
            trigger="cli",
        )
    except ValueError as exc:
        raise typer.BadParameter(str(exc)) from exc

    output_path = result.artifacts.get("ohlcv")
    if output_path is None:
        console.print("No Dukascopy intraday OHLCV rows fetched; Parquet left unchanged")
    else:
        console.print(f"Wrote Dukascopy intraday OHLCV: {output_path} ({result.rows} rows)")
    console.print(f"Universe: {result.metrics['universe']} / interval {result.metrics['interval']}")
    console.print(f"Mapped symbols: {result.metrics['mapped_symbols']}")
    console.print(f"Requested hours: {result.metrics['requested_hours']}")
    console.print(f"Timeout seconds: {result.metrics['timeout_seconds']}")
    console.print(f"Fetched rows: {result.metrics['fetched_rows']}")
    console.print(f"Fetch failures: {result.metrics['failure_rows']}")
    if store_db:
        console.print(f"Upserted ohlcv_intraday rows: {result.metrics['timescale_rows']}")
    for warning in result.warnings:
        console.print(f"[yellow]{warning}[/yellow]")


@app.command("fetch-yfinance-intraday")
def fetch_yfinance_intraday(
    universe: Annotated[
        str,
        typer.Option(help="yfinance intraday universe id."),
    ] = "yfinance_fx_crypto_5m",
    interval: Annotated[
        str,
        typer.Option(help="Intraday candle interval. Phase 4 fallback supports 5m only."),
    ] = "5m",
    from_datetime: Annotated[
        str | None,
        typer.Option(help="Start datetime, e.g. 2026-07-01T00:00:00Z."),
    ] = None,
    to_datetime: Annotated[
        str | None,
        typer.Option(help="End datetime, e.g. 2026-07-01T01:00:00Z."),
    ] = None,
    instrument: Annotated[
        str | None,
        typer.Option(help="Optional single instrument, e.g. EUR/USD, EURUSD=X, or BTC/USD."),
    ] = None,
    limit: Annotated[
        int | None,
        typer.Option(min=1, help="Optional instrument limit for smoke tests."),
    ] = None,
    store_db: Annotated[
        bool,
        typer.Option(help="Also upsert 5-minute candles into Timescale/Postgres."),
    ] = True,
) -> None:
    try:
        result = run_yfinance_intraday_ohlcv_pipeline(
            universe=universe,
            interval=interval,
            from_datetime=from_datetime,
            to_datetime=to_datetime,
            instrument=instrument,
            limit=limit,
            store_db=store_db,
            trigger="cli",
        )
    except ValueError as exc:
        raise typer.BadParameter(str(exc)) from exc

    output_path = result.artifacts.get("ohlcv")
    if output_path is None:
        console.print("No yfinance intraday OHLCV rows fetched; Parquet left unchanged")
    else:
        console.print(f"Wrote yfinance intraday OHLCV: {output_path} ({result.rows} rows)")
    console.print(f"Universe: {result.metrics['universe']} / interval {result.metrics['interval']}")
    console.print(f"Mapped symbols: {result.metrics['mapped_symbols']}")
    console.print(f"Fetched rows: {result.metrics['fetched_rows']}")
    console.print(f"Fetch failures: {result.metrics['failure_rows']}")
    if store_db:
        console.print(f"Upserted ohlcv_intraday rows: {result.metrics['timescale_rows']}")
    for warning in result.warnings:
        console.print(f"[yellow]{warning}[/yellow]")


def _subtract_months(value: date, months: int) -> date:
    year = value.year
    month = value.month - months
    while month <= 0:
        month += 12
        year -= 1

    import calendar

    day = min(value.day, calendar.monthrange(year, month)[1])
    return date(year, month, day)


def _parse_cli_date(value: str, option_name: str) -> date:
    try:
        return date.fromisoformat(value)
    except ValueError as exc:
        raise typer.BadParameter(
            f"{option_name} must use YYYY-MM-DD format, got {value!r}."
        ) from exc


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
    ohlcv_source: Annotated[
        str,
        typer.Option(help="Timescale OHLCV provider: upstox or yfinance."),
    ] = "upstox",
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
    store_db: Annotated[
        bool,
        typer.Option(
            "--store-db/--no-store-db",
            help="Also store feature rows, run metadata, and feature audits in TimescaleDB.",
        ),
    ] = False,
    incremental: Annotated[
        bool,
        typer.Option("--incremental/--full-rebuild", help="Only compute the new feature rows."),
    ] = False,
    replace_exchange: Annotated[
        bool,
        typer.Option(
            "--replace-exchange/--keep-existing",
            help="Delete the stored NSE feature version before a full provider rebuild.",
        ),
    ] = False,
    lookback_days: Annotated[
        int,
        typer.Option(help="Calendar-day warmup window for incremental feature computation."),
    ] = 320,
) -> None:
    try:
        result = run_daily_feature_pipeline(
            input_source=input_source,
            input_name=input_name,
            ohlcv_source=ohlcv_source,
            output_name=output_name,
            feature_version=feature_version,
            audit_output=audit_output,
            summary_output=summary_output,
            limit=limit,
            strict_invalid_rows=strict_invalid_rows,
            store_db=store_db,
            incremental=incremental,
            replace_exchange=replace_exchange,
            lookback_days=lookback_days,
        )
    except ValueError as exc:
        raise typer.Exit(str(exc)) from exc

    console.print(f"Wrote daily features: {result.artifacts['features']} ({result.rows} rows)")
    console.print(f"Wrote feature audit: {audit_output}")
    console.print(f"Wrote feature summary: {summary_output}")
    if store_db:
        console.print(
            "Stored Timescale features: "
            f"{result.metrics['timescale_rows']} rows, "
            f"{result.metrics['timescale_audit_rows']} audit rows, "
            f"run_id={result.metrics['timescale_run_id']}"
        )
    if result.metrics["invalid_ohlcv_count"]:
        console.print(
            f"[yellow]Excluded invalid OHLCV rows: {result.metrics['invalid_ohlcv_count']}[/yellow]"
        )
    console.print(json.dumps(result.metrics, indent=2))


@app.command("build-daily-targets")
def build_daily_targets(
    input_source: Annotated[
        str,
        typer.Option(help="Input source: parquet or timescale."),
    ] = "parquet",
    input_name: Annotated[
        str,
        typer.Option(help="Input Parquet path prefix under DATA_DIR."),
    ] = "processed/equities/nse_daily_ohlcv_upstox",
    ohlcv_source: Annotated[
        str,
        typer.Option(help="Timescale OHLCV provider: upstox or yfinance."),
    ] = "upstox",
    output_name: Annotated[
        str,
        typer.Option(help="Target Parquet path prefix under DATA_DIR."),
    ] = "processed/targets/daily_v1_forward_returns",
    target_version: Annotated[
        str,
        typer.Option(help="Target version stored in the output rows."),
    ] = DAILY_FORWARD_TARGET_VERSION_V1_0,
    audit_output: Annotated[
        Path,
        typer.Option(help="Target audit CSV path."),
    ] = Path("data/processed/targets/daily_v1_forward_returns_audit.csv"),
    summary_output: Annotated[
        Path,
        typer.Option(help="Target summary JSON path."),
    ] = Path("data/processed/targets/daily_v1_forward_returns_summary.json"),
    limit: Annotated[
        int | None,
        typer.Option(help="Optional symbol limit for smoke tests."),
    ] = None,
    strict_invalid_rows: Annotated[
        bool,
        typer.Option(help="Fail instead of excluding invalid OHLCV rows before target build."),
    ] = False,
    store_db: Annotated[
        bool,
        typer.Option(
            "--store-db/--no-store-db",
            help="Also store target rows, run metadata, and target audits in TimescaleDB.",
        ),
    ] = False,
    incremental: Annotated[
        bool,
        typer.Option(
            "--incremental/--full-rebuild",
            help="Recompute only the target dirty window from TimescaleDB.",
        ),
    ] = False,
    replace_exchange: Annotated[
        bool,
        typer.Option(
            "--replace-exchange/--keep-existing",
            help="Delete the stored NSE target version before a full provider rebuild.",
        ),
    ] = False,
    recompute_lookback_days: Annotated[
        int,
        typer.Option(help="Calendar-day target dirty window for incremental computation."),
    ] = 90,
) -> None:
    try:
        result = run_daily_target_pipeline(
            input_source=input_source,
            input_name=input_name,
            ohlcv_source=ohlcv_source,
            output_name=output_name,
            target_version=target_version,
            audit_output=audit_output,
            summary_output=summary_output,
            limit=limit,
            strict_invalid_rows=strict_invalid_rows,
            store_db=store_db,
            incremental=incremental,
            replace_exchange=replace_exchange,
            recompute_lookback_days=recompute_lookback_days,
        )
    except ValueError as exc:
        raise typer.Exit(str(exc)) from exc

    console.print(f"Wrote daily targets: {result.artifacts['targets']} ({result.rows} rows)")
    console.print(f"Wrote target audit: {audit_output}")
    console.print(f"Wrote target summary: {summary_output}")
    if store_db:
        console.print(
            "Stored Timescale targets: "
            f"{result.metrics['timescale_rows']} rows, "
            f"{result.metrics['timescale_audit_rows']} audit rows, "
            f"run_id={result.metrics['timescale_run_id']}"
        )
    if result.metrics["invalid_ohlcv_count"]:
        console.print(
            f"[yellow]Excluded invalid OHLCV rows: {result.metrics['invalid_ohlcv_count']}[/yellow]"
        )
    console.print(json.dumps(result.metrics, indent=2))


@app.command("build-factor-research")
def build_factor_research(
    feature_name: Annotated[
        str,
        typer.Option(help="Feature Parquet path prefix under DATA_DIR."),
    ] = "processed/features/daily_v1_ohlcv_technical",
    target_name: Annotated[
        str,
        typer.Option(help="Target Parquet path prefix under DATA_DIR."),
    ] = "processed/targets/daily_v1_forward_returns",
    output_dir: Annotated[
        Path,
        typer.Option(help="Directory for factor research CSV/JSON outputs."),
    ] = Path("data/processed/research/factors"),
    feature_version: Annotated[
        str,
        typer.Option(help="Feature version to analyze."),
    ] = FEATURE_VERSION_V1_0,
    target_version: Annotated[
        str,
        typer.Option(help="Target version to analyze."),
    ] = DAILY_FORWARD_TARGET_VERSION_V1_0,
    quantiles: Annotated[
        int,
        typer.Option(help="Number of same-date feature quantiles to evaluate."),
    ] = 5,
    min_date_rows: Annotated[
        int,
        typer.Option(help="Minimum rows per date for IC/rank IC calculation."),
    ] = 5,
    min_month_rows: Annotated[
        int,
        typer.Option(help="Minimum rows per month for monthly stability calculation."),
    ] = 20,
) -> None:
    result = run_factor_research_pipeline(
        feature_name=feature_name,
        target_name=target_name,
        output_dir=output_dir,
        feature_version=feature_version,
        target_version=target_version,
        quantiles=quantiles,
        min_date_rows=min_date_rows,
        min_month_rows=min_month_rows,
    )
    console.print(f"Wrote factor IC: {result.artifacts['ic']} ({result.metrics['ic_rows']} rows)")
    console.print(
        "Wrote factor quantiles: "
        f"{result.artifacts['quantiles']} ({result.metrics['quantile_rows']} rows)"
    )
    console.print(
        "Wrote factor hit rates: "
        f"{result.artifacts['hit_rates']} ({result.metrics['hit_rate_rows']} rows)"
    )
    console.print(
        "Wrote factor monthly stability: "
        f"{result.artifacts['monthly_stability']} "
        f"({result.metrics['monthly_stability_rows']} rows)"
    )
    console.print(f"Wrote factor summary: {result.artifacts['summary']}")
    console.print(json.dumps(result.metrics, indent=2))


@app.command("validate-processed-datasets")
def validate_processed_datasets_command(
    pass_coverage_threshold: Annotated[
        float,
        typer.Option(help="Date coverage ratio required for pass status."),
    ] = 0.90,
    warn_coverage_threshold: Annotated[
        float,
        typer.Option(help="Date coverage ratio required for warn status."),
    ] = 0.70,
) -> None:
    result = run_processed_dataset_validation_pipeline(
        pass_coverage_threshold=pass_coverage_threshold,
        warn_coverage_threshold=warn_coverage_threshold,
    )
    summary = result.metrics
    console.print(
        "Processed dataset validation: "
        f"{summary['overall_status']} | baseline_ml_ready={summary['baseline_ml_ready']}"
    )
    console.print(f"Summary: {result.artifacts['summary_md']}")
    if summary["blocking_issues"]:
        console.print("[red]Blocking issues:[/red]")
        for issue in summary["blocking_issues"]:
            console.print(f"- {issue}")
    if summary["warnings"]:
        console.print("[yellow]Warnings:[/yellow]")
        for warning in summary["warnings"]:
            console.print(f"- {warning}")


@app.command("build-ml-dataset-v1")
def build_ml_dataset_v1_command() -> None:
    result = run_ml_dataset_v1_pipeline()
    console.print(
        "ML dataset v1: "
        f"{result.status} | rows={result.metrics['row_count']} | "
        f"trainable_rows={result.metrics['trainable_row_count']}"
    )
    console.print(f"Dataset: {result.artifacts['dataset']}")
    console.print(f"Summary: {result.artifacts['summary']}")
    console.print(f"Leakage checks: {result.artifacts['leakage_checks']}")
    if result.warnings:
        console.print("[yellow]Warnings:[/yellow]")
        for warning in result.warnings:
            console.print(f"- {warning}")


@app.command("build-walk-forward-folds-v1")
def build_walk_forward_folds_v1_command(
    min_train_days: Annotated[
        int,
        typer.Option(min=1, help="Minimum labeled trading dates in each train window."),
    ] = 240,
    validation_days: Annotated[
        int,
        typer.Option(min=1, help="Labeled trading dates in each validation window."),
    ] = 60,
    prediction_step_days: Annotated[
        int,
        typer.Option(min=1, help="Generate every Nth valid prediction date."),
    ] = 1,
    start_date: Annotated[
        str | None,
        typer.Option(help="Optional first prediction date in YYYY-MM-DD format."),
    ] = None,
    end_date: Annotated[
        str | None,
        typer.Option(help="Optional last prediction date in YYYY-MM-DD format."),
    ] = None,
    max_folds: Annotated[
        int | None,
        typer.Option(min=1, help="Optional cap for quick smoke runs."),
    ] = None,
) -> None:
    config = WalkForwardManifestConfig(
        min_train_days=min_train_days,
        validation_days=validation_days,
        prediction_step_days=prediction_step_days,
        start_date=_parse_cli_date(start_date, "--start-date") if start_date else None,
        end_date=_parse_cli_date(end_date, "--end-date") if end_date else None,
        max_folds=max_folds,
    )
    result = run_walk_forward_folds_v1_pipeline(config=config)
    console.print(
        "Walk-forward folds v1: "
        f"{result.status} | folds={result.metrics['fold_count']} | "
        f"candidates={result.metrics['candidate_date_count']}"
    )
    console.print(f"Folds: {result.artifacts['folds']}")
    console.print(f"Summary: {result.artifacts['summary']}")
    if result.warnings:
        console.print("[yellow]Warnings:[/yellow]")
        for warning in result.warnings:
            console.print(f"- {warning}")


@app.command("run-baseline-predictions-v1")
def run_baseline_predictions_v1_command(
    min_train_days: Annotated[
        int,
        typer.Option(min=1, help="Minimum labeled trading dates in each train window."),
    ] = 180,
    validation_days: Annotated[
        int,
        typer.Option(min=1, help="Labeled trading dates in each validation window."),
    ] = 40,
    prediction_step_days: Annotated[
        int,
        typer.Option(min=1, help="Predict every Nth valid prediction date."),
    ] = 1,
    max_folds: Annotated[
        int | None,
        typer.Option(min=1, help="Optional cap for quick smoke runs."),
    ] = None,
) -> None:
    config = BaselineRunConfig(
        min_train_days=min_train_days,
        validation_days=validation_days,
        prediction_step_days=prediction_step_days,
        max_folds=max_folds,
    )
    result = run_baseline_predictions_v1_pipeline(config=config)
    console.print(
        "Baseline predictions v1: "
        f"{result.status} | rows={result.metrics['prediction_row_count']} | "
        f"models={result.metrics['model_count']} | folds={result.metrics['fold_count']}"
    )
    console.print(f"Predictions: {result.artifacts['predictions']}")
    console.print(f"Metrics: {result.artifacts['metrics']}")
    console.print(f"Summary: {result.artifacts['summary']}")
    if result.warnings:
        console.print("[yellow]Warnings:[/yellow]")
        for warning in result.warnings:
            console.print(f"- {warning}")


@app.command("run-lightgbm-predictions-v1")
def run_lightgbm_predictions_v1_command(
    min_train_days: Annotated[
        int,
        typer.Option(min=1, help="Minimum labeled trading dates in each train window."),
    ] = 180,
    validation_days: Annotated[
        int,
        typer.Option(min=1, help="Labeled trading dates in each validation window."),
    ] = 40,
    prediction_step_days: Annotated[
        int,
        typer.Option(min=1, help="Predict every Nth valid prediction date."),
    ] = 1,
    max_folds: Annotated[
        int | None,
        typer.Option(min=1, help="Optional cap. Defaults to 10 for a quick LightGBM run."),
    ] = 10,
    n_estimators: Annotated[
        int,
        typer.Option(min=1, help="LightGBM estimators per fold/model."),
    ] = 80,
) -> None:
    config = LightGBMRunConfig(
        min_train_days=min_train_days,
        validation_days=validation_days,
        prediction_step_days=prediction_step_days,
        max_folds=max_folds,
        n_estimators=n_estimators,
    )
    result = run_lightgbm_predictions_v1_pipeline(config=config)
    console.print(
        "LightGBM predictions v1: "
        f"{result.status} | rows={result.metrics['prediction_row_count']} | "
        f"models={result.metrics['model_count']} | folds={result.metrics['fold_count']}"
    )
    console.print(f"Predictions: {result.artifacts['predictions']}")
    console.print(f"Metrics: {result.artifacts['metrics']}")
    console.print(f"Summary: {result.artifacts['summary']}")
    if result.warnings:
        console.print("[yellow]Warnings:[/yellow]")
        for warning in result.warnings:
            console.print(f"- {warning}")


@app.command("run-prediction-backtest-v1")
def run_prediction_backtest_v1_command(
    predictions: Annotated[
        Path,
        typer.Option(help="Prediction parquet to backtest."),
    ] = Path("data/processed/ml/baselines_v1/baseline_predictions.parquet"),
    output_dir: Annotated[
        Path | None,
        typer.Option(help="Output directory for backtest artifacts."),
    ] = None,
    transaction_cost_bps: Annotated[
        float,
        typer.Option(min=0, help="One-way daily rebalance transaction cost in bps."),
    ] = 10.0,
) -> None:
    config = BacktestConfig(transaction_cost_bps=transaction_cost_bps)
    result = run_prediction_backtest_v1_pipeline(
        predictions_path=predictions,
        output_dir=output_dir,
        config=config,
    )
    console.print(
        "Prediction backtest v1: "
        f"{result.status} | results={result.metrics['result_count']} | "
        f"models={result.metrics['model_count']}"
    )
    console.print(f"Daily returns: {result.artifacts['daily_returns']}")
    console.print(f"Equity curve: {result.artifacts['equity_curve']}")
    console.print(f"Metrics: {result.artifacts['metrics']}")
    console.print(f"Summary: {result.artifacts['summary']}")
    if result.warnings:
        console.print("[yellow]Warnings:[/yellow]")
        for warning in result.warnings:
            console.print(f"- {warning}")


@app.command("run-latest-predictions-v1")
def run_latest_predictions_v1_command(
    min_train_days: Annotated[
        int,
        typer.Option(min=1, help="Minimum labeled trading dates in the train window."),
    ] = 180,
    validation_days: Annotated[
        int,
        typer.Option(min=1, help="Labeled trading dates in the validation window."),
    ] = 40,
    include_lightgbm: Annotated[
        bool,
        typer.Option(
            "--include-lightgbm/--no-include-lightgbm",
            help="Train and score LightGBM models for the latest prediction date.",
        ),
    ] = True,
    lightgbm_n_estimators: Annotated[
        int,
        typer.Option(min=1, help="LightGBM estimators for latest prediction models."),
    ] = 80,
    target_session_date: Annotated[
        str | None,
        typer.Option(
            help=(
                "Session date these latest predictions are intended for, in YYYY-MM-DD. "
                "Defaults to the next weekday after the feature date."
            ),
        ),
    ] = None,
) -> None:
    config = LatestPredictionConfig(
        min_train_days=min_train_days,
        validation_days=validation_days,
        include_lightgbm=include_lightgbm,
        lightgbm_n_estimators=lightgbm_n_estimators,
        target_session_date=(
            _parse_cli_date(target_session_date, "--target-session-date")
            if target_session_date
            else None
        ),
    )
    result = run_latest_predictions_v1_pipeline(config=config)
    console.print(
        "Latest predictions v1: "
        f"{result.status} | date={result.metrics['prediction_date']} | "
        f"target_session={result.metrics['target_session_date']} | "
        f"rows={result.metrics['prediction_row_count']} | "
        f"models={result.metrics['model_count']}"
    )
    console.print(f"Predictions: {result.artifacts['predictions']}")
    console.print(f"Candidates: {result.artifacts['candidates']}")
    console.print(f"Summary: {result.artifacts['summary']}")
    console.print(f"Report: {result.artifacts['report']}")
    if result.warnings:
        console.print("[yellow]Warnings:[/yellow]")
        for warning in result.warnings:
            console.print(f"- {warning}")


@app.command("validate-daily-pipeline-health")
def validate_daily_pipeline_health_command(
    run_live_fetch: Annotated[
        bool,
        typer.Option(
            "--run-live-fetch/--no-run-live-fetch",
            help="Attempt live Upstox daily fetch up to the latest expected trading date.",
        ),
    ] = False,
    run_factor_research: Annotated[
        bool,
        typer.Option(
            "--run-factor-research/--skip-factor-research",
            help="Rebuild factor research outputs after feature/target rebuild.",
        ),
    ] = True,
) -> None:
    result = run_daily_pipeline_health_pipeline(
        run_live_fetch=run_live_fetch,
        run_factor_research=run_factor_research,
    )
    summary = result.metrics
    console.print(
        "Daily pipeline health: "
        f"{summary['overall_status']} | baseline_ml_ready={summary['baseline_ml_ready']}"
    )
    console.print(f"Latest expected trading date: {summary['latest_expected_trading_date']}")
    console.print(f"Report: {result.artifacts['health_report']}")
    if summary["blocking_issues"]:
        console.print("[red]Blocking issues:[/red]")
        for issue in summary["blocking_issues"]:
            console.print(f"- {issue}")
    if summary["warnings"]:
        console.print("[yellow]Warnings:[/yellow]")
        for warning in summary["warnings"]:
            console.print(f"- {warning}")


if __name__ == "__main__":
    app()
