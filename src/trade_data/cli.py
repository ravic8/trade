import getpass
import sys
from datetime import date
from pathlib import Path
from typing import Annotated

import pandas as pd
import typer
from rich.console import Console
from rich.table import Table
from sqlalchemy.exc import SQLAlchemyError

from trade_data.analytics import create_or_update_analyst_role, revoke_analyst_role
from trade_data.cli_guard import enforce_cli_policy
from trade_data.config import get_settings
from trade_data.config_validation import ConfigurationError
from trade_data.data import (
    UpstoxInstrumentMasterProvider,
    instrument_master_audit,
    map_liquid_universe_to_upstox,
)
from trade_data.market_calendar import session_decision
from trade_data.operations.daily_canary import daily_canary_run_report, daily_canary_snapshot
from trade_data.pipelines import (
    PipelineRunResult,
    run_equity_universe_snapshot_pipeline,
    run_exchange_session_materialization_pipeline,
    run_nse_yfinance_cutover_readiness,
    run_upstox_daily_ohlcv_pipeline,
    run_upstox_daily_ohlcv_retry_pipeline,
    run_yfinance_daily_ohlcv_pipeline,
    run_yfinance_daily_work_planner,
    run_yfinance_daily_work_queue,
    run_yfinance_missing_ohlcv_pipeline,
    run_yfinance_nse_canary_planner,
    run_yfinance_nse_incremental_canary_planner,
    run_yfinance_nse_incremental_canary_worker,
    run_yfinance_provider_history_evidence_bootstrap,
    run_yfinance_tsx_canary_planner,
)
from trade_data.runtime_contract import inspect_runtime
from trade_data.storage import ParquetStore, TimescaleStore
from trade_data.universe import (
    NSEUniverseProvider,
    TSXUniverseProvider,
    YFinanceCanadaUniverseProvider,
    YFinanceUSUniverseProvider,
)

app = typer.Typer(help="Daily equity data operations CLI.", pretty_exceptions_show_locals=False)
console = Console()


@app.callback()
def enforce_production_cli_guard(ctx: typer.Context) -> None:
    """Apply the production mutation policy before any command callback."""

    try:
        current_settings = get_settings()
    except ConfigurationError as exc:
        console.print(str(exc), markup=False)
        raise typer.Exit(code=2) from None
    enforce_cli_policy(
        ctx.invoked_subcommand,
        app_env=str(getattr(current_settings, "app_env", "local")),
    )


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


@app.command("doctor")
def doctor(
    check_services: Annotated[
        bool, typer.Option(help="Probe PostgreSQL and Redis read-only.")
    ] = False,
) -> None:
    """Show self-hosting readiness without exposing connection strings or secrets."""

    report = inspect_runtime(get_settings(), check_services=check_services)
    console.print_json(data=report.as_dict())
    if not report.ready:
        raise typer.Exit(code=1)


@app.command("daily-canary-snapshot")
def daily_canary_snapshot_command(
    exchange: Annotated[str, typer.Argument(help="Exchange: NSE, TSX, or US.")],
    symbols: Annotated[str, typer.Option(help="One to five comma-separated provider symbols.")],
    from_date: Annotated[str, typer.Option(help="First completed session, YYYY-MM-DD.")],
    to_date: Annotated[str, typer.Option(help="Last completed session, YYYY-MM-DD.")],
) -> None:
    """Inspect accepted sessions and stored daily coverage without provider calls."""

    try:
        settings = get_settings()
        report = daily_canary_snapshot(
            TimescaleStore(settings.database_url),
            exchange=exchange,
            provider_symbols=symbols.split(","),
            start_date=date.fromisoformat(from_date),
            end_date=date.fromisoformat(to_date),
            provider_grace_minutes=settings.yfinance_provider_grace_minutes,
        )
    except ValueError as exc:
        raise typer.BadParameter(str(exc)) from exc
    except SQLAlchemyError:
        console.print("[red]Database unavailable while reading daily canary coverage.[/red]")
        raise typer.Exit(code=1) from None
    console.print_json(data=report)


@app.command("daily-canary-run-report")
def daily_canary_run_report_command(
    run_id: Annotated[str, typer.Argument(help="NSE yfinance worker run ID.")],
) -> None:
    """Show scoped work, provider requests, and stored coverage for one canary run."""
    try:
        settings = get_settings()
        report = daily_canary_run_report(
            TimescaleStore(settings.database_url),
            run_id=run_id,
            provider_grace_minutes=settings.yfinance_provider_grace_minutes,
        )
    except ValueError as exc:
        raise typer.BadParameter(str(exc)) from exc
    except SQLAlchemyError:
        console.print("[red]Database unavailable while reading daily canary run.[/red]")
        raise typer.Exit(code=1) from None
    console.print_json(data=report)


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
    console.print("Initialized daily data TimescaleDB schema")


@app.command("create-analyst-role")
def create_analyst_role(
    role_name: Annotated[
        str,
        typer.Argument(help="Unique lowercase PostgreSQL login for one analyst."),
    ],
    password_stdin: Annotated[
        bool,
        typer.Option(
            "--password-stdin",
            help="Read one password line from standard input for a secret-manager pipe.",
        ),
    ] = False,
) -> None:
    """Create or rotate an analytics-only role without exposing its password."""
    if password_stdin:
        password = sys.stdin.readline().rstrip("\r\n")
    else:
        password = getpass.getpass("Analyst database password: ")
        confirmation = getpass.getpass("Confirm analyst database password: ")
        if password != confirmation:
            raise typer.BadParameter("Passwords do not match.")
    try:
        create_or_update_analyst_role(get_settings().database_url, role_name, password)
    except ValueError as exc:
        raise typer.BadParameter(str(exc)) from exc
    console.print(f"Analyst role {role_name} is active with analytics-only read access.")


@app.command("revoke-analyst-role")
def revoke_analyst_login(
    role_name: Annotated[
        str,
        typer.Argument(help="Existing analyst login to disable immediately."),
    ],
) -> None:
    """Disable an analyst login while retaining the role and grants for audit."""
    try:
        revoke_analyst_role(get_settings().database_url, role_name)
    except ValueError as exc:
        raise typer.BadParameter(str(exc)) from exc
    console.print(f"Analyst role {role_name} can no longer log in.")


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
            "[yellow]Provider-history quarantine: " + ", ".join(quarantined) + "[/yellow]"
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
    symbols: Annotated[
        str | None,
        typer.Option(help="Optional comma-separated NSE or Yahoo symbols."),
    ] = None,
    session_count: Annotated[
        int,
        typer.Option(min=1, max=31, help="Recent eligible sessions in the bounded canary."),
    ] = 5,
    enqueue: Annotated[
        bool,
        typer.Option(
            "--enqueue/--dry-run",
            help="Persist work only when the bounded NSE canary flag is enabled.",
        ),
    ] = False,
) -> None:
    selected_symbols = (
        [value.strip() for value in symbols.split(",") if value.strip()] if symbols else None
    )
    try:
        result = run_yfinance_nse_canary_planner(
            symbol_limit=symbol_limit,
            provider_symbols=selected_symbols,
            enqueue=enqueue,
            force_refresh=True,
            session_count=session_count,
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
    console.print("Selected: " + ", ".join(nse.get("selected_provider_symbols", [])))
    quarantined = nse.get("provider_quarantined_symbols", [])
    if quarantined:
        console.print(
            "[yellow]Provider-history quarantine: " + ", ".join(quarantined) + "[/yellow]"
        )
    console.print(
        f"Durable work: {result.metrics['work_generated']} generated, "
        f"{result.metrics['work_inserted']} inserted"
    )
    console.print(
        "Canary window: "
        f"{nse.get('bounded_canary_window_start')} to "
        f"{nse.get('bounded_canary_window_end')}"
    )
    console.print(f"Queue state: {result.metrics['queue']}")
    if not enqueue:
        console.print(
            "[yellow]Dry run only. Enable the bounded NSE canary flag before using "
            "--enqueue.[/yellow]"
        )


@app.command("plan-yfinance-nse-incremental-canary")
def plan_yfinance_nse_incremental_canary(
    symbols: Annotated[str, typer.Option(help="One to five explicit NSE provider symbols.")],
    enqueue: Annotated[
        bool,
        typer.Option("--enqueue/--dry-run", help="Persist only when the NSE canary flag is on."),
    ] = False,
) -> None:
    """Plan current incremental work for only the selected NSE symbols."""
    try:
        result = run_yfinance_nse_incremental_canary_planner(
            provider_symbols=symbols.split(","), enqueue=enqueue, trigger="cli"
        )
    except ValueError as exc:
        raise typer.BadParameter(str(exc)) from exc
    except SQLAlchemyError:
        console.print("[red]Database unavailable while planning NSE incremental canary.[/red]")
        raise typer.Exit(code=1) from None
    nse = result.metrics["exchanges"]["NSE"]
    console.print_json(
        data={
            "mode": "enqueued" if enqueue else "dry_run",
            "selected_provider_symbols": nse["selected_provider_symbols"],
            "latest_eligible_session": nse["window_end"],
            "planned_windows": nse["planned_windows"],
            "work_generated": result.metrics["work_generated"],
            "work_inserted": result.metrics["work_inserted"],
            "queue": result.metrics["queue"],
        }
    )


@app.command("run-yfinance-nse-incremental-canary")
def run_yfinance_nse_incremental_canary(
    symbols: Annotated[str, typer.Option(help="Same one to five NSE provider symbols as plan.")],
) -> None:
    """Process only selected NSE incremental work items, never the full queue."""
    try:
        result = run_yfinance_nse_incremental_canary_worker(
            provider_symbols=symbols.split(","), trigger="cli"
        )
    except ValueError as exc:
        raise typer.BadParameter(str(exc)) from exc
    except SQLAlchemyError:
        console.print("[red]Database unavailable while running NSE incremental canary.[/red]")
        raise typer.Exit(code=1) from None
    console.print_json(
        data={
            "status": result.status,
            "run_id": result.metrics.get("run_id"),
            "claimed": result.metrics["claimed"],
            "succeeded": result.metrics["succeeded"],
            "retry_wait": result.metrics["retry_wait"],
            "terminal": result.metrics["terminal"],
            "ohlcv_rows_written": result.metrics.get("ohlcv_rows_written", 0),
            "queue": result.metrics["queue"],
            "warnings": result.warnings,
        }
    )
    if result.status != "pass":
        raise typer.Exit(code=1)


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
        [value.strip() for value in symbols.split(",") if value.strip()] if symbols else None
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


@app.command("export-daily-snapshot")
def export_daily_snapshot_command(
    destination: Annotated[Path, typer.Argument(help="New snapshot directory; never overwritten.")],
    start_date: Annotated[str, typer.Option(help="First session, YYYY-MM-DD.")],
    end_date: Annotated[str, typer.Option(help="Last session, YYYY-MM-DD.")],
) -> None:
    """Export accepted NSE/yfinance candles in a read-only consistent transaction."""
    from trade_data.operations.daily_snapshot import export_daily_snapshot

    manifest = export_daily_snapshot(
        TimescaleStore(get_settings().database_url).engine,
        destination=destination,
        start_date=_parse_cli_date(start_date, "--start-date"),
        end_date=_parse_cli_date(end_date, "--end-date"),
    )
    console.print(str(manifest))


@app.command("export-validated-history")
def export_validated_history_command(
    destination: Annotated[Path, typer.Argument(help="New audit/export directory.")],
    start_date: Annotated[str, typer.Option(help="First date, YYYY-MM-DD.")],
    end_date: Annotated[str, typer.Option(help="Last date, YYYY-MM-DD.")],
    source_validation: Annotated[
        Path | None, typer.Option(help="Optional preserved NSE comparison Parquet.")
    ] = None,
) -> None:
    """Revalidate all stored NSE history; export accepted, quarantine and review files."""
    from trade_data.operations.history_validation import export_validated_daily_history

    console.print(
        str(
            export_validated_daily_history(
                TimescaleStore(get_settings().database_url).engine,
                destination=destination,
                start_date=_parse_cli_date(start_date, "--start-date"),
                end_date=_parse_cli_date(end_date, "--end-date"),
                source_validation=source_validation,
            )
        )
    )


# Validate configuration before the CLI can open provider or database connections.
get_settings()


if __name__ == "__main__":
    app()
