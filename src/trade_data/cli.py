"""Small local operations CLI; production ingestion is owned by Dagster."""

import json

import typer

from trade_data.cli_guard import enforce_cli_policy
from trade_data.config import get_settings
from trade_data.runtime_contract import inspect_runtime
from trade_data.storage.timescale import TimescaleStore

# Validate configuration before loading any operational entrypoint.
get_settings()

app = typer.Typer(pretty_exceptions_show_locals=False)


@app.callback()
def guard(ctx: typer.Context):
    enforce_cli_policy(ctx.invoked_subcommand, app_env=get_settings().app_env)


@app.command("doctor")
def doctor(check_services: bool = False):
    report = inspect_runtime(get_settings(), check_services=check_services)
    typer.echo(json.dumps(report.as_dict()))
    if not report.ready:
        raise typer.Exit(1)


@app.command("init-db")
def init_db():

    TimescaleStore(get_settings().database_url).initialize()


@app.command("refresh-exchange-sessions")
def refresh_exchange_sessions():
    from trade_data.pipelines.exchange_sessions import run_exchange_session_materialization_pipeline

    typer.echo(str(run_exchange_session_materialization_pipeline("NSE")))


@app.command("fetch-nse-daily")
def fetch_nse_daily():
    from trade_data.pipelines.nse_daily import run_daily

    typer.echo(json.dumps(run_daily()))


@app.command("repair-nse-year")
def repair_nse_year():
    from trade_data.pipelines.nse_daily import repair_year

    repair_year()
