"""NSE-only scheduled work. Legacy provider jobs are not loaded."""

from dagster import (
    AssetExecutionContext,
    DefaultScheduleStatus,
    Definitions,
    RetryPolicy,
    ScheduleDefinition,
    asset,
    define_asset_job,
)

from trade_data.config import get_settings
from trade_data.pipelines.exchange_sessions import run_exchange_session_materialization_pipeline
from trade_data.pipelines.nse_daily import repair_year, run_daily

get_settings()


@asset(retry_policy=RetryPolicy(max_retries=2, delay=300))
def nse_daily_bhavcopy(context: AssetExecutionContext):
    result = run_daily()
    context.log.info(str(result))
    return result


@asset
def nse_exchange_sessions():
    return run_exchange_session_materialization_pipeline("NSE")


@asset
def nse_one_year_repair():
    # Explicit bounded job; never scheduled as a recurring historical fetch.
    repair_year()


nse_daily_bhavcopy_job = define_asset_job("nse_daily_bhavcopy_job", selection=[nse_daily_bhavcopy])
nse_exchange_sessions_job = define_asset_job(
    "nse_exchange_sessions_job", selection=[nse_exchange_sessions]
)
nse_one_year_repair_job = define_asset_job(
    "nse_one_year_repair_job", selection=[nse_one_year_repair]
)
nse_daily_bhavcopy_schedule = ScheduleDefinition(
    name="nse_daily_bhavcopy_schedule",
    job=nse_daily_bhavcopy_job,
    cron_schedule="0,30 18-23 * * *",
    execution_timezone="Asia/Kolkata",
    default_status=DefaultScheduleStatus.STOPPED,
)
nse_exchange_sessions_schedule = ScheduleDefinition(
    name="nse_exchange_sessions_schedule",
    job=nse_exchange_sessions_job,
    cron_schedule="0 6 1 * *",
    execution_timezone="Asia/Kolkata",
    default_status=DefaultScheduleStatus.STOPPED,
)
defs = Definitions(
    assets=[nse_daily_bhavcopy, nse_exchange_sessions, nse_one_year_repair],
    jobs=[nse_daily_bhavcopy_job, nse_exchange_sessions_job, nse_one_year_repair_job],
    schedules=[nse_daily_bhavcopy_schedule, nse_exchange_sessions_schedule],
)
