from dagster import DefaultScheduleStatus, Definitions, ScheduleDefinition, define_asset_job

from trade_data.config import get_settings
from trade_data.dagster.daily_assets import (
    nse_daily_ohlcv,
    nse_exchange_sessions,
    nse_universe_snapshot,
    tsx_exchange_sessions,
    tsx_universe_snapshot,
    upstox_daily_ohlcv,
    us_exchange_sessions,
    us_universe_snapshot,
    yfinance_canada_daily_ohlcv,
    yfinance_daily_work_plan,
    yfinance_daily_work_worker,
    yfinance_nse_completed_session_work_plan,
    yfinance_nse_daily_canary,
    yfinance_tsx_completed_session_work_plan,
    yfinance_us_completed_session_work_plan,
    yfinance_us_daily_ohlcv,
)
from trade_data.dagster.market_data_assets import (
    nse_daily_clickhouse_partition_reconciliation,
    nse_yfinance_provider_comparison,
)
from trade_data.dagster.workflow_requests import (
    data_pipeline_request_job,
    data_pipeline_request_sensor,
)

nse_exchange_sessions_job = define_asset_job(
    name="nse_exchange_sessions_job", selection=[nse_exchange_sessions]
)
tsx_exchange_sessions_job = define_asset_job(
    name="tsx_exchange_sessions_job", selection=[tsx_exchange_sessions]
)
us_exchange_sessions_job = define_asset_job(
    name="us_exchange_sessions_job", selection=[us_exchange_sessions]
)
nse_universe_refresh_job = define_asset_job(
    name="nse_universe_refresh_job", selection=[nse_universe_snapshot]
)
tsx_universe_refresh_job = define_asset_job(
    name="tsx_universe_refresh_job", selection=[tsx_universe_snapshot]
)
us_universe_refresh_job = define_asset_job(
    name="us_universe_refresh_job", selection=[us_universe_snapshot]
)
yfinance_daily_work_planner_job = define_asset_job(
    name="yfinance_daily_work_planner_job", selection=[yfinance_daily_work_plan]
)
nse_daily_clickhouse_partition_reconciliation_job = define_asset_job(
    name="nse_daily_clickhouse_partition_reconciliation_job",
    selection=[nse_daily_clickhouse_partition_reconciliation],
)
nse_yfinance_provider_comparison_job = define_asset_job(
    name="nse_yfinance_provider_comparison_job", selection=[nse_yfinance_provider_comparison]
)
yfinance_nse_daily_canary_job = define_asset_job(
    name="yfinance_nse_daily_canary_job", selection=[yfinance_nse_daily_canary]
)
yfinance_nse_completed_session_work_planner_job = define_asset_job(
    name="yfinance_nse_completed_session_work_planner_job",
    selection=[yfinance_nse_completed_session_work_plan],
)
yfinance_tsx_completed_session_work_planner_job = define_asset_job(
    name="yfinance_tsx_completed_session_work_planner_job",
    selection=[yfinance_tsx_completed_session_work_plan],
)
yfinance_us_completed_session_work_planner_job = define_asset_job(
    name="yfinance_us_completed_session_work_planner_job",
    selection=[yfinance_us_completed_session_work_plan],
)
yfinance_daily_work_worker_job = define_asset_job(
    name="yfinance_daily_work_worker_job", selection=[yfinance_daily_work_worker]
)
north_america_daily_yfinance_job = define_asset_job(
    name="north_america_daily_yfinance_job",
    selection=[yfinance_us_daily_ohlcv, yfinance_canada_daily_ohlcv],
)
north_america_daily_yfinance_schedule = ScheduleDefinition(
    name="north_america_daily_yfinance_schedule",
    job=north_america_daily_yfinance_job,
    cron_schedule="30 3 * * 2-6",
    execution_timezone="Asia/Kolkata",
    default_status=DefaultScheduleStatus.STOPPED,
)
nse_universe_refresh_schedule = ScheduleDefinition(
    name="nse_universe_refresh_schedule",
    job=nse_universe_refresh_job,
    cron_schedule="0 8 * * 1-5",
    execution_timezone="Asia/Kolkata",
    default_status=DefaultScheduleStatus.STOPPED,
)
tsx_universe_refresh_schedule = ScheduleDefinition(
    name="tsx_universe_refresh_schedule",
    job=tsx_universe_refresh_job,
    cron_schedule="0 8 * * 1-5",
    execution_timezone="America/Toronto",
    default_status=DefaultScheduleStatus.STOPPED,
)
us_universe_refresh_schedule = ScheduleDefinition(
    name="us_universe_refresh_schedule",
    job=us_universe_refresh_job,
    cron_schedule="0 8 * * 1-5",
    execution_timezone="America/New_York",
    default_status=DefaultScheduleStatus.STOPPED,
)
yfinance_daily_work_planner_schedule = ScheduleDefinition(
    name="yfinance_daily_work_planner_schedule",
    job=yfinance_daily_work_planner_job,
    cron_schedule="0 6 * * *",
    execution_timezone="UTC",
    default_status=DefaultScheduleStatus.STOPPED,
)
yfinance_nse_completed_session_work_planner_schedule = ScheduleDefinition(
    name="yfinance_nse_completed_session_work_planner_schedule",
    job=yfinance_nse_completed_session_work_planner_job,
    cron_schedule="15 12 * * 1-5",
    execution_timezone="UTC",
    default_status=DefaultScheduleStatus.STOPPED,
)
yfinance_tsx_completed_session_work_planner_schedule = ScheduleDefinition(
    name="yfinance_tsx_completed_session_work_planner_schedule",
    job=yfinance_tsx_completed_session_work_planner_job,
    cron_schedule="0 18 * * 1-5",
    execution_timezone="America/Toronto",
    default_status=DefaultScheduleStatus.STOPPED,
)
yfinance_us_completed_session_work_planner_schedule = ScheduleDefinition(
    name="yfinance_us_completed_session_work_planner_schedule",
    job=yfinance_us_completed_session_work_planner_job,
    cron_schedule="0 18 * * 1-5",
    execution_timezone="America/New_York",
    default_status=DefaultScheduleStatus.STOPPED,
)
yfinance_daily_work_worker_schedule = ScheduleDefinition(
    name="yfinance_daily_work_worker_schedule",
    job=yfinance_daily_work_worker_job,
    cron_schedule="*/5 * * * *",
    execution_timezone="UTC",
    default_status=DefaultScheduleStatus.STOPPED,
)
nse_exchange_sessions_schedule = ScheduleDefinition(
    name="nse_exchange_sessions_schedule",
    job=nse_exchange_sessions_job,
    cron_schedule="0 6 1 * *",
    execution_timezone="Asia/Kolkata",
    default_status=DefaultScheduleStatus.STOPPED,
)
tsx_exchange_sessions_schedule = ScheduleDefinition(
    name="tsx_exchange_sessions_schedule",
    job=tsx_exchange_sessions_job,
    cron_schedule="0 6 1 * *",
    execution_timezone="America/Toronto",
    default_status=DefaultScheduleStatus.STOPPED,
)
us_exchange_sessions_schedule = ScheduleDefinition(
    name="us_exchange_sessions_schedule",
    job=us_exchange_sessions_job,
    cron_schedule="0 6 1 * *",
    execution_timezone="America/New_York",
    default_status=DefaultScheduleStatus.STOPPED,
)
defs = Definitions(
    assets=[
        upstox_daily_ohlcv,
        nse_daily_ohlcv,
        nse_exchange_sessions,
        tsx_exchange_sessions,
        us_exchange_sessions,
        nse_universe_snapshot,
        tsx_universe_snapshot,
        us_universe_snapshot,
        yfinance_daily_work_plan,
        yfinance_nse_completed_session_work_plan,
        yfinance_nse_daily_canary,
        yfinance_tsx_completed_session_work_plan,
        yfinance_us_completed_session_work_plan,
        yfinance_daily_work_worker,
        yfinance_us_daily_ohlcv,
        yfinance_canada_daily_ohlcv,
        nse_daily_clickhouse_partition_reconciliation,
        nse_yfinance_provider_comparison,
    ],
    jobs=[
        north_america_daily_yfinance_job,
        nse_daily_clickhouse_partition_reconciliation_job,
        nse_yfinance_provider_comparison_job,
        yfinance_nse_daily_canary_job,
        nse_universe_refresh_job,
        tsx_universe_refresh_job,
        us_universe_refresh_job,
        yfinance_daily_work_planner_job,
        yfinance_nse_completed_session_work_planner_job,
        yfinance_tsx_completed_session_work_planner_job,
        yfinance_us_completed_session_work_planner_job,
        yfinance_daily_work_worker_job,
        nse_exchange_sessions_job,
        tsx_exchange_sessions_job,
        us_exchange_sessions_job,
        data_pipeline_request_job,
    ],
    schedules=[
        north_america_daily_yfinance_schedule,
        nse_universe_refresh_schedule,
        tsx_universe_refresh_schedule,
        us_universe_refresh_schedule,
        yfinance_daily_work_planner_schedule,
        yfinance_nse_completed_session_work_planner_schedule,
        yfinance_tsx_completed_session_work_planner_schedule,
        yfinance_us_completed_session_work_planner_schedule,
        yfinance_daily_work_worker_schedule,
        nse_exchange_sessions_schedule,
        tsx_exchange_sessions_schedule,
        us_exchange_sessions_schedule,
    ],
    sensors=[data_pipeline_request_sensor],
)
# Reject invalid runtime configuration while loading this code location.
get_settings()
