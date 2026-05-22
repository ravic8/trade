from dagster import DefaultScheduleStatus, Definitions, ScheduleDefinition, define_asset_job

from trade_research.dagster.assets import (
    nse_hourly_backlog_ohlcv,
    nse_hourly_ohlcv,
    nse_universe,
    tsx_hourly_backlog_ohlcv,
    tsx_hourly_ohlcv,
    tsx_universe,
)
from trade_research.dagster.resources import timescale_store
from trade_research.dagster.sensors import nse_hourly_backlog_sensor, tsx_hourly_backlog_sensor

nse_hourly_ingestion_job = define_asset_job(
    name="nse_hourly_ingestion_job",
    selection=[nse_universe, nse_hourly_ohlcv],
)

tsx_hourly_ingestion_job = define_asset_job(
    name="tsx_hourly_ingestion_job",
    selection=[tsx_universe, tsx_hourly_ohlcv],
)

nse_hourly_backlog_recovery_job = define_asset_job(
    name="nse_hourly_backlog_recovery_job",
    selection=[nse_universe, nse_hourly_backlog_ohlcv],
)

tsx_hourly_backlog_recovery_job = define_asset_job(
    name="tsx_hourly_backlog_recovery_job",
    selection=[tsx_universe, tsx_hourly_backlog_ohlcv],
)

nse_hourly_schedule = ScheduleDefinition(
    name="nse_hourly_schedule",
    job=nse_hourly_ingestion_job,
    cron_schedule="45 9-16 * * 1-5",
    execution_timezone="Asia/Kolkata",
    default_status=DefaultScheduleStatus.RUNNING,
)

tsx_hourly_schedule = ScheduleDefinition(
    name="tsx_hourly_schedule",
    job=tsx_hourly_ingestion_job,
    cron_schedule="45 9-16 * * 1-5",
    execution_timezone="America/Toronto",
    default_status=DefaultScheduleStatus.RUNNING,
)

defs = Definitions(
    assets=[
        nse_universe,
        nse_hourly_ohlcv,
        nse_hourly_backlog_ohlcv,
        tsx_universe,
        tsx_hourly_ohlcv,
        tsx_hourly_backlog_ohlcv,
    ],
    jobs=[
        nse_hourly_ingestion_job,
        tsx_hourly_ingestion_job,
        nse_hourly_backlog_recovery_job,
        tsx_hourly_backlog_recovery_job,
    ],
    schedules=[nse_hourly_schedule, tsx_hourly_schedule],
    sensors=[nse_hourly_backlog_sensor, tsx_hourly_backlog_sensor],
    resources={"timescale_store": timescale_store},
)
