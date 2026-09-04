from dagster import DefaultScheduleStatus, Definitions, ScheduleDefinition, define_asset_job

from trade_research.dagster.daily_assets import (
    bigquery_export_sync,
    bigquery_tsx_ohlcv_canary,
    daily_features_v1,
    daily_pipeline_health,
    daily_targets_v1,
    dukascopy_fx_intraday_ohlcv,
    factor_research_v1,
    fx_intraday_gap_validation,
    ml_dataset_v1,
    nse_completed_session_opportunity_targets,
    nse_daily_ohlcv,
    nse_exchange_sessions,
    nse_opportunity_targets_v1,
    nse_universe_snapshot,
    processed_dataset_validation,
    tsx_completed_session_opportunity_targets,
    tsx_exchange_sessions,
    tsx_opportunity_targets_v1,
    tsx_universe_snapshot,
    upstox_daily_ohlcv,
    us_completed_session_opportunity_targets,
    us_exchange_sessions,
    us_opportunity_targets_v1,
    us_universe_snapshot,
    yfinance_canada_daily_ohlcv,
    yfinance_daily_work_plan,
    yfinance_daily_work_worker,
    yfinance_fx_crypto_intraday_ohlcv,
    yfinance_fx_intraday_gap_validation,
    yfinance_nse_completed_session_work_plan,
    yfinance_tsx_completed_session_work_plan,
    yfinance_us_completed_session_work_plan,
    yfinance_us_daily_ohlcv,
)
from trade_research.dagster.workflow_requests import (
    data_pipeline_request_job,
    data_pipeline_request_sensor,
)

bigquery_export_sync_job = define_asset_job(
    name="bigquery_export_sync_job",
    selection=[bigquery_export_sync],
)

bigquery_tsx_ohlcv_canary_job = define_asset_job(
    name="bigquery_tsx_ohlcv_canary_job",
    selection=[bigquery_tsx_ohlcv_canary],
)

nse_exchange_sessions_job = define_asset_job(
    name="nse_exchange_sessions_job",
    selection=[nse_exchange_sessions],
)

tsx_exchange_sessions_job = define_asset_job(
    name="tsx_exchange_sessions_job",
    selection=[tsx_exchange_sessions],
)

us_exchange_sessions_job = define_asset_job(
    name="us_exchange_sessions_job",
    selection=[us_exchange_sessions],
)

nse_universe_refresh_job = define_asset_job(
    name="nse_universe_refresh_job",
    selection=[nse_universe_snapshot],
)

tsx_universe_refresh_job = define_asset_job(
    name="tsx_universe_refresh_job",
    selection=[tsx_universe_snapshot],
)

us_universe_refresh_job = define_asset_job(
    name="us_universe_refresh_job",
    selection=[us_universe_snapshot],
)

yfinance_daily_work_planner_job = define_asset_job(
    name="yfinance_daily_work_planner_job",
    selection=[yfinance_daily_work_plan],
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

nse_completed_session_opportunity_targets_job = define_asset_job(
    name="nse_completed_session_opportunity_targets_job",
    selection=[nse_completed_session_opportunity_targets],
)

tsx_completed_session_opportunity_targets_job = define_asset_job(
    name="tsx_completed_session_opportunity_targets_job",
    selection=[tsx_completed_session_opportunity_targets],
)

us_completed_session_opportunity_targets_job = define_asset_job(
    name="us_completed_session_opportunity_targets_job",
    selection=[us_completed_session_opportunity_targets],
)

yfinance_daily_work_worker_job = define_asset_job(
    name="yfinance_daily_work_worker_job",
    selection=[yfinance_daily_work_worker],
)

daily_research_pipeline_job = define_asset_job(
    name="daily_research_pipeline_job",
    selection=[
        nse_daily_ohlcv,
        processed_dataset_validation,
        daily_features_v1,
        daily_targets_v1,
        nse_opportunity_targets_v1,
        ml_dataset_v1,
        factor_research_v1,
        daily_pipeline_health,
    ],
)

factor_research_job = define_asset_job(
    name="factor_research_job",
    selection=[
        nse_daily_ohlcv,
        daily_features_v1,
        daily_targets_v1,
        processed_dataset_validation,
        ml_dataset_v1,
        factor_research_v1,
    ],
)

north_america_daily_yfinance_job = define_asset_job(
    name="north_america_daily_yfinance_job",
    selection=[
        yfinance_us_daily_ohlcv,
        yfinance_canada_daily_ohlcv,
        us_opportunity_targets_v1,
        tsx_opportunity_targets_v1,
    ],
)

fx_intraday_dukascopy_job = define_asset_job(
    name="fx_intraday_dukascopy_job",
    selection=[
        dukascopy_fx_intraday_ohlcv,
        fx_intraday_gap_validation,
    ],
)

yfinance_fx_intraday_job = define_asset_job(
    name="yfinance_fx_intraday_job",
    selection=[
        yfinance_fx_crypto_intraday_ohlcv,
        yfinance_fx_intraday_gap_validation,
    ],
)

daily_research_schedule = ScheduleDefinition(
    name="daily_research_schedule",
    job=daily_research_pipeline_job,
    cron_schedule="30 19 * * 1-5",
    execution_timezone="Asia/Kolkata",
    default_status=DefaultScheduleStatus.STOPPED,
)

north_america_daily_yfinance_schedule = ScheduleDefinition(
    name="north_america_daily_yfinance_schedule",
    job=north_america_daily_yfinance_job,
    cron_schedule="30 3 * * 2-6",
    execution_timezone="Asia/Kolkata",
    default_status=DefaultScheduleStatus.STOPPED,
)

fx_intraday_dukascopy_schedule = ScheduleDefinition(
    name="fx_intraday_dukascopy_schedule",
    job=fx_intraday_dukascopy_job,
    cron_schedule="15 * * * 1-5",
    execution_timezone="UTC",
    default_status=DefaultScheduleStatus.STOPPED,
)

yfinance_fx_intraday_schedule = ScheduleDefinition(
    name="yfinance_fx_intraday_schedule",
    job=yfinance_fx_intraday_job,
    cron_schedule="20 * * * *",
    execution_timezone="UTC",
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

nse_completed_session_opportunity_targets_schedule = ScheduleDefinition(
    name="nse_completed_session_opportunity_targets_schedule",
    job=nse_completed_session_opportunity_targets_job,
    cron_schedule="15 13-18 * * 1-5",
    execution_timezone="UTC",
    default_status=DefaultScheduleStatus.STOPPED,
)

tsx_completed_session_opportunity_targets_schedule = ScheduleDefinition(
    name="tsx_completed_session_opportunity_targets_schedule",
    job=tsx_completed_session_opportunity_targets_job,
    cron_schedule=[
        "15 18-23 * * 1-5",
        "15 0-1 * * 2-6",
    ],
    execution_timezone="America/Toronto",
    default_status=DefaultScheduleStatus.STOPPED,
)

us_completed_session_opportunity_targets_schedule = ScheduleDefinition(
    name="us_completed_session_opportunity_targets_schedule",
    job=us_completed_session_opportunity_targets_job,
    cron_schedule=[
        "15 18-23 * * 1-5",
        "15 0-1 * * 2-6",
    ],
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

bigquery_daily_sync_schedule = ScheduleDefinition(
    name="bigquery_daily_sync_schedule",
    job=bigquery_export_sync_job,
    cron_schedule="30 8 * * *",
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
        bigquery_export_sync,
        bigquery_tsx_ohlcv_canary,
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
        yfinance_tsx_completed_session_work_plan,
        yfinance_us_completed_session_work_plan,
        yfinance_daily_work_worker,
        yfinance_us_daily_ohlcv,
        yfinance_canada_daily_ohlcv,
        nse_opportunity_targets_v1,
        nse_completed_session_opportunity_targets,
        tsx_completed_session_opportunity_targets,
        us_completed_session_opportunity_targets,
        tsx_opportunity_targets_v1,
        us_opportunity_targets_v1,
        dukascopy_fx_intraday_ohlcv,
        fx_intraday_gap_validation,
        yfinance_fx_crypto_intraday_ohlcv,
        yfinance_fx_intraday_gap_validation,
        processed_dataset_validation,
        daily_features_v1,
        daily_targets_v1,
        ml_dataset_v1,
        factor_research_v1,
        daily_pipeline_health,
    ],
    jobs=[
        bigquery_export_sync_job,
        bigquery_tsx_ohlcv_canary_job,
        daily_research_pipeline_job,
        factor_research_job,
        north_america_daily_yfinance_job,
        fx_intraday_dukascopy_job,
        yfinance_fx_intraday_job,
        nse_universe_refresh_job,
        tsx_universe_refresh_job,
        us_universe_refresh_job,
        yfinance_daily_work_planner_job,
        yfinance_nse_completed_session_work_planner_job,
        yfinance_tsx_completed_session_work_planner_job,
        yfinance_us_completed_session_work_planner_job,
        yfinance_daily_work_worker_job,
        nse_completed_session_opportunity_targets_job,
        tsx_completed_session_opportunity_targets_job,
        us_completed_session_opportunity_targets_job,
        nse_exchange_sessions_job,
        tsx_exchange_sessions_job,
        us_exchange_sessions_job,
        data_pipeline_request_job,
    ],
    schedules=[
        bigquery_daily_sync_schedule,
        daily_research_schedule,
        north_america_daily_yfinance_schedule,
        fx_intraday_dukascopy_schedule,
        yfinance_fx_intraday_schedule,
        nse_universe_refresh_schedule,
        tsx_universe_refresh_schedule,
        us_universe_refresh_schedule,
        yfinance_daily_work_planner_schedule,
        yfinance_nse_completed_session_work_planner_schedule,
        yfinance_tsx_completed_session_work_planner_schedule,
        yfinance_us_completed_session_work_planner_schedule,
        yfinance_daily_work_worker_schedule,
        nse_completed_session_opportunity_targets_schedule,
        tsx_completed_session_opportunity_targets_schedule,
        us_completed_session_opportunity_targets_schedule,
        nse_exchange_sessions_schedule,
        tsx_exchange_sessions_schedule,
        us_exchange_sessions_schedule,
    ],
    sensors=[data_pipeline_request_sensor],
)
