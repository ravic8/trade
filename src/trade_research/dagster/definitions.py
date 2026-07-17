from dagster import DefaultScheduleStatus, Definitions, ScheduleDefinition, define_asset_job

from trade_research.dagster.daily_assets import (
    daily_features_v1,
    daily_pipeline_health,
    daily_targets_v1,
    dukascopy_fx_intraday_ohlcv,
    factor_research_v1,
    fx_intraday_gap_validation,
    ml_dataset_v1,
    nse_exchange_sessions,
    nse_universe_snapshot,
    processed_dataset_validation,
    tsx_exchange_sessions,
    tsx_universe_snapshot,
    upstox_daily_ohlcv,
    us_exchange_sessions,
    us_universe_snapshot,
    yfinance_canada_daily_ohlcv,
    yfinance_fx_crypto_intraday_ohlcv,
    yfinance_fx_intraday_gap_validation,
    yfinance_us_daily_ohlcv,
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

daily_research_pipeline_job = define_asset_job(
    name="daily_research_pipeline_job",
    selection=[
        upstox_daily_ohlcv,
        processed_dataset_validation,
        daily_features_v1,
        daily_targets_v1,
        ml_dataset_v1,
        factor_research_v1,
        daily_pipeline_health,
    ],
)

factor_research_job = define_asset_job(
    name="factor_research_job",
    selection=[
        upstox_daily_ohlcv,
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
        nse_exchange_sessions,
        tsx_exchange_sessions,
        us_exchange_sessions,
        nse_universe_snapshot,
        tsx_universe_snapshot,
        us_universe_snapshot,
        yfinance_us_daily_ohlcv,
        yfinance_canada_daily_ohlcv,
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
        daily_research_pipeline_job,
        factor_research_job,
        north_america_daily_yfinance_job,
        fx_intraday_dukascopy_job,
        yfinance_fx_intraday_job,
        nse_universe_refresh_job,
        tsx_universe_refresh_job,
        us_universe_refresh_job,
        nse_exchange_sessions_job,
        tsx_exchange_sessions_job,
        us_exchange_sessions_job,
    ],
    schedules=[
        daily_research_schedule,
        north_america_daily_yfinance_schedule,
        fx_intraday_dukascopy_schedule,
        yfinance_fx_intraday_schedule,
        nse_universe_refresh_schedule,
        tsx_universe_refresh_schedule,
        us_universe_refresh_schedule,
        nse_exchange_sessions_schedule,
        tsx_exchange_sessions_schedule,
        us_exchange_sessions_schedule,
    ],
)
