from dagster import DefaultScheduleStatus, Definitions, ScheduleDefinition, define_asset_job

from trade_research.dagster.daily_assets import (
    daily_features_v1,
    daily_pipeline_health,
    daily_targets_v1,
    factor_research_v1,
    processed_dataset_validation,
    upstox_daily_ohlcv,
)

daily_research_pipeline_job = define_asset_job(
    name="daily_research_pipeline_job",
    selection=[
        upstox_daily_ohlcv,
        processed_dataset_validation,
        daily_features_v1,
        daily_targets_v1,
        factor_research_v1,
        daily_pipeline_health,
    ],
)

factor_research_job = define_asset_job(
    name="factor_research_job",
    selection=[daily_features_v1, daily_targets_v1, factor_research_v1],
)

daily_research_schedule = ScheduleDefinition(
    name="daily_research_schedule",
    job=daily_research_pipeline_job,
    cron_schedule="30 19 * * 1-5",
    execution_timezone="Asia/Kolkata",
    default_status=DefaultScheduleStatus.STOPPED,
)

defs = Definitions(
    assets=[
        upstox_daily_ohlcv,
        processed_dataset_validation,
        daily_features_v1,
        daily_targets_v1,
        factor_research_v1,
        daily_pipeline_health,
    ],
    jobs=[
        daily_research_pipeline_job,
        factor_research_job,
    ],
    schedules=[daily_research_schedule],
)
