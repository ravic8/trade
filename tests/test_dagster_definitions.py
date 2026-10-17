from importlib import import_module

import pytest

dagster = pytest.importorskip("dagster")
definitions = import_module("trade_data.dagster.definitions")


def test_phase3_daily_partition_reconciliation_is_manual_only() -> None:
    assert (
        definitions.nse_daily_clickhouse_partition_reconciliation_job.name
        == "nse_daily_clickhouse_partition_reconciliation_job"
    )
    assert all(
        schedule.name != "nse_daily_clickhouse_partition_reconciliation_schedule"
        for schedule in definitions.defs.schedules
    )


def test_phase3_provider_comparison_is_manual_only() -> None:
    assert (
        definitions.nse_yfinance_provider_comparison_job.name
        == "nse_yfinance_provider_comparison_job"
    )
    assert all(
        schedule.name != "nse_yfinance_provider_comparison_schedule"
        for schedule in definitions.defs.schedules
    )


def test_phase3_daily_canary_is_manual_only() -> None:
    assert (
        definitions.yfinance_nse_daily_canary_job.name
        == "yfinance_nse_daily_canary_job"
    )
    assert all(
        schedule.name != "yfinance_nse_daily_canary_schedule"
        for schedule in definitions.defs.schedules
    )


def test_phase2_universe_refresh_jobs_and_schedules_are_stopped_by_default() -> None:
    for exchange in ("nse", "tsx", "us"):
        job = getattr(definitions, f"{exchange}_universe_refresh_job")
        schedule = getattr(definitions, f"{exchange}_universe_refresh_schedule")
        assert job.name == f"{exchange}_universe_refresh_job"
        assert schedule.name == f"{exchange}_universe_refresh_schedule"
        assert schedule.default_status == dagster.DefaultScheduleStatus.STOPPED


def test_phase3_exchange_session_jobs_and_schedules_are_stopped_by_default() -> None:
    for exchange in ("nse", "tsx", "us"):
        job = getattr(definitions, f"{exchange}_exchange_sessions_job")
        schedule = getattr(definitions, f"{exchange}_exchange_sessions_schedule")
        assert job.name == f"{exchange}_exchange_sessions_job"
        assert schedule.name == f"{exchange}_exchange_sessions_schedule"
        assert schedule.default_status == dagster.DefaultScheduleStatus.STOPPED


def test_phase5_yfinance_queue_jobs_and_schedules_are_stopped_by_default() -> None:
    for role in ("planner", "worker"):
        job = getattr(definitions, f"yfinance_daily_work_{role}_job")
        schedule = getattr(definitions, f"yfinance_daily_work_{role}_schedule")
        assert job.name == f"yfinance_daily_work_{role}_job"
        assert schedule.name == f"yfinance_daily_work_{role}_schedule"
        assert schedule.default_status == dagster.DefaultScheduleStatus.STOPPED

    assert (
        definitions.yfinance_nse_completed_session_work_planner_schedule.cron_schedule
        == "15 12 * * 1-5"
    )
    for exchange, timezone in (
        ("tsx", "America/Toronto"),
        ("us", "America/New_York"),
    ):
        planner_job = getattr(
            definitions,
            f"yfinance_{exchange}_completed_session_work_planner_job",
        )
        planner_schedule = getattr(
            definitions,
            f"yfinance_{exchange}_completed_session_work_planner_schedule",
        )
        assert (
            planner_job.name
            == f"yfinance_{exchange}_completed_session_work_planner_job"
        )
        assert planner_schedule.cron_schedule == "0 18 * * 1-5"
        assert planner_schedule.execution_timezone == timezone
        assert planner_schedule.default_status == dagster.DefaultScheduleStatus.STOPPED

def test_api_workflow_requests_have_one_stopped_by_default_dagster_dispatcher() -> None:
    assert definitions.data_pipeline_request_job.name == "data_pipeline_request_job"
    assert definitions.data_pipeline_request_sensor.name == "data_pipeline_request_sensor"
    assert (
        definitions.data_pipeline_request_sensor.default_status
        == dagster.DefaultSensorStatus.STOPPED
    )


def test_only_daily_jobs_and_assets_are_registered() -> None:
    dagster.Definitions.validate_loadable(definitions.defs)
    names = {job.name for job in definitions.defs.jobs}
    names.update(schedule.name for schedule in definitions.defs.schedules)
    names.update(key.to_user_string() for asset in definitions.defs.assets for key in asset.keys)
    for name in names:
        assert not any(token in name for token in (
            "opportunity", "research", "feature", "target", "ml_dataset",
            "intraday", "minute", "bigquery", "bounded_canary",
        )), name
    assert len(list(definitions.defs.schedules)) == 12
