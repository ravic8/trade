from importlib import import_module

import pytest

dagster = pytest.importorskip("dagster")
definitions = import_module("trade_research.dagster.definitions")


def test_fx_intraday_dukascopy_job_and_schedule_are_registered() -> None:
    assert definitions.fx_intraday_dukascopy_job.name == "fx_intraday_dukascopy_job"
    assert (
        definitions.fx_intraday_dukascopy_schedule.name
        == "fx_intraday_dukascopy_schedule"
    )
    assert (
        definitions.fx_intraday_dukascopy_schedule.default_status
        == dagster.DefaultScheduleStatus.STOPPED
    )


def test_yfinance_fx_intraday_job_and_schedule_are_registered() -> None:
    assert definitions.yfinance_fx_intraday_job.name == "yfinance_fx_intraday_job"
    assert (
        definitions.yfinance_fx_intraday_schedule.name
        == "yfinance_fx_intraday_schedule"
    )
    assert (
        definitions.yfinance_fx_intraday_schedule.default_status
        == dagster.DefaultScheduleStatus.STOPPED
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
    assert (
        definitions.nse_completed_session_opportunity_targets_schedule.cron_schedule
        == "15 13-18 * * 1-5"
    )
    assert (
        definitions.nse_completed_session_opportunity_targets_schedule.default_status
        == dagster.DefaultScheduleStatus.STOPPED
    )
    for exchange in ("tsx", "us"):
        job = getattr(
            definitions,
            f"{exchange}_completed_session_opportunity_targets_job",
        )
        schedule = getattr(
            definitions,
            f"{exchange}_completed_session_opportunity_targets_schedule",
        )
        assert job.name == f"{exchange}_completed_session_opportunity_targets_job"
        assert schedule.default_status == dagster.DefaultScheduleStatus.STOPPED
        assert schedule.cron_schedule == [
            "15 18-23 * * 1-5",
            "15 0-1 * * 2-6",
        ]
        assert schedule.execution_timezone == (
            "America/Toronto" if exchange == "tsx" else "America/New_York"
        )


def test_bigquery_export_job_and_schedule_are_registered_but_stopped() -> None:
    assert definitions.bigquery_export_sync_job.name == "bigquery_export_sync_job"
    assert definitions.bigquery_daily_sync_schedule.name == "bigquery_daily_sync_schedule"
    assert (
        definitions.bigquery_daily_sync_schedule.default_status
        == dagster.DefaultScheduleStatus.STOPPED
    )
    assert (
        definitions.bigquery_tsx_ohlcv_canary_job.name
        == "bigquery_tsx_ohlcv_canary_job"
    )
    assert all(
        schedule.name != "bigquery_tsx_ohlcv_canary_schedule"
        for schedule in definitions.defs.schedules
    )
