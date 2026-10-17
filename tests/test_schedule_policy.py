from trade_data.config import Settings
from trade_data.dagster.schedule_policy import (
    desired_schedule_statuses,
    load_schedule_manifest,
    schedule_policy,
)


def test_removed_registration_flags_cannot_enable_non_daily_schedules() -> None:
    settings = Settings(_env_file=None).model_copy(update={
        "research_pipelines_enabled": True,
        "opportunity_analytics_enabled": True,
        "bigquery_enabled": True,
        "bigquery_production_sync_enabled": True,
    })
    statuses = desired_schedule_statuses(settings)
    assert all(
        token not in name
        for name in statuses
        for token in ("research", "opportunity", "bigquery", "intraday", "minute")
    )


def test_schedule_policy_enables_daily_planners_from_yfinance_flags() -> None:
    settings = Settings(
        _env_file=None,
        yfinance_daily_enabled=True,
        yfinance_nse_enabled=True,
        yfinance_full_tsx_enabled=True,
        yfinance_full_us_enabled=True,
    )

    statuses = desired_schedule_statuses(settings)

    assert statuses["yfinance_tsx_completed_session_work_planner_schedule"] == "running"
    assert statuses["yfinance_us_completed_session_work_planner_schedule"] == "running"
    assert statuses["north_america_daily_yfinance_schedule"] == "stopped"


def test_north_america_post_close_planners_follow_exchange_flags() -> None:
    settings = Settings(
        _env_file=None,
        yfinance_daily_enabled=True,
        yfinance_full_tsx_enabled=True,
        yfinance_full_us_enabled=False,
    )

    statuses = desired_schedule_statuses(settings)

    assert statuses["yfinance_tsx_completed_session_work_planner_schedule"] == "running"
    assert statuses["yfinance_us_completed_session_work_planner_schedule"] == "stopped"


def test_schedule_policy_metadata_matches_dagster_definitions() -> None:
    from trade_data.dagster.definitions import defs

    definitions = {schedule.name: schedule for schedule in defs.schedules}
    policies = {
        policy.schedule_name: policy
        for policy in schedule_policy(Settings(_env_file=None))
    }

    assert definitions.keys() == policies.keys()
    for name, definition in definitions.items():
        policy = policies[name]
        cron = (
            "; ".join(definition.cron_schedule)
            if isinstance(definition.cron_schedule, list)
            else definition.cron_schedule
        )
        assert policy.job_name == definition.job_name
        assert policy.cron_schedule == cron
        assert policy.execution_timezone == definition.execution_timezone


def test_production_manifest_has_required_operational_fields() -> None:
    manifest = load_schedule_manifest()

    assert manifest["environment"] == "production"
    required = {
        "schedule_name",
        "job_name",
        "cron_schedule",
        "execution_timezone",
        "exchange",
        "enabled_when",
        "freshness_sla_minutes",
        "upstream_dependencies",
        "alert_owner",
    }
    assert all(required <= set(row) for row in manifest["schedules"])
    assert all(row["freshness_sla_minutes"] > 0 for row in manifest["schedules"])
    assert all(row["alert_owner"] for row in manifest["schedules"])


def test_daily_data_only_policy_does_not_restart_research_or_other_markets() -> None:
    statuses = desired_schedule_statuses(Settings(
        _env_file=None,
        nse_daily_primary_source="yfinance",
        legacy_upstox_nse_enabled=False,
        yfinance_daily_enabled=True,
        yfinance_nse_enabled=True,
        materialized_exchange_sessions_enabled=True,
        research_pipelines_enabled=False,
        opportunity_analytics_enabled=False,
    ))
    assert {name for name, state in statuses.items() if state == "running"} == {
        "yfinance_daily_work_planner_schedule",
        "yfinance_nse_completed_session_work_planner_schedule",
        "yfinance_daily_work_worker_schedule",
        "nse_universe_refresh_schedule",
        "nse_exchange_sessions_schedule",
    }
