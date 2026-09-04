from trade_research.config import Settings
from trade_research.dagster.schedule_policy import (
    desired_schedule_statuses,
    load_schedule_manifest,
    schedule_policy,
)


def test_schedule_policy_keeps_upstox_daily_research_running() -> None:
    settings = Settings(
        _env_file=None,
        nse_daily_primary_source="upstox",
        legacy_upstox_nse_enabled=True,
    )

    statuses = desired_schedule_statuses(settings)

    assert statuses["daily_research_schedule"] == "running"


def test_schedule_policy_enables_exchange_targets_from_yfinance_flags() -> None:
    settings = Settings(
        _env_file=None,
        yfinance_daily_enabled=True,
        yfinance_nse_enabled=True,
        yfinance_full_tsx_enabled=True,
        yfinance_full_us_enabled=True,
    )

    statuses = desired_schedule_statuses(settings)

    assert statuses["nse_completed_session_opportunity_targets_schedule"] == "running"
    assert statuses["yfinance_tsx_completed_session_work_planner_schedule"] == "running"
    assert statuses["yfinance_us_completed_session_work_planner_schedule"] == "running"
    assert statuses["tsx_completed_session_opportunity_targets_schedule"] == "running"
    assert statuses["us_completed_session_opportunity_targets_schedule"] == "running"
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
    from trade_research.dagster.definitions import defs

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
