from trade_data.config import Settings
from trade_data.dagster.definitions import defs
from trade_data.dagster.schedule_policy import desired_schedule_statuses, schedule_policy


def test_manifest_matches_loaded_nse_definitions():
    policies = {p.schedule_name: p for p in schedule_policy(Settings(_env_file=None))}
    for schedule in defs.schedules:
        policy = policies[schedule.name]
        assert policy.job_name == schedule.job_name
        assert policy.cron_schedule == schedule.cron_schedule
        assert policy.execution_timezone == schedule.execution_timezone
    assert set(policies) == {s.name for s in defs.schedules}


def test_legacy_flags_cannot_enable_removed_jobs():
    settings = Settings(
        _env_file=None,
        nse_daily_enabled=False,
        materialized_exchange_sessions_enabled=False,
        yfinance_daily_enabled=True,
        legacy_upstox_nse_enabled=True,
        yfinance_full_us_enabled=True,
    )
    assert set(desired_schedule_statuses(settings).values()) == {"stopped"}
