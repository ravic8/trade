import pytest

from trade_data import runtime_contract
from trade_data.config import Settings
from trade_data.runtime_contract import inspect_runtime


def test_doctor_does_not_expose_connection_strings_or_tokens() -> None:
    settings = Settings(
        _env_file=None,
        database_url="postgresql+psycopg://user:private-db-pass@localhost:5432/trade",
        redis_url="redis://:private-redis-pass@localhost:6379/0",
        upstox_access_token="private-provider-token",
    )
    report = inspect_runtime(settings).as_dict()
    rendered = str(report)
    assert report["ready"] is True
    assert "private-db-pass" not in rendered
    assert "private-redis-pass" not in rendered
    assert "private-provider-token" not in rendered
    assert (
        next(check for check in report["checks"] if check["name"] == "daily_provider")["status"]
        == "configured"
    )


def test_doctor_marks_missing_provider_without_blocking_local_startup() -> None:
    settings = Settings(_env_file=None, yfinance_daily_enabled=False, upstox_access_token=None)
    report = inspect_runtime(settings).as_dict()
    assert report["ready"] is True
    assert (
        next(check for check in report["checks"] if check["name"] == "daily_provider")["status"]
        == "configured"
    )


def test_doctor_fails_closed_when_required_services_are_unreachable(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    def unavailable(*_args: object, **_kwargs: object) -> None:
        raise RuntimeError("private connection detail")

    monkeypatch.setattr(runtime_contract, "create_engine", unavailable)
    monkeypatch.setattr(runtime_contract.Redis, "from_url", unavailable)
    report = inspect_runtime(Settings(_env_file=None), check_services=True).as_dict()
    assert report["ready"] is False
    assert "private connection detail" not in str(report)
    assert [check["status"] for check in report["checks"][:2]] == ["fail", "fail"]
