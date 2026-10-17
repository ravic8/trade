"""Endpoint forwarding and failure semantics, without contacting infrastructure."""

import ast
from pathlib import Path

import boto3
import pytest
import redis
from pydantic import ValidationError

from trade_data.config import Settings
from trade_data.data.rate_limits import (
    InMemoryProviderRateLimiter,
    build_provider_rate_limiter,
)
from trade_data.storage.object_store import create_object_store_client


def test_object_adapter_forwards_operator_endpoint_and_region(monkeypatch):
    captured = {}
    sentinel = object()

    def client(service, **kwargs):
        captured.update(service=service, **kwargs)
        return sentinel

    monkeypatch.setattr(boto3, "client", client)
    settings = Settings(
        _env_file=None,
        object_store_enabled=True,
        object_store_endpoint_url="https://operator-objects.example:9443",
        object_store_region="ap-south-1",
        object_store_access_key_id="contract-key",
        object_store_secret_access_key="contract-secret",
    )
    assert create_object_store_client(settings) is sentinel
    assert captured["service"] == "s3"
    assert captured["endpoint_url"] == settings.object_store_endpoint_url
    assert captured["region_name"] == settings.object_store_region


def test_redis_adapter_forwards_operator_endpoint_and_checks_service(monkeypatch):
    events = []

    class Client:
        def ping(self):
            events.append("ping")

        def register_script(self, _script):
            return lambda **_kwargs: 0

    def from_url(url, **_kwargs):
        events.append(url)
        return Client()

    monkeypatch.setattr(redis.Redis, "from_url", from_url)
    settings = Settings(
        _env_file=None,
        provider_rate_limit_backend="redis",
        redis_url="rediss://operator-redis.example:6380/2",
    )
    build_provider_rate_limiter(settings)
    assert events == [settings.redis_url, "ping"]


@pytest.mark.parametrize(
    "backend,required,fails",
    [
        ("redis", False, True),
        ("auto", True, True),
        ("auto", False, False),
    ],
)
def test_redis_failure_obeys_declared_requirement(monkeypatch, backend, required, fails):
    def offline(*_args, **_kwargs):
        raise ConnectionError("contract endpoint unavailable")

    monkeypatch.setattr(redis.Redis, "from_url", offline)
    settings = Settings(
        _env_file=None,
        provider_rate_limit_backend=backend,
        provider_rate_limit_require_redis=required,
    )
    if fails:
        with pytest.raises(ConnectionError):
            build_provider_rate_limiter(settings)
    else:
        assert isinstance(build_provider_rate_limiter(settings), InMemoryProviderRateLimiter)


@pytest.mark.parametrize("backend", ["memory", "none", "typo"])
def test_declared_distributed_requirement_cannot_silently_disable_redis(backend):
    with pytest.raises(ValidationError):
        Settings(
            _env_file=None,
            provider_rate_limit_backend=backend,
            provider_rate_limit_require_redis=True,
        )


def test_daily_domain_boundary_has_no_deployment_imports_or_host_paths():
    root = Path(__file__).resolve().parents[1] / "src/trade_data"
    for relative in (
        "pipelines/yfinance_daily.py",
        "pipelines/yfinance_work_queue.py",
        "storage/daily_contract.py",
        "data/daily_provider.py",
    ):
        tree = ast.parse((root / relative).read_text())
        for node in ast.walk(tree):
            if isinstance(node, ast.ImportFrom):
                assert not (node.module or "").startswith(("deploy", "colima"))
            if isinstance(node, ast.Constant) and isinstance(node.value, str):
                assert "/Users/" not in node.value
                assert "/opt/trade" not in node.value
                assert "http://postgres" not in node.value
                assert "http://minio" not in node.value
