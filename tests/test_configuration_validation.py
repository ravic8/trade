from __future__ import annotations

import os
import subprocess
import sys
from pathlib import Path

import pytest
from pydantic import ValidationError

from trade_data.config import Settings, get_settings
from trade_data.config_validation import ConfigurationError


@pytest.mark.parametrize(
    "field,value,label",
    [
        ("database_url", "", "DATABASE_URL"),
        (
            "database_url",
            "postgresql+psycopg://user:private-marker@host:notaport/db",
            "DATABASE_URL",
        ),
        ("database_url", "postgresql+psycopg://host", "DATABASE_URL"),
        ("redis_url", "http://host:6379", "REDIS_URL"),
        ("redis_url", "redis://host/not-a-db", "REDIS_URL"),
        ("redis_url", "redis://:private-marker@host:99999/0", "REDIS_URL"),
    ],
)
def test_invalid_core_endpoints_fail_without_values(field, value, label):
    with pytest.raises(ValidationError) as error:
        Settings(_env_file=None, **{field: value})
    rendered = str(error.value)
    assert label in rendered
    assert "private-marker" not in rendered
    assert "input_value" not in rendered


def test_multiple_invalid_endpoints_are_reported_together():
    with pytest.raises(ValidationError) as error:
        Settings(_env_file=None, database_url="", redis_url="")
    assert "DATABASE_URL" in str(error.value)
    assert "REDIS_URL" in str(error.value)


@pytest.mark.parametrize(
    "settings,label",
    [
        (
            {"clickhouse_enabled": True, "clickhouse_password": "password", "clickhouse_host": ""},
            "CLICKHOUSE_HOST",
        ),
        (
            {"otel_enabled": True, "otel_exporter_otlp_endpoint": "bad"},
            "OTEL_EXPORTER_OTLP_ENDPOINT",
        ),
        (
            {
                "object_store_enabled": True,
                "object_store_access_key_id": "key",
                "object_store_secret_access_key": "secret",
                "object_store_endpoint_url": "bad",
            },
            "OBJECT_STORE_ENDPOINT_URL",
        ),
    ],
)
def test_enabled_capabilities_require_valid_configuration(settings, label):
    with pytest.raises(ValidationError) as error:
        Settings(_env_file=None, **settings)
    assert label in str(error.value)


def test_disabled_capabilities_do_not_require_endpoints_or_credentials():
    Settings(
        _env_file=None,
        filing_enabled=False,
        filing_artifact_backend="s3",
        filing_s3_endpoint_url="bad",
        qdrant_url="bad",
        object_store_endpoint_url="bad",
        otel_exporter_otlp_endpoint="bad",
        clickhouse_host="",
    )


def test_regional_s3_default_and_secure_core_endpoints_are_supported():
    Settings(
        _env_file=None,
        redis_url="rediss://user:secret@host:6380/1",
        object_store_enabled=True,
        object_store_access_key_id="key",
        object_store_secret_access_key="secret",
        object_store_endpoint_url=None,
    )


def test_settings_loader_sanitizes_field_parse_failures(monkeypatch):
    monkeypatch.setenv("CLICKHOUSE_PORT", "private-marker")
    with pytest.raises(ConfigurationError) as error:
        get_settings()
    assert "CLICKHOUSE_PORT" in str(error.value)
    assert "private-marker" not in str(error.value)
    assert error.value.__suppress_context__


@pytest.mark.parametrize(
    "module",
    [
        "trade_data.api.app",
        "trade_data.dagster.definitions",
        "trade_data.cli",
    ],
)
def test_invalid_configuration_rejects_entrypoint_before_connections(module, tmp_path):
    env = os.environ.copy()
    env.update(
        DATABASE_URL="postgresql+psycopg://user:private-marker@host:bad/db",
        REDIS_URL="redis://host:6379/0",
        PYTHONPATH=str(Path(__file__).resolve().parents[1] / "src"),
    )
    result = subprocess.run(
        [sys.executable, "-c", f"import {module}"],
        cwd=tmp_path,
        env=env,
        text=True,
        capture_output=True,
        timeout=30,
    )
    assert result.returncode != 0
    output = result.stdout + result.stderr
    assert "DATABASE_URL" in output
    assert "private-marker" not in output
    assert "ConfigurationError" in output


def test_blank_credentials_are_missing_not_configured():
    with pytest.raises(ValidationError, match="CLICKHOUSE_PASSWORD"):
        Settings(_env_file=None, clickhouse_enabled=True, clickhouse_password="   ")


def test_nonblank_credentials_preserve_exact_value():
    settings = Settings(_env_file=None, clickhouse_password=" pass with spaces ")
    assert settings.clickhouse_password == " pass with spaces "
