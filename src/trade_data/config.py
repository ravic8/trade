from pathlib import Path
from typing import Literal, Self

from pydantic import Field, ValidationError, field_validator, model_validator
from pydantic_settings import BaseSettings, SettingsConfigDict, SettingsError

from trade_data.config_validation import ConfigurationError, validate_runtime_configuration


class Settings(BaseSettings):
    model_config = SettingsConfigDict(
        env_file=".env", env_file_encoding="utf-8", extra="ignore", hide_input_in_errors=True
    )

    app_env: str = "local"
    data_dir: Path = Path("data")
    dagster_readonly_home: Path | None = None

    api_cors_origins: str = "http://localhost:5173,http://localhost:8000"
    admin_emails: str = ""
    admin_email_headers: str = (
        "cf-access-authenticated-user-email,x-forwarded-email,x-authenticated-user-email"
    )
    app_secret_key: str | None = None

    database_url: str = "postgresql+psycopg://trade:trade@localhost:5432/trade_research"
    redis_url: str = "redis://localhost:6379/0"

    # Phase 2 research storage is additive and remains disabled until its
    # production canary and restore evidence have been accepted.
    clickhouse_enabled: bool = False
    clickhouse_write_enabled: bool = False
    clickhouse_host: str = "localhost"
    clickhouse_port: int = Field(default=8123, ge=1, le=65535)
    clickhouse_database: str = Field(
        default="research",
        pattern=r"^[A-Za-z_][A-Za-z0-9_]*$",
    )
    clickhouse_username: str = "api_reader"
    clickhouse_password: str | None = None
    clickhouse_secure: bool = False
    clickhouse_connect_timeout_seconds: float = Field(default=5.0, gt=0, le=60)
    clickhouse_query_timeout_seconds: float = Field(default=30.0, gt=0, le=300)

    object_store_enabled: bool = False
    object_store_write_enabled: bool = False
    object_store_endpoint_url: str | None = None
    object_store_region: str = "us-east-1"
    object_store_access_key_id: str | None = None
    object_store_secret_access_key: str | None = None
    object_store_raw_bucket: str = "trade-raw"
    object_store_datasets_bucket: str = "trade-datasets"
    object_store_exports_bucket: str = "trade-exports"
    object_store_server_side_encryption: Literal["AES256", "aws:kms"] = "AES256"

    telemetry_release: str = "local"
    otel_enabled: bool = False
    otel_service_name: str = "trade-data"
    otel_exporter_otlp_endpoint: str | None = None
    nse_daily_enabled: bool = True
    nse_publication_grace_minutes: int = Field(default=120, ge=0, le=720)
    nse_retry_lookback_days: int = Field(default=7, ge=1, le=31)
    nse_request_interval_seconds: float = Field(default=1.0, ge=0.5, le=60)
    materialized_exchange_sessions_enabled: bool = True
    exchange_session_history_years: int = Field(default=1, ge=1, le=20)
    exchange_session_future_years: int = Field(default=1, ge=1, le=2)
    exchange_session_minimum_open_days_per_year: int = Field(default=220, ge=1, le=366)
    exchange_session_maximum_open_days_per_year: int = Field(default=260, ge=1, le=366)
    exchange_session_shadow_max_discrepancies: int = Field(default=5, ge=0, le=100)
    exchange_session_observed_open_minimum_instruments: int = Field(default=10, ge=1)
    calendar_refresh_days: int = Field(default=30, ge=1, le=90)
    timescale_write_chunk_rows: int = Field(default=1000, ge=100, le=10000)

    @field_validator(
        "app_secret_key",
        "clickhouse_password",
        "object_store_access_key_id",
        "object_store_secret_access_key",
        mode="before",
    )
    @classmethod
    def normalize_credentials(cls, value: object) -> object:
        return None if isinstance(value, str) and not value.strip() else value

    @model_validator(mode="after")
    def validate_settings(self) -> Self:
        validate_runtime_configuration(self)
        if self.clickhouse_write_enabled and not self.clickhouse_enabled:
            raise ValueError("ClickHouse writes require CLICKHOUSE_ENABLED=true")
        if self.clickhouse_enabled and not self.clickhouse_password:
            raise ValueError("CLICKHOUSE_PASSWORD is required when CLICKHOUSE_ENABLED=true")
        if self.object_store_write_enabled and not self.object_store_enabled:
            raise ValueError("Object-store writes require OBJECT_STORE_ENABLED=true")
        if self.object_store_enabled and not (
            self.object_store_access_key_id and self.object_store_secret_access_key
        ):
            raise ValueError(
                "OBJECT_STORE_ACCESS_KEY_ID and OBJECT_STORE_SECRET_ACCESS_KEY "
                "are required when enabled"
            )
        if self.otel_enabled and not self.otel_exporter_otlp_endpoint:
            raise ValueError("OTEL_EXPORTER_OTLP_ENDPOINT is required when OTEL_ENABLED=true")
        return self


def get_settings() -> Settings:
    try:
        return Settings()
    except ValidationError as exc:
        details = []
        for error in exc.errors(include_input=False, include_context=False, include_url=False):
            location = error["loc"]
            # Model-validator messages are written by Trade and contain setting names
            # only. Field parser messages are replaced, so supplied values stay private.
            if not location:
                details.append(error["msg"].removeprefix("Value error, "))
            else:
                name = str(location[0]).upper()
                details.append(f"{name}: invalid type, format, or allowed range")
        raise ConfigurationError("Invalid configuration: " + "; ".join(details)) from None
    except SettingsError:
        raise ConfigurationError("Invalid configuration: check environment value formats") from None
