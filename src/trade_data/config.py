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

    # Phase 3 fails closed because accepted market data must retain raw evidence
    # and replicate to ClickHouse. It is intentionally disabled until both
    # Phase 2 write planes are production-ready.
    phase3_canary_max_instruments: int = Field(default=25, ge=1, le=500)
    phase3_market_data_enabled: bool = False
    phase3_yfinance_adapter_version: str = "yfinance-v1"

    telemetry_release: str = "local"
    otel_enabled: bool = False
    otel_service_name: str = "trade-data"
    otel_exporter_otlp_endpoint: str | None = None

    # Backward-compatible alias for installations using the Phase 9.2A variable.

    upstox_access_token: str | None = None
    data_pipeline_max_concurrent_fetches: int = Field(default=1, ge=1, le=16)
    data_pipeline_throttle_seconds: float = Field(default=0.0, ge=0, le=10)
    ingestion_profile: str = "local"
    provider_rate_limit_backend: str = Field(default="auto", pattern="^(auto|redis|memory|none)$")
    provider_rate_limit_require_redis: bool = False
    upstox_historical_concurrency: int = Field(default=4, ge=1, le=32)
    upstox_rate_per_second: int = Field(default=40, ge=1, le=50)
    upstox_rate_per_minute: int = Field(default=400, ge=1, le=500)
    upstox_rate_per_30_minutes: int = Field(default=1600, ge=1, le=2000)
    yfinance_batch_concurrency: int = Field(default=1, ge=1, le=8)
    yfinance_rate_per_minute: int = Field(default=30, ge=1, le=600)
    yfinance_daily_enabled: bool = False
    yfinance_daily_worker_exchange: str | None = Field(default=None, pattern="^(NSE|TSX|US)$")
    yfinance_adaptive_rate_mode: str = Field(
        default="fixed",
        pattern="^(fixed|observe|adaptive)$",
    )
    yfinance_initial_rpm: int = Field(default=300, ge=1, le=1200)
    yfinance_minimum_rpm: int = Field(default=30, ge=1, le=600)
    yfinance_maximum_rpm: int = Field(default=600, ge=1, le=1200)
    yfinance_initial_concurrency: int = Field(default=4, ge=1, le=32)
    yfinance_maximum_concurrency: int = Field(default=8, ge=1, le=32)
    yfinance_immediate_retry_attempts: int = Field(default=3, ge=1, le=5)
    yfinance_retry_wait_multiplier_seconds: float = Field(default=2.0, ge=0, le=30)
    yfinance_retry_wait_max_seconds: float = Field(default=15.0, ge=0, le=300)
    yfinance_adaptive_evaluation_window_seconds: int = Field(default=60, ge=1, le=3600)
    yfinance_adaptive_healthy_windows_before_increase: int = Field(default=2, ge=1, le=20)
    yfinance_adaptive_increase_rpm: int = Field(default=30, ge=1, le=600)
    yfinance_adaptive_error_threshold: float = Field(default=0.10, ge=0, le=1)
    yfinance_adaptive_cooldown_seconds: int = Field(default=60, ge=1, le=3600)
    yfinance_incremental_overlap_sessions: int = Field(default=5, ge=0, le=30)
    yfinance_provider_grace_minutes: int = Field(default=120, ge=0, le=720)
    yfinance_new_listing_grace_hours: int = Field(default=72, ge=1, le=336)
    yfinance_new_listing_retry_hours: int = Field(default=6, ge=1, le=72)
    yfinance_backfill_years: int = Field(default=10, ge=1, le=20)
    yfinance_work_planner_chunk_size: int = Field(default=250, ge=1, le=2_000)
    yfinance_work_claim_size: int = Field(default=100, ge=1, le=1_000)
    yfinance_work_heartbeat_seconds: int = Field(default=30, ge=5, le=300)
    yfinance_work_stale_minutes: int = Field(default=10, ge=1, le=1_440)
    yfinance_work_max_attempts: int = Field(default=9, ge=1, le=30)
    yfinance_full_us_enabled: bool = False
    yfinance_full_tsx_enabled: bool = False
    yfinance_tsx_canary_enabled: bool = False
    yfinance_tsx_canary_max_symbols: int = Field(default=100, ge=1, le=500)
    yfinance_nse_canary_enabled: bool = False
    yfinance_nse_canary_max_symbols: int = Field(default=100, ge=1, le=5_000)
    yfinance_provider_history_evidence_enabled: bool = False
    yfinance_sparse_history_minimum_expected_rows: int = Field(default=220, ge=1, le=10_000)
    yfinance_sparse_history_maximum_observed_rows: int = Field(default=5, ge=1, le=1_000)
    yfinance_nse_enabled: bool = False
    nse_daily_primary_source: str = Field(default="upstox", pattern="^(upstox|yfinance)$")
    nse_provider_comparison_sessions: int = Field(default=20, ge=5, le=250)
    nse_provider_comparison_minimum_symbols: int = Field(default=100, ge=1)
    nse_provider_comparison_minimum_row_overlap: float = Field(default=0.95, ge=0, le=1)
    nse_provider_comparison_close_tolerance: float = Field(default=0.01, ge=0, le=1)
    nse_provider_comparison_minimum_close_match: float = Field(default=0.98, ge=0, le=1)
    nse_provider_comparison_maximum_session_lag: int = Field(default=1, ge=0, le=10)
    nse_cutover_required_passing_windows: int = Field(default=5, ge=2, le=30)
    tsx_official_issuer_url: str = "https://www.tsx.com/en/resource/571"
    tsx_official_directory_base_url: str = "https://www.tsx.com/json/company-directory"
    tsx_official_timeout_seconds: float = Field(default=30.0, ge=1, le=120)
    tsx_official_retry_attempts: int = Field(default=3, ge=1, le=5)
    equity_universe_minimum_nse_symbols: int = Field(default=1_000, ge=1)
    equity_universe_minimum_tsx_symbols: int = Field(default=500, ge=1)
    equity_universe_minimum_us_symbols: int = Field(default=3_000, ge=1)
    equity_universe_maximum_change_ratio: float = Field(default=0.20, ge=0, le=1)
    equity_universe_missing_snapshots_before_inactive: int = Field(default=2, ge=2, le=10)
    materialized_exchange_sessions_enabled: bool = False
    exchange_session_history_years: int = Field(default=10, ge=1, le=20)
    exchange_session_future_years: int = Field(default=1, ge=1, le=5)
    exchange_session_minimum_open_days_per_year: int = Field(default=220, ge=1, le=366)
    exchange_session_maximum_open_days_per_year: int = Field(default=260, ge=1, le=366)
    exchange_session_shadow_max_discrepancies: int = Field(default=5, ge=0, le=100)
    exchange_session_observed_open_minimum_instruments: int = Field(default=10, ge=1)
    legacy_upstox_nse_enabled: bool = True
    timescale_write_chunk_rows: int = Field(default=1000, ge=100, le=10000)

    nse_ingest_limit: int | None = Field(default=None, ge=1)
    tsx_ingest_limit: int | None = Field(default=None, ge=1)
    universe_refresh_days: int = Field(default=7, ge=1, le=30)
    calendar_refresh_days: int = Field(default=30, ge=1, le=90)
    min_median_dollar_volume: float = Field(default=5_000_000, ge=0)
    bypass_calendar: bool = False

    @field_validator("provider_rate_limit_backend", mode="before")
    @classmethod
    def normalize_rate_limit_backend(cls, value: object) -> object:
        return value.strip().lower() if isinstance(value, str) else value

    @field_validator(
        "app_secret_key",
        "upstox_access_token",
        "clickhouse_password",
        "object_store_access_key_id",
        "object_store_secret_access_key",
        mode="before",
    )
    @classmethod
    def normalize_credentials(cls, value: object) -> object:
        return None if isinstance(value, str) and not value.strip() else value

    @model_validator(mode="after")
    def validate_yfinance_foundation_settings(self) -> Self:
        validate_runtime_configuration(self)
        if self.provider_rate_limit_require_redis and self.provider_rate_limit_backend not in {
            "auto",
            "redis",
        }:
            raise ValueError(
                "PROVIDER_RATE_LIMIT_REQUIRE_REDIS=true requires "
                "PROVIDER_RATE_LIMIT_BACKEND=auto or redis"
            )
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
                "OBJECT_STORE_ACCESS_KEY_ID and OBJECT_STORE_SECRET_ACCESS_KEY are "
                "required when OBJECT_STORE_ENABLED=true"
            )
        if self.phase3_market_data_enabled and not (
            self.object_store_write_enabled and self.clickhouse_write_enabled
        ):
            raise ValueError("Phase 3 market data requires object-store and ClickHouse writes")
        if not (
            self.yfinance_minimum_rpm <= self.yfinance_initial_rpm <= self.yfinance_maximum_rpm
        ):
            raise ValueError("yfinance RPM settings must satisfy minimum <= initial <= maximum")
        if self.yfinance_initial_concurrency > self.yfinance_maximum_concurrency:
            raise ValueError("yfinance concurrency settings must satisfy initial <= maximum")
        if self.yfinance_retry_wait_multiplier_seconds > self.yfinance_retry_wait_max_seconds:
            raise ValueError("yfinance retry waits must satisfy multiplier <= maximum")
        if self.yfinance_work_heartbeat_seconds >= self.yfinance_work_stale_minutes * 60:
            raise ValueError("yfinance work heartbeat must be shorter than the stale-lock timeout")
        if (
            self.exchange_session_minimum_open_days_per_year
            > self.exchange_session_maximum_open_days_per_year
        ):
            raise ValueError("exchange session open-day settings must satisfy minimum <= maximum")
        if (
            self.nse_daily_primary_source == "yfinance"
            and (self.yfinance_daily_enabled or self.yfinance_nse_enabled)
            and not (self.yfinance_daily_enabled and self.yfinance_nse_enabled)
        ):
            raise ValueError(
                "NSE yfinance primary requires YFINANCE_DAILY_ENABLED and YFINANCE_NSE_ENABLED"
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
