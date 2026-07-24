from pathlib import Path
from typing import Literal, Self

from pydantic import Field, model_validator
from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    model_config = SettingsConfigDict(env_file=".env", env_file_encoding="utf-8", extra="ignore")

    app_env: str = "local"
    data_dir: Path = Path("data")
    dagster_readonly_home: Path | None = None

    openai_api_key: str | None = None
    openai_base_url: str = "https://api.openai.com/v1"
    openai_embedding_model: str = "text-embedding-3-small"
    gemini_api_key: str | None = None
    gemini_base_url: str = "https://generativelanguage.googleapis.com/v1beta"

    qdrant_url: str = "http://localhost:6333"
    qdrant_api_key: str | None = None
    qdrant_collection: str = "market_research_documents"
    chat_enabled: bool = False
    chat_strict_citation_required: bool = True
    chat_default_exchange_scope: str = "BOTH"
    chat_max_latency_ms: int = Field(default=8000, ge=500, le=30000)
    chat_max_symbols_per_request: int = Field(default=200, ge=1, le=500)
    chat_max_lookback_hours: int = Field(default=72, ge=1, le=720)
    chat_max_research_top_k: int = Field(default=12, ge=1, le=50)
    chat_quality_nse_complete_threshold: float = Field(default=0.95, ge=0, le=1)
    chat_quality_tsx_complete_threshold: float = Field(default=0.90, ge=0, le=1)
    chat_stale_intervals_threshold: int = Field(default=2, ge=1, le=24)
    chat_planner_model: str = "gemini-2.5-flash"
    chat_answer_model: str = "gemini-2.5-flash"
    chat_use_llm_answer: bool = False
    chat_answer_max_output_tokens: int = Field(default=700, ge=1, le=4096)
    chat_llm_timeout_seconds: float = Field(default=20.0, ge=1, le=120)
    chat_llm_retry_attempts: int = Field(default=3, ge=1, le=5)
    chat_llm_retry_base_seconds: float = Field(default=0.5, ge=0, le=10)
    chat_llm_thinking_budget: int = Field(default=0, ge=-1, le=24576)
    api_cors_origins: str = "http://localhost:5173,http://localhost:8000"
    chat_rate_limit_enabled: bool = True
    chat_rate_limit_requests: int = Field(default=12, ge=1, le=1000)
    chat_rate_limit_window_seconds: int = Field(default=60, ge=1, le=3600)
    chat_rate_limit_trust_forwarded_for: bool = False
    admin_emails: str = ""
    admin_email_headers: str = (
        "cf-access-authenticated-user-email,x-forwarded-email,x-authenticated-user-email"
    )
    app_secret_key: str | None = None

    database_url: str = "postgresql+psycopg://trade:trade@localhost:5432/trade_research"
    redis_url: str = "redis://localhost:6379/0"

    filing_enabled: bool = True
    filing_default_workspace_id: str = "default"
    filing_manifest_path: Path = Path("data/filings/nse/INFY/manifest.json")
    filing_artifact_backend: Literal["local", "s3"] = "local"
    filing_artifact_dir: Path = Path("data/filings/artifacts")
    filing_s3_bucket: str = "lens-filings"
    filing_s3_prefix: str = "parsed"
    filing_s3_endpoint_url: str | None = None
    filing_s3_region: str = "ap-south-1"
    filing_s3_access_key_id: str | None = None
    filing_s3_secret_access_key: str | None = None
    filing_queue_mode: Literal["inline", "celery"] = "inline"
    filing_queue_name: str = "filing_intelligence"
    filing_worker_concurrency: int = Field(default=2, ge=1, le=16)
    filing_worker_lease_seconds: int = Field(default=300, ge=30, le=3600)
    filing_worker_heartbeat_seconds: int = Field(default=30, ge=5, le=300)
    filing_checkpoint_database_url: str | None = None
    filing_checkpoint_schema: str = Field(
        default="filing_checkpoints",
        pattern=r"^[A-Za-z_][A-Za-z0-9_]*$",
    )
    filing_require_workspace_header: bool = False
    filing_auto_approve_xbrl: bool = True
    filing_force_human_review: bool = False
    filing_min_auto_approve_confidence: float = Field(default=0.98, ge=0, le=1)
    filing_parse_min_quality: float = Field(default=0.60, ge=0, le=1)
    filing_max_document_bytes: int = Field(
        default=100 * 1024 * 1024,
        ge=1 * 1024 * 1024,
        le=500 * 1024 * 1024,
    )
    filing_pdf_max_pages: int = Field(default=600, ge=1, le=2_000)
    filing_pdf_claim_limit: int = Field(default=60, ge=1, le=500)
    filing_extractor_version: str = "nse-filing-extractor-v1"
    filing_golden_dataset_path: Path = Path(
        "evaluations/filings/infy_m1_golden.json"
    )
    filing_index_enabled: bool = False
    filing_qdrant_collection: str = "filing_evidence_v1"
    filing_index_version: str = "filing-index-v1"
    filing_chunk_size: int = Field(default=2_500, ge=500, le=10_000)
    filing_chunk_overlap: int = Field(default=250, ge=0, le=2_000)
    filing_embedding_batch_size: int = Field(default=64, ge=1, le=256)
    filing_embedding_vector_size: int = Field(default=1_536, ge=128, le=8_192)
    filing_index_max_chunks: int = Field(default=1_500, ge=1, le=20_000)

    langfuse_enabled: bool = False
    langfuse_public_key: str | None = None
    langfuse_secret_key: str | None = None
    langfuse_base_url: str = "https://cloud.langfuse.com"
    langfuse_sample_rate: float = Field(default=1.0, ge=0, le=1)
    telemetry_release: str = "local"
    otel_enabled: bool = False
    otel_service_name: str = "trade-research"
    otel_exporter_otlp_endpoint: str | None = None

    bigquery_enabled: bool = False
    bigquery_canary_enabled: bool = False
    bigquery_production_sync_enabled: bool = False
    bigquery_project_id: str | None = None
    bigquery_core_dataset: str = Field(
        default="trade_chain8_analytics",
        pattern=r"^[A-Za-z_][A-Za-z0-9_]*$",
    )
    bigquery_reporting_dataset: str = Field(
        default="trade_chain8_reporting",
        pattern=r"^[A-Za-z_][A-Za-z0-9_]*$",
    )
    # Backward-compatible alias for installations using the Phase 9.2A variable.
    bigquery_dataset: str | None = Field(default=None, pattern=r"^[A-Za-z_][A-Za-z0-9_]*$")
    bigquery_location: str = "US"
    bigquery_auth_method: str = Field(
        default="adc",
        pattern="^(adc|service_account_file)$",
    )
    bigquery_credentials_path: Path | None = None
    bigquery_expected_service_account_email: str | None = None
    bigquery_backfill_chunk_size: int = Field(default=10_000, ge=100, le=100_000)
    bigquery_retry_attempts: int = Field(default=3, ge=1, le=10)

    polygon_api_key: str | None = None
    upstox_access_token: str | None = None
    data_pipeline_max_concurrent_fetches: int = Field(default=1, ge=1, le=16)
    data_pipeline_throttle_seconds: float = Field(default=0.0, ge=0, le=10)
    ingestion_profile: str = "local"
    provider_rate_limit_backend: str = "auto"
    provider_rate_limit_require_redis: bool = False
    upstox_historical_concurrency: int = Field(default=4, ge=1, le=32)
    upstox_rate_per_second: int = Field(default=40, ge=1, le=50)
    upstox_rate_per_minute: int = Field(default=400, ge=1, le=500)
    upstox_rate_per_30_minutes: int = Field(default=1600, ge=1, le=2000)
    yfinance_batch_concurrency: int = Field(default=1, ge=1, le=8)
    yfinance_rate_per_minute: int = Field(default=30, ge=1, le=600)
    yfinance_daily_enabled: bool = False
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
    yfinance_sparse_history_minimum_expected_rows: int = Field(
        default=220, ge=1, le=10_000
    )
    yfinance_sparse_history_maximum_observed_rows: int = Field(
        default=5, ge=1, le=1_000
    )
    yfinance_nse_enabled: bool = False
    opportunity_minimum_session_coverage: float = Field(default=0.95, gt=0, le=1)
    nse_daily_primary_source: str = Field(default="upstox", pattern="^(upstox|yfinance)$")
    nse_provider_comparison_sessions: int = Field(default=20, ge=5, le=250)
    nse_provider_comparison_minimum_symbols: int = Field(default=100, ge=1)
    nse_provider_comparison_minimum_row_overlap: float = Field(
        default=0.95, ge=0, le=1
    )
    nse_provider_comparison_close_tolerance: float = Field(default=0.01, ge=0, le=1)
    nse_provider_comparison_minimum_close_match: float = Field(
        default=0.98, ge=0, le=1
    )
    nse_provider_comparison_maximum_session_lag: int = Field(default=1, ge=0, le=10)
    tsx_official_issuer_url: str = "https://www.tsx.com/en/resource/571"
    tsx_official_directory_base_url: str = (
        "https://www.tsx.com/json/company-directory"
    )
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
    forex_pipelines_enabled: bool = False
    dukascopy_historical_concurrency: int = Field(default=2, ge=1, le=16)
    dukascopy_rate_per_minute: int = Field(default=60, ge=1, le=600)
    timescale_write_chunk_rows: int = Field(default=1000, ge=100, le=10000)

    hourly_realtime_lookback_days: int = Field(default=1, ge=1, le=60)
    hourly_history_lookback_days: int = Field(default=10, ge=1, le=60)
    hourly_backlog_enabled: bool = True
    hourly_backlog_scan_days: int = Field(default=10, ge=1, le=60)
    hourly_backlog_coverage_threshold: float = Field(default=0.5, ge=0, le=1)
    hourly_backlog_max_windows_per_tick: int = Field(default=1, ge=1, le=24)
    hourly_backlog_max_attempts: int = Field(default=3, ge=1, le=20)
    hourly_backlog_min_candle_lag_minutes: int = Field(default=20, ge=0, le=180)
    hourly_backlog_stale_recovery_minutes: int = Field(default=30, ge=5, le=1440)
    nse_ingest_limit: int | None = Field(default=None, ge=1)
    tsx_ingest_limit: int | None = Field(default=None, ge=1)
    universe_refresh_days: int = Field(default=7, ge=1, le=30)
    calendar_refresh_days: int = Field(default=30, ge=1, le=90)
    feed_health_failure_threshold: int = Field(default=5, ge=1, le=20)
    feed_health_max_backoff_hours: int = Field(default=24, ge=1, le=168)
    feed_health_unsupported_retry_days: int = Field(default=7, ge=1, le=90)
    min_median_dollar_volume: float = Field(default=5_000_000, ge=0)
    bypass_calendar: bool = False

    @model_validator(mode="after")
    def validate_yfinance_foundation_settings(self) -> Self:
        if self.bigquery_enabled and not self.bigquery_project_id:
            raise ValueError("BIGQUERY_PROJECT_ID is required when BIGQUERY_ENABLED=true")
        if self.bigquery_dataset:
            self.bigquery_core_dataset = self.bigquery_dataset
        if self.bigquery_core_dataset == self.bigquery_reporting_dataset:
            raise ValueError("BigQuery core and reporting datasets must be different")
        if (self.bigquery_canary_enabled or self.bigquery_production_sync_enabled) and not (
            self.bigquery_enabled
        ):
            raise ValueError("BigQuery canary/production gates require BIGQUERY_ENABLED=true")
        if self.bigquery_enabled and self.bigquery_auth_method == "service_account_file" and not (
            self.bigquery_credentials_path
        ):
            raise ValueError(
                "BIGQUERY_CREDENTIALS_PATH is required for service_account_file authentication"
            )
        if (
            self.bigquery_enabled
            and self.bigquery_auth_method == "service_account_file"
            and not self.bigquery_expected_service_account_email
        ):
            raise ValueError(
                "BIGQUERY_EXPECTED_SERVICE_ACCOUNT_EMAIL is required for "
                "service_account_file authentication"
            )
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
        if self.nse_daily_primary_source == "yfinance" and not (
            self.yfinance_daily_enabled and self.yfinance_nse_enabled
        ):
            raise ValueError(
                "NSE yfinance primary requires YFINANCE_DAILY_ENABLED and "
                "YFINANCE_NSE_ENABLED"
            )
        if self.filing_worker_heartbeat_seconds >= self.filing_worker_lease_seconds:
            raise ValueError("filing worker heartbeat must be shorter than its lease")
        if self.langfuse_enabled and not (
            self.langfuse_public_key and self.langfuse_secret_key
        ):
            raise ValueError(
                "LANGFUSE_PUBLIC_KEY and LANGFUSE_SECRET_KEY are required "
                "when LANGFUSE_ENABLED=true"
            )
        if self.filing_chunk_overlap >= self.filing_chunk_size:
            raise ValueError("filing chunk overlap must be smaller than chunk size")
        if self.filing_index_enabled and not self.openai_api_key:
            raise ValueError(
                "OPENAI_API_KEY is required when FILING_INDEX_ENABLED=true"
            )
        if self.filing_artifact_backend == "s3" and not (
            self.filing_s3_access_key_id and self.filing_s3_secret_access_key
        ):
            raise ValueError(
                "FILING_S3_ACCESS_KEY_ID and FILING_S3_SECRET_ACCESS_KEY are "
                "required when FILING_ARTIFACT_BACKEND=s3"
            )
        if self.otel_enabled and not self.otel_exporter_otlp_endpoint:
            raise ValueError(
                "OTEL_EXPORTER_OTLP_ENDPOINT is required when OTEL_ENABLED=true"
            )
        return self


def get_settings() -> Settings:
    return Settings()
