from pathlib import Path

from pydantic import Field
from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    model_config = SettingsConfigDict(env_file=".env", env_file_encoding="utf-8", extra="ignore")

    app_env: str = "local"
    data_dir: Path = Path("data")

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
        "cf-access-authenticated-user-email,"
        "x-forwarded-email,"
        "x-authenticated-user-email"
    )
    app_secret_key: str | None = None

    database_url: str = "postgresql+psycopg://trade:trade@localhost:5432/trade_research"
    redis_url: str = "redis://localhost:6379/0"

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


def get_settings() -> Settings:
    return Settings()
