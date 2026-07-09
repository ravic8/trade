from datetime import date, datetime
from typing import Any, Literal

from pydantic import BaseModel, Field, HttpUrl


class Symbol(BaseModel):
    symbol: str
    exchange: str
    yahoo_symbol: str | None = None
    name: str | None = None
    currency: str | None = None
    source: str
    source_url: str | None = None


class MarketDataQualityReport(BaseModel):
    ticker: str
    rows: int
    start_date: date | None = None
    end_date: date | None = None
    missing_ohlcv_rows: int = 0
    zero_or_negative_close_rows: int = 0
    zero_volume_rows: int = 0
    warnings: list[str] = Field(default_factory=list)


class ResearchDocument(BaseModel):
    id: str
    symbol: str | None = None
    exchange: str | None = None
    source_type: str
    title: str | None = None
    url: HttpUrl | None = None
    published_at: datetime | None = None
    fetched_at: datetime
    text: str
    metadata: dict[str, Any] = Field(default_factory=dict)


class ScreenerResult(BaseModel):
    ticker: str
    strategy: str
    matched_at: datetime
    metrics: dict[str, int | float | str]


ExchangeCode = Literal["NSE", "TSX", "BOTH"]
QualityBadge = Literal["complete", "partial", "stale"]
CitationType = Literal["timescale_query", "qdrant_chunk"]
FeedbackRating = Literal["up", "down"]
IntentType = Literal[
    "smalltalk_or_identity",
    "price_lookup",
    "session_summary",
    "relative_performance",
    "data_quality_check",
    "research_lookup",
    "hybrid_explain",
]


class ChatContext(BaseModel):
    exchange: ExchangeCode = "BOTH"
    symbols: list[str] = Field(default_factory=list, max_length=200)
    timezone: str | None = None


class ChatOptions(BaseModel):
    max_latency_ms: int = Field(default=8000, ge=500, le=30000)
    strict_quality: bool = True


class ChatQueryRequest(BaseModel):
    message: str = Field(min_length=1, max_length=4000)
    context: ChatContext = Field(default_factory=ChatContext)
    options: ChatOptions = Field(default_factory=ChatOptions)
    session_id: str | None = None
    user_id: str | None = None


class FreshnessInfo(BaseModel):
    market_data_as_of: datetime | None = None
    research_data_as_of: datetime | None = None


class ChatAnswer(BaseModel):
    text: str
    quality_badge: QualityBadge
    freshness: FreshnessInfo
    warnings: list[str] = Field(default_factory=list)
    follow_ups: list[str] = Field(default_factory=list)


class Citation(BaseModel):
    id: str
    type: CitationType
    label: str
    provenance_ref: str


class ChatQueryResponse(BaseModel):
    response_id: str
    session_id: str | None = None
    answer: ChatAnswer
    citations: list[Citation] = Field(default_factory=list)
    trace_id: str


class ProvenanceTimeRange(BaseModel):
    start: datetime | None = None
    end: datetime | None = None


class ProvenanceTimescale(BaseModel):
    provenance_ref: str
    template_id: str
    parameters: dict[str, Any] = Field(default_factory=dict)
    time_range: ProvenanceTimeRange | None = None
    row_count: int = Field(ge=0)
    executed_at: datetime


class ProvenanceQdrant(BaseModel):
    provenance_ref: str
    collection: str
    doc_id: str
    chunk_id: str
    score: float
    as_of_date: date | None = None
    title: str | None = None


class SourcesPayload(BaseModel):
    timescale: list[ProvenanceTimescale] = Field(default_factory=list)
    qdrant: list[ProvenanceQdrant] = Field(default_factory=list)


class ChatSourcesResponse(BaseModel):
    response_id: str
    sources: SourcesPayload


class ChatFeedbackRequest(BaseModel):
    response_id: str
    rating: FeedbackRating
    reason: str | None = Field(default=None, max_length=1000)
    user_id: str | None = None


class ChatFeedbackResponse(BaseModel):
    status: Literal["accepted"]


class ProviderHistoricalCapability(BaseModel):
    unit: Literal["minutes", "hours", "days", "weeks", "months"]
    interval_min: int = Field(ge=1)
    interval_max: int = Field(ge=1)
    available_from: date
    max_window: str | None = None
    notes: str | None = None


class ProviderRateLimits(BaseModel):
    standard_api_per_second: int = Field(ge=1)
    standard_api_per_minute: int = Field(ge=1)
    standard_api_per_30_minutes: int = Field(ge=1)


class ProviderCapabilityResponse(BaseModel):
    provider: str
    api_version: str
    source_url: str
    historical: list[ProviderHistoricalCapability]
    rate_limits: ProviderRateLimits
    notes: list[str] = Field(default_factory=list)


class DataCoveragePreviewRequest(BaseModel):
    provider: Literal["upstox"] = "upstox"
    exchange: Literal["NSE"] = "NSE"
    symbols: list[str] = Field(min_length=1, max_length=200)
    unit: Literal["days"] = "days"
    interval: int = Field(default=1, ge=1)
    start_date: date
    end_date: date


class DataPipelineRequest(DataCoveragePreviewRequest):
    steps: list[Literal["fetch_ohlcv", "validate_ohlcv"]] = Field(
        default_factory=lambda: ["fetch_ohlcv", "validate_ohlcv"]
    )
    mode: Literal["incremental_missing_only"] = "incremental_missing_only"


class DataPipelineHealthResponse(BaseModel):
    provider: Literal["upstox"]
    exchange: Literal["NSE"]
    daily_ohlcv_enabled: bool
    upstox_access_token_configured: bool
    max_concurrent_fetches: int = Field(ge=1)
    checked_at: datetime


class DataCoveragePreviewTask(BaseModel):
    symbol: str
    trading_symbol: str
    instrument_key: str
    fetch_start: date
    fetch_end: date
    missing_rows: int = Field(ge=0)
    status: Literal["queued"]


class DataCoveragePreviewResponse(BaseModel):
    provider: str
    exchange: str
    unit: str
    interval: int
    start_date: date
    end_date: date
    symbols_requested: int = Field(ge=0)
    symbols_resolved: int = Field(ge=0)
    unresolved_symbols: list[str] = Field(default_factory=list)
    ambiguous_symbols: list[str] = Field(default_factory=list)
    expected_rows: int = Field(ge=0)
    already_present_rows: int = Field(ge=0)
    missing_rows: int = Field(ge=0)
    estimated_provider_calls: int = Field(ge=0)
    tasks: list[DataCoveragePreviewTask] = Field(default_factory=list)
    warnings: list[str] = Field(default_factory=list)


class DataPipelineRunSummary(BaseModel):
    id: str
    name: str
    status: str
    exchange: str
    source: str
    started_at: datetime
    finished_at: datetime | None = None
    duration_seconds: int | None = None
    items_requested: int = Field(ge=0)
    items_processed: int = Field(ge=0)
    items_succeeded: int = Field(ge=0)
    items_failed: int = Field(ge=0)
    error_message: str | None = None
    run_metadata: dict[str, Any] = Field(default_factory=dict)


class DailyOhlcvFetchCoverageRow(BaseModel):
    run_id: str
    instrument_key: str
    symbol: str
    source: str
    exchange: str
    latest_stored_date: date | None = None
    fetch_start: date | None = None
    fetch_end: date
    should_fetch: bool
    status: str
    rows_fetched: int = Field(ge=0)
    skip_reason: str | None = None
    error_message: str | None = None
    created_at: datetime


class DataPipelineRunDetail(BaseModel):
    run: DataPipelineRunSummary
    fetch_coverage: list[DailyOhlcvFetchCoverageRow] = Field(default_factory=list)


class ToolCallSpec(BaseModel):
    tool: str
    arguments: dict[str, Any] = Field(default_factory=dict)


class PlannerPlan(BaseModel):
    intent: IntentType
    requires_market_data: bool = False
    requires_research: bool = False
    tool_calls: list[ToolCallSpec] = Field(default_factory=list)
