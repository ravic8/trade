from datetime import date, datetime
from typing import Any, Literal

from pydantic import BaseModel, ConfigDict, Field, HttpUrl


class Symbol(BaseModel):
    symbol: str
    exchange: str
    yahoo_symbol: str | None = None
    name: str | None = None
    currency: str | None = None
    source: str
    source_url: str | None = None
    source_identity: str | None = None
    listing_status: str = "active"
    listing_status_reason: str | None = None
    listing_status_effective_at: datetime | None = None
    pipeline_eligibility: str = "incremental"
    instrument_type: str = "unknown"
    reconciliation_status: str = "not_required"
    reconciliation_reason: str | None = None
    official_sector: str | None = None
    official_security_type: str | None = None
    official_source_updated_at: datetime | None = None


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


class ProviderCredentialStatusResponse(BaseModel):
    provider: str
    credential_type: str
    configured: bool
    source: Literal["database", "env", "missing"]
    updated_at: datetime | None = None
    updated_by: str | None = None
    last_validated_at: datetime | None = None
    validation_status: str | None = None
    validation_message: str | None = None


class ProviderCredentialTokenRequest(BaseModel):
    model_config = ConfigDict(populate_by_name=True)

    access_token: str = Field(min_length=20, max_length=5000)
    validate_token: bool = Field(default=True, alias="validate")


class ProviderCredentialTestRequest(BaseModel):
    access_token: str | None = Field(default=None, min_length=20, max_length=5000)


class ProviderCredentialTestResponse(BaseModel):
    provider: str
    valid: bool
    checked_at: datetime
    message: str


class DataCoveragePreviewRequest(BaseModel):
    provider: Literal["upstox", "yfinance"] = "upstox"
    exchange: Literal["NSE", "TSX", "US"] = "NSE"
    symbols: list[str] = Field(min_length=1, max_length=500)
    unit: Literal["days"] = "days"
    interval: int = Field(default=1, ge=1)
    start_date: date
    end_date: date


def _default_data_pipeline_steps() -> list[Literal["fetch_ohlcv", "validate_ohlcv"]]:
    return ["fetch_ohlcv", "validate_ohlcv"]


class DataPipelineRequest(BaseModel):
    provider: Literal["upstox"] = "upstox"
    exchange: Literal["NSE"] = "NSE"
    symbols: list[str] = Field(min_length=1, max_length=500)
    unit: Literal["days"] = "days"
    interval: int = Field(default=1, ge=1)
    start_date: date
    end_date: date
    steps: list[Literal["fetch_ohlcv", "validate_ohlcv"]] = Field(
        default_factory=_default_data_pipeline_steps
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
    calendar_source: str | None = None
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


class DataAvailabilityRow(BaseModel):
    symbol: str
    name: str | None = None
    instrument_key: str
    provider: str
    exchange: str
    interval: Literal["1d", "5m"] = "1d"
    asset_class: str | None = None
    first_stored_date: date | None = None
    latest_stored_date: date | None = None
    first_stored_ts: datetime | None = None
    latest_stored_ts: datetime | None = None
    stored_rows: int = Field(ge=0)
    calendar_matched_rows: int = Field(default=0, ge=0)
    off_calendar_rows: int = Field(default=0, ge=0)
    expected_rows: int = Field(ge=0)
    coverage_pct: float = Field(ge=0.0)
    missing_rows: int = Field(ge=0)
    provider_unavailable_rows: int = Field(default=0, ge=0)
    actionable_missing_rows: int = Field(default=0, ge=0)
    missing_windows: int = Field(default=0, ge=0)
    coverage_status: Literal["complete", "partial", "empty"]
    last_successful_run: str | None = None
    last_fetch_status: str | None = None


class DataAvailabilitySummary(BaseModel):
    symbols_total: int = Field(ge=0)
    symbols_complete: int = Field(ge=0)
    symbols_partial: int = Field(ge=0)
    symbols_empty: int = Field(ge=0)
    expected_rows: int = Field(ge=0)
    stored_rows: int = Field(ge=0)
    calendar_matched_rows: int = Field(default=0, ge=0)
    off_calendar_rows: int = Field(default=0, ge=0)
    missing_rows: int = Field(ge=0)
    provider_unavailable_rows: int = Field(default=0, ge=0)
    actionable_missing_rows: int = Field(default=0, ge=0)
    symbols_provider_limited: int = Field(default=0, ge=0)
    symbols_actionable: int = Field(default=0, ge=0)
    estimated_provider_calls_for_missing: int = Field(ge=0)


class DataAvailabilityResponse(BaseModel):
    provider: str
    exchange: str
    interval: Literal["1d", "5m"] = "1d"
    start_date: date | None = None
    end_date: date | None = None
    limit: int = Field(ge=1)
    offset: int = Field(ge=0)
    sort: str
    total: int = Field(ge=0)
    rows: list[DataAvailabilityRow] = Field(default_factory=list)
    summary: DataAvailabilitySummary


class DataBulkFetchPreviewRow(DataAvailabilityRow):
    avg_daily_turnover: float | None = Field(default=None, ge=0.0)
    tasks: list[DataCoveragePreviewTask] = Field(default_factory=list)


class DataBulkFetchPreviewResponse(BaseModel):
    provider: str
    exchange: str
    universe_id: str
    interval: Literal["1d"] = "1d"
    start_date: date
    end_date: date
    query: str | None = None
    coverage_status: str | None = None
    min_avg_daily_turnover: float | None = Field(default=None, ge=0.0)
    min_coverage_pct: float | None = Field(default=None, ge=0.0, le=1.0)
    limit: int = Field(ge=1)
    offset: int = Field(ge=0)
    sort: str
    total: int = Field(ge=0)
    rows: list[DataBulkFetchPreviewRow] = Field(default_factory=list)
    summary: DataAvailabilitySummary
    tasks: list[DataCoveragePreviewTask] = Field(default_factory=list)
    warnings: list[str] = Field(default_factory=list)


class DataInstrumentSearchRow(BaseModel):
    symbol: str
    provider_symbol: str | None = None
    name: str | None = None
    instrument_key: str
    canonical_instrument_id: str | None = None
    provider: str
    exchange: str
    isin: str | None = None
    segment: str | None = None
    asset_type: str | None = None


class DataUniverseRow(BaseModel):
    universe_id: str
    name: str
    description: str | None = None
    exchange: str
    source: str
    criteria: dict[str, Any] = Field(default_factory=dict)
    created_at: datetime
    member_count: int = Field(ge=0)


class DataUniverseMemberRow(BaseModel):
    universe_id: str
    symbol: str
    instrument_key: str | None = None
    rank: int | None = None
    avg_daily_volume: float | None = None
    avg_daily_turnover: float | None = None
    trading_days: int | None = None
    zero_volume_ratio: float | None = None
    start_date: date | None = None
    end_date: date | None = None
    included_at: datetime


class UniverseReconciliationGroup(BaseModel):
    reconciliation_status: str
    reconciliation_reason: str | None = None
    instrument_type: str
    pipeline_eligibility: str
    symbols: int = Field(ge=0)


class UniverseReconciliationResponse(BaseModel):
    exchange: str
    snapshot_id: str
    fetched_at: datetime
    symbol_count: int = Field(ge=0)
    diagnostics: dict[str, Any] = Field(default_factory=dict)
    groups: list[UniverseReconciliationGroup] = Field(default_factory=list)


class DataPipelineRunExchangeResult(BaseModel):
    exchange: str
    items_requested: int = Field(ge=0)
    items_processed: int = Field(ge=0)
    items_succeeded: int = Field(ge=0)
    items_failed: int = Field(ge=0)
    items_retry_wait: int = Field(default=0, ge=0)
    items_terminal: int = Field(default=0, ge=0)
    items_cancelled: int = Field(default=0, ge=0)
    lost_claims: int = Field(default=0, ge=0)


class DataPipelineRunSummary(BaseModel):
    id: str
    name: str
    status: str
    exchange: str
    work_item_exchanges: list[str] = Field(default_factory=list)
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
    exchange_results: list[DataPipelineRunExchangeResult] = Field(default_factory=list)


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


class ProviderRequestSummaryRow(BaseModel):
    provider: str
    endpoint_group: str
    status: str
    requests: int = Field(ge=0)
    rate_limited_requests: int = Field(ge=0)
    wait_seconds: float = Field(ge=0.0)
    avg_duration_ms: float = Field(ge=0.0)


class ProviderRequestLogRow(BaseModel):
    id: str
    run_id: str | None = None
    provider: str
    endpoint_group: str
    request_key: str
    instrument_key: str | None = None
    symbol: str | None = None
    interval: str | None = None
    window_start: date | None = None
    window_end: date | None = None
    status_code: int | None = None
    status: str
    error_message: str | None = None
    retry_count: int = Field(ge=0)
    rate_limited: bool
    wait_seconds: float = Field(ge=0.0)
    duration_ms: float = Field(ge=0.0)
    created_at: datetime


class DataPipelineRunWorkItemRow(BaseModel):
    work_item_id: str
    work_type: str
    provider: str
    exchange: str
    canonical_instrument_id: str
    provider_symbol: str
    interval: str
    window_start: date
    window_end: date
    priority: int
    status: str
    attempt_count: int = Field(ge=0)
    max_attempts: int = Field(ge=0)
    next_attempt_at: datetime | None = None
    run_id: str | None = None
    last_status_code: int | None = None
    last_error_code: str | None = None
    last_error_message: str | None = None
    updated_at: datetime


class DataPipelineRunDetail(BaseModel):
    run: DataPipelineRunSummary
    selected_exchange: str | None = None
    fetch_coverage: list[DailyOhlcvFetchCoverageRow] = Field(default_factory=list)
    work_items: list[DataPipelineRunWorkItemRow] = Field(default_factory=list)
    provider_requests: list[ProviderRequestLogRow] = Field(default_factory=list)


class PipelineScheduleStatusRow(BaseModel):
    schedule_name: str
    job_name: str
    cron_schedule: str
    execution_timezone: str
    intended_status: Literal["running", "stopped"]
    notes: str | None = None


class OperationsQueueGroup(BaseModel):
    provider: str
    exchange: str
    work_type: str
    status: str
    items: int = Field(ge=0)
    symbols: int = Field(ge=0)
    maximum_attempts: int = Field(ge=0)
    oldest_created_at: datetime | None = None
    earliest_next_attempt_at: datetime | None = None


class OperationsWorkItemRow(BaseModel):
    work_item_id: str
    work_type: str
    provider: str
    exchange: str
    canonical_instrument_id: str
    provider_symbol: str
    interval: str
    window_start: date
    window_end: date
    priority: int
    status: str
    attempt_count: int = Field(ge=0)
    max_attempts: int = Field(ge=0)
    next_attempt_at: datetime | None = None
    locked_by: str | None = None
    locked_at: datetime | None = None
    run_id: str | None = None
    parent_work_item_id: str | None = None
    last_status_code: int | None = None
    last_error_code: str | None = None
    last_error_message: str | None = None
    created_at: datetime
    updated_at: datetime
    completed_at: datetime | None = None


class OperationsWorkItemsResponse(BaseModel):
    total: int = Field(ge=0)
    limit: int = Field(ge=1)
    offset: int = Field(ge=0)
    rows: list[OperationsWorkItemRow] = Field(default_factory=list)


class OperationsLifecycleEventRow(BaseModel):
    event_id: str
    canonical_instrument_id: str
    exchange: str
    symbol: str | None = None
    event_type: str
    old_value: dict[str, Any] | None = None
    new_value: dict[str, Any] | None = None
    snapshot_id: str | None = None
    created_at: datetime


class OperationsLifecycleEventsResponse(BaseModel):
    total: int = Field(ge=0)
    limit: int = Field(ge=1)
    offset: int = Field(ge=0)
    rows: list[OperationsLifecycleEventRow] = Field(default_factory=list)


class BigQuerySyncRunRow(BaseModel):
    run_id: str
    trigger: str
    status: str
    project_id: str
    dataset: str
    location: str
    exchange: str | None = None
    year: int | None = None
    entities: list[str] = Field(default_factory=list)
    started_at: datetime
    finished_at: datetime | None = None
    source_row_count: int = Field(ge=0)
    destination_row_count: int = Field(ge=0)
    count_difference: int
    inserted_rows: int = Field(ge=0)
    updated_rows: int = Field(ge=0)
    rejected_rows: int = Field(ge=0)
    retry_count: int = Field(ge=0)
    duration_seconds: float | None = Field(default=None, ge=0)
    source_watermark: str | None = None
    destination_watermark: str | None = None
    last_successful_sync_at: datetime | None = None
    bigquery_job_id: str | None = None
    schema_drift: dict[str, Any] = Field(default_factory=dict)
    error_details: str | None = None


class BigQuerySyncPartitionRow(BaseModel):
    partition_id: str
    run_id: str
    entity: str
    exchange: str | None = None
    partition_start: date | None = None
    partition_end: date | None = None
    status: str
    attempt_count: int = Field(ge=0)
    source_row_count: int = Field(ge=0)
    destination_row_count: int = Field(ge=0)
    count_difference: int
    inserted_rows: int = Field(ge=0)
    updated_rows: int = Field(ge=0)
    rejected_rows: int = Field(ge=0)
    source_watermark: str | None = None
    destination_watermark: str | None = None
    bigquery_job_id: str | None = None
    duration_seconds: float | None = Field(default=None, ge=0)
    schema_drift: dict[str, Any] = Field(default_factory=dict)
    error_details: str | None = None
    created_at: datetime
    updated_at: datetime
    completed_at: datetime | None = None


class BigQuerySyncOverviewResponse(BaseModel):
    enabled: bool
    project_id: str | None = None
    dataset: str
    location: str
    runs: list[BigQuerySyncRunRow] = Field(default_factory=list)
    partitions: list[BigQuerySyncPartitionRow] = Field(default_factory=list)


class OperationsAdaptiveRateStateRow(BaseModel):
    provider: str
    current_rpm: int = Field(ge=0)
    last_safe_rpm: int | None = Field(default=None, ge=0)
    minimum_rpm: int = Field(ge=0)
    maximum_rpm: int = Field(ge=0)
    current_concurrency: int = Field(ge=0)
    consecutive_healthy_windows: int = Field(ge=0)
    circuit_state: str
    cooldown_until: datetime | None = None
    last_429_at: datetime | None = None
    recent_error_rate: float = Field(ge=0.0)
    latency_baseline_ms: float | None = Field(default=None, ge=0.0)
    updated_at: datetime


class OperationsFreshnessRow(BaseModel):
    provider: str
    exchange: str
    first_date: date | None = None
    latest_date: date | None = None
    rows: int = Field(ge=0)
    symbols: int = Field(ge=0)
    suspicious_rows: int = Field(ge=0)
    latest_fetched_at: datetime | None = None


class OperationsUniverseSnapshotRow(BaseModel):
    snapshot_id: str
    exchange: str
    source: str
    status: str
    fetched_at: datetime
    symbol_count: int = Field(ge=0)
    validation: dict[str, Any] = Field(default_factory=dict)
    error_message: str | None = None


class OperationsOverviewResponse(BaseModel):
    generated_at: datetime
    provider: str | None = None
    exchange: str | None = None
    queue: list[OperationsQueueGroup] = Field(default_factory=list)
    freshness: list[OperationsFreshnessRow] = Field(default_factory=list)
    adaptive_rates: list[OperationsAdaptiveRateStateRow] = Field(default_factory=list)
    latest_universes: list[OperationsUniverseSnapshotRow] = Field(default_factory=list)
    recent_runs: list[DataPipelineRunSummary] = Field(default_factory=list)
    recent_lifecycle_events: list[OperationsLifecycleEventRow] = Field(default_factory=list)


class ToolCallSpec(BaseModel):
    tool: str
    arguments: dict[str, Any] = Field(default_factory=dict)


class PlannerPlan(BaseModel):
    intent: IntentType
    requires_market_data: bool = False
    requires_research: bool = False
    tool_calls: list[ToolCallSpec] = Field(default_factory=list)
