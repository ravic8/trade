export type MarketStatus = {
  exchange: string;
  universeSize: number;
  lastOhlcvRun: string;
  lastScreenerRun: string;
  staleSymbols: number;
  dataQualityScore: number;
};

export type ScreenerResult = {
  ticker: string;
  exchange: string;
  company: string;
  signal: string;
  liquidity: number;
  d5Up0100: number;
  d5Dn0100: number;
  d5ClUp0200: number;
  d5VUp0200: number;
  matchedAt: string;
};

export type OpportunityTargetRow = {
  instrument_key: string;
  source: string;
  date: string;
  target_version: string;
  symbol: string;
  exchange: string;
  quality_status: string;
  open: number;
  high: number;
  low: number;
  close: number;
  previous_close: number | null;
  volume: number;
  open_interest: number | null;
  session_return: number | null;
  gap: number | null;
  true_return: number | null;
  upside: number | null;
  downside: number | null;
  giveback: number | null;
  recovery: number | null;
  session_range: number | null;
  true_upside: number | null;
  true_downside: number | null;
  true_range: number | null;
  percentiles: Partial<Record<OpportunityDistributionMetric, number | null>>;
};

export type OpportunityDistributionMetric =
  | "session_return"
  | "recovery"
  | "upside"
  | "downside"
  | "giveback"
  | "true_range";

export type OpportunityPercentileRange = {
  minimum?: number;
  maximum?: number;
};

export type OpportunityDistributionBin = {
  start: number;
  end: number;
  count: number;
  percentile_min: number | null;
  percentile_max: number | null;
  lower_overflow: boolean;
  upper_overflow: boolean;
};

export type OpportunityDistribution = {
  metric: OpportunityDistributionMetric;
  count: number;
  minimum: number | null;
  maximum: number | null;
  display_minimum: number | null;
  display_maximum: number | null;
  percentiles: Partial<Record<"p10" | "p25" | "p50" | "p75" | "p90", number>>;
  bins: OpportunityDistributionBin[];
};

export type OpportunitySession = {
  date: string;
  instruments: number;
  expected_instruments: number;
  coverage_ratio: number | null;
  coverage_status: "complete" | "partial";
};

export type OpportunityTargetSummary = {
  average_return?: number | null;
  average_gap?: number | null;
  average_upside?: number | null;
  average_downside?: number | null;
  average_true_range?: number | null;
  positive_sessions?: number;
  positive_session_ratio?: number | null;
  quality_warning_sessions?: number;
  quality_warning_ratio?: number | null;
};

export type DailyOpportunitiesResponse = {
  exchange: "NSE" | "TSX" | "US";
  source: "yfinance";
  target_version: string;
  selection_mode: "automatic" | "explicit";
  requested_session_date: string | null;
  session_date: string | null;
  session_exists: boolean;
  latest_available_date: string | null;
  latest_complete_date: string | null;
  available_sessions: OpportunitySession[];
  session_instruments: number;
  expected_instruments: number;
  coverage_ratio: number | null;
  coverage_status: "complete" | "partial" | "unavailable";
  total: number;
  session_total: number;
  summary: OpportunityTargetSummary;
  percentile_filters: Partial<Record<OpportunityDistributionMetric, OpportunityPercentileRange>>;
  distributions: Partial<Record<OpportunityDistributionMetric, OpportunityDistribution>>;
  rows: OpportunityTargetRow[];
};

export type DailyOpportunitiesParams = {
  exchange: "NSE" | "TSX" | "US";
  sessionDate?: string;
  symbol?: string;
  sortBy?: string;
  direction?: "asc" | "desc";
  limit?: number;
  offset?: number;
  percentileFilters?: Partial<
    Record<OpportunityDistributionMetric, OpportunityPercentileRange>
  >;
};

export type Candle = {
  time: string;
  open: number;
  high: number;
  low: number;
  close: number;
  volume: number;
};

export type ResearchNote = {
  id: string;
  ticker: string;
  title: string;
  sourceType: "news" | "filing" | "exchange" | "agent";
  publishedAt: string;
  summary: string;
  confidence: number;
};

export type JobRun = {
  id: string;
  name: string;
  status: "completed" | "running" | "failed" | "queued";
  startedAt: string;
  durationSeconds: number | null;
  itemsProcessed: number;
};

export type ProviderHistoricalCapability = {
  unit: "minutes" | "hours" | "days" | "weeks" | "months";
  interval_min: number;
  interval_max: number;
  available_from: string;
  max_window: string | null;
  notes?: string | null;
};

export type ProviderCapabilityResponse = {
  provider: string;
  api_version: string;
  source_url: string;
  historical: ProviderHistoricalCapability[];
  rate_limits: {
    standard_api_per_second: number;
    standard_api_per_minute: number;
    standard_api_per_30_minutes: number;
  };
  notes: string[];
};

export type ProviderCredentialStatusResponse = {
  provider: string;
  credential_type: string;
  configured: boolean;
  source: "database" | "env" | "missing";
  updated_at: string | null;
  updated_by: string | null;
  last_validated_at: string | null;
  validation_status: string | null;
  validation_message: string | null;
};

export type ProviderCredentialTestResponse = {
  provider: string;
  valid: boolean;
  checked_at: string;
  message: string;
};

export type ProviderCredentialTokenRequest = {
  access_token: string;
  validate: boolean;
};

export type ProviderCredentialTestRequest = {
  access_token?: string | null;
};

export type DataCoveragePreviewRequest = {
  provider: "upstox";
  exchange: "NSE";
  symbols: string[];
  unit: "days";
  interval: number;
  start_date: string;
  end_date: string;
};

export type DataPipelineRequest = DataCoveragePreviewRequest & {
  steps: ("fetch_ohlcv" | "validate_ohlcv")[];
  mode: "incremental_missing_only";
};

export type DataPipelineHealthResponse = {
  provider: "upstox";
  exchange: "NSE";
  daily_ohlcv_enabled: boolean;
  upstox_access_token_configured: boolean;
  max_concurrent_fetches: number;
  checked_at: string;
};

export type DataCoveragePreviewTask = {
  symbol: string;
  trading_symbol: string;
  instrument_key: string;
  fetch_start: string;
  fetch_end: string;
  missing_rows: number;
  status: "queued";
};

export type DataCoveragePreviewResponse = {
  provider: string;
  exchange: string;
  unit: string;
  interval: number;
  start_date: string;
  end_date: string;
  symbols_requested: number;
  symbols_resolved: number;
  unresolved_symbols: string[];
  ambiguous_symbols: string[];
  expected_rows: number;
  already_present_rows: number;
  missing_rows: number;
  estimated_provider_calls: number;
  tasks: DataCoveragePreviewTask[];
  warnings: string[];
};

export type DataAvailabilityRow = {
  symbol: string;
  name: string | null;
  instrument_key: string;
  provider: string;
  exchange: string;
  interval: "1d" | "5m";
  asset_class: string | null;
  first_stored_date: string | null;
  latest_stored_date: string | null;
  first_stored_ts: string | null;
  latest_stored_ts: string | null;
  stored_rows: number;
  calendar_matched_rows: number;
  off_calendar_rows: number;
  expected_rows: number;
  coverage_pct: number;
  missing_rows: number;
  provider_unavailable_rows: number;
  actionable_missing_rows: number;
  missing_windows: number;
  coverage_status: "complete" | "partial" | "empty";
  last_successful_run: string | null;
  last_fetch_status: string | null;
};

export type DataAvailabilitySummary = {
  symbols_total: number;
  symbols_complete: number;
  symbols_partial: number;
  symbols_empty: number;
  expected_rows: number;
  stored_rows: number;
  calendar_matched_rows: number;
  off_calendar_rows: number;
  missing_rows: number;
  provider_unavailable_rows: number;
  actionable_missing_rows: number;
  symbols_provider_limited: number;
  symbols_actionable: number;
  estimated_provider_calls_for_missing: number;
};

export type DataAvailabilityResponse = {
  provider: string;
  exchange: string;
  interval: "1d" | "5m";
  start_date: string | null;
  end_date: string | null;
  limit: number;
  offset: number;
  sort: string;
  total: number;
  rows: DataAvailabilityRow[];
  summary: DataAvailabilitySummary;
};

export type DataAvailabilityParams = {
  provider?: "upstox" | "yfinance";
  exchange?: "NSE" | "US" | "TSX" | "GLOBAL";
  interval?: "1d" | "5m";
  start_date?: string;
  end_date?: string;
  query?: string;
  universe_id?: string;
  coverage_status?: "complete" | "partial" | "empty" | "";
  limit?: number;
  offset?: number;
  sort?: string;
};

export type DataInstrumentSearchRow = {
  symbol: string;
  provider_symbol: string | null;
  name: string | null;
  instrument_key: string;
  canonical_instrument_id: string | null;
  provider: string;
  exchange: string;
  isin: string | null;
  segment: string | null;
  asset_type: string | null;
};

export type DataInstrumentSearchParams = {
  provider?: "upstox" | "yfinance";
  exchange?: "NSE" | "TSX" | "US";
  query: string;
  limit?: number;
};

export type DataUniverseRow = {
  universe_id: string;
  name: string;
  description: string | null;
  exchange: string;
  source: string;
  criteria: Record<string, unknown>;
  created_at: string;
  member_count: number;
};

export type DataUniverseMemberRow = {
  universe_id: string;
  symbol: string;
  instrument_key: string | null;
  rank: number | null;
  avg_daily_volume: number | null;
  avg_daily_turnover: number | null;
  trading_days: number | null;
  zero_volume_ratio: number | null;
  start_date: string | null;
  end_date: string | null;
  included_at: string;
};

export type DataPipelineRunSummary = {
  id: string;
  name: string;
  status: string;
  exchange: string;
  work_item_exchanges: string[];
  source: string;
  started_at: string;
  finished_at: string | null;
  duration_seconds: number | null;
  items_requested: number;
  items_processed: number;
  items_succeeded: number;
  items_failed: number;
  error_message: string | null;
  run_metadata: Record<string, unknown>;
  exchange_results: DataPipelineRunExchangeResult[];
};

export type DataPipelineRunExchangeResult = {
  exchange: string;
  items_requested: number;
  items_processed: number;
  items_succeeded: number;
  items_failed: number;
  items_retry_wait: number;
  items_terminal: number;
  items_cancelled: number;
  lost_claims: number;
};

export type DailyOhlcvFetchCoverageRow = {
  run_id: string;
  instrument_key: string;
  symbol: string;
  source: string;
  exchange: string;
  latest_stored_date: string | null;
  fetch_start: string | null;
  fetch_end: string;
  should_fetch: boolean;
  status: string;
  rows_fetched: number;
  skip_reason: string | null;
  error_message: string | null;
  created_at: string;
};

export type DataPipelineRunDetail = {
  run: DataPipelineRunSummary;
  selected_exchange: string | null;
  fetch_coverage: DailyOhlcvFetchCoverageRow[];
  work_items: OperationsWorkItemRow[];
  provider_requests: ProviderRequestLogRow[];
};

export type ProviderRequestSummaryRow = {
  provider: string;
  endpoint_group: string;
  status: string;
  requests: number;
  rate_limited_requests: number;
  wait_seconds: number;
  avg_duration_ms: number;
};

export type ProviderRequestLogRow = {
  id: string;
  run_id: string | null;
  provider: string;
  endpoint_group: string;
  request_key: string;
  instrument_key: string | null;
  symbol: string | null;
  interval: string | null;
  window_start: string | null;
  window_end: string | null;
  status_code: number | null;
  status: string;
  error_message: string | null;
  retry_count: number;
  rate_limited: boolean;
  wait_seconds: number;
  duration_ms: number;
  created_at: string;
};

export type ProviderObservabilityParams = {
  provider?: "upstox" | "yfinance" | "";
  exchange?: "NSE" | "US" | "TSX" | "GLOBAL" | "";
  job?: string;
  endpoint_group?: string;
  status?: string;
  start_date?: string;
  end_date?: string;
  run_id?: string;
  limit?: number;
  offset?: number;
};

export type PipelineScheduleStatusRow = {
  schedule_name: string;
  job_name: string;
  cron_schedule: string;
  execution_timezone: string;
  desired_status: "running" | "stopped";
  intended_status: "running" | "stopped";
  actual_status: "running" | "stopped" | "unknown";
  status_drift: boolean | null;
  origin_health: "current" | "stale" | "mixed" | "unknown";
  origin_drift: boolean | null;
  stored_origin_count: number;
  active_origin_count: number;
  last_tick_status: string | null;
  last_tick_at: string | null;
  last_run_status: string | null;
  last_run_at: string | null;
  last_successful_run_at: string | null;
  notes: string | null;
};

export type OperationsExchange = "NSE" | "TSX" | "US";

export type OperationsQueueGroup = {
  provider: string;
  exchange: OperationsExchange;
  work_type: string;
  status: string;
  items: number;
  symbols: number;
  maximum_attempts: number;
  oldest_created_at: string | null;
  earliest_next_attempt_at: string | null;
};

export type OperationsWorkItemRow = {
  work_item_id: string;
  work_type: string;
  provider: string;
  exchange: OperationsExchange;
  canonical_instrument_id: string;
  provider_symbol: string;
  interval: string;
  window_start: string;
  window_end: string;
  priority: number;
  status: string;
  attempt_count: number;
  max_attempts: number;
  next_attempt_at: string | null;
  locked_by: string | null;
  locked_at: string | null;
  run_id: string | null;
  parent_work_item_id: string | null;
  last_status_code: number | null;
  last_error_code: string | null;
  last_error_message: string | null;
  created_at: string;
  updated_at: string;
  completed_at: string | null;
};

export type OperationsWorkItemsResponse = {
  total: number;
  limit: number;
  offset: number;
  rows: OperationsWorkItemRow[];
};

export type OperationsLifecycleEventRow = {
  event_id: string;
  canonical_instrument_id: string;
  exchange: OperationsExchange;
  symbol: string | null;
  event_type: string;
  old_value: Record<string, unknown> | null;
  new_value: Record<string, unknown> | null;
  snapshot_id: string | null;
  created_at: string;
};

export type OperationsLifecycleEventsResponse = {
  total: number;
  limit: number;
  offset: number;
  rows: OperationsLifecycleEventRow[];
};

export type OperationsAdaptiveRateStateRow = {
  provider: string;
  current_rpm: number;
  last_safe_rpm: number | null;
  minimum_rpm: number;
  maximum_rpm: number;
  current_concurrency: number;
  consecutive_healthy_windows: number;
  circuit_state: string;
  cooldown_until: string | null;
  last_429_at: string | null;
  recent_error_rate: number;
  latency_baseline_ms: number | null;
  updated_at: string;
};

export type OperationsFreshnessRow = {
  provider: string;
  exchange: OperationsExchange;
  first_date: string | null;
  latest_date: string | null;
  rows: number;
  symbols: number;
  suspicious_rows: number;
  latest_fetched_at: string | null;
};

export type OperationsUniverseSnapshotRow = {
  snapshot_id: string;
  exchange: OperationsExchange;
  source: string;
  status: string;
  fetched_at: string;
  symbol_count: number;
  validation: Record<string, unknown>;
  error_message: string | null;
};

export type OperationsOverviewResponse = {
  generated_at: string;
  provider: string | null;
  exchange: OperationsExchange | null;
  queue: OperationsQueueGroup[];
  freshness: OperationsFreshnessRow[];
  adaptive_rates: OperationsAdaptiveRateStateRow[];
  latest_universes: OperationsUniverseSnapshotRow[];
  recent_runs: DataPipelineRunSummary[];
  recent_lifecycle_events: OperationsLifecycleEventRow[];
};

export type BigQuerySyncRunRow = {
  run_id: string;
  trigger: string;
  status: string;
  project_id: string;
  dataset: string;
  reporting_dataset: string | null;
  authenticated_principal: string | null;
  location: string;
  exchange: string | null;
  year: number | null;
  entities: string[];
  started_at: string;
  finished_at: string | null;
  source_row_count: number;
  destination_row_count: number;
  count_difference: number;
  inserted_rows: number;
  updated_rows: number;
  rejected_rows: number;
  staging_row_count: number;
  merged_row_count: number;
  duplicate_business_key_count: number;
  retry_count: number;
  duration_seconds: number | null;
  source_watermark: string | null;
  destination_watermark: string | null;
  last_successful_sync_at: string | null;
  bigquery_job_id: string | null;
  schema_drift: Record<string, unknown>;
  error_details: string | null;
};

export type BigQuerySyncPartitionRow = {
  partition_id: string;
  run_id: string;
  entity: string;
  exchange: string | null;
  partition_start: string | null;
  partition_end: string | null;
  status: string;
  attempt_count: number;
  source_row_count: number;
  destination_row_count: number;
  count_difference: number;
  inserted_rows: number;
  updated_rows: number;
  rejected_rows: number;
  staging_row_count: number;
  merged_row_count: number;
  duplicate_business_key_count: number;
  source_min_date: string | null;
  source_max_date: string | null;
  destination_min_date: string | null;
  destination_max_date: string | null;
  source_watermark: string | null;
  destination_watermark: string | null;
  bigquery_job_id: string | null;
  duration_seconds: number | null;
  schema_drift: Record<string, unknown>;
  error_details: string | null;
  created_at: string;
  updated_at: string;
  completed_at: string | null;
};

export type BigQuerySyncOverviewResponse = {
  enabled: boolean;
  canary_enabled: boolean;
  production_sync_enabled: boolean;
  project_id: string | null;
  core_dataset: string;
  reporting_dataset: string;
  location: string;
  runs: BigQuerySyncRunRow[];
  partitions: BigQuerySyncPartitionRow[];
};

export type OperationsWorkItemsParams = {
  provider?: "yfinance";
  exchange: OperationsExchange;
  status?: string;
  work_type?: string;
  symbol?: string;
  limit?: number;
  offset?: number;
};

export type OperationsLifecycleEventsParams = {
  exchange: OperationsExchange;
  event_type?: string;
  symbol?: string;
  limit?: number;
  offset?: number;
};

export type ChatExchange = "NSE" | "TSX" | "BOTH";
export type ChatQualityBadge = "complete" | "partial" | "stale";
export type ChatCitationType = "timescale_query" | "qdrant_chunk";

export type ChatQueryRequest = {
  message: string;
  context: {
    exchange: ChatExchange;
    symbols: string[];
    timezone?: string | null;
  };
  options: {
    max_latency_ms: number;
    strict_quality: boolean;
  };
  session_id?: string | null;
  user_id?: string | null;
};

export type ChatCitation = {
  id: string;
  type: ChatCitationType;
  label: string;
  provenance_ref: string;
};

export type ChatQueryResponse = {
  response_id: string;
  session_id?: string | null;
  answer: {
    text: string;
    quality_badge: ChatQualityBadge;
    freshness: {
      market_data_as_of?: string | null;
      research_data_as_of?: string | null;
    };
    warnings: string[];
    follow_ups: string[];
  };
  citations: ChatCitation[];
  trace_id: string;
};

export type ChatTimescaleSource = {
  provenance_ref: string;
  template_id: string;
  parameters: Record<string, unknown>;
  time_range?: { start?: string | null; end?: string | null } | null;
  row_count: number;
  executed_at: string;
};

export type ChatQdrantSource = {
  provenance_ref: string;
  collection: string;
  doc_id: string;
  chunk_id: string;
  score: number;
  as_of_date?: string | null;
  title?: string | null;
};

export type ChatSourcesResponse = {
  response_id: string;
  sources: {
    timescale: ChatTimescaleSource[];
    qdrant: ChatQdrantSource[];
  };
};

export type ChatHealthResponse = {
  enabled: boolean;
  strictCitationRequired: boolean;
  qdrantConfigured: boolean;
  embeddingConfigured: boolean;
  checkedAt: string;
};

export type ChatAuditResponse = {
  recorded_at: string;
  request: Record<string, unknown>;
  plan: Record<string, unknown> | null;
  tool_outputs: Record<string, unknown>;
  response: Record<string, unknown>;
};

export type FilingInvestigationStatus =
  | "accepted"
  | "queued"
  | "running"
  | "completed"
  | "partial"
  | "abstained"
  | "failed";

export type FilingInvestigationRequest = {
  question: string;
  universe_id: "NIFTY50";
  strict_evidence: boolean;
  max_tool_calls: number;
  comparison: "auto" | "yoy" | "qoq";
};

export type FilingCoverageCompany = {
  company_id: string;
  symbol: string;
  name: string;
  status: "eligible" | "insufficient_history" | "no_approved_facts";
  approved_fact_count: number;
  available_periods: string[];
  available_metrics: string[];
  reason_codes: string[];
};

export type FilingUniverseCoverage = {
  universe_id: string;
  snapshot_id?: string | null;
  member_count: number;
  represented_company_count: number;
  eligible_company_count: number;
  excluded_company_count: number;
  companies: FilingCoverageCompany[];
};

export type FilingInvestigationRankingRow = {
  company_id: string;
  symbol: string;
  name: string;
  metric: string;
  comparison: "yoy" | "qoq";
  current_period: string;
  comparison_period: string;
  current_value: string;
  comparison_value: string;
  currency?: string | null;
  percent_change: string;
  fact_ids: string[];
  citation_ids: string[];
};

export type FilingEvidence = {
  evidence_id: string;
  company_id: string;
  filing_id: string;
  filing_version: number;
  page?: number | null;
  section_path?: string | null;
  row_label?: string | null;
  xbrl_concept?: string | null;
  context_ref?: string | null;
  source_hash: string;
  snippet?: string | null;
  effective_date?: string | null;
};

export type FilingInvestigationCitation = {
  citation_id: string;
  fact_id: string;
  evidence_ids: string[];
  label: string;
  company_id: string;
  filing_id: string;
  filing_version: number;
  period_end: string;
  evidence: FilingEvidence[];
};

export type FilingInvestigationResult = {
  plan: {
    intent: string;
    metric: string;
    comparison: "yoy" | "qoq";
    limit: number;
    scope: string;
    rationale: string;
  };
  coverage: FilingUniverseCoverage;
  ranking: {
    rows: FilingInvestigationRankingRow[];
    eligible_count: number;
    ranked_count: number;
    excluded_count: number;
    exclusions: Array<{
      company_id: string;
      symbol: string;
      name: string;
      reason_code: string;
    }>;
  };
  synthesis: {
    title: string;
    summary: string;
    claims: Array<{ text: string; citation_ids: string[] }>;
    limitations: string[];
    model_used: boolean;
    provider?: string | null;
    model?: string | null;
    usage: Record<string, number>;
  };
  citations: FilingInvestigationCitation[];
  claim_validation: {
    passed: boolean;
    valid_claim_count: number;
    rejected_claim_count: number;
    complete_row_count: number;
  };
  tool_calls: Array<Record<string, unknown>>;
  prompt_version: string;
};

export type FilingInvestigationRun = {
  analysis_id: string;
  thread_id: string;
  workspace_id: string;
  universe_id: string;
  universe_snapshot_id?: string | null;
  question: string;
  status: FilingInvestigationStatus;
  current_node: string;
  progress: number;
  request_payload: Record<string, unknown>;
  plan_payload: Record<string, unknown>;
  result_payload: Partial<FilingInvestigationResult>;
  error_code?: string | null;
  error_message?: string | null;
  trace_id?: string | null;
  created_at: string;
  updated_at: string;
  finished_at?: string | null;
};

export type FilingInvestigationSubmission = {
  run: FilingInvestigationRun;
  accepted: boolean;
  status_url: string;
  events_url: string;
};

export type FilingInvestigationEvent = {
  event_id: string;
  analysis_id: string;
  sequence: number;
  node: string;
  status: string;
  detail: Record<string, unknown>;
  created_at: string;
};

export type ResearchArtifact = {
  path: string;
  kind: string;
  required: boolean;
  status: "present" | "missing";
};

export type ResearchDetailItem = {
  label: string;
  value: string | number | null;
};

export type ResearchProgressStep = {
  step_id: string;
  title: string;
  status: "done" | "warning" | "missing";
  row_count: number | null;
  symbol_count: number | null;
  date_min: string | null;
  date_max: string | null;
  warning_count: number;
  failed_count: number;
  last_generated_at: string | null;
  command: string;
  timescale_tables: string[];
  artifacts: ResearchArtifact[];
  notes: string[];
  warning_explanation: string | null;
  detail_items: ResearchDetailItem[];
};

export type ResearchProgressResponse = {
  overall_status: "done" | "warning";
  step_count: number;
  completed_count: number;
  warning_count: number;
  missing_count: number;
  steps: ResearchProgressStep[];
};

export type FactorSummary = {
  dataset_name: string;
  feature_version: string;
  target_version: string;
  generated_at: string;
  row_count: number;
  symbol_count: number;
  date_min: string;
  date_max: string;
  feature_count: number;
  return_target_count: number;
  quantile_count: number;
  ic_rows: number;
  quantile_rows: number;
  hit_rate_rows: number;
  monthly_stability_rows: number;
};

export type FactorSummaryResponse = {
  status: "done" | "missing";
  path: string;
  summary: FactorSummary | null;
};

export type FactorICRow = {
  feature: string;
  target: string;
  dates: number;
  rows: number;
  mean_ic: number | null;
  mean_rank_ic: number | null;
  ic_t_stat: number | null;
  rank_ic_t_stat: number | null;
  positive_ic_pct: number | null;
  positive_rank_ic_pct: number | null;
};

export type FactorICResponse = {
  status: "done" | "missing";
  path: string;
  target: string | null;
  sort: string;
  direction: string;
  rows: FactorICRow[];
};

export type MLRunId = "all" | "baselines" | "lightgbm";
export type MLConcreteRunId = "baselines" | "lightgbm";

export type MLBacktestResult = {
  group?: MLConcreteRunId;
  model_id: string;
  top_n: number;
  day_count: number;
  total_return: number | null;
  average_daily_gross_return: number | null;
  average_daily_net_return: number | null;
  annualized_return: number | null;
  annualized_volatility: number | null;
  sharpe_ratio: number | null;
  max_drawdown: number | null;
  win_rate: number | null;
  average_turnover: number | null;
  total_transaction_cost: number | null;
  best_day: number | null;
  worst_day: number | null;
  profit_factor: number | null;
};

export type MLModelRunSummary = {
  run_id: MLConcreteRunId;
  status: "done" | "missing";
  generated_at: string | null;
  model_count: number;
  prediction_row_count: number;
  fold_count: number | null;
  best_backtest: MLBacktestResult | null;
};

export type MLDatasetSummary = {
  dataset_name: string;
  generated_at: string;
  target_column: string;
  coverage_policy: string;
  leakage_note: string;
  row_count: number;
  trainable_row_count: number;
  symbol_count: number;
  trainable_symbol_count: number;
  excluded_symbol_count: number;
  feature_column_count: number;
  date_min: string;
  date_max: string;
  leakage_checks_passed: boolean;
};

export type MLWalkForwardSummary = {
  generated_at: string;
  fold_count: number;
  candidate_date_count: number;
  skipped_candidate_count: number;
  first_prediction_date: string | null;
  last_prediction_date: string | null;
  leakage_checks_passed: boolean;
};

export type MLSummaryResponse = {
  status: "done" | "missing";
  paths: Record<string, string>;
  dataset: MLDatasetSummary | null;
  walk_forward: MLWalkForwardSummary | null;
  model_runs: MLModelRunSummary[];
  current_winner: MLBacktestResult | null;
  assumptions: {
    target: string;
    universe: string;
    evaluation: string;
    strategy: string;
    caveat: string;
  };
};

export type MLModelMetricRow = {
  run_id: MLConcreteRunId;
  model_id: string;
  prediction_rows: number;
  evaluated_rows: number;
  prediction_date_count: number;
  rank_ic_mean: number | null;
  average_realized_return: number | null;
  top_5_average_return: number | null;
  top_5_hit_rate: number | null;
  top_10_average_return: number | null;
  top_10_hit_rate: number | null;
  top_20_average_return: number | null;
  top_20_hit_rate: number | null;
};

export type MLModelMetricsResponse = {
  status: "done" | "missing";
  run: MLRunId;
  rows: MLModelMetricRow[];
};

export type MLBacktestsResponse = {
  status: "done" | "missing";
  group: MLRunId;
  rows: MLBacktestResult[];
};

export type MLCandidateRow = {
  prediction_date: string;
  symbol: string;
  instrument_key: string;
  model_id: string;
  rank: number;
  score: number;
  realized_forward_ret_1d: number | null;
};

export type MLCandidatesResponse = {
  status: "done" | "missing";
  path: string | null;
  run: MLConcreteRunId;
  model_id: string;
  top_n: number;
  rows: MLCandidateRow[];
};

export type MLEquityCurveRow = {
  model_id: string;
  top_n: number;
  prediction_date: string;
  equity: number;
  drawdown: number;
};

export type MLEquityCurveResponse = {
  status: "done" | "missing";
  path: string;
  group: MLConcreteRunId;
  model_id: string;
  top_n: number;
  rows: MLEquityCurveRow[];
};

export type MLLatestCandidateModel = {
  model_id: string;
  rows: MLCandidateRow[];
};

export type MLLatestCandidatesResponse = {
  status: "done" | "missing";
  path: string | null;
  run: MLConcreteRunId;
  top_n: number;
  prediction_date: string | null;
  target_session_date: string | null;
  model_count: number;
  models: MLLatestCandidateModel[];
  note?: string;
};

export type MLCostSensitivityRow = MLBacktestResult & {
  transaction_cost_bps: number;
};

export type MLDrawdownDailyRow = {
  model_id: string;
  prediction_date: string;
  top_n: number;
  selected_count: number;
  gross_return: number;
  turnover: number;
  transaction_cost: number;
  net_return: number;
};

export type MLDrawdownSummary = {
  peak_date: string;
  trough_date: string;
  recovery_date: string | null;
  max_drawdown: number;
  peak_equity: number;
  trough_equity: number;
  days: number;
  latest_peak_date: string;
  daily_returns: MLDrawdownDailyRow[];
};

export type MLRobustnessResponse = {
  status: "done" | "missing";
  group: MLConcreteRunId;
  model_id: string;
  top_n: number;
  cost_sensitivity: MLCostSensitivityRow[];
  top_n_comparison: MLBacktestResult[];
  drawdown: MLDrawdownSummary | null;
};
