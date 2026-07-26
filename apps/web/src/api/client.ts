import {
  candles,
  jobRuns,
  marketStatus,
  researchNotes,
  screenerResults,
} from "./mockData";
import type {
  BigQuerySyncOverviewResponse,
  Candle,
  ChatAuditResponse,
  ChatHealthResponse,
  ChatQueryRequest,
  ChatQueryResponse,
  ChatSourcesResponse,
  DataAvailabilityParams,
  DataAvailabilityResponse,
  DataCoveragePreviewRequest,
  DataCoveragePreviewResponse,
  DataInstrumentSearchParams,
  DataInstrumentSearchRow,
  DataPipelineHealthResponse,
  DataPipelineRequest,
  DataPipelineRunDetail,
  DataPipelineRunSummary,
  DataUniverseMemberRow,
  DataUniverseRow,
  DailyOpportunitiesParams,
  DailyOpportunitiesResponse,
  PipelineScheduleStatusRow,
  FactorICResponse,
  FactorSummaryResponse,
  FilingInvestigationEvent,
  FilingInvestigationRequest,
  FilingInvestigationRun,
  FilingInvestigationSubmission,
  FilingUniverseCoverage,
  JobRun,
  MLBacktestsResponse,
  MLCandidatesResponse,
  MLConcreteRunId,
  MLEquityCurveResponse,
  MLLatestCandidatesResponse,
  MLModelMetricsResponse,
  MLRobustnessResponse,
  MLRunId,
  MLSummaryResponse,
  MarketStatus,
  OperationsAdaptiveRateStateRow,
  OperationsExchange,
  OperationsLifecycleEventsParams,
  OperationsLifecycleEventsResponse,
  OperationsOverviewResponse,
  OperationsWorkItemsParams,
  OperationsWorkItemsResponse,
  ProviderCapabilityResponse,
  ProviderCredentialStatusResponse,
  ProviderCredentialTestRequest,
  ProviderCredentialTestResponse,
  ProviderCredentialTokenRequest,
  ProviderObservabilityParams,
  ProviderRequestLogRow,
  ProviderRequestSummaryRow,
  ResearchProgressResponse,
  ResearchNote,
  ScreenerResult,
} from "./types";

async function fetchJson<T>(path: string, fallback: T): Promise<T> {
  try {
    const response = await fetch(path);
    if (!response.ok) {
      return fallback;
    }
    return (await response.json()) as T;
  } catch {
    return fallback;
  }
}

async function strictFetchJson<T>(path: string, init?: RequestInit): Promise<T> {
  const response = await fetch(path, init);
  if (!response.ok) {
    let message = `Request failed: ${response.status}`;
    try {
      const payload = (await response.json()) as { detail?: unknown };
      if (payload.detail) message = formatApiError(payload.detail);
    } catch {
      // Keep the HTTP status fallback.
    }
    throw new Error(message);
  }
  return (await response.json()) as T;
}

function formatApiError(detail: unknown): string {
  if (typeof detail === "string") return detail;
  if (Array.isArray(detail)) {
    return detail
      .map((item) => {
        if (!item || typeof item !== "object") return String(item);
        const record = item as { loc?: unknown[]; msg?: unknown };
        const loc = Array.isArray(record.loc) ? record.loc.join(".") : "";
        const msg = typeof record.msg === "string" ? record.msg : JSON.stringify(item);
        return loc ? `${loc}: ${msg}` : msg;
      })
      .join("; ");
  }
  if (detail && typeof detail === "object") return JSON.stringify(detail);
  return String(detail);
}

export function getMarketStatus(): Promise<MarketStatus[]> {
  return fetchJson("/api/market/status", marketStatus);
}

export function getScreenerResults(): Promise<ScreenerResult[]> {
  return fetchJson("/api/screeners/intraday-range/latest", screenerResults);
}

export function getDailyOpportunities(
  params: DailyOpportunitiesParams,
): Promise<DailyOpportunitiesResponse> {
  const query = new URLSearchParams({ exchange: params.exchange });
  if (params.sessionDate) query.set("session_date", params.sessionDate);
  if (params.symbol) query.set("symbol", params.symbol);
  if (params.sortBy) query.set("sort_by", params.sortBy);
  if (params.direction) query.set("direction", params.direction);
  if (params.limit) query.set("limit", String(params.limit));
  if (params.offset) query.set("offset", String(params.offset));
  for (const [metric, range] of Object.entries(params.percentileFilters ?? {})) {
    if (range.minimum != null) {
      query.set(`${metric}_percentile_min`, String(range.minimum));
    }
    if (range.maximum != null) {
      query.set(`${metric}_percentile_max`, String(range.maximum));
    }
  }
  return strictFetchJson(`/api/opportunities/daily?${query.toString()}`);
}

export function getCandles(ticker: string): Promise<Candle[]> {
  return fetchJson(`/api/symbols/${ticker}/candles`, candles);
}

export function getResearchNotes(ticker?: string): Promise<ResearchNote[]> {
  const filtered = ticker
    ? researchNotes.filter((note) => note.ticker.toUpperCase() === ticker.toUpperCase())
    : researchNotes;
  const query = ticker ? `?ticker=${encodeURIComponent(ticker)}` : "";
  return fetchJson(`/api/research/notes${query}`, filtered);
}

export function getResearchProgress(): Promise<ResearchProgressResponse> {
  return fetchJson("/api/research/progress", {
    overall_status: "warning",
    step_count: 0,
    completed_count: 0,
    warning_count: 0,
    missing_count: 0,
    steps: [],
  });
}

export function getFactorSummary(): Promise<FactorSummaryResponse> {
  return fetchJson("/api/research/factors/summary", {
    status: "missing",
    path: "data/processed/research/factors/daily_v1_factor_research_summary.json",
    summary: null,
  });
}

export function getFactorIC(params: {
  target?: string;
  sort?: string;
  direction?: string;
  limit?: number;
}): Promise<FactorICResponse> {
  const query = new URLSearchParams();
  if (params.target) query.set("target", params.target);
  if (params.sort) query.set("sort", params.sort);
  if (params.direction) query.set("direction", params.direction);
  if (params.limit) query.set("limit", params.limit.toString());
  const suffix = query.toString() ? `?${query.toString()}` : "";
  return fetchJson(`/api/research/factors/ic${suffix}`, {
    status: "missing",
    path: "data/processed/research/factors/daily_v1_factor_ic.csv",
    target: params.target ?? null,
    sort: params.sort ?? "mean_rank_ic",
    direction: params.direction ?? "desc",
    rows: [],
  });
}

export function getMLSummary(): Promise<MLSummaryResponse> {
  return fetchJson("/api/research/ml/summary", {
    status: "missing",
    paths: {},
    dataset: null,
    walk_forward: null,
    model_runs: [],
    current_winner: null,
    assumptions: {
      target: "forward_ret_1d",
      universe: "static_full_history_100pct_coverage",
      evaluation: "leakage-aware walk-forward",
      strategy: "long_top_n_equal_weight_daily_rebalanced",
      caveat: "ML artifacts are not available yet.",
    },
  });
}

export function getMLModelMetrics(run: MLRunId): Promise<MLModelMetricsResponse> {
  return fetchJson(`/api/research/ml/model-metrics?run=${encodeURIComponent(run)}`, {
    status: "missing",
    run,
    rows: [],
  });
}

export function getMLBacktests(group: MLRunId): Promise<MLBacktestsResponse> {
  return fetchJson(`/api/research/ml/backtests?group=${encodeURIComponent(group)}`, {
    status: "missing",
    group,
    rows: [],
  });
}

export function getMLCandidates(params: {
  run: MLConcreteRunId;
  modelId: string;
  topN: number;
  limit?: number;
}): Promise<MLCandidatesResponse> {
  const query = new URLSearchParams({
    run: params.run,
    model_id: params.modelId,
    top_n: params.topN.toString(),
    limit: (params.limit ?? 200).toString(),
  });
  return fetchJson(`/api/research/ml/candidates?${query.toString()}`, {
    status: "missing",
    path: null,
    run: params.run,
    model_id: params.modelId,
    top_n: params.topN,
    rows: [],
  });
}

export function getMLLatestCandidates(params: {
  run: MLConcreteRunId;
  topN: number;
}): Promise<MLLatestCandidatesResponse> {
  const query = new URLSearchParams({
    run: params.run,
    top_n: params.topN.toString(),
  });
  return fetchJson(`/api/research/ml/latest-candidates?${query.toString()}`, {
    status: "missing",
    path: null,
    run: params.run,
    top_n: params.topN,
    prediction_date: null,
    target_session_date: null,
    model_count: 0,
    models: [],
  });
}

export function getMLEquityCurve(params: {
  group: MLConcreteRunId;
  modelId: string;
  topN: number;
}): Promise<MLEquityCurveResponse> {
  const query = new URLSearchParams({
    group: params.group,
    model_id: params.modelId,
    top_n: params.topN.toString(),
  });
  return fetchJson(`/api/research/ml/equity-curve?${query.toString()}`, {
    status: "missing",
    path: "",
    group: params.group,
    model_id: params.modelId,
    top_n: params.topN,
    rows: [],
  });
}

export function getMLRobustness(params: {
  group: MLConcreteRunId;
  modelId: string;
  topN: number;
}): Promise<MLRobustnessResponse> {
  const query = new URLSearchParams({
    group: params.group,
    model_id: params.modelId,
    top_n: params.topN.toString(),
  });
  return fetchJson(`/api/research/ml/robustness?${query.toString()}`, {
    status: "missing",
    group: params.group,
    model_id: params.modelId,
    top_n: params.topN,
    cost_sensitivity: [],
    top_n_comparison: [],
    drawdown: null,
  });
}

export function getJobRuns(): Promise<JobRun[]> {
  return fetchJson("/api/jobs/latest", jobRuns);
}

export function getUpstoxProviderCapabilities(): Promise<ProviderCapabilityResponse> {
  return strictFetchJson("/api/data/provider-capabilities/upstox");
}

export function getUpstoxCredentialStatus(): Promise<ProviderCredentialStatusResponse> {
  return strictFetchJson("/api/admin/provider-credentials/upstox/status");
}

export function testUpstoxCredential(
  payload: ProviderCredentialTestRequest,
): Promise<ProviderCredentialTestResponse> {
  return strictFetchJson("/api/admin/provider-credentials/upstox/test", {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify(payload),
  });
}

export function saveUpstoxCredential(
  payload: ProviderCredentialTokenRequest,
): Promise<ProviderCredentialStatusResponse> {
  return strictFetchJson("/api/admin/provider-credentials/upstox/token", {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify(payload),
  });
}

export function getDataPipelineHealth(): Promise<DataPipelineHealthResponse> {
  return strictFetchJson("/api/data/pipeline-health");
}

export function getDataAvailability(
  params: DataAvailabilityParams,
): Promise<DataAvailabilityResponse> {
  const query = new URLSearchParams();
  query.set("provider", params.provider ?? "upstox");
  query.set("exchange", params.exchange ?? "NSE");
  query.set("interval", params.interval ?? "1d");
  if (params.start_date) query.set("start_date", params.start_date);
  if (params.end_date) query.set("end_date", params.end_date);
  if (params.query) query.set("query", params.query);
  if (params.universe_id) query.set("universe_id", params.universe_id);
  if (params.coverage_status) query.set("coverage_status", params.coverage_status);
  if (params.limit) query.set("limit", params.limit.toString());
  if (params.offset) query.set("offset", params.offset.toString());
  if (params.sort) query.set("sort", params.sort);
  return strictFetchJson(`/api/data/availability?${query.toString()}`);
}

export function searchDataInstruments(
  params: DataInstrumentSearchParams,
): Promise<DataInstrumentSearchRow[]> {
  const query = new URLSearchParams();
  query.set("provider", params.provider ?? "upstox");
  query.set("exchange", params.exchange ?? "NSE");
  query.set("query", params.query);
  if (params.limit) query.set("limit", params.limit.toString());
  return strictFetchJson(`/api/data/instruments/search?${query.toString()}`);
}

export function getDataUniverses(): Promise<DataUniverseRow[]> {
  return strictFetchJson("/api/data/universes?exchange=NSE");
}

export function getDataUniverseMembers(
  universeId: string,
  limit = 500,
): Promise<DataUniverseMemberRow[]> {
  const query = new URLSearchParams({ limit: limit.toString() });
  return strictFetchJson(
    `/api/data/universes/${encodeURIComponent(universeId)}/members?${query.toString()}`,
  );
}

export function previewDataCoverage(
  payload: DataCoveragePreviewRequest,
): Promise<DataCoveragePreviewResponse> {
  return strictFetchJson("/api/data/coverage/preview", {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify(payload),
  });
}

export function createDataPipelineRequest(
  payload: DataPipelineRequest,
): Promise<DataPipelineRunDetail> {
  return strictFetchJson("/api/data/pipeline-requests", {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify(payload),
  });
}

export function getDataPipelineRuns(): Promise<DataPipelineRunSummary[]> {
  return strictFetchJson("/api/data/pipeline-runs");
}

export function getDataPipelineRunDetail(
  runId: string,
  exchange?: "NSE" | "TSX" | "US",
): Promise<DataPipelineRunDetail> {
  const query = new URLSearchParams();
  if (exchange) query.set("exchange", exchange);
  const suffix = query.size ? `?${query.toString()}` : "";
  return strictFetchJson(
    `/api/data/pipeline-runs/${encodeURIComponent(runId)}${suffix}`,
  );
}

function appendObservabilityParams(query: URLSearchParams, params: ProviderObservabilityParams) {
  if (params.provider) query.set("provider", params.provider);
  if (params.exchange) query.set("exchange", params.exchange);
  if (params.job) query.set("job", params.job);
  if (params.endpoint_group) query.set("endpoint_group", params.endpoint_group);
  if (params.status) query.set("status", params.status);
  if (params.start_date) query.set("start_date", params.start_date);
  if (params.end_date) query.set("end_date", params.end_date);
  if (params.run_id) query.set("run_id", params.run_id);
  if (params.limit) query.set("limit", params.limit.toString());
  if (params.offset) query.set("offset", params.offset.toString());
}

export function getProviderRuns(
  params: ProviderObservabilityParams,
): Promise<DataPipelineRunSummary[]> {
  const query = new URLSearchParams();
  appendObservabilityParams(query, params);
  return strictFetchJson(`/api/data/provider-runs?${query.toString()}`);
}

export function getProviderRequestSummary(
  params: ProviderObservabilityParams,
): Promise<ProviderRequestSummaryRow[]> {
  const query = new URLSearchParams();
  appendObservabilityParams(query, params);
  return strictFetchJson(`/api/data/provider-request-summary?${query.toString()}`);
}

export function getProviderRequestLogs(
  params: ProviderObservabilityParams,
): Promise<ProviderRequestLogRow[]> {
  const query = new URLSearchParams();
  appendObservabilityParams(query, params);
  return strictFetchJson(`/api/data/provider-request-logs?${query.toString()}`);
}

export function getPipelineScheduleStatus(): Promise<PipelineScheduleStatusRow[]> {
  return strictFetchJson("/api/data/schedules/status");
}

export function getOperationsOverview(
  exchange: OperationsExchange,
): Promise<OperationsOverviewResponse> {
  const query = new URLSearchParams({
    provider: "yfinance",
    exchange,
    recent_run_limit: "20",
    lifecycle_limit: "12",
  });
  return strictFetchJson(`/api/data/operations/overview?${query.toString()}`);
}

export function getOperationsWorkItems(
  params: OperationsWorkItemsParams,
): Promise<OperationsWorkItemsResponse> {
  const query = new URLSearchParams({
    provider: params.provider ?? "yfinance",
    exchange: params.exchange,
    limit: (params.limit ?? 50).toString(),
    offset: (params.offset ?? 0).toString(),
  });
  if (params.status) query.set("status", params.status);
  if (params.work_type) query.set("work_type", params.work_type);
  if (params.symbol) query.set("symbol", params.symbol);
  return strictFetchJson(`/api/data/operations/work-items?${query.toString()}`);
}

export function getOperationsLifecycleEvents(
  params: OperationsLifecycleEventsParams,
): Promise<OperationsLifecycleEventsResponse> {
  const query = new URLSearchParams({
    exchange: params.exchange,
    limit: (params.limit ?? 50).toString(),
    offset: (params.offset ?? 0).toString(),
  });
  if (params.event_type) query.set("event_type", params.event_type);
  if (params.symbol) query.set("symbol", params.symbol);
  return strictFetchJson(`/api/data/operations/lifecycle-events?${query.toString()}`);
}

export function getOperationsRateLimits(): Promise<OperationsAdaptiveRateStateRow[]> {
  return strictFetchJson("/api/data/operations/rate-limits?provider=yfinance");
}

export function getBigQuerySyncOverview(): Promise<BigQuerySyncOverviewResponse> {
  return strictFetchJson("/api/data/operations/bigquery-sync");
}

export async function postChatQuery(payload: ChatQueryRequest): Promise<ChatQueryResponse> {
  const response = await fetch("/api/chat/query", {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify(payload),
  });
  if (!response.ok) {
    throw new Error("Chat query failed");
  }
  return (await response.json()) as ChatQueryResponse;
}

export async function getChatSources(responseId: string): Promise<ChatSourcesResponse> {
  const response = await fetch(`/api/chat/sources/${encodeURIComponent(responseId)}`);
  if (!response.ok) {
    throw new Error("Failed to load chat sources");
  }
  return (await response.json()) as ChatSourcesResponse;
}

export function getChatHealth(): Promise<ChatHealthResponse> {
  return fetchJson("/api/chat/health", {
    enabled: false,
    strictCitationRequired: true,
    qdrantConfigured: false,
    embeddingConfigured: false,
    checkedAt: new Date().toISOString(),
  });
}

export async function getChatAudit(responseId: string): Promise<ChatAuditResponse> {
  const response = await fetch(`/api/chat/audit/${encodeURIComponent(responseId)}`);
  if (!response.ok) {
    throw new Error("Failed to load chat audit");
  }
  return (await response.json()) as ChatAuditResponse;
}

const filingHeaders = {
  "Content-Type": "application/json",
  "X-Workspace-ID": "default",
};

export function getFilingUniverseCoverage(
  universeId = "NIFTY50",
): Promise<FilingUniverseCoverage> {
  return strictFetchJson(
    `/api/filings/universes/${encodeURIComponent(universeId)}/coverage`,
    { headers: filingHeaders },
  );
}

export function submitFilingInvestigation(
  payload: FilingInvestigationRequest,
): Promise<FilingInvestigationSubmission> {
  return strictFetchJson("/api/filings/investigations", {
    method: "POST",
    headers: filingHeaders,
    body: JSON.stringify(payload),
  });
}

export function getFilingInvestigation(
  analysisId: string,
): Promise<FilingInvestigationRun> {
  return strictFetchJson(
    `/api/filings/investigations/${encodeURIComponent(analysisId)}`,
    { headers: filingHeaders },
  );
}

export function getFilingInvestigationEvents(
  analysisId: string,
): Promise<FilingInvestigationEvent[]> {
  return strictFetchJson(
    `/api/filings/investigations/${encodeURIComponent(analysisId)}/events`,
    { headers: filingHeaders },
  );
}
