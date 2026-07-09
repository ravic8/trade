import { candles, jobRuns, marketStatus, researchNotes, screenerResults } from "./mockData";
import type {
  Candle,
  ChatAuditResponse,
  ChatHealthResponse,
  ChatQueryRequest,
  ChatQueryResponse,
  ChatSourcesResponse,
  DataCoveragePreviewRequest,
  DataCoveragePreviewResponse,
  DataPipelineHealthResponse,
  DataPipelineRequest,
  DataPipelineRunDetail,
  DataPipelineRunSummary,
  FactorICResponse,
  FactorSummaryResponse,
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
  ProviderCapabilityResponse,
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
      const payload = (await response.json()) as { detail?: string };
      if (payload.detail) message = payload.detail;
    } catch {
      // Keep the HTTP status fallback.
    }
    throw new Error(message);
  }
  return (await response.json()) as T;
}

export function getMarketStatus(): Promise<MarketStatus[]> {
  return fetchJson("/api/market/status", marketStatus);
}

export function getScreenerResults(): Promise<ScreenerResult[]> {
  return fetchJson("/api/screeners/intraday-range/latest", screenerResults);
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

export function getDataPipelineHealth(): Promise<DataPipelineHealthResponse> {
  return strictFetchJson("/api/data/pipeline-health");
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

export function getDataPipelineRunDetail(runId: string): Promise<DataPipelineRunDetail> {
  return strictFetchJson(`/api/data/pipeline-runs/${encodeURIComponent(runId)}`);
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
