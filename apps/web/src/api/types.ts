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
