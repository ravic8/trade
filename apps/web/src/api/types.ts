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
