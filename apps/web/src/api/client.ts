import { candles, jobRuns, marketStatus, researchNotes, screenerResults } from "./mockData";
import type {
  Candle,
  ChatAuditResponse,
  ChatHealthResponse,
  ChatQueryRequest,
  ChatQueryResponse,
  ChatSourcesResponse,
  JobRun,
  MarketStatus,
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

export function getJobRuns(): Promise<JobRun[]> {
  return fetchJson("/api/jobs/latest", jobRuns);
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
