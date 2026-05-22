import { useMutation, useQuery } from "@tanstack/react-query";

import {
  getChatAudit,
  getChatHealth,
  getChatSources,
  getCandles,
  getJobRuns,
  getMarketStatus,
  getResearchNotes,
  getScreenerResults,
  postChatQuery,
} from "./client";
import type { ChatQueryRequest } from "./types";

export function useMarketStatus() {
  return useQuery({ queryKey: ["market-status"], queryFn: getMarketStatus });
}

export function useScreenerResults() {
  return useQuery({ queryKey: ["screener-results"], queryFn: getScreenerResults });
}

export function useCandles(ticker: string) {
  return useQuery({ queryKey: ["candles", ticker], queryFn: () => getCandles(ticker) });
}

export function useResearchNotes(ticker?: string) {
  return useQuery({ queryKey: ["research-notes", ticker ?? "all"], queryFn: () => getResearchNotes(ticker) });
}

export function useJobRuns() {
  return useQuery({ queryKey: ["job-runs"], queryFn: getJobRuns });
}

export function useChatHealth() {
  return useQuery({ queryKey: ["chat-health"], queryFn: getChatHealth });
}

export function useChatQuery() {
  return useMutation({
    mutationFn: (payload: ChatQueryRequest) => postChatQuery(payload),
  });
}

export function useChatSources(responseId: string | null) {
  return useQuery({
    queryKey: ["chat-sources", responseId],
    queryFn: () => getChatSources(responseId as string),
    enabled: Boolean(responseId),
  });
}

export function useChatAudit(responseId: string | null) {
  return useQuery({
    queryKey: ["chat-audit", responseId],
    queryFn: () => getChatAudit(responseId as string),
    enabled: Boolean(responseId),
  });
}
