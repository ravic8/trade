import { useMutation, useQuery } from "@tanstack/react-query";

import {
  getChatAudit,
  getChatHealth,
  getChatSources,
  getCandles,
  createDataPipelineRequest,
  getDataPipelineHealth,
  getDataPipelineRunDetail,
  getDataPipelineRuns,
  getFactorIC,
  getFactorSummary,
  getJobRuns,
  getMLBacktests,
  getMLCandidates,
  getMLEquityCurve,
  getMLLatestCandidates,
  getMLModelMetrics,
  getMLRobustness,
  getMLSummary,
  getMarketStatus,
  getResearchProgress,
  getResearchNotes,
  getScreenerResults,
  getUpstoxProviderCapabilities,
  previewDataCoverage,
  postChatQuery,
} from "./client";
import type {
  ChatQueryRequest,
  DataCoveragePreviewRequest,
  DataPipelineRequest,
  MLConcreteRunId,
  MLRunId,
} from "./types";

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

export function useResearchProgress() {
  return useQuery({ queryKey: ["research-progress"], queryFn: getResearchProgress });
}

export function useFactorSummary() {
  return useQuery({ queryKey: ["factor-summary"], queryFn: getFactorSummary });
}

export function useFactorIC(params: {
  target?: string;
  sort?: string;
  direction?: string;
  limit?: number;
}) {
  return useQuery({
    queryKey: ["factor-ic", params],
    queryFn: () => getFactorIC(params),
  });
}

export function useMLSummary() {
  return useQuery({ queryKey: ["ml-summary"], queryFn: getMLSummary });
}

export function useMLModelMetrics(run: MLRunId) {
  return useQuery({
    queryKey: ["ml-model-metrics", run],
    queryFn: () => getMLModelMetrics(run),
  });
}

export function useMLBacktests(group: MLRunId) {
  return useQuery({
    queryKey: ["ml-backtests", group],
    queryFn: () => getMLBacktests(group),
  });
}

export function useMLCandidates(params: {
  run: MLConcreteRunId;
  modelId: string;
  topN: number;
  limit?: number;
}) {
  return useQuery({
    queryKey: ["ml-candidates", params],
    queryFn: () => getMLCandidates(params),
  });
}

export function useMLLatestCandidates(params: {
  run: MLConcreteRunId;
  topN: number;
}) {
  return useQuery({
    queryKey: ["ml-latest-candidates", params],
    queryFn: () => getMLLatestCandidates(params),
  });
}

export function useMLEquityCurve(params: {
  group: MLConcreteRunId;
  modelId: string;
  topN: number;
}) {
  return useQuery({
    queryKey: ["ml-equity-curve", params],
    queryFn: () => getMLEquityCurve(params),
  });
}

export function useMLRobustness(params: {
  group: MLConcreteRunId;
  modelId: string;
  topN: number;
}) {
  return useQuery({
    queryKey: ["ml-robustness", params],
    queryFn: () => getMLRobustness(params),
  });
}

export function useJobRuns() {
  return useQuery({ queryKey: ["job-runs"], queryFn: getJobRuns });
}

export function useUpstoxProviderCapabilities() {
  return useQuery({
    queryKey: ["data-provider-capabilities", "upstox"],
    queryFn: getUpstoxProviderCapabilities,
  });
}

export function useDataPipelineRuns() {
  return useQuery({ queryKey: ["data-pipeline-runs"], queryFn: getDataPipelineRuns });
}

export function useDataPipelineHealth() {
  return useQuery({ queryKey: ["data-pipeline-health"], queryFn: getDataPipelineHealth });
}

export function useDataPipelineRunDetail(runId: string | null) {
  return useQuery({
    queryKey: ["data-pipeline-run", runId],
    queryFn: () => getDataPipelineRunDetail(runId as string),
    enabled: Boolean(runId),
  });
}

export function useDataCoveragePreview() {
  return useMutation({
    mutationFn: (payload: DataCoveragePreviewRequest) => previewDataCoverage(payload),
  });
}

export function useCreateDataPipelineRequest() {
  return useMutation({
    mutationFn: (payload: DataPipelineRequest) => createDataPipelineRequest(payload),
  });
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
