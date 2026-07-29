import { useMutation, useQuery } from "@tanstack/react-query";

import {
  getBigQuerySyncOverview,
  getChatAudit,
  getChatHealth,
  getChatSources,
  getCandles,
  createDataPipelineRequest,
  getDataAvailability,
  getDataPipelineHealth,
  getDataPipelineRunDetail,
  getDataPipelineRuns,
  getDataUniverseMembers,
  getDataUniverses,
  getDailyOpportunities,
  getFactorIC,
  getFactorSummary,
  getFilingInvestigation,
  getFilingInvestigationEvents,
  getFilingInvestigationValidation,
  getFilingUniverseCoverage,
  getJobRuns,
  getMLBacktests,
  getMLCandidates,
  getMLEquityCurve,
  getMLLatestCandidates,
  getMLModelMetrics,
  getMLRobustness,
  getMLSummary,
  getMarketStatus,
  getOperationsLifecycleEvents,
  getOperationsOverview,
  getOperationsRateLimits,
  getOperationsWorkItems,
  getPipelineScheduleStatus,
  getProviderRequestLogs,
  getProviderRequestSummary,
  getProviderRuns,
  getResearchProgress,
  getResearchNotes,
  getScreenerResults,
  getUpstoxCredentialStatus,
  getUpstoxProviderCapabilities,
  previewDataCoverage,
  postChatQuery,
  saveUpstoxCredential,
  searchDataInstruments,
  submitFilingInvestigation,
  evaluateFilingInvestigation,
  testUpstoxCredential,
} from "./client";
import type {
  ChatQueryRequest,
  DailyOpportunitiesParams,
  DataAvailabilityParams,
  DataCoveragePreviewRequest,
  DataInstrumentSearchParams,
  DataPipelineRequest,
  FilingInvestigationRequest,
  MLConcreteRunId,
  MLRunId,
  OperationsExchange,
  OperationsLifecycleEventsParams,
  OperationsWorkItemsParams,
  ProviderObservabilityParams,
  ProviderCredentialTestRequest,
  ProviderCredentialTokenRequest,
} from "./types";

export function useMarketStatus() {
  return useQuery({ queryKey: ["market-status"], queryFn: getMarketStatus });
}

export function useScreenerResults() {
  return useQuery({ queryKey: ["screener-results"], queryFn: getScreenerResults });
}

export function useDailyOpportunities(params: DailyOpportunitiesParams, enabled = true) {
  return useQuery({
    queryKey: ["daily-opportunities", params],
    queryFn: () => getDailyOpportunities(params),
    placeholderData: (previousData, previousQuery) => {
      const previousParams = previousQuery?.queryKey[1] as
        | DailyOpportunitiesParams
        | undefined;
      const sameSession =
        previousParams?.exchange === params.exchange &&
        (previousParams.sessionDate ?? "") === (params.sessionDate ?? "");
      return sameSession ? previousData : undefined;
    },
    staleTime: 30_000,
    enabled,
  });
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

export function useUpstoxCredentialStatus() {
  return useQuery({
    queryKey: ["admin-provider-credentials", "upstox"],
    queryFn: getUpstoxCredentialStatus,
    retry: false,
  });
}

export function useTestUpstoxCredential() {
  return useMutation({
    mutationFn: (payload: ProviderCredentialTestRequest) => testUpstoxCredential(payload),
  });
}

export function useSaveUpstoxCredential() {
  return useMutation({
    mutationFn: (payload: ProviderCredentialTokenRequest) => saveUpstoxCredential(payload),
  });
}

export function useDataPipelineRuns() {
  return useQuery({ queryKey: ["data-pipeline-runs"], queryFn: getDataPipelineRuns });
}

export function useProviderRuns(params: ProviderObservabilityParams, enabled = true) {
  return useQuery({
    queryKey: ["provider-runs", params],
    queryFn: () => getProviderRuns(params),
    enabled,
  });
}

export function useProviderRequestSummary(params: ProviderObservabilityParams) {
  return useQuery({
    queryKey: ["provider-request-summary", params],
    queryFn: () => getProviderRequestSummary(params),
  });
}

export function useProviderRequestLogs(params: ProviderObservabilityParams) {
  return useQuery({
    queryKey: ["provider-request-logs", params],
    queryFn: () => getProviderRequestLogs(params),
  });
}

export function usePipelineScheduleStatus(enabled = true) {
  return useQuery({
    queryKey: ["pipeline-schedule-status"],
    queryFn: getPipelineScheduleStatus,
    enabled,
  });
}

export function useDataPipelineHealth() {
  return useQuery({ queryKey: ["data-pipeline-health"], queryFn: getDataPipelineHealth });
}

export function useDataAvailability(params: DataAvailabilityParams, enabled = true) {
  return useQuery({
    queryKey: ["data-availability", params],
    queryFn: () => getDataAvailability(params),
    enabled,
  });
}

export function useOperationsOverview(exchange: OperationsExchange) {
  return useQuery({
    queryKey: ["data-operations-overview", exchange],
    queryFn: () => getOperationsOverview(exchange),
    refetchInterval: 60_000,
  });
}

export function useOperationsWorkItems(
  params: OperationsWorkItemsParams,
  enabled = true,
) {
  return useQuery({
    queryKey: ["data-operations-work-items", params],
    queryFn: () => getOperationsWorkItems(params),
    enabled,
  });
}

export function useOperationsLifecycleEvents(
  params: OperationsLifecycleEventsParams,
  enabled = true,
) {
  return useQuery({
    queryKey: ["data-operations-lifecycle", params],
    queryFn: () => getOperationsLifecycleEvents(params),
    enabled,
  });
}

export function useOperationsRateLimits() {
  return useQuery({
    queryKey: ["data-operations-rate-limits", "yfinance"],
    queryFn: getOperationsRateLimits,
    refetchInterval: 60_000,
  });
}

export function useBigQuerySyncOverview(enabled = true) {
  return useQuery({
    queryKey: ["data-operations-bigquery-sync"],
    queryFn: getBigQuerySyncOverview,
    enabled,
    refetchInterval: 60_000,
  });
}

export function useDataPipelineRunDetail(
  runId: string | null,
  exchange?: "NSE" | "TSX" | "US",
) {
  return useQuery({
    queryKey: ["data-pipeline-run", runId, exchange],
    queryFn: () => getDataPipelineRunDetail(runId as string, exchange),
    enabled: Boolean(runId),
  });
}

export function useDataCoveragePreview() {
  return useMutation({
    mutationFn: (payload: DataCoveragePreviewRequest) => previewDataCoverage(payload),
  });
}

export function useDataInstrumentSearch(
  params: DataInstrumentSearchParams,
  enabled: boolean,
) {
  return useQuery({
    queryKey: ["data-instruments-search", params],
    queryFn: () => searchDataInstruments(params),
    enabled,
  });
}

export function useDataUniverses() {
  return useQuery({ queryKey: ["data-universes"], queryFn: getDataUniverses });
}

export function useDataUniverseMembers(universeId: string | null, limit: number) {
  return useQuery({
    queryKey: ["data-universe-members", universeId, limit],
    queryFn: () => getDataUniverseMembers(universeId as string, limit),
    enabled: Boolean(universeId),
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

export function useFilingUniverseCoverage() {
  return useQuery({
    queryKey: ["filing-universe-coverage", "NIFTY50"],
    queryFn: () => getFilingUniverseCoverage("NIFTY50"),
    staleTime: 30_000,
  });
}

export function useSubmitFilingInvestigation() {
  return useMutation({
    mutationFn: (payload: FilingInvestigationRequest) =>
      submitFilingInvestigation(payload),
  });
}

export function useFilingInvestigation(analysisId: string | null) {
  return useQuery({
    queryKey: ["filing-investigation", analysisId],
    queryFn: () => getFilingInvestigation(analysisId as string),
    enabled: Boolean(analysisId),
    refetchInterval: (query) => {
      const status = query.state.data?.status;
      return status &&
        ["completed", "partial", "abstained", "failed"].includes(status)
        ? false
        : 1_000;
    },
  });
}

export function useFilingInvestigationEvents(
  analysisId: string | null,
  terminal = false,
) {
  return useQuery({
    queryKey: ["filing-investigation-events", analysisId, terminal],
    queryFn: () => getFilingInvestigationEvents(analysisId as string),
    enabled: Boolean(analysisId),
    refetchInterval: terminal ? false : 1_000,
  });
}

export function useFilingInvestigationValidation(
  analysisId: string | null,
  terminal: boolean,
) {
  return useQuery({
    queryKey: ["filing-investigation-validation", analysisId],
    queryFn: () => getFilingInvestigationValidation(analysisId as string),
    enabled: Boolean(analysisId) && terminal,
  });
}

export function useEvaluateFilingInvestigation() {
  return useMutation({
    mutationFn: (analysisId: string) => evaluateFilingInvestigation(analysisId),
  });
}
