import { DatabaseZap, Play, RefreshCw, SearchCheck, ShieldCheck } from "lucide-react";
import { FormEvent, useMemo, useState } from "react";
import { useQueryClient } from "@tanstack/react-query";

import {
  useCreateDataPipelineRequest,
  useDataCoveragePreview,
  useDataPipelineHealth,
  useDataPipelineRunDetail,
  useDataPipelineRuns,
  useUpstoxProviderCapabilities,
} from "../api/hooks";
import type {
  DataCoveragePreviewRequest,
  DataCoveragePreviewResponse,
  DataPipelineRunSummary,
  ProviderHistoricalCapability,
} from "../api/types";
import { EmptyState, LoadingState } from "../components/DataState";
import { MetricCard } from "../components/MetricCard";
import { PageHeader } from "../components/PageHeader";
import { formatDateTime } from "../utils/format";

function todayIso(): string {
  return new Date().toISOString().slice(0, 10);
}

function defaultStartIso(): string {
  const value = new Date();
  value.setFullYear(value.getFullYear() - 1);
  return value.toISOString().slice(0, 10);
}

function parseSymbols(value: string): string[] {
  return Array.from(
    new Set(
      value
        .split(/[,\s]+/)
        .map((item) => item.trim().toUpperCase())
        .filter(Boolean),
    ),
  );
}

function formatNumber(value: number | null | undefined): string {
  return value === null || value === undefined ? "0" : value.toLocaleString();
}

function statusClass(status: string): string {
  if (status.includes("fail")) return "failed";
  if (status.includes("warning") || status.includes("empty")) return "warning";
  if (status.includes("running") || status.includes("queued")) return "running";
  return "completed";
}

export function DataPipelinePage() {
  const queryClient = useQueryClient();
  const [symbolsText, setSymbolsText] = useState("RELIANCE, INFY");
  const [startDate, setStartDate] = useState(defaultStartIso());
  const [endDate, setEndDate] = useState(todayIso());
  const [selectedRunId, setSelectedRunId] = useState<string | null>(null);

  const capabilitiesQuery = useUpstoxProviderCapabilities();
  const healthQuery = useDataPipelineHealth();
  const runsQuery = useDataPipelineRuns();
  const previewMutation = useDataCoveragePreview();
  const createMutation = useCreateDataPipelineRequest();
  const selectedRunQuery = useDataPipelineRunDetail(selectedRunId);

  const requestPayload = useMemo<DataCoveragePreviewRequest>(
    () => ({
      provider: "upstox",
      exchange: "NSE",
      symbols: parseSymbols(symbolsText),
      unit: "days",
      interval: 1,
      start_date: startDate,
      end_date: endDate,
    }),
    [endDate, startDate, symbolsText],
  );

  const preview = previewMutation.data ?? null;
  const runDetail = selectedRunQuery.data ?? createMutation.data ?? null;
  const runs = runsQuery.data ?? [];
  const canSubmit = requestPayload.symbols.length > 0 && Boolean(startDate) && Boolean(endDate);
  const runBlockers = runSafetyBlockers(preview, healthQuery.data, canSubmit);
  const canRun = runBlockers.length === 0 && !createMutation.isPending;

  function onPreview(event: FormEvent) {
    event.preventDefault();
    if (!canSubmit) return;
    previewMutation.mutate(requestPayload);
  }

  async function onRun() {
    if (!canSubmit) return;
    const result = await createMutation.mutateAsync({
      ...requestPayload,
      steps: ["fetch_ohlcv", "validate_ohlcv"],
      mode: "incremental_missing_only",
    });
    setSelectedRunId(result.run.id);
    await queryClient.invalidateQueries({ queryKey: ["data-pipeline-runs"] });
    await queryClient.invalidateQueries({ queryKey: ["job-runs"] });
  }

  return (
    <>
      <PageHeader
        eyebrow="Data Pipelines"
        title="On-Demand OHLCV"
        subtitle="Request NSE daily candles, preview database coverage, and run missing-window Upstox fetches."
      />

      <div className="data-workspace">
        <section className="panel data-request-panel">
          <div className="panel-header">
            <h2>New Data Request</h2>
            <div className="inline-controls">
              <span className="muted-label">Upstox · NSE · 1 day</span>
            </div>
          </div>
          <form className="data-request-form" onSubmit={onPreview}>
            <label>
              Symbols
              <textarea
                value={symbolsText}
                onChange={(event) => setSymbolsText(event.target.value)}
                placeholder="RELIANCE, INFY, HDFCBANK"
                rows={4}
              />
            </label>
            <div className="form-grid">
              <label>
                Start
                <input
                  type="date"
                  value={startDate}
                  onChange={(event) => setStartDate(event.target.value)}
                />
              </label>
              <label>
                End
                <input
                  type="date"
                  value={endDate}
                  onChange={(event) => setEndDate(event.target.value)}
                />
              </label>
            </div>
            <div className="request-actions">
              <button className="icon-button" type="submit" disabled={!canSubmit || previewMutation.isPending}>
                <SearchCheck size={17} />
                <span>{previewMutation.isPending ? "Previewing" : "Preview"}</span>
              </button>
              <button
                className="icon-button primary"
                type="button"
                disabled={!canRun}
                onClick={() => void onRun()}
                title={runBlockers[0] ?? undefined}
              >
                <Play size={17} />
                <span>{createMutation.isPending ? "Running" : "Run"}</span>
              </button>
            </div>
            {previewMutation.error ? (
              <p className="form-error">{previewMutation.error.message}</p>
            ) : null}
            {createMutation.error ? <p className="form-error">{createMutation.error.message}</p> : null}
            <SafetyChecklist
              blockers={runBlockers}
              tokenConfigured={Boolean(healthQuery.data?.upstox_access_token_configured)}
              maxConcurrentFetches={healthQuery.data?.max_concurrent_fetches ?? 1}
              preview={preview}
            />
          </form>
        </section>

        <ProviderLimitsPanel capabilities={capabilitiesQuery.data?.historical ?? []} />
      </div>

      {preview ? <PreviewSection preview={preview} /> : null}

      <div className="data-workspace">
        <RunHistoryPanel
          runs={runs}
          isLoading={runsQuery.isLoading}
          selectedRunId={selectedRunId}
          onSelect={setSelectedRunId}
          onRefresh={() => void runsQuery.refetch()}
        />
        <RunDetailPanel detail={runDetail} isLoading={selectedRunQuery.isLoading} />
      </div>
    </>
  );
}

function runSafetyBlockers(
  preview: DataCoveragePreviewResponse | null,
  health: import("../api/types").DataPipelineHealthResponse | undefined,
  canSubmit: boolean,
): string[] {
  const blockers: string[] = [];
  if (!canSubmit) blockers.push("Enter at least one symbol and a date range.");
  if (!health?.daily_ohlcv_enabled) blockers.push("Daily OHLCV pipeline is not enabled.");
  if (!health?.upstox_access_token_configured) blockers.push("UPSTOX_ACCESS_TOKEN is not configured.");
  if (!preview) blockers.push("Preview coverage before running.");
  if (preview && preview.missing_rows <= 0) blockers.push("No missing rows to fetch.");
  if (preview && preview.unresolved_symbols.length > 0) blockers.push("Resolve unmatched symbols first.");
  if (preview && preview.ambiguous_symbols.length > 0) blockers.push("Resolve ambiguous symbols first.");
  return blockers;
}

function SafetyChecklist({
  blockers,
  tokenConfigured,
  maxConcurrentFetches,
  preview,
}: {
  blockers: string[];
  tokenConfigured: boolean;
  maxConcurrentFetches: number;
  preview: DataCoveragePreviewResponse | null;
}) {
  return (
    <div className={`safety-box ${blockers.length ? "warning" : "ready"}`}>
      <strong>{blockers.length ? "Run blocked" : "Ready to run"}</strong>
      <ul>
        {blockers.length ? (
          blockers.map((item) => <li key={item}>{item}</li>)
        ) : (
          <>
            <li>Upstox token configured.</li>
            <li>Coverage preview is clean.</li>
            <li>Concurrency limit: {maxConcurrentFetches} fetches.</li>
            <li>{formatNumber(preview?.missing_rows)} missing daily rows will be requested.</li>
          </>
        )}
        {!tokenConfigured && blockers.length === 0 ? <li>UPSTOX_ACCESS_TOKEN is missing.</li> : null}
      </ul>
    </div>
  );
}

function ProviderLimitsPanel({ capabilities }: { capabilities: ProviderHistoricalCapability[] }) {
  const daily = capabilities.find((item) => item.unit === "days");
  const intraday = capabilities.filter((item) => item.unit === "minutes" || item.unit === "hours");

  return (
    <section className="panel provider-limits-panel">
      <div className="panel-header">
        <h2>Upstox Limits</h2>
        <ShieldCheck size={18} />
      </div>
      {capabilities.length ? (
        <div className="limits-list">
          <div>
            <span>Daily MVP</span>
            <strong>{daily?.available_from ?? "n/a"}</strong>
            <small>{daily?.max_window ?? "No documented per-request limit"}</small>
          </div>
          {intraday.map((item) => (
            <div key={`${item.unit}-${item.interval_min}-${item.interval_max}`}>
              <span>
                {item.interval_min}-{item.interval_max} {item.unit}
              </span>
              <strong>{item.available_from}</strong>
              <small>{item.max_window ?? "No documented per-request limit"}</small>
            </div>
          ))}
        </div>
      ) : (
        <LoadingState />
      )}
    </section>
  );
}

function PreviewSection({ preview }: { preview: DataCoveragePreviewResponse }) {
  return (
    <>
      <div className="metric-grid">
        <MetricCard
          icon={DatabaseZap}
          label="Expected Rows"
          value={formatNumber(preview.expected_rows)}
          detail={`${preview.symbols_resolved}/${preview.symbols_requested} symbols resolved`}
        />
        <MetricCard
          icon={ShieldCheck}
          label="Already Stored"
          value={formatNumber(preview.already_present_rows)}
          detail="Canonical daily rows"
        />
        <MetricCard
          icon={SearchCheck}
          label="Missing Rows"
          value={formatNumber(preview.missing_rows)}
          detail={`${formatNumber(preview.estimated_provider_calls)} provider calls`}
        />
        <MetricCard
          icon={RefreshCw}
          label="Warnings"
          value={formatNumber(preview.warnings.length)}
          detail={preview.warnings[0] ?? "No warnings"}
        />
      </div>

      <section className="panel">
        <div className="panel-header">
          <h2>Preview Tasks</h2>
          <span className="muted-label">{preview.provider} · {preview.unit}/{preview.interval}</span>
        </div>
        {preview.tasks.length ? (
          <div className="table-wrap">
            <table>
              <thead>
                <tr>
                  <th>Symbol</th>
                  <th>Instrument</th>
                  <th>Fetch Start</th>
                  <th>Fetch End</th>
                  <th>Missing Rows</th>
                </tr>
              </thead>
              <tbody>
                {preview.tasks.map((task) => (
                  <tr key={`${task.instrument_key}-${task.fetch_start}-${task.fetch_end}`}>
                    <td>{task.symbol}</td>
                    <td>{task.instrument_key}</td>
                    <td>{task.fetch_start}</td>
                    <td>{task.fetch_end}</td>
                    <td>{task.missing_rows.toLocaleString()}</td>
                  </tr>
                ))}
              </tbody>
            </table>
          </div>
        ) : (
          <EmptyState label="No missing daily rows for the selected request." />
        )}
      </section>
      {preview.warnings.length ||
      preview.unresolved_symbols.length ||
      preview.ambiguous_symbols.length ? (
        <section className="panel">
          <div className="panel-header">
            <h2>Preview Warnings</h2>
          </div>
          <ul className="warning-list">
            {preview.warnings.map((warning) => (
              <li key={warning}>{warning}</li>
            ))}
            {preview.unresolved_symbols.map((symbol) => (
              <li key={`unresolved-${symbol}`}>{symbol} was not resolved to an Upstox NSE instrument.</li>
            ))}
            {preview.ambiguous_symbols.map((symbol) => (
              <li key={`ambiguous-${symbol}`}>{symbol} matched multiple instruments.</li>
            ))}
          </ul>
        </section>
      ) : null}
    </>
  );
}

function RunHistoryPanel({
  runs,
  isLoading,
  selectedRunId,
  onSelect,
  onRefresh,
}: {
  runs: DataPipelineRunSummary[];
  isLoading: boolean;
  selectedRunId: string | null;
  onSelect: (runId: string) => void;
  onRefresh: () => void;
}) {
  return (
    <section className="panel">
      <div className="panel-header">
        <h2>Run History</h2>
        <button className="icon-button" type="button" onClick={onRefresh}>
          <RefreshCw size={16} />
          <span>Refresh</span>
        </button>
      </div>
      {isLoading ? (
        <LoadingState />
      ) : runs.length ? (
        <div className="table-wrap">
          <table>
            <thead>
              <tr>
                <th>Run</th>
                <th>Status</th>
                <th>Started</th>
                <th>Items</th>
              </tr>
            </thead>
            <tbody>
              {runs.map((run) => (
                <tr
                  key={run.id}
                  className={selectedRunId === run.id ? "selected-row" : ""}
                  onClick={() => onSelect(run.id)}
                >
                  <td>{run.name}</td>
                  <td>
                    <span className={`status-pill ${statusClass(run.status)}`}>{run.status}</span>
                  </td>
                  <td>{formatDateTime(run.started_at)}</td>
                  <td>{run.items_processed.toLocaleString()}</td>
                </tr>
              ))}
            </tbody>
          </table>
        </div>
      ) : (
        <EmptyState label="No data pipeline runs found." />
      )}
    </section>
  );
}

function RunDetailPanel({
  detail,
  isLoading,
}: {
  detail: import("../api/types").DataPipelineRunDetail | null;
  isLoading: boolean;
}) {
  if (isLoading) {
    return (
      <section className="panel">
        <LoadingState />
      </section>
    );
  }

  if (!detail) {
    return (
      <section className="panel">
        <EmptyState label="Select a run to inspect fetch coverage." />
      </section>
    );
  }

  return (
    <section className="panel">
      <div className="panel-header">
        <h2>Run Detail</h2>
        <span className={`status-pill ${statusClass(detail.run.status)}`}>{detail.run.status}</span>
      </div>
      <dl className="detail-grid data-detail-grid">
        <div>
          <dt>Run ID</dt>
          <dd>{detail.run.id}</dd>
        </div>
        <div>
          <dt>Duration</dt>
          <dd>{detail.run.duration_seconds === null ? "Running" : `${detail.run.duration_seconds}s`}</dd>
        </div>
        <div>
          <dt>Failed</dt>
          <dd>{detail.run.items_failed.toLocaleString()}</dd>
        </div>
      </dl>
      {detail.fetch_coverage.length ? (
        <div className="table-wrap">
          <table>
            <thead>
              <tr>
                <th>Symbol</th>
                <th>Status</th>
                <th>Fetch Start</th>
                <th>Fetch End</th>
                <th>Rows</th>
              </tr>
            </thead>
            <tbody>
              {detail.fetch_coverage.map((row) => (
                <tr key={`${row.run_id}-${row.instrument_key}`}>
                  <td>{row.symbol}</td>
                  <td>
                    <span className={`status-pill ${statusClass(row.status)}`}>{row.status}</span>
                  </td>
                  <td>{row.fetch_start ?? "n/a"}</td>
                  <td>{row.fetch_end}</td>
                  <td>{row.rows_fetched.toLocaleString()}</td>
                </tr>
              ))}
            </tbody>
          </table>
        </div>
      ) : (
        <EmptyState label="No fetch coverage rows stored for this run." />
      )}
      {detail.fetch_coverage.some((row) => row.error_message) ? (
        <ul className="warning-list">
          {detail.fetch_coverage
            .filter((row) => row.error_message)
            .map((row) => (
              <li key={`${row.instrument_key}-error`}>
                {row.symbol}: {row.error_message}
              </li>
            ))}
        </ul>
      ) : null}
    </section>
  );
}
