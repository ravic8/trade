import {
  ChevronLeft,
  ChevronRight,
  CheckCircle2,
  DatabaseZap,
  History,
  Info,
  Play,
  RefreshCw,
  Search,
  SearchCheck,
  ShieldCheck,
  TableProperties,
  X,
} from "lucide-react";
import { FormEvent, useMemo, useState } from "react";
import type { ClipboardEvent, KeyboardEvent } from "react";
import { useQueryClient } from "@tanstack/react-query";

import {
  useCreateDataPipelineRequest,
  useDataAvailability,
  useDataCoveragePreview,
  useDataInstrumentSearch,
  useDataPipelineHealth,
  useDataPipelineRunDetail,
  useDataPipelineRuns,
  useDataUniverseMembers,
  useDataUniverses,
  useUpstoxProviderCapabilities,
} from "../api/hooks";
import type {
  DataAvailabilityParams,
  DataAvailabilityResponse,
  DataCoveragePreviewRequest,
  DataCoveragePreviewResponse,
  DataInstrumentSearchRow,
  DataPipelineRunSummary,
  DataUniverseMemberRow,
  DataUniverseRow,
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

function splitSymbols(value: string): string[] {
  return value
    .split(/[,\s]+/)
    .map((item) => item.trim().toUpperCase())
    .filter(Boolean);
}

function formatNumber(value: number | null | undefined): string {
  return value === null || value === undefined ? "0" : value.toLocaleString();
}

function formatPercent(value: number | null | undefined): string {
  if (value === null || value === undefined) return "0%";
  return `${Math.round(value * 1000) / 10}%`;
}

function statusClass(status: string): string {
  if (status.includes("fail")) return "failed";
  if (status.includes("warning") || status.includes("empty") || status.includes("partial")) {
    return "warning";
  }
  if (status.includes("running") || status.includes("queued")) return "running";
  return "completed";
}

type DataTab = "request" | "available" | "runs";
type SymbolScopeMode = "single" | "nifty50" | "nifty100" | "most_liquid" | "custom";
type BulkFetchMode = "missing_only" | "stale_only" | "all";

export function DataPipelinePage() {
  const queryClient = useQueryClient();
  const [activeTab, setActiveTab] = useState<DataTab>("request");
  const [selectedSymbols, setSelectedSymbols] = useState<string[]>(["RELIANCE", "INFY"]);
  const [symbolInput, setSymbolInput] = useState("");
  const [duplicateSymbols, setDuplicateSymbols] = useState<string[]>([]);
  const [symbolScopeMode, setSymbolScopeMode] = useState<SymbolScopeMode>("single");
  const [customUniverseId, setCustomUniverseId] = useState("");
  const [mostLiquidMaxSymbols, setMostLiquidMaxSymbols] = useState(100);
  const [bulkFetchMode, setBulkFetchMode] = useState<BulkFetchMode>("missing_only");
  const [startDate, setStartDate] = useState(defaultStartIso());
  const [endDate, setEndDate] = useState(todayIso());
  const [availabilityQuery, setAvailabilityQuery] = useState("");
  const [availabilityStatus, setAvailabilityStatus] =
    useState<DataAvailabilityParams["coverage_status"]>("");
  const [availabilitySort, setAvailabilitySort] = useState("-coverage_pct");
  const [availabilityPage, setAvailabilityPage] = useState(0);
  const [selectedRunId, setSelectedRunId] = useState<string | null>(null);

  const availabilityLimit = 25;
  const capabilitiesQuery = useUpstoxProviderCapabilities();
  const healthQuery = useDataPipelineHealth();
  const runsQuery = useDataPipelineRuns();
  const previewMutation = useDataCoveragePreview();
  const createMutation = useCreateDataPipelineRequest();
  const selectedRunQuery = useDataPipelineRunDetail(selectedRunId);
  const universesQuery = useDataUniverses();
  const universes = universesQuery.data ?? [];
  const selectedUniverseId = selectedUniverseForMode(
    symbolScopeMode,
    universes,
    customUniverseId,
  );
  const universeMemberLimit =
    symbolScopeMode === "most_liquid" ? mostLiquidMaxSymbols : 500;
  const universeMembersQuery = useDataUniverseMembers(
    selectedUniverseId,
    universeMemberLimit,
  );
  const symbolSearchTerm = symbolInput.trim();
  const instrumentSearchQuery = useDataInstrumentSearch(
    {
      provider: "upstox",
      exchange: "NSE",
      query: symbolSearchTerm,
      limit: 8,
    },
    symbolSearchTerm.length >= 2,
  );

  const availabilityParams = useMemo<DataAvailabilityParams>(
    () => ({
      provider: "upstox",
      exchange: "NSE",
      interval: "1d",
      start_date: startDate,
      end_date: endDate,
      query: availabilityQuery.trim() || undefined,
      coverage_status: availabilityStatus,
      limit: availabilityLimit,
      offset: availabilityPage * availabilityLimit,
      sort: availabilitySort,
    }),
    [availabilityPage, availabilityQuery, availabilitySort, availabilityStatus, endDate, startDate],
  );
  const availabilityQueryResult = useDataAvailability(availabilityParams);

  const requestPayload = useMemo<DataCoveragePreviewRequest>(
    () => ({
      provider: "upstox",
      exchange: "NSE",
      symbols: selectedSymbols,
      unit: "days",
      interval: 1,
      start_date: startDate,
      end_date: endDate,
    }),
    [endDate, selectedSymbols, startDate],
  );

  const preview = previewMutation.data ?? null;
  const runDetail = selectedRunQuery.data ?? createMutation.data ?? null;
  const runs = runsQuery.data ?? [];
  const canSubmit = requestPayload.symbols.length > 0 && Boolean(startDate) && Boolean(endDate);
  const runBlockers = runSafetyBlockers(
    preview,
    healthQuery.data,
    canSubmit,
    symbolScopeMode,
    bulkFetchMode,
  );
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
    await queryClient.invalidateQueries({ queryKey: ["data-availability"] });
    await queryClient.invalidateQueries({ queryKey: ["job-runs"] });
  }

  function updateSelectedSymbols(nextSymbols: string[], duplicates: string[] = []) {
    setSelectedSymbols(nextSymbols);
    setDuplicateSymbols(duplicates);
    previewMutation.reset();
    createMutation.reset();
  }

  function addSymbols(symbols: string[]) {
    const normalized = splitSymbols(symbols.join(" "));
    if (!normalized.length) return;
    const existing = new Set(selectedSymbols);
    const duplicates: string[] = [];
    const additions: string[] = [];
    for (const symbol of normalized) {
      if (existing.has(symbol) || additions.includes(symbol)) {
        duplicates.push(symbol);
      } else {
        additions.push(symbol);
      }
    }
    updateSelectedSymbols([...selectedSymbols, ...additions], Array.from(new Set(duplicates)));
    setSymbolInput("");
  }

  function removeSymbol(symbol: string) {
    updateSelectedSymbols(selectedSymbols.filter((item) => item !== symbol));
  }

  function removeSymbols(symbols: string[]) {
    const blocked = new Set(symbols);
    updateSelectedSymbols(selectedSymbols.filter((item) => !blocked.has(item)));
  }

  function applyUniverseMembers(members: DataUniverseMemberRow[]) {
    const symbols = members.map((member) => member.symbol).filter(Boolean);
    updateSelectedSymbols(Array.from(new Set(symbols)));
    setSymbolInput("");
  }

  return (
    <div className="data-console-page">
      <PageHeader
        eyebrow="Data Pipelines"
        title="Data Console"
        subtitle="Request NSE daily candles, inspect stored coverage, and review Upstox fetch runs."
        actions={<SystemStatusPill />}
      />

      <DataTabs activeTab={activeTab} onChange={setActiveTab} />

      {activeTab === "request" ? (
        <>
          <div className="data-workspace">
            <section className="panel data-request-panel">
              <div className="panel-header">
                <h2>New Data Request</h2>
                <div className="inline-controls">
                  <span className="muted-label">Upstox · NSE · 1 day</span>
                </div>
              </div>
              <form className="data-request-form" onSubmit={onPreview}>
                <UniverseSelector
                  mode={symbolScopeMode}
                  universes={universes}
                  selectedUniverseId={selectedUniverseId}
                  customUniverseId={customUniverseId}
                  maxSymbols={mostLiquidMaxSymbols}
                  bulkFetchMode={bulkFetchMode}
                  memberCount={universeMembersQuery.data?.length ?? 0}
                  isLoadingUniverses={universesQuery.isLoading}
                  isLoadingMembers={universeMembersQuery.isFetching}
                  onModeChange={(mode) => {
                    setSymbolScopeMode(mode);
                    setBulkFetchMode(mode === "most_liquid" ? "missing_only" : bulkFetchMode);
                    previewMutation.reset();
                    createMutation.reset();
                  }}
                  onCustomUniverseChange={(universeId) => {
                    setCustomUniverseId(universeId);
                    previewMutation.reset();
                    createMutation.reset();
                  }}
                  onMaxSymbolsChange={(value) => {
                    setMostLiquidMaxSymbols(value);
                    previewMutation.reset();
                    createMutation.reset();
                  }}
                  onBulkFetchModeChange={(mode) => {
                    setBulkFetchMode(mode);
                    previewMutation.reset();
                    createMutation.reset();
                  }}
                  onApply={() => applyUniverseMembers(universeMembersQuery.data ?? [])}
                />
                <SymbolPicker
                  selectedSymbols={selectedSymbols}
                  inputValue={symbolInput}
                  onInputChange={setSymbolInput}
                  onAddSymbols={addSymbols}
                  onRemoveSymbol={removeSymbol}
                  onRemoveSymbols={removeSymbols}
                  searchResults={instrumentSearchQuery.data ?? []}
                  isSearching={instrumentSearchQuery.isFetching}
                  searchError={instrumentSearchQuery.error}
                  duplicateSymbols={duplicateSymbols}
                  unresolvedSymbols={preview?.unresolved_symbols ?? []}
                  ambiguousSymbols={preview?.ambiguous_symbols ?? []}
                />
                <div className="form-grid">
                  <label>
                    Start
                    <input
                      type="date"
                      value={startDate}
                      onChange={(event) => {
                        setStartDate(event.target.value);
                        setAvailabilityPage(0);
                      }}
                    />
                  </label>
                  <label>
                    End
                    <input
                      type="date"
                      value={endDate}
                      onChange={(event) => {
                        setEndDate(event.target.value);
                        setAvailabilityPage(0);
                      }}
                    />
                  </label>
                </div>
                <div className="request-actions">
                  <button
                    className="icon-button"
                    type="submit"
                    disabled={!canSubmit || previewMutation.isPending}
                  >
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
                {createMutation.error ? (
                  <p className="form-error">{createMutation.error.message}</p>
                ) : null}
                <SafetyChecklist
                  blockers={runBlockers}
                  tokenConfigured={Boolean(healthQuery.data?.upstox_access_token_configured)}
                  maxConcurrentFetches={healthQuery.data?.max_concurrent_fetches ?? 1}
                  preview={preview}
                />
              </form>
            </section>

            <aside className="data-side-rail">
              <ProviderLimitsPanel capabilities={capabilitiesQuery.data?.historical ?? []} />
              <MarketDataNotice />
              <ReadinessPanel
                blockers={runBlockers}
                tokenConfigured={Boolean(healthQuery.data?.upstox_access_token_configured)}
                preview={preview}
              />
            </aside>
          </div>

          {preview ? <PreviewSection preview={preview} /> : null}
        </>
      ) : null}

      {activeTab === "available" ? (
        <AvailableDataPanel
          availability={availabilityQueryResult.data ?? null}
          error={availabilityQueryResult.error}
          isLoading={availabilityQueryResult.isLoading}
          isFetching={availabilityQueryResult.isFetching}
          query={availabilityQuery}
          status={availabilityStatus ?? ""}
          sort={availabilitySort}
          startDate={startDate}
          endDate={endDate}
          page={availabilityPage}
          limit={availabilityLimit}
          onQueryChange={(value) => {
            setAvailabilityQuery(value);
            setAvailabilityPage(0);
          }}
          onStatusChange={(value) => {
            setAvailabilityStatus(value);
            setAvailabilityPage(0);
          }}
          onSortChange={(value) => {
            setAvailabilitySort(value);
            setAvailabilityPage(0);
          }}
          onStartDateChange={(value) => {
            setStartDate(value);
            setAvailabilityPage(0);
          }}
          onEndDateChange={(value) => {
            setEndDate(value);
            setAvailabilityPage(0);
          }}
          onPageChange={setAvailabilityPage}
          onRefresh={() => void availabilityQueryResult.refetch()}
        />
      ) : null}

      {activeTab === "runs" ? (
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
      ) : null}
    </div>
  );
}

function DataTabs({
  activeTab,
  onChange,
}: {
  activeTab: DataTab;
  onChange: (tab: DataTab) => void;
}) {
  const tabs: { id: DataTab; label: string; icon: typeof SearchCheck }[] = [
    { id: "request", label: "Request Data", icon: SearchCheck },
    { id: "available", label: "Available Data", icon: TableProperties },
    { id: "runs", label: "Runs", icon: History },
  ];
  return (
    <div className="segmented-tabs" role="tablist" aria-label="Data console views">
      {tabs.map((tab) => {
        const Icon = tab.icon;
        return (
          <button
            key={tab.id}
            type="button"
            className={activeTab === tab.id ? "active" : ""}
            onClick={() => onChange(tab.id)}
          >
            <Icon size={16} />
            <span>{tab.label}</span>
          </button>
        );
      })}
    </div>
  );
}

function SystemStatusPill() {
  return (
    <span className="system-status-pill">
      <span aria-hidden="true" />
      System Active
    </span>
  );
}

function selectedUniverseForMode(
  mode: SymbolScopeMode,
  universes: DataUniverseRow[],
  customUniverseId: string,
): string | null {
  if (mode === "custom") return customUniverseId || null;
  if (mode === "most_liquid") return findUniverse(universes, ["liquid", "adt"]);
  if (mode === "nifty50") return findUniverse(universes, ["nifty", "50"]);
  if (mode === "nifty100") return findUniverse(universes, ["nifty", "100"]);
  return null;
}

function findUniverse(universes: DataUniverseRow[], terms: string[]): string | null {
  const match = universes.find((universe) => {
    const haystack = `${universe.universe_id} ${universe.name}`.toLowerCase();
    return terms.every((term) => haystack.includes(term));
  });
  return match?.universe_id ?? null;
}

function UniverseSelector({
  mode,
  universes,
  selectedUniverseId,
  customUniverseId,
  maxSymbols,
  bulkFetchMode,
  memberCount,
  isLoadingUniverses,
  isLoadingMembers,
  onModeChange,
  onCustomUniverseChange,
  onMaxSymbolsChange,
  onBulkFetchModeChange,
  onApply,
}: {
  mode: SymbolScopeMode;
  universes: DataUniverseRow[];
  selectedUniverseId: string | null;
  customUniverseId: string;
  maxSymbols: number;
  bulkFetchMode: BulkFetchMode;
  memberCount: number;
  isLoadingUniverses: boolean;
  isLoadingMembers: boolean;
  onModeChange: (mode: SymbolScopeMode) => void;
  onCustomUniverseChange: (universeId: string) => void;
  onMaxSymbolsChange: (value: number) => void;
  onBulkFetchModeChange: (mode: BulkFetchMode) => void;
  onApply: () => void;
}) {
  const selectedUniverse = universes.find(
    (universe) => universe.universe_id === selectedUniverseId,
  );
  const unavailable =
    mode !== "single" && !selectedUniverseId && !isLoadingUniverses;
  const applyDisabled =
    mode === "single" ||
    unavailable ||
    isLoadingMembers ||
    memberCount === 0;

  return (
    <div className="universe-selector">
      <div className="symbol-picker-label">
        <span>Selection</span>
        <small>{selectedUniverse?.member_count.toLocaleString() ?? "manual"}</small>
      </div>
      <div className="scope-grid" role="radiogroup" aria-label="Symbol selection scope">
        {[
          ["single", "Single symbols"],
          ["nifty50", "NIFTY 50"],
          ["nifty100", "NIFTY 100"],
          ["most_liquid", "Most liquid"],
          ["custom", "Custom"],
        ].map(([value, label]) => (
          <button
            key={value}
            type="button"
            className={mode === value ? "active" : ""}
            onClick={() => onModeChange(value as SymbolScopeMode)}
          >
            {label}
          </button>
        ))}
      </div>

      {mode === "custom" ? (
        <label className="universe-field">
          Universe
          <select
            value={customUniverseId}
            onChange={(event) => onCustomUniverseChange(event.target.value)}
          >
            <option value="">Select universe</option>
            {universes.map((universe) => (
              <option key={universe.universe_id} value={universe.universe_id}>
                {universe.name} ({universe.member_count})
              </option>
            ))}
          </select>
        </label>
      ) : null}

      {mode === "most_liquid" ? (
        <div className="universe-options">
          <label>
            Max symbols
            <select
              value={maxSymbols}
              onChange={(event) => onMaxSymbolsChange(Number(event.target.value))}
            >
              {[50, 100, 200, 500].map((value) => (
                <option key={value} value={value}>
                  {value}
                </option>
              ))}
            </select>
          </label>
          <label>
            Mode
            <select
              value={bulkFetchMode}
              onChange={(event) => onBulkFetchModeChange(event.target.value as BulkFetchMode)}
            >
              <option value="missing_only">Missing only</option>
              <option value="stale_only">Stale only</option>
              <option value="all">All</option>
            </select>
          </label>
        </div>
      ) : null}

      {mode !== "single" ? (
        <div className="universe-apply-row">
          <div>
            <strong>{selectedUniverse?.name ?? "Universe unavailable"}</strong>
            <span>
              {unavailable
                ? "No matching saved universe found."
                : `${memberCount.toLocaleString()} members ready to apply`}
            </span>
          </div>
          <button
            className="icon-button"
            type="button"
            disabled={applyDisabled}
            onClick={onApply}
          >
            <TableProperties size={16} />
            <span>{isLoadingMembers ? "Loading" : "Apply"}</span>
          </button>
        </div>
      ) : null}
    </div>
  );
}

function SymbolPicker({
  selectedSymbols,
  inputValue,
  onInputChange,
  onAddSymbols,
  onRemoveSymbol,
  onRemoveSymbols,
  searchResults,
  isSearching,
  searchError,
  duplicateSymbols,
  unresolvedSymbols,
  ambiguousSymbols,
}: {
  selectedSymbols: string[];
  inputValue: string;
  onInputChange: (value: string) => void;
  onAddSymbols: (symbols: string[]) => void;
  onRemoveSymbol: (symbol: string) => void;
  onRemoveSymbols: (symbols: string[]) => void;
  searchResults: DataInstrumentSearchRow[];
  isSearching: boolean;
  searchError: Error | null;
  duplicateSymbols: string[];
  unresolvedSymbols: string[];
  ambiguousSymbols: string[];
}) {
  const canSearch = inputValue.trim().length >= 2;
  const showSuggestions = canSearch;
  const issueRows = [
    duplicateSymbols.length
      ? `Duplicate symbols skipped: ${duplicateSymbols.join(", ")}`
      : null,
  ].filter((item): item is string => Boolean(item));

  function commitInput() {
    const symbols = splitSymbols(inputValue);
    if (symbols.length) onAddSymbols(symbols);
  }

  function onKeyDown(event: KeyboardEvent<HTMLInputElement>) {
    if (event.key === "Enter" || event.key === "," || event.key === "Tab") {
      if (!inputValue.trim()) return;
      event.preventDefault();
      commitInput();
    }
    if (event.key === "Backspace" && !inputValue && selectedSymbols.length) {
      onRemoveSymbol(selectedSymbols[selectedSymbols.length - 1]);
    }
  }

  function onPaste(event: ClipboardEvent<HTMLInputElement>) {
    const pasted = event.clipboardData.getData("text");
    const parsed = splitSymbols(pasted);
    if (parsed.length <= 1) return;
    event.preventDefault();
    onAddSymbols(parsed);
  }

  return (
    <div className="symbol-picker">
      <div className="symbol-picker-label">
        <span>Symbols</span>
        <small>{selectedSymbols.length.toLocaleString()} selected</small>
      </div>
      <div className="symbol-chip-box">
        {selectedSymbols.map((symbol) => (
          <span className="symbol-chip" key={symbol}>
            {symbol}
            <button
              type="button"
              aria-label={`Remove ${symbol}`}
              onClick={() => onRemoveSymbol(symbol)}
            >
              <X size={14} />
            </button>
          </span>
        ))}
        <input
          type="text"
          value={inputValue}
          onChange={(event) => onInputChange(event.target.value)}
          onKeyDown={onKeyDown}
          onPaste={onPaste}
          placeholder={selectedSymbols.length ? "Add symbol" : "Search or paste symbols"}
        />
      </div>
      {showSuggestions ? (
        <div className="instrument-suggestions">
          {isSearching ? <span className="suggestion-state">Searching instruments</span> : null}
          {searchError ? (
            <span className="suggestion-state error">{searchError.message}</span>
          ) : null}
          {!isSearching && !searchError && searchResults.length === 0 ? (
            <span className="suggestion-state">No matching NSE equity instruments</span>
          ) : null}
          {searchResults.map((row) => (
            <button
              type="button"
              key={row.instrument_key}
              onMouseDown={(event) => event.preventDefault()}
              onClick={() => onAddSymbols([row.symbol])}
            >
              <strong>{row.symbol}</strong>
              <span>{row.name ?? row.instrument_key}</span>
              <small>{row.instrument_key}</small>
            </button>
          ))}
        </div>
      ) : null}
      {issueRows.length ? (
        <ul className="symbol-issues">
          {issueRows.map((item) => (
            <li key={item}>{item}</li>
          ))}
        </ul>
      ) : null}
      {unresolvedSymbols.length ? (
        <SymbolIssueAction
          label="Unresolved symbols"
          symbols={unresolvedSymbols}
          actionLabel="Remove unresolved"
          onRemove={onRemoveSymbols}
        />
      ) : null}
      {ambiguousSymbols.length ? (
        <SymbolIssueAction
          label="Ambiguous symbols"
          symbols={ambiguousSymbols}
          actionLabel="Remove ambiguous"
          onRemove={onRemoveSymbols}
        />
      ) : null}
    </div>
  );
}

function SymbolIssueAction({
  label,
  symbols,
  actionLabel,
  onRemove,
}: {
  label: string;
  symbols: string[];
  actionLabel: string;
  onRemove: (symbols: string[]) => void;
}) {
  return (
    <div className="symbol-issue-action">
      <span>
        {label}: {symbols.join(", ")}
      </span>
      <button type="button" onClick={() => onRemove(symbols)}>
        {actionLabel}
      </button>
    </div>
  );
}

function AvailableDataPanel({
  availability,
  error,
  isLoading,
  isFetching,
  query,
  status,
  sort,
  startDate,
  endDate,
  page,
  limit,
  onQueryChange,
  onStatusChange,
  onSortChange,
  onStartDateChange,
  onEndDateChange,
  onPageChange,
  onRefresh,
}: {
  availability: DataAvailabilityResponse | null;
  error: Error | null;
  isLoading: boolean;
  isFetching: boolean;
  query: string;
  status: DataAvailabilityParams["coverage_status"];
  sort: string;
  startDate: string;
  endDate: string;
  page: number;
  limit: number;
  onQueryChange: (value: string) => void;
  onStatusChange: (value: DataAvailabilityParams["coverage_status"]) => void;
  onSortChange: (value: string) => void;
  onStartDateChange: (value: string) => void;
  onEndDateChange: (value: string) => void;
  onPageChange: (page: number) => void;
  onRefresh: () => void;
}) {
  const summary = availability?.summary;
  const total = availability?.total ?? 0;
  const pageStart = total === 0 ? 0 : page * limit + 1;
  const pageEnd = Math.min((page + 1) * limit, total);
  const canGoBack = page > 0;
  const canGoForward = pageEnd < total;

  return (
    <>
      <div className="metric-grid">
        <MetricCard
          icon={TableProperties}
          label="Symbols"
          value={formatNumber(summary?.symbols_total)}
          detail={`${formatNumber(summary?.symbols_complete)} complete`}
        />
        <MetricCard
          icon={ShieldCheck}
          label="Stored Rows"
          value={formatNumber(summary?.stored_rows)}
          detail={`${formatNumber(summary?.expected_rows)} expected`}
        />
        <MetricCard
          icon={SearchCheck}
          label="Missing Rows"
          value={formatNumber(summary?.missing_rows)}
          detail={`${formatNumber(summary?.estimated_provider_calls_for_missing)} provider calls`}
        />
        <MetricCard
          icon={DatabaseZap}
          label="Partial / Empty"
          value={`${formatNumber(summary?.symbols_partial)} / ${formatNumber(summary?.symbols_empty)}`}
          detail="Coverage status"
        />
      </div>

      <section className="panel">
        <div className="panel-header">
          <h2>Available Data</h2>
          <div className="inline-controls">
            <span className="muted-label">Upstox · NSE · 1 day</span>
            <button className="icon-button" type="button" onClick={onRefresh}>
              <RefreshCw size={16} />
              <span>{isFetching && !isLoading ? "Refreshing" : "Refresh"}</span>
            </button>
          </div>
        </div>

        <div className="availability-filters">
          <label className="search-field">
            <Search size={16} />
            <input
              type="search"
              value={query}
              onChange={(event) => onQueryChange(event.target.value)}
              placeholder="Search symbol, name, ISIN, instrument"
            />
          </label>
          <label>
            Status
            <select
              value={status ?? ""}
              onChange={(event) =>
                onStatusChange(
                  event.target.value as DataAvailabilityParams["coverage_status"],
                )
              }
            >
              <option value="">All</option>
              <option value="complete">Complete</option>
              <option value="partial">Partial</option>
              <option value="empty">Empty</option>
            </select>
          </label>
          <label>
            Start
            <input
              type="date"
              value={startDate}
              onChange={(event) => onStartDateChange(event.target.value)}
            />
          </label>
          <label>
            End
            <input
              type="date"
              value={endDate}
              onChange={(event) => onEndDateChange(event.target.value)}
            />
          </label>
          <label>
            Sort
            <select value={sort} onChange={(event) => onSortChange(event.target.value)}>
              <option value="symbol">Symbol A-Z</option>
              <option value="-symbol">Symbol Z-A</option>
              <option value="-coverage_pct">Coverage high-low</option>
              <option value="coverage_pct">Coverage low-high</option>
              <option value="-missing_rows">Missing high-low</option>
              <option value="missing_rows">Missing low-high</option>
              <option value="-latest_stored_date">Latest stored</option>
              <option value="latest_stored_date">Oldest stored</option>
            </select>
          </label>
        </div>

        {error ? <p className="form-error availability-error">{error.message}</p> : null}

        {isLoading ? (
          <LoadingState />
        ) : availability?.rows.length ? (
          <>
            <div className="table-wrap availability-table">
              <table>
                <thead>
                  <tr>
                    <th>Symbol</th>
                    <th>Name</th>
                    <th>Instrument</th>
                    <th>First Stored</th>
                    <th>Latest Stored</th>
                    <th>Stored</th>
                    <th>Expected</th>
                    <th>Coverage</th>
                    <th>Missing</th>
                    <th>Last Fetch</th>
                  </tr>
                </thead>
                <tbody>
                  {availability.rows.map((row) => (
                    <tr key={row.instrument_key}>
                      <td data-label="Symbol">
                        <strong>{row.symbol}</strong>
                      </td>
                      <td data-label="Name" className="text-cell">{row.name ?? "n/a"}</td>
                      <td data-label="Instrument" className="mono-cell">{row.instrument_key}</td>
                      <td data-label="First Stored">{row.first_stored_date ?? "n/a"}</td>
                      <td data-label="Latest Stored">{row.latest_stored_date ?? "n/a"}</td>
                      <td data-label="Stored">{formatNumber(row.stored_rows)}</td>
                      <td data-label="Expected">{formatNumber(row.expected_rows)}</td>
                      <td data-label="Coverage">
                        <span className={`status-pill ${statusClass(row.coverage_status)}`}>
                          {formatPercent(row.coverage_pct)}
                        </span>
                      </td>
                      <td data-label="Missing">{formatNumber(row.missing_rows)}</td>
                      <td data-label="Last Fetch">
                        {row.last_fetch_status ? (
                          <span className={`status-pill ${statusClass(row.last_fetch_status)}`}>
                            {row.last_fetch_status}
                          </span>
                        ) : (
                          "n/a"
                        )}
                      </td>
                    </tr>
                  ))}
                </tbody>
              </table>
            </div>
            <div className="pagination-bar">
              <span>
                {formatNumber(pageStart)}-{formatNumber(pageEnd)} of {formatNumber(total)}
              </span>
              <div className="pagination-actions">
                <button
                  className="icon-button"
                  type="button"
                  disabled={!canGoBack}
                  onClick={() => onPageChange(page - 1)}
                >
                  <ChevronLeft size={16} />
                  <span>Previous</span>
                </button>
                <button
                  className="icon-button"
                  type="button"
                  disabled={!canGoForward}
                  onClick={() => onPageChange(page + 1)}
                >
                  <span>Next</span>
                  <ChevronRight size={16} />
                </button>
              </div>
            </div>
          </>
        ) : (
          <EmptyState label="No stored data matched the selected filters." />
        )}
      </section>
    </>
  );
}

function runSafetyBlockers(
  preview: DataCoveragePreviewResponse | null,
  health: import("../api/types").DataPipelineHealthResponse | undefined,
  canSubmit: boolean,
  scopeMode: SymbolScopeMode,
  bulkFetchMode: BulkFetchMode,
): string[] {
  const blockers: string[] = [];
  if (!canSubmit) blockers.push("Enter at least one symbol and a date range.");
  if (!health?.daily_ohlcv_enabled) blockers.push("Daily OHLCV pipeline is not enabled.");
  if (!health?.upstox_access_token_configured) blockers.push("UPSTOX_ACCESS_TOKEN is not configured.");
  if (scopeMode === "most_liquid" && bulkFetchMode !== "missing_only") {
    blockers.push("The MVP runner supports missing-only universe fetches.");
  }
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

function MarketDataNotice() {
  return (
    <section className="panel notice-panel">
      <div className="notice-heading">
        <Info size={16} />
        <span>Notice</span>
      </div>
      <p>End-of-day candles for NSE are generally available after 18:30 IST.</p>
    </section>
  );
}

function ReadinessPanel({
  blockers,
  tokenConfigured,
  preview,
}: {
  blockers: string[];
  tokenConfigured: boolean;
  preview: DataCoveragePreviewResponse | null;
}) {
  const rows = [
    {
      label: tokenConfigured ? "Upstox token configured" : "Upstox token missing",
      ready: tokenConfigured,
    },
    {
      label: preview ? "Coverage preview is available" : "Coverage preview pending",
      ready: Boolean(preview),
    },
    {
      label: blockers.length
        ? `${blockers.length} blocker${blockers.length === 1 ? "" : "s"} to clear`
        : "Ready to run",
      ready: blockers.length === 0,
    },
  ];

  return (
    <section className="panel readiness-panel">
      <h2>Readiness</h2>
      <ul>
        {rows.map((row) => (
          <li key={row.label} className={row.ready ? "ready" : "pending"}>
            <CheckCircle2 size={16} />
            <span>{row.label}</span>
          </li>
        ))}
      </ul>
    </section>
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
                    <td data-label="Symbol">{task.symbol}</td>
                    <td data-label="Instrument">{task.instrument_key}</td>
                    <td data-label="Fetch Start">{task.fetch_start}</td>
                    <td data-label="Fetch End">{task.fetch_end}</td>
                    <td data-label="Missing Rows">{task.missing_rows.toLocaleString()}</td>
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
                  <td data-label="Run">{run.name}</td>
                  <td data-label="Status">
                    <span className={`status-pill ${statusClass(run.status)}`}>{run.status}</span>
                  </td>
                  <td data-label="Started">{formatDateTime(run.started_at)}</td>
                  <td data-label="Items">{run.items_processed.toLocaleString()}</td>
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
                  <td data-label="Symbol">{row.symbol}</td>
                  <td data-label="Status">
                    <span className={`status-pill ${statusClass(row.status)}`}>{row.status}</span>
                  </td>
                  <td data-label="Fetch Start">{row.fetch_start ?? "n/a"}</td>
                  <td data-label="Fetch End">{row.fetch_end}</td>
                  <td data-label="Rows">{row.rows_fetched.toLocaleString()}</td>
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
