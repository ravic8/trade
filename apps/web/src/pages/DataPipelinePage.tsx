import {
  Activity,
  AlertTriangle,
  CheckCircle2,
  Clock3,
  Database,
  DatabaseZap,
  Globe2,
  History,
  Play,
  RefreshCw,
  Search,
  SearchCheck,
  ShieldCheck,
  Sparkles,
  TableProperties,
  X,
} from "lucide-react";
import { FormEvent, useMemo, useState } from "react";
import { useQueryClient } from "@tanstack/react-query";

import {
  useCreateDataPipelineRequest,
  useDataAvailability,
  useDataCoveragePreview,
  useDataInstrumentSearch,
  useDataPipelineHealth,
  usePipelineScheduleStatus,
  useProviderRequestLogs,
  useProviderRequestSummary,
  useProviderRuns,
} from "../api/hooks";
import type {
  DataAvailabilityParams,
  DataAvailabilityResponse,
  DataAvailabilityRow,
  DataCoveragePreviewRequest,
  DataCoveragePreviewResponse,
  DataInstrumentSearchRow,
  DataPipelineRunSummary,
  PipelineScheduleStatusRow,
  ProviderRequestLogRow,
  ProviderRequestSummaryRow,
} from "../api/types";
import { EmptyState, LoadingState } from "../components/DataState";
import { MetricCard } from "../components/MetricCard";
import { PageHeader } from "../components/PageHeader";
import { formatDateTime } from "../utils/format";

type DataTab = "request" | "available" | "runs" | "health";
type MarketId = "nse" | "us" | "ca" | "global";

type MarketOption = {
  id: MarketId;
  label: string;
  detail: string;
  icon: string;
  provider: "upstox" | "yfinance";
  exchange: "NSE" | "US" | "CA" | "GLOBAL";
  interval: "1d" | "5m";
  requestEnabled: boolean;
  defaultSymbols: string[];
  notice: string;
};

const markets: MarketOption[] = [
  {
    id: "nse",
    label: "NSE",
    detail: "Upstox daily",
    icon: "IN",
    provider: "upstox",
    exchange: "NSE",
    interval: "1d",
    requestEnabled: true,
    defaultSymbols: ["RELIANCE", "INFY"],
    notice: "Daily NSE fetch is enabled when the Upstox token is configured.",
  },
  {
    id: "us",
    label: "USA",
    detail: "yfinance daily",
    icon: "US",
    provider: "yfinance",
    exchange: "US",
    interval: "1d",
    requestEnabled: false,
    defaultSymbols: ["AAPL", "MSFT"],
    notice: "US daily availability is visible here; public fetch controls are not exposed yet.",
  },
  {
    id: "ca",
    label: "TSX",
    detail: "Canada yfinance daily",
    icon: "CA",
    provider: "yfinance",
    exchange: "CA",
    interval: "1d",
    requestEnabled: false,
    defaultSymbols: ["SHOP.TO", "RY.TO"],
    notice: "TSX daily availability is visible here; public fetch controls are not exposed yet.",
  },
  {
    id: "global",
    label: "FX/Crypto",
    detail: "yfinance 5m",
    icon: "GL",
    provider: "yfinance",
    exchange: "GLOBAL",
    interval: "5m",
    requestEnabled: false,
    defaultSymbols: ["EUR/USD", "USD/JPY", "BTC/USD"],
    notice: "GLOBAL 5-minute FX/crypto rows are monitored here. Intraday schedule remains stopped until cadence is chosen.",
  },
];

function todayIso(): string {
  return new Date().toISOString().slice(0, 10);
}

function defaultStartIso(): string {
  const value = new Date();
  value.setDate(value.getDate() - 7);
  return value.toISOString().slice(0, 10);
}

function formatNumber(value: number | null | undefined): string {
  return value === null || value === undefined ? "0" : value.toLocaleString();
}

function formatPercent(value: number | null | undefined): string {
  if (value === null || value === undefined) return "0%";
  return `${Math.round(value * 1000) / 10}%`;
}

function statusClass(status: string): string {
  const normalized = status.toLowerCase();
  if (normalized.includes("fail") || normalized.includes("error") || normalized === "empty") {
    return "failed";
  }
  if (normalized.includes("running") || normalized.includes("queued")) return "running";
  if (normalized.includes("warn") || normalized.includes("partial") || normalized.includes("stopped")) {
    return "warning";
  }
  return "completed";
}

function splitSymbols(value: string): string[] {
  return value
    .split(/[,\s]+/)
    .map((item) => item.trim().toUpperCase())
    .filter(Boolean);
}

export function DataPipelinePage() {
  const queryClient = useQueryClient();
  const [activeTab, setActiveTab] = useState<DataTab>("request");
  const [marketId, setMarketId] = useState<MarketId>("nse");
  const [symbolsByMarket, setSymbolsByMarket] = useState<Record<MarketId, string[]>>({
    nse: ["RELIANCE", "INFY"],
    us: ["AAPL", "MSFT"],
    ca: ["SHOP.TO", "RY.TO"],
    global: ["EUR/USD", "USD/JPY", "BTC/USD"],
  });
  const [symbolInput, setSymbolInput] = useState("");
  const [startDate, setStartDate] = useState(defaultStartIso());
  const [endDate, setEndDate] = useState(todayIso());
  const [availabilityQueryText, setAvailabilityQueryText] = useState("");
  const [availabilityStatus, setAvailabilityStatus] =
    useState<DataAvailabilityParams["coverage_status"]>("");
  const [runStatus, setRunStatus] = useState("");
  const market = markets.find((item) => item.id === marketId) ?? markets[0];
  const selectedSymbols = symbolsByMarket[market.id] ?? market.defaultSymbols;
  const availabilityParams = useMemo<DataAvailabilityParams>(
    () => ({
      provider: market.provider,
      exchange: market.exchange,
      interval: market.interval,
      start_date: startDate,
      end_date: endDate,
      query: availabilityQueryText.trim() || undefined,
      coverage_status: availabilityStatus,
      limit: 100,
      sort: market.interval === "5m" ? "-latest_stored_ts" : "-coverage_pct",
    }),
    [availabilityQueryText, availabilityStatus, endDate, market.exchange, market.interval, market.provider, startDate],
  );
  const observabilityParams = useMemo(
    () => ({
      provider: market.provider,
      exchange: market.exchange,
      status: runStatus.trim() || undefined,
      start_date: startDate,
      end_date: endDate,
      limit: 50,
    }),
    [endDate, market.exchange, market.provider, runStatus, startDate],
  );
  const healthQuery = useDataPipelineHealth();
  const availabilityQuery = useDataAvailability(availabilityParams);
  const runsQuery = useProviderRuns(observabilityParams);
  const summaryQuery = useProviderRequestSummary(observabilityParams);
  const logsQuery = useProviderRequestLogs({ ...observabilityParams, limit: 50 });
  const schedulesQuery = usePipelineScheduleStatus();
  const symbolSearchQuery = useDataInstrumentSearch(
    {
      provider: "upstox",
      exchange: "NSE",
      query: symbolInput.trim(),
      limit: 8,
    },
    market.requestEnabled && symbolInput.trim().length > 0,
  );
  const previewMutation = useDataCoveragePreview();
  const createMutation = useCreateDataPipelineRequest();

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
  const tokenConfigured = Boolean(healthQuery.data?.upstox_access_token_configured);
  const requestBlocked =
    !market.requestEnabled ||
    selectedSymbols.length === 0 ||
    !tokenConfigured ||
    !previewMutation.data ||
    createMutation.isPending;

  function selectMarket(nextMarketId: MarketId) {
    const nextMarket = markets.find((item) => item.id === nextMarketId) ?? markets[0];
    setMarketId(nextMarketId);
    setActiveTab(nextMarket.requestEnabled ? "request" : "available");
    setAvailabilityQueryText("");
    setAvailabilityStatus("");
    setRunStatus("");
    setSymbolInput("");
    previewMutation.reset();
    createMutation.reset();
  }

  function updateSelectedSymbols(nextSymbols: string[]) {
    setSymbolsByMarket((current) => ({ ...current, [market.id]: nextSymbols }));
    previewMutation.reset();
    createMutation.reset();
  }

  function addSymbols(symbols: string[]) {
    const additions = splitSymbols(symbols.join(" "));
    if (!additions.length) return;
    updateSelectedSymbols(Array.from(new Set([...selectedSymbols, ...additions])));
    setSymbolInput("");
  }

  function removeSymbol(symbol: string) {
    updateSelectedSymbols(selectedSymbols.filter((item) => item !== symbol));
  }

  function onPreview(event: FormEvent) {
    event.preventDefault();
    if (!market.requestEnabled || selectedSymbols.length === 0) return;
    previewMutation.mutate(requestPayload);
  }

  async function onRun() {
    if (requestBlocked || !market.requestEnabled) return;
    await createMutation.mutateAsync({
      ...requestPayload,
      steps: ["fetch_ohlcv", "validate_ohlcv"],
      mode: "incremental_missing_only",
    });
    await queryClient.invalidateQueries({ queryKey: ["provider-runs"] });
    await queryClient.invalidateQueries({ queryKey: ["data-availability"] });
    await queryClient.invalidateQueries({ queryKey: ["provider-request-summary"] });
    await queryClient.invalidateQueries({ queryKey: ["provider-request-logs"] });
    setRunStatus("");
    setActiveTab("runs");
  }

  function refreshActive() {
    if (activeTab === "available") void availabilityQuery.refetch();
    if (activeTab === "runs") void runsQuery.refetch();
    if (activeTab === "health") {
      void summaryQuery.refetch();
      void logsQuery.refetch();
      void schedulesQuery.refetch();
    }
  }

  return (
    <div className="data-hub">
      <PageHeader
        eyebrow="Data Pipelines"
        title="Data Console"
        subtitle="Pick a market, check coverage, fetch safely, and watch provider health."
        actions={<MarketPill market={market} />}
      />

      <MarketSelector activeMarket={market.id} onChange={selectMarket} />
      <DataTabs activeTab={activeTab} requestEnabled={market.requestEnabled} onChange={setActiveTab} />

      {activeTab === "request" ? (
        <RequestView
          market={market}
          selectedSymbols={selectedSymbols}
          symbolInput={symbolInput}
          symbolSuggestions={symbolSearchQuery.data ?? []}
          isSymbolSearchLoading={symbolSearchQuery.isFetching}
          startDate={startDate}
          endDate={endDate}
          tokenConfigured={tokenConfigured}
          isHealthLoading={healthQuery.isLoading}
          preview={previewMutation.data}
          previewError={previewMutation.error}
          runError={createMutation.error}
          requestBlocked={requestBlocked}
          isPreviewing={previewMutation.isPending}
          isRunning={createMutation.isPending}
          onSymbolInputChange={setSymbolInput}
          onAddSymbols={addSymbols}
          onSelectSymbol={(symbol) => addSymbols([symbol])}
          onRemoveSymbol={removeSymbol}
          onStartDateChange={(value) => {
            setStartDate(value);
            previewMutation.reset();
          }}
          onEndDateChange={(value) => {
            setEndDate(value);
            previewMutation.reset();
          }}
          onPreview={onPreview}
          onRun={() => void onRun()}
        />
      ) : null}

      {activeTab === "available" ? (
        <AvailableView
          market={market}
          availability={availabilityQuery.data ?? null}
          isLoading={availabilityQuery.isLoading}
          isFetching={availabilityQuery.isFetching}
          error={availabilityQuery.error}
          query={availabilityQueryText}
          status={availabilityStatus}
          startDate={startDate}
          endDate={endDate}
          onQueryChange={setAvailabilityQueryText}
          onStatusChange={setAvailabilityStatus}
          onStartDateChange={setStartDate}
          onEndDateChange={setEndDate}
          onRefresh={refreshActive}
        />
      ) : null}

      {activeTab === "runs" ? (
        <RunsView
          runs={runsQuery.data ?? []}
          isLoading={runsQuery.isLoading}
          isFetching={runsQuery.isFetching}
          error={runsQuery.error}
          status={runStatus}
          onStatusChange={setRunStatus}
          onRefresh={refreshActive}
        />
      ) : null}

      {activeTab === "health" ? (
        <HealthView
          market={market}
          summaries={summaryQuery.data ?? []}
          logs={logsQuery.data ?? []}
          schedules={schedulesQuery.data ?? []}
          isLoading={summaryQuery.isLoading || logsQuery.isLoading || schedulesQuery.isLoading}
          error={summaryQuery.error ?? logsQuery.error ?? schedulesQuery.error}
          onRefresh={refreshActive}
        />
      ) : null}
    </div>
  );
}

function MarketPill({ market }: { market: MarketOption }) {
  return (
    <span className="data-market-pill">
      <span aria-hidden="true" />
      {market.icon} {market.provider} / {market.interval}
    </span>
  );
}

function MarketSelector({
  activeMarket,
  onChange,
}: {
  activeMarket: MarketId;
  onChange: (market: MarketId) => void;
}) {
  return (
    <section className="data-market-strip" aria-label="Markets">
      {markets.map((market) => (
        <button
          key={market.id}
          type="button"
          className={activeMarket === market.id ? "active" : ""}
          onClick={() => onChange(market.id)}
        >
          <span>{market.icon}</span>
          <strong>{market.label}</strong>
          <small>{market.detail}</small>
        </button>
      ))}
    </section>
  );
}

function DataTabs({
  activeTab,
  requestEnabled,
  onChange,
}: {
  activeTab: DataTab;
  requestEnabled: boolean;
  onChange: (tab: DataTab) => void;
}) {
  const tabs: { id: DataTab; label: string; icon: typeof Sparkles }[] = [
    { id: "request", label: "Request", icon: Sparkles },
    { id: "available", label: "Available", icon: Database },
    { id: "runs", label: "Runs", icon: History },
    { id: "health", label: "Health", icon: Activity },
  ];
  return (
    <div className="data-mode-tabs" role="tablist" aria-label="Data views">
      {tabs.map((tab) => {
        const Icon = tab.icon;
        const disabled = tab.id === "request" && !requestEnabled;
        return (
          <button
            key={tab.id}
            type="button"
            className={activeTab === tab.id ? "active" : ""}
            disabled={disabled}
            onClick={() => onChange(tab.id)}
          >
            <Icon size={17} />
            <span>{tab.label}</span>
          </button>
        );
      })}
    </div>
  );
}

function RequestView({
  market,
  selectedSymbols,
  symbolInput,
  symbolSuggestions,
  isSymbolSearchLoading,
  startDate,
  endDate,
  tokenConfigured,
  isHealthLoading,
  preview,
  previewError,
  runError,
  requestBlocked,
  isPreviewing,
  isRunning,
  onSymbolInputChange,
  onAddSymbols,
  onSelectSymbol,
  onRemoveSymbol,
  onStartDateChange,
  onEndDateChange,
  onPreview,
  onRun,
}: {
  market: MarketOption;
  selectedSymbols: string[];
  symbolInput: string;
  symbolSuggestions: DataInstrumentSearchRow[];
  isSymbolSearchLoading: boolean;
  startDate: string;
  endDate: string;
  tokenConfigured: boolean;
  isHealthLoading: boolean;
  preview: DataCoveragePreviewResponse | undefined;
  previewError: Error | null;
  runError: Error | null;
  requestBlocked: boolean;
  isPreviewing: boolean;
  isRunning: boolean;
  onSymbolInputChange: (value: string) => void;
  onAddSymbols: (symbols: string[]) => void;
  onSelectSymbol: (symbol: string) => void;
  onRemoveSymbol: (symbol: string) => void;
  onStartDateChange: (value: string) => void;
  onEndDateChange: (value: string) => void;
  onPreview: (event: FormEvent) => void;
  onRun: () => void;
}) {
  return (
    <div className="data-two-column">
      <section className="data-card data-request-card">
        <div className="data-card-header">
          <div>
            <h2>Build Request</h2>
            <p>{market.label} / {market.detail}</p>
          </div>
          <span>Step 1 / 2 / 3</span>
        </div>

        {!market.requestEnabled ? (
          <div className="data-readonly-panel">
            <Globe2 size={20} />
            <div>
              <strong>{market.label} is availability-only here</strong>
              <p>{market.notice}</p>
            </div>
          </div>
        ) : null}

        <form className="data-simple-form" onSubmit={onPreview}>
          <div className="data-step-label">
            <span>1</span>
            <strong>{market.requestEnabled ? "Pick Symbols" : "Symbols In View"}</strong>
          </div>
          <div className="data-symbol-box">
            {selectedSymbols.map((symbol) => (
              <span key={symbol} className="data-symbol-chip">
                {symbol}
                <button type="button" aria-label={`Remove ${symbol}`} onClick={() => onRemoveSymbol(symbol)}>
                  <X size={14} />
                </button>
              </span>
            ))}
            <input
              value={symbolInput}
              onChange={(event) => onSymbolInputChange(event.target.value)}
              onKeyDown={(event) => {
                if (event.key === "Enter" || event.key === ",") {
                  event.preventDefault();
                  onAddSymbols([symbolSuggestions[0]?.symbol ?? symbolInput]);
                }
              }}
              placeholder={market.requestEnabled ? "Add symbol" : "Symbols shown for context"}
            />
            <button
              type="button"
              className="data-inline-action"
              onClick={() => onAddSymbols([symbolSuggestions[0]?.symbol ?? symbolInput])}
            >
              Add
            </button>
          </div>
          {market.requestEnabled && symbolInput.trim() ? (
            <SymbolSuggestions
              suggestions={symbolSuggestions}
              selectedSymbols={selectedSymbols}
              isLoading={isSymbolSearchLoading}
              onSelect={onSelectSymbol}
            />
          ) : null}

          <div className="data-step-label">
            <span>2</span>
            <strong>{market.requestEnabled ? "Select Window" : "Window In View"}</strong>
          </div>
          <div className="data-date-grid">
            <label>
              Start
              <input type="date" value={startDate} onChange={(event) => onStartDateChange(event.target.value)} />
            </label>
            <label>
              End
              <input type="date" value={endDate} onChange={(event) => onEndDateChange(event.target.value)} />
            </label>
          </div>

          {market.requestEnabled ? (
            <>
              <div className="data-step-label">
                <span>3</span>
                <strong>Preview Missing Windows</strong>
              </div>
              <div className="data-action-row">
                <button className="icon-button" type="submit" disabled={isPreviewing}>
                  <SearchCheck size={17} />
                  <span>{isPreviewing ? "Previewing" : "Preview Coverage"}</span>
                </button>
                <button className="icon-button primary" type="button" disabled={requestBlocked} onClick={onRun}>
                  <Play size={17} />
                  <span>{isRunning ? "Fetching" : `Fetch ${selectedSymbols.length}`}</span>
                </button>
              </div>

              {preview ? <PreviewResult preview={preview} /> : null}
            </>
          ) : null}
          {previewError ? <p className="form-error">{previewError.message}</p> : null}
          {runError ? <p className="form-error">{runError.message}</p> : null}
        </form>
      </section>

      <aside className="data-side-stack">
        <ReadinessCard
          market={market}
          selectedCount={selectedSymbols.length}
          tokenConfigured={tokenConfigured}
          isLoading={isHealthLoading}
          hasPreview={Boolean(preview)}
        />
        <section className="data-card">
          <h2>Notice</h2>
          <p className="data-muted">{market.notice}</p>
        </section>
      </aside>
    </div>
  );
}

function SymbolSuggestions({
  suggestions,
  selectedSymbols,
  isLoading,
  onSelect,
}: {
  suggestions: DataInstrumentSearchRow[];
  selectedSymbols: string[];
  isLoading: boolean;
  onSelect: (symbol: string) => void;
}) {
  const selected = new Set(selectedSymbols.map((symbol) => symbol.toUpperCase()));
  if (isLoading) return <p className="data-symbol-helper">Searching symbols...</p>;
  if (!suggestions.length) return <p className="data-symbol-helper">No matching symbols found.</p>;
  return (
    <div className="data-symbol-suggestions" aria-label="Symbol suggestions">
      {suggestions.map((row) => {
        const isSelected = selected.has(row.symbol.toUpperCase());
        return (
          <button
            key={row.instrument_key}
            type="button"
            disabled={isSelected}
            onClick={() => onSelect(row.symbol)}
          >
            <span>
              <strong>{row.symbol}</strong>
              {row.name ? <small>{row.name}</small> : null}
            </span>
            <small>{isSelected ? "Selected" : row.exchange}</small>
          </button>
        );
      })}
    </div>
  );
}

function PreviewResult({ preview }: { preview: DataCoveragePreviewResponse }) {
  const visibleTasks = preview.tasks.slice(0, 8);
  return (
    <div className="data-preview-panel">
      <div className="data-preview-result">
        <strong>{formatNumber(preview.missing_rows)} missing rows</strong>
        <span>{formatNumber(preview.estimated_provider_calls)} provider calls queued</span>
      </div>
      {preview.tasks.length > 0 ? (
        <div className="data-preview-task-list" aria-label="Missing windows preview">
          {visibleTasks.map((task) => (
            <div key={`${task.instrument_key}-${task.fetch_start}-${task.fetch_end}`} className="data-preview-task">
              <div>
                <strong>{task.symbol}</strong>
                <span>{task.trading_symbol}</span>
              </div>
              <div>
                <span>{formatDisplayDate(task.fetch_start)} to {formatDisplayDate(task.fetch_end)}</span>
                <strong>{formatNumber(task.missing_rows)} rows</strong>
              </div>
            </div>
          ))}
          {preview.tasks.length > visibleTasks.length ? (
            <p className="data-muted">Showing {visibleTasks.length} of {preview.tasks.length} missing windows.</p>
          ) : null}
        </div>
      ) : (
        <p className="data-preview-empty">No missing windows in this selection.</p>
      )}
      {preview.warnings.length > 0 ? (
        <div className="data-preview-warnings">
          {preview.warnings.map((warning) => (
            <p key={warning}>{warning}</p>
          ))}
        </div>
      ) : null}
    </div>
  );
}

function ReadinessCard({
  market,
  selectedCount,
  tokenConfigured,
  isLoading,
  hasPreview,
}: {
  market: MarketOption;
  selectedCount: number;
  tokenConfigured: boolean;
  isLoading: boolean;
  hasPreview: boolean;
}) {
  const rows = market.requestEnabled
    ? [
        { label: "Market selected", ok: true },
        { label: "Upstox token configured", ok: tokenConfigured },
        { label: `${selectedCount} symbols selected`, ok: selectedCount > 0 },
        { label: "Missing-window preview ready", ok: hasPreview },
      ]
    : [
        { label: "Market selected", ok: true },
        { label: "Availability API enabled", ok: true },
        { label: "Fetch controls read-only", ok: true },
      ];
  return (
    <section className="data-card">
      <h2>Readiness</h2>
      {isLoading ? <LoadingState /> : null}
      <div className="data-check-list">
        {rows.map((row) => (
          <div key={row.label}>
            {row.ok ? <CheckCircle2 size={17} /> : <AlertTriangle size={17} />}
            <span>{row.label}</span>
          </div>
        ))}
      </div>
    </section>
  );
}

function AvailableView({
  market,
  availability,
  isLoading,
  isFetching,
  error,
  query,
  status,
  startDate,
  endDate,
  onQueryChange,
  onStatusChange,
  onStartDateChange,
  onEndDateChange,
  onRefresh,
}: {
  market: MarketOption;
  availability: DataAvailabilityResponse | null;
  isLoading: boolean;
  isFetching: boolean;
  error: Error | null;
  query: string;
  status: DataAvailabilityParams["coverage_status"];
  startDate: string;
  endDate: string;
  onQueryChange: (value: string) => void;
  onStatusChange: (value: DataAvailabilityParams["coverage_status"]) => void;
  onStartDateChange: (value: string) => void;
  onEndDateChange: (value: string) => void;
  onRefresh: () => void;
}) {
  const summary = availability?.summary;
  const coverage =
    summary && summary.expected_rows > 0 ? summary.stored_rows / summary.expected_rows : 0;
  return (
    <>
      <div className="metric-grid data-metric-grid">
        <MetricCard icon={TableProperties} label="Symbols" value={formatNumber(summary?.symbols_total)} detail={`${formatNumber(summary?.symbols_complete)} complete`} />
        <MetricCard icon={DatabaseZap} label="Stored Rows" value={formatNumber(summary?.stored_rows)} detail={`${formatNumber(summary?.expected_rows)} expected`} />
        <MetricCard icon={SearchCheck} label="Missing" value={formatNumber(summary?.missing_rows)} detail="Fetch to fill" />
        <MetricCard icon={ShieldCheck} label="Coverage" value={formatPercent(coverage)} detail={`${market.exchange} / ${market.interval}`} />
      </div>

      <section className="data-card">
        <div className="data-card-header">
          <div>
            <h2>Available Data</h2>
            <p>{market.label} / {market.detail}</p>
          </div>
          <button className="icon-button" type="button" onClick={onRefresh}>
            <RefreshCw size={16} />
            <span>{isFetching && !isLoading ? "Refreshing" : "Refresh"}</span>
          </button>
        </div>
        <div className="data-filter-row">
          <label className="data-search-field">
            <Search size={16} />
            <input value={query} onChange={(event) => onQueryChange(event.target.value)} placeholder={`Search ${market.label} symbol or name`} />
          </label>
          <label>
            Status
            <select value={status ?? ""} onChange={(event) => onStatusChange(event.target.value as DataAvailabilityParams["coverage_status"])}>
              <option value="">All</option>
              <option value="complete">Complete</option>
              <option value="partial">Partial</option>
              <option value="empty">Empty</option>
            </select>
          </label>
          <label>
            Start
            <input type="date" value={startDate} onChange={(event) => onStartDateChange(event.target.value)} />
          </label>
          <label>
            End
            <input type="date" value={endDate} onChange={(event) => onEndDateChange(event.target.value)} />
          </label>
        </div>
        {isLoading ? <LoadingState /> : null}
        {error ? <p className="form-error">Availability could not load: {error.message}</p> : null}
        {!isLoading && !error && availability?.rows.length === 0 ? (
          <EmptyState label="No stored data matched this market and date range." />
        ) : null}
        {availability?.rows.length ? <AvailabilityList rows={availability.rows} /> : null}
      </section>
    </>
  );
}

function AvailabilityList({ rows }: { rows: DataAvailabilityRow[] }) {
  return (
    <div className="data-list">
      {rows.map((row) => {
        const latest = row.interval === "5m" ? row.latest_stored_ts : row.latest_stored_date;
        return (
          <article key={row.instrument_key} className="data-list-row">
            <div>
              <strong>{row.symbol}</strong>
              <span>{row.name ?? row.instrument_key}</span>
            </div>
            <span className={`status-pill ${statusClass(row.coverage_status)}`}>{row.coverage_status}</span>
            <div className="data-progress-cell">
              <span style={{ width: `${Math.round(row.coverage_pct * 100)}%` }} />
            </div>
            <strong>{formatPercent(row.coverage_pct)}</strong>
            <small>{formatNumber(row.stored_rows)} rows / latest {latest ? formatDateTime(latest) : "never"}</small>
          </article>
        );
      })}
    </div>
  );
}

function RunsView({
  runs,
  isLoading,
  isFetching,
  error,
  status,
  onStatusChange,
  onRefresh,
}: {
  runs: DataPipelineRunSummary[];
  isLoading: boolean;
  isFetching: boolean;
  error: Error | null;
  status: string;
  onStatusChange: (value: string) => void;
  onRefresh: () => void;
}) {
  const succeeded = runs.filter((run) => statusClass(run.status) === "completed").length;
  const failed = runs.filter((run) => statusClass(run.status) === "failed").length;
  const rowsPulled = runs.reduce((total, run) => total + run.items_succeeded, 0);
  return (
    <>
      <div className="metric-grid data-metric-grid">
        <MetricCard icon={History} label="Runs" value={formatNumber(runs.length)} detail={`${formatNumber(failed)} failed`} />
        <MetricCard icon={Database} label="Rows Pulled" value={formatNumber(rowsPulled)} detail="Across visible runs" />
        <MetricCard icon={ShieldCheck} label="Success" value={formatPercent(runs.length ? succeeded / runs.length : 0)} detail="Visible window" />
        <MetricCard icon={Clock3} label="Avg Time" value={`${formatNumber(averageDuration(runs))}s`} detail="Per completed run" />
      </div>
      <section className="data-card">
        <div className="data-card-header">
          <div>
            <h2>Run History</h2>
            <p>{runs.length.toLocaleString()} visible runs</p>
          </div>
          <button className="icon-button" type="button" onClick={onRefresh}>
            <RefreshCw size={16} />
            <span>{isFetching && !isLoading ? "Refreshing" : "Refresh"}</span>
          </button>
        </div>
        <div className="data-pill-row">
          {["", "success", "completed", "failed", "running"].map((item) => (
            <button
              key={item || "all"}
              type="button"
              className={status === item ? "active" : ""}
              onClick={() => onStatusChange(item)}
            >
              {item || "All"}
            </button>
          ))}
        </div>
        {isLoading ? <LoadingState /> : null}
        {error ? <p className="form-error">Runs could not load: {error.message}</p> : null}
        {!isLoading && !error && runs.length === 0 ? <EmptyState label="No runs found for this market." /> : null}
        {runs.length ? (
          <div className="data-list">
            {runs.map((run) => (
              <article key={run.id} className="data-list-row run-row">
                <div>
                  <strong>{run.name}</strong>
                  <span>
                    {run.source} / {run.exchange} / {formatDateTime(run.started_at)}
                  </span>
                </div>
                <span className={`status-pill ${statusClass(run.status)}`}>{run.status}</span>
                <small>{formatNumber(run.items_succeeded)} rows</small>
                <small>{run.duration_seconds === null ? "running" : `${run.duration_seconds}s`}</small>
              </article>
            ))}
          </div>
        ) : null}
      </section>
    </>
  );
}

function HealthView({
  market,
  summaries,
  logs,
  schedules,
  isLoading,
  error,
  onRefresh,
}: {
  market: MarketOption;
  summaries: ProviderRequestSummaryRow[];
  logs: ProviderRequestLogRow[];
  schedules: PipelineScheduleStatusRow[];
  isLoading: boolean;
  error: Error | null;
  onRefresh: () => void;
}) {
  const requests = summaries.reduce((total, row) => total + row.requests, 0);
  const rateLimited = summaries.reduce((total, row) => total + row.rate_limited_requests, 0);
  const avgMs = summaries.length
    ? summaries.reduce((total, row) => total + row.avg_duration_ms, 0) / summaries.length
    : 0;
  return (
    <>
      <div className="metric-grid data-metric-grid">
        <MetricCard icon={ShieldCheck} label="Uptime" value="Read-only" detail="Dagster private" />
        <MetricCard icon={Activity} label="Requests" value={formatNumber(requests)} detail="Selected window" />
        <MetricCard icon={AlertTriangle} label="Rate Limited" value={formatNumber(rateLimited)} detail="Provider waits" />
        <MetricCard icon={Clock3} label="Avg Duration" value={`${Math.round(avgMs)} ms`} detail="Summary buckets" />
      </div>
      <div className="data-two-column">
        <section className="data-card">
          <div className="data-card-header">
            <div>
              <h2>Provider Health</h2>
              <p>{market.provider} / {market.exchange}</p>
            </div>
            <button className="icon-button" type="button" onClick={onRefresh}>
              <RefreshCw size={16} />
              <span>Refresh</span>
            </button>
          </div>
          {isLoading ? <LoadingState /> : null}
          {error ? <p className="form-error">Health could not load: {error.message}</p> : null}
          <div className="data-list">
            {summaries.map((row) => (
              <article key={`${row.provider}-${row.endpoint_group}-${row.status}`} className="data-list-row">
                <div>
                  <strong>{row.endpoint_group}</strong>
                  <span>{row.provider}</span>
                </div>
                <span className={`status-pill ${statusClass(row.status)}`}>{row.status}</span>
                <small>{formatNumber(row.requests)} requests</small>
                <small>{Math.round(row.avg_duration_ms)} ms avg</small>
              </article>
            ))}
            {!isLoading && summaries.length === 0 ? <EmptyState label="No provider requests matched this market." /> : null}
          </div>
        </section>
        <section className="data-card">
          <h2>Schedules</h2>
          <div className="data-list compact-list">
            {schedules.map((schedule) => (
              <article key={schedule.schedule_name} className="data-list-row">
                <div>
                  <strong>{schedule.schedule_name}</strong>
                  <span>{schedule.job_name}</span>
                </div>
                <span className={`status-pill ${statusClass(schedule.intended_status)}`}>
                  {schedule.intended_status}
                </span>
              </article>
            ))}
          </div>
        </section>
      </div>
      <section className="data-card">
        <h2>Recent Request Logs</h2>
        <div className="data-list compact-list">
          {logs.map((log) => (
            <article key={log.id} className="data-list-row">
              <div>
                <strong>{log.symbol ?? log.request_key}</strong>
                <span>{log.endpoint_group} / {formatDateTime(log.created_at)}</span>
              </div>
              <span className={`status-pill ${statusClass(log.status)}`}>{log.status}</span>
              <small>{log.wait_seconds.toFixed(2)}s wait</small>
              <small>{Math.round(log.duration_ms)} ms</small>
            </article>
          ))}
          {!isLoading && logs.length === 0 ? <EmptyState label="No recent request logs for this market." /> : null}
        </div>
      </section>
    </>
  );
}

function averageDuration(runs: DataPipelineRunSummary[]): number {
  const durations = runs
    .map((run) => run.duration_seconds)
    .filter((value): value is number => typeof value === "number");
  if (!durations.length) return 0;
  return Math.round(durations.reduce((total, value) => total + value, 0) / durations.length);
}

function formatDisplayDate(value: string): string {
  const parsed = new Date(value);
  if (Number.isNaN(parsed.getTime())) return value;
  return parsed.toLocaleDateString("en-GB", { day: "2-digit", month: "short", year: "numeric" });
}
