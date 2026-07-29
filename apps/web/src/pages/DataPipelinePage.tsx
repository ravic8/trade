import {
  Activity,
  AlertTriangle,
  ArrowLeft,
  ArrowRight,
  CalendarClock,
  CheckCircle2,
  CircleGauge,
  Clock3,
  Database,
  DatabaseZap,
  GitBranch,
  History,
  ListChecks,
  RefreshCw,
  Search,
  ShieldCheck,
  TableProperties,
  Users,
  Workflow,
  X,
  Zap,
} from "lucide-react";
import { useEffect, useMemo, useRef, useState, type KeyboardEvent } from "react";
import { useQueryClient } from "@tanstack/react-query";

import {
  useBigQuerySyncOverview,
  useDataAvailability,
  useDataInstrumentSearch,
  useDataPipelineRunDetail,
  useOperationsLifecycleEvents,
  useOperationsOverview,
  useOperationsRateLimits,
  useOperationsWorkItems,
  usePipelineScheduleStatus,
  useProviderRuns,
} from "../api/hooks";
import type {
  BigQuerySyncOverviewResponse,
  DataAvailabilityParams,
  DataAvailabilityResponse,
  DataAvailabilityRow,
  DataInstrumentSearchRow,
  DataPipelineRunExchangeResult,
  DataPipelineRunDetail,
  DataPipelineRunSummary,
  OperationsAdaptiveRateStateRow,
  OperationsExchange,
  OperationsFreshnessRow,
  OperationsLifecycleEventRow,
  OperationsOverviewResponse,
  OperationsQueueGroup,
  OperationsUniverseSnapshotRow,
  OperationsWorkItemRow,
  PipelineScheduleStatusRow,
} from "../api/types";
import { EmptyState, LoadingState } from "../components/DataState";
import { MetricCard } from "../components/MetricCard";
import { PageHeader } from "../components/PageHeader";
import { formatDateTime } from "../utils/format";

type DataTab = "overview" | "coverage" | "work" | "runs" | "lifecycle" | "warehouse";

type MarketOption = {
  exchange: OperationsExchange;
  label: string;
  region: string;
  icon: string;
  detail: string;
};

const markets: MarketOption[] = [
  {
    exchange: "NSE",
    label: "NSE",
    region: "India",
    icon: "IN",
    detail: "2,000+ active equities",
  },
  {
    exchange: "TSX",
    label: "TSX",
    region: "Canada",
    icon: "CA",
    detail: "Official TMX universe",
  },
  {
    exchange: "US",
    label: "USA",
    region: "United States",
    icon: "US",
    detail: "Nasdaq Trader universe",
  },
];

const pageSize = 50;

function useDebouncedValue<T>(value: T, delayMs = 200): T {
  const [debounced, setDebounced] = useState(value);
  useEffect(() => {
    const timeout = window.setTimeout(() => setDebounced(value), delayMs);
    return () => window.clearTimeout(timeout);
  }, [delayMs, value]);
  return debounced;
}

function todayIso(): string {
  return new Date().toISOString().slice(0, 10);
}

function yearsAgoIso(years: number): string {
  const value = new Date();
  value.setUTCFullYear(value.getUTCFullYear() - years);
  return value.toISOString().slice(0, 10);
}

function daysAgoIso(days: number): string {
  const value = new Date();
  value.setUTCDate(value.getUTCDate() - days);
  return value.toISOString().slice(0, 10);
}

function formatNumber(value: number | null | undefined): string {
  return value === null || value === undefined ? "—" : value.toLocaleString();
}

function formatPercent(value: number | null | undefined): string {
  if (value === null || value === undefined) return "—";
  return `${(value * 100).toFixed(value > 0 && value < 0.01 ? 2 : 1)}%`;
}

function formatDate(value: string | null | undefined): string {
  if (!value) return "Never";
  const parsed = new Date(`${value.slice(0, 10)}T00:00:00Z`);
  if (Number.isNaN(parsed.getTime())) return value;
  return parsed.toLocaleDateString(undefined, {
    day: "2-digit",
    month: "short",
    year: "numeric",
    timeZone: "UTC",
  });
}

function statusClass(status: string): string {
  const normalized = status.toLowerCase();
  if (
    normalized.includes("fail") ||
    normalized.includes("error") ||
    normalized === "terminal"
  ) {
    return "failed";
  }
  if (normalized.includes("running") || normalized === "queued") return "running";
  if (
    normalized.includes("warn") ||
    normalized.includes("partial") ||
    normalized.includes("inactive") ||
    normalized === "retry_wait" ||
    normalized === "open" ||
    normalized === "half_open"
  ) {
    return "warning";
  }
  if (
    normalized === "cancelled" ||
    normalized === "stopped" ||
    normalized === "shared_run"
  ) return "neutral";
  return "completed";
}

function humanize(value: string): string {
  return value
    .replaceAll("_", " ")
    .replace(/\b\w/g, (letter) => letter.toUpperCase());
}

function lifecycleEventLabel(eventType: string): string {
  return eventType === "added" ? "First Observed" : humanize(eventType);
}

function runResultForExchange(
  run: DataPipelineRunSummary,
  exchange: OperationsExchange,
): DataPipelineRunExchangeResult | null {
  return run.exchange_results.find((result) => result.exchange === exchange) ?? null;
}

function runStatusForExchange(
  run: DataPipelineRunSummary,
  exchange: OperationsExchange,
): string {
  const result = runResultForExchange(run, exchange);
  if (result) return result.items_failed > 0 ? "completed_with_failures" : "completed";
  if (run.exchange === exchange) return run.status;
  return run.exchange === "MULTI" ? "shared_run" : run.status;
}

export function DataPipelinePage() {
  const queryClient = useQueryClient();
  const [activeTab, setActiveTab] = useState<DataTab>("overview");
  const [exchange, setExchange] = useState<OperationsExchange>("NSE");
  const [coverageStart, setCoverageStart] = useState(yearsAgoIso(10));
  const [coverageEnd, setCoverageEnd] = useState(todayIso());
  const [coverageQuery, setCoverageQuery] = useState("");
  const [coverageStatus, setCoverageStatus] =
    useState<DataAvailabilityParams["coverage_status"]>("");
  const [workStatus, setWorkStatus] = useState("");
  const [workType, setWorkType] = useState("");
  const [workSymbol, setWorkSymbol] = useState("");
  const [workOffset, setWorkOffset] = useState(0);
  const [runStatus, setRunStatus] = useState("");
  const [runStart, setRunStart] = useState(daysAgoIso(30));
  const [runEnd, setRunEnd] = useState(todayIso());
  const [selectedRunId, setSelectedRunId] = useState<string | null>(null);
  const [lifecycleType, setLifecycleType] = useState("");
  const [lifecycleSymbol, setLifecycleSymbol] = useState("");
  const [lifecycleOffset, setLifecycleOffset] = useState(0);
  const debouncedCoverageQuery = useDebouncedValue(coverageQuery);
  const debouncedWorkSymbol = useDebouncedValue(workSymbol);
  const debouncedLifecycleSymbol = useDebouncedValue(lifecycleSymbol);

  const market = markets.find((item) => item.exchange === exchange) ?? markets[0];
  const overviewQuery = useOperationsOverview(exchange);
  const rateQuery = useOperationsRateLimits();
  const schedulesQuery = usePipelineScheduleStatus(activeTab === "overview");
  const bigQuerySyncQuery = useBigQuerySyncOverview(activeTab === "warehouse");
  const availabilityParams = useMemo<DataAvailabilityParams>(
    () => ({
      provider: "yfinance",
      exchange,
      interval: "1d",
      start_date: coverageStart,
      end_date: coverageEnd,
      query: debouncedCoverageQuery.trim() || undefined,
      coverage_status: coverageStatus,
      limit: 100,
      sort: "-coverage_pct",
    }),
    [coverageEnd, coverageStart, coverageStatus, debouncedCoverageQuery, exchange],
  );
  const availabilityQuery = useDataAvailability(
    availabilityParams,
    activeTab === "coverage",
  );
  const workItemsQuery = useOperationsWorkItems(
    {
      provider: "yfinance",
      exchange,
      status: workStatus || undefined,
      work_type: workType || undefined,
      symbol: debouncedWorkSymbol.trim() || undefined,
      limit: pageSize,
      offset: workOffset,
    },
    activeTab === "work",
  );
  const runsQuery = useProviderRuns(
    {
      provider: "yfinance",
      exchange,
      status: runStatus || undefined,
      start_date: runStart,
      end_date: runEnd,
      limit: 50,
    },
    activeTab === "runs",
  );
  const lifecycleQuery = useOperationsLifecycleEvents(
    {
      exchange,
      event_type: lifecycleType || undefined,
      symbol: debouncedLifecycleSymbol.trim() || undefined,
      limit: pageSize,
      offset: lifecycleOffset,
    },
    activeTab === "lifecycle",
  );
  const runDetailQuery = useDataPipelineRunDetail(selectedRunId, exchange);

  const overview = overviewQuery.data;
  const rate = rateQuery.data?.[0] ?? overview?.adaptive_rates[0];
  const openWork = sumQueue(overview?.queue ?? [], ["queued", "running", "retry_wait"]);
  const retryWork = sumQueue(overview?.queue ?? [], ["retry_wait"]);
  const recentFailures = (overview?.recent_runs ?? []).filter(
    (run) => statusClass(runStatusForExchange(run, exchange)) === "failed",
  ).length;
  const terminalWork = sumQueue(overview?.queue ?? [], ["terminal", "failed"]);
  const isHealthy =
    !overviewQuery.error &&
    rate?.circuit_state === "closed" &&
    openWork === 0 &&
    terminalWork === 0;

  function selectExchange(nextExchange: OperationsExchange) {
    setExchange(nextExchange);
    setSelectedRunId(null);
    setWorkOffset(0);
    setLifecycleOffset(0);
  }

  function refreshConsole() {
    void queryClient.invalidateQueries({ queryKey: ["data-operations-overview"] });
    void queryClient.invalidateQueries({ queryKey: ["data-operations-rate-limits"] });
    void queryClient.invalidateQueries({ queryKey: ["data-operations-work-items"] });
    void queryClient.invalidateQueries({ queryKey: ["data-operations-lifecycle"] });
    void queryClient.invalidateQueries({ queryKey: ["provider-runs"] });
    void queryClient.invalidateQueries({ queryKey: ["data-availability"] });
    void queryClient.invalidateQueries({ queryKey: ["pipeline-schedule-status"] });
    void queryClient.invalidateQueries({ queryKey: ["data-operations-bigquery-sync"] });
  }

  return (
    <div className="data-hub operations-console">
      <PageHeader
        eyebrow="Pipeline Operations"
        title="Data Console"
        subtitle="Coverage, freshness, durable work, and provider health for every equity universe."
        actions={
          <div className="operations-header-actions">
            <span className={`operations-live-pill ${isHealthy ? "healthy" : "attention"}`}>
              <span aria-hidden="true" />
              {overviewQuery.isLoading
                ? "Checking"
                : isHealthy
                  ? "Systems healthy"
                  : "Needs attention"}
            </span>
            <button className="icon-button" type="button" onClick={refreshConsole}>
              <RefreshCw size={16} />
              Refresh
            </button>
          </div>
        }
      />

      <MarketSelector exchange={exchange} onChange={selectExchange} />
      <DataTabs activeTab={activeTab} onChange={setActiveTab} />

      {overviewQuery.error ? (
        <section className="operations-alert failed">
          <AlertTriangle size={20} />
          <div>
            <strong>Operations data is unavailable</strong>
            <span>{overviewQuery.error.message}</span>
          </div>
        </section>
      ) : null}

      {activeTab === "overview" ? (
        <OverviewView
          market={market}
          overview={overview}
          rate={rate}
          schedules={schedulesQuery.data ?? []}
          isLoading={overviewQuery.isLoading || rateQuery.isLoading}
          openWork={openWork}
          retryWork={retryWork}
          recentFailures={recentFailures}
          onSelectTab={setActiveTab}
        />
      ) : null}

      {activeTab === "coverage" ? (
        <CoverageView
          market={market}
          availability={availabilityQuery.data ?? null}
          isLoading={availabilityQuery.isLoading}
          isFetching={availabilityQuery.isFetching}
          error={availabilityQuery.error}
          query={coverageQuery}
          status={coverageStatus}
          startDate={coverageStart}
          endDate={coverageEnd}
          onQueryChange={setCoverageQuery}
          onStatusChange={setCoverageStatus}
          onStartDateChange={setCoverageStart}
          onEndDateChange={setCoverageEnd}
          onRefresh={() => void availabilityQuery.refetch()}
        />
      ) : null}

      {activeTab === "work" ? (
        <WorkQueueView
          exchange={exchange}
          groups={overview?.queue ?? []}
          rows={workItemsQuery.data?.rows ?? []}
          total={workItemsQuery.data?.total ?? 0}
          offset={workOffset}
          isLoading={workItemsQuery.isLoading}
          isFetching={workItemsQuery.isFetching}
          error={workItemsQuery.error}
          status={workStatus}
          workType={workType}
          symbol={workSymbol}
          onStatusChange={(value) => {
            setWorkStatus(value);
            setWorkOffset(0);
          }}
          onWorkTypeChange={(value) => {
            setWorkType(value);
            setWorkOffset(0);
          }}
          onSymbolChange={(value) => {
            setWorkSymbol(value);
            setWorkOffset(0);
          }}
          onOffsetChange={setWorkOffset}
          onRefresh={() => void workItemsQuery.refetch()}
        />
      ) : null}

      {activeTab === "runs" ? (
        <RunsView
          exchange={exchange}
          runs={runsQuery.data ?? []}
          isLoading={runsQuery.isLoading}
          isFetching={runsQuery.isFetching}
          error={runsQuery.error}
          status={runStatus}
          startDate={runStart}
          endDate={runEnd}
          onStatusChange={setRunStatus}
          onStartDateChange={setRunStart}
          onEndDateChange={setRunEnd}
          onRefresh={() => void runsQuery.refetch()}
          onSelectRun={setSelectedRunId}
        />
      ) : null}

      {selectedRunId ? (
        <RunDetailDrawer
          exchange={exchange}
          detail={runDetailQuery.data ?? null}
          isLoading={runDetailQuery.isLoading}
          error={runDetailQuery.error}
          onClose={() => setSelectedRunId(null)}
        />
      ) : null}

      {activeTab === "lifecycle" ? (
        <LifecycleView
          exchange={exchange}
          rows={lifecycleQuery.data?.rows ?? []}
          total={lifecycleQuery.data?.total ?? 0}
          offset={lifecycleOffset}
          isLoading={lifecycleQuery.isLoading}
          isFetching={lifecycleQuery.isFetching}
          error={lifecycleQuery.error}
          eventType={lifecycleType}
          symbol={lifecycleSymbol}
          onEventTypeChange={(value) => {
            setLifecycleType(value);
            setLifecycleOffset(0);
          }}
          onSymbolChange={(value) => {
            setLifecycleSymbol(value);
            setLifecycleOffset(0);
          }}
          onOffsetChange={setLifecycleOffset}
          onRefresh={() => void lifecycleQuery.refetch()}
        />
      ) : null}

      {activeTab === "warehouse" ? (
        <WarehouseView
          exchange={exchange}
          overview={bigQuerySyncQuery.data ?? null}
          isLoading={bigQuerySyncQuery.isLoading}
          error={bigQuerySyncQuery.error}
          onRefresh={() => void bigQuerySyncQuery.refetch()}
        />
      ) : null}
    </div>
  );
}

function MarketSelector({
  exchange,
  onChange,
}: {
  exchange: OperationsExchange;
  onChange: (exchange: OperationsExchange) => void;
}) {
  return (
    <section className="data-market-strip operations-market-strip" aria-label="Equity universes">
      {markets.map((market) => (
        <button
          key={market.exchange}
          type="button"
          className={exchange === market.exchange ? "active" : ""}
          onClick={() => onChange(market.exchange)}
        >
          <span>{market.icon}</span>
          <strong>{market.label}</strong>
          <small>{market.region} · {market.detail}</small>
        </button>
      ))}
    </section>
  );
}

function DataTabs({
  activeTab,
  onChange,
}: {
  activeTab: DataTab;
  onChange: (tab: DataTab) => void;
}) {
  const tabs = [
    { id: "overview" as const, label: "Overview", icon: Activity },
    { id: "coverage" as const, label: "Coverage", icon: Database },
    { id: "work" as const, label: "Work Queue", icon: ListChecks },
    { id: "runs" as const, label: "Runs", icon: History },
    { id: "lifecycle" as const, label: "Lifecycle", icon: GitBranch },
    { id: "warehouse" as const, label: "Warehouse", icon: DatabaseZap },
  ];
  return (
    <div className="data-mode-tabs operations-tabs" role="tablist" aria-label="Data Console views">
      {tabs.map((tab) => {
        const Icon = tab.icon;
        return (
          <button
            key={tab.id}
            type="button"
            role="tab"
            aria-selected={activeTab === tab.id}
            className={activeTab === tab.id ? "active" : ""}
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

function OverviewView({
  market,
  overview,
  rate,
  schedules,
  isLoading,
  openWork,
  retryWork,
  recentFailures,
  onSelectTab,
}: {
  market: MarketOption;
  overview: OperationsOverviewResponse | undefined;
  rate: OperationsAdaptiveRateStateRow | undefined;
  schedules: PipelineScheduleStatusRow[];
  isLoading: boolean;
  openWork: number;
  retryWork: number;
  recentFailures: number;
  onSelectTab: (tab: DataTab) => void;
}) {
  const freshness = overview?.freshness[0];
  const universe = overview?.latest_universes[0];
  if (isLoading && !overview) return <LoadingState />;
  return (
    <>
      <div className="metric-grid data-metric-grid">
        <MetricCard
          icon={Users}
          label="Active Universe"
          value={formatNumber(universe?.symbol_count)}
          detail={`${market.label} latest accepted snapshot`}
        />
        <MetricCard
          icon={DatabaseZap}
          label="Stored Candles"
          value={formatNumber(freshness?.rows)}
          detail={`${formatNumber(freshness?.symbols)} symbols with data`}
        />
        <MetricCard
          icon={CalendarClock}
          label="Latest Session"
          value={freshness?.latest_date ? formatDate(freshness.latest_date) : "Never"}
          detail={freshness?.latest_fetched_at ? `Fetched ${formatDateTime(freshness.latest_fetched_at)}` : "No fetch recorded"}
        />
        <MetricCard
          icon={Workflow}
          label="Open Work"
          value={formatNumber(openWork)}
          detail={`${formatNumber(retryWork)} waiting to retry`}
        />
      </div>

      <section className={`operations-alert ${recentFailures || freshness?.suspicious_rows ? "warning" : "healthy"}`}>
        {recentFailures || freshness?.suspicious_rows ? <AlertTriangle size={21} /> : <CheckCircle2 size={21} />}
        <div>
          <strong>
            {recentFailures || freshness?.suspicious_rows
              ? "Review the highlighted operational signals"
              : `${market.label} pipeline is current and operating normally`}
          </strong>
          <span>
            {formatNumber(freshness?.suspicious_rows)} suspicious candles · {formatNumber(recentFailures)} recent failed runs · read-only console
          </span>
        </div>
      </section>

      <div className="operations-overview-grid">
        <FreshnessCard freshness={freshness} universe={universe} />
        <RateLimitCard rate={rate} />
      </div>

      <div className="operations-overview-grid wide-left">
        <QueueSnapshot groups={overview?.queue ?? []} onOpen={() => onSelectTab("work")} />
        <ScheduleSnapshot schedules={schedules} />
      </div>

      <div className="operations-overview-grid">
        <RecentRuns exchange={market.exchange} runs={overview?.recent_runs ?? []} onOpen={() => onSelectTab("runs")} />
        <RecentLifecycle rows={overview?.recent_lifecycle_events ?? []} onOpen={() => onSelectTab("lifecycle")} />
      </div>
    </>
  );
}

function FreshnessCard({
  freshness,
  universe,
}: {
  freshness: OperationsFreshnessRow | undefined;
  universe: OperationsUniverseSnapshotRow | undefined;
}) {
  return (
    <section className="data-card operations-detail-card">
      <div className="data-card-header">
        <div>
          <h2>Data Freshness</h2>
          <p>Persisted yfinance daily candles</p>
        </div>
        <ShieldCheck size={20} />
      </div>
      <dl className="operations-definition-list">
        <div><dt>First stored session</dt><dd>{formatDate(freshness?.first_date)}</dd></div>
        <div><dt>Latest stored session</dt><dd>{formatDate(freshness?.latest_date)}</dd></div>
        <div><dt>Symbols with data</dt><dd>{formatNumber(freshness?.symbols)}</dd></div>
        <div><dt>Suspicious rows</dt><dd>{formatNumber(freshness?.suspicious_rows)}</dd></div>
        <div><dt>Universe refreshed</dt><dd>{universe ? formatDateTime(universe.fetched_at) : "Never"}</dd></div>
      </dl>
    </section>
  );
}

function RateLimitCard({ rate }: { rate: OperationsAdaptiveRateStateRow | undefined }) {
  return (
    <section className="data-card operations-detail-card">
      <div className="data-card-header">
        <div>
          <h2>Yahoo Control Plane</h2>
          <p>Shared adaptive provider budget</p>
        </div>
        <span className={`status-pill ${statusClass(rate?.circuit_state ?? "unknown")}`}>
          {rate?.circuit_state ?? "unknown"}
        </span>
      </div>
      <div className="operations-rate-grid">
        <div><CircleGauge size={18} /><span>Current rate</span><strong>{formatNumber(rate?.current_rpm)} RPM</strong></div>
        <div><Zap size={18} /><span>Concurrency</span><strong>{formatNumber(rate?.current_concurrency)}</strong></div>
        <div><Activity size={18} /><span>Error rate</span><strong>{formatPercent(rate?.recent_error_rate)}</strong></div>
        <div><Clock3 size={18} /><span>Baseline</span><strong>{rate?.latency_baseline_ms ? `${Math.round(rate.latency_baseline_ms)} ms` : "—"}</strong></div>
      </div>
    </section>
  );
}

function QueueSnapshot({ groups, onOpen }: { groups: OperationsQueueGroup[]; onOpen: () => void }) {
  const activeGroups = groups.filter((row) => ["queued", "running", "retry_wait"].includes(row.status));
  const visible = (activeGroups.length ? activeGroups : groups).slice(0, 8);
  return (
    <section className="data-card">
      <CardHeader title="Durable Work Queue" subtitle={activeGroups.length ? "Runnable and retrying work" : "No open work; showing durable history"} onOpen={onOpen} />
      <div className="operations-table-wrap">
        <table className="operations-table">
          <thead><tr><th>Work type</th><th>Status</th><th>Items</th><th>Symbols</th><th>Attempts</th></tr></thead>
          <tbody>
            {visible.map((row) => (
              <tr key={`${row.exchange}-${row.work_type}-${row.status}`}>
                <td>{humanize(row.work_type)}</td>
                <td><span className={`status-pill ${statusClass(row.status)}`}>{humanize(row.status)}</span></td>
                <td>{formatNumber(row.items)}</td>
                <td>{formatNumber(row.symbols)}</td>
                <td>{formatNumber(row.maximum_attempts)}</td>
              </tr>
            ))}
          </tbody>
        </table>
        {!visible.length ? <EmptyState label="No durable work has been recorded." /> : null}
      </div>
    </section>
  );
}

function ScheduleSnapshot({ schedules }: { schedules: PipelineScheduleStatusRow[] }) {
  const relevant = schedules.filter((schedule) =>
    schedule.schedule_name.includes("yfinance_daily_work") ||
    schedule.schedule_name.includes("completed_session_work_planner") ||
    schedule.schedule_name.includes("universe_refresh") ||
    schedule.schedule_name.includes("exchange_sessions") ||
    schedule.schedule_name.includes("opportunity_targets"),
  );
  return (
    <section className="data-card">
      <div className="data-card-header"><div><h2>Automation</h2><p>Desired and observed Dagster schedule state</p></div></div>
      <div className="operations-stack-list">
        {relevant.map((schedule) => (
          <article className="schedule-status-row" key={schedule.schedule_name}>
            <div>
              <strong>{humanize(schedule.schedule_name)}</strong>
              <span>{schedule.cron_schedule} · {schedule.execution_timezone}</span>
              <span>
                Desired {schedule.desired_status}
                {schedule.last_successful_run_at ? ` · Last success ${formatDateTime(schedule.last_successful_run_at)}` : ""}
              </span>
            </div>
            <div className="schedule-status-group">
              {schedule.status_drift ? <span className="status-pill warning">drift</span> : null}
              <span className={`status-pill ${statusClass(schedule.actual_status)}`}>{schedule.actual_status}</span>
            </div>
          </article>
        ))}
        {!relevant.length ? <EmptyState label="No equity schedules were reported." /> : null}
      </div>
    </section>
  );
}

function RecentRuns({ exchange, runs, onOpen }: { exchange: OperationsExchange; runs: DataPipelineRunSummary[]; onOpen: () => void }) {
  return (
    <section className="data-card">
      <CardHeader title="Recent Runs" subtitle="Latest exchange-matched ingestion activity" onOpen={onOpen} />
      <div className="operations-stack-list">
        {runs.slice(0, 8).map((run) => {
          const result = runResultForExchange(run, exchange);
          const status = runStatusForExchange(run, exchange);
          const succeeded = result?.items_succeeded ?? run.items_succeeded;
          const failed = result?.items_failed ?? run.items_failed;
          return (
            <article key={run.id}>
              <div><strong>{humanize(run.name)}</strong><span>{formatDateTime(run.started_at)} · {formatNumber(succeeded)} succeeded · {formatNumber(failed)} failed</span></div>
              <span className={`status-pill ${statusClass(status)}`}>{humanize(status)}</span>
            </article>
          );
        })}
        {!runs.length ? <EmptyState label="No recent runs matched this exchange." /> : null}
      </div>
    </section>
  );
}

function RecentLifecycle({ rows, onOpen }: { rows: OperationsLifecycleEventRow[]; onOpen: () => void }) {
  return (
    <section className="data-card">
      <CardHeader title="Universe Changes" subtitle="Recent symbol lifecycle events" onOpen={onOpen} />
      <div className="operations-stack-list">
        {rows.slice(0, 8).map((row) => (
          <article key={row.event_id}>
            <div><strong>{row.symbol ?? row.canonical_instrument_id}</strong><span>{formatDateTime(row.created_at)} · {lifecycleEventLabel(row.event_type)}</span></div>
            <span className={`status-pill ${statusClass(row.event_type)}`}>{lifecycleEventLabel(row.event_type)}</span>
          </article>
        ))}
        {!rows.length ? <EmptyState label="No lifecycle events were recorded." /> : null}
      </div>
    </section>
  );
}

function CardHeader({ title, subtitle, onOpen }: { title: string; subtitle: string; onOpen: () => void }) {
  return (
    <div className="data-card-header">
      <div><h2>{title}</h2><p>{subtitle}</p></div>
      <button className="operations-text-button" type="button" onClick={onOpen}>View all <ArrowRight size={15} /></button>
    </div>
  );
}

function CoverageView({
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
    !error && summary && summary.expected_rows > 0
      ? summary.calendar_matched_rows / summary.expected_rows
      : null;
  return (
    <>
      <div className="metric-grid data-metric-grid">
        <MetricCard icon={Users} label="Symbols" value={formatNumber(summary?.symbols_total)} detail={`${formatNumber(summary?.symbols_complete)} complete`} />
        <MetricCard icon={DatabaseZap} label="Calendar Covered" value={formatNumber(summary?.calendar_matched_rows)} detail={`${formatNumber(summary?.expected_rows)} expected`} />
        <MetricCard icon={Search} label="Actionable Gaps" value={formatNumber(summary?.actionable_missing_rows)} detail={`${formatNumber(summary?.symbols_actionable)} symbols require work`} />
        <MetricCard icon={ShieldCheck} label="Coverage" value={formatPercent(coverage)} detail={`${market.label} · ten-year target`} />
      </div>
      {!error && summary ? (
        <div className="operations-coverage-breakdown" role="note">
          <div><strong>{formatNumber(summary.stored_rows)}</strong><span>Raw stored observations</span><small>Every persisted candle in the selected window, including off-calendar dates.</small></div>
          <div><strong>{formatNumber(summary.provider_unavailable_rows)}</strong><span>Provider-unavailable sessions</span><small>Verified history Yahoo does not expose; no retry is required.</small></div>
          <div><strong>{formatNumber(summary.off_calendar_rows)}</strong><span>Outside-calendar observations</span><small>Stored special or unclassified sessions excluded from the coverage ratio.</small></div>
          <div><strong>{formatNumber(summary.missing_rows)}</strong><span>Total calendar gaps</span><small>Provider-unavailable plus actionable sessions.</small></div>
        </div>
      ) : null}
      <section className="data-card">
        <div className="data-card-header">
          <div><h2>Exact Session Coverage</h2><p>Calendar-aware yfinance daily coverage</p></div>
          <button className="icon-button" type="button" onClick={onRefresh}><RefreshCw size={16} />{isFetching && !isLoading ? "Refreshing" : "Refresh"}</button>
        </div>
        <div className="data-filter-row operations-coverage-filters">
          <SymbolAutocomplete
            label="Symbol or name"
            exchange={market.exchange}
            value={query}
            onChange={onQueryChange}
            placeholder={`Search ${market.label}`}
          />
          <label>Status<select value={status ?? ""} onChange={(event) => onStatusChange(event.target.value as DataAvailabilityParams["coverage_status"])}><option value="">All</option><option value="complete">Complete</option><option value="partial">Partial</option><option value="empty">Empty</option></select></label>
          <label>Start<input type="date" value={startDate} onChange={(event) => onStartDateChange(event.target.value)} /></label>
          <label>End<input type="date" value={endDate} onChange={(event) => onEndDateChange(event.target.value)} /></label>
        </div>
        {isLoading ? <LoadingState /> : null}
        {error ? <p className="form-error operations-form-error">Coverage could not load: {error.message}</p> : null}
        {!isLoading && !error && !availability?.rows.length ? <EmptyState label="No symbols matched these coverage filters." /> : null}
        {availability?.rows.length ? <CoverageTable rows={availability.rows} /> : null}
      </section>
    </>
  );
}

function CoverageTable({ rows }: { rows: DataAvailabilityRow[] }) {
  return (
    <div className="operations-table-wrap">
      <table className="operations-table">
        <thead><tr><th>Symbol</th><th>Status</th><th>Coverage</th><th>Covered / expected</th><th>Unavailable</th><th>Actionable</th><th>Outside calendar</th><th>Latest</th></tr></thead>
        <tbody>{rows.map((row) => <tr key={row.instrument_key}><td><strong>{row.symbol}</strong><small>{row.name ?? row.instrument_key}</small></td><td><span className={`status-pill ${statusClass(row.coverage_status)}`}>{row.coverage_status}</span></td><td><div className="operations-coverage-cell"><div><span style={{ width: `${Math.min(row.coverage_pct * 100, 100)}%` }} /></div><strong>{formatPercent(row.coverage_pct)}</strong></div></td><td>{formatNumber(row.calendar_matched_rows)} / {formatNumber(row.expected_rows)}</td><td>{formatNumber(row.provider_unavailable_rows)}</td><td>{formatNumber(row.actionable_missing_rows)}</td><td>{formatNumber(row.off_calendar_rows)}</td><td>{formatDate(row.latest_stored_date)}</td></tr>)}</tbody>
      </table>
    </div>
  );
}

function WorkQueueView({
  exchange,
  groups,
  rows,
  total,
  offset,
  isLoading,
  isFetching,
  error,
  status,
  workType,
  symbol,
  onStatusChange,
  onWorkTypeChange,
  onSymbolChange,
  onOffsetChange,
  onRefresh,
}: {
  exchange: OperationsExchange;
  groups: OperationsQueueGroup[];
  rows: OperationsWorkItemRow[];
  total: number;
  offset: number;
  isLoading: boolean;
  isFetching: boolean;
  error: Error | null;
  status: string;
  workType: string;
  symbol: string;
  onStatusChange: (value: string) => void;
  onWorkTypeChange: (value: string) => void;
  onSymbolChange: (value: string) => void;
  onOffsetChange: (value: number) => void;
  onRefresh: () => void;
}) {
  const queued = sumQueue(groups, ["queued"]);
  const running = sumQueue(groups, ["running"]);
  const retrying = sumQueue(groups, ["retry_wait"]);
  const terminal = sumQueue(groups, ["terminal", "failed"]);
  return (
    <>
      <div className="metric-grid data-metric-grid">
        <MetricCard icon={ListChecks} label="Queued" value={formatNumber(queued)} detail="Ready durable work" />
        <MetricCard icon={Activity} label="Running" value={formatNumber(running)} detail="Currently claimed" />
        <MetricCard icon={Clock3} label="Retry Wait" value={formatNumber(retrying)} detail="Automatic recovery" />
        <MetricCard icon={AlertTriangle} label="Terminal" value={formatNumber(terminal)} detail="Manual review candidates" />
      </div>
      <section className="data-card">
        <div className="data-card-header"><div><h2>Durable Work Items</h2><p>{formatNumber(total)} matching records · read-only</p></div><button className="icon-button" type="button" onClick={onRefresh}><RefreshCw size={16} />{isFetching && !isLoading ? "Refreshing" : "Refresh"}</button></div>
        <div className="data-filter-row operations-work-filters">
          <SymbolAutocomplete
            label="Symbol"
            exchange={exchange}
            value={symbol}
            onChange={onSymbolChange}
            placeholder="AAPL, RELIANCE, RY.TO"
          />
          <label>Status<select value={status} onChange={(event) => onStatusChange(event.target.value)}><option value="">All</option><option value="queued">Queued</option><option value="running">Running</option><option value="retry_wait">Retry wait</option><option value="succeeded">Succeeded</option><option value="cancelled">Cancelled</option><option value="terminal">Terminal</option></select></label>
          <label>Work type<select value={workType} onChange={(event) => onWorkTypeChange(event.target.value)}><option value="">All</option><option value="daily_incremental">Daily incremental</option><option value="initial_backfill">Initial backfill</option><option value="new_symbol_backfill">New symbol backfill</option><option value="gap_repair">Gap repair</option></select></label>
        </div>
        {isLoading ? <LoadingState /> : null}
        {error ? <p className="form-error operations-form-error">Work items could not load: {error.message}</p> : null}
        {!isLoading && !error && !rows.length ? <EmptyState label="No durable work matched these filters." /> : null}
        {rows.length ? <WorkItemsTable rows={rows} /> : null}
        <Pagination total={total} offset={offset} onChange={onOffsetChange} />
      </section>
    </>
  );
}

function WorkItemsTable({ rows }: { rows: OperationsWorkItemRow[] }) {
  return (
    <div className="operations-table-wrap">
      <table className="operations-table">
        <thead><tr><th>Symbol / work</th><th>Status</th><th>Window</th><th>Attempts</th><th>Last signal</th><th>Updated</th></tr></thead>
        <tbody>{rows.map((row) => <tr key={row.work_item_id}><td><strong>{row.provider_symbol}</strong><small>{humanize(row.work_type)}</small></td><td><span className={`status-pill ${statusClass(row.status)}`}>{humanize(row.status)}</span></td><td>{formatDate(row.window_start)}<small>to {formatDate(row.window_end)}</small></td><td>{row.attempt_count} / {row.max_attempts}</td><td>{row.last_error_code ? <><strong className="operations-error-code">{humanize(row.last_error_code)}</strong><small>{row.last_error_message ?? "No detail"}</small></> : "—"}</td><td>{formatDateTime(row.updated_at)}</td></tr>)}</tbody>
      </table>
    </div>
  );
}

function RunsView({
  exchange,
  runs,
  isLoading,
  isFetching,
  error,
  status,
  startDate,
  endDate,
  onStatusChange,
  onStartDateChange,
  onEndDateChange,
  onRefresh,
  onSelectRun,
}: {
  exchange: OperationsExchange;
  runs: DataPipelineRunSummary[];
  isLoading: boolean;
  isFetching: boolean;
  error: Error | null;
  status: string;
  startDate: string;
  endDate: string;
  onStatusChange: (value: string) => void;
  onStartDateChange: (value: string) => void;
  onEndDateChange: (value: string) => void;
  onRefresh: () => void;
  onSelectRun: (runId: string) => void;
}) {
  const statuses = runs.map((run) => runStatusForExchange(run, exchange));
  const attributableStatuses = statuses.filter((status) => status !== "shared_run");
  const succeeded = attributableStatuses.filter(
    (status) => statusClass(status) === "completed",
  ).length;
  const failed = attributableStatuses.filter(
    (status) => statusClass(status) === "failed",
  ).length;
  const processed = runs.reduce((total, run) => {
    const result = runResultForExchange(run, exchange);
    if (result) return total + result.items_processed;
    return run.exchange === exchange ? total + run.items_processed : total;
  }, 0);
  return (
    <>
      <div className="metric-grid data-metric-grid">
        <MetricCard icon={History} label="Visible Runs" value={formatNumber(runs.length)} detail={`${formatNumber(failed)} ${exchange} failures`} />
        <MetricCard icon={TableProperties} label="Items Processed" value={formatNumber(processed)} detail={`${exchange}-attributed items`} />
        <MetricCard icon={ShieldCheck} label="Success Rate" value={formatPercent(attributableStatuses.length ? succeeded / attributableStatuses.length : null)} detail={`${formatNumber(attributableStatuses.length)} attributable runs`} />
        <MetricCard icon={Clock3} label="Average Time" value={`${formatNumber(averageDuration(runs))}s`} detail="Finished runs" />
      </div>
      <section className="data-card">
        <div className="data-card-header"><div><h2>Ingestion Runs</h2><p>{exchange}-scoped outcomes; legacy shared runs are explicitly marked</p></div><button className="icon-button" type="button" onClick={onRefresh}><RefreshCw size={16} />{isFetching && !isLoading ? "Refreshing" : "Refresh"}</button></div>
        <div className="data-filter-row operations-run-filters">
          <label>Status<select value={status} onChange={(event) => onStatusChange(event.target.value)}><option value="">All</option><option value="completed">Completed</option><option value="completed_with_failures">With failures</option><option value="running">Running</option><option value="failed">Failed</option></select></label>
          <label>Start<input type="date" value={startDate} onChange={(event) => onStartDateChange(event.target.value)} /></label>
          <label>End<input type="date" value={endDate} onChange={(event) => onEndDateChange(event.target.value)} /></label>
        </div>
        {isLoading ? <LoadingState /> : null}
        {error ? <p className="form-error operations-form-error">Runs could not load: {error.message}</p> : null}
        {!isLoading && !error && !runs.length ? <EmptyState label="No ingestion runs matched this exchange and window." /> : null}
        {runs.length ? <RunsTable exchange={exchange} runs={runs} onSelectRun={onSelectRun} /> : null}
      </section>
    </>
  );
}

function RunsTable({ exchange, runs, onSelectRun }: { exchange: OperationsExchange; runs: DataPipelineRunSummary[]; onSelectRun: (runId: string) => void }) {
  return (
    <div className="operations-table-wrap">
      <table className="operations-table">
        <thead><tr><th>Run</th><th>Status</th><th>Exchange scope</th><th>Processed</th><th>Succeeded</th><th>Failed</th><th>Duration</th><th>Started</th><th>Details</th></tr></thead>
        <tbody>{runs.map((run) => {
          const result = runResultForExchange(run, exchange);
          const isDirect = run.exchange === exchange;
          const status = runStatusForExchange(run, exchange);
          const processed = result?.items_processed ?? (isDirect ? run.items_processed : null);
          const runSucceeded = result?.items_succeeded ?? (isDirect ? run.items_succeeded : null);
          const runFailed = result?.items_failed ?? (isDirect ? run.items_failed : null);
          return <tr key={run.id}><td><strong>{humanize(run.name)}</strong><small>{run.id.slice(0, 8)} · {String(run.run_metadata.trigger ?? "pipeline")}</small></td><td><span className={`status-pill ${statusClass(status)}`}>{humanize(status)}</span></td><td>{result ? exchange : run.exchange}<small>{result ? `${exchange} outcome` : run.exchange === "MULTI" ? `Global: ${run.work_item_exchanges.join(", ")}` : exchange}</small></td><td>{formatNumber(processed)}</td><td>{formatNumber(runSucceeded)}</td><td>{formatNumber(runFailed)}</td><td>{run.duration_seconds === null ? "Running" : `${run.duration_seconds}s`}</td><td>{formatDateTime(run.started_at)}</td><td><button className="operations-text-button" type="button" onClick={() => onSelectRun(run.id)}>Inspect <ArrowRight size={14} /></button></td></tr>;
        })}</tbody>
      </table>
    </div>
  );
}

function RunDetailDrawer({
  exchange,
  detail,
  isLoading,
  error,
  onClose,
}: {
  exchange: OperationsExchange;
  detail: DataPipelineRunDetail | null;
  isLoading: boolean;
  error: Error | null;
  onClose: () => void;
}) {
  const run = detail?.run;
  const result = run ? runResultForExchange(run, exchange) : null;
  const workItems = detail?.work_items ?? [];
  const unresolved = workItems.filter((item) =>
    ["queued", "running", "retry_wait", "terminal", "failed"].includes(item.status),
  );
  const errorGroups = (() => {
    const groups = new Map<string, { message: string; code: string; statusCode: number | null; symbols: Set<string>; occurrences: number }>();
    for (const item of workItems) {
      if (!item.last_error_code && !item.last_error_message) continue;
      const code = item.last_error_code ?? "work_item_error";
      const message = item.last_error_message ?? "No work-item error detail was recorded.";
      const key = `${code}|${item.last_status_code ?? ""}|${message}`;
      const group = groups.get(key) ?? { message, code, statusCode: item.last_status_code, symbols: new Set<string>(), occurrences: 0 };
      group.symbols.add(item.provider_symbol);
      group.occurrences += 1;
      groups.set(key, group);
    }
    for (const request of detail?.provider_requests ?? []) {
      if (!request.error_message && !request.rate_limited && statusClass(request.status) !== "failed") continue;
      const code = request.rate_limited ? "rate_limited" : request.status || "provider_error";
      const message = request.error_message ?? (request.rate_limited ? "Yahoo rate limited this request." : `Provider request ended with status ${request.status}.`);
      const key = `${code}|${request.status_code ?? ""}|${message}`;
      const group = groups.get(key) ?? { message, code, statusCode: request.status_code, symbols: new Set<string>(), occurrences: 0 };
      if (request.symbol) group.symbols.add(request.symbol);
      group.occurrences += 1;
      groups.set(key, group);
    }
    return [...groups.values()].sort((left, right) => right.occurrences - left.occurrences);
  })();
  const wasRecovered = Boolean(run?.items_failed) && unresolved.length === 0 && workItems.length > 0;

  return (
    <div className="operations-drawer-backdrop" role="presentation" onMouseDown={onClose}>
      <aside
        className="operations-run-drawer"
        role="dialog"
        aria-modal="true"
        aria-label="Pipeline run details"
        onMouseDown={(event) => event.stopPropagation()}
      >
        <div className="operations-drawer-header">
          <div>
            <span className="operations-eyebrow">Run inspection</span>
            <h2>{run ? humanize(run.name) : "Loading run"}</h2>
            {run ? <p>{run.id} · {String(run.run_metadata.trigger ?? "pipeline")}</p> : null}
          </div>
          <button className="icon-button" type="button" onClick={onClose} aria-label="Close run details"><X size={18} /></button>
        </div>
        {isLoading ? <LoadingState /> : null}
        {error ? <p className="form-error operations-form-error">Run details could not load: {error.message}</p> : null}
        {run ? (
          <div className="operations-drawer-body">
            <div className="operations-run-summary-grid">
              <div><span>Status</span><strong><span className={`status-pill ${statusClass(runStatusForExchange(run, exchange))}`}>{humanize(runStatusForExchange(run, exchange))}</span></strong></div>
              <div><span>Exchange</span><strong>{result ? exchange : `${run.exchange} global`}</strong></div>
              <div><span>Processed</span><strong>{formatNumber(result?.items_processed ?? run.items_processed)}</strong></div>
              <div><span>Succeeded</span><strong>{formatNumber(result?.items_succeeded ?? run.items_succeeded)}</strong></div>
              <div><span>Failed</span><strong>{formatNumber(result?.items_failed ?? run.items_failed)}</strong></div>
              <div><span>Duration</span><strong>{run.duration_seconds === null ? "Running" : `${run.duration_seconds}s`}</strong></div>
            </div>

            {wasRecovered ? <div className="operations-recovery-note"><CheckCircle2 size={18} /><div><strong>Recovered after this run</strong><span>All claimed work is now resolved or safely cancelled.</span></div></div> : null}
            {run.error_message ? <section className="operations-run-errors"><h3>Run error</h3><p>{run.error_message}</p></section> : null}

            <section className="operations-run-errors">
              <h3>Error evidence</h3>
              {errorGroups.length ? errorGroups.map((group) => (
                <article key={`${group.code}-${group.statusCode}-${group.message}`}>
                  <div><strong>{humanize(group.code)}</strong><span>{formatNumber(group.occurrences)} occurrence{group.occurrences === 1 ? "" : "s"}{group.statusCode ? ` · HTTP ${group.statusCode}` : ""}</span></div>
                  <p>{group.message}</p>
                  {group.symbols.size ? <small>{[...group.symbols].slice(0, 20).join(", ")}{group.symbols.size > 20 ? ` and ${group.symbols.size - 20} more` : ""}</small> : null}
                </article>
              )) : <EmptyState label="No item-level or provider error message was retained for this run." />}
            </section>

            <section className="operations-run-errors">
              <h3>Claimed work ({formatNumber(workItems.length)})</h3>
              <div className="operations-table-wrap">
                <table className="operations-table compact">
                  <thead><tr><th>Symbol</th><th>Exchange</th><th>Current status</th><th>Attempts</th><th>Next retry</th></tr></thead>
                  <tbody>{workItems.slice(0, 100).map((item) => <tr key={item.work_item_id}><td><strong>{item.provider_symbol}</strong><small>{formatDate(item.window_start)}–{formatDate(item.window_end)}</small></td><td>{item.exchange}</td><td><span className={`status-pill ${statusClass(item.status)}`}>{humanize(item.status)}</span></td><td>{item.attempt_count} / {item.max_attempts}</td><td>{item.next_attempt_at ? formatDateTime(item.next_attempt_at) : "—"}</td></tr>)}</tbody>
                </table>
              </div>
              {workItems.length > 100 ? <p>Showing the first 100 claimed items.</p> : null}
            </section>
          </div>
        ) : null}
      </aside>
    </div>
  );
}

function LifecycleView({
  exchange,
  rows,
  total,
  offset,
  isLoading,
  isFetching,
  error,
  eventType,
  symbol,
  onEventTypeChange,
  onSymbolChange,
  onOffsetChange,
  onRefresh,
}: {
  exchange: OperationsExchange;
  rows: OperationsLifecycleEventRow[];
  total: number;
  offset: number;
  isLoading: boolean;
  isFetching: boolean;
  error: Error | null;
  eventType: string;
  symbol: string;
  onEventTypeChange: (value: string) => void;
  onSymbolChange: (value: string) => void;
  onOffsetChange: (value: number) => void;
  onRefresh: () => void;
}) {
  return (
    <section className="data-card">
      <div className="data-card-header"><div><h2>Universe Lifecycle</h2><p>{formatNumber(total)} observations, removals, and status transitions</p></div><button className="icon-button" type="button" onClick={onRefresh}><RefreshCw size={16} />{isFetching && !isLoading ? "Refreshing" : "Refresh"}</button></div>
      <div className="data-filter-row operations-lifecycle-filters">
        <SymbolAutocomplete
          label="Symbol"
          exchange={exchange}
          value={symbol}
          onChange={onSymbolChange}
          placeholder="Search exchange symbol"
        />
        <label>Event<select value={eventType} onChange={(event) => onEventTypeChange(event.target.value)}><option value="">All</option><option value="added">First observed</option><option value="reactivated">Reactivated</option><option value="suspected_inactive">Suspected inactive</option><option value="inactive">Inactive</option><option value="renamed">Renamed</option></select></label>
      </div>
      {isLoading ? <LoadingState /> : null}
      {error ? <p className="form-error operations-form-error">Lifecycle events could not load: {error.message}</p> : null}
      {!isLoading && !error && !rows.length ? <EmptyState label="No lifecycle events matched these filters." /> : null}
      {rows.length ? <LifecycleTable rows={rows} /> : null}
      <Pagination total={total} offset={offset} onChange={onOffsetChange} />
    </section>
  );
}

function LifecycleTable({ rows }: { rows: OperationsLifecycleEventRow[] }) {
  return (
    <div className="operations-table-wrap">
      <table className="operations-table">
        <thead><tr><th>Symbol</th><th>Event</th><th>Detected</th><th>Snapshot</th><th>Canonical identity</th></tr></thead>
        <tbody>{rows.map((row) => <tr key={row.event_id}><td><strong>{row.symbol ?? "Unknown symbol"}</strong><small>{row.exchange}</small></td><td><span className={`status-pill ${statusClass(row.event_type)}`}>{lifecycleEventLabel(row.event_type)}</span></td><td>{formatDateTime(row.created_at)}</td><td>{row.snapshot_id ? row.snapshot_id.slice(0, 12) : "—"}</td><td><small className="operations-mono">{row.canonical_instrument_id}</small></td></tr>)}</tbody>
      </table>
    </div>
  );
}

function WarehouseView({
  exchange,
  overview,
  isLoading,
  error,
  onRefresh,
}: {
  exchange: OperationsExchange;
  overview: BigQuerySyncOverviewResponse | null;
  isLoading: boolean;
  error: Error | null;
  onRefresh: () => void;
}) {
  const runs = (overview?.runs ?? []).filter(
    (run) => run.exchange === null || run.exchange === exchange,
  );
  const partitions = (overview?.partitions ?? []).filter(
    (partition) => partition.exchange === null || partition.exchange === exchange,
  );
  const latest = runs[0];
  const pending = partitions.filter((partition) =>
    ["pending", "running"].includes(partition.status),
  ).length;
  const failed = partitions.filter((partition) => partition.status === "failed").length;
  if (isLoading && !overview) return <LoadingState />;
  return (
    <>
      <div className="metric-grid data-metric-grid">
        <MetricCard
          icon={DatabaseZap}
          label="BigQuery Export"
          value={overview?.production_sync_enabled ? "Production" : overview?.canary_enabled ? "Canary only" : overview?.enabled ? "Preflight only" : "Disabled"}
          detail={overview?.project_id ? `${overview.project_id}.${overview.core_dataset}` : "Safe default"}
        />
        <MetricCard
          icon={CheckCircle2}
          label="Last Sync"
          value={latest ? humanize(latest.status) : "Never"}
          detail={latest?.finished_at ? `${formatDateTime(latest.finished_at)} · ${latest.retry_count} retries` : overview?.location ?? "Not configured"}
        />
        <MetricCard
          icon={Workflow}
          label="Active Partitions"
          value={formatNumber(pending)}
          detail={`${formatNumber(failed)} failed partitions`}
        />
        <MetricCard
          icon={TableProperties}
          label="Count Difference"
          value={formatNumber(latest?.count_difference)}
          detail={`${formatNumber(latest?.inserted_rows)} inserted · ${formatNumber(latest?.updated_rows)} updated`}
        />
      </div>
      {!overview?.enabled ? (
        <section className="operations-alert warning">
          <ShieldCheck size={21} />
          <div><strong>BigQuery synchronization is disabled</strong><span>PostgreSQL remains the source of truth. Activate only after the deployment checklist and credentials are complete.</span></div>
        </section>
      ) : null}
      {error ? <p className="form-error operations-form-error">Warehouse status could not load: {error.message}</p> : null}
      <section className="data-card">
        <div className="data-card-header"><div><h2>Synchronization Partitions</h2><p>Watermarks, reconciliation, retries, and BigQuery jobs for {exchange}</p></div><button className="icon-button" type="button" onClick={onRefresh}><RefreshCw size={16} />Refresh</button></div>
        {!error && !partitions.length ? <EmptyState label="No BigQuery synchronization partitions have been recorded." /> : null}
        {partitions.length ? (
          <div className="operations-table-wrap">
            <table className="operations-table">
              <thead><tr><th>Entity</th><th>Status</th><th>Scope</th><th>Rows</th><th>Watermarks / dates</th><th>Staging / MERGE</th><th>Job / error</th></tr></thead>
              <tbody>{partitions.map((partition) => <tr key={partition.partition_id}><td><strong>{humanize(partition.entity)}</strong><small>{partition.exchange ?? "All exchanges"}</small></td><td><span className={`status-pill ${statusClass(partition.status)}`}>{humanize(partition.status)}</span><small>{partition.duration_seconds === null ? "—" : `${partition.duration_seconds.toFixed(1)}s`} · {partition.attempt_count} attempts</small></td><td>{partition.partition_start ? `${formatDate(partition.partition_start)}–${formatDate(partition.partition_end)}` : "Current state"}</td><td><strong>{formatNumber(partition.source_row_count)} → {formatNumber(partition.destination_row_count)}</strong><small>Difference {formatNumber(partition.count_difference)} · {formatNumber(partition.duplicate_business_key_count)} duplicates</small></td><td><small>{partition.source_watermark ?? "—"}<br />{partition.destination_watermark ?? "—"}<br />{partition.source_min_date ?? "—"}–{partition.source_max_date ?? "—"}</small></td><td><strong>{formatNumber(partition.staging_row_count)} / {formatNumber(partition.merged_row_count)}</strong><small>staged / merged · {formatNumber(partition.inserted_rows)} inserted · {formatNumber(partition.updated_rows)} updated · {formatNumber(partition.rejected_rows)} rejected</small></td><td><small className="operations-mono">{partition.bigquery_job_id ?? partition.error_details ?? "—"}</small>{Object.keys(partition.schema_drift).length ? <small>Schema drift detected</small> : null}</td></tr>)}</tbody>
            </table>
          </div>
        ) : null}
      </section>
    </>
  );
}

function SymbolAutocomplete({
  label,
  exchange,
  value,
  onChange,
  placeholder,
}: {
  label: string;
  exchange: OperationsExchange;
  value: string;
  onChange: (value: string) => void;
  placeholder: string;
}) {
  const [open, setOpen] = useState(false);
  const [activeIndex, setActiveIndex] = useState(-1);
  const blurTimer = useRef<number | null>(null);
  const debounced = useDebouncedValue(value.trim(), 200);
  const suggestionsQuery = useDataInstrumentSearch(
    {
      provider: "yfinance",
      exchange,
      query: debounced,
      limit: 10,
    },
    debounced.length >= 2,
  );
  const suggestions = suggestionsQuery.data ?? [];

  useEffect(() => () => {
    if (blurTimer.current !== null) window.clearTimeout(blurTimer.current);
  }, []);

  function selectSuggestion(suggestion: DataInstrumentSearchRow) {
    onChange(suggestion.symbol);
    setOpen(false);
    setActiveIndex(-1);
  }

  function handleKeyDown(event: KeyboardEvent<HTMLInputElement>) {
    if (event.key === "Escape") {
      setOpen(false);
      return;
    }
    if (!open || !suggestions.length) return;
    if (event.key === "ArrowDown") {
      event.preventDefault();
      setActiveIndex((current) => Math.min(current + 1, suggestions.length - 1));
    } else if (event.key === "ArrowUp") {
      event.preventDefault();
      setActiveIndex((current) => Math.max(current - 1, 0));
    } else if (event.key === "Enter" && activeIndex >= 0) {
      event.preventDefault();
      selectSuggestion(suggestions[activeIndex]);
    }
  }

  return (
    <label className="data-search-field operations-autocomplete">
      {label}
      <Search size={16} />
      <input
        value={value}
        onChange={(event) => {
          onChange(event.target.value);
          setActiveIndex(-1);
          setOpen(event.target.value.trim().length >= 2);
        }}
        onFocus={() => setOpen(value.trim().length >= 2)}
        onBlur={() => {
          blurTimer.current = window.setTimeout(() => setOpen(false), 120);
        }}
        onKeyDown={handleKeyDown}
        placeholder={placeholder}
        autoComplete="off"
        role="combobox"
        aria-autocomplete="list"
        aria-expanded={open}
        aria-busy={suggestionsQuery.isFetching}
        aria-controls={`${exchange}-${label.replaceAll(" ", "-")}-suggestions`}
      />
      {open ? (
        <div
          id={`${exchange}-${label.replaceAll(" ", "-")}-suggestions`}
          className="operations-suggestions"
          role="listbox"
        >
          {suggestionsQuery.isFetching ? <span className="operations-suggestion-state">Searching…</span> : null}
          {!suggestionsQuery.isFetching && suggestions.map((suggestion, index) => (
            <button
              key={suggestion.canonical_instrument_id ?? suggestion.instrument_key}
              type="button"
              role="option"
              aria-selected={index === activeIndex}
              className={index === activeIndex ? "active" : ""}
              onMouseDown={(event) => event.preventDefault()}
              onClick={() => selectSuggestion(suggestion)}
            >
              <strong>{suggestion.symbol}</strong>
              <span>{suggestion.name ?? suggestion.provider_symbol ?? suggestion.instrument_key}</span>
              {suggestion.provider_symbol && suggestion.provider_symbol !== suggestion.symbol ? <small>{suggestion.provider_symbol}</small> : null}
            </button>
          ))}
          {!suggestionsQuery.isFetching && !suggestions.length ? <span className="operations-suggestion-state">No active {exchange} symbols found.</span> : null}
        </div>
      ) : null}
    </label>
  );
}

function Pagination({ total, offset, onChange }: { total: number; offset: number; onChange: (offset: number) => void }) {
  if (total <= pageSize) return null;
  const first = offset + 1;
  const last = Math.min(offset + pageSize, total);
  return (
    <div className="operations-pagination">
      <span>{formatNumber(first)}–{formatNumber(last)} of {formatNumber(total)}</span>
      <div>
        <button className="icon-button" type="button" disabled={offset === 0} onClick={() => onChange(Math.max(0, offset - pageSize))}><ArrowLeft size={15} />Previous</button>
        <button className="icon-button" type="button" disabled={offset + pageSize >= total} onClick={() => onChange(offset + pageSize)}>Next<ArrowRight size={15} /></button>
      </div>
    </div>
  );
}

function sumQueue(groups: OperationsQueueGroup[], statuses: string[]): number {
  return groups
    .filter((row) => statuses.includes(row.status))
    .reduce((total, row) => total + row.items, 0);
}

function averageDuration(runs: DataPipelineRunSummary[]): number {
  const durations = runs
    .map((run) => run.duration_seconds)
    .filter((value): value is number => typeof value === "number");
  if (!durations.length) return 0;
  return Math.round(durations.reduce((total, value) => total + value, 0) / durations.length);
}
