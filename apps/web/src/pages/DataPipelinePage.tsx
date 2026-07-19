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
  Zap,
} from "lucide-react";
import { useMemo, useState } from "react";
import { useQueryClient } from "@tanstack/react-query";

import {
  useDataAvailability,
  useOperationsLifecycleEvents,
  useOperationsOverview,
  useOperationsRateLimits,
  useOperationsWorkItems,
  usePipelineScheduleStatus,
  useProviderRuns,
} from "../api/hooks";
import type {
  DataAvailabilityParams,
  DataAvailabilityResponse,
  DataAvailabilityRow,
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

type DataTab = "overview" | "coverage" | "work" | "runs" | "lifecycle";

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
  if (normalized === "cancelled" || normalized === "stopped") return "neutral";
  return "completed";
}

function humanize(value: string): string {
  return value
    .replaceAll("_", " ")
    .replace(/\b\w/g, (letter) => letter.toUpperCase());
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
  const [lifecycleType, setLifecycleType] = useState("");
  const [lifecycleSymbol, setLifecycleSymbol] = useState("");
  const [lifecycleOffset, setLifecycleOffset] = useState(0);

  const market = markets.find((item) => item.exchange === exchange) ?? markets[0];
  const overviewQuery = useOperationsOverview(exchange);
  const rateQuery = useOperationsRateLimits();
  const schedulesQuery = usePipelineScheduleStatus(activeTab === "overview");
  const availabilityParams = useMemo<DataAvailabilityParams>(
    () => ({
      provider: "yfinance",
      exchange,
      interval: "1d",
      start_date: coverageStart,
      end_date: coverageEnd,
      query: coverageQuery.trim() || undefined,
      coverage_status: coverageStatus,
      limit: 100,
      sort: "-coverage_pct",
    }),
    [coverageEnd, coverageQuery, coverageStart, coverageStatus, exchange],
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
      symbol: workSymbol.trim() || undefined,
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
      symbol: lifecycleSymbol.trim() || undefined,
      limit: pageSize,
      offset: lifecycleOffset,
    },
    activeTab === "lifecycle",
  );

  const overview = overviewQuery.data;
  const freshness = overview?.freshness[0];
  const rate = rateQuery.data?.[0] ?? overview?.adaptive_rates[0];
  const openWork = sumQueue(overview?.queue ?? [], ["queued", "running", "retry_wait"]);
  const retryWork = sumQueue(overview?.queue ?? [], ["retry_wait"]);
  const recentFailures = (overview?.recent_runs ?? []).filter(
    (run) => statusClass(run.status) === "failed",
  ).length;
  const isHealthy =
    !overviewQuery.error &&
    rate?.circuit_state === "closed" &&
    recentFailures === 0 &&
    (freshness?.suspicious_rows ?? 0) === 0;

  function selectExchange(nextExchange: OperationsExchange) {
    setExchange(nextExchange);
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
        />
      ) : null}

      {activeTab === "lifecycle" ? (
        <LifecycleView
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
        <RecentRuns runs={overview?.recent_runs ?? []} onOpen={() => onSelectTab("runs")} />
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
    schedule.schedule_name.includes("universe_refresh") ||
    schedule.schedule_name.includes("exchange_sessions"),
  );
  return (
    <section className="data-card">
      <div className="data-card-header"><div><h2>Automation</h2><p>Expected Dagster schedule state</p></div></div>
      <div className="operations-stack-list">
        {relevant.map((schedule) => (
          <article key={schedule.schedule_name}>
            <div><strong>{humanize(schedule.schedule_name)}</strong><span>{schedule.cron_schedule} · {schedule.execution_timezone}</span></div>
            <span className={`status-pill ${statusClass(schedule.intended_status)}`}>{schedule.intended_status}</span>
          </article>
        ))}
        {!relevant.length ? <EmptyState label="No equity schedules were reported." /> : null}
      </div>
    </section>
  );
}

function RecentRuns({ runs, onOpen }: { runs: DataPipelineRunSummary[]; onOpen: () => void }) {
  return (
    <section className="data-card">
      <CardHeader title="Recent Runs" subtitle="Latest exchange-matched ingestion activity" onOpen={onOpen} />
      <div className="operations-stack-list">
        {runs.slice(0, 8).map((run) => (
          <article key={run.id}>
            <div><strong>{humanize(run.name)}</strong><span>{formatDateTime(run.started_at)} · {formatNumber(run.items_succeeded)} succeeded</span></div>
            <span className={`status-pill ${statusClass(run.status)}`}>{humanize(run.status)}</span>
          </article>
        ))}
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
            <div><strong>{row.symbol ?? row.canonical_instrument_id}</strong><span>{formatDateTime(row.created_at)} · {humanize(row.event_type)}</span></div>
            <span className={`status-pill ${statusClass(row.event_type)}`}>{humanize(row.event_type)}</span>
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
  const coverage = summary && summary.expected_rows > 0 ? summary.stored_rows / summary.expected_rows : 0;
  return (
    <>
      <div className="metric-grid data-metric-grid">
        <MetricCard icon={Users} label="Symbols" value={formatNumber(summary?.symbols_total)} detail={`${formatNumber(summary?.symbols_complete)} complete`} />
        <MetricCard icon={DatabaseZap} label="Stored Rows" value={formatNumber(summary?.stored_rows)} detail={`${formatNumber(summary?.expected_rows)} expected`} />
        <MetricCard icon={Search} label="Missing Rows" value={formatNumber(summary?.missing_rows)} detail={`${formatNumber(summary?.symbols_partial)} partial symbols`} />
        <MetricCard icon={ShieldCheck} label="Coverage" value={formatPercent(coverage)} detail={`${market.label} · ten-year target`} />
      </div>
      <section className="data-card">
        <div className="data-card-header">
          <div><h2>Exact Session Coverage</h2><p>Calendar-aware yfinance daily coverage</p></div>
          <button className="icon-button" type="button" onClick={onRefresh}><RefreshCw size={16} />{isFetching && !isLoading ? "Refreshing" : "Refresh"}</button>
        </div>
        <div className="data-filter-row operations-coverage-filters">
          <label className="data-search-field">Symbol or name<Search size={16} /><input value={query} onChange={(event) => onQueryChange(event.target.value)} placeholder={`Search ${market.label}`} /></label>
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
        <thead><tr><th>Symbol</th><th>Status</th><th>Coverage</th><th>Stored / expected</th><th>Missing</th><th>Latest</th></tr></thead>
        <tbody>{rows.map((row) => <tr key={row.instrument_key}><td><strong>{row.symbol}</strong><small>{row.name ?? row.instrument_key}</small></td><td><span className={`status-pill ${statusClass(row.coverage_status)}`}>{row.coverage_status}</span></td><td><div className="operations-coverage-cell"><div><span style={{ width: `${Math.min(row.coverage_pct * 100, 100)}%` }} /></div><strong>{formatPercent(row.coverage_pct)}</strong></div></td><td>{formatNumber(row.stored_rows)} / {formatNumber(row.expected_rows)}</td><td>{formatNumber(row.missing_rows)}</td><td>{formatDate(row.latest_stored_date)}</td></tr>)}</tbody>
      </table>
    </div>
  );
}

function WorkQueueView({
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
          <label className="data-search-field">Symbol<Search size={16} /><input value={symbol} onChange={(event) => onSymbolChange(event.target.value)} placeholder="AAPL, RELIANCE, RY.TO" /></label>
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
}: {
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
}) {
  const succeeded = runs.filter((run) => statusClass(run.status) === "completed").length;
  const failed = runs.filter((run) => statusClass(run.status) === "failed").length;
  const processed = runs.reduce((total, run) => total + run.items_processed, 0);
  return (
    <>
      <div className="metric-grid data-metric-grid">
        <MetricCard icon={History} label="Visible Runs" value={formatNumber(runs.length)} detail={`${formatNumber(failed)} failed`} />
        <MetricCard icon={TableProperties} label="Items Processed" value={formatNumber(processed)} detail="Across visible runs" />
        <MetricCard icon={ShieldCheck} label="Success Rate" value={formatPercent(runs.length ? succeeded / runs.length : 0)} detail="Visible run window" />
        <MetricCard icon={Clock3} label="Average Time" value={`${formatNumber(averageDuration(runs))}s`} detail="Finished runs" />
      </div>
      <section className="data-card">
        <div className="data-card-header"><div><h2>Ingestion Runs</h2><p>Exchange-aware history includes shared MULTI workers</p></div><button className="icon-button" type="button" onClick={onRefresh}><RefreshCw size={16} />{isFetching && !isLoading ? "Refreshing" : "Refresh"}</button></div>
        <div className="data-filter-row operations-run-filters">
          <label>Status<select value={status} onChange={(event) => onStatusChange(event.target.value)}><option value="">All</option><option value="completed">Completed</option><option value="completed_with_failures">With failures</option><option value="running">Running</option><option value="failed">Failed</option></select></label>
          <label>Start<input type="date" value={startDate} onChange={(event) => onStartDateChange(event.target.value)} /></label>
          <label>End<input type="date" value={endDate} onChange={(event) => onEndDateChange(event.target.value)} /></label>
        </div>
        {isLoading ? <LoadingState /> : null}
        {error ? <p className="form-error operations-form-error">Runs could not load: {error.message}</p> : null}
        {!isLoading && !error && !runs.length ? <EmptyState label="No ingestion runs matched this exchange and window." /> : null}
        {runs.length ? <RunsTable runs={runs} /> : null}
      </section>
    </>
  );
}

function RunsTable({ runs }: { runs: DataPipelineRunSummary[] }) {
  return (
    <div className="operations-table-wrap">
      <table className="operations-table">
        <thead><tr><th>Run</th><th>Status</th><th>Exchange scope</th><th>Processed</th><th>Failed</th><th>Duration</th><th>Started</th></tr></thead>
        <tbody>{runs.map((run) => <tr key={run.id}><td><strong>{humanize(run.name)}</strong><small>{run.id.slice(0, 8)} · {String(run.run_metadata.trigger ?? "pipeline")}</small></td><td><span className={`status-pill ${statusClass(run.status)}`}>{humanize(run.status)}</span></td><td>{run.exchange}<small>{run.work_item_exchanges.length ? run.work_item_exchanges.join(", ") : "No claimed exchange"}</small></td><td>{formatNumber(run.items_processed)}</td><td>{formatNumber(run.items_failed)}</td><td>{run.duration_seconds === null ? "Running" : `${run.duration_seconds}s`}</td><td>{formatDateTime(run.started_at)}</td></tr>)}</tbody>
      </table>
    </div>
  );
}

function LifecycleView({
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
      <div className="data-card-header"><div><h2>Universe Lifecycle</h2><p>{formatNumber(total)} additions, removals, and status transitions</p></div><button className="icon-button" type="button" onClick={onRefresh}><RefreshCw size={16} />{isFetching && !isLoading ? "Refreshing" : "Refresh"}</button></div>
      <div className="data-filter-row operations-lifecycle-filters">
        <label className="data-search-field">Symbol<Search size={16} /><input value={symbol} onChange={(event) => onSymbolChange(event.target.value)} placeholder="Search exchange symbol" /></label>
        <label>Event<select value={eventType} onChange={(event) => onEventTypeChange(event.target.value)}><option value="">All</option><option value="added">Added</option><option value="reactivated">Reactivated</option><option value="suspected_inactive">Suspected inactive</option><option value="inactive">Inactive</option><option value="renamed">Renamed</option></select></label>
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
        <tbody>{rows.map((row) => <tr key={row.event_id}><td><strong>{row.symbol ?? "Unknown symbol"}</strong><small>{row.exchange}</small></td><td><span className={`status-pill ${statusClass(row.event_type)}`}>{humanize(row.event_type)}</span></td><td>{formatDateTime(row.created_at)}</td><td>{row.snapshot_id ? row.snapshot_id.slice(0, 12) : "—"}</td><td><small className="operations-mono">{row.canonical_instrument_id}</small></td></tr>)}</tbody>
      </table>
    </div>
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
