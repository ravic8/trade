import {
  Activity,
  AlertTriangle,
  ArrowLeft,
  ArrowRight,
  ArrowUpRight,
  LoaderCircle,
  RefreshCw,
  RotateCcw,
  SlidersHorizontal,
  TrendingUp,
  Waves,
} from "lucide-react";
import {
  useCallback,
  useDeferredValue,
  useMemo,
  useRef,
  useState,
} from "react";

import { useDailyOpportunities } from "../api/hooks";
import type {
  DailyOpportunitiesParams,
  OpportunityDistributionMetric,
  OpportunityPercentileRange,
  OpportunityReturnRange,
  OpportunitySession,
} from "../api/types";
import { EmptyState, LoadingState } from "../components/DataState";
import { MetricCard } from "../components/MetricCard";
import { OpportunityDistributionChart } from "../components/OpportunityDistributionChart";
import { OpportunityTable } from "../components/OpportunityTable";
import { PageHeader } from "../components/PageHeader";
import { formatPercent } from "../utils/format";
import { opportunityMetricConfig } from "../utils/opportunityMetrics";

const pageSize = 100;
const distributionMetrics: OpportunityDistributionMetric[] = [
  "session_return",
  "recovery",
  "upside",
  "downside",
  "giveback",
  "true_range",
];

const sortOptions = [
  ["true_range", "True Range"],
  ["upside", "Upside"],
  ["downside", "Downside"],
  ["session_return", "Return"],
  ["gap", "Gap"],
  ["true_return", "True Return"],
  ["true_upside", "True Upside"],
  ["true_downside", "True Downside"],
  ["giveback", "Giveback"],
  ["recovery", "Recovery"],
];

function normalizedRange(range: OpportunityPercentileRange) {
  const minimum = range.minimum === 0 ? undefined : range.minimum;
  const maximum = range.maximum === 100 ? undefined : range.maximum;
  return minimum == null && maximum == null ? undefined : { minimum, maximum };
}

function formatSessionDate(value: string) {
  return new Intl.DateTimeFormat(undefined, {
    day: "numeric",
    month: "short",
    year: "numeric",
    timeZone: "UTC",
  }).format(new Date(`${value}T00:00:00Z`));
}

function sessionOptionLabel(session: OpportunitySession) {
  const coverage =
    session.coverage_status === "partial"
      ? ` · partial ${formatPercent(session.coverage_ratio, 0)}`
      : "";
  return `${formatSessionDate(session.date)} · ${session.instruments.toLocaleString()} symbols${coverage}`;
}

function sessionAgeInCalendarDays(value: string | null | undefined) {
  if (!value) return null;
  const sessionTime = Date.parse(`${value}T00:00:00Z`);
  const now = new Date();
  const todayTime = Date.UTC(now.getUTCFullYear(), now.getUTCMonth(), now.getUTCDate());
  return Math.max(0, Math.floor((todayTime - sessionTime) / 86_400_000));
}

function parseOptionalPercent(value: string) {
  if (!value.trim()) return undefined;
  const parsed = Number(value);
  return Number.isFinite(parsed) ? parsed : undefined;
}

function resolveReturnRange(
  minimum: string,
  maximum: string,
): { range: OpportunityReturnRange; error: string | null } {
  const minimumPercent = parseOptionalPercent(minimum);
  const maximumPercent = parseOptionalPercent(maximum);
  if (
    minimumPercent != null &&
    maximumPercent != null &&
    minimumPercent > maximumPercent
  ) {
    return {
      range: {},
      error: "Minimum cannot exceed maximum",
    };
  }
  return {
    range: { minimumPercent, maximumPercent },
    error: null,
  };
}

function formatReturnBound(value: number) {
  return `${new Intl.NumberFormat(undefined, {
    maximumFractionDigits: 2,
  }).format(value)}%`;
}

function describeReturnRange(range: OpportunityReturnRange) {
  if (range.minimumPercent != null && range.maximumPercent != null) {
    return `${formatReturnBound(range.minimumPercent)} to ${formatReturnBound(range.maximumPercent)} · inclusive`;
  }
  if (range.minimumPercent != null) {
    return `${formatReturnBound(range.minimumPercent)} or more · inclusive`;
  }
  if (range.maximumPercent != null) {
    return `${formatReturnBound(range.maximumPercent)} or less · inclusive`;
  }
  return "All reported returns";
}

export function OpportunitiesPage() {
  const [exchange, setExchange] = useState<DailyOpportunitiesParams["exchange"]>("NSE");
  const [sessionDate, setSessionDate] = useState("");
  const [symbol, setSymbol] = useState("");
  const deferredSymbol = useDeferredValue(symbol.trim());
  const [returnMinimum, setReturnMinimum] = useState("1");
  const [returnMaximum, setReturnMaximum] = useState("");
  const deferredReturnMinimum = useDeferredValue(returnMinimum);
  const deferredReturnMaximum = useDeferredValue(returnMaximum);
  const [sortBy, setSortBy] = useState("true_range");
  const [direction, setDirection] = useState<"asc" | "desc">("desc");
  const [offset, setOffset] = useState(0);
  const resultsRef = useRef<HTMLElement | null>(null);
  const [visibleMetrics, setVisibleMetrics] = useState<OpportunityDistributionMetric[]>([
    "session_return",
    "recovery",
    "upside",
    "downside",
  ]);
  const [percentileFilters, setPercentileFilters] = useState<
    Partial<Record<OpportunityDistributionMetric, OpportunityPercentileRange>>
  >({});
  const returnRange = useMemo(
    () => resolveReturnRange(returnMinimum, returnMaximum),
    [returnMaximum, returnMinimum],
  );
  const deferredReturnRange = useMemo(
    () => resolveReturnRange(deferredReturnMinimum, deferredReturnMaximum),
    [deferredReturnMaximum, deferredReturnMinimum],
  );

  const updateRange = useCallback(
    (metric: OpportunityDistributionMetric, nextRange: OpportunityPercentileRange) => {
      setPercentileFilters((current) => {
        const next = { ...current };
        const normalized = normalizedRange(nextRange);
        if (normalized) next[metric] = normalized;
        else delete next[metric];
        return next;
      });
      setOffset(0);
    },
    [],
  );

  const query = useDailyOpportunities(
    {
      exchange,
      sessionDate: sessionDate || undefined,
      symbol: deferredSymbol || undefined,
      sortBy,
      direction,
      limit: pageSize,
      offset,
      sessionReturnRange: deferredReturnRange.range,
      percentileFilters,
    },
    !deferredReturnRange.error,
  );
  const payload = query.data;
  const summary = payload?.summary ?? {};
  const activeFilterCount = Object.keys(percentileFilters).length;
  const returnBandUpdating =
    deferredReturnMinimum !== returnMinimum ||
    deferredReturnMaximum !== returnMaximum ||
    query.isFetching;
  const filtersUpdating =
    deferredSymbol !== symbol.trim() || returnBandUpdating;

  const shownDistributions = useMemo(
    () =>
      visibleMetrics.flatMap((metric) => {
        const distribution = payload?.distributions[metric];
        return distribution ? [distribution] : [];
      }),
    [payload?.distributions, visibleMetrics],
  );

  const latestSession = payload?.available_sessions.find(
    (session) => session.date === payload.latest_available_date,
  );
  const automaticPartialFallback =
    payload?.selection_mode === "automatic" &&
    payload.session_exists &&
    payload.latest_available_date &&
    payload.session_date &&
    payload.latest_available_date > payload.session_date;
  const explicitUnavailable =
    payload?.selection_mode === "explicit" && !payload.session_exists;
  const explicitPartial =
    payload?.selection_mode === "explicit" &&
    payload.session_exists &&
    payload.coverage_status === "partial";
  const latestSessionAge = sessionAgeInCalendarDays(payload?.latest_available_date);

  const toggleMetric = (metric: OpportunityDistributionMetric) => {
    setVisibleMetrics((current) => {
      if (current.includes(metric)) {
        return current.length === 1 ? current : current.filter((item) => item !== metric);
      }
      return distributionMetrics.filter((item) => item === metric || current.includes(item));
    });
  };

  const changePage = (nextOffset: number) => {
    setOffset(nextOffset);
    requestAnimationFrame(() =>
      resultsRef.current?.scrollIntoView({ behavior: "smooth", block: "start" }),
    );
  };

  return (
    <>
      <PageHeader
        eyebrow="Opportunities"
        title="Explore completed-session opportunities"
        subtitle="Compare the full market distribution, combine percentile bands, and drill into matching symbols. These are realized historical outcomes—not forecasts."
        actions={
          <button
            className="icon-button"
            type="button"
            onClick={() => void query.refetch()}
            disabled={query.isFetching}
          >
            <RefreshCw className={query.isFetching ? "spin" : ""} size={16} />
            {query.isFetching ? "Refreshing" : "Refresh"}
          </button>
        }
      />

      <section className="opportunity-notice" aria-label="Target timing notice">
        <Activity size={18} />
        <div>
          <strong>Realized session data · yfinance</strong>
          <span>
            High, low, and close are only known during or after the session. Filters rank each
            symbol against the complete selected market session.
          </span>
        </div>
      </section>

      {automaticPartialFallback ? (
        <section
          className="opportunity-notice opportunity-coverage-notice"
          aria-label="Coverage notice"
        >
          <Waves size={18} />
          <div>
            <strong>Showing the latest complete session</strong>
            <span>
              {formatSessionDate(payload.latest_available_date!)} is still being populated
              {latestSession
                ? ` (${latestSession.instruments.toLocaleString()} of approximately ${latestSession.expected_instruments.toLocaleString()} symbols)`
                : ""}
              , so analytics remain on {formatSessionDate(payload.session_date!)}.
            </span>
          </div>
        </section>
      ) : null}

      {explicitPartial ? (
        <section
          className="opportunity-notice opportunity-coverage-notice"
          aria-label="Partial selected session"
        >
          <AlertTriangle size={18} />
          <div>
            <strong>Selected session is partial</strong>
            <span>
              Showing {payload.session_instruments.toLocaleString()} of approximately{" "}
              {payload.expected_instruments.toLocaleString()} symbols for{" "}
              {formatSessionDate(payload.session_date!)}. Rankings may move as more rows arrive.
            </span>
          </div>
        </section>
      ) : null}

      {explicitUnavailable ? (
        <section
          className="opportunity-notice opportunity-unavailable-notice"
          aria-label="Unavailable selected session"
        >
          <AlertTriangle size={18} />
          <div>
            <strong>No computed Opportunity data for {formatSessionDate(payload.session_date!)}</strong>
            <span>
              {payload.latest_available_date
                ? `The latest available session is ${formatSessionDate(payload.latest_available_date)}.`
                : "No sessions are available for this exchange yet."}
            </span>
          </div>
          {payload.latest_complete_date ? (
            <button type="button" onClick={() => setSessionDate("")}>
              Use latest complete
            </button>
          ) : null}
        </section>
      ) : null}

      {latestSessionAge != null && latestSessionAge >= 4 ? (
        <section
          className="opportunity-notice opportunity-quality-notice"
          aria-label="Stale Opportunity data notice"
        >
          <AlertTriangle size={18} />
          <div>
            <strong>Opportunity targets may be behind</strong>
            <span>
              The latest computed {payload?.exchange} session is{" "}
              {formatSessionDate(payload!.latest_available_date!)} ({latestSessionAge} calendar
              days ago). Check the daily target pipeline if newer market sessions should exist.
            </span>
          </div>
        </section>
      ) : null}

      {summary.quality_warning_sessions ? (
        <section
          className="opportunity-notice opportunity-quality-notice"
          aria-label="Opportunity data quality notice"
        >
          <AlertTriangle size={18} />
          <div>
            <strong>
              {summary.quality_warning_sessions.toLocaleString()} matching{" "}
              {summary.quality_warning_sessions === 1 ? "row has" : "rows have"} incomplete target
              inputs
            </strong>
            <span>
              Missing previous-close metrics are shown as unavailable and excluded from their
              distributions.
            </span>
          </div>
        </section>
      ) : null}

      <section className="panel opportunity-filter-panel">
        <div className="opportunity-filters">
          <label>
            Exchange
            <select
              value={exchange}
              onChange={(event) => {
                setExchange(event.target.value as DailyOpportunitiesParams["exchange"]);
                setSessionDate("");
                setOffset(0);
              }}
            >
              <option value="NSE">NSE</option>
              <option value="TSX">TSX</option>
              <option value="US">US</option>
            </select>
          </label>
          <label>
            Session
            <select
              value={sessionDate}
              onChange={(event) => {
                setSessionDate(event.target.value);
                setOffset(0);
              }}
              disabled={!payload?.available_sessions.length}
            >
              <option value="">
                {payload?.latest_complete_date
                  ? `Latest complete · ${formatSessionDate(payload.latest_complete_date)}`
                  : "Latest complete"}
              </option>
              {sessionDate &&
              !payload?.available_sessions.some((session) => session.date === sessionDate) ? (
                <option value={sessionDate} disabled>
                  {formatSessionDate(sessionDate)} · unavailable
                </option>
              ) : null}
              {payload?.available_sessions.map((session) => (
                <option key={session.date} value={session.date}>
                  {sessionOptionLabel(session)}
                </option>
              ))}
            </select>
          </label>
          <label>
            Symbol
            <input
              value={symbol}
              onChange={(event) => {
                setSymbol(event.target.value);
                setOffset(0);
              }}
              placeholder="Search symbol"
              inputMode="search"
            />
          </label>
          <label>
            Rank by
            <select
              value={sortBy}
              onChange={(event) => {
                setSortBy(event.target.value);
                setOffset(0);
              }}
            >
              {sortOptions.map(([value, label]) => (
                <option key={value} value={value}>
                  {label}
                </option>
              ))}
            </select>
          </label>
          <label>
            Direction
            <select
              value={direction}
              onChange={(event) => {
                setDirection(event.target.value as "asc" | "desc");
                setOffset(0);
              }}
            >
              <option value="desc">Highest first</option>
              <option value="asc">Lowest first</option>
            </select>
          </label>
        </div>
      </section>

      <div className="metric-grid">
        <MetricCard
          icon={Activity}
          label="Matches"
          value={(payload?.total ?? 0).toLocaleString()}
          detail={
            payload?.session_exists && payload.session_date
              ? `of ${payload.session_total.toLocaleString()} · ${payload.exchange} · ${formatSessionDate(payload.session_date)}`
              : "No computed session"
          }
        />
        <ReturnBandMetricCard
          minimum={returnMinimum}
          maximum={returnMaximum}
          onMinimumChange={setReturnMinimum}
          onMaximumChange={setReturnMaximum}
          value={
            returnRange.error
              ? "—"
              : returnBandUpdating
                ? "…"
                : (summary.return_band_sessions ?? 0).toLocaleString()
          }
          detail={returnRange.error ?? describeReturnRange(returnRange.range)}
          invalid={Boolean(returnRange.error)}
        />
        <MetricCard
          icon={ArrowUpRight}
          label="Median upside"
          value={formatPercent(summary.median_upside)}
          detail="50th percentile of matching stocks"
        />
        <MetricCard
          icon={Waves}
          label="Median true range"
          value={formatPercent(summary.median_true_range)}
          detail={
            payload?.session_exists
              ? `${formatPercent(payload.coverage_ratio, 1)} session coverage`
              : "No session coverage"
          }
        />
      </div>

      <section className="panel opportunity-explorer-panel" aria-busy={filtersUpdating}>
        <div className="panel-header opportunity-explorer-header">
          <div>
            <h2>Distribution explorer</h2>
            <span>Choose any combination of figures. Filters combine across them.</span>
          </div>
          <button
            className="opportunity-reset-button"
            type="button"
            onClick={() => {
              setPercentileFilters({});
              setOffset(0);
            }}
            disabled={activeFilterCount === 0}
          >
            <RotateCcw size={15} />
            Reset filters {activeFilterCount ? `(${activeFilterCount})` : ""}
          </button>
        </div>

        <div className="opportunity-metric-picker" aria-label="Visible distribution figures">
          <span>
            <SlidersHorizontal size={15} />
            Figures
          </span>
          {distributionMetrics.map((metric) => (
            <button
              key={metric}
              className={visibleMetrics.includes(metric) ? "active" : ""}
              type="button"
              aria-pressed={visibleMetrics.includes(metric)}
              onClick={() => toggleMetric(metric)}
            >
              {opportunityMetricConfig[metric].label}
            </button>
          ))}
          <button
            type="button"
            onClick={() => setVisibleMetrics(distributionMetrics)}
            disabled={visibleMetrics.length === distributionMetrics.length}
          >
            Select all
          </button>
        </div>

        {query.isLoading ? (
          <LoadingState />
        ) : query.isError ? (
          <EmptyState label={`Opportunity data could not be loaded: ${query.error.message}`} />
        ) : !payload?.session_exists ? (
          <EmptyState label="Choose an available session to explore its distributions." />
        ) : shownDistributions.length ? (
          <div className="opportunity-chart-grid">
            {shownDistributions.map((distribution) => (
              <OpportunityDistributionChart
                key={distribution.metric}
                distribution={distribution}
                range={percentileFilters[distribution.metric] ?? {}}
                onRangeChange={(range) => updateRange(distribution.metric, range)}
                onRemove={() => toggleMetric(distribution.metric)}
              />
            ))}
          </div>
        ) : (
          <EmptyState label="No valid distribution values are available for this session." />
        )}
      </section>

      <section className="panel" ref={resultsRef} aria-busy={filtersUpdating}>
        <div className="panel-header opportunity-results-header">
          <div>
            <h2>Matching opportunities</h2>
            <span>
              {filtersUpdating ? (
                <>
                  <LoaderCircle className="spin" size={13} /> Updating results
                </>
              ) : (
                <>
                  {(payload?.total ?? 0).toLocaleString()} symbols match · showing{" "}
                  {(payload?.rows.length ?? 0).toLocaleString()}
                </>
              )}
            </span>
          </div>
          <span className="opportunity-version">
            {payload?.target_version ?? "daily_opportunity_outcomes_v1_0"}
          </span>
        </div>
        {query.isLoading ? (
          <LoadingState />
        ) : query.isError ? (
          <EmptyState label={`Opportunity data could not be loaded: ${query.error.message}`} />
        ) : !payload?.session_exists ? (
          <EmptyState label="No results are available for the selected session." />
        ) : payload.rows.length ? (
          <>
            <OpportunityTable data={payload.rows} />
            <OpportunityPagination
              total={payload.total}
              offset={offset}
              onChange={changePage}
            />
          </>
        ) : (
          <EmptyState label="No symbols match the current search and percentile filters." />
        )}
      </section>
    </>
  );
}

function ReturnBandMetricCard({
  minimum,
  maximum,
  onMinimumChange,
  onMaximumChange,
  value,
  detail,
  invalid,
}: {
  minimum: string;
  maximum: string;
  onMinimumChange: (value: string) => void;
  onMaximumChange: (value: string) => void;
  value: string;
  detail: string;
  invalid: boolean;
}) {
  return (
    <section className="metric-card return-band-card">
      <div className="metric-icon">
        <TrendingUp size={18} />
      </div>
      <span>Stocks in return range</span>
      <strong>{value}</strong>
      <div className="return-band-controls" aria-label="Inclusive return range">
        <label>
          <span>Min %</span>
          <input
            type="number"
            inputMode="decimal"
            step="0.1"
            value={minimum}
            onChange={(event) => onMinimumChange(event.target.value)}
            placeholder="No min"
            aria-label="Minimum return percent"
          />
        </label>
        <label>
          <span>Max %</span>
          <input
            type="number"
            inputMode="decimal"
            step="0.1"
            value={maximum}
            onChange={(event) => onMaximumChange(event.target.value)}
            placeholder="No max"
            aria-label="Maximum return percent"
          />
        </label>
      </div>
      <small className={invalid ? "return-band-error" : undefined}>{detail}</small>
    </section>
  );
}

function OpportunityPagination({
  total,
  offset,
  onChange,
}: {
  total: number;
  offset: number;
  onChange: (offset: number) => void;
}) {
  if (total <= pageSize) return null;
  const first = offset + 1;
  const last = Math.min(offset + pageSize, total);
  return (
    <nav className="opportunity-pagination" aria-label="Opportunity result pages">
      <span>
        {first.toLocaleString()}–{last.toLocaleString()} of {total.toLocaleString()}
      </span>
      <div>
        <button
          className="icon-button"
          type="button"
          disabled={offset === 0}
          onClick={() => onChange(Math.max(0, offset - pageSize))}
        >
          <ArrowLeft size={15} />
          Previous
        </button>
        <button
          className="icon-button"
          type="button"
          disabled={offset + pageSize >= total}
          onClick={() => onChange(offset + pageSize)}
        >
          Next
          <ArrowRight size={15} />
        </button>
      </div>
    </nav>
  );
}
