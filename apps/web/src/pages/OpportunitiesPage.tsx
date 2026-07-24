import {
  Activity,
  ArrowUpRight,
  RefreshCw,
  RotateCcw,
  SlidersHorizontal,
  TrendingUp,
  Waves,
} from "lucide-react";
import { useCallback, useMemo, useState } from "react";

import { useDailyOpportunities } from "../api/hooks";
import type {
  DailyOpportunitiesParams,
  OpportunityDistributionMetric,
  OpportunityPercentileRange,
} from "../api/types";
import { EmptyState, LoadingState } from "../components/DataState";
import { MetricCard } from "../components/MetricCard";
import { OpportunityDistributionChart } from "../components/OpportunityDistributionChart";
import { OpportunityTable } from "../components/OpportunityTable";
import { PageHeader } from "../components/PageHeader";
import { formatPercent } from "../utils/format";
import { opportunityMetricConfig } from "../utils/opportunityMetrics";

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

export function OpportunitiesPage() {
  const [exchange, setExchange] = useState<DailyOpportunitiesParams["exchange"]>("NSE");
  const [sessionDate, setSessionDate] = useState("");
  const [symbol, setSymbol] = useState("");
  const [sortBy, setSortBy] = useState("true_range");
  const [direction, setDirection] = useState<"asc" | "desc">("desc");
  const [visibleMetrics, setVisibleMetrics] = useState<OpportunityDistributionMetric[]>([
    "session_return",
    "recovery",
    "upside",
    "downside",
  ]);
  const [percentileFilters, setPercentileFilters] = useState<
    Partial<Record<OpportunityDistributionMetric, OpportunityPercentileRange>>
  >({});

  const updateRange = useCallback(
    (metric: OpportunityDistributionMetric, nextRange: OpportunityPercentileRange) => {
      setPercentileFilters((current) => {
        const next = { ...current };
        const normalized = normalizedRange(nextRange);
        if (normalized) next[metric] = normalized;
        else delete next[metric];
        return next;
      });
    },
    [],
  );

  const query = useDailyOpportunities({
    exchange,
    sessionDate: sessionDate || undefined,
    symbol: symbol.trim() || undefined,
    sortBy,
    direction,
    limit: 200,
    percentileFilters,
  });
  const payload = query.data;
  const summary = payload?.summary ?? {};
  const activeFilterCount = Object.keys(percentileFilters).length;

  const shownDistributions = useMemo(
    () =>
      visibleMetrics.flatMap((metric) => {
        const distribution = payload?.distributions[metric];
        return distribution ? [distribution] : [];
      }),
    [payload?.distributions, visibleMetrics],
  );

  const toggleMetric = (metric: OpportunityDistributionMetric) => {
    setVisibleMetrics((current) => {
      if (current.includes(metric)) {
        return current.length === 1 ? current : current.filter((item) => item !== metric);
      }
      return distributionMetrics.filter((item) => item === metric || current.includes(item));
    });
  };

  return (
    <>
      <PageHeader
        eyebrow="Opportunities"
        title="Explore completed-session opportunities"
        subtitle="Compare the full market distribution, combine percentile bands, and drill into matching symbols. These are realized historical outcomes—not forecasts."
        actions={
          <button className="icon-button" type="button" onClick={() => void query.refetch()}>
            <RefreshCw size={16} />
            Refresh
          </button>
        }
      />

      <section className="opportunity-notice" aria-label="Target timing notice">
        <Activity size={18} />
        <div>
          <strong>Realized session data</strong>
          <span>
            High, low, and close are only known during or after the session. Filters rank each
            symbol against the complete selected market session.
          </span>
        </div>
      </section>

      {payload?.latest_available_date &&
      payload.session_date !== payload.latest_available_date ? (
        <section
          className="opportunity-notice opportunity-coverage-notice"
          aria-label="Coverage notice"
        >
          <Waves size={18} />
          <div>
            <strong>Showing the latest complete session</strong>
            <span>
              {payload.latest_available_date} is still partial, so rankings remain on{" "}
              {payload.session_date}.
              {` Coverage: ${payload.session_instruments.toLocaleString()} of approximately ${payload.expected_instruments.toLocaleString()} instruments.`}
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
              onChange={(event) =>
                setExchange(event.target.value as DailyOpportunitiesParams["exchange"])
              }
            >
              <option value="NSE">NSE</option>
              <option value="TSX">TSX</option>
              <option value="US">US</option>
            </select>
          </label>
          <label>
            Session date
            <input
              type="date"
              value={sessionDate}
              onChange={(event) => setSessionDate(event.target.value)}
            />
          </label>
          <label>
            Symbol
            <input
              value={symbol}
              onChange={(event) => setSymbol(event.target.value)}
              placeholder="Search symbol"
            />
          </label>
          <label>
            Rank by
            <select value={sortBy} onChange={(event) => setSortBy(event.target.value)}>
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
              onChange={(event) => setDirection(event.target.value as "asc" | "desc")}
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
            payload?.session_date
              ? `of ${(payload.session_total ?? 0).toLocaleString()} · ${exchange} · ${payload.session_date}`
              : "No computed session"
          }
        />
        <MetricCard
          icon={TrendingUp}
          label="Positive return"
          value={formatPercent(summary.positive_session_ratio, 1)}
          detail={`${summary.positive_sessions ?? 0} matching symbols closed above open`}
        />
        <MetricCard
          icon={ArrowUpRight}
          label="Average upside"
          value={formatPercent(summary.average_upside)}
          detail="Across the filtered result set"
        />
        <MetricCard
          icon={Waves}
          label="Average true range"
          value={formatPercent(summary.average_true_range)}
          detail={`${formatPercent(payload?.coverage_ratio, 1)} session coverage`}
        />
      </div>

      <section className="panel opportunity-explorer-panel">
        <div className="panel-header opportunity-explorer-header">
          <div>
            <h2>Distribution explorer</h2>
            <span>Choose any combination of figures. Filters combine across them.</span>
          </div>
          <button
            className="opportunity-reset-button"
            type="button"
            onClick={() => setPercentileFilters({})}
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
          <EmptyState label="No distribution data is available for this session." />
        )}
      </section>

      <section className="panel">
        <div className="panel-header opportunity-results-header">
          <div>
            <h2>Matching opportunities</h2>
            <span>
              {(payload?.total ?? 0).toLocaleString()} symbols match · showing{" "}
              {(payload?.rows.length ?? 0).toLocaleString()}
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
        ) : payload?.rows.length ? (
          <OpportunityTable data={payload.rows} />
        ) : (
          <EmptyState label="No symbols match the selected percentile bands." />
        )}
      </section>
    </>
  );
}
