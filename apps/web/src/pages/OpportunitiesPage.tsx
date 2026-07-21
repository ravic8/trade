import { Activity, ArrowUpRight, RefreshCw, TrendingUp, Waves } from "lucide-react";
import { useState } from "react";

import { useDailyOpportunities } from "../api/hooks";
import type { DailyOpportunitiesParams } from "../api/types";
import { EmptyState, LoadingState } from "../components/DataState";
import { MetricCard } from "../components/MetricCard";
import { OpportunityTable } from "../components/OpportunityTable";
import { PageHeader } from "../components/PageHeader";
import { formatPercent } from "../utils/format";

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

export function OpportunitiesPage() {
  const [exchange, setExchange] = useState<DailyOpportunitiesParams["exchange"]>("NSE");
  const [sessionDate, setSessionDate] = useState("");
  const [symbol, setSymbol] = useState("");
  const [sortBy, setSortBy] = useState("true_range");
  const [direction, setDirection] = useState<"asc" | "desc">("desc");
  const query = useDailyOpportunities({
    exchange,
    sessionDate: sessionDate || undefined,
    symbol: symbol.trim() || undefined,
    sortBy,
    direction,
    limit: 200,
  });
  const payload = query.data;
  const summary = payload?.summary ?? {};

  return (
    <>
      <PageHeader
        eyebrow="Opportunities"
        title="Completed-session target analytics"
        subtitle="Realized OHLC outcomes using the exact Target Variables definitions. These are historical analytics, not pre-session forecasts."
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
            High, low, and close are only known during or after the session. A future Forecast view
            will use lagged inputs and remain separate to prevent leakage.
          </span>
        </div>
      </section>

      {payload?.latest_available_date &&
      payload.session_date !== payload.latest_available_date ? (
        <section className="opportunity-notice opportunity-coverage-notice" aria-label="Coverage notice">
          <Waves size={18} />
          <div>
            <strong>Showing the latest complete session</strong>
            <span>
              {payload.latest_available_date} is still partial, so rankings remain on {payload.session_date}.
              {` Coverage: ${payload.session_instruments.toLocaleString()} of approximately ${payload.expected_instruments.toLocaleString()} instruments.`}
            </span>
          </div>
        </section>
      ) : null}

      <section className="panel opportunity-filter-panel">
        <div className="opportunity-filters">
          <label>
            Exchange
            <select value={exchange} onChange={(event) => setExchange(event.target.value as DailyOpportunitiesParams["exchange"])}>
              <option value="NSE">NSE</option>
              <option value="TSX">TSX</option>
              <option value="US">US</option>
            </select>
          </label>
          <label>
            Session date
            <input type="date" value={sessionDate} onChange={(event) => setSessionDate(event.target.value)} />
          </label>
          <label>
            Symbol
            <input value={symbol} onChange={(event) => setSymbol(event.target.value)} placeholder="Search symbol" />
          </label>
          <label>
            Rank by
            <select value={sortBy} onChange={(event) => setSortBy(event.target.value)}>
              {sortOptions.map(([value, label]) => <option key={value} value={value}>{label}</option>)}
            </select>
          </label>
          <label>
            Direction
            <select value={direction} onChange={(event) => setDirection(event.target.value as "asc" | "desc")}>
              <option value="desc">Highest first</option>
              <option value="asc">Lowest first</option>
            </select>
          </label>
        </div>
      </section>

      <div className="metric-grid">
        <MetricCard
          icon={Activity}
          label="Rows"
          value={(payload?.total ?? 0).toLocaleString()}
          detail={payload?.session_date ? `${exchange} · ${payload.session_date} · ${formatPercent(payload.coverage_ratio, 1)} coverage` : "No computed session"}
        />
        <MetricCard
          icon={TrendingUp}
          label="Positive return"
          value={formatPercent(summary.positive_session_ratio, 1)}
          detail={`${summary.positive_sessions ?? 0} symbols closed above open`}
        />
        <MetricCard
          icon={ArrowUpRight}
          label="Average upside"
          value={formatPercent(summary.average_upside)}
          detail="(High − Open) / Open"
        />
        <MetricCard
          icon={Waves}
          label="Average true range"
          value={formatPercent(summary.average_true_range)}
          detail="True Upside + True Downside"
        />
      </div>

      <section className="panel">
        <div className="panel-header">
          <h2>Target variables</h2>
          <span className="opportunity-version">{payload?.target_version ?? "daily_opportunity_outcomes_v1_0"}</span>
        </div>
        {query.isLoading ? (
          <LoadingState />
        ) : query.isError ? (
          <EmptyState label={`Opportunity data could not be loaded: ${query.error.message}`} />
        ) : payload?.rows.length ? (
          <OpportunityTable data={payload.rows} />
        ) : (
          <EmptyState label="No Opportunity targets have been computed for this selection." />
        )}
      </section>
    </>
  );
}
