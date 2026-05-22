import { AlertTriangle, Database, SearchCheck, ShieldCheck } from "lucide-react";

import { useMarketStatus, useScreenerResults } from "../api/hooks";
import { EmptyState, LoadingState } from "../components/DataState";
import { FeatureChart } from "../components/FeatureChart";
import { MarketStatusTable } from "../components/MarketStatusTable";
import { MetricCard } from "../components/MetricCard";
import { PageHeader } from "../components/PageHeader";

export function DashboardPage() {
  const statusQuery = useMarketStatus();
  const screenerQuery = useScreenerResults();
  const status = statusQuery.data ?? [];
  const results = screenerQuery.data ?? [];
  const totalUniverse = status.reduce((sum, item) => sum + item.universeSize, 0);
  const staleSymbols = status.reduce((sum, item) => sum + item.staleSymbols, 0);

  return (
    <>
      <PageHeader
        eyebrow="Operations"
        title="Market Research Console"
        subtitle="Data freshness, screener activity, and agent-ready research signals."
      />
      <div className="metric-grid">
        <MetricCard
          icon={Database}
          label="Universe"
          value={totalUniverse.toLocaleString()}
          detail="NSE + TSX tracked symbols"
        />
        <MetricCard
          icon={SearchCheck}
          label="Signals"
          value={results.length.toString()}
          detail="Latest intraday-range matches"
        />
        <MetricCard
          icon={ShieldCheck}
          label="Quality"
          value={`${(status[0]?.dataQualityScore ?? 0).toFixed(1)}%`}
          detail="Top exchange quality score"
        />
        <MetricCard
          icon={AlertTriangle}
          label="Stale"
          value={staleSymbols.toString()}
          detail="Symbols needing inspection"
        />
      </div>

      <section className="panel">
        <div className="panel-header">
          <h2>Exchange Status</h2>
        </div>
        {statusQuery.isLoading ? <LoadingState /> : <MarketStatusTable data={status} />}
      </section>

      <section className="panel">
        <div className="panel-header">
          <h2>Signal Shape</h2>
        </div>
        {screenerQuery.isLoading ? (
          <LoadingState />
        ) : results.length ? (
          <FeatureChart results={results} />
        ) : (
          <EmptyState label="No signals available." />
        )}
      </section>
    </>
  );
}
