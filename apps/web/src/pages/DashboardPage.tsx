import { AlertTriangle, Database, SearchCheck, ShieldCheck } from "lucide-react";

import { useDailyOpportunities, useMarketStatus } from "../api/hooks";
import { EmptyState, LoadingState } from "../components/DataState";
import { FeatureChart } from "../components/FeatureChart";
import { MarketStatusTable } from "../components/MarketStatusTable";
import { MetricCard } from "../components/MetricCard";
import { PageHeader } from "../components/PageHeader";

export function DashboardPage() {
  const statusQuery = useMarketStatus();
  const opportunityQuery = useDailyOpportunities({ exchange: "NSE", limit: 20 });
  const status = statusQuery.data ?? [];
  const results = opportunityQuery.data?.rows ?? [];
  const totalUniverse = status.reduce((sum, item) => sum + item.universeSize, 0);
  const staleSymbols = status.reduce((sum, item) => sum + item.staleSymbols, 0);

  return (
    <>
      <PageHeader
        eyebrow="Operations"
        title="Market Research Console"
        subtitle="Data freshness, completed-session Opportunity analytics, and research readiness."
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
          label="Opportunities"
          value={(opportunityQuery.data?.total ?? 0).toLocaleString()}
          detail="Latest completed NSE session"
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
          <h2>Opportunity Shape</h2>
        </div>
        {opportunityQuery.isLoading ? (
          <LoadingState />
        ) : opportunityQuery.isError ? (
          <EmptyState label="Opportunity analytics are unavailable." />
        ) : results.length ? (
          <FeatureChart results={results} />
        ) : (
          <EmptyState label="No completed-session Opportunity data available." />
        )}
      </section>
    </>
  );
}
