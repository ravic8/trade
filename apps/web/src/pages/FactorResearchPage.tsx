import { ArrowDownWideNarrow, BarChart3, Percent, Sigma } from "lucide-react";
import { useState } from "react";

import { useFactorIC, useFactorSummary } from "../api/hooks";
import type { FactorICRow } from "../api/types";
import { EmptyState, LoadingState } from "../components/DataState";
import { MetricCard } from "../components/MetricCard";
import { PageHeader } from "../components/PageHeader";

const targetOptions = [
  "forward_ret_1d",
  "forward_ret_5d",
  "forward_ret_10d",
  "forward_ret_20d",
  "forward_ret_60d",
];

function formatDecimal(value: number | null, digits = 4): string {
  return value === null || Number.isNaN(value) ? "n/a" : value.toFixed(digits);
}

function formatPercent(value: number | null): string {
  return value === null || Number.isNaN(value) ? "n/a" : `${(value * 100).toFixed(1)}%`;
}

function featureFamily(feature: string): string {
  if (feature.startsWith("ret_") || feature.startsWith("log_ret")) return "Momentum";
  if (feature.includes("sma") || feature.includes("ema")) return "Trend";
  if (feature.startsWith("volatility") || feature.startsWith("atr") || feature === "true_range") {
    return "Risk";
  }
  if (feature.startsWith("volume") || feature.startsWith("turnover")) return "Liquidity";
  return "Other";
}

export function FactorResearchPage() {
  const [target, setTarget] = useState("forward_ret_20d");
  const [sort, setSort] = useState("mean_rank_ic");
  const factorSummaryQuery = useFactorSummary();
  const factorICQuery = useFactorIC({ target, sort, direction: "desc", limit: 100 });
  const summary = factorSummaryQuery.data?.summary ?? null;
  const rows = factorICQuery.data?.rows ?? [];
  const topRows = rows.slice(0, 10);

  return (
    <>
      <PageHeader
        eyebrow="Factor Research"
        title="Feature Evidence"
        subtitle="First-pass IC, rank IC, and stability evidence from frozen features and targets."
      />
      <div className="metric-grid">
        <MetricCard
          icon={Sigma}
          label="Joined Rows"
          value={summary?.row_count.toLocaleString() ?? "0"}
          detail={`${summary?.symbol_count ?? 0} symbols`}
        />
        <MetricCard
          icon={BarChart3}
          label="Features"
          value={summary?.feature_count.toString() ?? "0"}
          detail={`${summary?.return_target_count ?? 0} target horizons`}
        />
        <MetricCard
          icon={ArrowDownWideNarrow}
          label="IC Rows"
          value={summary?.ic_rows.toLocaleString() ?? "0"}
          detail="Feature-target pairs"
        />
        <MetricCard
          icon={Percent}
          label="Monthly"
          value={summary?.monthly_stability_rows.toLocaleString() ?? "0"}
          detail="Stability rows"
        />
      </div>

      <section className="panel">
        <div className="panel-header">
          <h2>Top Features</h2>
          <div className="inline-controls">
            <label>
              Target
              <select value={target} onChange={(event) => setTarget(event.target.value)}>
                {targetOptions.map((option) => (
                  <option value={option} key={option}>
                    {option}
                  </option>
                ))}
              </select>
            </label>
            <label>
              Sort
              <select value={sort} onChange={(event) => setSort(event.target.value)}>
                <option value="mean_rank_ic">Mean rank IC</option>
                <option value="rank_ic_t_stat">Rank IC t-stat</option>
                <option value="positive_rank_ic_pct">Positive rank IC %</option>
                <option value="mean_ic">Mean IC</option>
              </select>
            </label>
          </div>
        </div>
        {factorICQuery.isLoading ? (
          <LoadingState />
        ) : topRows.length ? (
          <FactorICTable rows={topRows} />
        ) : (
          <EmptyState label="No factor IC rows available." />
        )}
      </section>

      <section className="panel">
        <div className="panel-header">
          <h2>All IC Rows</h2>
        </div>
        {factorICQuery.isLoading ? (
          <LoadingState />
        ) : rows.length ? (
          <FactorICTable rows={rows} />
        ) : (
          <EmptyState label="No rows match the selected target." />
        )}
      </section>
    </>
  );
}

function FactorICTable({ rows }: { rows: FactorICRow[] }) {
  return (
    <div className="table-wrap">
      <table>
        <thead>
          <tr>
            <th>Feature</th>
            <th>Family</th>
            <th>Target</th>
            <th>Dates</th>
            <th>Rows</th>
            <th>Mean Rank IC</th>
            <th>Rank IC t-stat</th>
            <th>Positive Rank IC</th>
            <th>Mean IC</th>
          </tr>
        </thead>
        <tbody>
          {rows.map((row) => (
            <tr key={`${row.feature}-${row.target}`}>
              <td>{row.feature}</td>
              <td>{featureFamily(row.feature)}</td>
              <td>{row.target}</td>
              <td>{row.dates.toLocaleString()}</td>
              <td>{row.rows.toLocaleString()}</td>
              <td>{formatDecimal(row.mean_rank_ic)}</td>
              <td>{formatDecimal(row.rank_ic_t_stat, 2)}</td>
              <td>{formatPercent(row.positive_rank_ic_pct)}</td>
              <td>{formatDecimal(row.mean_ic)}</td>
            </tr>
          ))}
        </tbody>
      </table>
    </div>
  );
}
