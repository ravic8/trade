import { ArrowUpRight } from "lucide-react";
import { Link } from "react-router-dom";

import type {
  OpportunityDistributionMetric,
  OpportunityTargetRow,
} from "../api/types";
import { formatPercent } from "../utils/format";

const displayedMetrics: Array<{
  metric: OpportunityDistributionMetric;
  label: string;
  tone: "signed" | "favorable" | "adverse" | "neutral";
}> = [
  { metric: "session_return", label: "Return", tone: "signed" },
  { metric: "recovery", label: "Recovery", tone: "favorable" },
  { metric: "upside", label: "Upside", tone: "favorable" },
  { metric: "downside", label: "Downside", tone: "adverse" },
  { metric: "giveback", label: "Giveback", tone: "adverse" },
  { metric: "true_range", label: "True range", tone: "neutral" },
];

function MetricValue({
  row,
  metric,
  tone,
}: {
  row: OpportunityTargetRow;
  metric: OpportunityDistributionMetric;
  tone: "signed" | "favorable" | "adverse" | "neutral";
}) {
  const value = row[metric];
  const percentile = row.percentiles[metric];
  let className = "";
  if (value != null && tone === "signed") {
    className = value > 0 ? "positive-value" : value < 0 ? "negative-value" : "";
  } else if (value != null && tone === "favorable") {
    className = "positive-value";
  } else if (value != null && tone === "adverse") {
    className = "negative-value";
  }
  return (
    <span className="opportunity-result-value">
      <strong className={className}>{formatPercent(value)}</strong>
      <small>{percentile == null ? "—" : `P${Math.round(percentile)}`}</small>
    </span>
  );
}

function SymbolLink({ symbol }: { symbol: string }) {
  return (
    <Link className="ticker-link" to={`/symbols/${encodeURIComponent(symbol)}`}>
      {symbol}
      <ArrowUpRight size={14} />
    </Link>
  );
}

export function OpportunityTable({ data }: { data: OpportunityTargetRow[] }) {
  return (
    <>
      <div className="table-wrap opportunity-table-wrap opportunity-table-desktop">
        <table>
          <thead>
            <tr>
              <th>Symbol</th>
              {displayedMetrics.map(({ metric, label }) => (
                <th key={metric}>{label}</th>
              ))}
              <th>Quality</th>
            </tr>
          </thead>
          <tbody>
            {data.map((row) => (
              <tr key={row.instrument_key}>
                <td>
                  <SymbolLink symbol={row.symbol} />
                </td>
                {displayedMetrics.map(({ metric, tone }) => (
                  <td key={metric}>
                    <MetricValue row={row} metric={metric} tone={tone} />
                  </td>
                ))}
                <td>
                  <span className="opportunity-quality">{row.quality_status}</span>
                </td>
              </tr>
            ))}
          </tbody>
        </table>
      </div>

      <div className="opportunity-result-cards">
        {data.map((row) => (
          <article className="opportunity-result-card" key={row.instrument_key}>
            <header>
              <SymbolLink symbol={row.symbol} />
              <span className="opportunity-quality">{row.quality_status}</span>
            </header>
            <div>
              {displayedMetrics.map(({ metric, label, tone }) => (
                <section key={metric}>
                  <span>{label}</span>
                  <MetricValue row={row} metric={metric} tone={tone} />
                </section>
              ))}
            </div>
          </article>
        ))}
      </div>
    </>
  );
}
