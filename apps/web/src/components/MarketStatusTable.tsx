import type { MarketStatus } from "../api/types";
import { formatDateTime } from "../utils/format";

export function MarketStatusTable({ data }: { data: MarketStatus[] }) {
  return (
    <div className="table-wrap">
      <table>
        <thead>
          <tr>
            <th>Exchange</th>
            <th>Universe</th>
            <th>Quality</th>
            <th>Stale</th>
            <th>OHLCV</th>
            <th>Screener</th>
          </tr>
        </thead>
        <tbody>
          {data.map((row) => (
            <tr key={row.exchange}>
              <td><strong>{row.exchange}</strong></td>
              <td>{row.universeSize.toLocaleString()}</td>
              <td>{row.dataQualityScore.toFixed(1)}%</td>
              <td>{row.staleSymbols}</td>
              <td>{formatDateTime(row.lastOhlcvRun)}</td>
              <td>{formatDateTime(row.lastScreenerRun)}</td>
            </tr>
          ))}
        </tbody>
      </table>
    </div>
  );
}
