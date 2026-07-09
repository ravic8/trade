import {
  BrainCircuit,
  GitCompareArrows,
  LineChart,
  ShieldCheck,
  Trophy,
} from "lucide-react";
import { useEffect, useMemo, useState } from "react";

import {
  useMLBacktests,
  useMLCandidates,
  useMLEquityCurve,
  useMLLatestCandidates,
  useMLModelMetrics,
  useMLRobustness,
  useMLSummary,
} from "../api/hooks";
import type {
  MLBacktestResult,
  MLConcreteRunId,
  MLDrawdownDailyRow,
  MLEquityCurveRow,
  MLLatestCandidateModel,
  MLModelMetricRow,
  MLRunId,
} from "../api/types";
import { EmptyState, LoadingState } from "../components/DataState";
import { MetricCard } from "../components/MetricCard";
import { PageHeader } from "../components/PageHeader";

const runOptions: { label: string; value: MLRunId }[] = [
  { label: "All", value: "all" },
  { label: "Baselines", value: "baselines" },
  { label: "LightGBM", value: "lightgbm" },
];

const concreteRunOptions: { label: string; value: MLConcreteRunId }[] = [
  { label: "Baselines", value: "baselines" },
  { label: "LightGBM", value: "lightgbm" },
];

const topNOptions = [5, 10, 20];

function formatNumber(value: number | null | undefined): string {
  return value === null || value === undefined ? "n/a" : value.toLocaleString();
}

function formatDecimal(value: number | null | undefined, digits = 4): string {
  return value === null || value === undefined || Number.isNaN(value)
    ? "n/a"
    : value.toFixed(digits);
}

function formatPercent(value: number | null | undefined, digits = 1): string {
  return value === null || value === undefined || Number.isNaN(value)
    ? "n/a"
    : `${(value * 100).toFixed(digits)}%`;
}

function formatReturn(value: number | null | undefined): string {
  return formatPercent(value, 2);
}

function runLabel(run: string | undefined): string {
  if (run === "lightgbm") return "LightGBM";
  if (run === "baselines") return "Baselines";
  return "All";
}

export function MLResearchPage() {
  const [metricRun, setMetricRun] = useState<MLRunId>("all");
  const [backtestGroup, setBacktestGroup] = useState<MLRunId>("all");
  const [candidateRun, setCandidateRun] = useState<MLConcreteRunId>("baselines");
  const [selectedModel, setSelectedModel] = useState("momentum_1d");
  const [selectedTopN, setSelectedTopN] = useState(5);

  const summaryQuery = useMLSummary();
  const metricsQuery = useMLModelMetrics(metricRun);
  const backtestsQuery = useMLBacktests(backtestGroup);
  const candidatesQuery = useMLCandidates({
    run: candidateRun,
    modelId: selectedModel,
    topN: selectedTopN,
    limit: 120,
  });
  const baselineLatestQuery = useMLLatestCandidates({ run: "baselines", topN: 5 });
  const lightgbmLatestQuery = useMLLatestCandidates({ run: "lightgbm", topN: 5 });
  const equityQuery = useMLEquityCurve({
    group: candidateRun,
    modelId: selectedModel,
    topN: selectedTopN,
  });
  const robustnessQuery = useMLRobustness({
    group: candidateRun,
    modelId: selectedModel,
    topN: selectedTopN,
  });

  const summary = summaryQuery.data;
  const dataset = summary?.dataset ?? null;
  const winner = summary?.current_winner ?? null;
  const metricRows = metricsQuery.data?.rows ?? [];
  const backtestRows = backtestsQuery.data?.rows ?? [];
  const candidateRows = candidatesQuery.data?.rows ?? [];
  const equityRows = equityQuery.data?.rows ?? [];

  const modelOptions = useMemo(() => {
    const fromMetrics = metricRows
      .filter((row) => row.run_id === candidateRun)
      .map((row) => row.model_id);
    const fromBacktests = backtestRows
      .filter((row) => row.group === candidateRun)
      .map((row) => row.model_id);
    const options = Array.from(new Set([...fromMetrics, ...fromBacktests])).sort();
    return options.length ? options : [selectedModel];
  }, [backtestRows, candidateRun, metricRows, selectedModel]);

  useEffect(() => {
    const compatible = modelOptions.includes(selectedModel);
    if (!compatible && modelOptions.length) {
      setSelectedModel(modelOptions[0]);
    }
  }, [modelOptions, selectedModel]);

  return (
    <>
      <PageHeader
        eyebrow="ML Research"
        title="Model Evidence"
        subtitle="Leakage-aware dataset, prediction metrics, and long-only top-N backtests."
      />

      <div className="metric-grid">
        <MetricCard
          icon={BrainCircuit}
          label="Trainable Rows"
          value={formatNumber(dataset?.trainable_row_count)}
          detail={`${formatNumber(dataset?.row_count)} total rows`}
        />
        <MetricCard
          icon={ShieldCheck}
          label="Leakage"
          value={dataset?.leakage_checks_passed ? "passed" : "n/a"}
          detail={dataset?.coverage_policy ?? "ML dataset not found"}
        />
        <MetricCard
          icon={GitCompareArrows}
          label="Models"
          value={formatNumber(summary?.model_runs.reduce((sum, run) => sum + run.model_count, 0))}
          detail={`${formatNumber(summary?.walk_forward?.fold_count)} strict folds`}
        />
        <MetricCard
          icon={Trophy}
          label="Current Winner"
          value={winner?.model_id ?? "n/a"}
          detail={winner ? `top ${winner.top_n} | ${formatReturn(winner.total_return)}` : "No backtest yet"}
        />
      </div>

      <section className="panel">
        <div className="panel-header">
          <h2>Research Assumptions</h2>
        </div>
        {summaryQuery.isLoading ? (
          <LoadingState />
        ) : summary ? (
          <div className="assumption-grid">
            <Assumption label="Target" value={summary.assumptions.target} />
            <Assumption label="Universe" value={summary.assumptions.universe} />
            <Assumption label="Evaluation" value={summary.assumptions.evaluation} />
            <Assumption label="Strategy" value={summary.assumptions.strategy} />
            <Assumption label="Caveat" value={summary.assumptions.caveat} wide />
          </div>
        ) : (
          <EmptyState label="No ML summary is available." />
        )}
      </section>

      <section className="panel">
        <div className="panel-header">
          <h2>Latest Top Stocks</h2>
          <span className="muted-label">Artifact-backed candidates for the next available session</span>
        </div>
        <div className="candidate-run-grid">
          <LatestCandidatePanel
            title="Baseline Models"
            run="baselines"
            loading={baselineLatestQuery.isLoading}
            predictionDate={baselineLatestQuery.data?.prediction_date ?? null}
            targetSessionDate={baselineLatestQuery.data?.target_session_date ?? null}
            note={baselineLatestQuery.data?.note}
            models={baselineLatestQuery.data?.models ?? []}
          />
          <LatestCandidatePanel
            title="LightGBM Models"
            run="lightgbm"
            loading={lightgbmLatestQuery.isLoading}
            predictionDate={lightgbmLatestQuery.data?.prediction_date ?? null}
            targetSessionDate={lightgbmLatestQuery.data?.target_session_date ?? null}
            note={lightgbmLatestQuery.data?.note}
            models={lightgbmLatestQuery.data?.models ?? []}
          />
        </div>
      </section>

      <section className="panel">
        <div className="panel-header">
          <h2>Prediction Metrics</h2>
          <SegmentedRunControl value={metricRun} onChange={setMetricRun} />
        </div>
        {metricsQuery.isLoading ? (
          <LoadingState />
        ) : metricRows.length ? (
          <ModelMetricsTable rows={metricRows} />
        ) : (
          <EmptyState label="No prediction metrics are available." />
        )}
      </section>

      <section className="panel">
        <div className="panel-header">
          <h2>Backtest Comparison</h2>
          <SegmentedRunControl value={backtestGroup} onChange={setBacktestGroup} />
        </div>
        {backtestsQuery.isLoading ? (
          <LoadingState />
        ) : backtestRows.length ? (
          <BacktestTable rows={backtestRows} />
        ) : (
          <EmptyState label="No backtest rows are available." />
        )}
      </section>

      <section className="panel">
        <div className="panel-header">
          <h2>Selected Model Drilldown</h2>
          <div className="inline-controls">
            <label>
              Run
              <select
                value={candidateRun}
                onChange={(event) => {
                  const nextRun = event.target.value as MLConcreteRunId;
                  setCandidateRun(nextRun);
                  setSelectedModel(
                    nextRun === "lightgbm" ? "lgbm_regressor_momentum_blend" : "momentum_1d",
                  );
                }}
              >
                {concreteRunOptions.map((option) => (
                  <option key={option.value} value={option.value}>
                    {option.label}
                  </option>
                ))}
              </select>
            </label>
            <label>
              Model
              <select
                value={selectedModel}
                onChange={(event) => setSelectedModel(event.target.value)}
              >
                {modelOptions.map((modelId) => (
                  <option key={modelId} value={modelId}>
                    {modelId}
                  </option>
                ))}
              </select>
            </label>
            <label>
              Top N
              <select
                value={selectedTopN}
                onChange={(event) => setSelectedTopN(Number(event.target.value))}
              >
                {topNOptions.map((value) => (
                  <option key={value} value={value}>
                    {value}
                  </option>
                ))}
              </select>
            </label>
          </div>
        </div>
        <div className="drilldown-grid">
          <div className="drilldown-chart">
            <div className="mini-section-header">
              <LineChart size={17} />
              <strong>Equity and Drawdown</strong>
            </div>
            {equityQuery.isLoading ? (
              <LoadingState />
            ) : equityRows.length ? (
              <EquityCurve rows={equityRows} />
            ) : (
              <EmptyState label="No equity curve for this selection." />
            )}
          </div>
          <div className="drilldown-table">
            <div className="mini-section-header">
              <GitCompareArrows size={17} />
              <strong>Recent Candidates</strong>
            </div>
            {candidatesQuery.isLoading ? (
              <LoadingState />
            ) : candidateRows.length ? (
              <div className="table-wrap compact-table">
                <table>
                  <thead>
                    <tr>
                      <th>Date</th>
                      <th>Rank</th>
                      <th>Symbol</th>
                      <th>Score</th>
                      <th>Realized</th>
                    </tr>
                  </thead>
                  <tbody>
                    {candidateRows.slice(-40).map((row) => (
                      <tr key={`${row.prediction_date}-${row.instrument_key}-${row.rank}`}>
                        <td>{row.prediction_date}</td>
                        <td>{row.rank}</td>
                        <td>{row.symbol}</td>
                        <td>{formatDecimal(row.score)}</td>
                        <td>{formatReturn(row.realized_forward_ret_1d)}</td>
                      </tr>
                    ))}
                  </tbody>
                </table>
              </div>
            ) : (
              <EmptyState label="No candidates for this model/top-N selection." />
            )}
          </div>
        </div>
      </section>

      <section className="panel">
        <div className="panel-header">
          <h2>Robustness</h2>
          <div className="inline-summary">
            <span>{runLabel(candidateRun)}</span>
            <span>{selectedModel}</span>
            <span>top {selectedTopN}</span>
          </div>
        </div>
        {robustnessQuery.isLoading ? (
          <LoadingState />
        ) : robustnessQuery.data?.status === "done" ? (
          <div className="robustness-grid">
            <div>
              <div className="mini-section-header">
                <strong>Cost Sensitivity</strong>
              </div>
              <CostSensitivityTable rows={robustnessQuery.data.cost_sensitivity} />
            </div>
            <div>
              <div className="mini-section-header">
                <strong>Top-N Comparison</strong>
              </div>
              <BacktestTable rows={robustnessQuery.data.top_n_comparison} compact />
            </div>
            <div className="wide">
              <div className="mini-section-header">
                <strong>Worst Drawdown Window</strong>
              </div>
              {robustnessQuery.data.drawdown ? (
                <DrawdownPanel
                  drawdown={robustnessQuery.data.drawdown}
                  rows={robustnessQuery.data.drawdown.daily_returns}
                />
              ) : (
                <EmptyState label="No drawdown window available for this selection." />
              )}
            </div>
          </div>
        ) : (
          <EmptyState label="No robustness diagnostics for this selection." />
        )}
      </section>
    </>
  );
}

function Assumption({ label, value, wide = false }: { label: string; value: string; wide?: boolean }) {
  return (
    <div className={wide ? "assumption-item wide" : "assumption-item"}>
      <dt>{label}</dt>
      <dd>{value}</dd>
    </div>
  );
}

function SegmentedRunControl({
  value,
  onChange,
}: {
  value: MLRunId;
  onChange: (value: MLRunId) => void;
}) {
  return (
    <div className="segmented-control" aria-label="Model run filter">
      {runOptions.map((option) => (
        <button
          className={value === option.value ? "active" : ""}
          key={option.value}
          type="button"
          onClick={() => onChange(option.value)}
        >
          {option.label}
        </button>
      ))}
    </div>
  );
}

function ModelMetricsTable({ rows }: { rows: MLModelMetricRow[] }) {
  return (
    <div className="table-wrap">
      <table>
        <thead>
          <tr>
            <th>Run</th>
            <th>Model</th>
            <th>Dates</th>
            <th>Evaluated Rows</th>
            <th>Rank IC</th>
            <th>Top 5 Avg</th>
            <th>Top 10 Avg</th>
            <th>Top 10 Hit</th>
            <th>Top 20 Avg</th>
          </tr>
        </thead>
        <tbody>
          {rows.map((row) => (
            <tr key={`${row.run_id}-${row.model_id}`}>
              <td>{runLabel(row.run_id)}</td>
              <td>{row.model_id}</td>
              <td>{formatNumber(row.prediction_date_count)}</td>
              <td>{formatNumber(row.evaluated_rows)}</td>
              <td>{formatDecimal(row.rank_ic_mean)}</td>
              <td>{formatReturn(row.top_5_average_return)}</td>
              <td>{formatReturn(row.top_10_average_return)}</td>
              <td>{formatPercent(row.top_10_hit_rate)}</td>
              <td>{formatReturn(row.top_20_average_return)}</td>
            </tr>
          ))}
        </tbody>
      </table>
    </div>
  );
}

function BacktestTable({ rows, compact = false }: { rows: MLBacktestResult[]; compact?: boolean }) {
  return <BacktestTableBase rows={rows} compact={compact} />;
}

function BacktestTableBase({ rows, compact }: { rows: MLBacktestResult[]; compact: boolean }) {
  return (
    <div className={compact ? "table-wrap compact-table" : "table-wrap"}>
      <table>
        <thead>
          <tr>
            <th>Run</th>
            <th>Model</th>
            <th>Top N</th>
            <th>Days</th>
            <th>Total Return</th>
            <th>Sharpe</th>
            <th>Max DD</th>
            <th>Win Rate</th>
            <th>Turnover</th>
            <th>Cost</th>
          </tr>
        </thead>
        <tbody>
          {rows.map((row) => (
            <tr key={`${row.group}-${row.model_id}-${row.top_n}`}>
              <td>{runLabel(row.group)}</td>
              <td>{row.model_id}</td>
              <td>{row.top_n}</td>
              <td>{formatNumber(row.day_count)}</td>
              <td>{formatReturn(row.total_return)}</td>
              <td>{formatDecimal(row.sharpe_ratio, 2)}</td>
              <td>{formatReturn(row.max_drawdown)}</td>
              <td>{formatPercent(row.win_rate)}</td>
              <td>{formatPercent(row.average_turnover)}</td>
              <td>{formatReturn(row.total_transaction_cost)}</td>
            </tr>
          ))}
        </tbody>
      </table>
    </div>
  );
}

function LatestCandidatePanel({
  title,
  run,
  loading,
  predictionDate,
  targetSessionDate,
  note,
  models,
}: {
  title: string;
  run: MLConcreteRunId;
  loading: boolean;
  predictionDate: string | null;
  targetSessionDate: string | null;
  note?: string;
  models: MLLatestCandidateModel[];
}) {
  return (
    <div className="candidate-run-panel">
      <div className="candidate-run-header">
        <strong>{title}</strong>
        <span>
          {targetSessionDate
            ? `target ${targetSessionDate}${predictionDate ? ` | feature ${predictionDate}` : ""}`
            : predictionDate
              ? `feature date ${predictionDate}`
              : "no prediction date"}
        </span>
      </div>
      {note ? <p className="candidate-note">{note}</p> : null}
      {loading ? (
        <LoadingState />
      ) : models.length ? (
        <div className="candidate-model-list">
          {models.map((model) => (
            <div className="candidate-model" key={`${run}-${model.model_id}`}>
              <div className="candidate-model-title">
                <strong>{model.model_id}</strong>
                <span>{runLabel(run)}</span>
              </div>
              <div className="candidate-chip-row">
                {model.rows.map((row) => (
                  <span className="candidate-chip" key={`${model.model_id}-${row.symbol}-${row.rank}`}>
                    <b>{row.rank}</b>
                    {row.symbol}
                    <small>{formatDecimal(row.score)}</small>
                  </span>
                ))}
              </div>
            </div>
          ))}
        </div>
      ) : (
        <EmptyState label="No latest candidates available." />
      )}
    </div>
  );
}

function CostSensitivityTable({ rows }: { rows: Array<MLBacktestResult & { transaction_cost_bps: number }> }) {
  return (
    <div className="table-wrap compact-table">
      <table>
        <thead>
          <tr>
            <th>Cost bps</th>
            <th>Total Return</th>
            <th>Sharpe</th>
            <th>Max DD</th>
            <th>Avg Net</th>
            <th>Cost</th>
          </tr>
        </thead>
        <tbody>
          {rows.map((row) => (
            <tr key={row.transaction_cost_bps}>
              <td>{row.transaction_cost_bps}</td>
              <td>{formatReturn(row.total_return)}</td>
              <td>{formatDecimal(row.sharpe_ratio, 2)}</td>
              <td>{formatReturn(row.max_drawdown)}</td>
              <td>{formatReturn(row.average_daily_net_return)}</td>
              <td>{formatReturn(row.total_transaction_cost)}</td>
            </tr>
          ))}
        </tbody>
      </table>
    </div>
  );
}

function DrawdownPanel({
  drawdown,
  rows,
}: {
  drawdown: {
    peak_date: string;
    trough_date: string;
    recovery_date: string | null;
    max_drawdown: number;
    peak_equity: number;
    trough_equity: number;
    days: number;
  };
  rows: MLDrawdownDailyRow[];
}) {
  return (
    <div>
      <dl className="drawdown-summary">
        <div>
          <dt>Peak</dt>
          <dd>{drawdown.peak_date}</dd>
        </div>
        <div>
          <dt>Trough</dt>
          <dd>{drawdown.trough_date}</dd>
        </div>
        <div>
          <dt>Recovery</dt>
          <dd>{drawdown.recovery_date ?? "not recovered"}</dd>
        </div>
        <div>
          <dt>Max drawdown</dt>
          <dd>{formatReturn(drawdown.max_drawdown)}</dd>
        </div>
        <div>
          <dt>Days</dt>
          <dd>{drawdown.days}</dd>
        </div>
      </dl>
      <div className="table-wrap compact-table">
        <table>
          <thead>
            <tr>
              <th>Date</th>
              <th>Gross</th>
              <th>Net</th>
              <th>Turnover</th>
              <th>Cost</th>
            </tr>
          </thead>
          <tbody>
            {rows.map((row) => (
              <tr key={row.prediction_date}>
                <td>{row.prediction_date}</td>
                <td>{formatReturn(row.gross_return)}</td>
                <td>{formatReturn(row.net_return)}</td>
                <td>{formatPercent(row.turnover)}</td>
                <td>{formatReturn(row.transaction_cost)}</td>
              </tr>
            ))}
          </tbody>
        </table>
      </div>
    </div>
  );
}

function EquityCurve({ rows }: { rows: MLEquityCurveRow[] }) {
  const width = 680;
  const height = 260;
  const padding = 28;
  const equityValues = rows.map((row) => row.equity);
  const drawdownValues = rows.map((row) => row.drawdown);
  const minEquity = Math.min(...equityValues, 1);
  const maxEquity = Math.max(...equityValues, 1);
  const minDrawdown = Math.min(...drawdownValues, 0);
  const maxDrawdown = 0;

  const xFor = (index: number) =>
    padding + (index / Math.max(rows.length - 1, 1)) * (width - padding * 2);
  const yForEquity = (value: number) =>
    height - padding - ((value - minEquity) / Math.max(maxEquity - minEquity, 0.0001)) * 150;
  const yForDrawdown = (value: number) =>
    height - padding - ((value - minDrawdown) / Math.max(maxDrawdown - minDrawdown, 0.0001)) * 70;

  const equityPath = rows
    .map((row, index) => `${index === 0 ? "M" : "L"} ${xFor(index)} ${yForEquity(row.equity)}`)
    .join(" ");
  const drawdownPath = rows
    .map((row, index) => `${index === 0 ? "M" : "L"} ${xFor(index)} ${yForDrawdown(row.drawdown)}`)
    .join(" ");

  return (
    <div className="equity-card">
      <svg viewBox={`0 0 ${width} ${height}`} role="img" aria-label="Equity and drawdown curve">
        <line x1={padding} x2={width - padding} y1={height - padding} y2={height - padding} />
        <path className="equity-line" d={equityPath} />
        <path className="drawdown-line" d={drawdownPath} />
      </svg>
      <div className="chart-legend">
        <span><i className="legend-equity" /> Equity</span>
        <span><i className="legend-drawdown" /> Drawdown</span>
        <span>{rows[0]?.prediction_date} to {rows.at(-1)?.prediction_date}</span>
      </div>
    </div>
  );
}
