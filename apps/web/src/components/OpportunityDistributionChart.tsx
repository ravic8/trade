import { Maximize2, X } from "lucide-react";
import { useEffect, useRef } from "react";

import type {
  OpportunityDistribution,
  OpportunityPercentileRange,
} from "../api/types";
import { echarts } from "../utils/echarts";
import { formatPercent } from "../utils/format";
import { opportunityMetricConfig } from "../utils/opportunityMetrics";

const percentileOptions = [0, 10, 20, 25, 30, 40, 50, 60, 70, 75, 80, 90, 95, 100];

function percentileLabel(value: number, boundary: "minimum" | "maximum") {
  if (boundary === "minimum" && value === 0) return "No minimum";
  if (boundary === "maximum" && value === 100) return "No maximum";
  return `P${value}`;
}

function optionsIncluding(value: number) {
  return [...new Set([...percentileOptions, value])].sort((left, right) => left - right);
}

type OpportunityDistributionChartProps = {
  distribution: OpportunityDistribution;
  range: OpportunityPercentileRange;
  onRangeChange: (range: OpportunityPercentileRange) => void;
  onRemove: () => void;
};

export function OpportunityDistributionChart({
  distribution,
  range,
  onRangeChange,
  onRemove,
}: OpportunityDistributionChartProps) {
  const ref = useRef<HTMLDivElement | null>(null);
  const onRangeChangeRef = useRef(onRangeChange);
  const config = opportunityMetricConfig[distribution.metric];
  const minimum = range.minimum ?? 0;
  const maximum = range.maximum ?? 100;

  useEffect(() => {
    onRangeChangeRef.current = onRangeChange;
  }, [onRangeChange]);

  useEffect(() => {
    if (!ref.current) return;

    const chart = echarts.init(ref.current);
    const labels = distribution.bins.map((bin) => {
      if (bin.lower_overflow) return `≤ ${formatPercent(bin.end, 1)}`;
      if (bin.upper_overflow) return `≥ ${formatPercent(bin.start, 1)}`;
      return `${formatPercent(bin.start, 1)} – ${formatPercent(bin.end, 1)}`;
    });
    chart.setOption({
      animationDuration: 250,
      aria: {
        enabled: true,
        description: `${config.label} distribution for ${distribution.count} symbols`,
      },
      tooltip: {
        trigger: "axis",
        confine: true,
        axisPointer: { type: "shadow" },
        formatter: (items: unknown) => {
          const item = Array.isArray(items) ? items[0] : items;
          const index =
            item && typeof item === "object" && "dataIndex" in item
              ? Number(item.dataIndex)
              : 0;
          const bin = distribution.bins[index];
          if (!bin) return "";
          const percentileText =
            bin.percentile_min == null || bin.percentile_max == null
              ? "No observations"
              : `P${Math.round(bin.percentile_min)}–P${Math.round(bin.percentile_max)}`;
          const valueRange = bin.lower_overflow
            ? `Up to ${formatPercent(bin.end, 2)}`
            : bin.upper_overflow
              ? `${formatPercent(bin.start, 2)} and above`
              : `${formatPercent(bin.start, 2)} to ${formatPercent(bin.end, 2)}`;
          return [
            `<strong>${config.label}</strong>`,
            valueRange,
            `${bin.count.toLocaleString()} symbols · ${percentileText}`,
            "Click to filter this percentile band",
          ].join("<br/>");
        },
      },
      grid: { top: 18, right: 16, bottom: 68, left: 48 },
      xAxis: {
        type: "category",
        data: labels,
        axisLabel: {
          color: "#64757d",
          fontSize: 10,
          interval: Math.max(0, Math.ceil(labels.length / 6) - 1),
        },
        axisTick: { show: false },
      },
      yAxis: {
        type: "value",
        minInterval: 1,
        axisLabel: { color: "#64757d", fontSize: 10 },
        splitLine: { lineStyle: { color: "#e8eeeb" } },
      },
      dataZoom: [
        {
          type: "inside",
          filterMode: "none",
          zoomOnMouseWheel: true,
          moveOnMouseMove: true,
          moveOnMouseWheel: true,
        },
        {
          type: "slider",
          height: 18,
          bottom: 16,
          borderColor: "#d7e2dd",
          fillerColor: "rgba(0, 109, 63, 0.12)",
          handleStyle: { color: "#006d3f" },
          textStyle: { color: "#64757d", fontSize: 9 },
        },
      ],
      series: [
        {
          name: config.label,
          type: "bar",
          data: distribution.bins.map((bin) => ({
            value: bin.count,
            itemStyle: {
              color:
                bin.percentile_min != null &&
                bin.percentile_max != null &&
                bin.percentile_max >= minimum &&
                bin.percentile_min <= maximum
                  ? config.color
                  : "#cfd9d5",
              borderRadius: [3, 3, 0, 0],
            },
          })),
          barMaxWidth: 34,
          emphasis: { focus: "series" },
        },
      ],
    });

    chart.on("click", (event) => {
      const bin = distribution.bins[event.dataIndex];
      if (bin?.percentile_min == null || bin.percentile_max == null) return;
      onRangeChangeRef.current({
        minimum: Math.floor(bin.percentile_min),
        maximum: Math.ceil(bin.percentile_max),
      });
    });

    const resizeObserver = new ResizeObserver(() => chart.resize());
    resizeObserver.observe(ref.current);
    return () => {
      resizeObserver.disconnect();
      chart.dispose();
    };
  }, [config.color, config.label, distribution, maximum, minimum]);

  return (
    <article className="opportunity-chart-card">
      <header className="opportunity-chart-header">
        <div>
          <span className="opportunity-chart-title">{config.label}</span>
          <span className="opportunity-chart-description">{config.description}</span>
        </div>
        <button
          className="opportunity-chart-remove"
          type="button"
          onClick={onRemove}
          aria-label={`Hide ${config.label} figure`}
        >
          <X size={16} />
        </button>
      </header>

      <div className="opportunity-chart-controls">
        <label>
          From
          <select
            value={minimum}
            onChange={(event) =>
              onRangeChange({ minimum: Number(event.target.value), maximum })
            }
          >
            {optionsIncluding(minimum)
              .filter((value) => value <= maximum)
              .map((value) => (
                <option key={value} value={value}>
                  {percentileLabel(value, "minimum")}
                </option>
              ))}
          </select>
        </label>
        <label>
          To
          <select
            value={maximum}
            onChange={(event) =>
              onRangeChange({ minimum, maximum: Number(event.target.value) })
            }
          >
            {optionsIncluding(maximum)
              .filter((value) => value >= minimum)
              .map((value) => (
                <option key={value} value={value}>
                  {percentileLabel(value, "maximum")}
                </option>
              ))}
          </select>
        </label>
        <button
          type="button"
          onClick={() => onRangeChange({})}
          disabled={minimum === 0 && maximum === 100}
        >
          Reset
        </button>
        <span className="opportunity-chart-hint">
          <Maximize2 size={13} />
          Pinch, scroll, or drag to zoom
        </span>
      </div>

      <div className="opportunity-distribution-chart" ref={ref} />

      <footer className="opportunity-chart-percentiles">
        <span>P25 {formatPercent(distribution.percentiles.p25 ?? null)}</span>
        <strong>Median {formatPercent(distribution.percentiles.p50 ?? null)}</strong>
        <span>P75 {formatPercent(distribution.percentiles.p75 ?? null)}</span>
      </footer>
      {distribution.minimum !== distribution.display_minimum ||
      distribution.maximum !== distribution.display_maximum ? (
        <p className="opportunity-chart-scale-note">
          Chart scale uses P1–P99; observations outside it remain in the end bars.
        </p>
      ) : null}
    </article>
  );
}
