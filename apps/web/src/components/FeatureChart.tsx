import { useEffect, useRef } from "react";

import type { OpportunityTargetRow } from "../api/types";
import { echarts } from "../utils/echarts";

export function FeatureChart({ results }: { results: OpportunityTargetRow[] }) {
  const ref = useRef<HTMLDivElement | null>(null);

  useEffect(() => {
    if (!ref.current) {
      return;
    }
    const chart = echarts.init(ref.current);
    chart.setOption({
      tooltip: { trigger: "axis" },
      legend: { bottom: 0 },
      grid: { top: 24, right: 16, bottom: 54, left: 36 },
      xAxis: {
        type: "category",
        data: results.map((item) => item.symbol),
        axisLabel: { color: "#54656f" },
      },
      yAxis: { type: "value", axisLabel: { color: "#54656f" } },
      series: [
        {
          name: "Upside",
          type: "bar",
          data: results.map((item) => Number(((item.upside ?? 0) * 100).toFixed(2))),
          itemStyle: { color: "#168a62" },
        },
        {
          name: "Downside",
          type: "bar",
          data: results.map((item) => Number(((item.downside ?? 0) * 100).toFixed(2))),
          itemStyle: { color: "#c2413a" },
        },
      ],
    });

    const resizeObserver = new ResizeObserver(() => chart.resize());
    resizeObserver.observe(ref.current);

    return () => {
      resizeObserver.disconnect();
      chart.dispose();
    };
  }, [results]);

  return <div className="feature-chart" ref={ref} />;
}
