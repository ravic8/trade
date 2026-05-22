import * as echarts from "echarts";
import { useEffect, useRef } from "react";

import type { ScreenerResult } from "../api/types";

export function FeatureChart({ results }: { results: ScreenerResult[] }) {
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
        data: results.map((item) => item.ticker),
        axisLabel: { color: "#54656f" },
      },
      yAxis: { type: "value", axisLabel: { color: "#54656f" } },
      series: [
        {
          name: "5D Up 1%",
          type: "bar",
          data: results.map((item) => item.d5Up0100),
          itemStyle: { color: "#168a62" },
        },
        {
          name: "5D Down 1%",
          type: "bar",
          data: results.map((item) => item.d5Dn0100),
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
