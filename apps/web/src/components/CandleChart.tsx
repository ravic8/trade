import { createChart, CandlestickSeries, HistogramSeries, type IChartApi } from "lightweight-charts";
import { useEffect, useRef } from "react";

import type { Candle } from "../api/types";

export function CandleChart({ data }: { data: Candle[] }) {
  const containerRef = useRef<HTMLDivElement | null>(null);
  const chartRef = useRef<IChartApi | null>(null);

  useEffect(() => {
    if (!containerRef.current) {
      return;
    }

    const chart = createChart(containerRef.current, {
      height: 360,
      layout: {
        background: { color: "#ffffff" },
        textColor: "#263238",
      },
      grid: {
        vertLines: { color: "#eef2f4" },
        horzLines: { color: "#eef2f4" },
      },
      rightPriceScale: { borderColor: "#d8e0e5" },
      timeScale: { borderColor: "#d8e0e5" },
    });
    chartRef.current = chart;

    const candleSeries = chart.addSeries(CandlestickSeries, {
      upColor: "#168a62",
      downColor: "#c2413a",
      borderVisible: false,
      wickUpColor: "#168a62",
      wickDownColor: "#c2413a",
    });
    candleSeries.setData(data);

    const volumeSeries = chart.addSeries(HistogramSeries, {
      priceFormat: { type: "volume" },
      priceScaleId: "",
      color: "#9db4c0",
    });
    volumeSeries.priceScale().applyOptions({
      scaleMargins: {
        top: 0.78,
        bottom: 0,
      },
    });
    volumeSeries.setData(
      data.map((item) => ({
        time: item.time,
        value: item.volume,
        color: item.close >= item.open ? "#9ccfbb" : "#e7aaa6",
      })),
    );

    const resizeObserver = new ResizeObserver(([entry]) => {
      chart.applyOptions({ width: entry.contentRect.width });
    });
    resizeObserver.observe(containerRef.current);
    chart.timeScale().fitContent();

    return () => {
      resizeObserver.disconnect();
      chart.remove();
      chartRef.current = null;
    };
  }, [data]);

  return <div className="chart-panel" ref={containerRef} />;
}
