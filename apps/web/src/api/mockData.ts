import type { Candle, JobRun, MarketStatus, ResearchNote, ScreenerResult } from "./types";

export const marketStatus: MarketStatus[] = [
  {
    exchange: "NSE",
    universeSize: 2365,
    lastOhlcvRun: "2026-05-20T15:45:00+05:30",
    lastScreenerRun: "2026-05-20T16:10:00+05:30",
    staleSymbols: 18,
    dataQualityScore: 98.2,
  },
  {
    exchange: "TSX",
    universeSize: 687,
    lastOhlcvRun: "2026-05-20T15:25:00+05:30",
    lastScreenerRun: "2026-05-20T16:02:00+05:30",
    staleSymbols: 11,
    dataQualityScore: 96.8,
  },
];

export const screenerResults: ScreenerResult[] = [
  {
    ticker: "RELIANCE.NS",
    exchange: "NSE",
    company: "Reliance Industries",
    signal: "Intraday range expansion",
    liquidity: 2145000000,
    d5Up0100: 4,
    d5Dn0100: 3,
    d5ClUp0200: 1,
    d5VUp0200: 2,
    matchedAt: "2026-05-20T16:10:00+05:30",
  },
  {
    ticker: "INFY.NS",
    exchange: "NSE",
    company: "Infosys",
    signal: "Range expansion without close shock",
    liquidity: 1128000000,
    d5Up0100: 3,
    d5Dn0100: 4,
    d5ClUp0200: 0,
    d5VUp0200: 2,
    matchedAt: "2026-05-20T16:10:00+05:30",
  },
  {
    ticker: "WDO.TO",
    exchange: "TSX",
    company: "Wesdome Gold Mines",
    signal: "Two-way intraday expansion",
    liquidity: 43800000,
    d5Up0100: 3,
    d5Dn0100: 3,
    d5ClUp0200: 2,
    d5VUp0200: 1,
    matchedAt: "2026-05-20T16:02:00+05:30",
  },
];

export const candles: Candle[] = Array.from({ length: 70 }, (_, index) => {
  const base = 100 + Math.sin(index / 5) * 4 + index * 0.08;
  const open = Number((base + Math.sin(index) * 0.8).toFixed(2));
  const close = Number((base + Math.cos(index / 2) * 0.9).toFixed(2));
  const high = Number((Math.max(open, close) + 1.2 + (index % 4) * 0.18).toFixed(2));
  const low = Number((Math.min(open, close) - 1.1 - (index % 3) * 0.15).toFixed(2));
  return {
    time: `2026-03-${String((index % 28) + 1).padStart(2, "0")}`,
    open,
    high,
    low,
    close,
    volume: 850000 + index * 12000 + (index % 5) * 70000,
  };
});

export const researchNotes: ResearchNote[] = [
  {
    id: "note-1",
    ticker: "RELIANCE.NS",
    title: "Energy and telecom subsidiaries remain primary catalyst cluster",
    sourceType: "agent",
    publishedAt: "2026-05-20T16:12:00+05:30",
    summary:
      "The signal is price-behavior driven. Current retrieved context points to sector flows rather than a single confirmed company-specific event.",
    confidence: 0.74,
  },
  {
    id: "note-2",
    ticker: "INFY.NS",
    title: "IT sector range widening with muted close-to-open follow-through",
    sourceType: "news",
    publishedAt: "2026-05-20T15:44:00+05:30",
    summary:
      "Recent documents mention broad IT rotation. The screener pattern suggests active intraday participation without large overnight shock.",
    confidence: 0.69,
  },
  {
    id: "note-3",
    ticker: "WDO.TO",
    title: "Gold miners screen as volatility candidates",
    sourceType: "exchange",
    publishedAt: "2026-05-20T15:30:00+05:30",
    summary:
      "Commodity-linked names are clustering in the range screen. Confirm source-level news before attributing the move.",
    confidence: 0.66,
  },
];

export const jobRuns: JobRun[] = [
  {
    id: "job-101",
    name: "NSE OHLCV ingestion",
    status: "completed",
    startedAt: "2026-05-20T15:20:00+05:30",
    durationSeconds: 615,
    itemsProcessed: 2347,
  },
  {
    id: "job-102",
    name: "Intraday range screener",
    status: "completed",
    startedAt: "2026-05-20T16:04:00+05:30",
    durationSeconds: 18,
    itemsProcessed: 1654,
  },
  {
    id: "job-103",
    name: "Research document embedding",
    status: "running",
    startedAt: "2026-05-20T16:15:00+05:30",
    durationSeconds: null,
    itemsProcessed: 128,
  },
];
