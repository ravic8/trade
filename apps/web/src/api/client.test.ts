import { afterEach, describe, expect, it, vi } from "vitest";

import {
  fetchJson,
  getDailyOpportunities,
  getMarketDataHealth,
  getPhase3Readiness,
} from "./client";

describe("getDailyOpportunities", () => {
  afterEach(() => vi.restoreAllMocks());

  it("serializes explicit-session, return, and combined percentile filters", async () => {
    const fetchMock = vi.spyOn(globalThis, "fetch").mockResolvedValue(
      new Response(
        JSON.stringify({ exchange: "NSE", session_exists: false, rows: [] }),
        { status: 200, headers: { "Content-Type": "application/json" } },
      ),
    );

    await getDailyOpportunities({
      exchange: "NSE",
      sessionDate: "2026-08-24",
      symbol: "INFY",
      sessionReturnRange: { minimumPercent: 1, maximumPercent: 2 },
      percentileFilters: {
        upside: { minimum: 75 },
        recovery: { maximum: 90 },
      },
    });

    const url = new URL(String(fetchMock.mock.calls[0][0]), "http://localhost");
    expect(url.pathname).toBe("/api/opportunities/daily");
    expect(Object.fromEntries(url.searchParams)).toEqual({
      exchange: "NSE",
      session_date: "2026-08-24",
      symbol: "INFY",
      session_return_min_percent: "1",
      session_return_max_percent: "2",
      upside_percentile_min: "75",
      recovery_percentile_max: "90",
    });
  });
});

describe("fetchJson", () => {
  it("rejects instead of returning demo data when fallback is disabled", async () => {
    vi.spyOn(globalThis, "fetch").mockResolvedValue(
      new Response("unavailable", { status: 503 }),
    );

    await expect(fetchJson("/api/market/status", [{ demo: true }], false)).rejects.toThrow(
      "Request failed: 503",
    );
  });
});

describe("getMarketDataHealth", () => {
  afterEach(() => vi.restoreAllMocks());

  it("uses the NSE yfinance operational endpoint", async () => {
    const fetchMock = vi.spyOn(globalThis, "fetch").mockResolvedValue(
      new Response(JSON.stringify({ health_status: "unknown", quality: [] }), {
        status: 200,
        headers: { "Content-Type": "application/json" },
      }),
    );

    await getMarketDataHealth();

    expect(fetchMock).toHaveBeenCalledWith(
      "/api/data/operations/market-data-health?provider=yfinance&exchange=NSE",
      undefined,
    );
  });
});

describe("getPhase3Readiness", () => {
  afterEach(() => vi.restoreAllMocks());

  it("uses the Phase 3 production-readiness endpoint", async () => {
    const fetchMock = vi.spyOn(globalThis, "fetch").mockResolvedValue(
      new Response(JSON.stringify({ ready_for_production: false, gates: [] }), {
        status: 200,
        headers: { "Content-Type": "application/json" },
      }),
    );

    await getPhase3Readiness();

    expect(fetchMock).toHaveBeenCalledWith(
      "/api/data/operations/phase3-readiness",
      undefined,
    );
  });
});
