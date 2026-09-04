import { expect, test } from "@playwright/test";

const distribution = {
  metric: "session_return",
  count: 2,
  minimum: -0.01,
  maximum: 0.03,
  display_minimum: -0.01,
  display_maximum: 0.03,
  percentiles: { p10: -0.006, p25: 0, p50: 0.01, p75: 0.02, p90: 0.026 },
  bins: [
    { start: -0.01, end: 0.01, count: 1, percentile_min: 50, percentile_max: 50, lower_overflow: false, upper_overflow: false },
    { start: 0.01, end: 0.03, count: 1, percentile_min: 100, percentile_max: 100, lower_overflow: false, upper_overflow: false },
  ],
};

test.beforeEach(async ({ page }) => {
  await page.route("**/api/opportunities/daily?**", async (route) => {
    await route.fulfill({
      contentType: "application/json",
      body: JSON.stringify({
        exchange: "NSE",
        source: "yfinance",
        target_version: "daily_opportunity_outcomes_v1_0",
        selection_mode: "automatic",
        requested_session_date: null,
        session_date: "2026-08-24",
        session_exists: true,
        latest_available_date: "2026-08-25",
        latest_complete_date: "2026-08-24",
        available_sessions: [
          { date: "2026-08-25", instruments: 1, expected_instruments: 2, coverage_ratio: 0.5, coverage_status: "partial" },
          { date: "2026-08-24", instruments: 2, expected_instruments: 2, coverage_ratio: 1, coverage_status: "complete" },
        ],
        session_instruments: 2,
        expected_instruments: 2,
        coverage_ratio: 1,
        coverage_status: "complete",
        total: 0,
        session_total: 2,
        summary: { return_band_sessions: 0, return_band_eligible_sessions: 2 },
        percentile_filters: {},
        distributions: { session_return: distribution },
        rows: [],
      }),
    });
  });
});

test("renders latest-complete warning and a usable distribution chart", async ({ page }) => {
  await page.goto("/opportunities");
  await expect(page.getByRole("heading", { name: "Explore completed-session opportunities" })).toBeVisible();
  await expect(page.getByLabel("Coverage notice")).toContainText("Showing the latest complete session");
  const chart = page.locator(".opportunity-distribution-chart").first();
  await expect(chart).toBeVisible();
  const box = await chart.boundingBox();
  expect(box?.width ?? 0).toBeGreaterThan(250);
  await expect(page.getByText("No symbols match the current search and percentile filters.")).toBeVisible();
});
