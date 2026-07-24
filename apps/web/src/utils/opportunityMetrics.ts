import type { OpportunityDistributionMetric } from "../api/types";

export const opportunityMetricConfig: Record<
  OpportunityDistributionMetric,
  { label: string; description: string; color: string }
> = {
  session_return: {
    label: "Return",
    description: "(Close − Open) / Open",
    color: "#08734f",
  },
  recovery: {
    label: "Recovery",
    description: "(Close − Low) / Open",
    color: "#1877a8",
  },
  upside: {
    label: "Upside",
    description: "(High − Open) / Open",
    color: "#168a62",
  },
  downside: {
    label: "Downside",
    description: "(Open − Low) / Open",
    color: "#c2413a",
  },
  giveback: {
    label: "Giveback",
    description: "(High − Close) / Open",
    color: "#b77716",
  },
  true_range: {
    label: "True range",
    description: "True upside + true downside",
    color: "#6d5ea8",
  },
};
