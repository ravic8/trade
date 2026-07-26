import {
  Bot,
  CheckCircle2,
  CircleAlert,
  FileSearch,
  LoaderCircle,
  Network,
  ShieldCheck,
  X,
} from "lucide-react";
import { FormEvent, useMemo, useState } from "react";

import {
  useFilingInvestigation,
  useFilingInvestigationEvents,
  useFilingUniverseCoverage,
  useSubmitFilingInvestigation,
} from "../api/hooks";
import type {
  FilingInvestigationCitation,
  FilingInvestigationRequest,
} from "../api/types";
import { PageHeader } from "../components/PageHeader";
import { formatDateTime } from "../utils/format";

const DEFAULT_QUESTION =
  "Across the Nifty 50, rank companies by year-over-year net-profit growth for their latest comparable consolidated quarter. Explain coverage and cite every result.";

const PRESETS = [
  {
    label: "Net profit leaders",
    question: DEFAULT_QUESTION,
  },
  {
    label: "Revenue leaders",
    question:
      "Rank comparable Nifty 50 companies by year-over-year revenue growth using approved consolidated quarterly filings.",
  },
  {
    label: "EPS momentum",
    question:
      "Which Nifty 50 companies have the strongest quarter-over-quarter basic EPS growth in approved consolidated filings?",
  },
];

export function LensAgentPage() {
  const coverageQuery = useFilingUniverseCoverage();
  const submitMutation = useSubmitFilingInvestigation();
  const [question, setQuestion] = useState(DEFAULT_QUESTION);
  const [comparison, setComparison] =
    useState<FilingInvestigationRequest["comparison"]>("auto");
  const [strictEvidence, setStrictEvidence] = useState(true);
  const [analysisId, setAnalysisId] = useState<string | null>(null);
  const [selectedCitation, setSelectedCitation] =
    useState<FilingInvestigationCitation | null>(null);
  const runQuery = useFilingInvestigation(analysisId);
  const run = runQuery.data ?? submitMutation.data?.run ?? null;
  const result = run?.result_payload;
  const terminal = Boolean(
    run && ["completed", "partial", "abstained", "failed"].includes(run.status),
  );
  const eventsQuery = useFilingInvestigationEvents(analysisId, terminal);
  const canSubmit = question.trim().length >= 8 && !submitMutation.isPending;
  const coverage = coverageQuery.data;
  const coverageValue = (value: number | undefined) =>
    coverageQuery.isPending ? "…" : coverageQuery.isError ? "—" : (value ?? 0);
  const statusLabel = run?.status.replaceAll("_", " ") ?? "ready";

  const citationMap = useMemo(
    () =>
      new Map(
        (result?.citations ?? []).map((citation) => [
          citation.citation_id,
          citation,
        ]),
      ),
    [result?.citations],
  );

  const submit = (event: FormEvent<HTMLFormElement>) => {
    event.preventDefault();
    if (!canSubmit) return;
    setSelectedCitation(null);
    submitMutation.mutate(
      {
        question: question.trim(),
        universe_id: "NIFTY50",
        strict_evidence: strictEvidence,
        max_tool_calls: 8,
        comparison,
      },
      {
        onSuccess: (submission) => setAnalysisId(submission.run.analysis_id),
      },
    );
  };

  return (
    <div className="lens-agent">
      <PageHeader
        eyebrow="Lens"
        title="Nifty 50 Filing Agent"
        subtitle="A bounded investigation agent operating only on approved financial facts and exact filing evidence."
        actions={
          <span className="lens-trust-badge">
            <ShieldCheck size={16} />
            Strict evidence
          </span>
        }
      />

      <section className="lens-coverage-strip" aria-label="Nifty 50 filing coverage">
        <CoverageCard
          label="Universe"
          value={coverageValue(coverage?.member_count)}
          detail={coverage?.snapshot_id ? "Versioned snapshot" : "Derived from filings"}
        />
        <CoverageCard
          label="Represented"
          value={coverageValue(coverage?.represented_company_count)}
          detail="Approved core facts"
        />
        <CoverageCard
          label="Eligible"
          value={coverageValue(coverage?.eligible_company_count)}
          detail="Two or more periods"
          positive
        />
        <CoverageCard
          label="Excluded"
          value={coverageValue(coverage?.excluded_company_count)}
          detail="Disclosed, never hidden"
        />
      </section>
      {coverageQuery.isError ? (
        <div className="lens-error lens-coverage-error">
          <CircleAlert size={17} />
          Filing coverage is unavailable. The agent will not present zero as verified
          coverage.
        </div>
      ) : null}

      <div className="lens-layout">
        <section className="panel lens-composer">
          <div className="panel-header">
            <div>
              <h2>Investigation objective</h2>
              <p>LLM planning and synthesis; deterministic retrieval and arithmetic.</p>
            </div>
            <Bot size={20} />
          </div>
          <div className="lens-presets">
            {PRESETS.map((preset) => (
              <button
                key={preset.label}
                type="button"
                onClick={() => setQuestion(preset.question)}
              >
                {preset.label}
              </button>
            ))}
          </div>
          <form onSubmit={submit} className="lens-form">
            <textarea
              rows={5}
              value={question}
              onChange={(event) => setQuestion(event.target.value)}
              aria-label="Investigation question"
            />
            <div className="lens-form-controls">
              <label>
                Comparison
                <select
                  value={comparison}
                  onChange={(event) =>
                    setComparison(
                      event.target.value as FilingInvestigationRequest["comparison"],
                    )
                  }
                >
                  <option value="auto">Agent decides</option>
                  <option value="yoy">Year over year</option>
                  <option value="qoq">Quarter over quarter</option>
                </select>
              </label>
              <label className="checkbox-label">
                <input
                  type="checkbox"
                  checked={strictEvidence}
                  onChange={(event) => setStrictEvidence(event.target.checked)}
                />
                Require complete evidence
              </label>
              <button className="lens-run-button" type="submit" disabled={!canSubmit}>
                {submitMutation.isPending ? (
                  <LoaderCircle className="spin" size={17} />
                ) : (
                  <Network size={17} />
                )}
                Run investigation
              </button>
            </div>
          </form>
          {submitMutation.isError ? (
            <div className="lens-error">
              <CircleAlert size={17} />
              {submitMutation.error.message}
            </div>
          ) : null}
        </section>

        <section className="panel lens-run-panel">
          <div className="panel-header">
            <div>
              <h2>Agent execution</h2>
              <p>
                {analysisId
                  ? `Run ${analysisId.slice(0, 8)}`
                  : "Submit an objective to start a durable graph run."}
              </p>
            </div>
            <span className={`status-pill ${terminal ? "completed" : "running"}`}>
              {statusLabel}
            </span>
          </div>
          {run ? (
            <>
              <div className="lens-progress">
                <span style={{ width: `${Math.round(run.progress * 100)}%` }} />
              </div>
              <ol className="lens-timeline">
                {(eventsQuery.data ?? []).map((event) => (
                  <li key={event.event_id}>
                    <CheckCircle2 size={16} />
                    <div>
                      <strong>{event.node.replaceAll("_", " ")}</strong>
                      <span>{eventSummary(event.detail)}</span>
                    </div>
                    <time>{formatDateTime(event.created_at)}</time>
                  </li>
                ))}
              </ol>
              {run.error_message ? (
                <div className="lens-error">{run.error_message}</div>
              ) : null}
            </>
          ) : (
            <div className="lens-empty">
              <Network size={30} />
              Graph nodes and bounded tool calls will appear here.
            </div>
          )}
        </section>
      </div>

      {result?.synthesis ? (
        <section className="panel lens-answer">
          <div className="panel-header">
            <div>
              <h2>{result.synthesis.title}</h2>
              <p>
                {result.synthesis.model_used
                  ? `${result.synthesis.provider} · ${result.synthesis.model}`
                  : "Deterministic fallback"}
                {" · "}
                {result.prompt_version}
              </p>
            </div>
            <span
              className={`status-pill ${
                result.claim_validation?.passed ? "completed" : "warning"
              }`}
            >
              {result.claim_validation?.passed ? "Claims verified" : "Partial"}
            </span>
          </div>
          <p className="lens-summary">{result.synthesis.summary}</p>
          <div className="lens-claims">
            {result.synthesis.claims.map((claim, index) => (
              <article key={`${claim.text}-${index}`}>
                <p>{claim.text}</p>
                <div>
                  {claim.citation_ids.map((citationId) => (
                    <button
                      key={citationId}
                      type="button"
                      onClick={() =>
                        setSelectedCitation(citationMap.get(citationId) ?? null)
                      }
                    >
                      [{citationId}]
                    </button>
                  ))}
                </div>
              </article>
            ))}
          </div>
          {result.synthesis.limitations.length ? (
            <div className="lens-limitations">
              <strong>Coverage limitations</strong>
              <ul>
                {result.synthesis.limitations.map((item) => (
                  <li key={item}>{item}</li>
                ))}
              </ul>
            </div>
          ) : null}
        </section>
      ) : null}

      {result?.ranking ? (
        <section className="panel lens-results">
          <div className="panel-header">
            <div>
              <h2>Validated ranking</h2>
              <p>
                {result.ranking.eligible_count} comparable companies ·{" "}
                {result.ranking.excluded_count} excluded
              </p>
            </div>
            <FileSearch size={20} />
          </div>
          <div className="lens-table-wrap">
            <table>
              <thead>
                <tr>
                  <th>Rank</th>
                  <th>Company</th>
                  <th>Period</th>
                  <th>Current</th>
                  <th>Comparison</th>
                  <th>Change</th>
                  <th>Evidence</th>
                </tr>
              </thead>
              <tbody>
                {result.ranking.rows.map((row, index) => (
                  <tr key={row.company_id}>
                    <td>{index + 1}</td>
                    <td>
                      <strong>{row.symbol}</strong>
                      <span>{row.name}</span>
                    </td>
                    <td>
                      {row.current_period}
                      <span>vs {row.comparison_period}</span>
                    </td>
                    <td>{formatValue(row.current_value, row.currency)}</td>
                    <td>{formatValue(row.comparison_value, row.currency)}</td>
                    <td
                      className={
                        Number(row.percent_change) >= 0
                          ? "lens-positive"
                          : "lens-negative"
                      }
                    >
                      {Number(row.percent_change) >= 0 ? "+" : ""}
                      {row.percent_change}%
                    </td>
                    <td>
                      <div className="lens-citation-buttons">
                        {row.citation_ids.map((citationId) => (
                          <button
                            key={citationId}
                            type="button"
                            onClick={() =>
                              setSelectedCitation(
                                citationMap.get(citationId) ?? null,
                              )
                            }
                          >
                            {citationId}
                          </button>
                        ))}
                      </div>
                    </td>
                  </tr>
                ))}
              </tbody>
            </table>
          </div>
        </section>
      ) : null}

      {selectedCitation ? (
        <aside className="lens-evidence-drawer" aria-label="Filing evidence">
          <div className="lens-evidence-header">
            <div>
              <span>Evidence {selectedCitation.citation_id}</span>
              <h2>{selectedCitation.label}</h2>
            </div>
            <button type="button" onClick={() => setSelectedCitation(null)}>
              <X size={18} />
            </button>
          </div>
          <dl>
            <dt>Filing</dt>
            <dd>{selectedCitation.filing_id}</dd>
            <dt>Version</dt>
            <dd>{selectedCitation.filing_version}</dd>
            <dt>Period</dt>
            <dd>{selectedCitation.period_end}</dd>
            <dt>Fact ID</dt>
            <dd>{selectedCitation.fact_id}</dd>
          </dl>
          {selectedCitation.evidence.map((evidence) => (
            <article key={evidence.evidence_id}>
              <span>{evidence.section_path ?? "filing"}</span>
              <strong>{evidence.xbrl_concept ?? evidence.row_label ?? "Evidence"}</strong>
              <p>{evidence.snippet ?? "Exact source reference recorded."}</p>
              <dl>
                <dt>Context</dt>
                <dd>{evidence.context_ref ?? "n/a"}</dd>
                <dt>Source hash</dt>
                <dd>{evidence.source_hash}</dd>
              </dl>
            </article>
          ))}
        </aside>
      ) : null}
    </div>
  );
}

function CoverageCard({
  label,
  value,
  detail,
  positive = false,
}: {
  label: string;
  value: number | string;
  detail: string;
  positive?: boolean;
}) {
  return (
    <article className={positive ? "positive" : ""}>
      <span>{label}</span>
      <strong>{value}</strong>
      <small>{detail}</small>
    </article>
  );
}

function eventSummary(detail: Record<string, unknown>): string {
  const useful = Object.entries(detail)
    .filter(([key]) => key !== "status")
    .slice(0, 3)
    .map(([key, value]) => `${key.replaceAll("_", " ")}: ${String(value)}`);
  return useful.join(" · ") || String(detail.status ?? "completed");
}

function formatValue(value: string, currency?: string | null): string {
  const numeric = Number(value);
  const rendered = Number.isFinite(numeric)
    ? new Intl.NumberFormat("en-IN", { maximumFractionDigits: 2 }).format(numeric)
    : value;
  return currency ? `${currency} ${rendered}` : rendered;
}
