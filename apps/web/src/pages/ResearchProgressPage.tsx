import { useMemo, useState } from "react";
import {
  CheckCircle2,
  ChevronRight,
  FileWarning,
  GitBranch,
  TableProperties,
} from "lucide-react";

import { useResearchProgress } from "../api/hooks";
import type { ResearchProgressStep } from "../api/types";
import { EmptyState, LoadingState } from "../components/DataState";
import { MetricCard } from "../components/MetricCard";
import { PageHeader } from "../components/PageHeader";

function formatNumber(value: number | null): string {
  return value === null ? "n/a" : value.toLocaleString();
}

function formatRange(step: ResearchProgressStep): string {
  if (!step.date_min || !step.date_max) return "n/a";
  return `${step.date_min} -> ${step.date_max}`;
}

function statusClass(status: ResearchProgressStep["status"]): string {
  if (status === "done") return "completed";
  if (status === "warning") return "warning";
  return "failed";
}

type ArtifactFilter = "all" | "missing" | "present" | "json" | "csv" | "parquet";

function formatDetailValue(value: string | number | null): string {
  if (value === null) return "n/a";
  if (typeof value === "number") return value.toLocaleString();
  return value;
}

export function ResearchProgressPage() {
  const [selectedStepId, setSelectedStepId] = useState<string | null>(null);
  const [artifactFilter, setArtifactFilter] = useState<ArtifactFilter>("all");
  const progressQuery = useResearchProgress();
  const progress = progressQuery.data;
  const steps = useMemo(() => progress?.steps ?? [], [progress?.steps]);
  const artifactCount = steps.reduce((sum, step) => sum + step.artifacts.length, 0);
  const selectedStep =
    steps.find((step) => step.step_id === selectedStepId) ?? steps.find((step) => step.status === "warning") ?? steps[0];
  const artifacts = useMemo(
    () =>
      steps.flatMap((step) =>
        step.artifacts.map((artifact) => ({
          ...artifact,
          stepId: step.step_id,
          stepTitle: step.title,
        })),
      ),
    [steps],
  );
  const filteredArtifacts = artifacts.filter((artifact) => {
    if (artifactFilter === "all") return true;
    if (artifactFilter === "missing" || artifactFilter === "present") {
      return artifact.status === artifactFilter;
    }
    return artifact.kind === artifactFilter;
  });

  return (
    <>
      <PageHeader
        eyebrow="Research Pipeline"
        title="Progress Dashboard"
        subtitle="Read-only status of generated datasets, audits, and research artifacts."
      />
      <div className="metric-grid">
        <MetricCard
          icon={GitBranch}
          label="Steps"
          value={progress?.step_count.toString() ?? "0"}
          detail={`${progress?.completed_count ?? 0} completed`}
        />
        <MetricCard
          icon={CheckCircle2}
          label="Status"
          value={progress?.overall_status ?? "loading"}
          detail="Artifact-backed pipeline state"
        />
        <MetricCard
          icon={FileWarning}
          label="Missing"
          value={(progress?.missing_count ?? 0).toString()}
          detail="Required artifacts not found"
        />
        <MetricCard
          icon={TableProperties}
          label="Artifacts"
          value={artifactCount.toString()}
          detail="Files tracked by this view"
        />
      </div>

      <section className="panel">
        <div className="panel-header">
          <h2>Pipeline Steps</h2>
        </div>
        {progressQuery.isLoading ? (
          <LoadingState />
        ) : steps.length ? (
          <>
            <div className="pipeline-timeline" aria-label="Pipeline timeline">
              {steps.map((step, index) => (
                <button
                  className={`timeline-step ${statusClass(step.status)} ${
                    selectedStep?.step_id === step.step_id ? "active" : ""
                  }`}
                  key={step.step_id}
                  type="button"
                  onClick={() => setSelectedStepId(step.step_id)}
                >
                  <span>{index + 1}</span>
                  <strong>{step.title}</strong>
                </button>
              ))}
            </div>
            <div className="pipeline-grid">
              {steps.map((step) => (
                <button
                  className={`pipeline-card ${selectedStep?.step_id === step.step_id ? "active" : ""}`}
                  key={step.step_id}
                  type="button"
                  onClick={() => setSelectedStepId(step.step_id)}
                >
                  <div className="pipeline-card-header">
                    <span className={`status-pill ${statusClass(step.status)}`}>{step.status}</span>
                    <strong>{step.title}</strong>
                    <ChevronRight aria-hidden="true" size={18} />
                  </div>
                  <dl className="step-metrics">
                    <div>
                      <dt>Rows</dt>
                      <dd>{formatNumber(step.row_count)}</dd>
                    </div>
                    <div>
                      <dt>Symbols</dt>
                      <dd>{formatNumber(step.symbol_count)}</dd>
                    </div>
                    <div>
                      <dt>Date Range</dt>
                      <dd>{formatRange(step)}</dd>
                    </div>
                    <div>
                      <dt>Warnings</dt>
                      <dd>{formatNumber(step.warning_count)}</dd>
                    </div>
                    <div>
                      <dt>Failed</dt>
                      <dd>{formatNumber(step.failed_count)}</dd>
                    </div>
                  </dl>
                  <div className="command-line">{step.command}</div>
                  {step.timescale_tables.length ? (
                    <div className="tag-row">
                      {step.timescale_tables.map((table) => (
                        <span className="badge" key={table}>
                          {table}
                        </span>
                      ))}
                    </div>
                  ) : null}
                  {step.warning_explanation ? (
                    <p className="step-warning">{step.warning_explanation}</p>
                  ) : null}
                  {step.notes.length ? <p className="step-note">{step.notes[0]}</p> : null}
                </button>
              ))}
            </div>
            {selectedStep ? (
              <aside className="step-detail">
                <div>
                  <span className={`status-pill ${statusClass(selectedStep.status)}`}>
                    {selectedStep.status}
                  </span>
                  <h3>{selectedStep.title}</h3>
                  <p>{selectedStep.warning_explanation ?? selectedStep.notes[0]}</p>
                </div>
                <dl className="detail-grid">
                  {(selectedStep.detail_items ?? []).map((item) => (
                    <div key={item.label}>
                      <dt>{item.label}</dt>
                      <dd>{formatDetailValue(item.value)}</dd>
                    </div>
                  ))}
                  <div>
                    <dt>Last generated</dt>
                    <dd>{selectedStep.last_generated_at ?? "n/a"}</dd>
                  </div>
                </dl>
              </aside>
            ) : null}
          </>
        ) : (
          <EmptyState label="No pipeline progress data available." />
        )}
      </section>

      <section className="panel">
        <div className="panel-header">
          <h2>Artifacts</h2>
          <div className="segmented-control" aria-label="Artifact filter">
            {(["all", "missing", "present", "json", "csv", "parquet"] as ArtifactFilter[]).map(
              (filter) => (
                <button
                  className={artifactFilter === filter ? "active" : ""}
                  key={filter}
                  type="button"
                  onClick={() => setArtifactFilter(filter)}
                >
                  {filter}
                </button>
              ),
            )}
          </div>
        </div>
        <div className="table-wrap">
          <table>
            <thead>
              <tr>
                <th>Step</th>
                <th>Kind</th>
                <th>Status</th>
                <th>Path</th>
              </tr>
            </thead>
            <tbody>
              {filteredArtifacts.map((artifact) => (
                <tr key={`${artifact.stepId}-${artifact.path}`}>
                  <td>{artifact.stepTitle}</td>
                  <td>{artifact.kind}</td>
                  <td>
                    <span
                      className={`status-pill ${
                        artifact.status === "present" ? "completed" : "failed"
                      }`}
                    >
                      {artifact.status}
                    </span>
                  </td>
                  <td>{artifact.path}</td>
                </tr>
              ))}
            </tbody>
          </table>
        </div>
      </section>
    </>
  );
}
