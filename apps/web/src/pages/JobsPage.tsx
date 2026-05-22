import { useJobRuns } from "../api/hooks";
import { LoadingState } from "../components/DataState";
import { PageHeader } from "../components/PageHeader";
import { formatDateTime } from "../utils/format";

export function JobsPage() {
  const query = useJobRuns();
  const jobs = query.data ?? [];

  return (
    <>
      <PageHeader
        eyebrow="Pipelines"
        title="Job Runs"
        subtitle="Ingestion, screening, scraping, and embedding activity."
      />
      <section className="panel">
        {query.isLoading ? (
          <LoadingState />
        ) : (
          <div className="table-wrap">
            <table>
              <thead>
                <tr>
                  <th>Job</th>
                  <th>Status</th>
                  <th>Started</th>
                  <th>Duration</th>
                  <th>Items</th>
                </tr>
              </thead>
              <tbody>
                {jobs.map((job) => (
                  <tr key={job.id}>
                    <td><strong>{job.name}</strong></td>
                    <td><span className={`status-pill ${job.status}`}>{job.status}</span></td>
                    <td>{formatDateTime(job.startedAt)}</td>
                    <td>{job.durationSeconds === null ? "Running" : `${job.durationSeconds}s`}</td>
                    <td>{job.itemsProcessed.toLocaleString()}</td>
                  </tr>
                ))}
              </tbody>
            </table>
          </div>
        )}
      </section>
    </>
  );
}
