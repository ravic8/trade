import { RefreshCw } from "lucide-react";

import { useScreenerResults } from "../api/hooks";
import { EmptyState, LoadingState } from "../components/DataState";
import { PageHeader } from "../components/PageHeader";
import { ScreenerTable } from "../components/ScreenerTable";

export function ScreenersPage() {
  const query = useScreenerResults();
  const results = query.data ?? [];

  return (
    <>
      <PageHeader
        eyebrow="Screeners"
        title="Intraday Range V1"
        subtitle="Liquid names with repeated two-way range expansion and limited close/volume shock."
        actions={
          <button className="icon-button" type="button" onClick={() => void query.refetch()}>
            <RefreshCw size={16} />
            Refresh
          </button>
        }
      />
      <section className="panel">
        {query.isLoading ? (
          <LoadingState />
        ) : results.length ? (
          <ScreenerTable data={results} />
        ) : (
          <EmptyState label="No screener matches." />
        )}
      </section>
    </>
  );
}
