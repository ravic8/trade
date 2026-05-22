import { useParams } from "react-router-dom";

import { useCandles, useResearchNotes } from "../api/hooks";
import { CandleChart } from "../components/CandleChart";
import { EmptyState, LoadingState } from "../components/DataState";
import { PageHeader } from "../components/PageHeader";
import { ResearchList } from "../components/ResearchList";

export function SymbolPage() {
  const { ticker = "" } = useParams();
  const decodedTicker = decodeURIComponent(ticker);
  const candlesQuery = useCandles(decodedTicker);
  const notesQuery = useResearchNotes(decodedTicker);

  return (
    <>
      <PageHeader
        eyebrow="Symbol"
        title={decodedTicker}
        subtitle="Price behavior, volume, and retrieved research context."
      />

      <section className="panel">
        <div className="panel-header">
          <h2>OHLCV</h2>
        </div>
        {candlesQuery.isLoading ? (
          <LoadingState />
        ) : candlesQuery.data?.length ? (
          <CandleChart data={candlesQuery.data} />
        ) : (
          <EmptyState label="No candles available." />
        )}
      </section>

      <section className="panel">
        <div className="panel-header">
          <h2>Research Context</h2>
        </div>
        {notesQuery.isLoading ? (
          <LoadingState />
        ) : notesQuery.data?.length ? (
          <ResearchList notes={notesQuery.data} />
        ) : (
          <EmptyState label="No research notes found." />
        )}
      </section>
    </>
  );
}
