import { Search } from "lucide-react";
import { FormEvent, useMemo, useState } from "react";

import { useChatAudit, useChatHealth, useChatQuery, useChatSources, useResearchNotes } from "../api/hooks";
import type { ChatExchange, ChatQueryRequest } from "../api/types";
import { EmptyState, LoadingState } from "../components/DataState";
import { PageHeader } from "../components/PageHeader";
import { ResearchList } from "../components/ResearchList";
import { formatDateTime } from "../utils/format";

export function ResearchPage() {
  const notesQuery = useResearchNotes();
  const healthQuery = useChatHealth();
  const chatMutation = useChatQuery();
  const [message, setMessage] = useState("");
  const [exchange, setExchange] = useState<ChatExchange>("NSE");
  const [strictQuality, setStrictQuality] = useState(true);
  const latestResponse = chatMutation.data ?? null;
  const sourcesQuery = useChatSources(latestResponse?.response_id ?? null);
  const auditQuery = useChatAudit(latestResponse?.response_id ?? null);
  const canSend = useMemo(
    () => message.trim().length > 0 && !chatMutation.isPending && healthQuery.data?.enabled,
    [message, chatMutation.isPending, healthQuery.data?.enabled],
  );

  const onSubmit = (event: FormEvent<HTMLFormElement>) => {
    event.preventDefault();
    const payload: ChatQueryRequest = {
      message: message.trim(),
      context: { exchange, symbols: [], timezone: Intl.DateTimeFormat().resolvedOptions().timeZone },
      options: { max_latency_ms: 8000, strict_quality: strictQuality },
      session_id: "web-ui-session",
      user_id: "web-ui-user",
    };
    chatMutation.mutate(payload);
  };

  return (
    <>
      <PageHeader
        eyebrow="Research"
        title="Analyst Chat"
        subtitle="Ask market and research questions with quality badges and source provenance."
        actions={
          <button className="icon-button" type="button">
            <Search size={16} />
            Source Explorer
          </button>
        }
      />
      <section className="panel chat-panel">
        <div className="panel-header">
          <h2>Trade Analyst Assistant</h2>
        </div>
        <div className="chat-controls">
          <label>
            Exchange
            <select value={exchange} onChange={(event) => setExchange(event.target.value as ChatExchange)}>
              <option value="NSE">NSE</option>
              <option value="TSX">TSX</option>
              <option value="BOTH">Both</option>
            </select>
          </label>
          <label className="checkbox-label">
            <input
              type="checkbox"
              checked={strictQuality}
              onChange={(event) => setStrictQuality(event.target.checked)}
            />
            Strict quality mode
          </label>
          <span className="muted">
            Chat: {healthQuery.data?.enabled ? "enabled" : "disabled"} | Citations required:{" "}
            {healthQuery.data?.strictCitationRequired ? "yes" : "no"}
          </span>
        </div>
        <div className="chat-ask-guide">
          <h3>What You Can Ask</h3>
          <ul>
            <li>How did NSE perform in the latest complete session?</li>
            <li>Compare BOTH exchanges quality snapshot.</li>
            <li>Show data quality for TSX (coverage, backlog, stale windows).</li>
            <li>For RELIANCE.NS, show hourly move in the last 24 hours.</li>
            <li>Why did NSE move today? (market + research context)</li>
            <li>Find research notes about Indian banking momentum.</li>
          </ul>
        </div>
        <form className="chat-form" onSubmit={onSubmit}>
          <textarea
            value={message}
            onChange={(event) => setMessage(event.target.value)}
            placeholder="Ask: How did NSE perform in the latest complete session?"
            rows={4}
          />
          <button className="icon-button" type="submit" disabled={!canSend}>
            {chatMutation.isPending ? "Running..." : "Ask Analyst Chat"}
          </button>
        </form>

        {chatMutation.isError ? (
          <div className="state">Unable to run chat query right now.</div>
        ) : null}

        {latestResponse ? (
          <div className="chat-response">
            <div className={`status-pill quality-${latestResponse.answer.quality_badge}`}>
              {latestResponse.answer.quality_badge}
            </div>
            <p>{latestResponse.answer.text}</p>
            <div className="chat-meta">
              <span>
                Market freshness:{" "}
                {latestResponse.answer.freshness.market_data_as_of
                  ? formatDateTime(latestResponse.answer.freshness.market_data_as_of)
                  : "n/a"}
              </span>
              <span>
                Research freshness:{" "}
                {latestResponse.answer.freshness.research_data_as_of
                  ? formatDateTime(latestResponse.answer.freshness.research_data_as_of)
                  : "n/a"}
              </span>
            </div>
            {latestResponse.answer.warnings.length ? (
              <ul className="chat-warnings">
                {latestResponse.answer.warnings.map((warning) => (
                  <li key={warning}>{warning}</li>
                ))}
              </ul>
            ) : null}
            {latestResponse.citations.length ? (
              <div className="chat-citations">
                {latestResponse.citations.map((citation) => (
                  <span key={citation.id} className="badge">
                    {citation.type}: {citation.label}
                  </span>
                ))}
              </div>
            ) : null}
            <details className="chat-sources">
              <summary>View Provenance</summary>
              {sourcesQuery.isLoading ? <p>Loading sources...</p> : null}
              {sourcesQuery.data ? (
                <>
                  <h4>Timescale Queries</h4>
                  {sourcesQuery.data.sources.timescale.length ? (
                    <ul>
                      {sourcesQuery.data.sources.timescale.map((source) => (
                        <li key={source.provenance_ref}>
                          {source.template_id} | rows: {source.row_count} | executed:{" "}
                          {formatDateTime(source.executed_at)}
                        </li>
                      ))}
                    </ul>
                  ) : (
                    <p>No market-data sources.</p>
                  )}
                  <h4>Research Chunks</h4>
                  {sourcesQuery.data.sources.qdrant.length ? (
                    <ul>
                      {sourcesQuery.data.sources.qdrant.map((source) => (
                        <li key={source.provenance_ref}>
                          {source.title ?? source.doc_id} | score: {source.score.toFixed(2)}
                        </li>
                      ))}
                    </ul>
                  ) : (
                    <p>No research sources.</p>
                  )}
                </>
              ) : null}
            </details>
            <details className="chat-sources">
              <summary>Debug Audit</summary>
              {auditQuery.isLoading ? <p>Loading audit...</p> : null}
              {auditQuery.data ? (
                <pre className="chat-audit-json">
                  {JSON.stringify(
                    {
                      recorded_at: auditQuery.data.recorded_at,
                      plan: auditQuery.data.plan,
                      errors: auditQuery.data.tool_outputs?.errors ?? [],
                    },
                    null,
                    2,
                  )}
                </pre>
              ) : null}
            </details>
          </div>
        ) : null}
      </section>
      <section className="panel">
        {notesQuery.isLoading ? (
          <LoadingState />
        ) : notesQuery.data?.length ? (
          <ResearchList notes={notesQuery.data} />
        ) : (
          <EmptyState label="No research notes available." />
        )}
      </section>
    </>
  );
}
