import { KeyRound, Save, ShieldCheck } from "lucide-react";
import { FormEvent, useState } from "react";
import { useQueryClient } from "@tanstack/react-query";

import { PageHeader } from "../components/PageHeader";
import {
  useSaveUpstoxCredential,
  useTestUpstoxCredential,
  useUpstoxCredentialStatus,
} from "../api/hooks";

export function ProviderSettingsPage() {
  const queryClient = useQueryClient();
  const statusQuery = useUpstoxCredentialStatus();
  const testMutation = useTestUpstoxCredential();
  const saveMutation = useSaveUpstoxCredential();
  const [token, setToken] = useState("");
  const [validateOnSave, setValidateOnSave] = useState(true);

  const status = statusQuery.data;
  const canSubmit = token.trim().length >= 20 && !saveMutation.isPending;

  function handleTest() {
    testMutation.mutate({ access_token: token.trim() || null });
  }

  function handleSave(event: FormEvent<HTMLFormElement>) {
    event.preventDefault();
    saveMutation.mutate(
      { access_token: token.trim(), validate: validateOnSave },
      {
        onSuccess: () => {
          setToken("");
          void queryClient.invalidateQueries({
            queryKey: ["admin-provider-credentials", "upstox"],
          });
          void queryClient.invalidateQueries({ queryKey: ["data-pipeline-health"] });
        },
      },
    );
  }

  return (
    <>
      <PageHeader
        eyebrow="Settings"
        title="Provider Credentials"
        subtitle="Upstox access"
      />

      <section className="settings-grid">
        <div className="panel provider-credential-panel">
          <div className="panel-header">
            <div>
              <h2>Upstox</h2>
              <span>Daily OHLCV provider</span>
            </div>
            <StatusPill
              status={
                statusQuery.isError
                  ? "failed"
                  : status?.configured
                    ? "completed"
                    : "warning"
              }
              label={
                statusQuery.isError
                  ? "Blocked"
                  : status?.configured
                    ? "Configured"
                    : "Missing"
              }
            />
          </div>

          <dl className="credential-meta">
            <div>
              <dt>Source</dt>
              <dd>{status?.source ?? "-"}</dd>
            </div>
            <div>
              <dt>Updated</dt>
              <dd>{formatDate(status?.updated_at)}</dd>
            </div>
            <div>
              <dt>Updated By</dt>
              <dd>{status?.updated_by ?? "-"}</dd>
            </div>
            <div>
              <dt>Validation</dt>
              <dd>{status?.validation_status ?? "-"}</dd>
            </div>
          </dl>

          {status?.validation_message ? (
            <p className="credential-note">{status.validation_message}</p>
          ) : null}
          {statusQuery.isError ? (
            <p className="form-error">{errorMessage(statusQuery.error)}</p>
          ) : null}
        </div>

        <form className="panel data-request-form provider-token-form" onSubmit={handleSave}>
          <label htmlFor="upstox-token">
            Access Token
            <textarea
              id="upstox-token"
              value={token}
              onChange={(event) => setToken(event.target.value)}
              placeholder="Paste Upstox access token"
              spellCheck={false}
            />
          </label>

          <label className="checkbox-label">
            <input
              type="checkbox"
              checked={validateOnSave}
              onChange={(event) => setValidateOnSave(event.target.checked)}
            />
            Validate before saving
          </label>

          <div className="request-actions">
            <button
              className="icon-button"
              type="button"
              disabled={testMutation.isPending}
              onClick={handleTest}
            >
              <ShieldCheck size={16} />
              Test
            </button>
            <button
              className="icon-button primary"
              type="submit"
              disabled={!canSubmit}
            >
              <Save size={16} />
              Save
            </button>
          </div>

          {testMutation.data ? (
            <p className={testMutation.data.valid ? "form-success" : "form-error"}>
              {testMutation.data.message}
            </p>
          ) : null}
          {testMutation.isError ? (
            <p className="form-error">{errorMessage(testMutation.error)}</p>
          ) : null}
          {saveMutation.isSuccess ? (
            <p className="form-success">Token saved.</p>
          ) : null}
          {saveMutation.isError ? (
            <p className="form-error">{errorMessage(saveMutation.error)}</p>
          ) : null}
        </form>
      </section>
    </>
  );
}

function StatusPill({ status, label }: { status: string; label: string }) {
  return (
    <span className={`status-pill ${status}`}>
      <KeyRound size={14} />
      {label}
    </span>
  );
}

function formatDate(value: string | null | undefined) {
  if (!value) return "-";
  return new Intl.DateTimeFormat(undefined, {
    dateStyle: "medium",
    timeStyle: "short",
  }).format(new Date(value));
}

function errorMessage(error: unknown) {
  return error instanceof Error ? error.message : "Request failed";
}
