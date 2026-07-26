# Lens validation and evaluation center

## Purpose

The `/lens` demo now exposes the quality controls behind the Nifty 50 filing
investigation agent. A user can submit an objective, watch the durable LangGraph
run, inspect the typed plan and bounded tool trajectory, open exact filing
evidence, review all exclusions, and run a persisted production scorecard.

This workflow uses structured financial retrieval over approved PostgreSQL facts.
It does not claim a semantic/vector retrieval score while the Qdrant filing index
is disabled.

## Runtime validation gates

`GET /api/filings/investigations/{analysis_id}/validation` recomputes intent-aware
gates from the stored run. Every route checks:

1. durable graph execution reached a valid terminal state;
2. the LLM plan satisfies the bounded Pydantic schema;
3. the planned intent matches a separately versioned labeled utterance oracle,
   never the runtime routing policy itself;
4. the returned answer type and payload answer the requested intent;
5. universe counts and exclusions reconcile;
6. the route-specific tool allowlist, arguments, order, and call budget are
   respected.

Financial-analysis routes additionally recompute period pairing, ranking order,
percentage arithmetic, exact evidence, and citation-bound claims. Coverage and
capability routes instead validate the complete live company inventory or the
versioned system capability contract. A schema-valid but semantically wrong LLM
plan is recorded as `semantic_mismatch` only when a high-confidence routing rule
applies. Low-confidence wording is allowed to use the structured provider plan
instead of being silently coerced into a financial ranking. If the evaluation
oracle has no sufficiently similar labeled case, semantic intent and answer
relevance are reported as `not_evaluated`; the scorecard cannot claim a pass.

Validation is automatic and read-only. A failed gate remains visible and is not
hidden behind an aggregate score.

## Persisted evaluation pipeline

`POST /api/filings/investigations/{analysis_id}/evaluations` runs evaluator
`filing-investigation-evaluator-v3`, records its report in
`filing_investigation_evaluations`, and writes an audit event. The suites are:

- LLM intent and plan quality, including semantic alignment, provider success,
  and fallback detection;
- question-answer relevance as an independent hard gate;
- the locked `nifty50-intent-routing-v1` regression set, including UI presets
  and paraphrases for coverage, capabilities, limitations, rankings, and direct
  comparisons;
- tool selection, argument accuracy, order, and budget;
- route-specific system-contract quality for coverage/capability questions, or
  structured retrieval, exact evidence, and grounded-claim quality for financial
  questions;
- the locked 13-quarter INFY extraction baseline (52 values and evidence links).

The extraction baseline references the immutable
`evaluations/filings/infy_m1_locked_sources.json` provenance index instead of the
mutable acquisition manifest. Its exact hash is locked by the golden dataset;
additive compatibility remains supported for controlled fixture evolution, while
a missing or changed locked source still fails.

The planner may perform one bounded structured repair when its first response is
schema-invalid or conflicts with a high-confidence routing policy. Repair attempts, original failure
codes, provider statuses, latency, and combined token usage are retained in
planner telemetry and exposed by the UI. A failed repair remains a release-gate
failure and uses the deterministic safety plan.

`GET /api/filings/investigations/{analysis_id}/evaluations/latest` returns the
latest immutable scorecard. Hard-gate failure makes the evaluation fail even when
the average score is otherwise high. A missing local golden file is shown as
`not_evaluated` and informational; the production acceptance check already
requires that file to exist.

## Demo flow

1. Open `/lens` and confirm 50 universe members and current represented coverage.
2. Submit the default year-over-year net-profit objective.
3. Show the LangGraph node timeline and the OpenAI plan metadata.
4. Show the exact three-tool trajectory and its arguments.
5. Open two citations from a ranked company and point to filing version, fact ID,
   XBRL concept/context, snippet, and source hash.
6. Show the disclosed exclusion cards and reason codes.
7. In **Validation and evaluation**, confirm intent alignment and answer relevance
   as well as the route-specific runtime gates.
8. Select **Run quality evaluation** and show the persisted scorecard, evaluator
   version, evaluation ID, and Langfuse-compatible trace ID.
9. Ask **For which Nifty 50 stocks do you currently have approved filing data?**
   and show the full represented and
   unavailable company inventories without a ranking tool call.
10. Ask **What are your capabilities and limitations?** and show the versioned,
    runtime-aware capability contract and current system boundaries.

## Deployment

Migration `20260726_0012` adds the evaluation table. Deploy normally so Alembic
runs before the API and worker are replaced. After deployment:

```bash
docker compose --env-file /opt/trade/.env -f docker-compose.prod.yml \
  exec -T api alembic current

docker compose --env-file /opt/trade/.env -f docker-compose.prod.yml \
  exec -T api trade-research verify-filing-production
```

The expected migration head is `20260726_0012`; production readiness must remain
green before the demo.
