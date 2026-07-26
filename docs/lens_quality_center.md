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

`GET /api/filings/investigations/{analysis_id}/validation` recomputes seven gates
from the stored run:

1. durable graph execution reached a valid terminal state;
2. the LLM plan satisfies the bounded Pydantic schema;
3. universe counts and exclusions reconcile;
4. the exact tool allowlist, arguments, order, and call budget are respected;
5. period pairing, ranking order, and percentage arithmetic recompute exactly;
6. every ranked fact resolves to versioned evidence with a source hash;
7. every displayed claim passed deterministic citation validation.

Validation is automatic and read-only. A failed gate remains visible and is not
hidden behind an aggregate score.

## Persisted evaluation pipeline

`POST /api/filings/investigations/{analysis_id}/evaluations` runs evaluator
`filing-investigation-evaluator-v1`, records its report in
`filing_investigation_evaluations`, and writes an audit event. The suites are:

- plan quality, including provider success and fallback detection;
- tool selection, argument accuracy, order, and budget;
- structured retrieval quality, period pairs, ranking, math, and exclusions;
- exact evidence resolution and citation-bound claim quality;
- the locked 13-quarter INFY extraction baseline (52 values and evidence links).

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
7. In **Validation and evaluation**, confirm all seven runtime gates.
8. Select **Run quality evaluation** and show the persisted scorecard, evaluator
   version, evaluation ID, and Langfuse-compatible trace ID.

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
