# Phase 3 Aggregation Golden Contract

The locked fixture at
`evaluations/market_data/nse_intraday_aggregation_v1.json` is the language-neutral
contract for an optional future Rust aggregation kernel. Python remains the
authoritative implementation.

The fixture uses column arrays rather than per-row objects so another runtime
can consume it without Python-specific serialization. All timestamps are UTC
ISO-8601 values, decimal prices are strings, volume/counts are integers, and
output ordering is ascending by bucket timestamp. The declared `source_columns`
and `output_columns` order is part of the schema.

The eight required cases cover `5m`, `15m`, `30m`, and `1h`, each with complete
buckets only and with incomplete buckets included. The data locks these rules:

- buckets are anchored to the 09:15 Asia/Kolkata NSE session open;
- open/close follow timestamp order, high/low use extrema, and volume is summed;
- an absent source minute makes the bucket incomplete;
- the final session bucket ends at 15:30, so the last `30m` and `1h` buckets
  correctly expect only 15 source minutes;
- source digests and raw-artifact lineage are deterministic.

Validate the Python reference:

```bash
trade-research verify-market-data-aggregation-golden
```

A future Rust binary must emit JSON containing the same `schema_version` and an
`output` object keyed by the eight case IDs. Validate it without executing
arbitrary binaries inside the application:

```bash
trade-research verify-market-data-aggregation-golden \
  --candidate-output /path/to/rust-output.json
```

The production Rust path must remain disabled until its independently produced
output matches every locked row exactly. No floating-point tolerance is used;
prices cross the boundary as decimal strings.
