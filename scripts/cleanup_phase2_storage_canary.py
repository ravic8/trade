from __future__ import annotations

import argparse
import json

from trade_research.config import Settings
from trade_research.storage.canary import cleanup_canary
from trade_research.storage.clickhouse import create_clickhouse_client


def main() -> None:
    parser = argparse.ArgumentParser(description="Remove one explicit Phase 2 ClickHouse canary")
    parser.add_argument("--source-run-id", required=True)
    parser.add_argument("--workspace-id", default="phase2-canary")
    parser.add_argument("--apply", action="store_true")
    args = parser.parse_args()
    if not args.apply:
        raise SystemExit("cleanup is mutation-only; pass --apply after reviewing the run id")
    settings = Settings()
    if not settings.clickhouse_enabled or not settings.clickhouse_write_enabled:
        raise SystemExit(
            "cleanup requires CLICKHOUSE_ENABLED=true and CLICKHOUSE_WRITE_ENABLED=true"
        )
    deleted = cleanup_canary(
        create_clickhouse_client(settings),
        source_run_id=args.source_run_id,
        database=settings.clickhouse_database,
        workspace_id=args.workspace_id,
    )
    print(json.dumps({"source_run_id": args.source_run_id, "deleted_rows": deleted}, indent=2))


if __name__ == "__main__":
    main()
