from __future__ import annotations

import argparse
import json
import os
from pathlib import Path

from trade_research.config import Settings
from trade_research.storage.capacity import collect_capacity_report


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Collect read-only Phase 2 capacity evidence")
    parser.add_argument("--projected-feature-count", type=int, required=True)
    parser.add_argument("--retention-years", type=int, default=10)
    parser.add_argument("--report", type=Path, required=True)
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    if not 1 <= args.projected_feature_count <= 10_000:
        raise SystemExit("projected feature count must be between 1 and 10000")
    if not 1 <= args.retention_years <= 100:
        raise SystemExit("retention years must be between 1 and 100")
    settings = Settings()
    report = collect_capacity_report(
        database_url=settings.database_url,
        paths={
            "postgres": Path(os.environ.get("PROD_POSTGRES_DATA_DIR", "/opt/trade/postgres")),
            "clickhouse": Path(
                os.environ.get("PROD_CLICKHOUSE_DATA_DIR", "/opt/trade/clickhouse")
            ),
            "minio": Path(os.environ.get("PROD_MINIO_DATA_DIR", "/opt/trade/minio")),
            "backups": Path(os.environ.get("PROD_BACKUP_DIR", "/opt/trade/backups")),
        },
        projected_feature_count=args.projected_feature_count,
        retention_years=args.retention_years,
    )
    args.report.parent.mkdir(parents=True, exist_ok=True)
    args.report.write_text(json.dumps(report, indent=2) + "\n", encoding="utf-8")
    print(json.dumps(report, indent=2))


if __name__ == "__main__":
    main()
