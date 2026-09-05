from __future__ import annotations

import argparse
import json
from datetime import date
from pathlib import Path

from trade_research.config import Settings
from trade_research.control_plane.artifacts import ArtifactManifestRepository
from trade_research.storage.canary import (
    build_canary_payload,
    load_canary,
    reconcile_canary,
    run_benchmarks,
)
from trade_research.storage.clickhouse import create_clickhouse_client
from trade_research.storage.object_store import ArtifactNamespace, ObjectArtifactStore
from trade_research.storage.timescale import TimescaleStore


def _date(value: str) -> date:
    return date.fromisoformat(value)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Run the bounded Phase 2 storage canary")
    parser.add_argument("--start-date", type=_date, required=True)
    parser.add_argument("--end-date", type=_date, required=True)
    parser.add_argument("--exchange", default="NSE")
    parser.add_argument("--source", default="upstox")
    parser.add_argument("--instrument-limit", type=int, default=25)
    parser.add_argument("--row-limit", type=int, default=25_000)
    parser.add_argument("--iterations", type=int, default=5)
    parser.add_argument("--concurrency", type=int, default=4)
    parser.add_argument("--workspace-id", default="phase2-canary")
    parser.add_argument("--report", type=Path, required=True)
    parser.add_argument("--apply", action="store_true")
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    if args.start_date > args.end_date:
        raise SystemExit("start date must not be after end date")
    if (args.end_date - args.start_date).days > 366:
        raise SystemExit("canary date range must not exceed 366 days")
    if not 1 <= args.instrument_limit <= 100:
        raise SystemExit("instrument limit must be between 1 and 100")
    if not 1 <= args.row_limit <= 50_000:
        raise SystemExit("row limit must be between 1 and 50000")

    settings = Settings()
    timescale = TimescaleStore(settings.database_url)
    frame = timescale.daily_ohlcv_frame(
        exchange=args.exchange,
        source=args.source,
        limit=args.instrument_limit,
        start_date=args.start_date,
        end_date=args.end_date,
    )
    payload = build_canary_payload(
        frame,
        workspace_id=args.workspace_id,
        maximum_rows=args.row_limit,
    )
    report: dict[str, object] = {
        "schema_version": 1,
        "status": "planned",
        "authority": "PostgreSQL remains authoritative; no application reads changed",
        "bounds": {
            "start_date": args.start_date.isoformat(),
            "end_date": args.end_date.isoformat(),
            "exchange": args.exchange.upper(),
            "source": args.source.lower(),
            "instrument_limit": args.instrument_limit,
            "row_limit": args.row_limit,
        },
        "source": {
            "row_count": len(payload.ohlcv_rows),
            "feature_row_count": len(payload.feature_rows),
            "target_row_count": len(payload.target_rows),
            "content_sha256": payload.content_sha256,
            "source_run_id": payload.source_run_id,
        },
    }

    if args.apply:
        if not (
            settings.clickhouse_enabled
            and settings.clickhouse_write_enabled
            and settings.object_store_enabled
            and settings.object_store_write_enabled
        ):
            raise SystemExit(
                "--apply requires the ClickHouse and object-store enable/write gates"
            )
        client = create_clickhouse_client(settings)
        load_canary(client, payload, database=settings.clickhouse_database)
        load_canary(client, payload, database=settings.clickhouse_database)
        reconciliation = reconcile_canary(
            client,
            payload,
            database=settings.clickhouse_database,
        )
        report["reconciliation"] = reconciliation
        report["benchmarks"] = run_benchmarks(
            client,
            payload,
            database=settings.clickhouse_database,
            iterations=args.iterations,
            concurrency=args.concurrency,
            client_factory=lambda: create_clickhouse_client(settings),
        )
        fixture_content = json.dumps(
            {
                "schema_version": 1,
                "source_run_id": payload.source_run_id,
                "content_sha256": payload.content_sha256,
                "ohlcv": payload.ohlcv_rows,
            },
            sort_keys=True,
            separators=(",", ":"),
        ).encode()
        artifact_store = ObjectArtifactStore.from_settings(settings)
        artifact = artifact_store.put_bytes(
            ArtifactNamespace.DATASETS,
            f"canaries/{payload.source_run_id}/ohlcv.json",
            fixture_content,
            media_type="application/json",
            metadata={"source-run-id": payload.source_run_id},
        )
        artifact_store.get_bytes(artifact)
        manifest = ArtifactManifestRepository(timescale.engine).register(
            artifact_type="phase2_canary_dataset",
            storage_uri=artifact.storage_uri,
            sha256=artifact.sha256,
            size_bytes=artifact.size_bytes,
            media_type=artifact.media_type,
            object_version_id=artifact.version_id,
            metadata={"source_run_id": payload.source_run_id},
        )
        report["dataset_artifact"] = {
            "artifact_manifest_id": manifest["artifact_manifest_id"],
            "storage_uri": artifact.storage_uri,
            "sha256": artifact.sha256,
            "size_bytes": artifact.size_bytes,
            "version_id": artifact.version_id,
            "read_after_write_verified": True,
        }
        report["status"] = (
            "passed" if all(item["passed"] for item in reconciliation.values()) else "failed"
        )

    args.report.parent.mkdir(parents=True, exist_ok=True)
    args.report.write_text(json.dumps(report, indent=2) + "\n", encoding="utf-8")
    print(json.dumps(report, indent=2))
    if report["status"] == "failed":
        raise SystemExit(1)


if __name__ == "__main__":
    main()
