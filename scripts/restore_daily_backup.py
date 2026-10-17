"""Restore a verified PostgreSQL backup into disposable, isolated infrastructure."""

from __future__ import annotations

import argparse
import hashlib
import json
import os
import subprocess
import time
from pathlib import Path
from secrets import token_hex
from uuid import uuid4


def file_sha256(path: Path) -> str:
    hasher = hashlib.sha256()
    with path.open("rb") as stream:
        for block in iter(lambda: stream.read(1024 * 1024), b""):
            hasher.update(block)
    return hasher.hexdigest()


def verify_backup(root: Path) -> Path:
    root = root.resolve()
    seen = set()
    for line in (root / "SHA256SUMS").read_text().splitlines():
        digest, name = line.split(maxsplit=1)
        name = name.lstrip("*")
        path = (root / name).resolve()
        if path.parent != root or name in seen or len(digest) != 64:
            raise ValueError("Invalid backup checksum manifest.")
        seen.add(name)
        if file_sha256(path) != digest:
            raise ValueError(f"Backup checksum mismatch: {name}")
    if "postgres.dump" not in seen:
        raise ValueError("A checksummed postgres.dump is required.")
    return root / "postgres.dump"


def restore_backup(root: Path, report: Path, *, image: str) -> dict:
    dump = verify_backup(root)
    if report.exists():
        raise FileExistsError(report)
    identity = "trade-restore-" + uuid4().hex
    volume = identity + "-postgres"
    environment = {**os.environ, "POSTGRES_PASSWORD": token_hex(24)}
    created_volume = False
    created_container = False

    def docker(*args: str, timeout: int = 120) -> str:
        result = subprocess.run(
            ["docker", *args],
            check=True,
            capture_output=True,
            text=True,
            env=environment,
            timeout=timeout,
        )
        return result.stdout

    try:
        docker("volume", "create", "--label", "trade.restore=" + identity, volume)
        created_volume = True
        docker(
            "run",
            "-d",
            "--name",
            identity,
            "--label",
            "trade.restore=" + identity,
            "--network",
            "none",
            "--memory",
            "1g",
            "--cpus",
            "1",
            "--shm-size",
            "256m",
            "--mount",
            f"type=volume,source={volume},target=/var/lib/postgresql/data",
            "-e",
            "POSTGRES_USER=trade",
            "-e",
            "POSTGRES_DB=trade_restore",
            "-e",
            "POSTGRES_PASSWORD",
            image,
        )
        created_container = True
        for _attempt in range(60):
            try:
                docker(
                    "exec",
                    identity,
                    "pg_isready",
                    "-h",
                    "127.0.0.1",
                    "-U",
                    "trade",
                    "-d",
                    "trade_restore",
                )
                break
            except subprocess.CalledProcessError:
                time.sleep(2)
        else:
            raise RuntimeError("Isolated restore database did not become ready.")
        docker("cp", str(dump), identity + ":/tmp/postgres.dump", timeout=600)
        docker(
            "exec",
            identity,
            "psql",
            "-U",
            "trade",
            "-d",
            "trade_restore",
            "-v",
            "ON_ERROR_STOP=1",
            "-c",
            "CREATE EXTENSION IF NOT EXISTS timescaledb; SELECT timescaledb_pre_restore();",
        )
        docker(
            "exec",
            identity,
            "pg_restore",
            "-U",
            "trade",
            "-d",
            "trade_restore",
            "--exit-on-error",
            "--no-owner",
            "--no-privileges",
            "/tmp/postgres.dump",
            timeout=3600,
        )
        docker(
            "exec",
            identity,
            "psql",
            "-U",
            "trade",
            "-d",
            "trade_restore",
            "-v",
            "ON_ERROR_STOP=1",
            "-c",
            "SELECT timescaledb_post_restore();",
        )
        rows = docker(
            "exec",
            identity,
            "psql",
            "-U",
            "trade",
            "-d",
            "trade_restore",
            "-At",
            "-v",
            "ON_ERROR_STOP=1",
            "-c",
            "SELECT count(*) FROM ohlcv_daily;",
        ).strip()
        revisions = docker(
            "exec",
            identity,
            "psql",
            "-U",
            "trade",
            "-d",
            "trade_restore",
            "-At",
            "-c",
            "SELECT version_num FROM alembic_version ORDER BY version_num;",
        ).splitlines()
        proof = {
            "status": "restored",
            "source_dump_sha256": file_sha256(dump),
            "daily_rows": int(rows),
            "migration_revisions": revisions,
            "image": image,
            "network": "none",
            "ports_published": False,
            "memory_limit": "1g",
            "cpu_limit": 1,
            "scope": "PostgreSQL only; MinIO/KES and ClickHouse restore remain separate checks",
        }
        report.parent.mkdir(parents=True, exist_ok=True)
        with report.open("x") as stream:
            json.dump(proof, stream, indent=2)
            stream.write("\n")
        return proof
    finally:
        try:
            if created_container:
                docker("rm", "-f", identity)
        finally:
            if created_volume:
                docker("volume", "rm", volume)


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("backup", type=Path)
    parser.add_argument("--report", type=Path, required=True)
    parser.add_argument("--image", default="timescale/timescaledb:2.23.1-pg16")
    args = parser.parse_args()
    restore_backup(args.backup, args.report, image=args.image)
    print("Isolated PostgreSQL restore verified; report saved.")


if __name__ == "__main__":
    main()
