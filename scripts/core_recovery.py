#!/usr/bin/env python3
"""Inventory, back up, and restore a quiesced core Compose installation."""

from __future__ import annotations

import argparse
import hashlib
import json
import subprocess
import tarfile
import tempfile
from datetime import UTC, datetime
from pathlib import Path, PurePosixPath
from uuid import uuid4

try:
    from .verify_fresh_setup import docker_environment
except ImportError:
    from verify_fresh_setup import docker_environment

CLIENT_IMAGE = "timescale/timescaledb:latest-pg16"
FILES = ("postgres.dump", "data.tgz", "artifacts.tgz")
ROOTS = {"data.tgz": "/app/data", "artifacts.tgz": "/app/artifacts"}

RUNTIME_INFO = """
import json
from sqlalchemy import create_engine, text
from sqlalchemy.engine import make_url
from trade_data.config import get_settings
s = get_settings()
u = make_url(s.database_url)
if u.get_backend_name() != "postgresql":
    raise RuntimeError("Core recovery requires PostgreSQL")
e = create_engine(s.database_url, hide_parameters=True, connect_args={"connect_timeout": 10})
with e.connect() as c, c.begin():
    c.execute(text("SET TRANSACTION READ ONLY"))
    pairs = c.execute(text("SELECT schemaname, tablename FROM pg_tables ORDER BY 1, 2")).all()
    pairs = [(schema, table) for schema, table in pairs if
             not schema.startswith(("pg_", "_timescaledb")) and schema != "information_schema"]
    quote = e.dialect.identifier_preparer.quote
    counts = {schema + "." + table: c.execute(text("SELECT count(*) FROM " +
              quote(schema) + "." + quote(table))).scalar_one() for schema, table in pairs}
    revision = (c.execute(text("SELECT version_num FROM alembic_version ORDER BY 1"))
                .scalars().all() if "public.alembic_version" in counts else [])
    scope = ("n.nspname <> 'information_schema' AND left(n.nspname, 3) <> 'pg_' "
             "AND strpos(n.nspname, '_timescaledb') <> 1")
    extension_objects = ("WITH RECURSIVE ext_objects AS ("
        "SELECT classid, objid FROM pg_depend WHERE deptype='e' UNION "
        "SELECT d.classid, d.objid FROM pg_depend d JOIN ext_objects x "
        "ON d.refclassid=x.classid AND d.refobjid=x.objid WHERE d.deptype IN ('a','i')) ")
    objects = 0
    for catalog in ("pg_class", "pg_proc", "pg_type", "pg_collation"):
        namespace = {"pg_class": "relnamespace", "pg_proc": "pronamespace",
                     "pg_type": "typnamespace", "pg_collation": "collnamespace"}[catalog]
        query = (extension_objects + "SELECT count(*) FROM " + catalog +
                 " o JOIN pg_namespace n ON n.oid=o." + namespace + " WHERE " + scope +
                 " AND NOT EXISTS (SELECT 1 FROM ext_objects x WHERE "
                 "x.classid='" + catalog + "'::regclass AND x.objid=o.oid)")
        objects += c.execute(text(query)).scalar_one()
    schemas = c.execute(text(extension_objects + "SELECT n.nspname FROM pg_namespace n WHERE " +
        scope + " AND n.nspname <> 'public' AND NOT EXISTS (SELECT 1 FROM ext_objects x WHERE "
        "x.classid='pg_namespace'::regclass AND x.objid=n.oid) ORDER BY 1")).scalars().all()
    state = {"user_objects": objects, "extra_schemas": schemas,
             "server_version": c.execute(text("SHOW server_version_num")).scalar_one(),
             "timescale_version": c.execute(text("SELECT extversion FROM pg_extension "
                 "WHERE extname='timescaledb'")).scalar(),
             "available_timescale_version": c.execute(text("SELECT default_version "
                 "FROM pg_available_extensions WHERE name='timescaledb'")).scalar(),
             "revision": revision, "row_counts": counts}
connection = {"PGHOST": u.host, "PGPORT": str(u.port or 5432), "PGDATABASE": u.database,
              "PGUSER": u.username or "", "PGPASSWORD": u.password or "", "PGCONNECT_TIMEOUT": "10"}
options = {"sslmode": "PGSSLMODE", "sslrootcert": "PGSSLROOTCERT", "sslcert": "PGSSLCERT",
           "sslkey": "PGSSLKEY", "channel_binding": "PGCHANNELBINDING", "options": "PGOPTIONS",
           "gssencmode": "PGGSSENCMODE", "target_session_attrs": "PGTARGETSESSIONATTRS",
           "application_name": "PGAPPNAME", "connect_timeout": "PGCONNECT_TIMEOUT"}
for key, value in u.query.items():
    if key not in options or not isinstance(value, str):
        raise RuntimeError("Unsupported PostgreSQL connection option for recovery")
    connection[options[key]] = value
optional = [name for name in ("filing_enabled", "filing_index_enabled", "chat_enabled",
            "clickhouse_enabled", "object_store_enabled", "bigquery_enabled") if getattr(s, name)]
print(json.dumps({"state": state, "connection": connection, "optional_enabled": optional}))
e.dispose()
"""

ARCHIVE = """
import pathlib, sys, tarfile
root = pathlib.Path(sys.argv[1])
with tarfile.open(fileobj=sys.stdout.buffer, mode="w|gz") as archive:
    for path in sorted(root.rglob("*")):
        if path.is_symlink() or not (path.is_file() or path.is_dir()):
            raise RuntimeError("Recovery archives support only regular files/directories")
        archive.add(path, arcname=str(path.relative_to(root)), recursive=False)
"""
EXTRACT = """
import pathlib, shutil, sys, tarfile
root = pathlib.Path(sys.argv[1]).resolve()
if any(root.iterdir()):
    raise RuntimeError("Restore filesystem target is not empty")
with tarfile.open(fileobj=sys.stdin.buffer, mode="r|gz") as archive:
    for member in archive:
        name = pathlib.PurePosixPath(member.name)
        if (name.is_absolute() or ".." in name.parts or not name.parts
                or not (member.isfile() or member.isdir())):
            raise RuntimeError("Unsafe archive member")
        path = root.joinpath(*name.parts)
        if not path.resolve().is_relative_to(root):
            raise RuntimeError("Archive path escapes target")
        if member.isdir():
            path.mkdir(parents=True, exist_ok=True, mode=0o700)
        else:
            path.parent.mkdir(parents=True, exist_ok=True, mode=0o700)
            with path.open("xb") as target, archive.extractfile(member) as source:
                shutil.copyfileobj(source, target)
            path.chmod(0o600)
"""
FILESYSTEM = """
import hashlib, json, pathlib
result = {}
for name in ("data.tgz", "artifacts.tgz"):
    root = pathlib.Path("/app/data" if name == "data.tgz" else "/app/artifacts")
    entries = {}
    for path in sorted(root.rglob("*")):
        if path.is_symlink() or not (path.is_file() or path.is_dir()):
            raise RuntimeError("Unsupported recovery filesystem entry")
        key = str(path.relative_to(root))
        if path.is_dir():
            entries[key] = {"type": "directory"}
        else:
            digest = hashlib.sha256()
            with path.open("rb") as stream:
                for block in iter(lambda: stream.read(1024*1024), b""):
                    digest.update(block)
            entries[key] = {"type": "file", "size": path.stat().st_size,
                            "sha256": digest.hexdigest()}
    result[name] = entries
print(json.dumps(result))
"""


def checksum(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as stream:
        for block in iter(lambda: stream.read(1024 * 1024), b""):
            digest.update(block)
    return digest.hexdigest()


def describe_archive(path: Path) -> dict:
    entries = {}
    with tarfile.open(path, "r:gz") as archive:
        for member in archive:
            name = PurePosixPath(member.name)
            if (
                name.is_absolute()
                or ".." in name.parts
                or not name.parts
                or "\\" in member.name
                or not (member.isfile() or member.isdir())
            ):
                raise RuntimeError("Unsafe recovery archive member")
            key = str(name)
            if key in entries:
                raise RuntimeError("Duplicate recovery archive member")
            if member.isdir():
                entries[key] = {"type": "directory"}
            else:
                digest = hashlib.sha256()
                with archive.extractfile(member) as stream:
                    for block in iter(lambda: stream.read(1024 * 1024), b""):
                        digest.update(block)
                entries[key] = {"type": "file", "size": member.size, "sha256": digest.hexdigest()}
    return entries


def validate_bundle(bundle: Path) -> dict:
    if bundle.is_symlink() or bundle.name.startswith(".incomplete-"):
        raise RuntimeError("Refusing incomplete or symlinked recovery bundle")
    manifest_path = bundle / "manifest.json"
    if manifest_path.is_symlink():
        raise RuntimeError("Recovery manifest must be a regular file")
    manifest = json.loads(manifest_path.read_text())
    if manifest.get("format_version") != 1 or set(manifest.get("files", {})) != set(FILES):
        raise RuntimeError("Unsupported or incomplete recovery manifest")
    for name in FILES:
        path = bundle / name
        if path.is_symlink() or not path.is_file() or path.stat().st_size == 0:
            raise RuntimeError("Missing or invalid recovery payload")
        if checksum(path) != manifest["files"][name]["sha256"]:
            raise RuntimeError("Recovery payload checksum mismatch")
    for name in ROOTS:
        if describe_archive(bundle / name) != manifest["files"][name]["entries"]:
            raise RuntimeError("Recovery archive inventory mismatch")
    if not manifest.get("database", {}).get("timescale_version"):
        raise RuntimeError("Recovery bundle must record the TimescaleDB version")
    return manifest


class CoreRecovery:
    def __init__(
        self,
        root: Path,
        project: str,
        env_file: Path,
        compose_files: list[Path],
        client_image: str = CLIENT_IMAGE,
    ):
        self.root, self.project = root.resolve(), project
        self.environment = docker_environment()
        self.base = ["docker", "compose", "--env-file", str(env_file.resolve()), "-p", project]
        for path in compose_files:
            self.base.extend(["-f", str(path.resolve())])
        self.client_image = client_image

    def run(self, args: list[str], *, stdout=None, stdin=None) -> bytes:
        result = subprocess.run(
            args,
            cwd=self.root,
            env=self.environment,
            stdin=stdin,
            stdout=stdout if stdout is not None else subprocess.PIPE,
            stderr=subprocess.PIPE,
            timeout=900,
        )
        if result.returncode:
            operation = "command"
            if "--entrypoint" in args:
                candidate = args[args.index("--entrypoint") + 1]
                if candidate in {"pg_dump", "pg_restore", "psql"}:
                    operation = candidate
            raise RuntimeError(
                f"Recovery {operation} failed (exit {result.returncode}); "
                "inspect service health and permissions"
            )
        return result.stdout or b""

    def compose(self, args: list[str], **kwargs) -> bytes:
        return self.run(self.base + args, **kwargs)

    def api_id(self) -> str:
        value = self.compose(["ps", "--status", "running", "-q", "api"]).decode().strip()
        if not value or "\n" in value:
            raise RuntimeError("Recovery requires exactly one running API container")
        return value

    def python(self, code: str, *args: str, **kwargs) -> bytes:
        return self.compose(["exec", "-T", "api", "python", "-c", code, *args], **kwargs)

    def runtime(self) -> dict:
        return json.loads(self.python(RUNTIME_INFO))

    def inventory(self) -> dict:
        rows = self.compose(["ps", "--status", "running", "--format", "json"]).decode()
        services = {json.loads(line)["Service"] for line in rows.splitlines() if line}
        if not services <= {"api", "postgres", "redis"}:
            raise RuntimeError(
                "Core recovery excludes optional services; use their recovery procedure"
            )
        mounts = {}
        for service in sorted(services):
            identifier = self.compose(["ps", "--status", "running", "-q", service]).decode().strip()
            mounts[service] = sorted(
                json.loads(
                    self.run(["docker", "inspect", "--format", "{{json .Mounts}}", identifier])
                ),
                key=lambda mount: mount["Destination"],
            )
        api_targets = {mount["Destination"] for mount in mounts.get("api", [])}
        if not set(ROOTS.values()) <= api_targets:
            raise RuntimeError("Core recovery requires mounted /app/data and /app/artifacts")
        access = json.loads(
            self.python("""
import json, os
from pathlib import Path
roots = {}
for name in ("/app/data", "/app/artifacts"):
    path = Path(name)
    stat = path.stat()
    roots[name] = {"uid": stat.st_uid, "gid": stat.st_gid, "mode": oct(stat.st_mode & 0o777),
                   "is_directory": path.is_dir(), "writable": os.access(path, os.W_OK)}
print(json.dumps({"uid": os.getuid(), "gid": os.getgid(), "roots": roots}))
""")
        )
        if not all(item["is_directory"] for item in access["roots"].values()):
            raise RuntimeError("Core recovery requires data/artifact directory mounts")
        return {
            "project": self.project,
            "mounts": mounts,
            "api_access": access,
            "redis_policy": "ephemeral queue/rate-limit state; drain jobs before maintenance",
            "external_infrastructure": "postgres" not in services,
        }

    def client(self, command: str, args: list[str], connection: dict, **kwargs) -> bytes:
        for key, value in connection.items():
            if not isinstance(value, str) or any(character in value for character in "\n\r\0"):
                raise RuntimeError(f"Unsupported recovery connection setting: {key}")
        identifier = self.api_id()
        if not self.client_image.startswith("sha256:"):
            try:
                image = self.run(
                    ["docker", "image", "inspect", "--format", "{{.Id}}", self.client_image]
                )
            except RuntimeError:
                self.run(["docker", "pull", self.client_image])
                image = self.run(
                    ["docker", "image", "inspect", "--format", "{{.Id}}", self.client_image]
                )
            self.client_image = image.decode().strip()
        name = "trade-recovery-client-" + uuid4().hex[:16]
        with tempfile.TemporaryDirectory(prefix="trade-pgclient-") as temporary:
            env_file = Path(temporary) / "connection.env"
            env_file.touch(mode=0o600)
            env_file.write_text("".join(f"{key}={value}\n" for key, value in connection.items()))
            try:
                return self.run(
                    [
                        "docker",
                        "run",
                        "--rm",
                        "-i",
                        "--name",
                        name,
                        "--network",
                        f"container:{identifier}",
                        "--volumes-from",
                        f"{identifier}:ro",
                        "--env-file",
                        str(env_file),
                        "--entrypoint",
                        command,
                        self.client_image,
                        *args,
                    ],
                    **kwargs,
                )
            finally:
                subprocess.run(
                    ["docker", "rm", "-f", name],
                    env=self.environment,
                    stdout=subprocess.DEVNULL,
                    stderr=subprocess.DEVNULL,
                    timeout=30,
                )

    def backup(self, destination: Path, *, quiesced: bool) -> dict:
        if not quiesced:
            raise RuntimeError("Suspend all writers and drain queues, then pass --quiesced")
        destination = destination.resolve()
        if destination.exists():
            raise RuntimeError("Recovery destination already exists")
        ownership = self.inventory()
        for service in ("api", "postgres"):
            for mount in ownership["mounts"].get(service, []):
                if destination.is_relative_to(Path(mount["Source"])):
                    raise RuntimeError("Backup destination must be outside live storage mounts")
        runtime = self.runtime()
        if runtime.get("optional_enabled"):
            raise RuntimeError(
                "Enabled optional capabilities require their separate recovery procedure"
            )
        if not runtime["state"]["timescale_version"] or not runtime["state"]["revision"]:
            raise RuntimeError("Core backup requires a migrated TimescaleDB database")
        if int(runtime["state"]["server_version"]) // 10000 != 16:
            raise RuntimeError("Core recovery currently supports PostgreSQL 16")
        destination.parent.mkdir(parents=True, exist_ok=True)
        staging = destination.parent / (".incomplete-" + uuid4().hex)
        staging.mkdir(mode=0o700)
        image = (
            self.run(["docker", "inspect", "--format", "{{.Image}}", self.api_id()])
            .decode()
            .strip()
        )
        manifest = {
            "format_version": 1,
            "created_at": datetime.now(UTC).isoformat(),
            "database": runtime["state"],
            "api_image_id": image,
            "ownership": ownership,
            "files": {},
        }
        for name in FILES:
            path = staging / name
            path.touch(mode=0o600)
            with path.open("wb") as stream:
                if name == "postgres.dump":
                    self.client(
                        "pg_dump",
                        ["--format=custom", "--no-owner", "--no-acl"],
                        runtime["connection"],
                        stdout=stream,
                    )
                else:
                    self.python(ARCHIVE, ROOTS[name], stdout=stream)
            manifest["files"][name] = {"sha256": checksum(path), "size": path.stat().st_size}
            if name in ROOTS:
                manifest["files"][name]["entries"] = describe_archive(path)
        manifest["postgres_client_image_id"] = self.client_image
        if self.runtime()["state"] != runtime["state"]:
            raise RuntimeError("Database changed during backup; incomplete bundle retained")
        path = staging / "manifest.json"
        path.touch(mode=0o600)
        path.write_text(json.dumps(manifest, indent=2) + "\n")
        # Validate before publication. Incomplete directories are intentionally
        # rejected by the public validator, so validate through a temporary name.
        complete = staging.with_name(".validated-" + uuid4().hex)
        staging.rename(complete)
        try:
            validate_bundle(complete)
            complete.rename(destination)
        except BaseException:
            complete.rename(staging)
            raise
        return manifest

    def restore(self, bundle: Path, *, quiesced: bool) -> dict:
        if not quiesced:
            raise RuntimeError("Suspend all writers and drain queues, then pass --quiesced")
        manifest = validate_bundle(bundle)
        ownership = self.inventory()
        roots = ownership.get("api_access", {}).get("roots", {})
        if not all(item["writable"] for item in roots.values()):
            raise RuntimeError("Restore requires writable data/artifact mounts for the API user")
        target = self.runtime()
        if target.get("optional_enabled"):
            raise RuntimeError("Restore target must have optional capabilities disabled")
        source = manifest["database"]
        if (
            target["state"]["row_counts"]
            or target["state"].get("user_objects")
            or target["state"].get("extra_schemas")
        ):
            raise RuntimeError("Refusing restore into a nonempty database")
        filesystem = json.loads(self.python(FILESYSTEM))
        if any(filesystem.values()):
            raise RuntimeError("Refusing restore into nonempty data/artifact mounts")
        if (
            int(target["state"]["server_version"]) // 10000
            != int(source["server_version"]) // 10000
        ):
            raise RuntimeError("Restore requires the same PostgreSQL major version")
        version = (
            target["state"]["timescale_version"] or target["state"]["available_timescale_version"]
        )
        if version != source["timescale_version"]:
            raise RuntimeError("Restore requires the backed-up TimescaleDB version")
        self.client(
            "psql",
            [
                "-v",
                "ON_ERROR_STOP=1",
                "-c",
                "CREATE EXTENSION IF NOT EXISTS timescaledb; SELECT timescaledb_pre_restore();",
            ],
            target["connection"],
        )
        try:
            with (bundle / "postgres.dump").open("rb") as stream:
                self.client(
                    "pg_restore",
                    [
                        "--exit-on-error",
                        "--no-owner",
                        "--no-acl",
                        "--dbname",
                        target["connection"]["PGDATABASE"],
                    ],
                    target["connection"],
                    stdin=stream,
                )
        finally:
            self.client(
                "psql",
                ["-v", "ON_ERROR_STOP=1", "-c", "SELECT timescaledb_post_restore();"],
                target["connection"],
            )
        for name, root in ROOTS.items():
            with (bundle / name).open("rb") as stream:
                self.python(EXTRACT, root, stdin=stream)
        restored = self.runtime()["state"]
        for key in ("revision", "row_counts", "timescale_version", "user_objects", "extra_schemas"):
            if restored[key] != source[key]:
                raise RuntimeError("Restored schema/data counts do not match backup")
        filesystem = json.loads(self.python(FILESYSTEM))
        if any(filesystem[name] != manifest["files"][name]["entries"] for name in ROOTS):
            raise RuntimeError("Restored file bytes/paths do not match backup")
        return {"status": "passed", "database": restored, "files_verified": True}


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--client-image", default=CLIENT_IMAGE)
    parser.add_argument("--project", required=True)
    parser.add_argument("--env-file", type=Path, required=True)
    parser.add_argument("--compose-file", type=Path, action="append", required=True)
    subparsers = parser.add_subparsers(dest="command", required=True)
    subparsers.add_parser("inventory")
    backup = subparsers.add_parser("backup")
    backup.add_argument("destination", type=Path)
    backup.add_argument("--quiesced", action="store_true")
    restore = subparsers.add_parser("restore")
    restore.add_argument("bundle", type=Path)
    restore.add_argument("--quiesced", action="store_true")
    args = parser.parse_args()
    recovery = CoreRecovery(
        Path.cwd(), args.project, args.env_file, args.compose_file, client_image=args.client_image
    )
    try:
        if args.command == "inventory":
            result = recovery.inventory()
        elif args.command == "backup":
            result = recovery.backup(args.destination, quiesced=args.quiesced)
        else:
            result = recovery.restore(args.bundle, quiesced=args.quiesced)
        print(json.dumps(result, indent=2))
    except Exception as error:
        print(f"Core recovery failed: {error}")
        raise SystemExit(1) from None


if __name__ == "__main__":
    main()
