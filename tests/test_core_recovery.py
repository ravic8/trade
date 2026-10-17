"""Recovery integrity and refusal of unsafe or occupied targets."""

import importlib.util
import io
import json
import sys
import tarfile
from pathlib import Path

import pytest

SCRIPT = Path(__file__).resolve().parents[1] / "scripts/core_recovery.py"
spec = importlib.util.spec_from_file_location("core_recovery_tests", SCRIPT)
assert spec and spec.loader
recovery = importlib.util.module_from_spec(spec)
sys.path.insert(0, str(SCRIPT.parent))
try:
    spec.loader.exec_module(recovery)
finally:
    sys.path.pop(0)


def make_bundle(tmp_path):
    bundle = tmp_path / "bundle"
    bundle.mkdir()
    (bundle / "postgres.dump").write_bytes(b"unit-test dump bytes")
    files = {}
    for name in recovery.ROOTS:
        with tarfile.open(bundle / name, "w:gz") as archive:
            member = tarfile.TarInfo("nested/checkpoint.bin")
            content = b"\x00operational marker\xff"
            member.size = len(content)
            archive.addfile(member, io.BytesIO(content))
        files[name] = {
            "sha256": recovery.checksum(bundle / name),
            "entries": recovery.describe_archive(bundle / name),
        }
    files["postgres.dump"] = {"sha256": recovery.checksum(bundle / "postgres.dump")}
    manifest = {
        "format_version": 1,
        "files": files,
        "database": {
            "timescale_version": "2.27.1",
            "server_version": "160010",
            "row_counts": {},
            "revision": ["20260726_0012"],
        },
    }
    (bundle / "manifest.json").write_text(json.dumps(manifest))
    return bundle, manifest


def tool(tmp_path):
    return recovery.CoreRecovery(tmp_path, "owned", tmp_path / ".env", [tmp_path / "compose.yml"])


def test_bundle_validates_exact_payloads_and_binary_file_inventory(tmp_path):
    bundle, manifest = make_bundle(tmp_path)
    assert recovery.validate_bundle(bundle) == manifest
    assert manifest["files"]["data.tgz"]["entries"]["nested/checkpoint.bin"]["size"] == 20


def test_checksum_failure_never_inspects_or_mutates_restore_target(tmp_path, monkeypatch):
    bundle, _ = make_bundle(tmp_path)
    (bundle / "postgres.dump").write_bytes(b"corrupt")
    instance = tool(tmp_path)
    monkeypatch.setattr(
        instance, "inventory", lambda: pytest.fail("Touched target before validation")
    )
    with pytest.raises(RuntimeError, match="checksum"):
        instance.restore(bundle, quiesced=True)


@pytest.mark.parametrize("kind", ["traversal", "absolute", "symlink", "hardlink", "duplicate"])
def test_archive_rejects_unsafe_members_before_extraction(tmp_path, kind):
    archive_path = tmp_path / "unsafe.tgz"
    with tarfile.open(archive_path, "w:gz") as archive:
        name = {"traversal": "../escape", "absolute": "/escape"}.get(kind, "entry")
        member = tarfile.TarInfo(name)
        if kind in {"symlink", "hardlink"}:
            member.type = tarfile.SYMTYPE if kind == "symlink" else tarfile.LNKTYPE
            member.linkname = "outside"
        archive.addfile(member)
        if kind == "duplicate":
            archive.addfile(member)
    with pytest.raises(RuntimeError):
        recovery.describe_archive(archive_path)


@pytest.mark.parametrize("occupied", ["database", "objects", "files", "version"])
def test_restore_refuses_occupied_or_incompatible_target_without_writes(
    tmp_path, monkeypatch, occupied
):
    bundle, _ = make_bundle(tmp_path)
    instance = tool(tmp_path)
    monkeypatch.setattr(instance, "inventory", lambda: {})
    state = {
        "row_counts": {"public.existing": 0} if occupied == "database" else {},
        "user_objects": 1 if occupied == "objects" else 0,
        "server_version": "160010",
        "timescale_version": None,
        "available_timescale_version": "wrong-version" if occupied == "version" else "2.27.1",
    }
    monkeypatch.setattr(instance, "runtime", lambda: {"state": state, "connection": {}})
    monkeypatch.setattr(
        instance,
        "python",
        lambda *args: json.dumps(
            {"data.tgz": {"existing": {}} if occupied == "files" else {}, "artifacts.tgz": {}}
        ),
    )
    monkeypatch.setattr(
        instance, "client", lambda *args, **kwargs: pytest.fail("Attempted restore write")
    )
    with pytest.raises(RuntimeError):
        instance.restore(bundle, quiesced=True)


def test_quiescence_required_before_accessing_services(tmp_path, monkeypatch):
    instance = tool(tmp_path)
    monkeypatch.setattr(instance, "inventory", lambda: pytest.fail("Service accessed"))
    with pytest.raises(RuntimeError, match="writers"):
        instance.backup(tmp_path / "backup", quiesced=False)
    with pytest.raises(RuntimeError, match="writers"):
        instance.restore(tmp_path / "backup", quiesced=False)


def test_existing_backup_destination_is_preserved(tmp_path, monkeypatch):
    instance = tool(tmp_path)
    destination = tmp_path / "keep"
    destination.mkdir()
    (destination / "sentinel").write_text("keep")
    monkeypatch.setattr(instance, "inventory", lambda: pytest.fail("Service accessed"))
    with pytest.raises(RuntimeError, match="exists"):
        instance.backup(destination, quiesced=True)
    assert (destination / "sentinel").read_text() == "keep"


def test_symlinked_payload_and_incomplete_backup_are_rejected(tmp_path):
    bundle, _ = make_bundle(tmp_path)
    payload = bundle / "postgres.dump"
    payload.rename(tmp_path / "payload")
    payload.symlink_to(tmp_path / "payload")
    with pytest.raises(RuntimeError, match="payload"):
        recovery.validate_bundle(bundle)
    incomplete = tmp_path / ".incomplete-test"
    bundle.rename(incomplete)
    with pytest.raises(RuntimeError, match="incomplete"):
        recovery.validate_bundle(incomplete)


def test_backup_cannot_include_itself_inside_live_storage(tmp_path, monkeypatch):
    instance = tool(tmp_path)
    source = tmp_path / "data"
    source.mkdir()
    monkeypatch.setattr(
        instance, "inventory", lambda: {"mounts": {"api": [{"Source": str(source)}]}}
    )
    monkeypatch.setattr(
        instance, "runtime", lambda: pytest.fail("Backup attempted database access")
    )
    with pytest.raises(RuntimeError, match="outside live storage"):
        instance.backup(source / "nested-backup", quiesced=True)
    assert not list(source.iterdir())


def test_optional_external_capabilities_cannot_be_misreported_as_core_restore(
    tmp_path, monkeypatch
):
    bundle, _ = make_bundle(tmp_path)
    instance = tool(tmp_path)
    monkeypatch.setattr(instance, "inventory", lambda: {})
    monkeypatch.setattr(instance, "runtime", lambda: {"optional_enabled": ["object_store_enabled"]})
    monkeypatch.setattr(instance, "client", lambda *args, **kwargs: pytest.fail("Attempted write"))
    with pytest.raises(RuntimeError, match="optional capabilities"):
        instance.restore(bundle, quiesced=True)


def test_restore_refuses_read_only_mount_before_database_access(tmp_path, monkeypatch):
    bundle, _ = make_bundle(tmp_path)
    instance = tool(tmp_path)
    monkeypatch.setattr(
        instance, "inventory", lambda: {"api_access": {"roots": {"/app/data": {"writable": False}}}}
    )
    monkeypatch.setattr(
        instance, "runtime", lambda: pytest.fail("Database accessed before access check")
    )
    with pytest.raises(RuntimeError, match="writable"):
        instance.restore(bundle, quiesced=True)


def test_connection_env_injection_is_rejected_without_echoing_password(tmp_path, monkeypatch):
    instance = tool(tmp_path)
    monkeypatch.setattr(
        instance, "api_id", lambda: pytest.fail("Created helper for invalid connection")
    )
    with pytest.raises(RuntimeError) as error:
        instance.client("pg_dump", [], {"PGPASSWORD": "credential-marker\nPGHOST=wrong"})
    assert "PGPASSWORD" in str(error.value)
    assert "credential-marker" not in str(error.value)


def test_volume_identity_ignores_label_representation_but_detects_changed_storage():
    script = SCRIPT.with_name("verify_core_recovery.py")
    spec = importlib.util.spec_from_file_location("core_drill_tests", script)
    assert spec and spec.loader
    drill = importlib.util.module_from_spec(spec)
    sys.path.insert(0, str(SCRIPT.parent))
    try:
        spec.loader.exec_module(drill)
    finally:
        sys.path.pop(0)
    before = {
        "api": [
            {
                "Type": "volume",
                "Name": "retained",
                "Source": "/owned/data",
                "Destination": "/app/data",
                "RW": True,
                "Mode": "",
            }
        ]
    }
    after = json.loads(json.dumps(before))
    after["api"][0]["Mode"] = "z"
    assert drill.storage_identity(before) == drill.storage_identity(after)
    after["api"][0]["Source"] = "/different/data"
    assert drill.storage_identity(before) != drill.storage_identity(after)
