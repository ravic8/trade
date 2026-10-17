from __future__ import annotations

import hashlib
import importlib.util
import json
import subprocess
from pathlib import Path
from types import SimpleNamespace

import pytest

spec = importlib.util.spec_from_file_location(
    "restore_daily_backup", Path(__file__).resolve().parents[1] / "scripts/restore_daily_backup.py"
)
assert spec and spec.loader
restore = importlib.util.module_from_spec(spec)
spec.loader.exec_module(restore)


def backup(root: Path) -> Path:
    source = root / "backup"
    source.mkdir()
    payload = b"test postgres custom dump"
    (source / "postgres.dump").write_bytes(payload)
    (source / "SHA256SUMS").write_text(hashlib.sha256(payload).hexdigest() + "  postgres.dump\n")
    return source


def test_checksum_failure_does_not_touch_docker(tmp_path, monkeypatch):
    source = backup(tmp_path)
    (source / "postgres.dump").write_bytes(b"corrupt")
    monkeypatch.setattr(restore.subprocess, "run", lambda *a, **kw: pytest.fail("Docker called"))
    with pytest.raises(ValueError, match="checksum mismatch"):
        restore.restore_backup(source, tmp_path / "report.json", image="test-image")


@pytest.mark.parametrize("fail_restore", [False, True])
def test_restore_is_isolated_and_cleans_only_its_resources(tmp_path, monkeypatch, fail_restore):
    source = backup(tmp_path)
    calls = []

    def run(argv, **kwargs):
        calls.append(argv)
        assert kwargs["check"] is True
        assert kwargs["env"]["POSTGRES_PASSWORD"] not in " ".join(argv)
        if fail_restore and "pg_restore" in argv:
            raise subprocess.CalledProcessError(1, argv)
        output = ""
        if "SELECT count(*) FROM ohlcv_daily;" in argv:
            output = "123\n"
        if "SELECT version_num FROM alembic_version ORDER BY version_num;" in argv:
            output = "revision-a\n"
        return SimpleNamespace(stdout=output)

    monkeypatch.setattr(restore.subprocess, "run", run)
    report = tmp_path / "report.json"
    if fail_restore:
        with pytest.raises(subprocess.CalledProcessError):
            restore.restore_backup(source, report, image="test-image")
        assert not report.exists()
    else:
        proof = restore.restore_backup(source, report, image="test-image")
        assert json.loads(report.read_text()) == proof
        assert proof["daily_rows"] == 123
        assert proof["migration_revisions"] == ["revision-a"]
    create = next(call for call in calls if call[1] == "run")
    identity = create[create.index("--name") + 1]
    assert identity.startswith("trade-restore-")
    assert create[create.index("--network") + 1] == "none"
    assert create[create.index("--memory") + 1] == "1g"
    assert create[create.index("--cpus") + 1] == "1"
    assert "-p" not in create
    assert calls[-2] == ["docker", "rm", "-f", identity]
    assert calls[-1] == ["docker", "volume", "rm", identity + "-postgres"]
    assert "--exit-on-error" in next(call for call in calls if "pg_restore" in call)


def test_manifest_rejects_outside_backup_path(tmp_path):
    source = backup(tmp_path)
    (source / "SHA256SUMS").write_text("0" * 64 + "  ../postgres.dump\n")
    with pytest.raises(ValueError, match="Invalid backup"):
        restore.verify_backup(source)
