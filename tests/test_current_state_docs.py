from __future__ import annotations

import subprocess
import sys
from pathlib import Path


def test_current_state_documentation_matches_repository() -> None:
    root = Path(__file__).resolve().parents[1]
    result = subprocess.run(
        [sys.executable, "scripts/check_current_state_docs.py"],
        cwd=root,
        check=False,
        capture_output=True,
        text=True,
    )
    assert result.returncode == 0, result.stdout + result.stderr
