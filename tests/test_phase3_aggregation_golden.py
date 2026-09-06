from __future__ import annotations

import hashlib
import json
from pathlib import Path

from typer.testing import CliRunner

from trade_research import cli
from trade_research.market_data.aggregation_golden import (
    evaluate_aggregation_candidate,
    evaluate_python_aggregation_golden,
    load_aggregation_golden,
    load_candidate_output,
)

GOLDEN_PATH = Path("evaluations/market_data/nse_intraday_aggregation_v1.json")
GOLDEN_FILE_SHA256 = "4b9a95b34a849a6fd961d4e1fcd3ce316ea7dd5b5d42790769d26a012acf9689"


def test_locked_aggregation_golden_fixture_passes_python_reference() -> None:
    assert hashlib.sha256(GOLDEN_PATH.read_bytes()).hexdigest() == GOLDEN_FILE_SHA256
    dataset = load_aggregation_golden(GOLDEN_PATH)

    report = evaluate_python_aggregation_golden(dataset)

    assert report.passed is True
    assert report.cases_total == report.cases_passed == 8
    assert report.mismatches == ()


def test_golden_locks_incomplete_opening_and_complete_final_hour_bucket() -> None:
    dataset = load_aggregation_golden(GOLDEN_PATH)

    opening_complete = dataset["expected"]["1h:complete_only"]
    all_hour_buckets = dataset["expected"]["1h:include_incomplete"]

    assert len(opening_complete) == 1
    assert opening_complete[0][0] == "2026-09-04T09:45:00+00:00"
    assert opening_complete[0][7:9] == [15, True]
    assert all_hour_buckets[0][0] == "2026-09-04T03:45:00+00:00"
    assert all_hour_buckets[0][6:9] == [14, 60, False]


def test_external_candidate_must_match_every_locked_row(tmp_path: Path) -> None:
    dataset = load_aggregation_golden(GOLDEN_PATH)
    candidate = json.loads(json.dumps(dataset["expected"]))
    candidate["5m:complete_only"][0][5] += 1
    candidate_path = tmp_path / "candidate.json"
    candidate_path.write_text(
        json.dumps(
            {
                "schema_version": dataset["schema_version"],
                "output": candidate,
            }
        ),
        encoding="utf-8",
    )

    report = evaluate_aggregation_candidate(
        dataset, load_candidate_output(candidate_path)
    )

    assert report.passed is False
    assert report.cases_passed == 7
    assert report.mismatches == (
        "5m:complete_only: output does not match the locked fixture",
    )


def test_golden_cli_is_a_read_only_pass_gate() -> None:
    result = CliRunner().invoke(
        cli.app,
        ["verify-market-data-aggregation-golden", "--dataset-path", str(GOLDEN_PATH)],
    )

    assert result.exit_code == 0
    assert "Aggregation golden: PASS" in result.stdout
    assert "Cases: 8/8" in result.stdout
