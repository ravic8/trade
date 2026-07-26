from __future__ import annotations

import json
from pathlib import Path

import pytest

from trade_research.config import Settings
from trade_research.filings.evaluation import (
    evaluate_golden_dataset,
    load_golden_dataset,
)
from trade_research.filings.registry import import_manifest
from trade_research.filings.runtime import FilingRuntime

INFY_MANIFEST = Path("data/filings/nse/INFY/manifest.json")
INFY_GOLDEN = Path("evaluations/filings/infy_m1_golden.json")


def test_infy_golden_dataset_contract_is_locked() -> None:
    dataset = load_golden_dataset(INFY_GOLDEN, verify_manifest=False)

    assert dataset.dataset_id == "infy-m1-13q-core-v1"
    assert len(dataset.cases) == 13
    assert sum(len(case.expected_facts) for case in dataset.cases) == 52
    assert dataset.review_status.endswith("pending_dual_analyst_signoff")


@pytest.mark.skipif(
    not INFY_MANIFEST.is_file(),
    reason="local licensed/downloaded INFY filing pack is not present",
)
def test_infy_13_quarter_golden_dataset_passes_end_to_end(tmp_path: Path) -> None:
    settings = Settings(
        app_env="test",
        data_dir=Path("data"),
        database_url=f"sqlite:///{tmp_path / 'golden.sqlite3'}",
        filing_artifact_dir=tmp_path / "artifacts",
        filing_worker_heartbeat_seconds=5,
        filing_worker_lease_seconds=30,
        filing_index_enabled=False,
        langfuse_enabled=False,
        otel_enabled=False,
    )
    runtime = FilingRuntime(settings)
    imported = import_manifest(
        runtime.store,
        manifest_path=INFY_MANIFEST,
        workspace_id="golden",
    )
    assert imported.registered == 108

    documents = [
        document
        for document in runtime.store.documents(
            workspace_id="golden",
            company_id="NSE:INFY",
            category="xbrl_financial",
            current_only=False,
            limit=500,
        )
        if document.consolidation_scope.value == "consolidated"
    ]
    assert len(documents) == 13
    for document in documents:
        run, created = runtime.store.create_run(
            workspace_id="golden",
            company_id=document.company_id,
            filing_id=document.filing_id,
            idempotency_key=f"golden-{document.sha256}",
            max_attempts=1,
        )
        assert created is True
        runtime.store.mark_run_queued(run.run_id)
        completed = runtime.run_once(
            run.run_id,
            worker_id="golden-evaluation-worker",
        )
        assert completed.status.value == "completed"

    dataset = load_golden_dataset(INFY_GOLDEN)
    report = evaluate_golden_dataset(
        runtime.store,
        workspace_id="golden",
        dataset=dataset,
    )

    assert report.passed is True
    assert report.case_count == 13
    assert report.expected_fact_count == 52
    assert report.matched_fact_count == 52
    assert report.value_correct_count == 52
    assert report.evidence_correct_count == 52
    assert report.defects == []


@pytest.mark.skipif(
    not INFY_MANIFEST.is_file(),
    reason="local licensed/downloaded INFY filing pack is not present",
)
def test_golden_lock_allows_additive_manifest_evolution(tmp_path: Path) -> None:
    manifest_payload = json.loads(INFY_MANIFEST.read_text(encoding="utf-8"))
    manifest_payload["documents"].append(
        {
            "filename": "future-additive-document.xml",
            "sha256": "f" * 64,
            "acquisition_status": "success",
        }
    )
    manifest = tmp_path / "data/filings/nse/INFY/manifest.json"
    manifest.parent.mkdir(parents=True)
    manifest.write_text(json.dumps(manifest_payload), encoding="utf-8")

    dataset = load_golden_dataset(INFY_GOLDEN, repository_root=tmp_path)

    assert len(dataset.cases) == 13


@pytest.mark.skipif(
    not INFY_MANIFEST.is_file(),
    reason="local licensed/downloaded INFY filing pack is not present",
)
def test_golden_lock_rejects_missing_locked_source(tmp_path: Path) -> None:
    dataset = load_golden_dataset(INFY_GOLDEN, verify_manifest=False)
    missing_case = dataset.cases[0]
    manifest_payload = json.loads(INFY_MANIFEST.read_text(encoding="utf-8"))
    manifest_payload["documents"] = [
        item
        for item in manifest_payload["documents"]
        if item.get("sha256") != missing_case.source_sha256
    ]
    manifest = tmp_path / "data/filings/nse/INFY/manifest.json"
    manifest.parent.mkdir(parents=True)
    manifest.write_text(json.dumps(manifest_payload), encoding="utf-8")

    with pytest.raises(ValueError, match="locked sources changed or disappeared"):
        load_golden_dataset(INFY_GOLDEN, repository_root=tmp_path)
