"""Inventory the current migration without moving files or inspecting secrets."""

from __future__ import annotations

import argparse
import ast
import json
import subprocess
from collections import Counter
from pathlib import Path

RESEARCH = (
    "features",
    "targets",
    "modeling",
    "screeners",
    "research.artifacts",
    "research.factors",
    "research.ml_artifacts",
    "pipelines.backtest",
    "pipelines.baselines",
    "pipelines.daily_features",
    "pipelines.daily_targets",
    "pipelines.daily_pipeline_health",
    "pipelines.factor_research",
    "pipelines.latest_predictions",
    "pipelines.lightgbm_models",
    "pipelines.ml_dataset",
    "pipelines.opportunity_targets",
    "pipelines.processed_validation",
    "pipelines.walk_forward",
    "validation.processed_datasets",
)
DISCARD = (
    "filings",
    "chat",
    "scraping",
    "research.documents",
    "research.embeddings",
    "storage.vector",
    "pipelines.dukascopy_intraday",
    "pipelines.yfinance_intraday",
    "pipelines.yfinance_nse_minute",
    "data.dukascopy_provider",
    "universe.dukascopy",
    "universe.yfinance_intraday",
    "market_data.aggregation_golden",
)
# Remaining historical columns/tables are intentionally frozen schema compatibility.
FROZEN_SCHEMA_MODULES = {"storage.timescale", "storage.legacy_schema", "control_plane.tables"}



def module_owner(module: str) -> str:
    name = module.removeprefix("trade_data.")
    if any(name == p or name.startswith(p + ".") for p in DISCARD):
        return "discard"
    if any(name == p or name.startswith(p + ".") for p in RESEARCH):
        return "trade-research"
    return "trade"


def inventory(root: Path) -> list[dict[str, object]]:
    names = (
        subprocess.check_output(
            ["git", "ls-files", "-z", "--cached", "--others", "--exclude-standard"],
            cwd=root,
        )
        .decode()
        .split("\0")
    )
    result: list[dict[str, object]] = []
    for name in sorted(set(names) - {""}):
        path = root / name
        if not path.is_file():
            continue
        entry: dict[str, object] = {
            "path": name,
            "present": path.is_file(),
            "owner": "trade",
            "state": "inventoried",
        }
        if name.startswith("src/trade_data/"):
            module = name.removeprefix("src/").removesuffix(".py").replace("/", ".")
            module = module.removesuffix(".__init__")
            entry["owner"] = module_owner(module)
            if module.removeprefix("trade_data.") in FROZEN_SCHEMA_MODULES:
                entry["state"] = "daily-runtime-with-preserved-historical-schema"
        elif name.startswith("apps/web/") or name == "Dockerfile.web":
            entry["owner"] = "trade-web"
        elif name.startswith(("native/", "notebooks/", "experiments/", "evaluations/")):
            entry["owner"] = "discard"
        elif name.startswith(("migrations/", "clickhouse/migrations/")):
            entry["owner"] = "trade"
            entry["state"] = "preserve-applied-history"
        if path.suffix == ".py" and path.is_file():
            tree = ast.parse(path.read_text(), filename=name)
            imports = sorted(
                {
                    node.module
                    for node in ast.walk(tree)
                    if isinstance(node, ast.ImportFrom)
                    and node.module
                    and node.module.startswith("trade_data")
                }
                | {
                    alias.name
                    for node in ast.walk(tree)
                    if isinstance(node, ast.Import)
                    for alias in node.names
                    if alias.name.startswith("trade_data")
                }
            )
            entry["project_imports"] = imports
            if name.startswith("tests/") and name != "tests/test_daily_import_boundaries.py":
                consumers = {module_owner(m) for m in imports} - {"trade"}
                if consumers:
                    entry["owner"] = (
                        next(iter(consumers)) if len(consumers) == 1 else "split-required"
                    )
            if entry["owner"] == "split-required":
                entry["symbols"] = [
                    node.name
                    for node in tree.body
                    if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef, ast.ClassDef))
                ]
        entry["action"] = {
            "trade": "keep",
            "trade-web": "move",
            "trade-research": "move",
            "discard": "remove-after-reference-check",
            "split-required": "classify-symbols",
        }[str(entry["owner"])]
        result.append(entry)
    return result


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    root = Path(__file__).resolve().parents[1]
    files = inventory(root)
    report = {
        "schema_version": 1,
        "status": "current present files; extracted/retired sources live in migration archives",
        "production_changes": False,
        "independent_repos_untouched": ["lens_fa", "nifty_stream"],
        "counts": dict(Counter(str(row["owner"]) for row in files)),
        "files": files,
    }
    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_text(json.dumps(report, indent=2) + "\n")
    print(json.dumps(report["counts"], sort_keys=True))


if __name__ == "__main__":
    main()
