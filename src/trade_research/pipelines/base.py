from __future__ import annotations

from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Literal

PipelineBusinessOutcome = Literal["succeeded", "degraded", "failed"]


@dataclass(frozen=True)
class PipelineRunResult:
    name: str
    status: str
    rows: int = 0
    artifacts: dict[str, Path] = field(default_factory=dict)
    metrics: dict[str, Any] = field(default_factory=dict)
    warnings: list[str] = field(default_factory=list)
    blocking_issues: list[str] = field(default_factory=list)

    @property
    def business_outcome(self) -> PipelineBusinessOutcome:
        if self.status == "pass":
            return "succeeded"
        if self.status == "warn":
            return "degraded"
        if self.status == "fail":
            return "failed"
        raise ValueError(
            f"Unsupported pipeline status {self.status!r} for {self.name!r}; "
            "expected pass, warn, or fail."
        )
