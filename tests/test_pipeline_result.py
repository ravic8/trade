import pytest

from trade_research.pipelines.base import PipelineRunResult


@pytest.mark.parametrize(
    ("status", "expected"),
    [
        ("pass", "succeeded"),
        ("warn", "degraded"),
        ("fail", "failed"),
    ],
)
def test_pipeline_result_exposes_canonical_business_outcome(
    status: str,
    expected: str,
) -> None:
    result = PipelineRunResult(name="pipeline", status=status)

    assert result.business_outcome == expected


def test_pipeline_result_rejects_unknown_status_at_boundary() -> None:
    result = PipelineRunResult(name="pipeline", status="completed")

    with pytest.raises(ValueError, match="expected pass, warn, or fail"):
        _ = result.business_outcome
