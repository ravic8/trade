from __future__ import annotations

import pytest

from trade_research.storage.capacity import project_analytical_capacity


def test_project_analytical_capacity_is_explicit_and_deterministic() -> None:
    result = project_analytical_capacity(
        instrument_count=2_000,
        feature_count=100,
        retention_years=10,
    )

    assert result["feature_rows"] == 504_000_000
    assert result["prediction_rows_per_model"] == 5_040_000
    assert result["uncompressed_feature_bytes_lower"] == 24_192_000_000
    assert result["uncompressed_feature_bytes_upper"] == 60_480_000_000


@pytest.mark.parametrize("field", ["instrument_count", "feature_count", "retention_years"])
def test_project_analytical_capacity_rejects_non_positive_inputs(field: str) -> None:
    inputs = {"instrument_count": 1, "feature_count": 1, "retention_years": 1}
    inputs[field] = 0
    with pytest.raises(ValueError, match="must be positive"):
        project_analytical_capacity(**inputs)
