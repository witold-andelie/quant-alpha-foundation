from __future__ import annotations

from quant_alpha.ingestion.energy import generate_synthetic_power_market
from quant_alpha.platform.quality import run_energy_quality_checks


def test_energy_quality_checks_pass_for_synthetic_data() -> None:
    frame = generate_synthetic_power_market(["DE_LU", "CZ"], "2024-01-01", "2024-01-03")
    checks = run_energy_quality_checks(frame)

    assert checks["passed"].all()
