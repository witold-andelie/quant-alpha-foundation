"""Bruin asset runner: raw_power_market."""
from __future__ import annotations

import os
import sys
from pathlib import Path

from quant_alpha.config import load_project_config
from quant_alpha.pipeline_energy import run_energy_pipeline

if __name__ == "__main__":
    try:
        root = Path(os.environ.get("PROJECT_ROOT", "."))
        config = root / "configs" / "second_foundation_project.yaml"
        source = os.environ.get("ENERGY_SOURCE", None)

        result = run_energy_pipeline(config, root.resolve(), source_override=source)
        print(f"raw_power_market: {result['rows']} rows → {result['duckdb_path']}")
        print(f"data_source: {result['data_source']}")
    except Exception as exc:
        print(f"ERROR: {exc}", file=sys.stderr)
        sys.exit(1)
