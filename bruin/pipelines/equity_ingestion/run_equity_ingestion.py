"""Bruin asset runner: raw_equity_ohlcv."""
from __future__ import annotations

import os
import sys
from pathlib import Path

from quant_alpha.config import load_project_config, load_universe
from quant_alpha.pipeline import run_pipeline

if __name__ == "__main__":
    try:
        root = Path(os.environ.get("PROJECT_ROOT", "."))
        config = root / "configs" / "project.yaml"
        offline = os.environ.get("OFFLINE", "true").lower() == "true"

        result = run_pipeline(config, root.resolve(), offline=offline)
        print(f"raw_equity_ohlcv: {result['rows']} rows → {result['duckdb_path']}")
    except Exception as exc:
        print(f"ERROR: {exc}", file=sys.stderr)
        sys.exit(1)
