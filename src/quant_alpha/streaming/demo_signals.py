from __future__ import annotations

from datetime import datetime, timezone
from pathlib import Path

import pandas as pd

from quant_alpha.ingestion.energy import generate_synthetic_power_market
from quant_alpha.features.energy_alpha import add_energy_alpha_features
from quant_alpha.storage.duckdb import write_table


def generate_live_signals(
    markets: list[str] | None = None,
    n_hours: int = 48,
) -> pd.DataFrame:
    """Generate recent synthetic energy signals as if they were streamed."""
    markets = markets or ["DE_LU", "CZ", "FR"]
    end = pd.Timestamp.utcnow().floor("h")
    start = end - pd.Timedelta(hours=n_hours)
    raw = generate_synthetic_power_market(markets, start.isoformat(), end.isoformat(), freq="h")
    features = add_energy_alpha_features(raw)

    alpha_cols = [c for c in features.columns if c.startswith("alpha_energy_")]
    keep = ["timestamp", "market", "spot_price", "residual_load", "imbalance_price",
            "gas_price", "actual_load", *alpha_cols]
    frame = features[[c for c in keep if c in features.columns]].copy()
    frame["ingested_at"] = datetime.now(timezone.utc).isoformat()
    return frame.sort_values(["timestamp", "market"]).reset_index(drop=True)


def seed_demo_signals(duckdb_path: Path, n_hours: int = 48) -> int:
    """Write synthetic live signals into DuckDB for dashboard demo."""
    frame = generate_live_signals(n_hours=n_hours)
    write_table(duckdb_path, "live_energy_signals", frame)
    return len(frame)


if __name__ == "__main__":
    root = Path(__file__).resolve().parents[3]
    db = root / "data/warehouse/second_foundation.duckdb"
    n = seed_demo_signals(db)
    print(f"Seeded {n} rows into live_energy_signals")
