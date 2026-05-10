"""
dlt-based energy data pipeline.

Covers the DataTalksClub Zoomcamp Workshop (Data Ingestion) knowledge points:
  - Declarative resource definitions with @dlt.resource
  - Incremental loading (only fetches records newer than the last run)
  - Automatic schema inference and evolution
  - Normalization pipeline with dlt.run()
  - DuckDB and BigQuery as interchangeable destinations
"""
from __future__ import annotations

import os
from pathlib import Path
from typing import Iterator

import dlt
import pandas as pd

from quant_alpha.ingestion.energy import generate_synthetic_power_market


@dlt.source(name="power_market")
def power_market_source(
    markets: list[str],
    start: str,
    end: str,
    freq: str = "h",
):
    """dlt source for European power-market data (synthetic or ENTSO-E)."""

    @dlt.resource(
        name="power_market_raw",
        write_disposition="append",
        primary_key=["timestamp", "market"],
        columns={
            "timestamp":       {"data_type": "timestamp", "nullable": False},
            "market":          {"data_type": "text",      "nullable": False},
            "spot_price":      {"data_type": "double",    "nullable": True},
            "load_forecast":   {"data_type": "double",    "nullable": True},
            "actual_load":     {"data_type": "double",    "nullable": True},
            "wind_forecast":   {"data_type": "double",    "nullable": True},
            "solar_forecast":  {"data_type": "double",    "nullable": True},
            "residual_load":   {"data_type": "double",    "nullable": True},
            "imbalance_price": {"data_type": "double",    "nullable": True},
            "gas_price":       {"data_type": "double",    "nullable": True},
        },
    )
    def power_market_raw(
        last_timestamp=dlt.sources.incremental(
            "timestamp",
            initial_value="2000-01-01T00:00:00",
        ),
    ) -> Iterator[dict]:
        """Yield hourly power-market records, only those after the last loaded timestamp."""
        frame = generate_synthetic_power_market(markets, start, end, freq=freq)
        frame["timestamp"] = pd.to_datetime(frame["timestamp"])

        cutoff = pd.Timestamp(last_timestamp.last_value) if last_timestamp.last_value else None
        if cutoff is not None:
            frame = frame[frame["timestamp"] > cutoff]

        for row in frame.to_dict(orient="records"):
            row["timestamp"] = pd.Timestamp(row["timestamp"]).isoformat()
            yield row

    return power_market_raw


def build_energy_pipeline(
    duckdb_path: Path,
    dataset_name: str = "dlt_energy_raw",
) -> dlt.Pipeline:
    """Create and return a configured dlt pipeline for energy data."""
    os.environ["DESTINATION__DUCKDB__CREDENTIALS"] = str(duckdb_path)
    try:
        return dlt.pipeline(
            pipeline_name="second_foundation_energy",
            destination="duckdb",
            dataset_name=dataset_name,
        )
    finally:
        os.environ.pop("DESTINATION__DUCKDB__CREDENTIALS", None)


def run_dlt_energy_pipeline(
    duckdb_path: Path,
    markets: list[str] | None = None,
    start: str = "2023-01-01",
    end: str | None = None,
) -> dict:
    """
    Run the dlt energy ingestion pipeline.

    Supports incremental loads: on the first run loads the full history;
    on subsequent runs only loads records newer than the last ingested timestamp.
    """
    if end is None:
        end = pd.Timestamp.utcnow().date().isoformat()
    markets = markets or ["DE_LU", "CZ", "FR"]

    pipeline = build_energy_pipeline(duckdb_path)
    source = power_market_source(markets=markets, start=start, end=end)
    try:
        load_info = pipeline.run(source)
    except Exception as exc:
        raise RuntimeError(f"dlt energy pipeline failed: {exc}") from exc

    return {
        "pipeline": pipeline.pipeline_name,
        "dataset": pipeline.dataset_name,
        "duckdb_path": str(duckdb_path),
        "load_packages": len(load_info.load_packages),
        "schema": pipeline.default_schema_name,
    }


if __name__ == "__main__":
    root = Path(__file__).resolve().parents[3]
    db = root / "data/warehouse/second_foundation.duckdb"
    info = run_dlt_energy_pipeline(db)
    print("dlt energy pipeline complete:", info)
