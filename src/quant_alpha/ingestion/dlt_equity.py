"""
dlt-based equity price pipeline.

Covers the Zoomcamp Workshop knowledge points:
  - REST API source with normalization
  - Incremental loading by date
  - Schema inference and column-level type hints
  - Pipeline state persistence (only loads new trading days on each run)
"""
from __future__ import annotations

import os
from pathlib import Path
from typing import Iterator

import dlt
import pandas as pd

from quant_alpha.config import ProjectConfig, Universe
from quant_alpha.ingestion.yahoo import fetch_prices


@dlt.source(name="equity_prices")
def equity_source(
    cfg: ProjectConfig,
    universe: Universe,
    offline: bool = True,
):
    """dlt source for equity daily OHLCV data."""

    @dlt.resource(
        name="equity_ohlcv",
        write_disposition="append",
        primary_key=["date", "symbol"],
        columns={
            "date":      {"data_type": "date",   "nullable": False},
            "symbol":    {"data_type": "text",   "nullable": False},
            "open":      {"data_type": "double", "nullable": True},
            "high":      {"data_type": "double", "nullable": True},
            "low":       {"data_type": "double", "nullable": True},
            "close":     {"data_type": "double", "nullable": False},
            "adj_close": {"data_type": "double", "nullable": True},
            "volume":    {"data_type": "bigint", "nullable": True},
        },
    )
    def equity_ohlcv(
        last_date=dlt.sources.incremental(
            "date",
            initial_value="2000-01-01",
        ),
    ) -> Iterator[dict]:
        """Yield OHLCV rows, only those after the last loaded date."""
        prices = fetch_prices(cfg, universe, offline=offline)
        prices["date"] = pd.to_datetime(prices["date"])

        cutoff = pd.Timestamp(last_date.last_value) if last_date.last_value else None
        if cutoff is not None:
            prices = prices[prices["date"] > cutoff]

        for row in prices.to_dict(orient="records"):
            row["date"] = pd.Timestamp(row["date"]).date().isoformat()
            row["volume"] = int(row.get("volume", 0) or 0)
            yield row

    return equity_ohlcv


def build_equity_pipeline(
    duckdb_path: Path,
    dataset_name: str = "dlt_equity_raw",
) -> dlt.Pipeline:
    os.environ["DESTINATION__DUCKDB__CREDENTIALS"] = str(duckdb_path)
    try:
        return dlt.pipeline(
            pipeline_name="equity_alpha",
            destination="duckdb",
            dataset_name=dataset_name,
        )
    finally:
        os.environ.pop("DESTINATION__DUCKDB__CREDENTIALS", None)


def run_dlt_equity_pipeline(
    duckdb_path: Path,
    cfg: ProjectConfig | None = None,
    universe: Universe | None = None,
    offline: bool = True,
) -> dict:
    """
    Run the dlt equity ingestion pipeline.

    On first run: loads full history from cfg.start_date.
    Subsequent runs: only loads trading days newer than the last ingested date.
    """
    if cfg is None:
        cfg = ProjectConfig()
    if universe is None:
        universe = Universe(name="demo", symbols=["AAPL", "MSFT", "GOOGL", "AMZN", "META"])

    pipeline = build_equity_pipeline(duckdb_path)
    source = equity_source(cfg=cfg, universe=universe, offline=offline)
    try:
        load_info = pipeline.run(source)
    except Exception as exc:
        raise RuntimeError(f"dlt equity pipeline failed: {exc}") from exc

    return {
        "pipeline": pipeline.pipeline_name,
        "dataset": pipeline.dataset_name,
        "duckdb_path": str(duckdb_path),
        "load_packages": len(load_info.load_packages),
        "schema": pipeline.default_schema_name,
    }


if __name__ == "__main__":
    root = Path(__file__).resolve().parents[3]
    db = root / "data/warehouse/quant_alpha.duckdb"
    info = run_dlt_equity_pipeline(db)
    print("dlt equity pipeline complete:", info)
