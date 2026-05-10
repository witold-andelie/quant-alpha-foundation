from __future__ import annotations

import tempfile
from pathlib import Path

import duckdb

from quant_alpha.ingestion.dlt_energy import run_dlt_energy_pipeline
from quant_alpha.ingestion.dlt_equity import run_dlt_equity_pipeline
from quant_alpha.config import ProjectConfig, Universe


def test_dlt_energy_pipeline_creates_table() -> None:
    with tempfile.TemporaryDirectory() as tmp:
        db = Path(tmp) / "test.duckdb"
        info = run_dlt_energy_pipeline(
            db,
            markets=["DE_LU", "FR"],
            start="2024-01-01",
            end="2024-01-08",
        )
        assert info["load_packages"] == 1
        with duckdb.connect(str(db), read_only=True) as con:
            n = con.execute(
                "SELECT count(*) FROM dlt_energy_raw.power_market_raw"
            ).fetchone()[0]
        # date_range("2024-01-01", "2024-01-08", freq="h") includes both ends → 169h × 2 markets
        assert n > 300, f"Expected >300 rows, got {n}"


def test_dlt_energy_incremental_skips_loaded_data() -> None:
    with tempfile.TemporaryDirectory() as tmp:
        db = Path(tmp) / "test.duckdb"
        run_dlt_energy_pipeline(db, markets=["DE_LU"], start="2024-01-01", end="2024-01-08")
        # Second run with same range: nothing to load
        info2 = run_dlt_energy_pipeline(db, markets=["DE_LU"], start="2024-01-01", end="2024-01-08")
        assert info2["load_packages"] == 0, "Incremental run should produce 0 packages"


def test_dlt_energy_incremental_loads_new_dates() -> None:
    with tempfile.TemporaryDirectory() as tmp:
        db = Path(tmp) / "test.duckdb"
        run_dlt_energy_pipeline(db, markets=["DE_LU"], start="2024-01-01", end="2024-01-08")
        # Extend range: should only load new data
        info2 = run_dlt_energy_pipeline(db, markets=["DE_LU"], start="2024-01-01", end="2024-01-15")
        assert info2["load_packages"] == 1, "Extended range should load 1 package"

        with duckdb.connect(str(db), read_only=True) as con:
            n = con.execute(
                "SELECT count(*) FROM dlt_energy_raw.power_market_raw"
            ).fetchone()[0]
        # date_range includes both endpoints; 14 days + 1 endpoint = 337h
        assert n > 300, f"Expected >300 rows after extension, got {n}"


def test_dlt_equity_pipeline_creates_table() -> None:
    with tempfile.TemporaryDirectory() as tmp:
        db = Path(tmp) / "test.duckdb"
        cfg = ProjectConfig(start_date="2024-01-01", end_date="2024-02-01")
        universe = Universe(name="test", symbols=["AAA", "BBB", "CCC"])
        info = run_dlt_equity_pipeline(db, cfg=cfg, universe=universe, offline=True)
        assert info["load_packages"] == 1
        with duckdb.connect(str(db), read_only=True) as con:
            n = con.execute(
                "SELECT count(*) FROM dlt_equity_raw.equity_ohlcv"
            ).fetchone()[0]
        assert n > 0


def test_dlt_equity_schema_has_required_columns() -> None:
    with tempfile.TemporaryDirectory() as tmp:
        db = Path(tmp) / "test.duckdb"
        cfg = ProjectConfig(start_date="2024-01-01", end_date="2024-01-15")
        universe = Universe(name="test", symbols=["AAA", "BBB"])
        run_dlt_equity_pipeline(db, cfg=cfg, universe=universe, offline=True)
        with duckdb.connect(str(db), read_only=True) as con:
            cols = [
                r[0]
                for r in con.execute(
                    "SELECT column_name FROM information_schema.columns "
                    "WHERE table_schema='dlt_equity_raw' AND table_name='equity_ohlcv'"
                ).fetchall()
            ]
        for required in ("date", "symbol", "close", "volume"):
            assert required in cols, f"Missing column: {required}"
