from __future__ import annotations

from pathlib import Path

import duckdb
import pandas as pd


def _safe_identifier(name: str) -> str:
    """Validate that name is a safe SQL identifier (alphanumeric + underscores)."""
    if not name.replace(".", "_").replace("_", "a").isalnum():
        raise ValueError(f"Unsafe SQL identifier: {name!r}")
    return name


def write_table(db_path: Path, table_name: str, frame: pd.DataFrame) -> None:
    _safe_identifier(table_name)
    db_path.parent.mkdir(parents=True, exist_ok=True)
    with duckdb.connect(str(db_path)) as con:
        con.register("_frame", frame)
        con.execute(f"create or replace table {table_name} as select * from _frame")
        con.unregister("_frame")


def write_metrics(db_path: Path, metrics: dict[str, float], table_name: str = "backtest_metrics") -> None:
    frame = pd.DataFrame([metrics]) if metrics else pd.DataFrame()
    write_table(db_path, table_name, frame)


def table_exists(db_path: Path, table_name: str) -> bool:
    if not db_path.exists():
        return False
    try:
        with duckdb.connect(str(db_path), read_only=True) as con:
            result = con.execute(
                "select count(*) from information_schema.tables where table_name = ?",
                [table_name],
            ).fetchone()[0]
        return bool(result)
    except Exception:
        return False
