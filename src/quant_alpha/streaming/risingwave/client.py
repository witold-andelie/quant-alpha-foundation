"""RisingWave client — applies streaming SQL views and queries materialized results."""
from __future__ import annotations

from pathlib import Path
from typing import Any

import pandas as pd

_VIEWS_SQL = Path(__file__).parent / "views.sql"

# DDL statements that are safe to re-run (CREATE ... IF NOT EXISTS)
_DDL_KEYWORDS = ("CREATE SOURCE", "CREATE MATERIALIZED VIEW")


def _split_statements(sql: str) -> list[str]:
    """Split a SQL file on ';' boundaries, drop comments and blanks."""
    stmts = []
    current: list[str] = []
    for line in sql.splitlines():
        stripped = line.strip()
        if stripped.startswith("--"):
            continue
        current.append(line)
        if stripped.endswith(";"):
            joined = "\n".join(current).strip().rstrip(";")
            if any(kw in joined.upper() for kw in _DDL_KEYWORDS) or joined:
                stmts.append(joined)
            current = []
    return [s for s in stmts if s.strip()]


def apply_views(conn: Any, views_sql_path: Path | None = None) -> list[str]:
    """
    Apply all CREATE SOURCE / CREATE MATERIALIZED VIEW statements.

    `conn` must be a psycopg2-compatible connection (RisingWave speaks PostgreSQL wire protocol).

    Returns list of statement names applied.
    """
    sql = (views_sql_path or _VIEWS_SQL).read_text()
    stmts = _split_statements(sql)
    applied = []
    with conn.cursor() as cur:
        for stmt in stmts:
            if not any(kw in stmt.upper() for kw in _DDL_KEYWORDS):
                continue
            cur.execute(stmt)
            name = stmt.split()[-1].split("(")[0].strip().lower()
            applied.append(name)
    conn.commit()
    return applied


def query_realtime_scores(
    conn: Any,
    market: str | None = None,
    limit: int = 100,
) -> pd.DataFrame:
    """Query the mv_realtime_alpha_scores materialized view."""
    params: list[object] = []
    where = ""
    if market:
        where = "WHERE market = %s"
        params.append(market)
    params.append(limit)
    sql = f"""
        SELECT *
        FROM mv_realtime_alpha_scores
        {where}
        ORDER BY timestamp DESC
        LIMIT %s
    """
    with conn.cursor() as cur:
        cur.execute(sql, params)
        cols = [d[0] for d in cur.description]
        rows = cur.fetchall()
    return pd.DataFrame(rows, columns=cols)


def query_hourly_window(
    conn: Any,
    market: str | None = None,
    hours: int = 24,
) -> pd.DataFrame:
    """Query the mv_energy_hourly_window materialized view for the last N hours."""
    if not isinstance(hours, int) or hours <= 0:
        raise ValueError(f"hours must be a positive integer, got {hours!r}")
    params: list[object] = []
    where_parts: list[str] = [f"window_start >= NOW() - INTERVAL '{hours} hours'"]
    if market:
        where_parts.insert(0, "market = %s")
        params.append(market)
    where = "WHERE " + " AND ".join(where_parts)
    sql = f"""
        SELECT *
        FROM mv_energy_hourly_window
        {where}
        ORDER BY window_start DESC
    """
    with conn.cursor() as cur:
        cur.execute(sql, params or None)
        cols = [d[0] for d in cur.description]
        rows = cur.fetchall()
    return pd.DataFrame(rows, columns=cols)


def query_scarcity_alerts(conn: Any, level: str = "HIGH") -> pd.DataFrame:
    """Return active scarcity alerts at or above the given level."""
    levels = {"HIGH": ["HIGH"], "MEDIUM": ["HIGH", "MEDIUM"], "LOW": ["HIGH", "MEDIUM", "LOW"]}
    valid_levels = levels.get(level.upper(), ["HIGH"])
    placeholders = ", ".join(["%s"] * len(valid_levels))
    sql = f"""
        SELECT *
        FROM mv_scarcity_alerts
        WHERE scarcity_level IN ({placeholders})
        ORDER BY timestamp DESC
        LIMIT 50
    """
    with conn.cursor() as cur:
        cur.execute(sql, valid_levels)
        cols = [d[0] for d in cur.description]
        rows = cur.fetchall()
    return pd.DataFrame(rows, columns=cols)


def get_connection(host: str = "localhost", port: int = 4566, database: str = "dev") -> Any:
    """
    Open a psycopg2 connection to RisingWave.
    RisingWave uses the PostgreSQL wire protocol — psycopg2 works natively.
    """
    try:
        import psycopg2  # type: ignore[import]
    except ImportError as e:
        raise ImportError("Install psycopg2: pip install psycopg2-binary") from e

    return psycopg2.connect(
        host=host,
        port=port,
        database=database,
        user="root",
        password="",
    )
