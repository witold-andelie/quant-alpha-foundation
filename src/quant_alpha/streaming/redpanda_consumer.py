from __future__ import annotations

from datetime import datetime, timezone
from pathlib import Path

import pandas as pd


def _load_schema(path: Path) -> dict:
    import json
    from fastavro import parse_schema

    with path.open("r", encoding="utf-8") as f:
        return parse_schema(json.load(f))


def consume_energy_signals(
    bootstrap_servers: str,
    topic: str,
    schema_path: Path,
    max_messages: int = 10,
    max_empty_polls: int = 30,
) -> list[dict]:
    import io
    from confluent_kafka import Consumer
    from fastavro import schemaless_reader

    import re

    schema = _load_schema(schema_path)
    consumer: Consumer | None = None
    try:
        consumer = Consumer(
            {
                "bootstrap.servers": bootstrap_servers,
                "group.id": "second-foundation-demo",
                "auto.offset.reset": "earliest",
            }
        )
        consumer.subscribe([topic])
        messages: list[dict] = []
        empty_polls = 0

        while len(messages) < max_messages and empty_polls < max_empty_polls:
            msg = consumer.poll(1.0)
            if msg is None or msg.error():
                empty_polls += 1
                continue
            messages.append(schemaless_reader(io.BytesIO(msg.value()), schema))
            empty_polls = 0

        return messages
    finally:
        if consumer is not None:
            consumer.close()


def consume_and_store(
    bootstrap_servers: str,
    topic: str,
    schema_path: Path,
    duckdb_path: Path,
    table: str = "live_energy_signals",
    max_messages: int = 500,
) -> int:
    """Consume Avro messages from Redpanda and upsert into a DuckDB table.

    Returns the count of rows written.
    """
    import duckdb

    rows = consume_energy_signals(bootstrap_servers, topic, schema_path, max_messages=max_messages)
    if not rows:
        return 0

    frame = pd.DataFrame(rows)
    frame["ingested_at"] = datetime.now(timezone.utc).isoformat()

    import re
    if not re.match(r'^[a-zA-Z_][a-zA-Z0-9_]*$', table):
        raise ValueError(f"Invalid table name: {table!r}")

    with duckdb.connect(str(duckdb_path)) as con:
        # Create table on first call; schema inferred from frame columns.
        con.execute(
            f"CREATE TABLE IF NOT EXISTS {table} AS SELECT * FROM frame WHERE false"
        )
        con.execute(f"INSERT INTO {table} SELECT * FROM frame")

    return len(frame)


if __name__ == "__main__":
    root = Path(__file__).resolve().parents[3]
    schema_path = root / "schemas/energy_signal.avsc"
    duckdb_path = root / "data/warehouse/second_foundation.duckdb"
    n = consume_and_store("localhost:19092", "energy-signals", schema_path, duckdb_path)
    print(f"Wrote {n} rows to live_energy_signals")
