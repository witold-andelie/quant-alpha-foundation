from __future__ import annotations

import json
from pathlib import Path

import pandas as pd

from quant_alpha.ingestion.energy import generate_synthetic_power_market


def _load_schema(path: Path) -> dict:
    from fastavro import parse_schema

    with path.open("r", encoding="utf-8") as f:
        return parse_schema(json.load(f))


def _serialize(schema: dict, payload: dict) -> bytes:
    import io
    from fastavro import schemaless_writer

    buf = io.BytesIO()
    schemaless_writer(buf, schema, payload)
    return buf.getvalue()


def publish_energy_signals(bootstrap_servers: str, topic: str, schema_path: Path, sample_size: int = 100) -> None:
    from confluent_kafka import Producer, KafkaException

    try:
        producer = Producer({"bootstrap.servers": bootstrap_servers})
        schema = _load_schema(schema_path)
        market = generate_synthetic_power_market(["DE_LU", "CZ", "FR"], "2024-01-01", "2024-01-07")
        for row in market.head(sample_size).to_dict(orient="records"):
            payload = {
                "timestamp": pd.Timestamp(row["timestamp"]).isoformat(),
                "market": row["market"],
                "spot_price": float(row["spot_price"]),
                "residual_load": float(row["residual_load"]),
                "imbalance_price": float(row["imbalance_price"]),
            }
            producer.produce(topic, _serialize(schema, payload))
        producer.flush()
    except KafkaException as exc:
        raise RuntimeError(f"Kafka error publishing to {topic!r}: {exc}") from exc
    except Exception as exc:
        raise RuntimeError(f"Failed to publish signals to {topic!r}: {exc}") from exc


if __name__ == "__main__":
    root = Path(__file__).resolve().parents[3]
    publish_energy_signals("localhost:19092", "energy-signals", root / "schemas/energy_signal.avsc")
