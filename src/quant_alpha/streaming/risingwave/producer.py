"""
Kafka/Redpanda producer — streams synthetic energy signals to the
power_market_signals topic in JSON format.

Run standalone:
    python -m quant_alpha.streaming.risingwave.producer

Or via docker-compose.risingwave.yml (signal-producer service).
"""
from __future__ import annotations

import json
import os
import time
from datetime import datetime, timezone

import pandas as pd

from quant_alpha.ingestion.energy import generate_synthetic_power_market

BOOTSTRAP = os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
TOPIC = os.environ.get("KAFKA_TOPIC", "power_market_signals")
MARKETS = [m.strip() for m in os.environ.get("MARKETS", "DE_LU,CZ,FR").split(",")]
INTERVAL = float(os.environ.get("INTERVAL_SECONDS", "60"))


def _make_producer():
    try:
        from confluent_kafka import Producer  # type: ignore[import]
    except ImportError as e:
        raise ImportError("Install confluent-kafka: pip install confluent-kafka") from e
    return Producer({"bootstrap.servers": BOOTSTRAP})


def _delivery_report(err, msg):
    if err:
        print(f"[producer] delivery failed: {err}")


def stream_signals(producer, once: bool = False) -> None:
    """Continuously emit one 'current hour' row per market to the topic."""
    while True:
        now = pd.Timestamp.utcnow().floor("h")
        frame = generate_synthetic_power_market(
            MARKETS,
            now.isoformat(),
            (now + pd.Timedelta(hours=1)).isoformat(),
            freq="h",
        )
        for _, row in frame.iterrows():
            payload = {
                k: (v.isoformat() if isinstance(v, pd.Timestamp) else float(v) if pd.notna(v) else None)
                for k, v in row.items()
            }
            payload["produced_at"] = datetime.now(timezone.utc).isoformat()
            producer.produce(
                TOPIC,
                key=f"{payload['market']}:{payload['timestamp']}",
                value=json.dumps(payload).encode(),
                callback=_delivery_report,
            )
        producer.flush()
        print(f"[producer] emitted {len(frame)} rows for {now} → {MARKETS}")
        if once:
            break
        time.sleep(INTERVAL)


if __name__ == "__main__":
    p = _make_producer()
    print(f"[producer] streaming to {BOOTSTRAP}/{TOPIC}, interval={INTERVAL}s")
    stream_signals(p)
