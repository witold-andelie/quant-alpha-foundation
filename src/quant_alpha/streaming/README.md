# Streaming

This module implements the real-time signal bus for the energy track. It uses Redpanda (Kafka-compatible) as the message broker and Avro for schema-enforced serialization.

## Architecture

```
Synthetic / ENTSO-E data
        |
        v
   redpanda_producer.py  -->  Redpanda topic: energy-signals
                                      |
                               Avro (schemaless)
                                      |
                                      v
                          redpanda_consumer.py  -->  DuckDB: live_energy_signals
                                      |
                                      v
                          streamlit_app/app.py  -->  Live Signals tab
```

## Avro Schema (`schemas/energy_signal.avsc`)

Each message carries the minimal payload needed for real-time signal computation:

```json
{
  "timestamp": "string (ISO-8601)",
  "market":    "string (bidding zone, e.g. DE_LU)",
  "spot_price":       "double (€/MWh)",
  "residual_load":    "double (GW)",
  "imbalance_price":  "double (€/MWh)"
}
```

## Producer (`redpanda_producer.py`)

Publishes serialized Avro messages to the `energy-signals` topic. Uses the synthetic power-market generator for demo runs.

```bash
# Requires Redpanda running (see docker-compose.yml)
python -m quant_alpha.streaming.redpanda_producer
```

## Consumer (`redpanda_consumer.py`)

Two entry points:

| Function | Purpose |
|---|---|
| `consume_energy_signals()` | Returns a list of decoded dicts, does not persist |
| `consume_and_store()` | Decodes messages and upserts them into DuckDB `live_energy_signals` |

```bash
# Consume and write to DuckDB
python -m quant_alpha.streaming.redpanda_consumer
```

## Demo Mode (`demo_signals.py`)

Generates synthetic energy signals for the last N hours and writes them directly to DuckDB without Redpanda. Used when Docker is not available.

```bash
python -m quant_alpha.streaming.demo_signals
```

The Streamlit dashboard also exposes a "Seed demo signals" button that calls this function interactively.

## Starting the Full Streaming Stack

```bash
# Start Redpanda and Redpanda Console
docker compose up -d redpanda redpanda-console

# Publish sample messages
python -m quant_alpha.streaming.redpanda_producer

# Consume and write to DuckDB
python -m quant_alpha.streaming.redpanda_consumer

# View Redpanda Console (topic browser, consumer groups)
open http://localhost:8081
```

## Configuration

| Parameter | Default | Description |
|---|---|---|
| `bootstrap_servers` | `localhost:19092` | Redpanda external listener |
| `topic` | `energy-signals` | Kafka topic name |
| `group.id` | `second-foundation-demo` | Consumer group |
| `max_messages` | 500 | Max messages per consumer run |

## Adding a New Topic

1. Define a new Avro schema in `schemas/`.
2. Create a producer function following the pattern in `redpanda_producer.py`.
3. Create a consumer function that decodes the new schema and writes to a named DuckDB table.
4. Register the topic in `docker-compose.yml` if it needs to be auto-created.
