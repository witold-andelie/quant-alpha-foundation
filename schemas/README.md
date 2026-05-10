# Schemas

This directory contains Avro schema definitions for the streaming layer. Schema enforcement ensures that producers and consumers agree on message structure without relying on implicit conventions.

## Why Avro

Avro provides:

- Binary serialization (compact, fast)
- Schema evolution with backward and forward compatibility
- Integration with Kafka/Redpanda via the schema registry pattern
- Language-agnostic definitions that can be used by Python, Java, and Scala consumers

## `energy_signal.avsc`

The primary schema for the `energy-signals` Redpanda topic.

```json
{
  "type": "record",
  "name": "EnergySignal",
  "fields": [
    {"name": "timestamp",      "type": "string"},
    {"name": "market",         "type": "string"},
    {"name": "spot_price",     "type": "double"},
    {"name": "residual_load",  "type": "double"},
    {"name": "imbalance_price","type": "double"}
  ]
}
```

### Field Descriptions

| Field | Type | Description |
|---|---|---|
| `timestamp` | string (ISO-8601) | UTC hour of the observation |
| `market` | string | ENTSO-E bidding zone code (e.g. `DE_LU`, `FR`) |
| `spot_price` | double | Day-ahead spot price (€/MWh) |
| `residual_load` | double | Residual load after renewable generation (GW) |
| `imbalance_price` | double | Balancing market price (€/MWh) |

## Using the Schema

**Serialize (producer side):**

```python
from fastavro import parse_schema, schemaless_writer
import io, json

with open("schemas/energy_signal.avsc") as f:
    schema = parse_schema(json.load(f))

buf = io.BytesIO()
schemaless_writer(buf, schema, payload)
message_bytes = buf.getvalue()
```

**Deserialize (consumer side):**

```python
from fastavro import schemaless_reader
import io

record = schemaless_reader(io.BytesIO(msg.value()), schema)
```

## Schema Evolution Guidelines

When changing an existing schema:

1. Add new fields with a `"default"` value so older messages remain readable.
2. Never remove or rename fields; instead, deprecate and add a new field.
3. Update this README and the corresponding test in `tests/test_entsoe.py`.
4. Bump the schema version in the field metadata if using a registry.

## Adding a New Schema

1. Create a `.avsc` file in this directory.
2. Register it with a corresponding producer and consumer module in `src/quant_alpha/streaming/`.
3. Add validation of the schema in the CI pipeline.
