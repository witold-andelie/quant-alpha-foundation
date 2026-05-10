# Batch Processing

This module implements Apache Spark-based feature computation for large-scale energy datasets. It covers the zoomcamp Module 6 (Batch Processing) knowledge points.

## Why Spark

The Python factor engine in `features/energy_alpha.py` uses pandas and runs in-process. For large historical backtests or real-time recomputation over many markets and long histories, Spark provides:

- Distributed execution across a cluster
- Native Parquet read/write with predicate pushdown
- Window functions over partitioned datasets without pandas memory constraints
- Easy scaling from local mode to a GKE or EMR cluster

## Entry Point (`spark_energy_features.py`)

```bash
python -m quant_alpha.batch.spark_energy_features
```

Reads `data/raw/power_market.parquet`, computes rolling features, and writes the result to `data/processed/power_market_spark_features.parquet`.

## Spark Session Configuration

The session runs in local mode by default:

```python
SparkSession.builder
    .appName("second-foundation-energy-batch")
    .master("local[*]")
    .config("spark.sql.session.timeZone", "UTC")
    .getOrCreate()
```

For cluster deployment, replace `.master("local[*]")` with the cluster URL or use `spark-submit` with `--master yarn` / `--master k8s://...`.

## Computed Features

| Feature | Window | Description |
|---|---|---|
| `spot_return_1h` | 1 period | Log return of spot price |
| `rolling_spot_mean_24h` | 24 hours | 24-hour rolling mean of spot price |
| `rolling_spot_std_24h` | 24 hours | 24-hour rolling standard deviation |
| `rolling_residual_mean_168h` | 168 hours | 7-day rolling mean of residual load |
| `residual_load_shock` | 168 hours | Deviation from 7-day mean (scarcity proxy) |
| `imbalance_premium` | — | Spot-to-imbalance price spread |
| `scarcity_flag` | — | Binary indicator when residual load shock > 5 GW |

## Window Functions

All rolling aggregations use Spark `Window` with `rowsBetween`, partitioned by `market` and ordered by `timestamp`:

```python
w_market = Window.partitionBy("market").orderBy("timestamp")
w_24  = w_market.rowsBetween(-23, 0)   # 24-hour window (inclusive)
w_168 = w_market.rowsBetween(-167, 0)  # 7-day window
```

## Output

The output is coalesced to a single Parquet file. For large datasets, remove `.coalesce(1)` and let Spark choose the partition count.

```
data/processed/power_market_spark_features.parquet
```

## Kestra Integration

The Spark job runs as the second task in the energy pipeline flow:

```yaml
- id: spark_batch_features
  type: io.kestra.plugin.scripts.shell.Commands
  commands:
    - python -m quant_alpha.batch.spark_energy_features
```

## Cluster Deployment

To run on a real Spark cluster (e.g., Dataproc or a self-managed cluster on GKE):

1. Build and push the Docker image via the CI/CD pipeline.
2. Set `spark.master` to the cluster URL in `build_spark_session()`.
3. Upload the input Parquet to GCS and update `input_path` in `compute_energy_features()`.
4. Submit via `spark-submit` or a Kestra `SparkSubmit` task.
