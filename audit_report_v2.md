# Code Review Report

## Summary

Found 283 issue(s): 18 critical, 13 errors, 138 warnings, 114 info. Categories: 147 potential_bug, 40 convention, 37 readability, 23 performance, 18 code_style, 13 security, 5 architecture.

### Statistics

| Metric | Count |
|--------|-------|
| Critical | 18 |
| Error | 13 |
| Warning | 138 |
| Info | 114 |

**Files Reviewed**: 36

---

## CRITICAL Issues (18)

### [CRITICAL] Division by zero risk in return calculation

**File**: `src/quant_alpha/backtest/alpha_decay.py:30-31`
**Category**: Potential Bug
**Confidence**: 90%

The lambda `(s.shift(-horizon) / s) - 1` will divide by zero if any price `s` is zero. This will produce `inf` or `NaN` values that may silently propagate through the correlation calculation.

**Suggestion**:
```
Add a guard against zero values: `lambda s: (s.shift(-horizon) / s.replace(0, np.nan)) - 1`
```

---

### [CRITICAL] Missing error handling for I/O operations

**File**: `src/quant_alpha/batch/spark_energy_features.py:16-48`
**Category**: Potential Bug
**Confidence**: 90%

The function perform several critical operations without error handling: reading parquet files, computing features, and writing output. If any of these fail (e.g., input file doesn't exist, permission issues, invalid schema), the Spark session may not be properly stopped, leading to resource leaks.

**Suggestion**:
```
Add try/finally to ensure Spark session cleanup:
```python
def compute_energy_features(input_path: str, output_path: str) -> None:
    from pyspark.sql import Window, functions as F
    
    spark = build_spark_session()
    try:
        frame = spark.read.parquet(input_path)
        # ... rest of the code ...
        (enriched.coalesce(1)
         .write.mode("overwrite")
         .parquet(output_path))
    except Exception as e:
        logger.error(f"Error computing energy features: {e}")
        raise
    finally:
        spark.stop()
```
```

---

### [CRITICAL] coalesce(1) may cause performance bottleneck

**File**: `src/quant_alpha/batch/spark_energy_features.py:43`
**Category**: Performance
**Confidence**: 70%

Using coalesce(1) forces all output into a single partition/file, which can cause memory issues and performance bottlenecks for large datasets. This creates a single point of failure and makes downstream reading slower.

**Suggestion**:
```
Consider removing coalesce(1) or using a more reasonable partition count:
```python
# Option 1: Let Spark decide
enriched.write.mode("overwrite").parquet(output_path)

# Option 2: Partition by market for better query performance
enriched.write.mode("overwrite").partitionBy("market").parquet(output_path)
```
```

---

### [CRITICAL] No path traversal protection in resolve_path

**File**: `src/quant_alpha/config.py:86`
**Category**: Security
**Confidence**: 70%

resolve_path blindly concatenates root and path without checking for path traversal (e.g., '../../../etc/passwd'). If path comes from user-controlled YAML, this could allow accessing files outside the expected directory.

**Suggestion**:
```
Validate that the resolved path is still within root:
```python
def resolve_path(root: Path, path: Path) -> Path:
    resolved = (root / path).resolve() if not path.is_absolute() else path.resolve()
    if not resolved.is_relative_to(root.resolve()):
        raise ValueError(f'Path {path} escapes root directory {root}')
    return resolved
```
```

---

### [CRITICAL] expected_direction type allows non-logical values

**File**: `src/quant_alpha/features/registry.py:18`
**Category**: Potential Bug
**Confidence**: 85%

The expected_direction field is typed as int, but semantically should only be -1 or 1. There's no validation at the dataclass level to enforce this constraint, allowing values like 0, 2, or any other integer to be stored.

**Suggestion**:
```
Add a __post_init__ validation:
```python
@dataclass(frozen=True)
class AlphaDefinition:
    # ...
    expected_direction: int
    
    def __post_init__(self):
        if self.expected_direction not in (-1, 1):
            raise ValueError(f"expected_direction must be -1 or 1, got {self.expected_direction}")
```
```

---

### [CRITICAL] Missing type hint for power_market_source return

**File**: `src/quant_alpha/ingestion/dlt_energy.py:26`
**Category**: Convention
**Confidence**: 70%

The power_market_source function is missing a return type annotation. While the nested function has a type hint, the source function itself should indicate it returns a dlt resource.

**Suggestion**:
```
Add return type annotation:
```python
@dlt.source(name="power_market")
def power_market_source(
    markets: list[str],
    start: str,
    end: str,
    freq: str = "h",
) -> dlt.sources.DltResource:
```
```

---

### [CRITICAL] No error handling for pipeline.run()

**File**: `src/quant_alpha/ingestion/dlt_energy.py:93`
**Category**: Potential Bug
**Confidence**: 80%

The pipeline.run(source) call can fail due to network issues, permission errors, or data validation failures. This exception would propagate unhandled, potentially leaving the pipeline in an inconsistent state.

**Suggestion**:
```
Add error handling and consider logging:
```python
try:
    load_info = pipeline.run(source)
except Exception as e:
    logger.error(f"Pipeline run failed: {e}")
    raise
```
```

---

### [CRITICAL] Hardcoded path traversal in __main__

**File**: `src/quant_alpha/ingestion/dlt_energy.py:106-110`
**Category**: Architecture
**Confidence**: 75%

The __main__ block uses a relative path traversal (parents[3]) to locate the database file, which is fragile and depends on the script's exact location in the directory structure.

**Suggestion**:
```
Use environment variables or configuration for the database path:
```python
if __name__ == "__main__":
    db_path = os.environ.get("ENERGY_DB_PATH", "data/warehouse/second_foundation.duckdb")
    db = Path(db_path)
    info = run_dlt_energy_pipeline(db)
```
```

---

### [CRITICAL] Missing null check for cfg.entsoe attribute

**File**: `src/quant_alpha/pipeline_energy.py:42`
**Category**: Potential Bug
**Confidence**: 85%

When data_source is 'entsoe', the code accesses cfg.entsoe.token_env, cfg.entsoe.base_url, and cfg.entsoe.timeout_seconds without checking if cfg.entsoe exists or is None, which could raise AttributeError.

**Suggestion**:
```
Add validation before accessing cfg.entsoe attributes:
if not hasattr(cfg, 'entsoe') or cfg.entsoe is None:
    raise ValueError('entsoe configuration required when data_source is entsoe')
```

---

### [CRITICAL] Command injection via subprocess with user-controlled run_file

**File**: `src/quant_alpha/platform/bruin_graph.py:193-206`
**Category**: Security
**Confidence**: 95%

The `_execute_node()` method executes `subprocess.run()` with `node.run_file` which comes from user-controlled YAML configuration files. An attacker could craft a malicious YAML file with arbitrary commands in the `run.file` field, leading to command injection.

**Suggestion**:
```
Validate the run_file path to ensure it's within the expected directory and doesn't contain malicious patterns:
```python
def _execute_node(self, node: AssetNode, env: dict[str, str]) -> None:
    if node.run_file:
        run_path = Path(node.run_file).resolve()
        # Ensure the file is within the project directory
        if not str(run_path).startswith(str(self.root.resolve())):
            raise RuntimeError(f"Run file outside project directory: {node.run_file}")
        if not run_path.exists():
            raise RuntimeError(f"Run file not found: {node.run_file}")
        result = subprocess.run(
            [sys.executable, str(run_path)],
            ...
        )
```
```

---

### [CRITICAL] SQL injection via f-string interpolation

**File**: `src/quant_alpha/storage/duckdb.py:14`
**Category**: Security
**Confidence**: 95%

The table_name parameter is directly interpolated into the SQL query using an f-string without any sanitization or validation. An attacker could inject arbitrary SQL by providing a malicious table_name like 'test; DROP TABLE other_table; --' or similar payloads.

**Suggestion**:
```
Use parameterized queries or validate/sanitize the table name:

```python
import re

def _validate_table_name(name: str) -> str:
    if not re.match(r'^[a-zA-Z_][a-zA-Z0-9_]*$', name):
        raise ValueError(f"Invalid table name: {name}")
    return name

def write_table(db_path: Path, table_name: str, frame: pd.DataFrame) -> None:
    table_name = _validate_table_name(table_name)
    db_path.parent.mkdir(parents=True, exist_ok=True)
    with duckdb.connect(str(db_path)) as con:
        con.register("_frame", frame)
        con.execute(f"create or replace table {table_name} as select * from _frame")
        con.unregister("_frame")
```
```

---

### [CRITICAL] Unsanitized table_name used in blob path

**File**: `src/quant_alpha/storage/gcp.py:45-46`
**Category**: Security
**Confidence**: 75%

The table_name from the frames dictionary keys is used directly in the blob_name path without sanitization. If table_name contains path traversal characters (e.g., '../') or special characters, it could cause unexpected behavior in GCS.

**Suggestion**:
```
Sanitize table_name before using it in paths:
```python
import re
safe_name = re.sub(r'[^a-zA-Z0-9_\-]', '_', table_name)
blob_name = f"{config.gcs_prefix.rstrip('/')}/{safe_name}/{safe_name}.parquet"
```
```

---

### [CRITICAL] Unsanitized table_name used in BigQuery table ID

**File**: `src/quant_alpha/storage/gcp.py:50-51`
**Category**: Security
**Confidence**: 80%

The table_name is used directly in the BigQuery table_id. A malicious or malformed table_name could potentially cause SQL injection-like issues in BigQuery or create unexpected table names.

**Suggestion**:
```
Validate table_name against a safe pattern before constructing the table_id:
```python
if not re.match(r'^[a-zA-Z_][a-zA-Z0-9_]*$', table_name):
    raise CloudExportError(f"Invalid table name: {table_name}")
```
```

---

### [CRITICAL] Resource leak on exception in consumer

**File**: `src/quant_alpha/streaming/redpanda_consumer.py:37-48`
**Category**: Potential Bug
**Confidence**: 95%

If an exception occurs during the while loop (e.g., schema parsing error in schemaless_reader, or any unexpected error), `consumer.close()` is never called, leaving the Kafka consumer connection open and potentially leaking resources.

**Suggestion**:
```
Use a try/finally block or context manager to ensure consumer cleanup:

try:
    while len(messages) < max_messages and empty_polls < max_empty_polls:
        # ...
finally:
    consumer.close()
```

---

### [CRITICAL] SQL injection via table name interpolation

**File**: `src/quant_alpha/streaming/redpanda_consumer.py:82`
**Category**: Security
**Confidence**: 85%

The `table` parameter is directly interpolated into SQL statements using f-strings. An attacker controlling the `table` parameter could inject arbitrary SQL (e.g., table name like 'x; DROP TABLE other; --'). While `table` defaults to a safe value, callers could pass malicious input.

**Suggestion**:
```
Validate the table name against a whitelist pattern or use DuckDB's identifier quoting. Example:

import re
if not re.match(r'^[a-zA-Z_][a-zA-Z0-9_]*$', table):
    raise ValueError(f'Invalid table name: {table}')

Or quote the identifier:
con.execute(f'CREATE TABLE IF NOT EXISTS "{table}" AS SELECT * FROM frame WHERE false')
```

---

### [CRITICAL] SQL injection in query_realtime_scores

**File**: `src/quant_alpha/streaming/risingwave/client.py:59`
**Category**: Security
**Confidence**: 99%

The 'market' parameter is directly interpolated into the SQL query using f-string formatting without any sanitization or parameterized query usage. An attacker could inject arbitrary SQL code via the market parameter.

**Suggestion**:
```
Use parameterized queries:
```python
def query_realtime_scores(
    conn: Any,
    market: str | None = None,
    limit: int = 100,
) -> pd.DataFrame:
    where = "WHERE market = %s" if market else ""
    sql = """
        SELECT *
        FROM mv_realtime_alpha_scores
        {where}
        ORDER BY timestamp DESC
        LIMIT %s
    """
    params = []
    if market:
        params.append(market)
    params.append(limit)
    with conn.cursor() as cur:
        cur.execute(sql, params)
        cols = [d[0] for d in cur.description]
        rows = cur.fetchall()
    return pd.DataFrame(rows, columns=cols)
```
```

---

### [CRITICAL] SQL injection in query_hourly_window

**File**: `src/quant_alpha/streaming/risingwave/client.py:80-81`
**Category**: Security
**Confidence**: 99%

Both 'market' and 'hours' parameters are directly interpolated into the SQL query using f-string formatting. The 'hours' parameter, while typed as int, could still be manipulated if passed from untrusted sources, and 'market' is a string that could contain SQL injection payloads.

**Suggestion**:
```
Use parameterized queries:
```python
def query_hourly_window(
    conn: Any,
    market: str | None = None,
    hours: int = 24,
) -> pd.DataFrame:
    where_parts = []
    params = []
    if market:
        where_parts.append("market = %s")
        params.append(market)
    where_parts.append("window_start >= NOW() - INTERVAL '%s hours'")
    params.append(hours)
    where = "WHERE " + " AND ".join(where_parts)
    sql = """
        SELECT *
        FROM mv_energy_hourly_window
        {where}
        ORDER BY window_start DESC
    """
    with conn.cursor() as cur:
        cur.execute(sql, params)
        cols = [d[0] for d in cur.description]
        rows = cur.fetchall()
    return pd.DataFrame(rows, columns=cols)
```
```

---

### [CRITICAL] NULLIF with zero may still produce NULL momentum_6h

**File**: `src/quant_alpha/streaming/risingwave/simulator.py:47`
**Category**: Potential Bug
**Confidence**: 70%

The `momentum_6h` calculation `s.spot_price / NULLIF(AVG(...), 0) - 1` will produce NULL when the 6-period rolling average is zero. While this is technically correct behavior, downstream code that uses `momentum_6h` in `PERCENT_RANK()` will treat NULLs differently than zeroes, potentially causing unexpected scarcity alert behavior (line 90).

**Suggestion**:
```
Consider using COALESCE to handle the NULL case explicitly, e.g., `COALESCE(s.spot_price / NULLIF(...), 0) - 1` or document that NULL momentum values are expected and handled downstream.
```

---

## ERROR Issues (13)

### [ERROR] Energy decay return calculation is incorrect

**File**: `src/quant_alpha/backtest/alpha_decay.py:62-68`
**Category**: Potential Bug
**Confidence**: 95%

The formula `(s.shift(-h) / s.abs().clip(lower=20.0)) - (s / s.abs().clip(lower=20.0))` does not compute a forward return. It calculates `(P_future/P_denom) - (P_current/P_denom)` which is `(P_future - P_current)/P_denom`, not `(P_future - P_current)/P_current`. The denominator should use the current price, not a clipped absolute value.

**Suggestion**:
```
For a proper forward return: `lambda s: (s.shift(-h) / s.abs().clip(lower=20.0)) - 1` or more carefully handle near-zero prices: `lambda s: (s.shift(-h) - s) / s.abs().clip(lower=20.0)`
```

---

### [ERROR] Global environment variable mutation

**File**: `src/quant_alpha/ingestion/dlt_energy.py:63`
**Category**: Architecture
**Confidence**: 85%

Setting DESTINATION__DUCKDB__CREDENTIALS as an environment variable mutates global process state, which can cause race conditions in multi-threaded or concurrent usage and side effects for other parts of the application.

**Suggestion**:
```
Consider passing credentials directly to the dlt pipeline configuration instead of mutating environment variables. If environment variable is required, document this side effect clearly and consider restoring original value afterwards.
```

---

### [ERROR] Missing error handling for fetch_prices failure

**File**: `src/quant_alpha/ingestion/dlt_equity.py:35-37`
**Category**: Potential Bug
**Confidence**: 75%

The call to fetch_prices() on line 47 can raise exceptions (network errors, invalid data, missing symbols). No try/except wraps this call, so any failure will crash the entire pipeline with an unhandled exception rather than providing a meaningful error message.

**Suggestion**:
```
Add error handling:
```python
try:
    prices = fetch_prices(cfg, universe, offline=offline)
except Exception as e:
    raise RuntimeError(f"Failed to fetch equity prices: {e}") from e
```
```

---

### [ERROR] Environment variable credential injection vulnerability

**File**: `src/quant_alpha/ingestion/dlt_equity.py:57`
**Category**: Security
**Confidence**: 85%

Setting DESTINATION__DUCKDB__CREDENTIALS via os.environ directly writes a file path as a credential. This is an environment side-effect that could leak sensitive connection strings if shared across processes and may conflict with actual credentials if the environment is pre-configured.

**Suggestion**:
```
Pass credentials directly to the pipeline configuration instead of polluting the global environment:
```python
pipeline = dlt.pipeline(
    pipeline_name="equity_alpha",
    destination=dlt.destinations.duckdb(duckdb_path),
    dataset_name=dataset_name,
)
```
Or at minimum, clean up after:
```python
os.environ.pop("DESTINATION__DUCKDB__CREDENTIALS", None)
```
```

---

### [ERROR] Global state mutation via os.environ

**File**: `src/quant_alpha/ingestion/dlt_equity.py:57`
**Category**: Potential Bug
**Confidence**: 80%

build_equity_pipeline mutates the global process environment. This is not thread-safe and can cause race conditions if multiple pipelines run concurrently, or cause unexpected behavior in test suites sharing the same process.

**Suggestion**:
```
Pass configuration via dlt pipeline parameters rather than environment variables, or document clearly that this function is not safe for concurrent use.
```

---

### [ERROR] No error handling for yf.download failures

**File**: `src/quant_alpha/ingestion/yahoo.py:94`
**Category**: Potential Bug
**Confidence**: 80%

The yf.download call has no try/except block. Network errors, rate limiting, or invalid ticker symbols could cause unhandled exceptions that bubble up with unhelpful error messages.

**Suggestion**:
```
Wrap the download call in a try/except:
```python
try:
    data = yf.download(...)
except Exception as e:
    raise RuntimeError(f"Failed to download prices from Yahoo Finance: {e}") from e
```
```

---

### [ERROR] No error handling in _write_parquet helper

**File**: `src/quant_alpha/pipeline.py:17-18`
**Category**: Potential Bug
**Confidence**: 70%

The _write_parquet function writes to disk without any exception handling. If the disk is full, permissions are denied, or the DataFrame is empty/corrupt, an unhandled exception will propagate up and potentially leave the pipeline in an inconsistent state (some files written, some not).

**Suggestion**:
```
Add try/except with logging, or let the caller handle it with appropriate cleanup:

```python
def _write_parquet(frame: pd.DataFrame, path: Path) -> Path:
    path.parent.mkdir(parents=True, exist_ok=True)
    frame.to_parquet(path, index=False)
    return path
```

At minimum, add a docstring noting that the caller should handle IOError/OSError.
```

---

### [ERROR] KeyError when querying unknown asset name

**File**: `src/quant_alpha/platform/bruin_graph.py:97-109`
**Category**: Potential Bug
**Confidence**: 95%

The `upstream()` method accesses `self.nodes[name].depends` without checking if `name` exists in `self.nodes`. If a non-existent asset name is passed, this will raise a KeyError.

**Suggestion**:
```
Add a check at the beginning of the method:
```python
def upstream(self, name: str, depth: int = 99) -> list[str]:
    if name not in self.nodes:
        return []
    visited: list[str] = []
    ...
```
```

---

### [ERROR] Missing timeout for subprocess execution

**File**: `src/quant_alpha/platform/bruin_graph.py:193-206`
**Category**: Potential Bug
**Confidence**: 90%

The `subprocess.run()` call in `_execute_node()` has no timeout parameter. If a run script hangs or enters an infinite loop, the entire pipeline will hang indefinitely.

**Suggestion**:
```
Add a timeout parameter:
```python
result = subprocess.run(
    [sys.executable, node.run_file],
    env=env,
    capture_output=True,
    text=True,
    timeout=300,  # 5 minute timeout
)
```
And handle `subprocess.TimeoutExpired` exception.
```

---

### [ERROR] No error handling for BigQuery load job failures

**File**: `src/quant_alpha/storage/gcp.py:47-55`
**Category**: Potential Bug
**Confidence**: 95%

load_job.result() can raise google.api_core.exceptions exceptions on failure (quota exceeded, schema mismatch, etc.). This would cause an unhandled exception that doesn't clean up the GCS blob.

**Suggestion**:
```
Wrap the load job in a try-except to handle failures gracefully and optionally clean up the GCS blob:
```python
try:
    load_job.result()
except Exception as exc:
    # Optionally delete the uploaded blob
    blob.delete()
    raise CloudExportError(
        f"BigQuery load failed for table {table_name}: {exc}"
    ) from exc
```
```

---

### [ERROR] Missing error handling for file I/O in seed_demo_signals

**File**: `src/quant_alpha/streaming/demo_signals.py:38`
**Category**: Potential Bug
**Confidence**: 70%

The `write_table` function is called without any error handling. If the DuckDB path is invalid, the directory doesn't exist, or there are permission issues, the function will raise an unhandled exception. The caller in `__main__` also lacks error handling.

**Suggestion**:
```
Add error handling or at minimum document the exceptions that can be raised:
```python
def seed_demo_signals(duckdb_path: Path, n_hours: int = 48) -> int:
    """Write synthetic live signals into DuckDB for dashboard demo.
    
    Raises:
        FileNotFoundError: If parent directory doesn't exist.
        PermissionError: If database file isn't writable.
    """
```
```

---

### [ERROR] Missing error handling for Kafka producer failures

**File**: `src/quant_alpha/streaming/redpanda_producer.py:26-44`
**Category**: Potential Bug
**Confidence**: 95%

The publish_energy_signals function doesn't handle exceptions from Kafka producer initialization, produce(), or flush(). Network issues, broker unavailability, or serialization errors will cause unhandled exceptions.

**Suggestion**:
```
Add error handling:
```python
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
    except KafkaException as e:
        raise RuntimeError(f"Kafka error: {e}") from e
    except Exception as e:
        raise RuntimeError(f"Failed to publish signals: {e}") from e
```
```

---

### [ERROR] SQL injection risk in query_scarcity_alerts

**File**: `src/quant_alpha/streaming/risingwave/client.py:93-94`
**Category**: Security
**Confidence**: 85%

The 'level' parameter is used to construct SQL IN clause values. While currently using a whitelist dict, the f-string construction of the IN list could be dangerous if the whitelist approach is modified. Also, if level.upper() doesn't match any key, it falls back to ['HIGH'] but the variable name suggests it should handle invalid input.

**Suggestion**:
```
Use parameterized queries for the IN clause:
```python
def query_scarcity_alerts(conn: Any, level: str = "HIGH") -> pd.DataFrame:
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
```
```

---

## WARNING Issues (138)

### [WARNING] Missing column validation before access

**File**: `src/quant_alpha/backtest/alpha_decay.py:31-36`
**Category**: Potential Bug
**Confidence**: 85%

The function checks if `price_col` exists but not if `alpha_col`, `date_col`, or `symbol_col` exist in the panel. A KeyError will be raised if any of these columns are missing.

**Suggestion**:
```
Add validation: `required = [date_col, symbol_col, alpha_col]; missing = [c for c in required if c not in panel.columns]; if missing: return np.nan`
```

---

### [WARNING] Missing column validation for energy decay

**File**: `src/quant_alpha/backtest/alpha_decay.py:57-58`
**Category**: Potential Bug
**Confidence**: 85%

The function `compute_energy_alpha_decay` hardcodes column names `timestamp`, `market`, `spot_price` without checking if they exist in the panel. This will raise a KeyError if the panel has a different schema.

**Suggestion**:
```
Add validation at the start of the function: `required = ['timestamp', 'market', 'spot_price']; missing = [c for c in required if c not in panel.columns]; if missing: raise ValueError(f'Missing columns: {missing}')`
```

---

### [WARNING] Unused parameter cfg in walk_forward_ic

**File**: `src/quant_alpha/backtest/alpha_decay.py:80-84`
**Category**: Potential Bug
**Confidence**: 90%

The `cfg: BacktestConfig` parameter is accepted but never used within the function body. This suggests either dead code or a missing implementation that should use config values.

**Suggestion**:
```
Either remove the parameter if unused, or use it to derive `is_days`, `oos_days`, or `step_days` from the config. If removing: `def walk_forward_ic(panel, alpha_col, is_days=252, oos_days=63, step_days=63, date_col='date'):`
```

---

### [WARNING] Redundant date sorting on every call

**File**: `src/quant_alpha/backtest/alpha_decay.py:81`
**Category**: Performance
**Confidence**: 70%

The line `dates = sorted(pd.to_datetime(panel[date_col].dropna().unique()))` creates a full copy of unique dates and sorts them every time the function is called. For large panels this is expensive and could be cached or done once.

**Suggestion**:
```
Consider extracting date sorting into a helper or documenting that the caller should pre-sort. Alternatively, verify the panel is already sorted to skip this step.
```

---

### [WARNING] Repeated pd.to_datetime conversion in loop

**File**: `src/quant_alpha/backtest/alpha_decay.py:93-96`
**Category**: Performance
**Confidence**: 90%

Inside the while loop, `pd.to_datetime(panel[date_col])` is called twice per iteration (lines 95-96) to filter the OOS window. This conversion should be done once before the loop.

**Suggestion**:
```
Move conversion before the loop: `panel_dates = pd.to_datetime(panel[date_col]); dates_sorted = sorted(panel_dates.dropna().unique())` then use `panel_dates` in the filter.
```

---

### [WARNING] Missing forward_return column in OOS filter

**File**: `src/quant_alpha/backtest/alpha_decay.py:97-100`
**Category**: Potential Bug
**Confidence**: 85%

The `_daily_rank_ic` function expects a `forward_return` column in the panel, but `walk_forward_ic` does not verify this column exists before passing the filtered OOS data. If `forward_return` is missing, a KeyError will be raised deep inside `_daily_rank_ic`.

**Suggestion**:
```
Add validation at the start of `walk_forward_ic`: `if 'forward_return' not in panel.columns: raise ValueError('panel must contain forward_return column')`
```

---

### [WARNING] Potential NaN propagation in IC series std

**File**: `src/quant_alpha/backtest/alpha_decay.py:103-104`
**Category**: Potential Bug
**Confidence**: 75%

When `ic_series` is empty, the code checks `not ic_series.empty` but `std = ic_series.std(ddof=0)` is computed before that check. If `ic_series` is empty, `std` will be `NaN`, and the `std > 0` condition will be `False` (NaN comparison), so this works accidentally but is fragile.

**Suggestion**:
```
Move std computation inside the conditional: `ic_mean = float(ic_series.mean()) if not ic_series.empty else np.nan; ic_ir = float(ic_series.mean() / ic_series.std(ddof=0)) if not ic_series.empty and ic_series.std(ddof=0) > 0 else np.nan`
```

---

### [WARNING] split_is_oos repeated datetime parsing

**File**: `src/quant_alpha/backtest/diagnostics.py:10-17`
**Category**: Potential Bug
**Confidence**: 85%

Line 11 parses `panel['date']` with `pd.to_datetime()` to compute the split. Then lines 15-16 parse `panel['date']` again with `pd.to_datetime()` for filtering. This is redundant and could cause inconsistent parsing if date formats vary across rows.

**Suggestion**:
```
Parse dates once and reuse:
```python
def split_is_oos(panel: pd.DataFrame, split_date: str | None = None) -> tuple[pd.DataFrame, pd.DataFrame]:
    dates = pd.to_datetime(panel['date'])
    ordered_dates = pd.Series(sorted(dates.dropna().unique()))
    if ordered_dates.empty:
        return panel.iloc[0:0].copy(), panel.iloc[0:0].copy()
    split = pd.Timestamp(split_date) if split_date else ordered_dates.iloc[int(len(ordered_dates) * 0.7)]
    is_panel = panel[dates <= split].copy()
    oos_panel = panel[dates > split].copy()
    return is_panel, oos_panel
```
```

---

### [WARNING] groupby apply with include_groups may fail

**File**: `src/quant_alpha/backtest/diagnostics.py:28`
**Category**: Potential Bug
**Confidence**: 60%

The `include_groups=False` parameter is only available in pandas >= 2.2.0. If the codebase targets older pandas versions, this will raise a TypeError at runtime. Additionally, `apply` with a function that returns a scalar may behave differently across pandas versions.

**Suggestion**:
```
Add a comment documenting the minimum pandas version requirement, or use a more compatible approach:
```python
return panel.groupby('date')[alpha_col].apply(
    lambda day: day.rank().corr(day.rank()) if len(day.dropna()) >= 3 else np.nan
)
```
```

---

### [WARNING] Inefficient groupby apply for rank correlation

**File**: `src/quant_alpha/backtest/diagnostics.py:28`
**Category**: Performance
**Confidence**: 70%

Using `groupby().apply()` with a Python-level function is slow for large panels. The function accesses `day[[alpha_col, 'forward_return']]` inside the lambda, creating overhead per group. Vectorized approaches using `groupby().rank()` followed by correlation would be significantly faster.

**Suggestion**:
```
Consider a vectorized approach:
```python
def daily_rank_ic(panel: pd.DataFrame, alpha_col: str) -> pd.Series:
    ranked = panel.groupby('date')[[alpha_col, 'forward_return']].rank()
    return ranked.groupby(panel['date']).apply(
        lambda x: x[alpha_col].corr(x['forward_return'])
    ).dropna()
```
```

---

### [WARNING] Missing error handling for backtest call

**File**: `src/quant_alpha/backtest/diagnostics.py:52-54`
**Category**: Potential Bug
**Confidence**: 70%

`run_long_short_backtest` may raise exceptions (e.g., missing columns, empty data, configuration errors). There is no try/except wrapper, so a single failing alpha will crash the entire `evaluate_alpha_suite` run.

**Suggestion**:
```
Wrap the backtest call in a try/except to allow processing remaining alphas:
```python
try:
    _, metrics = run_long_short_backtest(panel, cfg, alpha_col=alpha_col)
except Exception as e:
    logging.warning(f'Backtest failed for {alpha_col}: {e}')
    return {f"{prefix}_{k}": np.nan for k in expected_metric_keys}
```
```

---

### [WARNING] Arithmetic on NaN values in same_sign check

**File**: `src/quant_alpha/backtest/diagnostics.py:82`
**Category**: Potential Bug
**Confidence**: 80%

Line 82-84 uses `np.sign()` on potentially NaN values from `row.get()`. `np.sign(np.nan)` returns NaN, and `NaN == NaN` is False. This means if either IS or OOS IC mean is NaN, `is_oos_ic_same_sign` will always be False, which may not be the intended behavior (should arguably be NaN or None to indicate 'unknown').

**Suggestion**:
```
Handle NaN explicitly:
```python
is_ic = row.get('is_ic_mean', np.nan)
oos_ic = row.get('oos_ic_mean', np.nan)
if np.isnan(is_ic) or np.isnan(oos_ic):
    row['is_oos_ic_same_sign'] = None  # or np.nan
else:
    row['is_oos_ic_same_sign'] = bool(np.sign(is_ic) == np.sign(oos_ic))
```
```

---

### [WARNING] evaluate_alpha_suite runs backtest 3 times per alpha

**File**: `src/quant_alpha/backtest/diagnostics.py:89-90`
**Category**: Performance
**Confidence**: 80%

For each alpha, the function runs `run_long_short_backtest` on the full panel (line 90), then `_backtest_metrics` runs it on IS panel (line 96) and OOS panel (line 97). This triples the computational cost. Each call likely involves portfolio construction and P&L calculation.

**Suggestion**:
```
Cache backtest results or refactor to compute metrics from a single full-panel backtest, splitting results by date afterward:
```python
full_daily, full_metrics = run_long_short_backtest(panel, cfg, alpha_col=alpha_col)
is_daily = full_daily[full_daily['date'] <= split]
oos_daily = full_daily[full_daily['date'] > split]
# compute metrics from is_daily and oos_daily
```
```

---

### [WARNING] select_consistent_alphas fallback is arbitrary

**File**: `src/quant_alpha/backtest/diagnostics.py:115`
**Category**: Potential Bug
**Confidence**: 70%

When no alphas pass the consistency threshold, the function falls back to `alpha_cols[:2]`. This silently selects the first two alphas regardless of their quality. If `alpha_cols` has fewer than 2 elements, this may return fewer than expected, and the selection has no quality guarantee.

**Suggestion**:
```
Add a warning log and make the fallback behavior explicit:
```python
if not usable:
    logging.warning('No alphas passed consistency threshold, falling back to first 2')
    usable = alpha_cols[:2]
```
```

---

### [WARNING] build_orthogonal_composite missing column errors

**File**: `src/quant_alpha/backtest/diagnostics.py:123-125`
**Category**: Potential Bug
**Confidence**: 70%

The function accesses `panel.groupby('date')[col]` for each column in `usable`. If any column doesn't exist in the panel DataFrame, this will raise a KeyError. There's no validation that the columns exist in the panel.

**Suggestion**:
```
Add column existence check:
```python
missing = [c for c in usable if c not in panel.columns]
if missing:
    raise ValueError(f'Missing columns in panel: {missing}')
```
```

---

### [WARNING] value_added_report uses full panel for composite then splits

**File**: `src/quant_alpha/backtest/diagnostics.py:138-142`
**Category**: Potential Bug
**Confidence**: 85%

Line 138 calls `split_is_oos(scored)` without a split_date, so it uses the default 70/30 split. But the composite score was computed on the FULL panel (including OOS data), which causes lookahead bias. The composite is already contaminated with OOS information before the split occurs.

**Suggestion**:
```
This is a significant methodological issue. The composite should be built only on IS data:
```python
is_panel, oos_panel = split_is_oos(scored)
is_composite = build_orthogonal_composite(is_panel, diagnostics, alpha_cols)
oos_scored = oos_panel.copy()
oos_scored[composite_col] = build_orthogonal_composite(oos_scored, diagnostics, alpha_cols)
```
```

---

### [WARNING] Magic numbers in scoring functions

**File**: `src/quant_alpha/backtest/diagnostics.py:155`
**Category**: Readability
**Confidence**: 80%

Lines 155-158 and 163-168 use hardcoded magic numbers (0.6, 0.4, 252, 0.1, etc.) without explanation. These are domain-specific thresholds that should be documented or extracted as named constants.

**Suggestion**:
```
Extract constants:
```python
_TRADING_DAYS_PER_YEAR = 252
_MIN_IR_THRESHOLD = 0.1
_CONSISTENCY_SIGN_WEIGHT = 0.6
_CONSISTENCY_MAGNITUDE_WEIGHT = 0.4
```
```

---

### [WARNING] Missing validation of alpha_col existence

**File**: `src/quant_alpha/backtest/long_short.py:11`
**Category**: Potential Bug
**Confidence**: 80%

The function does not validate that alpha_col exists in the panel DataFrame. If the column is missing, pandas will raise a KeyError without a clear error message.

**Suggestion**:
```
Add validation at the start of the function:
```python
if alpha_col not in panel.columns:
    raise ValueError(f"Alpha column '{alpha_col}' not found in panel. Available: {list(panel.columns)}")
```
```

---

### [WARNING] Missing validation of required columns

**File**: `src/quant_alpha/backtest/long_short.py:11`
**Category**: Potential Bug
**Confidence**: 80%

The function assumes 'forward_return', 'date', and 'symbol' columns exist in the panel DataFrame but never validates their presence. This can lead to confusing error messages downstream.

**Suggestion**:
```
Add validation at the start:
```python
required_cols = {alpha_col, "forward_return", "date", "symbol"}
missing = required_cols - set(panel.columns)
if missing:
    raise ValueError(f"Missing required columns: {missing}")
```
```

---

### [WARNING] Overlapping long and short positions possible

**File**: `src/quant_alpha/backtest/long_short.py:13-14`
**Category**: Potential Bug
**Confidence**: 75%

When there are many tied values at the quantile boundaries, `longs` (>= top_cut) and `shorts` (<= bottom_cut) can include the same symbols if top_cut <= bottom_cut. This creates contradictory long AND short weights for the same symbol on the same date.

**Suggestion**:
```
Add an assertion or check to ensure no overlap:
```python
overlap = set(longs["symbol"]) & set(shorts["symbol"])
if overlap:
    # Either skip this date or resolve the overlap
    longs = longs[~longs["symbol"].isin(overlap)]
    shorts = shorts[~shorts["symbol"].isin(overlap)]
    if longs.empty or shorts.empty:
        continue
```
```

---

### [WARNING] Potential division by zero in weight calculation

**File**: `src/quant_alpha/backtest/long_short.py:14`
**Category**: Potential Bug
**Confidence**: 60%

Although the code checks `if longs.empty or shorts.empty`, if `len(longs)` or `len(shorts)` somehow returns 0 after the check (edge case with corrupted data), division by zero could occur. Additionally, there's no validation that cfg.top_quantile and cfg.bottom_quantile are valid values (e.g., between 0 and 1).

**Suggestion**:
```
Add defensive validation:
```python
if len(longs) == 0 or len(shorts) == 0:
    continue
longs["weight"] = 0.5 / len(longs)
shorts["weight"] = -0.5 / len(shorts)
```
```

---

### [WARNING] Turnover calculation assumes contiguous dates

**File**: `src/quant_alpha/backtest/long_short.py:45`
**Category**: Potential Bug
**Confidence**: 60%

The `wide_weights.diff()` call computes the difference between consecutive rows in the pivot table. If there are gaps in dates (e.g., weekends/holidays removed), the first day after a gap will be compared to the last day before the gap, which may overstate or understate actual turnover.

**Suggestion**:
```
Consider sorting the index explicitly and/or documenting this assumption:
```python
wide_weights = wide_weights.sort_index()
# Note: diff() will correctly handle non-contiguous dates,
# but turnover between non-consecutive trading days may need special handling
```
```

---

### [WARNING] Index misalignment between gross, cost, and counts DataFrames

**File**: `src/quant_alpha/backtest/long_short.py:56`
**Category**: Potential Bug
**Confidence**: 70%

The `gross` DataFrame is indexed by date, but `cost` and `counts` are derived from different pivot operations. If there are dates present in `cost` or `counts` but not in `gross` (or vice versa), the assignment `daily["transaction_cost"] = cost` will silently create NaN values or misalign data.

**Suggestion**:
```
Ensure index alignment by using `.reindex(daily.index)`:
```python
daily["transaction_cost"] = cost.reindex(daily.index, fill_value=0)
daily["long_count"] = counts.get("long", 0).reindex(daily.index, fill_value=0)
daily["short_count"] = counts.get("short", 0).reindex(daily.index, fill_value=0)
```
```

---

### [WARNING] Max drawdown could be zero causing division error

**File**: `src/quant_alpha/backtest/long_short.py:66`
**Category**: Potential Bug
**Confidence**: 60%

The Calmar ratio uses `abs(max_dd)` with a check against `1e-9`. However, if `max_dd` is exactly 0 (which happens when equity curve is monotonically increasing), the ratio is set to 0.0, which is correct but the message is ambiguous — a zero max drawdown with positive return should arguably yield infinity or a very large number, not zero.

**Suggestion**:
```
Consider documenting this edge case behavior or returning a large sentinel value:
```python
calmar = float(ann_ret / abs(max_dd)) if abs(max_dd) > 1e-9 else (float('inf') if ann_ret > 0 else 0.0)
```
```

---

### [WARNING] Potential division by zero in annualized volatility

**File**: `src/quant_alpha/backtest/long_short.py:70`
**Category**: Potential Bug
**Confidence**: 65%

If `ann_vol` is exactly 0 (all returns are identical), the Sharpe ratio calculation could produce inf. The code handles this with a ternary, but `ann_vol` could be a very small number (not exactly 0) that produces an extremely large Sharpe value, which may be misleading.

**Suggestion**:
```
Consider using a small epsilon threshold instead of exact zero:
```python
"sharpe": float(ann_ret / ann_vol) if ann_vol > 1e-10 else 0.0,
```
```

---

### [WARNING] Missing module docstring

**File**: `src/quant_alpha/batch/spark_energy_features.py:1`
**Category**: Convention
**Confidence**: 90%

The module lacks a docstring explaining its purpose, usage, and assumptions. This makes it harder for other developers to understand what this module does.

**Suggestion**:
```
Add a module docstring:
```python
"""Spark-based energy market feature engineering.

This module computes rolling statistics and derived features
from energy market parquet data for use in quantitative analysis.
"""
```
```

---

### [WARNING] Missing function docstring for build_spark_session

**File**: `src/quant_alpha/batch/spark_energy_features.py:6-14`
**Category**: Convention
**Confidence**: 90%

The function lacks a docstring explaining its purpose, parameters, return value, and any configuration assumptions.

**Suggestion**:
```
Add function docstring:
```python
def build_spark_session(app_name: str = "second-foundation-energy-batch"):    """Create and configure a SparkSession.
    
    Args:
        app_name: Name for the Spark application.
        
    Returns:
        Configured SparkSession instance.
        
    Note:
        Uses local[*] master for development. Override via SPARK_MASTER env var.
    """
```
```

---

### [WARNING] Hardcoded Spark master configuration

**File**: `src/quant_alpha/batch/spark_energy_features.py:7`
**Category**: Potential Bug
**Confidence**: 75%

The Spark session is hardcoded to use 'local[*]' as the master. This is appropriate for development/testing but will cause issues in production environments where a cluster manager (YARN, Mesos, Kubernetes) should be used. The master configuration should be parameterized or read from environment variables.

**Suggestion**:
```
Make the master configurable:
```python
def build_spark_session(app_name: str = "second-foundation-energy-batch", master: str = None):
    from pyspark.sql import SparkSession
    
    builder = SparkSession.builder.appName(app_name)
    if master:
        builder = builder.master(master)
    else:
        import os
        builder = builder.master(os.environ.get("SPARK_MASTER", "local[*]"))
    return (
        builder
        .config("spark.sql.session.timeZone", "UTC")
        .getOrCreate()
    )
```
```

---

### [WARNING] Missing function docstring for compute_energy_features

**File**: `src/quant_alpha/batch/spark_energy_features.py:16-48`
**Category**: Convention
**Confidence**: 90%

The function lacks a docstring explaining its purpose, parameters, and the features it computes.

**Suggestion**:
```
Add function docstring:
```python
def compute_energy_features(input_path: str, output_path: str) -> None:
    """Compute energy market features from raw parquet data.
    
    Features computed:
        - spot_return_1h: Log return of spot price
        - rolling_spot_mean_24h: 24-hour rolling mean of spot price
        - rolling_spot_std_24h: 24-hour rolling standard deviation
        - rolling_residual_mean_168h: 168-hour rolling mean of residual load
        - residual_load_shock: Deviation from 168h mean residual load
        - imbalance_premium: Imbalance price minus spot price
        - scarcity_flag: Binary flag for high residual load shock
        
    Args:
        input_path: Path to input parquet file.
        output_path: Path for output parquet file.
        
    Raises:
        ValueError: If required columns are missing from input.
    """
```
```

---

### [WARNING] Missing validation of window partition columns

**File**: `src/quant_alpha/batch/spark_energy_features.py:20-23`
**Category**: Potential Bug
**Confidence**: 80%

The code assumes columns 'market' and 'timestamp' exist in the input parquet file. If these columns are missing, the Spark job will fail at runtime with a cryptic error. Input validation should be performed.

**Suggestion**:
```
Add schema validation:
```python
def compute_energy_features(input_path: str, output_path: str) -> None:
    from pyspark.sql import Window, functions as F
    
    spark = build_spark_session()
    try:
        frame = spark.read.parquet(input_path)
        
        # Validate required columns
        required_columns = {'market', 'timestamp', 'spot_price', 'residual_load', 'imbalance_price'}
        missing_columns = required_columns - set(frame.columns)
        if missing_columns:
            raise ValueError(f"Missing required columns: {missing_columns}")
        
        # ... rest of code ...
```
```

---

### [WARNING] Division by zero risk in log calculation

**File**: `src/quant_alpha/batch/spark_energy_features.py:26`
**Category**: Potential Bug
**Confidence**: 85%

The line 'F.log("spot_price") - F.log(F.lag("spot_price").over(w_market))' computes log returns. If spot_price is zero or negative, F.log will return null or -infinity, which could cause downstream issues. There's no validation that spot_price values are positive.

**Suggestion**:
```
Add filtering or null handling:
```python
.withColumn("spot_return_1h", 
    F.when((F.col("spot_price") > 0) & (F.lag("spot_price").over(w_market) > 0),
           F.log("spot_price") - F.log(F.lag("spot_price").over(w_market)))
    .otherwise(F.lit(None))
)
```
```

---

### [WARNING] Hardcoded magic number for scarcity threshold

**File**: `src/quant_alpha/batch/spark_energy_features.py:35`
**Category**: Readability
**Confidence**: 95%

The value '5' in the condition 'F.col("residual_load_shock") > 5' is a hardcoded magic number without explanation. This threshold should be configurable and documented to indicate what unit it represents.

**Suggestion**:
```
Extract to a named constant:
```python
SCARCITY_THRESHOLD_GW = 5  # Residual load shock threshold in GW

# Later in code:
.withColumn(
    "scarcity_flag",
    F.when(F.col("residual_load_shock") > SCARCITY_THRESHOLD_GW, F.lit(1)).otherwise(F.lit(0)),
)
```
```

---

### [WARNING] Missing null handling for residual_load_shock

**File**: `src/quant_alpha/batch/spark_energy_features.py:38`
**Category**: Potential Bug
**Confidence**: 75%

The scarcity_flag condition assumes residual_load_shock is never null. If any of the input columns (residual_load or the rolling mean) have nulls, the condition will propagate nulls, potentially causing unexpected behavior.

**Suggestion**:
```
Add null handling:
```python
.withColumn(
    "scarcity_flag",
    F.when(F.col("residual_load_shock").isNull(), F.lit(0))
     .when(F.col("residual_load_shock") > SCARCITY_THRESHOLD_GW, F.lit(1))
     .otherwise(F.lit(0)),
)
```
```

---

### [WARNING] Missing error handling in _run

**File**: `src/quant_alpha/cli.py:19-23`
**Category**: Potential Bug
**Confidence**: 80%

The `_run` function has no error handling at all. If `run_pipeline` raises an exception, the user sees an unhandled traceback instead of a clean error message. Compare with `_run_energy` which at least catches `EntsoeError`.

**Suggestion**:
```
Add try/except for expected errors:
```python
def _run(config: Path, root: Path, offline: bool) -> None:
    try:
        result = run_pipeline(config, root.resolve(), offline=offline)
    except Exception as exc:
        typer.echo(f"Pipeline failed: {exc}", err=True)
        raise typer.Exit(code=1) from exc
    ...
```
```

---

### [WARNING] Missing error handling in _run_energy

**File**: `src/quant_alpha/cli.py:25-37`
**Category**: Potential Bug
**Confidence**: 75%

Only `EntsoeError` is caught in `_run_energy`. Other exceptions (e.g., `FileNotFoundError` for config, `ConnectionError`, `KeyError` from result dict) will propagate as unhandled tracebacks, producing a poor CLI user experience.

**Suggestion**:
```
Consider adding a broader `except Exception` handler (after `EntsoeError`) or at least catching common operational errors:
```python
except EntsoeError as exc:
    typer.echo(f"ENTSO-E ingestion failed: {exc}", err=True)
    raise typer.Exit(code=1) from exc
except Exception as exc:
    typer.echo(f"Energy pipeline failed: {exc}", err=True)
    raise typer.Exit(code=1) from exc
```
```

---

### [WARNING] Hardcoded config path in dlt_energy_command

**File**: `src/quant_alpha/cli.py:88`
**Category**: Potential Bug
**Confidence**: 85%

The `dlt_energy_command` uses a hardcoded config path `Path("configs/second_foundation_project.yaml")` instead of accepting it as a CLI option like other commands do. This makes the command inflexible and inconsistent with other commands.

**Suggestion**:
```
Add a `config` option like other commands:
```python
def dlt_energy_command(
    config: Path = typer.Option(Path("configs/second_foundation_project.yaml"), help="Energy project config YAML."),
    root: Path = typer.Option(Path("."), help="Project root."),
    ...
) -> None:
    cfg = load_project_config(config, root=root.resolve())
```
```

---

### [WARNING] Missing error handling in dlt_energy_command

**File**: `src/quant_alpha/cli.py:94-99`
**Category**: Potential Bug
**Confidence**: 80%

The `dlt_energy_command` function has no try/except around `run_dlt_energy_pipeline`. Any exception will produce an unhandled traceback. This is inconsistent with `_run_energy` which handles `EntsoeError`.

**Suggestion**:
```
Add error handling consistent with other commands:
```python
try:
    info = run_dlt_energy_pipeline(...)
except Exception as exc:
    typer.echo(f"dlt energy pipeline failed: {exc}", err=True)
    raise typer.Exit(code=1) from exc
```
```

---

### [WARNING] Missing error handling in dlt_equity_command

**File**: `src/quant_alpha/cli.py:106-115`
**Category**: Potential Bug
**Confidence**: 80%

The `dlt_equity_command` function has no try/except around `run_dlt_equity_pipeline`. Any exception will produce an unhandled traceback.

**Suggestion**:
```
Add error handling:
```python
try:
    info = run_dlt_equity_pipeline(...)
except Exception as exc:
    typer.echo(f"dlt equity pipeline failed: {exc}", err=True)
    raise typer.Exit(code=1) from exc
```
```

---

### [WARNING] No file existence check for bruin_root directory

**File**: `src/quant_alpha/cli.py:117-128`
**Category**: Potential Bug
**Confidence**: 75%

The `bruin_lineage_command` passes `bruin_root.resolve()` directly to `AssetGraph` without checking if the directory exists. If the path doesn't exist, the error will be an opaque exception from `AssetGraph` rather than a clear CLI message.

**Suggestion**:
```
Add directory existence check:
```python
resolved = bruin_root.resolve()
if not resolved.is_dir():
    typer.echo(f"Bruin directory not found: {resolved}", err=True)
    raise typer.Exit(1)
graph = AssetGraph(resolved)
```
```

---

### [WARNING] Missing error handling in bruin_run_command

**File**: `src/quant_alpha/cli.py:134-161`
**Category**: Potential Bug
**Confidence**: 75%

The `bruin_run_command` calls `graph.run()` which could raise exceptions (e.g., subprocess failures, file I/O errors). These are not caught and will produce unhandled tracebacks.

**Suggestion**:
```
Add try/except around graph operations:
```python
try:
    results = graph.run(targets=target_list, env=env, dry_run=dry_run)
except Exception as exc:
    typer.echo(f"Bruin run failed: {exc}", err=True)
    raise typer.Exit(code=1) from exc
```
```

---

### [WARNING] Weak date validation in config model

**File**: `src/quant_alpha/config.py:55-57`
**Category**: Potential Bug
**Confidence**: 85%

start_date and end_date are plain strings without validation, allowing invalid date formats like '2021-13-45' or non-date strings to be accepted. This can cause runtime failures downstream when dates are parsed.

**Suggestion**:
```
Use a custom validator or a more constrained type:
```python
from datetime import date
from pydantic import field_validator

class ProjectConfig(BaseModel):
    start_date: date = date(2021, 1, 1)
    end_date: date | None = None
    
    @field_validator('start_date', 'end_date', mode='before')
    @classmethod
    def parse_date(cls, v: str | date | None) -> date | None:
        if isinstance(v, str):
            return date.fromisoformat(v)
        return v
```
```

---

### [WARNING] No error handling for invalid YAML

**File**: `src/quant_alpha/config.py:81`
**Category**: Potential Bug
**Confidence**: 80%

yaml.safe_load can raise yaml.YAMLError on malformed YAML. load_project_config and load_universe do not catch this exception, leading to unhandled tracebacks for invalid config files.

**Suggestion**:
```
Add error handling or document that callers must handle YAML errors:
```python
def load_yaml(path: Path) -> dict[str, Any]:
    try:
        with path.open('r', encoding='utf-8') as f:
            return yaml.safe_load(f) or {}
    except yaml.YAMLError as e:
        raise ValueError(f'Invalid YAML in {path}: {e}') from e
```
```

---

### [WARNING] No handling for missing config files

**File**: `src/quant_alpha/config.py:81`
**Category**: Potential Bug
**Confidence**: 75%

path.open() will raise FileNotFoundError if the config file doesn't exist. Neither load_project_config nor load_universe provide a helpful error message.

**Suggestion**:
```
Add a check or wrap with a clearer error:
```python
def load_yaml(path: Path) -> dict[str, Any]:
    if not path.exists():
        raise FileNotFoundError(f'Config file not found: {path}')
    with path.open('r', encoding='utf-8') as f:
        return yaml.safe_load(f) or {}
```
```

---

### [WARNING] Module docstring missing module description

**File**: `src/quant_alpha/features/__init__.py:1`
**Category**: Convention
**Confidence**: 80%

The module docstring 'Alpha factor calculations.' is minimal and doesn't provide sufficient context about what specific alpha factors this module contains or how to use them.

**Suggestion**:
```
Expand the docstring to include a brief overview of the alpha factors provided and usage examples: """Alpha factor calculations for quantitative strategies.

This module provides various alpha factor calculations for quantitative trading strategies.
"""
```

---

### [WARNING] Module-level side effect during import

**File**: `src/quant_alpha/features/alpha_factors.py:10`
**Category**: Potential Bug
**Confidence**: 70%

BASE_FACTOR_COLUMNS is computed at module import time by calling make_equity_alpha_registry(). This creates a side effect during import, which can cause issues if the registry isn't ready or if the module is imported before configuration is complete. It also means the list is computed once and never refreshed if the registry changes.

**Suggestion**:
```
Make it lazy-evaluated with a function:
```python
def get_base_factor_columns() -> list[str]:
    return [alpha.name for alpha in make_equity_alpha_registry()]
```
```

---

### [WARNING] Division by zero in rolling z-score

**File**: `src/quant_alpha/features/alpha_factors.py:13-16`
**Category**: Potential Bug
**Confidence**: 65%

The replace(0, np.nan) on std only catches exact zero values. Due to floating-point precision, std values extremely close to zero (but not exactly zero) could still produce extreme/unstable z-scores. Also, periods with fewer than 2 observations will produce NaN std by default, which is handled, but the window parameter is not validated.

**Suggestion**:
```
Add a minimum std threshold to prevent near-zero division:
```python
def _rolling_zscore(series: pd.Series, window: int) -> pd.Series:
    mean = series.rolling(window).mean()
    std = series.rolling(window).std()
    std = std.replace(0, np.nan)
    std[std.abs() < 1e-10] = np.nan
    return (series - mean) / std
```
```

---

### [WARNING] Division by zero in breakout position

**File**: `src/quant_alpha/features/alpha_factors.py:19-22`
**Category**: Potential Bug
**Confidence**: 60%

Similar to _rolling_zscore, _breakout_position uses replace(0, np.nan) on width. If rolling_max equals rolling_min (flat price), width is zero. While replace catches exact zero, near-zero widths from floating-point arithmetic could still produce numerically unstable results.

**Suggestion**:
```
Add a minimum width threshold:
```python
width = (rolling_max - rolling_min).replace(0, np.nan)
width[width.abs() < 1e-10] = np.nan
```
```

---

### [WARNING] No validation of required DataFrame columns

**File**: `src/quant_alpha/features/alpha_factors.py:29`
**Category**: Potential Bug
**Confidence**: 85%

The add_alpha_factors function assumes the input DataFrame 'prices' contains 'symbol', 'date', 'adj_close', and 'close' columns. No validation is performed, which will produce cryptic KeyError exceptions if columns are missing.

**Suggestion**:
```
Add column validation at the start:
```python
def add_alpha_factors(prices: pd.DataFrame, cfg: ProjectConfig) -> pd.DataFrame:
    required = {'symbol', 'date', 'adj_close', 'close'}
    missing = required - set(prices.columns)
    if missing:
        raise ValueError(f"Missing required columns: {missing}")
    df = prices.copy()
```
```

---

### [WARNING] Missing forward_return values for last N rows per symbol

**File**: `src/quant_alpha/features/alpha_factors.py:46`
**Category**: Potential Bug
**Confidence**: 70%

The forward_return calculation uses shift(-forward_days), which will produce NaN for the last forward_days rows of each symbol. These NaN values will propagate to any model training and may cause silent issues if not filtered downstream.

**Suggestion**:
```
Document this behavior clearly and consider adding a warning or dropping these rows:
```python
df['forward_return'] = grouped['adj_close'].transform(
    lambda s: (s.shift(-forward_days) / s) - 1
)
# Note: Last forward_days rows per symbol will have NaN forward_return
```
```

---

### [WARNING] No null/empty check on alpha values before ranking

**File**: `src/quant_alpha/features/alpha_factors.py:55-59`
**Category**: Potential Bug
**Confidence**: 60%

If alpha.compute() returns NaN for some rows, the percentile rank calculation will still assign ranks. Depending on how pd.rank handles NaN (NaN is excluded from ranking), this could silently produce biased composite scores if many NaN values exist.

**Suggestion**:
```
Consider documenting or filtering NaN values before ranking:
```python
df[rank_col] = df.groupby('date')[col].rank(pct=True, na_option='keep')
```
```

---

### [WARNING] Composite alpha biased by NaN alpha factors

**File**: `src/quant_alpha/features/alpha_factors.py:61`
**Category**: Potential Bug
**Confidence**: 70%

df[ranked_cols].mean(axis=1) uses skipna=True by default. If some alpha factors produce NaN for certain rows, the composite will be computed from fewer factors for those rows, which can bias cross-sectional comparisons since different rows may use different numbers of factors.

**Suggestion**:
```
Explicitly handle NaN biasing, either by requiring all factors or documenting this behavior:
```python
df['alpha_composite'] = df[ranked_cols].mean(axis=1) - 0.5
# Or to require all factors:
# df['alpha_composite'] = df[ranked_cols].apply(
#     lambda row: row.mean() - 0.5 if row.notna().all() else np.nan, axis=1
# )
```
```

---

### [WARNING] Mutable default argument pattern in alpha_registry_frame

**File**: `src/quant_alpha/features/alpha_factors.py:65`
**Category**: Potential Bug
**Confidence**: 65%

The function uses 'registry = registry or make_equity_alpha_registry()' which is a common pattern but has a subtle issue: if registry is an empty list (which is falsy), it will be replaced by the default registry. This may not be the intended behavior.

**Suggestion**:
```
Use explicit None check:
```python
def alpha_registry_frame(registry: list[AlphaDefinition] | None = None) -> pd.DataFrame:
    if registry is None:
        registry = make_equity_alpha_registry()
```
```

---

### [WARNING] Division by zero in _zscore function

**File**: `src/quant_alpha/features/energy_alpha.py:90-93`
**Category**: Potential Bug
**Confidence**: 75%

The _zscore function replaces zeros in the standard deviation with pd.NA using std.replace(0, pd.NA), but if all values in the rolling window are identical, std will be 0.0. However, replace(0, pd.NA) only matches exact 0.0, but floating-point comparisons can be imprecise. More critically, if the series contains pd.NA values initially, the rolling operations may produce unexpected results.

**Suggestion**:
```
Add explicit handling for edge cases:
```python
def _zscore(series: pd.Series, window: int) -> pd.Series:
    mean = series.rolling(window).mean()
    std = series.rolling(window).std()
    # Replace zero std with NaN to avoid division by zero
    std = std.replace(0, np.nan)
    return (series - mean) / std
```
```

---

### [WARNING] Potential division by zero in solar penetration calculation

**File**: `src/quant_alpha/features/energy_alpha.py:110-113`
**Category**: Potential Bug
**Confidence**: 70%

Line 113 calculates _solar_pen as df['solar_forecast'] / df['load_forecast'].clip(lower=1.0). While clip(lower=1.0) prevents division by zero for positive values, if load_forecast contains NaN values after numeric coercion, this could propagate NaN unexpectedly. Additionally, if load_forecast is exactly 0 and not caught by clip, it would cause division by zero.

**Suggestion**:
```
Add explicit zero handling:
```python
df['_solar_pen'] = df['solar_forecast'] / df['load_forecast'].clip(lower=1.0).replace(0, np.nan)
```
```

---

### [WARNING] Missing NaN handling in momentum calculation

**File**: `src/quant_alpha/features/energy_alpha.py:130-133`
**Category**: Potential Bug
**Confidence**: 70%

The price momentum calculation (s / s.shift(6) - 1) doesn't handle NaN values in the input series. If spot_price contains NaN values, the calculation will propagate NaNs but may not handle them gracefully, potentially leading to unexpected results.

**Suggestion**:
```
Add explicit NaN handling:
```python
df['alpha_energy_price_momentum_6h'] = grouped['spot_price'].transform(
    lambda s: ((s / s.shift(6).replace(0, np.nan)) - 1).shift(1)
)
```
```

---

### [WARNING] Division by zero in price momentum calculation

**File**: `src/quant_alpha/features/energy_alpha.py:145-148`
**Category**: Potential Bug
**Confidence**: 80%

Line 146 calculates price momentum as (s / s.shift(6) - 1). If spot_price is 0 or contains zeros, this will cause division by zero. The spot price could be zero in certain market conditions or due to data errors.

**Suggestion**:
```
Add zero protection:
```python
df['alpha_energy_price_momentum_6h'] = grouped['spot_price'].transform(
    lambda s: ((s / s.shift(6).replace(0, np.nan)) - 1).shift(1)
)
```
```

---

### [WARNING] Missing handling for gas_price column in else branch

**File**: `src/quant_alpha/features/energy_alpha.py:156-159`
**Category**: Potential Bug
**Confidence**: 85%

When gas_price is not in the DataFrame, the code sets alpha_energy_gas_spark_spread to pd.NA for all rows. However, this creates a column with a single NA value rather than a Series of NAs matching the DataFrame length, which could cause issues in downstream processing.

**Suggestion**:
```
Create a proper Series of NAs:
```python
else:
    df['alpha_energy_gas_spark_spread'] = pd.Series([pd.NA] * len(df), index=df.index)
```
```

---

### [WARNING] ts_corr result alignment may be fragile

**File**: `src/quant_alpha/features/registry.py:39-42`
**Category**: Potential Bug
**Confidence**: 60%

The ts_corr function uses groupby.apply with group_keys=False. When groups have different lengths or when the index has duplicate values within a group, apply() can produce unexpected index alignment issues. The rolling().corr() on the concatenated frame may also have alignment issues if left and right have different NaN patterns.

**Suggestion**:
```
Add explicit handling for NaN alignment and consider using groupby().rolling() directly:
```python
def ts_corr(left: pd.Series, right: pd.Series, window: int) -> pd.Series:
    return left.groupby(level=1).rolling(window).corr(right.groupby(level=1).rolling(window))
```
```

---

### [WARNING] safe_divide can produce incorrect results for negative values

**File**: `src/quant_alpha/features/registry.py:55-58`
**Category**: Potential Bug
**Confidence**: 70%

The safe_divide function replaces 0 with np.nan then fills with eps. However, it also replaces legitimate negative zeros. More importantly, using replace(0, np.nan) will only match exact 0.0 values, not values very close to zero due to floating point precision, which could still cause division-by-zero or extreme values in pandas operations.

**Suggestion**:
```
Consider using np.where with a threshold:
```python
def safe_divide(left: pd.Series, right: pd.Series, eps: float = 1e-9) -> pd.Series:
    mask = right.abs() < eps
    return left / right.where(~mask, eps)
```
```

---

### [WARNING] Magic number 0.001 in intraday range calculation

**File**: `src/quant_alpha/features/registry.py:99`
**Category**: Potential Bug
**Confidence**: 80%

The expression uses `x["high"] - x["low"] + 0.001` to avoid division by zero, but the safe_divide function already handles zero denominators. Using both 0.001 addition AND safe_divide creates double protection with inconsistent eps values (0.001 vs 1e-9), which could mask genuine data quality issues where high equals low.

**Suggestion**:
```
Use only safe_divide for zero protection:
```python
compute=lambda x: cs_rank(safe_divide(x["close"] - x["open"], x["high"] - x["low"])),
```
Or remove safe_divide and rely on the 0.001 offset consistently.
```

---

### [WARNING] DataFrame iteration without error handling

**File**: `src/quant_alpha/ingestion/dlt_energy.py:52-57`
**Category**: Potential Bug
**Confidence**: 75%

The generate_synthetic_power_market call and subsequent DataFrame operations lack error handling. If the function returns an empty DataFrame, has unexpected columns, or fails, the error propagates unhandled.

**Suggestion**:
```
Add error handling for the data generation:
```python
frame = generate_synthetic_power_market(markets, start, end, freq=freq)
if frame.empty:
    return
if 'timestamp' not in frame.columns:
    raise ValueError("Generated data missing 'timestamp' column")
```
```

---

### [WARNING] Missing validation of duckdb_path

**File**: `src/quant_alpha/ingestion/dlt_energy.py:63`
**Category**: Potential Bug
**Confidence**: 80%

The duckdb_path is converted to string and set as a credential without validation. If the path doesn't exist or the directory isn't writable, the error will only surface later during pipeline.run(), making debugging harder.

**Suggestion**:
```
Add path validation before setting credentials:
```python
def build_energy_pipeline(duckdb_path: Path, dataset_name: str = "dlt_energy_raw") -> dlt.Pipeline:
    duckdb_path.parent.mkdir(parents=True, exist_ok=True)
    if not os.access(duckdb_path.parent, os.W_OK):
        raise PermissionError(f"Directory not writable: {duckdb_path.parent}")
    os.environ["DESTINATION__DUCKDB__CREDENTIALS"] = str(duckdb_path)
    ...
```
```

---

### [WARNING] Mutable default argument risk

**File**: `src/quant_alpha/ingestion/dlt_energy.py:84`
**Category**: Potential Bug
**Confidence**: 65%

Using `markets: list[str] | None = None` with `markets = markets or [...]` is correct, but the fallback list `['DE_LU', 'CZ', 'FR']` is hardcoded as a magic value without documentation of what these market codes represent.

---

### [WARNING] No null check on fetch_prices return value

**File**: `src/quant_alpha/ingestion/dlt_equity.py:47-50`
**Category**: Potential Bug
**Confidence**: 70%

If fetch_prices returns None or an empty DataFrame, the code will fail on line 48 (pd.to_datetime on None) or silently yield no rows without indication. There is no guard clause to handle these cases.

**Suggestion**:
```
Add a guard:
```python
prices = fetch_prices(cfg, universe, offline=offline)
if prices is None or prices.empty:
    return
prices["date"] = pd.to_datetime(prices["date"])
```
```

---

### [WARNING] Missing duckdb_path parent directory creation

**File**: `src/quant_alpha/ingestion/dlt_equity.py:55`
**Category**: Potential Bug
**Confidence**: 70%

If the parent directory of duckdb_path does not exist when the pipeline runs, duckdb will fail to create the database file. There is no os.makedirs() call to ensure the directory structure exists.

**Suggestion**:
```
Add directory creation in build_equity_pipeline or at the start of run_dlt_equity_pipeline:
```python
duckdb_path.parent.mkdir(parents=True, exist_ok=True)
```
```

---

### [WARNING] No validation on empty markets list

**File**: `src/quant_alpha/ingestion/energy.py:20`
**Category**: Potential Bug
**Confidence**: 95%

If an empty list is passed for markets, pd.concat will be called on an empty list, which raises a ValueError.

**Suggestion**:
```
Add validation at the start:
if not markets:
    raise ValueError("markets list cannot be empty")

Or handle gracefully by returning an empty DataFrame:
if not frames:
    return pd.DataFrame(columns=['timestamp', 'market', 'spot_price', 'load_forecast', 'actual_load', 'wind_forecast', 'solar_forecast', 'residual_load', 'imbalance_price', 'gas_price'])
```

---

### [WARNING] No validation on start/end date strings

**File**: `src/quant_alpha/ingestion/energy.py:21`
**Category**: Potential Bug
**Confidence**: 80%

Invalid or malformed date strings will cause pd.date_range to raise an error with an unclear message. There's no input validation.

**Suggestion**:
```
Add validation:
try:
    timestamps = pd.date_range(start, end, freq=freq)
except (ValueError, TypeError) as e:
    raise ValueError(f"Invalid date range or frequency: {e}") from e
```

---

### [WARNING] Solar generation not constrained to daylight hours

**File**: `src/quant_alpha/ingestion/energy.py:28`
**Category**: Potential Bug
**Confidence**: 75%

The solar formula uses np.maximum(0, ...) but the sin function can produce positive values at night hours (e.g., hour < 6 or hour > 18), resulting in unrealistic non-zero solar generation during nighttime.

**Suggestion**:
```
Add a night-time mask:
night_mask = (hour < 6) | (hour > 18)
solar = np.maximum(0, 14 * np.sin((hour - 6) / 12 * np.pi)) + rng.normal(0, 1, len(timestamps))
solar[night_mask] = np.maximum(0, rng.normal(0, 0.5, np.sum(night_mask)))
```

---

### [WARNING] Solar noise can make values negative before maximum

**File**: `src/quant_alpha/ingestion/energy.py:28`
**Category**: Potential Bug
**Confidence**: 90%

The rng.normal noise is added after np.maximum(0, ...), so the noise can push values negative, which is then not corrected. This creates negative solar generation values.

**Suggestion**:
```
Move the np.maximum to wrap the entire expression:
solar = np.maximum(0, 14 * np.sin((hour - 6) / 12 * np.pi) + rng.normal(0, 1, len(timestamps)))
```

---

### [WARNING] Scarcity pricing may not behave as expected

**File**: `src/quant_alpha/ingestion/energy.py:30-31`
**Category**: Potential Bug
**Confidence**: 60%

The scarcity premium uses the 80th percentile of residual_load, but for non-uniform distributions this may not accurately represent a 'scarcity' threshold. Also, the hard-coded 0.8 quantile is not parameterized.

**Suggestion**:
```
Consider making the scarcity quantile a parameter:
def generate_synthetic_power_market(..., scarcity_quantile: float = 0.8) -> pd.DataFrame:
And add a comment explaining why 0.8 was chosen.
```

---

### [WARNING] Repeated timestamp array copied to each DataFrame

**File**: `src/quant_alpha/ingestion/energy.py:48-58`
**Category**: Performance
**Confidence**: 60%

The same 'timestamps' array is copied into every market's DataFrame. For many markets, this creates redundant data. Consider building a single DataFrame with a market column instead of concatenating per-market frames.

**Suggestion**:
```
For better performance with many markets, consider vectorized approach:
all_data = []
for market in markets:
    ...
    all_data.append({'market': market, 'spot_price': spot, ...})
# Or use pd.concat which is fine for moderate market counts
```

---

### [WARNING] Inconsistent error handling between empty data checks

**File**: `src/quant_alpha/ingestion/yahoo.py:56-83`
**Category**: Potential Bug
**Confidence**: 70%

The `_normalize_yfinance_frame` function raises RuntimeError immediately if data is empty (line 58), but `fetch_prices` also checks if the result is empty after normalization (line 100). If yfinance returns non-empty data that gets fully filtered out during normalization (e.g., all NaN close values), the RuntimeError at line 58 won't trigger, but the one at line 100 will — with a different, less informative message. The empty check in _normalize_yfinance_frame may be overly aggressive since it could be valid for yfinance to return some empty subframes.

**Suggestion**:
```
Consider removing the early empty check in _normalize_yfinance_frame and relying solely on the check in fetch_prices, or provide consistent error messages.
```

---

### [WARNING] Silent skipping of missing symbols

**File**: `src/quant_alpha/ingestion/yahoo.py:64`
**Category**: Potential Bug
**Confidence**: 85%

When yfinance returns data with a MultiIndex and a requested symbol is not found in the returned data, it is silently skipped via `continue`. This means if some symbols fail to download, the function will return partial data without any warning, which could lead to incorrect analysis results.

**Suggestion**:
```
Add logging or a warning when symbols are skipped:
```python
import logging
logger = logging.getLogger(__name__)

for symbol in symbols:
    if symbol not in data.columns.get_level_values(0):
        logger.warning("Symbol %s not found in Yahoo Finance response", symbol)
        continue
```
```

---

### [WARNING] Column renaming redundancy and potential confusion

**File**: `src/quant_alpha/ingestion/yahoo.py:70-72`
**Category**: Potential Bug
**Confidence**: 85%

The rename_map maps 'adj_close' to 'adj_close' (identity mapping, no-op) and 'datetime' to 'date'. The adj_close identity mapping is dead code. Also, after lowercasing all column names, 'datetime' should already be lowercase, making the rename technically correct but the identity mapping is confusing.

**Suggestion**:
```
Remove the redundant identity mapping:
```python
rename_map = {"datetime": "date"}
```
```

---

### [WARNING] Deprecation warning: datetime.utcnow()

**File**: `src/quant_alpha/ingestion/yahoo.py:102`
**Category**: Potential Bug
**Confidence**: 95%

datetime.utcnow() is deprecated in Python 3.12 and will be removed in future versions. It also returns a naive datetime object which can cause timezone-related bugs.

**Suggestion**:
```
Replace with: `from datetime import datetime, timezone; as_of = datetime.now(timezone.utc).isoformat(timespec='seconds')`
```

---

### [WARNING] Missing docstring for run_pipeline function

**File**: `src/quant_alpha/pipeline.py:23-82`
**Category**: Readability
**Confidence**: 95%

The main pipeline function lacks a docstring. This is the primary public API of this module and should document its purpose, parameters, return value, and any side effects (file I/O, database writes).

**Suggestion**:
```
Add a docstring:
```python
def run_pipeline(config_path: Path, root: Path, offline: bool = False) -> dict[str, object]:
    """Run the full alpha research pipeline.

    Fetches prices, computes alpha factors, runs backtests and diagnostics,
    and persists all results to parquet files and a DuckDB database.

    Args:
        config_path: Path to the project YAML config file.
        root: Project root directory.
        offline: If True, skip network fetches and use cached data.

    Returns:
        Dictionary with output file paths, row counts, and backtest metrics.
    """
```
```

---

### [WARNING] No error handling or partial-failure recovery

**File**: `src/quant_alpha/pipeline.py:23-82`
**Category**: Potential Bug
**Confidence**: 85%

The run_pipeline function performs many sequential I/O operations (file writes and database inserts). If any operation fails mid-way (e.g., at line 55), the pipeline leaves partial results — some tables written to DuckDB, some parquet files created — with no cleanup or transactional consistency. There's no logging at all, making debugging difficult.

**Suggestion**:
```
Consider: (1) adding logging at each major step; (2) wrapping the function in a try/except that cleans up on failure, or (3) using DuckDB transactions for atomic writes. At minimum, add logging:

```python
import logging
logger = logging.getLogger(__name__)

# In run_pipeline:
logger.info('Fetching prices for %d tickers', len(universe))
prices = fetch_prices(cfg, universe, offline=offline)
logger.info('Fetched %d rows of price data', len(prices))
```
```

---

### [WARNING] Missing type hint for cfg parameter

**File**: `src/quant_alpha/pipeline_energy.py:28`
**Category**: Potential Bug
**Confidence**: 75%

The _load_power_market function's 'cfg' parameter has no type hint, making it unclear what object type is expected and reducing code clarity.

**Suggestion**:
```
Add a type hint: def _load_power_market(cfg: Any, markets: list[str], universe: dict[str, object]) -> pd.DataFrame:
```

---

### [WARNING] No exception handling in main pipeline function

**File**: `src/quant_alpha/pipeline_energy.py:55-139`
**Category**: Potential Bug
**Confidence**: 90%

The run_energy_pipeline function performs many operations (file I/O, database writes, cloud exports) without any try/except blocks. If any step fails, partial data may be written without cleanup, leaving inconsistent state.

**Suggestion**:
```
Add try/except with rollback/cleanup logic, or at minimum add logging for failures:
try:
    # existing code
except Exception as e:
    logger.error(f'Pipeline failed: {e}')
    raise
```

---

### [WARNING] Missing error handling for groupby shift operation

**File**: `src/quant_alpha/pipeline_energy.py:72`
**Category**: Potential Bug
**Confidence**: 75%

The groupby('market')['spot_price'].shift(-1) operation assumes 'market' and 'spot_price' columns exist in the features DataFrame. If add_energy_alpha_features returns data without these columns, this will raise a KeyError.

**Suggestion**:
```
Add validation:
required_cols = {'market', 'spot_price'}
if not required_cols.issubset(features.columns):
    raise ValueError(f'Features DataFrame missing required columns: {required_cols - set(features.columns)}')
```

---

### [WARNING] Division by zero risk with clip lower bound

**File**: `src/quant_alpha/pipeline_energy.py:73-74`
**Category**: Potential Bug
**Confidence**: 80%

The denominator uses .abs().clip(lower=20.0), but if spot_price is NaN, then abs() will produce NaN, and clip() won't replace NaN values, leading to NaN in forward_return rather than a handled case.

**Suggestion**:
```
Add NaN handling: denominator = features['spot_price'].abs().fillna(0).clip(lower=20.0)
```

---

### [WARNING] Empty alpha columns list could cause issues

**File**: `src/quant_alpha/pipeline_energy.py:80`
**Category**: Potential Bug
**Confidence**: 85%

If ENERGY_ALPHA_EXPRESSIONS is empty, alpha_cols will be an empty list, causing alpha_composite calculation to produce NaN for all rows and potentially errors in backtest functions.

**Suggestion**:
```
Add validation after line 80:
if not alpha_cols:
    raise ValueError('No alpha expressions defined in ENERGY_ALPHA_EXPRESSIONS')
```

---

### [WARNING] Duplicate asset names silently overwritten

**File**: `src/quant_alpha/platform/bruin_graph.py:44-56`
**Category**: Potential Bug
**Confidence**: 90%

In `_load_all_assets()`, if multiple YAML files or SQL files define assets with the same name, the later one silently overwrites the earlier one. This could lead to unexpected behavior without any warning.

**Suggestion**:
```
Add duplicate detection:
```python
def _load_all_assets(self) -> None:
    pipelines = self.root / "pipelines"
    if not pipelines.exists():
        return
    for asset_file in sorted(pipelines.rglob("*.asset.yml")):
        node = self._parse_asset_yml(asset_file)
        if node.name in self.nodes:
            raise ValueError(f"Duplicate asset name '{node.name}' found in {asset_file}")
        self.nodes[node.name] = node
    ...
```
```

---

### [WARNING] Missing validation for 'name' field in asset YAML

**File**: `src/quant_alpha/platform/bruin_graph.py:73-85`
**Category**: Potential Bug
**Confidence**: 90%

The `_parse_asset_yml()` method doesn't validate that the 'name' field exists in the YAML data before accessing it. If the YAML file is malformed or missing the required 'name' field, this will raise a KeyError with an unhelpful error message.

**Suggestion**:
```
Add explicit validation:
```python
def _parse_asset_yml(self, path: Path) -> AssetNode:
    data = yaml.safe_load(path.read_text())
    if 'name' not in data:
        raise ValueError(f"Asset YAML missing required 'name' field: {path}")
    run_cfg = data.get("run", {})
    ...
```
```

---

### [WARNING] Fragile YAML parsing in SQL frontmatter

**File**: `src/quant_alpha/platform/bruin_graph.py:87-95`
**Category**: Potential Bug
**Confidence**: 80%

The SQL frontmatter parser uses string operations to find '/* @asset' and '*/' markers, but doesn't handle edge cases like multiple comment blocks, nested comments, or comments within the YAML content itself.

**Suggestion**:
```
Use more robust parsing or regex:
```python
import re

def _parse_sql_asset(self, path: Path) -> AssetNode | None:
    text = path.read_text()
    match = re.search(r'/\*\s*@asset\s*\n(.*?)\*/', text, re.DOTALL)
    if not match:
        return None
    yaml_block = match.group(1).strip()
    ...
```
```

---

### [WARNING] Cycle detection missing in upstream traversal

**File**: `src/quant_alpha/platform/bruin_graph.py:97-109`
**Category**: Potential Bug
**Confidence**: 85%

The `upstream()` method uses a visited list but does not detect cycles in the dependency graph. If a circular dependency exists (e.g., A depends on B, B depends on A), the breadth-first traversal could still revisit nodes incorrectly since `visited` is checked but dependencies are extended without verifying the node itself has been visited.

**Suggestion**:
```
Add cycle detection or use a set for O(1) lookup:
```python
def upstream(self, name: str, depth: int = 99) -> list[str]:
    visited: set[str] = set()
    queue = list(self.nodes[name].depends)
    while queue and depth > 0:
        depth -= 1
        nxt: list[str] = []
        for dep in queue:
            if dep not in visited:
                visited.add(dep)
                nxt.extend(self.nodes[dep].depends if dep in self.nodes else [])
        queue = nxt
    return list(visited)
```
```

---

### [WARNING] Inefficient downstream implementation with O(n²) complexity

**File**: `src/quant_alpha/platform/bruin_graph.py:112-117`
**Category**: Performance
**Confidence**: 90%

The `downstream()` method calls `self.upstream()` for every node in the graph, resulting in O(n² * m) complexity where n is number of nodes and m is average depth. For large graphs, this will be very slow.

**Suggestion**:
```
Implement a single reverse BFS from the target node:
```python
def downstream(self, name: str) -> list[str]:
    result: list[str] = []
    # Build reverse adjacency list
    reverse_deps: dict[str, list[str]] = {n: [] for n in self.nodes}
    for node in self.nodes.values():
        for dep in node.depends:
            if dep in reverse_deps:
                reverse_deps[dep].append(node.name)
    # BFS
    queue = [name]
    visited = {name}
    while queue:
        current = queue.pop(0)
        for child in reverse_deps.get(current, []):
            if child not in visited:
                visited.add(child)
                result.append(child)
                queue.append(child)
    return result
```
```

---

### [WARNING] Topological sort doesn't detect cycles

**File**: `src/quant_alpha/platform/bruin_graph.py:119-139`
**Category**: Potential Bug
**Confidence**: 95%

The `topological_order()` implementation uses Kahn's algorithm but doesn't check if all nodes were processed. If there's a cycle, the algorithm will silently return an incomplete order without raising an error.

**Suggestion**:
```
Add cycle detection after the algorithm completes:
```python
def topological_order(self) -> list[str]:
    ...
    if len(order) != len(self.nodes):
        remaining = set(self.nodes) - set(order)
        raise ValueError(f"Cycle detected involving assets: {remaining}")
    return order
```
```

---

### [WARNING] State mutation makes run() non-reentrant

**File**: `src/quant_alpha/platform/bruin_graph.py:155-190`
**Category**: Potential Bug
**Confidence**: 85%

The `run()` method mutates the status of AssetNode objects in place. If `run()` is called multiple times (e.g., for retry logic), the previous execution state will affect subsequent runs. This violates the principle of least surprise.

**Suggestion**:
```
Either reset node statuses before running or create a separate execution context:
```python
def run(self, targets=None, env=None, dry_run=False):
    # Reset all node statuses
    for node in self.nodes.values():
        node.status = AssetStatus.PENDING
        node.duration_s = 0.0
        node.error = None
    ...
```
```

---

### [WARNING] Missing stdout capture in error reporting

**File**: `src/quant_alpha/platform/bruin_graph.py:193-206`
**Category**: Potential Bug
**Confidence**: 80%

When subprocess fails, only `stderr` is included in the error message. However, many programs output useful debugging information to stdout, which is lost in the error report.

**Suggestion**:
```
Include both stdout and stderr in error messages:
```python
if result.returncode != 0:
    error_msg = result.stderr.strip() or result.stdout.strip() or "non-zero exit"
    raise RuntimeError(error_msg)
```
```

---

### [WARNING] SQL file validation only checks file existence

**File**: `src/quant_alpha/platform/bruin_graph.py:207-210`
**Category**: Potential Bug
**Confidence**: 75%

For SQL assets, the code only reads the file to verify it exists, but doesn't validate the SQL syntax or check for malicious content. This could lead to runtime errors when the SQL is actually executed.

**Suggestion**:
```
Add basic SQL validation or at least document the limitation:
```python
elif node.sql_file:
    sql_content = Path(node.sql_file).read_text()
    # Could add basic SQL parsing/validation here
    if not sql_content.strip():
        raise RuntimeError(f"SQL file is empty: {node.sql_file}")
```
```

---

### [WARNING] No validation of freshness_expectation values

**File**: `src/quant_alpha/platform/contracts.py:7-12`
**Category**: Potential Bug
**Confidence**: 75%

The freshness_expectation field is a freeform string with no validation. This allows inconsistent values like 'Daily' vs 'daily', or typos like 'daliy', which could cause data quality checks to silently fail.

**Suggestion**:
```
Either use an Enum for valid values or add a __post_init__ validator:
```python
from enum import Enum

class Freshness(str, Enum):
    HOURLY = "hourly"
    DAILY = "daily"
    WEEKLY = "weekly"

@dataclass(frozen=True)
class DatasetContract:
    ...
    freshness_expectation: Freshness

    def __post_init__(self) -> None:
        if not isinstance(self.freshness_expectation, Freshness):
            object.__setattr__(self, 'freshness_expectation', Freshness(self.freshness_expectation))
```
```

---

### [WARNING] No validation of primary_keys tuple

**File**: `src/quant_alpha/platform/contracts.py:7-12`
**Category**: Potential Bug
**Confidence**: 70%

There is no validation that primary_keys is non-empty. A DatasetContract could be created with an empty tuple of primary keys, which would be a logical error for data governance purposes.

**Suggestion**:
```
Add validation in __post_init__:
```python
def __post_init__(self) -> None:
    if not self.primary_keys:
        raise ValueError(f"Dataset '{self.name}' must have at least one primary key")
```
```

---

### [WARNING] Possible contract name collisions across domains

**File**: `src/quant_alpha/platform/contracts.py:15-48`
**Category**: Potential Bug
**Confidence**: 75%

EQUITY_DATASETS and ENERGY_DATASETS are concatenated into ALL_DATASETS without checking for duplicate names. If a contract name is accidentally reused across domains, consumers indexing by name would silently get the wrong contract.

**Suggestion**:
```
Add a runtime uniqueness check after constructing ALL_DATASETS:
```python
ALL_DATASETS = EQUITY_DATASETS + ENERGY_DATASETS

def _validate_unique_names() -> None:
    names = [d.name for d in ALL_DATASETS]
    dupes = {n for n in names if names.count(n) > 1}
    if dupes:
        raise ValueError(f"Duplicate dataset contract names found: {dupes}")

_validate_unique_names()
```
```

---

### [WARNING] Missing docstring and edge case handling

**File**: `src/quant_alpha/platform/quality.py:6`
**Category**: Potential Bug
**Confidence**: 85%

The function validate_primary_key lacks a docstring and doesn't handle edge cases like empty DataFrames, empty keys list, or columns that don't exist in the DataFrame.

**Suggestion**:
```
Add docstring and input validation:
```python
def validate_primary_key(frame: pd.DataFrame, keys: list[str]) -> dict[str, object]:
    """Validate that specified columns form a primary key (no duplicates).
    
    Args:
        frame: DataFrame to validate
        keys: Column names forming the primary key
        
    Returns:
        Dictionary with validation results
    
    Raises:
        KeyError: If keys don't exist in frame
    """
    if not keys:
        raise ValueError("keys list cannot be empty")
    missing = [k for k in keys if k not in frame.columns]
    if missing:
        raise KeyError(f"Columns not found in DataFrame: {missing}")
    # ... rest of implementation
```
```

---

### [WARNING] Missing input validation in validate_non_null

**File**: `src/quant_alpha/platform/quality.py:10-18`
**Category**: Potential Bug
**Confidence**: 85%

The function validate_non_null doesn't validate that the requested columns exist in the DataFrame before accessing them, which will raise a KeyError with an unclear error message.

**Suggestion**:
```
Add column existence check:
```python
def validate_non_null(frame: pd.DataFrame, columns: list[str]) -> list[dict[str, object]]:
    """Validate that specified columns contain no null values.
    
    Args:
        frame: DataFrame to validate
        columns: Column names to check for nulls
        
    Returns:
        List of validation result dictionaries
    
    Raises:
        KeyError: If columns don't exist in frame
    """
    if not columns:
        return []
    missing = [c for c in columns if c not in frame.columns]
    if missing:
        raise KeyError(f"Columns not found in DataFrame: {missing}")
    # ... rest of implementation
```
```

---

### [WARNING] Hardcoded column names create tight coupling

**File**: `src/quant_alpha/platform/quality.py:22-32`
**Category**: Potential Bug
**Confidence**: 80%

The function run_energy_quality_checks hardcodes specific column names (timestamp, market, spot_price, load_forecast, residual_load), making it brittle to schema changes and reducing reusability.

**Suggestion**:
```
Make columns configurable with sensible defaults:
```python
def run_energy_quality_checks(
    frame: pd.DataFrame,
    primary_key: list[str] | None = None,
    required_columns: list[str] | None = None
) -> pd.DataFrame:
    """Run data quality checks on energy market DataFrame.
    
    Args:
        frame: DataFrame to validate
        primary_key: Columns forming primary key (default: ["timestamp", "market"])
        required_columns: Columns that must not be null
                          (default: ["timestamp", "market", "spot_price", "load_forecast", "residual_load"])
        
    Returns:
        DataFrame containing validation results
    """
    if primary_key is None:
        primary_key = ["timestamp", "market"]
    if required_columns is None:
        required_columns = ["timestamp", "market", "spot_price", "load_forecast", "residual_load"]
    # ... rest of implementation
```
```

---

### [WARNING] Missing module-level docstring

**File**: `src/quant_alpha/storage/duckdb.py:1-33`
**Category**: Convention
**Confidence**: 90%

The module lacks a docstring explaining its purpose, which reduces code readability and makes it harder for new developers to understand the module's responsibility.

**Suggestion**:
```
Add a module-level docstring:

```python
"""DuckDB storage utilities for writing DataFrames and metrics to local databases."""
```
```

---

### [WARNING] Missing function docstring

**File**: `src/quant_alpha/storage/duckdb.py:10-15`
**Category**: Convention
**Confidence**: 90%

The write_table function lacks a docstring explaining its parameters, behavior, and potential exceptions.

**Suggestion**:
```
Add a docstring:

```python
def write_table(db_path: Path, table_name: str, frame: pd.DataFrame) -> None:
    """Write a DataFrame as a table in a DuckDB database.
    
    Args:
        db_path: Path to the DuckDB database file.
        table_name: Name for the table to create/replace.
        frame: DataFrame to write.
    """
```
```

---

### [WARNING] Missing function docstring

**File**: `src/quant_alpha/storage/duckdb.py:18-20`
**Category**: Convention
**Confidence**: 90%

The write_metrics function lacks a docstring explaining its purpose and parameters.

**Suggestion**:
```
Add a docstring:

```python
def write_metrics(db_path: Path, metrics: dict[str, float], table_name: str = "backtest_metrics") -> None:
    """Write a dictionary of metrics as a single-row table.
    
    Args:
        db_path: Path to the DuckDB database file.
        metrics: Dictionary of metric name to value.
        table_name: Name for the table (default: backtest_metrics).
    """
```
```

---

### [WARNING] Missing function docstring

**File**: `src/quant_alpha/storage/duckdb.py:23-33`
**Category**: Convention
**Confidence**: 90%

The table_exists function lacks a docstring explaining its purpose and behavior.

**Suggestion**:
```
Add a docstring:

```python
def table_exists(db_path: Path, table_name: str) -> bool:
    """Check if a table exists in a DuckDB database.
    
    Args:
        db_path: Path to the DuckDB database file.
        table_name: Name of the table to check.
    
    Returns:
        True if the table exists, False otherwise.
    """
```
```

---

### [WARNING] Silent exception swallowing masks errors

**File**: `src/quant_alpha/storage/duckdb.py:30-33`
**Category**: Potential Bug
**Confidence**: 85%

The bare except clause catches ALL exceptions including KeyboardInterrupt and SystemExit, silently returning False. This masks potential database corruption, permission issues, or configuration problems that should be logged or propagated.

**Suggestion**:
```
Catch specific exceptions and optionally log the error:

```python
import logging

logger = logging.getLogger(__name__)

def table_exists(db_path: Path, table_name: str) -> bool:
    ...
    except (duckdb.Error, OSError) as e:
        logger.warning("Failed to check table existence: %s", e)
        return False
```
```

---

### [WARNING] Missing module docstring

**File**: `src/quant_alpha/storage/gcp.py:1`
**Category**: Convention
**Confidence**: 90%

The module lacks a docstring explaining its purpose and usage.

**Suggestion**:
```
Add a module-level docstring:
"""GCS and BigQuery export utilities for Quant Alpha dataframes."""
```

---

### [WARNING] Missing function docstring

**File**: `src/quant_alpha/storage/gcp.py:15-18`
**Category**: Convention
**Confidence**: 95%

The export_frames_to_gcs_bigquery function has no docstring explaining its purpose, parameters, return value, or exceptions.

**Suggestion**:
```
Add a comprehensive docstring:
"""Export DataFrame dictionary to GCS as Parquet and load into BigQuery.

Args:
    frames: Dictionary mapping table names to DataFrames.
    config: Cloud export configuration.

Returns:
    Dictionary mapping table names to fully-qualified BigQuery table IDs.

Raises:
    CloudExportError: If configuration is invalid or cloud libraries are missing.
"""
```

---

### [WARNING] Missing validation of gcs_prefix configuration

**File**: `src/quant_alpha/storage/gcp.py:22-25`
**Category**: Potential Bug
**Confidence**: 70%

The config.gcs_prefix is used to construct blob names but is not validated. If gcs_prefix is None or contains problematic characters, it could cause unexpected behavior.

**Suggestion**:
```
Add validation:
```python
if config.gcs_prefix is None:
    config.gcs_prefix = ""
```
```

---

### [WARNING] No error handling for GCS upload failures

**File**: `src/quant_alpha/storage/gcp.py:43-44`
**Category**: Potential Bug
**Confidence**: 90%

blob.upload_from_filename() can raise exceptions (network errors, permission errors, bucket not found). If the upload fails, the error message won't be wrapped in CloudExportError.

**Suggestion**:
```
Wrap the upload in a try-except:
```python
try:
    blob.upload_from_filename(str(local_path))
except Exception as exc:
    raise CloudExportError(
        f"GCS upload failed for {table_name}: {exc}"
    ) from exc
```
```

---

### [WARNING] autodetect=True may cause schema inconsistencies

**File**: `src/quant_alpha/storage/gcp.py:53-55`
**Category**: Potential Bug
**Confidence**: 70%

Using autodetect=True for BigQuery schema detection means the schema is inferred from the first batch of data. Subsequent loads with different column types could fail or produce unexpected results.

**Suggestion**:
```
Consider explicitly defining the schema based on the DataFrame dtypes, or document that autodetect is intentional and what the implications are.
```

---

### [WARNING] No input validation on n_hours parameter

**File**: `src/quant_alpha/streaming/demo_signals.py:17`
**Category**: Potential Bug
**Confidence**: 80%

The `n_hours` parameter is used directly in `pd.Timedelta(hours=n_hours)`. If a negative value or zero is passed, `start` would be after or equal to `end`, potentially causing issues in `generate_synthetic_power_market` or returning an empty dataframe silently.

**Suggestion**:
```
Add validation:
```python
def generate_live_signals(
    markets: list[str] | None = None,
    n_hours: int = 48,
) -> pd.DataFrame:
    if n_hours <= 0:
        raise ValueError(f'n_hours must be positive, got {n_hours}')
```
```

---

### [WARNING] No error handling for upstream data generation

**File**: `src/quant_alpha/streaming/demo_signals.py:22`
**Category**: Potential Bug
**Confidence**: 70%

Both `generate_synthetic_power_market` and `add_energy_alpha_features` are called without error handling. If either raises an exception (e.g., due to invalid market codes, network issues, or internal errors), the error propagates up with no context about which step failed.

**Suggestion**:
```
Consider wrapping in try/except with informative error messages, or at minimum document the exceptions these functions may raise in the docstring.
```

---

### [WARNING] Missing columns silently dropped from output

**File**: `src/quant_alpha/streaming/demo_signals.py:27-29`
**Category**: Potential Bug
**Confidence**: 85%

The list comprehension `features[[c for c in keep if c in features.columns]]` silently drops any columns in `keep` that don't exist in `features`. This means if critical columns like 'timestamp' or 'market' are missing (which would break the sort on line 31), there's no warning or error. The subsequent `sort_values(['timestamp', 'market'])` would raise a KeyError if those columns are missing, masking the real problem.

**Suggestion**:
```
Add validation for required columns:
```python
required_cols = {'timestamp', 'market'}
missing = required_cols - set(features.columns)
if missing:
    raise ValueError(f'Missing required columns: {missing}')
frame = features[keep].copy()
```
```

---

### [WARNING] No error handling for missing schema file

**File**: `src/quant_alpha/streaming/redpanda_consumer.py:10-11`
**Category**: Potential Bug
**Confidence**: 75%

_load_schema will raise a FileNotFoundError or JSONDecodeError if the schema file doesn't exist or is invalid. These errors are not caught or wrapped with a helpful message.

**Suggestion**:
```
Add error handling:

def _load_schema(path: Path) -> dict:
    if not path.exists():
        raise FileNotFoundError(f'Schema file not found: {path}')
    # ... rest of function
```

---

### [WARNING] Return type dict is too generic

**File**: `src/quant_alpha/streaming/redpanda_consumer.py:15`
**Category**: Potential Bug
**Confidence**: 60%

_load_schema returns `dict` but parse_schema actually returns a more specific Schema type from fastavro. The type hint is misleading.

**Suggestion**:
```
Use the correct return type from fastavro:
from fastavro.types import Schema

def _load_schema(path: Path) -> Schema:
    # or use Any if the specific type is not easily importable
```

---

### [WARNING] msg.error() not logged or handled

**File**: `src/quant_alpha/streaming/redpanda_consumer.py:48`
**Category**: Potential Bug
**Confidence**: 85%

When `msg.error()` returns a non-falsy value, the code silently increments empty_polls and continues. This means actual Kafka errors (connection issues, auth failures, etc.) are treated as empty polls and swallowed silently.

**Suggestion**:
```
Check and log the specific error:
if msg is None:
    empty_polls += 1
    continue
if msg.error():
    logging.error(f'Kafka error: {msg.error()}')
    # Consider raising or handling specific error codes
    empty_polls += 1
    continue
```

---

### [WARNING] CREATE TABLE IF NOT EXISTS race condition

**File**: `src/quant_alpha/streaming/redpanda_consumer.py:82`
**Category**: Potential Bug
**Confidence**: 70%

Using 'CREATE TABLE IF NOT EXISTS ... AS SELECT * FROM frame WHERE false' followed by INSERT is not atomic. If the table schema changes between the CREATE and INSERT, or if the frame columns don't match an existing table, this will fail or produce incorrect results.

**Suggestion**:
```
Consider using `CREATE OR REPLACE TABLE` if you want to overwrite, or handle the case where the table already exists with a different schema. Also consider using DuckDB's `INSERT OR REPLACE` or upsert syntax if the docstring claims 'upsert' behavior.
```

---

### [WARNING] DuckDB table schema may not match frame

**File**: `src/quant_alpha/streaming/redpanda_consumer.py:85`
**Category**: Potential Bug
**Confidence**: 70%

If the table already exists with a different schema than the current frame, the INSERT will fail. The code assumes schema evolution never happens.

**Suggestion**:
```
Add schema validation or handle the duckdb error gracefully:
try:
    con.execute(f'INSERT INTO {table} SELECT * FROM frame')
except duckdb.BinderException as e:
    logging.error(f'Schema mismatch for table {table}: {e}')
    raise
```

---

### [WARNING] Hardcoded Kafka configuration in __main__

**File**: `src/quant_alpha/streaming/redpanda_consumer.py:89`
**Category**: Performance
**Confidence**: 70%

Bootstrap server address 'localhost:19092' and topic 'energy-signals' are hardcoded in the __main__ block. This makes it inflexible for different environments.

**Suggestion**:
```
Use environment variables or a configuration file:
import os
bootstrap = os.environ.get('KAFKA_BOOTSTRAP_SERVERS', 'localhost:19092')
topic = os.environ.get('KAFKA_TOPIC', 'energy-signals')
```

---

### [WARNING] Missing error handling for file operations

**File**: `src/quant_alpha/streaming/redpanda_producer.py:12-15`
**Category**: Potential Bug
**Confidence**: 90%

The _load_schema function opens and reads a JSON file without handling potential FileNotFoundError, PermissionError, or JSONDecodeError exceptions.

**Suggestion**:
```
Add try-except blocks:
```python
def _load_schema(path: Path) -> dict:
    from fastavro import parse_schema
    try:
        with path.open("r", encoding="utf-8") as f:
            return parse_schema(json.load(f))
    except FileNotFoundError:
        raise ValueError(f"Schema file not found: {path}") from None
    except json.JSONDecodeError as e:
        raise ValueError(f"Invalid JSON in schema file: {e}") from e
```
```

---

### [WARNING] Missing error handling for Avro serialization

**File**: `src/quant_alpha/streaming/redpanda_producer.py:18-23`
**Category**: Potential Bug
**Confidence**: 85%

The _serialize function doesn't handle potential errors from schemaless_writer, such as schema validation failures or serialization errors.

**Suggestion**:
```
Add error handling:
```python
def _serialize(schema: dict, payload: dict) -> bytes:
    import io
    from fastavro import schemaless_writer
    try:
        buf = io.BytesIO()
        schemaless_writer(buf, schema, payload)
        return buf.getvalue()
    except Exception as e:
        raise ValueError(f"Avro serialization failed: {e}") from e
```
```

---

### [WARNING] Missing function docstring

**File**: `src/quant_alpha/streaming/redpanda_producer.py:26`
**Category**: Convention
**Confidence**: 95%

The publish_energy_signals function lacks a docstring explaining its parameters, return value, and side effects.

**Suggestion**:
```
Add a function docstring:
```python
def publish_energy_signals(bootstrap_servers: str, topic: str, schema_path: Path, sample_size: int = 100) -> None:
    """Publish energy market signals to Kafka/Redpanda.
    
    Args:
        bootstrap_servers: Kafka broker addresses
        topic: Target topic name
        schema_path: Path to Avro schema file
        sample_size: Number of records to publish (default: 100)
    """
```
```

---

### [WARNING] Missing validation for sample_size parameter

**File**: `src/quant_alpha/streaming/redpanda_producer.py:29`
**Category**: Potential Bug
**Confidence**: 85%

The sample_size parameter is used directly without validation, which could cause issues if negative or zero.

**Suggestion**:
```
Add input validation:
```python
def publish_energy_signals(bootstrap_servers: str, topic: str, schema_path: Path, sample_size: int = 100) -> None:
    if sample_size <= 0:
        raise ValueError("sample_size must be positive")
```
```

---

### [WARNING] Missing validation for bootstrap_servers parameter

**File**: `src/quant_alpha/streaming/redpanda_producer.py:31`
**Category**: Potential Bug
**Confidence**: 80%

The bootstrap_servers parameter is passed directly to Kafka producer without validation. Invalid format could cause confusing errors.

**Suggestion**:
```
Add basic validation:
```python
def publish_energy_signals(bootstrap_servers: str, topic: str, schema_path: Path, sample_size: int = 100) -> None:
    if not bootstrap_servers or not isinstance(bootstrap_servers, str):
        raise ValueError("bootstrap_servers must be a non-empty string")
```
```

---

### [WARNING] Missing error handling for DataFrame operations

**File**: `src/quant_alpha/streaming/redpanda_producer.py:36-44`
**Category**: Potential Bug
**Confidence**: 80%

The market.head(sample_size).to_dict(orient='records') operation could fail if the DataFrame is empty or has unexpected structure.

**Suggestion**:
```
Add validation:
```python
market = generate_synthetic_power_market(["DE_LU", "CZ", "FR"], "2024-01-01", "2024-01-07")
if market.empty:
    raise ValueError("No market data generated")
if sample_size > len(market):
    sample_size = len(market)
```
```

---

### [WARNING] Hardcoded localhost address in __main__ block

**File**: `src/quant_alpha/streaming/redpanda_producer.py:48`
**Category**: Security
**Confidence**: 85%

The bootstrap server address 'localhost:19092' is hardcoded in the __main__ block, which is not suitable for production environments.

**Suggestion**:
```
Use environment variables or configuration:
```python
if __name__ == "__main__":
    import os
    bootstrap_servers = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:19092")
    root = Path(__file__).resolve().parents[3]
    publish_energy_signals(bootstrap_servers, "energy-signals", root / "schemas/energy_signal.avsc")
```
```

---

### [WARNING] Hardcoded topic name in __main__ block

**File**: `src/quant_alpha/streaming/redpanda_producer.py:48`
**Category**: Potential Bug
**Confidence**: 80%

The topic name 'energy-signals' is hardcoded in the __main__ block, which may not be appropriate for different environments.

**Suggestion**:
```
Use environment variables or configuration:
```python
if __name__ == "__main__":
    import os
    topic = os.getenv("KAFKA_TOPIC", "energy-signals")
    root = Path(__file__).resolve().parents[3]
    publish_energy_signals("localhost:19092", topic, root / "schemas/energy_signal.avsc")
```
```

---

### [WARNING] No error handling for SQL execution in apply_views

**File**: `src/quant_alpha/streaming/risingwave/client.py:43`
**Category**: Potential Bug
**Confidence**: 90%

The function executes DDL statements without try-except blocks. If any statement fails, the connection may be left in an inconsistent state, and the function won't provide useful error information.

**Suggestion**:
```
Add error handling:
```python
def apply_views(conn: Any, views_sql_path: Path | None = None) -> list[str]:
    sql = (views_sql_path or _VIEWS_SQL).read_text()
    stmts = _split_statements(sql)
    applied = []
    try:
        with conn.cursor() as cur:
            for stmt in stmts:
                if not any(kw in stmt.upper() for kw in _DDL_KEYWORDS):
                    continue
                try:
                    cur.execute(stmt)
                    name = stmt.split()[-1].split("(")[0].strip().lower()
                    applied.append(name)
                except Exception as e:
                    raise RuntimeError(f"Failed to execute statement: {stmt[:100]}... Error: {e}") from e
        conn.commit()
    except Exception:
        conn.rollback()
        raise
    return applied
```
```

---

### [WARNING] Fragile statement name extraction logic

**File**: `src/quant_alpha/streaming/risingwave/client.py:48`
**Category**: Potential Bug
**Confidence**: 85%

The logic `stmt.split()[-1].split('(')[0].strip().lower()` to extract statement names is brittle. It assumes the statement format is exactly 'CREATE [TYPE] [IF NOT EXISTS] name(...)', but this could fail with different SQL formatting, comments, or unexpected whitespace.

**Suggestion**:
```
Use regex for more robust parsing:
```python
import re
def _extract_statement_name(stmt: str) -> str:
    # Match CREATE SOURCE/MATERIALIZED VIEW [IF NOT EXISTS] name
    match = re.search(r'CREATE\s+(?:MATERIALIZED\s+)?(?:VIEW|SOURCE)(?:\s+IF\s+NOT\s+EXISTS)?\s+[`(]?([^`(\s]+)', stmt, re.IGNORECASE)
    return match.group(1).lower() if match else "unknown"
```
```

---

### [WARNING] Resource leak: connection not closed on error

**File**: `src/quant_alpha/streaming/risingwave/client.py:57-67`
**Category**: Potential Bug
**Confidence**: 80%

If an exception occurs during query execution, the connection and cursor may not be properly closed. While context managers handle cursors, the connection itself isn't managed.

**Suggestion**:
```
Consider using a connection pool or context manager for connections:
```python
def get_connection(host: str = "localhost", port: int = 4566, database: str = "dev") -> Any:
    # ... existing code ...
    conn = psycopg2.connect(...)
    # Wrap connection in a context manager or use a pool
    return conn
```
Or document that callers must use `with` statement:
```python
# Usage:
with get_connection() as conn:
    result = query_realtime_scores(conn)
```
```

---

### [WARNING] Missing input validation for 'hours' parameter

**File**: `src/quant_alpha/streaming/risingwave/client.py:70-86`
**Category**: Potential Bug
**Confidence**: 85%

The 'hours' parameter is directly used in SQL without validation. While it's typed as int, negative values or extremely large values could cause unexpected behavior or performance issues.

**Suggestion**:
```
Add validation:
```python
def query_hourly_window(
    conn: Any,
    market: str | None = None,
    hours: int = 24,
) -> pd.DataFrame:
    if hours <= 0:
        raise ValueError("hours must be positive")
    if hours > 8760:  # 1 year
        raise ValueError("hours cannot exceed 8760 (1 year)")
    # ... rest of function
```
```

---

### [WARNING] Missing input validation for 'level' parameter

**File**: `src/quant_alpha/streaming/risingwave/client.py:89-103`
**Category**: Potential Bug
**Confidence**: 80%

The 'level' parameter is used without validation. While there's a fallback to 'HIGH', invalid inputs silently use a default value which could mask bugs in calling code.

**Suggestion**:
```
Add explicit validation:
```python
def query_scarcity_alerts(conn: Any, level: str = "HIGH") -> pd.DataFrame:
    levels = {"HIGH": ["HIGH"], "MEDIUM": ["HIGH", "MEDIUM"], "LOW": ["HIGH", "MEDIUM", "LOW"]}
    level = level.upper()
    if level not in levels:
        raise ValueError(f"Invalid level '{level}'. Must be one of: {list(levels.keys())}")
    valid_levels = levels[level]
    # ... rest of function
```
```

---

### [WARNING] Hardcoded credentials in get_connection

**File**: `src/quant_alpha/streaming/risingwave/client.py:129`
**Category**: Security
**Confidence**: 95%

The function hardcodes user='root' and empty password=''. This is a security risk as it bypasses authentication and provides superuser access. Production systems should use environment variables or a secure configuration mechanism.

**Suggestion**:
```
Use environment variables or secure configuration:
```python
def get_connection(host: str = "localhost", port: int = 4566, database: str = "dev", user: str | None = None, password: str | None = None) -> Any:
    import os
    user = user or os.environ.get("RISINGWAVE_USER", "root")
    password = password or os.environ.get("RISINGWAVE_PASSWORD", "")
    # ... rest of function
```
```

---

### [WARNING] Missing input validation for environment variables

**File**: `src/quant_alpha/streaming/risingwave/producer.py:22`
**Category**: Potential Bug
**Confidence**: 85%

Environment variable INTERVAL_SECONDS is converted to float without validation. Invalid values like 'abc', negative numbers, or extremely large values will either crash or cause unexpected behavior.

**Suggestion**:
```
Add validation: `INTERVAL = max(1.0, float(os.environ.get('INTERVAL_SECONDS', '60')))` or wrap in try-except with a sensible default.
```

---

### [WARNING] No error handling in delivery callback

**File**: `src/quant_alpha/streaming/risingwave/producer.py:36`
**Category**: Potential Bug
**Confidence**: 80%

The _delivery_report callback only prints errors but doesn't track or handle failed deliveries. Critical message delivery failures could go unnoticed in production.

**Suggestion**:
```
Implement proper error tracking: `if err: logging.error(f'delivery failed: {err}'); # increment metric or raise alert`
```

---

### [WARNING] Missing type hints for producer parameter

**File**: `src/quant_alpha/streaming/risingwave/producer.py:42`
**Category**: Potential Bug
**Confidence**: 70%

The `producer` parameter in stream_signals lacks type hints, making it unclear what type of producer object is expected.

**Suggestion**:
```
Add type hint: `def stream_signals(producer: confluent_kafka.Producer, once: bool = False) -> None:`
```

---

### [WARNING] Missing error handling in main execution

**File**: `src/quant_alpha/streaming/risingwave/producer.py:56`
**Category**: Potential Bug
**Confidence**: 80%

The main block lacks try-except handling. If _make_producer() or stream_signals() fails, the program will crash with a traceback instead of graceful error handling.

**Suggestion**:
```
Wrap main logic in try-except: `try: p = _make_producer(); stream_signals(p) except KeyboardInterrupt: pass except Exception as e: print(f'Error: {e}'); sys.exit(1)`
```

---

### [WARNING] DuckDB connection resource leak on exception

**File**: `src/quant_alpha/streaming/risingwave/simulator.py:35`
**Category**: Potential Bug
**Confidence**: 95%

If an exception occurs during SQL execution (lines 37-76), the `con.close()` on line 81 will be skipped, leaving the DuckDB connection open. This is especially problematic when db_path is not ':memory:' as file locks may persist.

**Suggestion**:
```
Use a context manager or try/finally to ensure the connection is closed:
```python
con = duckdb.connect(db_path)
try:
    con.register("power_market_signals", features)
    result = con.execute("""...""").df()
finally:
    con.close()
```
```

---

### [WARNING] Hardcoded magic number for gas price fallback

**File**: `src/quant_alpha/streaming/risingwave/simulator.py:48`
**Category**: Potential Bug
**Confidence**: 80%

The value 35.0 is used as a default gas price in `COALESCE(s.gas_price, 35.0)`. This hardcoded magic number is brittle — if gas price units change or if the simulation context changes, this could produce incorrect calculations silently.

**Suggestion**:
```
Extract the default gas price into a named constant with a comment explaining the unit and context:
```python
DEFAULT_GAS_PRICE_EUR_MWH = 35.0  # Typical European gas price
```
Then reference it in the SQL as a parameter or define it as a constant in the module.
```

---

### [WARNING] ORDER BY may be lost when converting to pandas

**File**: `src/quant_alpha/streaming/risingwave/simulator.py:73`
**Category**: Potential Bug
**Confidence**: 60%

The SQL query includes `ORDER BY timestamp DESC, market`, but DuckDB's `.df()` method may not preserve this ordering in all scenarios, especially with larger datasets. Pandas operations downstream could also reorder the results.

**Suggestion**:
```
If ordering matters for the return value, explicitly sort in pandas after conversion:
```python
result = con.execute("""...""").df()
result = result.sort_values(["timestamp", "market"], ascending=[False, True]).reset_index(drop=True)
```
```

---

### [WARNING] Missing null/NaN checks in get_scarcity_alerts

**File**: `src/quant_alpha/streaming/risingwave/simulator.py:87-91`
**Category**: Potential Bug
**Confidence**: 85%

The `get_scarcity_alerts` function accesses `panel["alpha_residual_load_rank"]` and `panel["alpha_momentum_6h"]` without checking if these columns exist or if they contain NaN values. If the panel DataFrame is empty or the columns are missing, this will raise a KeyError. NaN comparisons in boolean indexing behave as False, but the subsequent `.loc` assignment on lines 89-91 may produce unexpected results with NaN values in `alpha_momentum_6h`.

**Suggestion**:
```
Add input validation:
```python
def get_scarcity_alerts(panel: pd.DataFrame, threshold: float = 0.8) -> pd.DataFrame:
    required_cols = {"alpha_residual_load_rank", "alpha_momentum_6h", "timestamp"}
    missing = required_cols - set(panel.columns)
    if missing:
        raise ValueError(f"Missing required columns: {missing}")
    if panel.empty:
        return panel.copy()
    ...
```
```

---

## INFO Issues (114)

### [INFO] Missing module-level docstring

**File**: `src/quant_alpha/backtest/alpha_decay.py:1-121`
**Category**: Convention
**Confidence**: 80%

The module lacks a docstring explaining its purpose and the functions it provides.

**Suggestion**:
```
Add a module docstring: `"""Alpha decay analysis: compute IC decay curves and walk-forward IC for factor evaluation."""`
```

---

### [INFO] Magic numbers in _DECAY_HORIZONS

**File**: `src/quant_alpha/backtest/alpha_decay.py:9`
**Category**: Readability
**Confidence**: 80%

The values in `_DECAY_HORIZONS = [1, 3, 5, 10, 22, 44]` appear to represent trading days (1 day, 1 week, ~2 weeks, 1 month, 2 months) but this is not documented. Similarly for energy horizons `[1, 3, 6, 12, 24, 48]` which appear to be hours.

**Suggestion**:
```
Add comments: `_DECAY_HORIZONS = [1, 3, 5, 10, 22, 44]  # trading days: 1d, ~1w, ~2w, ~1m, ~2m` and document the energy horizons similarly.
```

---

### [INFO] GroupBy apply may be slow for large panels

**File**: `src/quant_alpha/backtest/alpha_decay.py:115-121`
**Category**: Performance
**Confidence**: 70%

Using `groupby().apply()` with a Python lambda for daily rank IC calculation is slow for large datasets. Vectorized alternatives using `groupby().rank()` and `groupby().corr()` would be significantly faster.

**Suggestion**:
```
Consider a vectorized approach: `grouped = panel.groupby(date_col); ranked_alpha = grouped[alpha_col].rank(); ranked_fwd = grouped['forward_return'].rank(); return panel.groupby(date_col).apply(lambda g: g[alpha_col].rank().corr(g['forward_return'].rank()))`
```

---

### [INFO] Missing module-level docstring

**File**: `src/quant_alpha/backtest/diagnostics.py:1-186`
**Category**: Convention
**Confidence**: 90%

The file has no module-level docstring explaining the purpose of the diagnostics module. For a quantitative finance library, documenting the module's role in backtest analysis is important.

**Suggestion**:
```
Add module docstring:
```python
"""Backtest diagnostics and alpha evaluation utilities.

Provides in-sample/out-of-sample splitting, information coefficient analysis,
turnover calculation, and alpha consistency scoring for factor research.
"""
```
```

---

### [INFO] Minimum sample size of 3 is undocumented

**File**: `src/quant_alpha/backtest/diagnostics.py:21`
**Category**: Readability
**Confidence**: 70%

The `daily_rank_ic` function requires at least 3 observations per day to compute correlation (line 21). This threshold is a magic number with no explanation of why 3 was chosen or what the statistical implications are.

**Suggestion**:
```
Extract as a named constant and document:
```python
_MIN_OBS_FOR_CORRELATION = 3  # Minimum observations for rank correlation stability
```
```

---

### [INFO] alpha_turnover recalculates quantiles per iteration

**File**: `src/quant_alpha/backtest/diagnostics.py:60-72`
**Category**: Performance
**Confidence**: 60%

For each alpha column, the function performs two `groupby().transform(lambda s: s.quantile(...))` operations. If there are many alpha columns, this could be slow. The quantile computation inside a lambda is not vectorized.

**Suggestion**:
```
Consider batch processing or documenting that this is expected to be slow for large alpha sets. No immediate fix needed but worth noting for future optimization.
```

---

### [INFO] daily_turnover mean on first NaN diff

**File**: `src/quant_alpha/backtest/diagnostics.py:68`
**Category**: Potential Bug
**Confidence**: 60%

Line 68 computes `wide.diff().abs().sum(axis=1)`. The first row after `diff()` will be all NaN, and `abs().sum()` on NaN rows produces 0.0. This means the first day's turnover is silently zeroed out, slightly underestimating mean turnover.

**Suggestion**:
```
Drop the first row or document the behavior:
```python
daily_turnover = wide.diff().iloc[1:].abs().sum(axis=1)
```
```

---

### [INFO] Potential empty usable list if alpha_cols is empty

**File**: `src/quant_alpha/backtest/diagnostics.py:115-117`
**Category**: Potential Bug
**Confidence**: 60%

If `alpha_cols` is empty, `usable` will be empty after list comprehension, then `alpha_cols[:2]` will also be empty. Downstream code using `usable` (e.g., in `build_orthogonal_composite`) may produce unexpected results with an empty list.

**Suggestion**:
```
Add a guard for empty input:
```python
if not alpha_cols:
    return []
```
```

---

### [INFO] Missing docstrings for private scoring functions

**File**: `src/quant_alpha/backtest/diagnostics.py:150-158`
**Category**: Convention
**Confidence**: 80%

The `_consistency_score` and `_robustness_score` functions have no docstrings. The scoring logic uses magic numbers (0.6, 0.4, 0.4, 0.4, 0.2, 252, 0.1) that should be documented or made configurable.

**Suggestion**:
```
Add docstrings explaining the scoring methodology:
```python
def _consistency_score(row: dict[str, float | str | bool]) -> float:
    """Score alpha consistency between IS and OOS.
    
    Weights: 60% sign consistency, 40% magnitude retention.
    Returns 0.0 if either IC is NaN.
    """
```
```

---

### [INFO] Unnecessary __future__ import

**File**: `src/quant_alpha/backtest/long_short.py:1`
**Category**: Code Style
**Confidence**: 60%

The `from __future__ import annotations` import is used, but the file uses `list[pd.DataFrame]` (PEP 604 style) and `dict[str, float]` which are native in Python 3.9+. If the project targets Python 3.9+, this import is redundant.

**Suggestion**:
```
Remove the import if the project targets Python 3.9+, or keep it if supporting Python 3.7-3.8.
```

---

### [INFO] Missing docstring for _daily_weights function

**File**: `src/quant_alpha/backtest/long_short.py:10`
**Category**: Readability
**Confidence**: 85%

The _daily_weights helper function lacks a docstring explaining its purpose, parameters, and return value.

**Suggestion**:
```
Add a docstring:
```python
def _daily_weights(panel: pd.DataFrame, alpha_col: str, cfg: BacktestConfig) -> pd.DataFrame:
    """Compute daily long/short weights based on alpha quantiles.

    Args:
        panel: DataFrame with alpha signals and forward returns.
        alpha_col: Column name containing alpha scores.
        cfg: Backtest configuration with quantile thresholds.

    Returns:
        DataFrame with date, symbol, weight, and forward_return columns.
    """
```
```

---

### [INFO] Repeated DataFrame copying in groupby loop

**File**: `src/quant_alpha/backtest/long_short.py:23`
**Category**: Performance
**Confidence**: 65%

`day.copy()` is called at line 13, and then `longs` and `shorts` are also copied again via `.copy()`. This creates unnecessary memory overhead when iterating over many dates.

**Suggestion**:
```
Consider removing one of the copies, as the `.copy()` on `longs`/`shorts` after the filter already decouples them from the original. The initial `day.copy()` may be unnecessary:
```python
# Remove day = day.copy() and use day directly
# Keep .copy() on longs and shorts only
```
```

---

### [INFO] Missing docstring for run_long_short_backtest function

**File**: `src/quant_alpha/backtest/long_short.py:29`
**Category**: Readability
**Confidence**: 85%

The public function run_long_short_backtest lacks a docstring explaining its purpose, parameters, and return value.

**Suggestion**:
```
Add a docstring:
```python
def run_long_short_backtest(...):
    """Run a long-short backtest and compute performance metrics.

    Args:
        factor_panel: DataFrame with alpha signals, forward returns, and metadata.
        cfg: Backtest configuration.
        alpha_col: Column name for alpha composite scores.

    Returns:
        Tuple of (daily returns DataFrame, performance metrics dict).
    """
```
```

---

### [INFO] Observations metric type mismatch

**File**: `src/quant_alpha/backtest/long_short.py:78`
**Category**: Convention
**Confidence**: 60%

The `observations` metric is cast to `float(len(daily))`, but an observation count is inherently an integer. This is inconsistent with the natural type and may cause confusion when consumed downstream.

**Suggestion**:
```
Use integer type instead:
```python
"observations": len(daily),
```
```

---

### [INFO] Module has minimal content

**File**: `src/quant_alpha/batch/__init__.py:1`
**Category**: Code Style
**Confidence**: 60%

This __init__.py file only contains a docstring and no actual code or imports. While this is acceptable for package initialization, it may indicate an empty or incomplete module.

**Suggestion**:
```
Consider adding necessary imports or initialization code if this module should have functionality.
```

---

### [INFO] Consider adding logging for monitoring

**File**: `src/quant_alpha/batch/spark_energy_features.py:45`
**Category**: Readability
**Confidence**: 80%

The function performs significant computation but provides no logging or progress indication. For batch processing jobs, logging is important for monitoring and debugging.

**Suggestion**:
```
Add logging:
```python
import logging

logger = logging.getLogger(__name__)

def compute_energy_features(input_path: str, output_path: str) -> None:
    logger.info(f"Starting energy feature computation")
    logger.info(f"Input: {input_path}")
    # ... after computation ...
    logger.info(f"Features written to {output_path}")
```
```

---

### [INFO] Hardcoded relative path traversal

**File**: `src/quant_alpha/batch/spark_energy_features.py:50-55`
**Category**: Architecture
**Confidence**: 70%

The __main__ block uses parents[3] which assumes a specific directory structure (3 levels up from the file). This is fragile and will break if the file is moved or the project structure changes.

**Suggestion**:
```
Use environment variables or configuration for paths:
```python
if __name__ == "__main__":
    import os
    
    # Allow paths to be configured via environment variables
    input_path = os.environ.get("ENERGY_INPUT_PATH", "data/raw/power_market.parquet")
    output_path = os.environ.get("ENERGY_OUTPUT_PATH", "data/processed/power_market_spark_features.parquet")
    
    compute_energy_features(input_path, output_path)
```
```

---

### [INFO] Module-level docstring is missing

**File**: `src/quant_alpha/cli.py:1`
**Category**: Convention
**Confidence**: 70%

The module has no docstring explaining its purpose, even though it's the main CLI entry point. This is a minor convention issue but helpful for discoverability.

**Suggestion**:
```
Add a module docstring:
```python
"""CLI entry point for quant-alpha data engineering pipelines."""
from __future__ import annotations
```
```

---

### [INFO] Unused imports at module level

**File**: `src/quant_alpha/cli.py:7-10`
**Category**: Code Style
**Confidence**: 70%

Several imports are only used inside functions (run_dlt_energy_pipeline, run_dlt_equity_pipeline, EntsoeError) but are imported at the top level. While this isn't a bug, the delayed imports inside commands suggest a desire for lazy loading — the top-level imports contradict this pattern and increase startup time.

**Suggestion**:
```
Either import all at the top level (removing the local imports inside commands) or move all heavy imports to be local to keep a consistent lazy-loading pattern. Top-level: keep lightweight ones (typer, Path). Local: keep heavier pipeline/ingestion modules.
```

---

### [INFO] Inconsistent config path default between commands

**File**: `src/quant_alpha/cli.py:85-99`
**Category**: Code Style
**Confidence**: 70%

The `energy_run_command` defaults to `configs/second_foundation_project.yaml` while `dlt_energy_command` hardcodes the same path internally. Other commands default to `configs/project.yaml`. This inconsistency can confuse users about which config a command uses.

**Suggestion**:
```
Standardize config handling: either expose config as a parameter in all commands, or document the default paths in help text.
```

---

### [INFO] No validation of date format inputs

**File**: `src/quant_alpha/cli.py:94`
**Category**: Potential Bug
**Confidence**: 80%

The `start` and `end` parameters in `dlt_energy_command` are plain strings. There is no validation that they are valid ISO date formats (YYYY-MM-DD). Invalid dates will propagate as opaque errors from the pipeline.

**Suggestion**:
```
Add validation or use a date parsing library:
```python
from datetime import date

start: date = typer.Option(..., formats=["%Y-%m-%d"])
```
Or validate manually:
```python
from datetime import datetime
try:
    datetime.strptime(start, "%Y-%m-%d")
except ValueError:
    typer.echo(f"Invalid start date: {start}. Expected YYYY-MM-DD.", err=True)
    raise typer.Exit(1)
```
```

---

### [INFO] No progress feedback during long-running bruin_run

**File**: `src/quant_alpha/cli.py:134-161`
**Category**: Readability
**Confidence**: 60%

The `bruin_run_command` runs a graph execution which could take a long time but only prints a single status line before starting. There's no indication of progress during execution.

**Suggestion**:
```
Consider using `typer.echo` within the graph.run callback, or use a progress bar via rich/typer for long-running operations.
```

---

### [INFO] Missing module docstring

**File**: `src/quant_alpha/config.py:1`
**Category**: Convention
**Confidence**: 85%

The config module lacks a module-level docstring explaining its purpose, which reduces discoverability for new developers.

**Suggestion**:
```
Add a docstring:
```python
"""Configuration models and loaders for the quant-alpha project.

Defines Pydantic models for backtest, data source, and project settings,
with utilities to load them from YAML files."""
```
```

---

### [INFO] No validation ranges on numeric config values

**File**: `src/quant_alpha/config.py:15-18`
**Category**: Potential Bug
**Confidence**: 85%

BacktestConfig fields like top_quantile and bottom_quantile have no range validation. Values like top_quantile=1.5 or bottom_quantile=-0.1 would be accepted but are semantically invalid and would cause incorrect backtest results.

**Suggestion**:
```
Add Pydantic validators with constraints:
```python
from pydantic import field_validator

class BacktestConfig(BaseModel):
    top_quantile: float = Field(default=0.2, gt=0.0, lt=1.0)
    bottom_quantile: float = Field(default=0.2, gt=0.0, lt=1.0)
    forward_return_days: int = Field(default=5, gt=0)
    transaction_cost_bps: float = Field(default=5.0, ge=0.0)
```
```

---

### [INFO] No validation of write_disposition values

**File**: `src/quant_alpha/config.py:44`
**Category**: Potential Bug
**Confidence**: 80%

CloudExportConfig.write_disposition accepts any string, but BigQuery only accepts 'WRITE_TRUNCATE', 'WRITE_APPEND', or 'WRITE_EMPTY'. Invalid values will cause runtime errors during cloud export.

**Suggestion**:
```
Use a Literal type to constrain valid values:
```python
from typing import Literal

class CloudExportConfig(BaseModel):
    write_disposition: Literal['WRITE_TRUNCATE', 'WRITE_APPEND', 'WRITE_EMPTY'] = 'WRITE_TRUNCATE'
```
```

---

### [INFO] Universe symbols list accepts empty list

**File**: `src/quant_alpha/config.py:62-64`
**Category**: Potential Bug
**Confidence**: 75%

Universe.symbols has no min_items constraint, so an empty symbols list would be valid but likely cause downstream issues when trying to fetch data for zero symbols.

**Suggestion**:
```
Add a minimum length constraint:
```python
from pydantic import Field

class Universe(BaseModel):
    symbols: list[str] = Field(min_length=1)
```
```

---

### [INFO] Missing function docstrings

**File**: `src/quant_alpha/config.py:86-88`
**Category**: Convention
**Confidence**: 90%

Public functions load_yaml, resolve_path, load_project_config, load_universe, and ensure_project_dirs lack docstrings.

**Suggestion**:
```
Add docstrings to all public functions, e.g.:
```python
def load_project_config(path: Path, root: Path | None = None) -> ProjectConfig:
    """Load and resolve a project configuration from a YAML file.
    
    Args:
        path: Relative or absolute path to the config YAML.
        root: Base directory for resolving relative paths. Defaults to cwd.
    
    Returns:
        Fully resolved ProjectConfig instance.
    """
```
```

---

### [INFO] Side effect mutates input config object

**File**: `src/quant_alpha/config.py:90`
**Category**: Readability
**Confidence**: 65%

load_project_config mutates the cfg object in-place (changing path fields and end_date) after construction, mixing construction with resolution logic. This makes the function's behavior non-obvious.

**Suggestion**:
```
Consider making path resolution a separate method or using a class method that handles both parsing and resolution:
```python
class ProjectConfig(BaseModel):
    # ... fields ...
    
    def with_resolved_paths(self, root: Path) -> 'ProjectConfig':
        return self.model_copy(update={
            'raw_dir': resolve_path(root, self.raw_dir),
            # etc.
        })
```
```

---

### [INFO] Missing module-level exports or imports

**File**: `src/quant_alpha/features/__init__.py:1`
**Category**: Convention
**Confidence**: 70%

This __init__.py file contains only a docstring and no actual code, imports, or exports. For a features module, this suggests the module may be incomplete or not yet implemented.

**Suggestion**:
```
Add the appropriate imports and __all__ definition to expose the public API: """Alpha factor calculations."""

from .factor1 import Factor1
from .factor2 import Factor2

__all__ = ['Factor1', 'Factor2']
```

---

### [INFO] Missing docstrings for utility functions

**File**: `src/quant_alpha/features/alpha_factors.py:13-22`
**Category**: Readability
**Confidence**: 85%

_rolling_zscore and _breakout_position have no docstrings explaining their purpose, parameters, or return values. These are non-trivial financial computations that would benefit from documentation.

**Suggestion**:
```
Add docstrings:
```python
def _rolling_zscore(series: pd.Series, window: int) -> pd.Series:
    """Compute rolling z-score normalization of a time series.

    Args:
        series: Input time series.
        window: Rolling window size in periods.
    Returns:
        Z-score normalized series.
    """
```
```

---

### [INFO] Missing docstring for add_alpha_factors

**File**: `src/quant_alpha/features/alpha_factors.py:25-26`
**Category**: Code Style
**Confidence**: 90%

The add_alpha_factors function is a public function with complex logic involving multiple transformations, grouping, and ranking. It lacks a docstring explaining its purpose, parameters, return value, and side effects (e.g., that it modifies column names in the returned DataFrame).

**Suggestion**:
```
Add a comprehensive docstring:
```python
def add_alpha_factors(prices: pd.DataFrame, cfg: ProjectConfig) -> pd.DataFrame:
    """Compute alpha factors and composite score for equity universe.

    Args:
        prices: DataFrame with columns ['symbol', 'date', 'adj_close', 'close'].
        cfg: Project configuration containing backtest parameters.

    Returns:
        DataFrame with original columns plus alpha factors, ranks,
        and alpha_composite score.
    """
```
```

---

### [INFO] adj_close fallback to close without warning

**File**: `src/quant_alpha/features/alpha_factors.py:34`
**Category**: Potential Bug
**Confidence**: 65%

Line 34 fills NaN adj_close values with close prices. While this is a reasonable fallback, it silently treats unadjusted and adjusted prices as interchangeable, which could introduce subtle biases in return calculations for stocks with splits/dividends.

**Suggestion**:
```
Consider logging a warning when fallback occurs:
```python
n_filled = df['adj_close'].isna().sum()
if n_filled > 0:
    import warnings
    warnings.warn(f"Filled {n_filled} NaN adj_close values with close prices")
df['adj_close'] = df['adj_close'].fillna(df['close'])
```
```

---

### [INFO] Missing docstring for EnergyAlphaDefinition class

**File**: `src/quant_alpha/features/energy_alpha.py:10-17`
**Category**: Readability
**Confidence**: 90%

The EnergyAlphaDefinition dataclass lacks a docstring explaining its purpose and the meaning of its fields. While the field names are descriptive, a docstring would provide context about the class's role in the system.

**Suggestion**:
```
Add a class docstring:
```python
@dataclass(frozen=True)
class EnergyAlphaDefinition:
    """Definition of an energy market alpha factor.
    
    Attributes:
        name: Unique identifier for the alpha
        expression: Mathematical expression defining the alpha calculation
        family: Category or family the alpha belongs to
        hypothesis: Explanation of the economic rationale behind the alpha
    """
    name: str
    expression: str
    family: str
    hypothesis: str
```
```

---

### [INFO] Missing docstring for energy_alpha_registry_frame function

**File**: `src/quant_alpha/features/energy_alpha.py:68-71`
**Category**: Readability
**Confidence**: 90%

The energy_alpha_registry_frame function lacks a docstring explaining what it returns and its purpose in the system.

**Suggestion**:
```
Add a docstring:
```python
def energy_alpha_registry_frame() -> pd.DataFrame:
    """Convert the energy alpha registry to a DataFrame.
    
    Returns:
        DataFrame with columns: alpha_name, expression, family, hypothesis, expected_direction
    """
    return pd.DataFrame(
        [
            {
                "alpha_name": alpha.name,
                "expression": alpha.expression,
                "family": alpha.family,
                "hypothesis": alpha.hypothesis,
                "expected_direction": 1,
            }
            for alpha in ENERGY_ALPHA_REGISTRY
        ]
    )
```
```

---

### [INFO] Hardcoded expected_direction value

**File**: `src/quant_alpha/features/energy_alpha.py:68-71`
**Category**: Potential Bug
**Confidence**: 75%

The energy_alpha_registry_frame function hardcodes expected_direction to 1 for all alphas. However, some alphas (like solar penetration and wind forecast error) have negative signs in their expressions, suggesting they might have negative expected directions. This inconsistency could lead to incorrect signal interpretation.

**Suggestion**:
```
Consider adding expected_direction to the EnergyAlphaDefinition or calculating it based on the expression:
```python
# Add to EnergyAlphaDefinition
class EnergyAlphaDefinition:
    name: str
    expression: str
    family: str
    hypothesis: str
    expected_direction: int = 1  # 1 for positive, -1 for negative

# Update registry definitions
EnergyAlphaDefinition(
    name="alpha_energy_solar_penetration",
    expression="-zscore(solar_forecast / load_forecast, 168)",
    family="renewables",
    hypothesis="High solar penetration...",
    expected_direction=-1
)
```
```

---

### [INFO] Missing docstring for add_energy_alpha_features function

**File**: `src/quant_alpha/features/energy_alpha.py:75-82`
**Category**: Readability
**Confidence**: 90%

The main function add_energy_alpha_features lacks a docstring explaining its purpose, parameters, return value, and side effects. This makes it harder for other developers to understand the function's behavior.

**Suggestion**:
```
Add a comprehensive docstring:
```python
def add_energy_alpha_features(frame: pd.DataFrame) -> pd.DataFrame:
    """Add energy market alpha features to a DataFrame.
    
    Calculates various energy market alpha signals including residual load shock,
    wind forecast error, imbalance premium, cross-market spread, demand surprise,
    solar penetration, price momentum, and gas spark spread.
    
    Args:
        frame: Input DataFrame with columns for market, timestamp, and energy data
              (spot_price, load_forecast, actual_load, wind_forecast, solar_forecast,
              residual_load, imbalance_price, gas_price)
    
    Returns:
        DataFrame with added alpha feature columns
    
    Raises:
        KeyError: If required columns are missing
    """
```
```

---

### [INFO] Missing type hints for complex parameters

**File**: `src/quant_alpha/features/energy_alpha.py:75-82`
**Category**: Convention
**Confidence**: 60%

The function signature only has basic type hints for the input and output DataFrames, but doesn't specify the expected column names or data types within the DataFrame. This could lead to runtime errors if the DataFrame structure doesn't match expectations.

**Suggestion**:
```
Consider adding a TypedDict or DataFrame type hint with expected columns:
```python
from typing import TypedDict

class EnergyDataFrame(TypedDict):
    market: str
    timestamp: datetime
    spot_price: float
    # ... other required columns

def add_energy_alpha_features(frame: pd.DataFrame) -> pd.DataFrame:
    """..."""
```
```

---

### [INFO] Multiple rolling window calculations in _zscore

**File**: `src/quant_alpha/features/energy_alpha.py:84-95`
**Category**: Performance
**Confidence**: 60%

The _zscore function calculates both mean and std using separate rolling window operations. For large datasets, this could be optimized by calculating both statistics in a single pass or using more efficient algorithms.

**Suggestion**:
```
Consider using scipy.stats.zscore with a rolling window or implementing a more efficient single-pass algorithm:
```python
from scipy.stats import zscore

def _zscore(series: pd.Series, window: int) -> pd.Series:
    # Use a custom rolling zscore implementation
    def rolling_zscore(x):
        if len(x) < window:
            return pd.Series(np.nan, index=x.index)
        return (x - x.rolling(window).mean()) / x.rolling(window).std()
    return rolling_zscore(series)
```
```

---

### [INFO] Magic number 2.0 in gas spark spread calculation

**File**: `src/quant_alpha/features/energy_alpha.py:126-129`
**Category**: Readability
**Confidence**: 80%

The heat rate conversion factor of 2.0 is used as a magic number in the gas spark spread calculation. This should be documented or made configurable as it represents a simplified thermal equivalent that may vary by market or technology.

**Suggestion**:
```
Add a comment explaining the magic number or make it a configurable parameter:
```python
# Simplified thermal equivalent: 2.0 MMBtu/MWh (typical gas turbine heat rate)
HEAT_RATE = 2.0
df['_gas_spark'] = df['spot_price'] - df['gas_price'] * HEAT_RATE
```
```

---

### [INFO] Missing module docstring

**File**: `src/quant_alpha/features/registry.py:1-153`
**Category**: Code Style
**Confidence**: 90%

The module lacks a docstring explaining its purpose, the structure of the alpha registry, and how to use/extend it. This is a new file so establishing documentation early is valuable.

**Suggestion**:
```
Add a module docstring at the top:
```python
"""Feature registry for equity alpha definitions.

Contains cross-sectional and time-series utility functions for
alpha signal computation, plus a factory function that produces
the canonical set of alpha definitions.
"""
```
```

---

### [INFO] ts_rank uses lambda with rolling rank

**File**: `src/quant_alpha/features/registry.py:27`
**Category**: Performance
**Confidence**: 60%

The ts_rank function applies rolling().rank() via groupby transform with a lambda. Rolling rank operations on large datasets can be very slow. Consider whether this operation is called frequently and if vectorized alternatives exist.

---

### [INFO] Repeated groupby operations in utility functions

**File**: `src/quant_alpha/features/registry.py:44-47`
**Category**: Performance
**Confidence**: 60%

Each utility function (ts_std, ts_mean, etc.) independently calls groupby(level=1). When these are chained in alpha computations (e.g., ts_std(ts_mean(series, 20), 20)), multiple redundant groupby operations are performed on the same data.

**Suggestion**:
```
Consider caching the groupby object or implementing a context that holds the groupby state for batch computations. Alternatively, document that callers should pre-sort data for optimal performance.
```

---

### [INFO] Missing docstrings for module-level functions

**File**: `src/quant_alpha/features/registry.py:81-93`
**Category**: Code Style
**Confidence**: 90%

The utility functions cs_rank, ts_rank, delta, delay, ts_corr, ts_std, ts_mean, and safe_divide all lack docstrings. These are core building blocks for alpha computation and would benefit from documentation explaining their behavior, expected input format (e.g., MultiIndex with specific levels), and return values.

**Suggestion**:
```
Add docstrings:
```python
def cs_rank(series: pd.Series) -> pd.Series:
    """Cross-sectional rank normalized to [-0.5, 0.5].
    
    Assumes series has a MultiIndex with level=0 being the date
    (cross-sectional dimension).
    """
    return series.groupby(level=0).rank(pct=True) - 0.5
```
```

---

### [INFO] Long lambda expression reduces readability

**File**: `src/quant_alpha/features/registry.py:141-153`
**Category**: Readability
**Confidence**: 75%

The compute lambda for alpha_wq_009_volume_weighted_return spans multiple lines and contains nested groupby/transform/rolling calls, making it difficult to read and debug. Complex alpha computations would be clearer as named functions.

**Suggestion**:
```
Extract to a named function:
```python
def _volume_weighted_return(x: pd.DataFrame, window: int = 10) -> pd.Series:
    vw_sum = (x["ret_1d"] * x["volume"]).groupby(level=1).transform(
        lambda s: s.rolling(window, min_periods=5).sum()
    )
    vol_sum = x["volume"].groupby(level=1).transform(
        lambda s: s.rolling(window, min_periods=5).sum()
    )
    return cs_rank(safe_divide(vw_sum, vol_sum))
```
```

---

### [INFO] Module docstring references future features

**File**: `src/quant_alpha/ingestion/dlt_energy.py:1-10`
**Category**: Readability
**Confidence**: 90%

The docstring mentions 'BigQuery as interchangeable destinations' but the implementation only supports DuckDB. This could mislead users about current capabilities.

**Suggestion**:
```
Update docstring to reflect current implementation or mark BigQuery as planned:
```python
"""...
  - DuckDB as destination (BigQuery support planned)
"""
```
```

---

### [INFO] Hardcoded default symbols in function body

**File**: `src/quant_alpha/ingestion/dlt_equity.py:80-82`
**Category**: Readability
**Confidence**: 65%

The default Universe is hardcoded with specific ticker symbols (AAPL, MSFT, GOOGL, AMZN, META). This creates a hidden coupling between the pipeline code and the trading universe. If the default universe changes, this code must be updated manually.

**Suggestion**:
```
Move the default to a constant at module level or to the config:
```python
DEFAULT_UNIVERSE = Universe(name="demo", symbols=["AAPL", "MSFT", "GOOGL", "AMZN", "META"])
```
Or define a classmethod on Universe as a default factory.
```

---

### [INFO] Return dict lacks load_info details

**File**: `src/quant_alpha/ingestion/dlt_equity.py:89-96`
**Category**: Readability
**Confidence**: 60%

The returned dictionary only includes len(load_info.load_packages) but omits load status, errors, or timing information that would be useful for monitoring and debugging pipeline runs.

**Suggestion**:
```
Include more useful metrics:
```python
return {
    "pipeline": pipeline.pipeline_name,
    "dataset": pipeline.dataset_name,
    "duckdb_path": str(duckdb_path),
    "load_packages": len(load_info.load_packages),
    "schema": pipeline.default_schema_name,
    "has_failed_jobs": load_info.has_failed_jobs,
}
```
```

---

### [INFO] Hardcoded relative path in __main__ block

**File**: `src/quant_alpha/ingestion/dlt_equity.py:107`
**Category**: Code Style
**Confidence**: 60%

The __main__ block uses parents[3] to navigate three directories up from the file. This is fragile and will break if the file is moved within the project structure.

**Suggestion**:
```
Consider using a project root discovery utility or environment variable:
```python
root = Path(os.environ.get("PROJECT_ROOT", Path(__file__).resolve().parents[3]))
```
Or use a package like `importlib.resources` or a configuration file.
```

---

### [INFO] Missing module docstring

**File**: `src/quant_alpha/ingestion/energy.py:1-60`
**Category**: Convention
**Confidence**: 90%

The module lacks a docstring explaining its purpose, which is synthetic power market data generation for energy trading analysis.

**Suggestion**:
```
Add a module docstring at the top:
"""Synthetic power market data generator for energy trading analysis.

Generates realistic synthetic data including spot prices, load forecasts,
wind/solar generation, imbalance prices, and gas prices.
"""
```

---

### [INFO] Unused import: __future__ annotations

**File**: `src/quant_alpha/ingestion/energy.py:1`
**Category**: Code Style
**Confidence**: 60%

from __future__ import annotations is imported but the type hints used (list[str]) already work in Python 3.9+. This import is a no-op in Python 3.10+ and only needed for Python 3.9 compatibility with lowercase generics.

**Suggestion**:
```
If targeting Python 3.10+, this import can be removed. If targeting 3.9, it's correctly used. Add a comment or ensure consistent Python version targeting.
```

---

### [INFO] Missing docstring for _seed function

**File**: `src/quant_alpha/ingestion/energy.py:9-10`
**Category**: Convention
**Confidence**: 85%

The _seed helper function lacks a docstring explaining its purpose of creating a deterministic seed from a market name.

**Suggestion**:
```
Add a docstring:
def _seed(name: str) -> int:
    """Generate deterministic RNG seed from market name using SHA-256."""
```

---

### [INFO] SHA-256 truncated for seed - not cryptographically concerning

**File**: `src/quant_alpha/ingestion/energy.py:10`
**Category**: Security
**Confidence**: 85%

SHA-256 is used correctly for deterministic seeding (not for security). The truncation to 8 hex chars (32 bits) is intentional for NumPy seed compatibility. This is acceptable usage.

---

### [INFO] Missing docstring for main function

**File**: `src/quant_alpha/ingestion/energy.py:13-18`
**Category**: Convention
**Confidence**: 90%

The generate_synthetic_power_market function lacks a docstring explaining parameters, return value, and the data generation methodology.

**Suggestion**:
```
Add a comprehensive docstring:
"""Generate synthetic power market data.

Args:
    markets: List of market identifiers.
    start: Start timestamp string.
    end: End timestamp string.
    freq: Pandas frequency string (default 'h' for hourly).

Returns:
    DataFrame with columns: timestamp, market, spot_price, load_forecast,
    actual_load, wind_forecast, solar_forecast, residual_load,
    imbalance_price, gas_price.
"""
```

---

### [INFO] Magic numbers in synthetic model parameters

**File**: `src/quant_alpha/ingestion/energy.py:26-28`
**Category**: Readability
**Confidence**: 70%

The model parameters (55, 12, 7, 24, 18, 8, 365, 14, 6, 12, etc.) are hardcoded magic numbers. While acceptable for synthetic data generation, they would benefit from named constants or comments explaining the domain meaning.

**Suggestion**:
```
Extract named constants:
BASE_LOAD_MW = 55       # Base load in MW
LOAD_AMPLITUDE = 12     # Load seasonal variation
LOAD_PEAK_HOUR = 7      # Hour of peak load

Or add inline comments explaining each parameter's domain meaning.
```

---

### [INFO] Inconsistent divisor for solar formula

**File**: `src/quant_alpha/ingestion/energy.py:29`
**Category**: Readability
**Confidence**: 65%

Solar uses /12 * np.pi but the range for valid solar hours (6-18) spans 12 hours. However, at hour=18, sin((18-6)/12 * pi) = sin(pi) = 0, which is correct. But at hour=6, sin(0) = 0, also correct. The formula is actually fine but the 2*pi scaling is missing (it uses /12 * pi instead of /12 * 2*pi like other formulas), making solar peak at noon (hour=12) with sin(pi/2) = 1, which is correct.

**Suggestion**:
```
Add a clarifying comment:
# Solar peaks at noon (hour=12), zero at 6am and 6pm
```

---

### [INFO] Missing module docstring

**File**: `src/quant_alpha/ingestion/yahoo.py:1`
**Category**: Convention
**Confidence**: 90%

The module lacks a docstring explaining its purpose, which is to fetch and generate price data for the quant_alpha project.

**Suggestion**:
```
Add a module docstring at the top:
```python
"""Price data ingestion from Yahoo Finance with synthetic fallback."""
```
```

---

### [INFO] Missing docstring for _stable_seed function

**File**: `src/quant_alpha/ingestion/yahoo.py:16-18`
**Category**: Convention
**Confidence**: 85%

The helper function lacks a docstring explaining its purpose of creating a deterministic seed from a symbol string.

**Suggestion**:
```
Add docstring:
```python
def _stable_seed(symbol: str) -> int:
    """Create a deterministic integer seed from a stock symbol string."""
```
```

---

### [INFO] Missing docstring for generate_synthetic_prices

**File**: `src/quant_alpha/ingestion/yahoo.py:21`
**Category**: Convention
**Confidence**: 90%

The public function lacks a docstring explaining its purpose, parameters, and return value.

**Suggestion**:
```
Add docstring:
```python
def generate_synthetic_prices(cfg: ProjectConfig, universe: Universe) -> pd.DataFrame:
    """Generate synthetic OHLCV price data using geometric Brownian motion.

    Args:
        cfg: Project configuration with start/end dates.
        universe: Universe containing stock symbols.

    Returns:
        DataFrame with columns matching PRICE_COLUMNS.
    """
```
```

---

### [INFO] Missing docstring for _normalize_yfinance_frame

**File**: `src/quant_alpha/ingestion/yahoo.py:56`
**Category**: Convention
**Confidence**: 85%

The helper function lacks a docstring explaining its normalization logic.

---

### [INFO] Missing docstring for fetch_prices

**File**: `src/quant_alpha/ingestion/yahoo.py:85`
**Category**: Convention
**Confidence**: 85%

The main public function lacks a docstring explaining its purpose, parameters, return value, and the offline fallback behavior.

---

### [INFO] Missing input validation on config parameters

**File**: `src/quant_alpha/ingestion/yahoo.py:85`
**Category**: Architecture
**Confidence**: 75%

The fetch_prices and generate_synthetic_prices functions don't validate that cfg.start_date < cfg.end_date or that universe.symbols is non-empty. Passing invalid dates or an empty symbol list would result in confusing downstream errors.

**Suggestion**:
```
Add validation at the start of the function:
```python
if not universe.symbols:
    raise ValueError("Universe must contain at least one symbol")
if cfg.start_date >= cfg.end_date:
    raise ValueError("start_date must be before end_date")
```
```

---

### [INFO] Repeated DuckDB connection opens may cause overhead

**File**: `src/quant_alpha/pipeline.py:1-82`
**Category**: Performance
**Confidence**: 70%

The code calls write_table and write_metrics with cfg.duckdb_path multiple times (13 calls). If each call opens and closes a DuckDB connection independently, this adds unnecessary overhead. A single connection or connection pool would be more efficient.

**Suggestion**:
```
Consider opening the DuckDB connection once and passing it through, or using a context manager:
```python
import duckdb
with duckdb.connect(str(cfg.duckdb_path)) as conn:
    write_table(conn, 'raw_prices', prices)
    write_table(conn, 'factor_panel', factors)
    # ... etc
```
```

---

### [INFO] Missing docstring for _write_parquet helper

**File**: `src/quant_alpha/pipeline.py:14`
**Category**: Readability
**Confidence**: 70%

The _write_parquet private helper function lacks a docstring. While it's simple, documenting its behavior (creates parent dirs, writes without index) helps maintainability.

**Suggestion**:
```
Add a brief docstring:
```python
def _write_parquet(frame: pd.DataFrame, path: Path) -> Path:
    """Write DataFrame to parquet, creating parent directories if needed."""
```
```

---

### [INFO] God function — pipeline does too many things

**File**: `src/quant_alpha/pipeline.py:23-82`
**Category**: Architecture
**Confidence**: 75%

run_pipeline is an 80-line function that handles data fetching, feature engineering, backtesting, diagnostics, decay analysis, turnover analysis, walk-forward analysis, file I/O, and database writes. This makes it hard to test individual steps, hard to resume on failure, and violates single responsibility principle.

**Suggestion**:
```
Break into composable stages, e.g.:
```python
def run_pipeline(config_path: Path, root: Path, offline: bool = False) -> dict[str, object]:
    cfg = load_project_config(config_path, root=root)
    ensure_project_dirs(cfg)
    universe = load_universe(cfg.universe_path)
    prices = _fetch_and_store(cfg, universe, offline)
    factors = _compute_and_store_factors(cfg, prices)
    backtest_results = _run_and_store_backtest(cfg, factors)
    diagnostics_results = _run_and_store_diagnostics(cfg, factors)
    _run_and_store_decay_analysis(cfg, factors)
    return _build_summary(prices, factors, backtest_results, diagnostics_results, cfg)
```
```

---

### [INFO] No validation of loaded config or universe

**File**: `src/quant_alpha/pipeline.py:25-26`
**Category**: Potential Bug
**Confidence**: 80%

After loading the config and universe, there's no validation that critical fields are present or that the universe is non-empty. If load_universe returns an empty list, the entire pipeline will run with no data, producing empty results with no warning.

**Suggestion**:
```
Add basic validation after loading:
```python
universe = load_universe(cfg.universe_path)
if not universe:
    raise ValueError(f'Universe is empty, check {cfg.universe_path}')
```
```

---

### [INFO] No validation of fetched price data

**File**: `src/quant_alpha/pipeline.py:28-30`
**Category**: Potential Bug
**Confidence**: 80%

The prices DataFrame returned by fetch_prices is used immediately for factor computation and storage without checking for emptiness or data quality. If Yahoo Finance returns no data (e.g., all tickers delisted), the pipeline silently produces empty results downstream.

**Suggestion**:
```
Add a check after fetching:
```python
prices = fetch_prices(cfg, universe, offline=offline)
if prices.empty:
    raise ValueError('No price data fetched; check universe and network connectivity')
```
```

---

### [INFO] Missing docstring for _write_parquet

**File**: `src/quant_alpha/pipeline_energy.py:21`
**Category**: Code Style
**Confidence**: 85%

The _write_parquet helper function lacks a docstring.

**Suggestion**:
```
Add docstring:
def _write_parquet(frame: pd.DataFrame, path: Path) -> Path:
    """Write DataFrame to parquet file, creating parent directories if needed."""
```

---

### [INFO] Missing docstring for _load_power_market

**File**: `src/quant_alpha/pipeline_energy.py:28`
**Category**: Code Style
**Confidence**: 90%

The _load_power_market helper function lacks a docstring explaining its purpose and parameters.

**Suggestion**:
```
Add docstring:
def _load_power_market(cfg, markets: list[str], universe: dict[str, object]) -> pd.DataFrame:
    """Load power market data from configured source.
    
    Args:
        cfg: Project configuration object.
        markets: List of market identifiers.
        universe: Universe configuration dictionary.
    
    Returns:
        DataFrame with raw market data.
    """
```

---

### [INFO] Magic number for default markets

**File**: `src/quant_alpha/pipeline_energy.py:60-61`
**Category**: Readability
**Confidence**: 70%

The default markets list ['DE_LU', 'CZ', 'FR'] is a hardcoded magic value. This should be a named constant for better maintainability.

**Suggestion**:
```
Define DEFAULT_ENERGY_MARKETS = ['DE_LU', 'CZ', 'FR'] at module level, then use: markets = universe.get('markets', DEFAULT_ENERGY_MARKETS)
```

---

### [INFO] Repeated energy_alpha_registry_frame() call

**File**: `src/quant_alpha/pipeline_energy.py:83`
**Category**: Performance
**Confidence**: 80%

energy_alpha_registry_frame() is called on line 84 for writing to DuckDB and again on line 111 for cloud_tables dict. This is redundant computation if the function is not cached.

**Suggestion**:
```
Cache the result:
registry_frame = energy_alpha_registry_frame()
write_table(cfg.duckdb_path, 'energy_alpha_registry', registry_frame)
# Then use registry_frame in cloud_tables dict
```

---

### [INFO] Missing docstring for run_energy_pipeline

**File**: `src/quant_alpha/pipeline_energy.py:85`
**Category**: Code Style
**Confidence**: 90%

The main public function run_energy_pipeline lacks a docstring explaining its purpose, parameters, return value, and any side effects.

**Suggestion**:
```
Add docstring:
def run_energy_pipeline(
    config_path: Path,
    root: Path,
    source_override: str | None = None,
) -> dict[str, object]:
    """Run the full energy alpha pipeline.
    
    Args:
        config_path: Path to project configuration file.
        root: Root directory for the project.
        source_override: Optional override for data source.
    
    Returns:
        Dictionary containing paths and metrics.
    """
```

---

### [INFO] Inconsistent date column conversion

**File**: `src/quant_alpha/pipeline_energy.py:97`
**Category**: Readability
**Confidence**: 70%

Line 97 converts 'date' column to string with .astype(str) for turnover calculation, but the 'date' column format is not documented. This could cause issues if date format expectations change.

**Suggestion**:
```
Add a comment explaining why string conversion is needed, or use consistent date formatting:
# Convert to string format expected by alpha_turnover function
energy_turnover_panel['date'] = energy_turnover_panel['date'].dt.strftime('%Y-%m-%d')
```

---

### [INFO] Unused import os

**File**: `src/quant_alpha/platform/bruin_graph.py:1-11`
**Category**: Code Style
**Confidence**: 70%

The `os` module is imported inside the `run()` method (line 158) but is also listed in the import statements at the top of the file according to the provided import list. This is redundant and could be confusing.

**Suggestion**:
```
Remove the redundant import inside the method since os is already imported at module level, or move the import to the top if it's only used in the method.
```

---

### [INFO] Missing docstring for AssetNode dataclass

**File**: `src/quant_alpha/platform/bruin_graph.py:33-44`
**Category**: Readability
**Confidence**: 80%

The `AssetNode` dataclass lacks a docstring explaining what it represents and the purpose of its fields. This makes the code less self-documenting.

**Suggestion**:
```
Add a docstring:
```python
@dataclass
class AssetNode:
    """Represents a single asset in the Bruin pipeline graph.
    
    An asset can be a Python script, DuckDB table, or BigQuery table
    with dependencies on other assets.
    """
    name: str
    ...
```
```

---

### [INFO] Using list instead of set for visited nodes

**File**: `src/quant_alpha/platform/bruin_graph.py:106`
**Category**: Performance
**Confidence**: 90%

In the `upstream()` method, `visited` is a list which provides O(n) lookup time for `if dep not in visited`. Using a set would provide O(1) lookup time.

**Suggestion**:
```
Change to use a set:
```python
visited: set[str] = set()
...
if dep not in visited:
    visited.add(dep)
...
return list(visited)
```
```

---

### [INFO] Hardcoded separator length in reports

**File**: `src/quant_alpha/platform/bruin_graph.py:212-226`
**Category**: Readability
**Confidence**: 60%

The `lineage_report()` and `status_report()` methods use hardcoded separator lengths ("=" * 50). This could be made configurable or dynamic based on content length.

**Suggestion**:
```
Consider making the separator length dynamic or at least defining it as a class constant:
```python
class AssetGraph:
    REPORT_WIDTH = 50
    ...
    lines = ["Asset Lineage Graph", "=" * self.REPORT_WIDTH]
```
```

---

### [INFO] Module missing docstring

**File**: `src/quant_alpha/platform/contracts.py:1-70`
**Category**: Readability
**Confidence**: 85%

The module has no docstring explaining its purpose, which is to define dataset contracts and catalogs for equity and energy domains.

**Suggestion**:
```
Add a module-level docstring:
```python
"""Dataset contracts defining metadata, ownership, and freshness expectations.

This module provides DatasetContract definitions for data governance,
schema validation, and data quality monitoring across equity and energy domains.
"""
```

---

### [INFO] Missing class docstring for DatasetContract

**File**: `src/quant_alpha/platform/contracts.py:7-12`
**Category**: Convention
**Confidence**: 85%

The DatasetContract dataclass lacks a docstring explaining its purpose and field semantics (e.g., what format grain expects, freshness_expectation valid values).

**Suggestion**:
```
Add a docstring:
```python
@dataclass(frozen=True)
class DatasetContract:
    """Contract defining metadata and expectations for a dataset.

    Attributes:
        name: Unique dataset identifier.
        grain: Data granularity (e.g., 'daily x symbol').
        owner: Team responsible for dataset maintenance.
        primary_keys: Tuple of column names forming the primary key.
        freshness_expectation: Expected update frequency (e.g., 'daily', 'hourly').
    """
    name: str
    grain: str
    owner: str
    primary_keys: tuple[str, ...]
    freshness_expectation: str
```
```

---

### [INFO] Missing docstring for ALL_DATASETS

**File**: `src/quant_alpha/platform/contracts.py:70`
**Category**: Readability
**Confidence**: 70%

ALL_DATASETS is a module-level constant intended for external use but has no documentation explaining its purpose or how it should be consumed.

**Suggestion**:
```
Add a comment or docstring:
```python
# Combined catalog of all dataset contracts across domains.
# Used by validation framework to enforce data governance.
ALL_DATASETS: list[DatasetContract] = EQUITY_DATASETS + ENERGY_DATASETS
```
```

---

### [INFO] Missing module docstring

**File**: `src/quant_alpha/platform/quality.py:1`
**Category**: Convention
**Confidence**: 90%

The module lacks a docstring explaining its purpose, which is data quality validation for energy market data.

**Suggestion**:
```
Add a module docstring:
```python
"""Data quality validation utilities for energy market data.

Provides functions to validate DataFrame integrity including
primary key constraints and null value checks.
"""
```

---

### [INFO] Potential performance issue with large DataFrames

**File**: `src/quant_alpha/platform/quality.py:6-8`
**Category**: Performance
**Confidence**: 60%

For very large DataFrames, frame.duplicated(keys).sum() creates a boolean Series and then sums it. While generally efficient, if called frequently in loops, consider if this meets performance requirements.

**Suggestion**:
```
For critical performance paths, consider adding a parameter to limit validation to a sample of the data, or document the expected performance characteristics.
```

---

### [INFO] Function name could be more generic

**File**: `src/quant_alpha/platform/quality.py:22-32`
**Category**: Readability
**Confidence**: 70%

The function name 'run_energy_quality_checks' suggests it's specific to energy data, but the underlying logic (primary key and null checks) is generic and could be reused for other domains.

**Suggestion**:
```
Consider renaming to 'run_data_quality_checks' or 'validate_dataframe' and making the column lists parameters as suggested above.
```

---

### [INFO] Missing error handling for write operations

**File**: `src/quant_alpha/storage/duckdb.py:10-15`
**Category**: Potential Bug
**Confidence**: 70%

The write_table function has no error handling. If the database file is corrupted, the disk is full, or the DataFrame has issues, the exception will propagate without any context or cleanup.

**Suggestion**:
```
Consider adding error handling or at minimum logging:

```python
def write_table(db_path: Path, table_name: str, frame: pd.DataFrame) -> None:
    """Write a DataFrame as a table in a DuckDB database."""
    try:
        db_path.parent.mkdir(parents=True, exist_ok=True)
        with duckdb.connect(str(db_path)) as con:
            con.register("_frame", frame)
            con.execute(f"create or replace table {table_name} as select * from _frame")
            con.unregister("_frame")
    except Exception as e:
        raise RuntimeError(f"Failed to write table '{table_name}' to {db_path}") from e
```
```

---

### [INFO] Missing exception class docstring

**File**: `src/quant_alpha/storage/gcp.py:11-12`
**Category**: Convention
**Confidence**: 80%

The CloudExportError class has no docstring explaining when it is raised.

**Suggestion**:
```
Add a docstring:
"""Raised when cloud export operations fail due to configuration or API errors."""
```

---

### [INFO] Missing client cleanup on error

**File**: `src/quant_alpha/storage/gcp.py:35-36`
**Category**: Potential Bug
**Confidence**: 65%

The storage_client and bq_client are created but not explicitly closed. While Python's garbage collector will eventually clean them up, on error the connections may linger.

**Suggestion**:
```
Consider using context managers if the clients support them, or add a try-finally block:
```python
try:
    # ... existing code ...
finally:
    storage_client.close()
    bq_client.close()
```
```

---

### [INFO] No validation that frames dictionary is not empty

**File**: `src/quant_alpha/storage/gcp.py:39-42`
**Category**: Potential Bug
**Confidence**: 60%

If frames is an empty dictionary, the function will succeed but return an empty dict without warning. While not necessarily a bug, it may indicate a caller issue.

**Suggestion**:
```
Optionally add a warning or early return:
```python
if not frames:
    return {}
```
```

---

### [INFO] Future annotations import may be unnecessary

**File**: `src/quant_alpha/streaming/demo_signals.py:1`
**Category**: Readability
**Confidence**: 60%

The `from __future__ import annotations` import enables PEP 604 union syntax (`list[str] | None`). This is only needed for Python < 3.10. If the project targets Python 3.10+, this import is unnecessary. If targeting 3.9, it's correct but should be documented.

**Suggestion**:
```
Verify the project's minimum Python version. If 3.10+, consider removing the import to reduce noise.
```

---

### [INFO] Deprecated pd.Timestamp.utcnow() usage

**File**: `src/quant_alpha/streaming/demo_signals.py:20`
**Category**: Code Style
**Confidence**: 90%

pd.Timestamp.utcnow() is deprecated in pandas >= 2.0 in favor of pd.Timestamp.now(tz='UTC'). This will generate a FutureWarning and will eventually be removed.

**Suggestion**:
```
Replace with: `end = pd.Timestamp.now(tz='UTC').floor('h')`
```

---

### [INFO] Function lacks return type hints for error path

**File**: `src/quant_alpha/streaming/demo_signals.py:35`
**Category**: Convention
**Confidence**: 60%

While `seed_demo_signals` has a return type hint `-> int`, the `write_table` function's behavior and return value are unknown from this context. If `write_table` could fail silently (returning without writing), `len(frame)` would be misleading as it doesn't confirm the write succeeded.

**Suggestion**:
```
Consider verifying the write succeeded or documenting that `write_table` raises on failure.
```

---

### [INFO] Hardcoded database path in __main__ block

**File**: `src/quant_alpha/streaming/demo_signals.py:40-43`
**Category**: Code Style
**Confidence**: 75%

The database path `data/warehouse/second_foundation.duckdb` is hardcoded relative to the project root. This is fragile — it depends on the file being exactly 3 directories deep from the project root (`parents[3]`). If the file moves, this breaks silently.

**Suggestion**:
```
Consider using environment variables or a config file:
```python
import os
if __name__ == "__main__":
    db = Path(os.environ.get(
        "QUANT_ALPHA_DB",
        Path(__file__).resolve().parents[3] / "data/warehouse/second_foundation.duckdb"
    ))
```
Or use a package-level config constant.
```

---

### [INFO] Missing module-level docstring

**File**: `src/quant_alpha/streaming/redpanda_consumer.py:1`
**Category**: Convention
**Confidence**: 70%

The module lacks a docstring explaining its purpose, usage, and configuration requirements (Redpanda/Kafka, DuckDB, schema paths).

**Suggestion**:
```
Add a module docstring:
"""Redpanda/Kafka consumer for energy signal data.

Consumes Avro-encoded messages and stores them in DuckDB for analysis.
"""
```

---

### [INFO] Imports inside function body

**File**: `src/quant_alpha/streaming/redpanda_consumer.py:10-11`
**Category**: Code Style
**Confidence**: 60%

Several functions import modules inside their body (json, fastavro, confluent_kafka, duckdb, io). While this can be intentional for lazy loading, it's inconsistent - pd is imported at the top level.

**Suggestion**:
```
Either import all dependencies at the top of the file for consistency, or document why lazy imports are needed (e.g., optional dependency).
```

---

### [INFO] Missing docstring for consume_energy_signals

**File**: `src/quant_alpha/streaming/redpanda_consumer.py:20`
**Category**: Convention
**Confidence**: 80%

The function lacks a docstring explaining its parameters, return value, and behavior (polling loop, max_messages/max_empty_polls semantics).

**Suggestion**:
```
Add a docstring:
"""Consume Avro-encoded messages from a Redpanda/Kafka topic.

Args:
    bootstrap_servers: Kafka broker addresses.
    topic: Topic to consume from.
    schema_path: Path to Avro schema file.
    max_messages: Maximum messages to collect.
    max_empty_polls: Stop after this many consecutive empty polls.

Returns:
    List of decoded message dicts.
"""
```

---

### [INFO] Hardcoded consumer group ID

**File**: `src/quant_alpha/streaming/redpanda_consumer.py:33`
**Category**: Convention
**Confidence**: 70%

The consumer group ID 'second-foundation-demo' is hardcoded. This should be configurable for different deployments or use cases.

**Suggestion**:
```
Add a `group_id` parameter to consume_energy_signals:

def consume_energy_signals(
    bootstrap_servers: str,
    topic: str,
    schema_path: Path,
    group_id: str = 'second-foundation-demo',
    # ...
) -> list[dict]:
```

---

### [INFO] Consumer close not in finally block pattern

**File**: `src/quant_alpha/streaming/redpanda_consumer.py:49`
**Category**: Readability
**Confidence**: 90%

While consumer.close() is called at line 49, the pattern doesn't follow best practices. If the while loop completes normally but a later line throws, cleanup still happens. But if an exception occurs in the loop, it doesn't.

**Suggestion**:
```
Already covered in the resource leak issue above, but reinforcing the pattern is important for robustness.
```

---

### [INFO] Docstring claims upsert but only INSERT

**File**: `src/quant_alpha/streaming/redpanda_consumer.py:68-69`
**Category**: Potential Bug
**Confidence**: 85%

The docstring says 'upsert into a DuckDB table' but the implementation only does INSERT. This means duplicate rows will be inserted on subsequent calls rather than being updated/replaced.

**Suggestion**:
```
Either update the docstring to say 'insert' or implement actual upsert logic using DuckDB's INSERT OR REPLACE or ON CONFLICT syntax if deduplication is needed.
```

---

### [INFO] Missing module docstring

**File**: `src/quant_alpha/streaming/redpanda_producer.py:1`
**Category**: Convention
**Confidence**: 95%

The module lacks a docstring explaining its purpose and functionality.

**Suggestion**:
```
Add a module-level docstring:
"""Redpanda/Kafka producer for energy market signals."""
```

---

### [INFO] Hardcoded date range in generate_synthetic_power_market call

**File**: `src/quant_alpha/streaming/redpanda_producer.py:35`
**Category**: Readability
**Confidence**: 70%

The date range '2024-01-01' to '2024-01-07' is hardcoded, making the function less flexible for different time periods.

**Suggestion**:
```
Consider making date range parameters optional or configurable:
```python
def publish_energy_signals(
    bootstrap_servers: str, 
    topic: str, 
    schema_path: Path, 
    sample_size: int = 100,
    start_date: str = "2024-01-01",
    end_date: str = "2024-01-07"
) -> None:
```
```

---

### [INFO] Hardcoded market list in generate_synthetic_power_market call

**File**: `src/quant_alpha/streaming/redpanda_producer.py:35`
**Category**: Readability
**Confidence**: 70%

The market list ['DE_LU', 'CZ', 'FR'] is hardcoded, making the function less flexible for different market selections.

**Suggestion**:
```
Consider making the market list a parameter:
```python
def publish_energy_signals(
    bootstrap_servers: str, 
    topic: str, 
    schema_path: Path, 
    sample_size: int = 100,
    markets: list[str] = None
) -> None:
    if markets is None:
        markets = ["DE_LU", "CZ", "FR"]
```
```

---

### [INFO] Potential performance issue with to_dict(orient='records')

**File**: `src/quant_alpha/streaming/redpanda_producer.py:36-44`
**Category**: Performance
**Confidence**: 70%

The to_dict(orient='records') method creates a list of dictionaries which may be memory-intensive for large DataFrames.

**Suggestion**:
```
Consider using iterrows() or itertuples() for large datasets:
```python
for _, row in market.head(sample_size).iterrows():
    payload = {
        "timestamp": pd.Timestamp(row["timestamp"]).isoformat(),
        "market": row["market"],
        "spot_price": float(row["spot_price"]),
        "residual_load": float(row["residual_load"]),
        "imbalance_price": float(row["imbalance_price"]),
    }
    producer.produce(topic, _serialize(schema, payload))
```
```

---

### [INFO] Magic number 3 in path resolution

**File**: `src/quant_alpha/streaming/redpanda_producer.py:48`
**Category**: Readability
**Confidence**: 75%

The number 3 in parents[3] is a magic number that's unclear without explanation.

**Suggestion**:
```
Add a comment explaining the path hierarchy:
```python
# Navigate from src/quant_alpha/streaming/redpanda_producer.py to project root
root = Path(__file__).resolve().parents[3]
```
```

---

### [INFO] Missing module-level docstring or type hints for parameters

**File**: `src/quant_alpha/streaming/risingwave/client.py:1`
**Category**: Convention
**Confidence**: 75%

While functions have docstrings, they don't follow NumPy/Google docstring style consistently. Some parameters lack type descriptions, and return types could be more explicit.

**Suggestion**:
```
Add proper docstrings:
```python
def query_realtime_scores(
    conn: Any,
    market: str | None = None,
    limit: int = 100,
) -> pd.DataFrame:
    """Query the mv_realtime_alpha_scores materialized view.
    
    Parameters
    ----------
    conn : Any
        psycopg2-compatible database connection.
    market : str or None, optional
        Filter by market name (default: None for all markets).
    limit : int, optional
        Maximum number of rows to return (default: 100).
    
    Returns
    -------
    pd.DataFrame
        DataFrame containing realtime alpha scores.
    """
```
```

---

### [INFO] Select all columns in queries

**File**: `src/quant_alpha/streaming/risingwave/client.py:7`
**Category**: Performance
**Confidence**: 70%

Multiple queries use `SELECT *` which fetches all columns. This can be inefficient if the tables have many columns or large text/blob fields that aren't needed.

**Suggestion**:
```
Specify needed columns:
```python
def query_realtime_scores(...):
    sql = """
        SELECT market, symbol, alpha_score, timestamp, confidence
        FROM mv_realtime_alpha_scores
        WHERE ...
    """
```
```

---

### [INFO] Redundant condition in statement filtering

**File**: `src/quant_alpha/streaming/risingwave/client.py:25`
**Category**: Code Style
**Confidence**: 90%

The condition `if any(kw in joined.upper() for kw in _DDL_KEYWORDS) or joined` is redundant. If `joined` is truthy (non-empty), the condition is always True regardless of the keyword check. This makes the keyword check useless for non-DDL statements.

**Suggestion**:
```
Simplify the condition:
```python
def _split_statements(sql: str) -> list[str]:
    # ... existing code ...
    for line in sql.splitlines():
        # ... existing code ...
        if stripped.endswith(";"):
            joined = "\n".join(current).strip().rstrip(";")
            if joined:  # Only append non-empty statements
                stmts.append(joined)
            current = []
    return [s for s in stmts if s.strip()]
```
```

---

### [INFO] No connection pooling or caching

**File**: `src/quant_alpha/streaming/risingwave/client.py:57-67`
**Category**: Performance
**Confidence**: 75%

The module creates individual connections without pooling. For high-frequency queries, this could cause connection overhead and potential connection exhaustion.

**Suggestion**:
```
Consider using psycopg2's connection pool:
```python
from psycopg2 import pool

_connection_pool = None

def get_connection_pool(minconn=1, maxconn=10, **kwargs):
    global _connection_pool
    if _connection_pool is None:
        _connection_pool = pool.SimpleConnectionPool(minconn, maxconn, **kwargs)
    return _connection_pool
```
```

---

### [INFO] Missing type hints for module-level constants

**File**: `src/quant_alpha/streaming/risingwave/producer.py:21`
**Category**: Convention
**Confidence**: 60%

Module-level constants lack type annotations, making the expected types unclear.

**Suggestion**:
```
Add type hints: `BOOTSTRAP: str = os.environ.get(...)` etc.
```

---

### [INFO] Missing docstring for _make_producer function

**File**: `src/quant_alpha/streaming/risingwave/producer.py:28`
**Category**: Readability
**Confidence**: 60%

The function lacks a docstring explaining its purpose, parameters, and return value.

**Suggestion**:
```
Add docstring: `"""Create and return a Kafka producer instance."""`
```

---

### [INFO] Missing docstring for _delivery_report function

**File**: `src/quant_alpha/streaming/risingwave/producer.py:35`
**Category**: Readability
**Confidence**: 60%

The function lacks a docstring explaining its purpose as a delivery callback.

**Suggestion**:
```
Add docstring: `"""Callback for Kafka message delivery reports."""`
```

---

### [INFO] Potential N+1 query pattern

**File**: `src/quant_alpha/streaming/risingwave/producer.py:44`
**Category**: Performance
**Confidence**: 60%

Using iterrows() on DataFrame is inefficient for large datasets. For production use, consider vectorized operations or batch processing.

**Suggestion**:
```
If frame size grows, consider: `frame.apply(lambda row: send_message(row), axis=1)` or batch processing.
```

---

### [INFO] Variable name 'p' is too short

**File**: `src/quant_alpha/streaming/risingwave/producer.py:56`
**Category**: Code Style
**Confidence**: 70%

Single-letter variable name 'p' is not descriptive and reduces code readability.

**Suggestion**:
```
Use descriptive name: `producer = _make_producer()`
```

---

### [INFO] Missing return type annotation for docstring completeness

**File**: `src/quant_alpha/streaming/risingwave/simulator.py:16`
**Category**: Convention
**Confidence**: 70%

The function docstring mentions it returns a DataFrame equivalent to the RisingWave materialized view, but doesn't document the expected columns or their types. This makes it harder for callers to know what to expect.

**Suggestion**:
```
Add column documentation to the docstring:
```python
"""
Returns a DataFrame with columns:
    - timestamp: datetime
    - market: str
    - spot_price: float
    - alpha_residual_load_rank: float (0-1)
    ...
"""
```
```

---

### [INFO] Using deprecated utcnow() method

**File**: `src/quant_alpha/streaming/risingwave/simulator.py:29-30`
**Category**: Performance
**Confidence**: 65%

`pd.Timestamp.utcnow()` uses UTC timezone info but returns a timezone-aware timestamp. This is fine functionally, but be aware that the `utcnow()` method on `datetime.datetime` is deprecated in Python 3.12+. Using `pd.Timestamp.now(tz='UTC')` is more future-proof.

**Suggestion**:
```
Replace with:
```python
end = pd.Timestamp.now(tz='UTC').floor('h')
```
```

---

### [INFO] No validation of empty input parameters

**File**: `src/quant_alpha/streaming/risingwave/simulator.py:33`
**Category**: Potential Bug
**Confidence**: 85%

The function `build_realtime_alpha_panel` does not validate that `markets` is non-empty or that `hours` is positive. An empty `markets` list would pass an empty DataFrame to DuckDB, potentially causing confusing errors in the SQL query.

**Suggestion**:
```
Add input validation at function entry:
```python
if not markets:
    raise ValueError("markets list must not be empty")
if hours <= 0:
    raise ValueError("hours must be positive")
```
```

---

### [INFO] Magic numbers for scarcity thresholds

**File**: `src/quant_alpha/streaming/risingwave/simulator.py:86-91`
**Category**: Readability
**Confidence**: 80%

The scarcity alert function uses hardcoded thresholds: 0.8 for MEDIUM, 0.9 for residual load, and 0.7 for momentum. These should be documented or extracted as named constants for maintainability.

**Suggestion**:
```
Extract constants:
```python
SCARCICTY_THRESHOLD_MEDIUM = 0.8
SCARCITY_THRESHOLD_HIGH_LOAD = 0.9
SCARCITY_THRESHOLD_HIGH_MOMENTUM = 0.7
```
Or add docstring documentation explaining the threshold semantics.
```

---

