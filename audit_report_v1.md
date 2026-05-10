# Code Review Report

## Summary

Found 398 issue(s): 31 critical, 22 errors, 190 warnings, 155 info. Categories: 181 potential_bug, 74 convention, 69 readability, 28 code_style, 26 performance, 11 architecture, 9 security.

### Statistics

| Metric | Count |
|--------|-------|
| Critical | 31 |
| Error | 22 |
| Warning | 190 |
| Info | 155 |

**Files Reviewed**: 64

---

## CRITICAL Issues (31)

### [CRITICAL] Missing error handling for pipeline execution

**File**: `bruin/pipelines/energy_ingestion/run_energy_ingestion.py:13`
**Category**: Potential Bug
**Confidence**: 90%

The `run_energy_pipeline` call has no exception handling. If the pipeline fails (network error, invalid config, missing data, etc.), the script will crash with an unhandled exception and potentially leave resources in an inconsistent state.

**Suggestion**:
```
Wrap the pipeline call in try/except with appropriate logging:
```python
try:
    result = run_energy_pipeline(config, root.resolve(), source_override=source)
except Exception as e:
    print(f"Pipeline execution failed: {e}", file=sys.stderr)
    sys.exit(1)
```
```

---

### [CRITICAL] No validation of result dictionary keys

**File**: `bruin/pipelines/energy_ingestion/run_energy_ingestion.py:14-15`
**Category**: Potential Bug
**Confidence**: 80%

The code directly accesses `result['rows']`, `result['duckdb_path']`, and `result['data_source']` without checking that these keys exist. If `run_energy_pipeline` returns a different structure (e.g., on partial success or error), this will raise a `KeyError`.

**Suggestion**:
```
Validate the result structure before accessing keys:
```python
required_keys = ['rows', 'duckdb_path', 'data_source']
for key in required_keys:
    if key not in result:
        raise ValueError(f"Pipeline result missing expected key: {key}")
```
```

---

### [CRITICAL] No WHERE clause for data filtering

**File**: `dbt_energy_alpha/models/marts/fct_energy_alpha_diagnostics.sql:1-10`
**Category**: Potential Bug
**Confidence**: 75%

The query selects from a source table without any WHERE clause. This will return ALL rows from the source table, which could include stale data, test data, or historical records that aren't relevant. This is especially concerning for a 'diagnostics' table that may accumulate records over time.

**Suggestion**:
```
Add appropriate filtering conditions. Consider adding date filters or status filters:
```sql
select
    alpha_name,
    is_ic_mean,
    oos_ic_mean,
    is_oos_ic_same_sign,
    consistency_score,
    robustness_score,
    oos_sharpe,
    oos_max_drawdown
from {{ source('energy_raw', 'energy_alpha_diagnostics') }}
where date = '{{ var('run_date') }}'  -- or appropriate filter
   or where is_current = true
```
```

---

### [CRITICAL] Missing materialization strategy

**File**: `dbt_energy_alpha/models/marts/fct_energy_alpha_diagnostics.sql:1-10`
**Category**: Convention
**Confidence**: 70%

The model doesn't specify a materialization strategy. Depending on the size of the source data and how frequently it's queried, this could be materialized as a table for better performance or kept as a view for freshness.

**Suggestion**:
```
Add materialization config at the top of the file:
```sql
{{ config(
    materialized='table',
    schema='marts',
    tags=['diagnostics', 'energy']
) }}

select
    alpha_name,
    ...
```
```

---

### [CRITICAL] Redundant cast(timestamp as timestamp)

**File**: `dbt_energy_alpha/models/staging/stg_energy_alphas.sql:2`
**Category**: Potential Bug
**Confidence**: 75%

The expression `cast(timestamp as timestamp)` casts a column named 'timestamp' to the timestamp type. If the source column is already a timestamp, this is a no-op. If the source column is a string/varchar, this implicit conversion may fail or produce unexpected results depending on the database engine and the string format in the source data. Without knowing the source schema, this cast provides a false sense of safety.

**Suggestion**:
```
If the source column is already a timestamp, remove the redundant cast. If it's a string, use an explicit parse function for the target database (e.g., `TO_TIMESTAMP(timestamp, 'YYYY-MM-DD HH24:MI:SS')` for Postgres, or `PARSE_TIMESTAMP('%Y-%m-%d %H:%M:%S', timestamp)` for BigQuery). Add a comment explaining the source type:
```sql
-- source 'timestamp' column is varchar; cast to native timestamp
cast(timestamp as timestamp) as market_ts
```
```

---

### [CRITICAL] Redundant timestamp cast may hide data issues

**File**: `dbt_energy_alpha/models/staging/stg_power_market.sql:2`
**Category**: Potential Bug
**Confidence**: 70%

The column 'timestamp' is being cast to timestamp type, which appears redundant if the source column is already timestamp type. This cast will fail if the source data contains malformed timestamps or NULL values without proper error handling, potentially causing the entire model to fail.

**Suggestion**:
```
Either remove the redundant cast if the source is already timestamp, or add error handling:
```sql
coalesce(cast(timestamp as timestamp), cast('1970-01-01' as timestamp)) as market_ts
```
Or use dbt's safe casting macros if available.
```

---

### [CRITICAL] Missing source freshness check configuration

**File**: `dbt_energy_alpha/models/staging/stg_power_market.sql:10`
**Category**: Readability
**Confidence**: 75%

The model references a source table but there's no indication of source freshness monitoring. For energy market data, freshness is critical for real-time analysis and forecasting.

**Suggestion**:
```
Add source freshness configuration in the source YAML:
```yaml
sources:
  - name: energy_raw
    freshness:
      warn_after: {count: 2, period: hour}
      error_after: {count: 24, period: hour}
    loaded_at_field: timestamp
```
```

---

### [CRITICAL] Pass-through select with no transformations

**File**: `dbt_quant_alpha/models/marts/fct_alpha_diagnostics.sql:1-10`
**Category**: Readability
**Confidence**: 70%

This model is a simple pass-through select from a source table with no transformations, joins, filters, or aggregations. Consider whether this mart adds value or if users should reference the source directly, or whether additional business logic (e.g., renaming, categorizing scores, adding thresholds) would make this mart more useful.

**Suggestion**:
```
Either add meaningful transformations, or if the purpose is to create a clean interface layer, add a comment explaining the rationale:
```sql
-- Mart providing a clean interface to alpha diagnostics
-- Adds standardized naming and filters to active alphas only
select
    ...
```
```

---

### [CRITICAL] No WHERE clause filters on date or symbol

**File**: `dbt_quant_alpha/models/staging/stg_factor_panel.sql:1-14`
**Category**: Potential Bug
**Confidence**: 80%

The query selects all rows from the source table without any filtering. If the factor_panel table grows over time, this staging model will process all historical data on every run, which may cause performance issues and unexpected behavior in downstream models that expect incremental updates.

**Suggestion**:
```
Add date filters or implement dbt incremental strategy:
```sql
select
    cast(date as date) as signal_date,
    ...
from {{ source('quant_alpha_raw', 'factor_panel') }}
{% if is_incremental() %}
where cast(date as date) > (select max(signal_date) from {{ this }})
{% endif %}
```
```

---

### [CRITICAL] No documentation or description for the model

**File**: `dbt_quant_alpha/models/staging/stg_factor_panel.sql:1-14`
**Category**: Readability
**Confidence**: 70%

The model lacks a description explaining its purpose, data source, and intended use. This reduces maintainability for other team members.

**Suggestion**:
```
Add a YAML description in the dbt schema file or add a comment block:
```sql
{{
  config(
    materialized='table',
    description='Staging model for factor panel data from quant_alpha_raw'
  )
}}

-- Staging model: cleans and types factor panel data
select
    ...
```
```

---

### [CRITICAL] Missing model documentation

**File**: `dbt_quant_alpha/models/staging/stg_prices.sql:1-10`
**Category**: Convention
**Confidence**: 70%

The staging model lacks a description in the dbt schema.yml file and has no inline comments explaining the data transformation logic or business context of the raw_prices source.

**Suggestion**:
```
Add a model description in the schema.yml file and consider adding comments to explain the date casting transformation and the purpose of selecting these specific columns.
```

---

### [CRITICAL] Missing source freshness check

**File**: `dbt_quant_alpha/models/staging/stg_prices.sql:1-10`
**Category**: Architecture
**Confidence**: 70%

The model uses a source but doesn't implement source freshness checking, which could lead to stale data being used in downstream models without warning.

**Suggestion**:
```
Add source freshness configuration in the schema.yml file for the 'raw_prices' source to monitor data recency.
```

---

### [CRITICAL] coalesce(1) forces single-partition output

**File**: `src/quant_alpha/batch/spark_energy_features.py:40-44`
**Category**: Performance
**Confidence**: 85%

coalesce(1) forces all data into a single partition/file, which will cause a performance bottleneck and potential OOM for large datasets. This is likely intended for downstream compatibility but is dangerous at scale.

**Suggestion**:
```
Consider removing coalesce(1) or making it configurable. If single file is needed downstream, use: enriched.write.mode('overwrite').parquet(output_path) and handle file merging separately.
```

---

### [CRITICAL] Spark stop not in finally block

**File**: `src/quant_alpha/batch/spark_energy_features.py:45-46`
**Category**: Potential Bug
**Confidence**: 90%

spark.stop() is not guaranteed to execute if an exception occurs during the write operation. This can lead to resource leaks with orphaned Spark processes.

**Suggestion**:
```
Use try/finally: try: ... (enriched write) finally: spark.stop()
```

---

### [CRITICAL] Redundant numeric conversion for already-numeric columns

**File**: `src/quant_alpha/features/energy_alpha.py:68-75`
**Category**: Performance
**Confidence**: 80%

The code calls `pd.to_numeric(df[col], errors='coerce')` on every column in numeric_cols regardless of whether the column is already numeric. If the DataFrame already has float64 columns, this creates unnecessary copies and computation. Only object/string columns need conversion.

**Suggestion**:
```
Add a dtype check to skip already-numeric columns:
```python
for col in numeric_cols:
    if col in df.columns and not pd.api.types.is_numeric_dtype(df[col]):
        df[col] = pd.to_numeric(df[col], errors='coerce')
```
```

---

### [CRITICAL] Missing return type hint on equity_ohlcv

**File**: `src/quant_alpha/ingestion/dlt_equity.py:46-60`
**Category**: Convention
**Confidence**: 75%

The inner function equity_ohlcv has a return type annotation (Iterator[dict]) but the outer function equity_source lacks a return type hint.

**Suggestion**:
```
Add return type hint to equity_source: `def equity_source(...) -> dlt.sources.DltSource:` or appropriate return type.
```

---

### [CRITICAL] Missing __main__ guard documentation

**File**: `src/quant_alpha/ingestion/dlt_equity.py:107-114`
**Category**: Convention
**Confidence**: 70%

The __main__ block has a hardcoded relative path traversal (parents[3]) which is fragile and depends on the file's exact location in the project hierarchy.

**Suggestion**:
```
Consider using a project root discovery utility or environment variable instead of hardcoded parents[3]. Add a comment explaining the expected project structure.
```

---

### [CRITICAL] No validation of raw DataFrame columns

**File**: `src/quant_alpha/pipeline_energy.py:60-63`
**Category**: Potential Bug
**Confidence**: 80%

After loading raw data, there's no validation that expected columns (e.g., 'spot_price', 'market', 'timestamp') exist. If the data source returns unexpected schema, downstream operations will fail with cryptic KeyError messages.

**Suggestion**:
```
Add column validation after loading:
```python
raw = _load_power_market(cfg, markets, universe)
required_cols = {"timestamp", "market", "spot_price"}
missing = required_cols - set(raw.columns)
if missing:
    raise ValueError(f"Raw data missing required columns: {missing}")
```
```

---

### [CRITICAL] Subprocess execution without timeout

**File**: `src/quant_alpha/platform/bruin_graph.py:72`
**Category**: Potential Bug
**Confidence**: 80%

subprocess.run() is called without a timeout parameter. If a child process hangs indefinitely, the parent process will also hang, potentially causing resource exhaustion.

**Suggestion**:
```
Add a timeout parameter: `result = subprocess.run(..., timeout=300)` (5 minutes or appropriate limit)
```

---

### [CRITICAL] Path construction may fail with special characters

**File**: `src/quant_alpha/platform/bruin_graph.py:82`
**Category**: Potential Bug
**Confidence**: 70%

str(path.parent / run_cfg["file"]) constructs a file path by joining with user-provided data from YAML. If run_cfg['file'] contains path traversal sequences (e.g., '../'), this could access unintended directories.

**Suggestion**:
```
Validate the path doesn't escape the expected directory: `run_path = (path.parent / run_cfg["file"]).resolve(); if not str(run_path).startswith(str(path.parent)): raise ValueError("Path traversal detected")`
```

---

### [CRITICAL] SQL injection via f-string interpolation

**File**: `src/quant_alpha/storage/duckdb.py:13`
**Category**: Security
**Confidence**: 95%

The table_name parameter is directly interpolated into an SQL query using an f-string on line 13: f"create or replace table {table_name} as select * from _frame". This allows SQL injection if an attacker can control the table_name parameter. A malicious value like 'x; DROP TABLE important_data; --' could execute arbitrary SQL statements.

**Suggestion**:
```
Sanitize the table_name by validating it against a whitelist of allowed characters (alphanumeric and underscores only), or use DuckDB's proper identifier quoting:
```python
def _sanitize_identifier(name: str) -> str:
    """Validate that name is a safe SQL identifier."""
    if not name.isidentifier():
        raise ValueError(f"Invalid identifier: {name!r}")
    return name

# In write_table:
table_name = _sanitize_identifier(table_name)
con.execute(f"create or replace table {table_name} as select * from _frame")
```
```

---

### [CRITICAL] Table name used without validation in paths and IDs

**File**: `src/quant_alpha/storage/gcp.py:32`
**Category**: Security
**Confidence**: 70%

The table_name from the frames dictionary keys is used directly in GCS blob paths (line 35) and BigQuery table IDs (line 41) without validation. Malicious or malformed table names could cause path traversal or invalid identifiers.

**Suggestion**:
```
Validate table_name contains only safe characters:
```python
import re
if not re.match(r'^[a-zA-Z_][a-zA-Z0-9_]*$', table_name):
    raise CloudExportError(f"Invalid table name: {table_name}")
```
```

---

### [CRITICAL] SQL injection via f-string table name

**File**: `src/quant_alpha/streaming/redpanda_consumer.py:75`
**Category**: Security
**Confidence**: 85%

The `table` parameter is directly interpolated into SQL queries using f-strings on lines 75 and 78. An attacker who controls the `table` parameter could inject arbitrary SQL commands. While this may seem internal, if `table` is ever derived from external input or configuration files, this becomes exploitable.

**Suggestion**:
```
Validate the table name against a whitelist or use identifier quoting:
```python
import re
if not re.match(r'^[a-zA-Z_][a-zA-Z0-9_]*$', table):
    raise ValueError(f"Invalid table name: {table}")
# Or use duckdb's identifier quoting:
con.execute(f'CREATE TABLE IF NOT EXISTS "{table}" AS SELECT * FROM frame WHERE false')
```
```

---

### [CRITICAL] Hardcoded Kafka bootstrap server address

**File**: `src/quant_alpha/streaming/redpanda_producer.py:47`
**Category**: Security
**Confidence**: 90%

The Kafka bootstrap server address 'localhost:19092' is hardcoded in the main block. In production, this could connect to an unintended or insecure server, and the address should be configurable via environment variables or configuration files.

**Suggestion**:
```
Use environment variables or a configuration file:
```python
import os
bootstrap_servers = os.getenv('KAFKA_BOOTSTRAP_SERVERS', 'localhost:19092')
publish_energy_signals(bootstrap_servers, 'energy-signals', root / 'schemas/energy_signal.avsc')
```
```

---

### [CRITICAL] No error handling for database operations

**File**: `src/quant_alpha/streaming/risingwave/simulator.py:36-76`
**Category**: Potential Bug
**Confidence**: 95%

The duckdb.connect(), .register(), and .execute() calls are not wrapped in try/except or try/finally. If the SQL query fails or registration fails, con.close() will never be called, causing a resource leak. Additionally, any DuckDBError or pandas-related error will propagate unhandled.

**Suggestion**:
```
Use a context manager or try/finally to ensure connection cleanup:
```python
con = duckdb.connect(db_path)
try:
    con.register("power_market_signals", features)
    result = con.execute(...).df()
finally:
    con.close()
return result
```
```

---

### [CRITICAL] No validation of threshold parameter range

**File**: `src/quant_alpha/streaming/risingwave/simulator.py:80`
**Category**: Potential Bug
**Confidence**: 70%

The threshold parameter is accepted as a float but there's no validation that it's in the range [0.0, 1.0]. Since PERCENT_RANK() returns values in [0, 1], thresholds outside this range would either return all or no rows, which may be confusing to callers.

**Suggestion**:
```
Add validation:
```python
def get_scarcity_alerts(
    panel: pd.DataFrame,
    threshold: float = 0.8,
) -> pd.DataFrame:
    if not 0.0 <= threshold <= 1.0:
        raise ValueError(f"threshold must be between 0.0 and 1.0, got {threshold}")
```
```

---

### [CRITICAL] Hardcoded Kafka broker address

**File**: `src/quant_alpha/streaming/risingwave/views.sql:27`
**Category**: Security
**Confidence**: 70%

The Kafka bootstrap server 'redpanda:9092' is hardcoded directly in the SQL source definition. This makes it difficult to change the broker address across environments (dev, staging, production) without modifying the SQL file.

**Suggestion**:
```
Use a configuration management approach or environment variable substitution if RisingWave supports it. Alternatively, document this clearly and consider parameterizing in deployment scripts.
```

---

### [CRITICAL] demand_surprise is always zero or NULL with COALESCE

**File**: `src/quant_alpha/streaming/risingwave/views.sql:74`
**Category**: Potential Bug
**Confidence**: 90%

COALESCE(actual_load, load_forecast) - load_forecast simplifies to: actual_load - load_forecast when actual_load is NOT NULL, or load_forecast - load_forecast = 0 when actual_load IS NULL. This means whenever actual_load is missing, demand_surprise is always exactly 0, which is misleading — it's not a 'zero surprise' but rather 'unknown'. This zero will bias the PERCENT_RANK downstream.

**Suggestion**:
```
If actual_load is NULL, set demand_surprise to NULL to distinguish 'unknown' from 'zero surprise': CASE WHEN actual_load IS NOT NULL THEN actual_load - load_forecast ELSE NULL END AS demand_surprise
```

---

### [CRITICAL] Unbounded NULL propagation in solar_penetration

**File**: `src/quant_alpha/streaming/risingwave/views.sql:76`
**Category**: Potential Bug
**Confidence**: 75%

solar_penetration is computed as solar_forecast / NULLIF(load_forecast, 0). If both solar_forecast and load_forecast are NULL or zero, this produces NULL. Downstream PERCENT_RANK() on solar_penetration (mv_realtime_alpha_scores, line 127-129) will handle NULLs, but alpha_solar_penetration will silently become NULL, potentially causing gaps in the composite signal.

**Suggestion**:
```
Add explicit NULL handling or COALESCE to provide a sensible default: COALESCE(solar_forecast / NULLIF(load_forecast, 0), 0) AS solar_penetration
```

---

### [CRITICAL] Cross-market spread uses self-JOIN on source

**File**: `src/quant_alpha/streaming/risingwave/views.sql:83-93`
**Category**: Performance
**Confidence**: 70%

mv_cross_market_spread JOINs power_market_source with a CTE that also reads from power_market_source. In RisingWave, each materialized view reading from the same source causes separate internal operators. This effectively doubles the source scan and may cause significant resource usage at high throughput.

**Suggestion**:
```
Consider creating an intermediate materialized view for the market mean, then JOINing the two MVs together. This allows RisingWave to optimize internal state management more effectively.
```

---

### [CRITICAL] Hardcoded new factor names may become stale

**File**: `tests/test_alpha_factors.py:55-59`
**Category**: Potential Bug
**Confidence**: 70%

The `new_factors` list contains hardcoded column name strings. If these factor definitions change in the source code, this test will fail with unclear errors rather than gracefully indicating the schema changed.

**Suggestion**:
```
Import the expected new factor names from a constant defined alongside where they're implemented, or use a fixture:
```python
NEW_WQ_FACTORS = [...]  # defined in alpha_factors module
```
```

---

## ERROR Issues (22)

### [ERROR] No validation that config file exists

**File**: `bruin/pipelines/energy_ingestion/run_energy_ingestion.py:10`
**Category**: Potential Bug
**Confidence**: 85%

The config file path is constructed but never validated before being passed to `run_energy_pipeline`. If the file doesn't exist, the error will be opaque and occur deep in the pipeline code.

**Suggestion**:
```
Add file existence check before proceeding:
```python
config = root / "configs" / "second_foundation_project.yaml"
if not config.is_file():
    raise FileNotFoundError(f"Config file not found: {config}")
```
```

---

### [ERROR] Script executes on import without main guard

**File**: `bruin/pipelines/equity_ingestion/run_equity_ingestion.py:1-15`
**Category**: Potential Bug
**Confidence**: 95%

All code at module level runs immediately when the file is imported, not just when executed as a script. This will cause unintended side effects if another module imports this file, including printing to stdout and potentially modifying the filesystem.

**Suggestion**:
```
Wrap executable code in an if __name__ == '__main__': guard:

```python
if __name__ == '__main__':
    root = Path(os.environ.get("PROJECT_ROOT", "."))
    config = root / "configs" / "project.yaml"
    offline = os.environ.get("OFFLINE", "true").lower() == "true"
    result = run_pipeline(config, root.resolve(), offline=offline)
    print(f"raw_equity_ohlcv: {result['rows']} rows → {result['duckdb_path']}
```
```

---

### [ERROR] No error handling for pipeline execution

**File**: `bruin/pipelines/equity_ingestion/run_equity_ingestion.py:13-15`
**Category**: Potential Bug
**Confidence**: 85%

run_pipeline() and dictionary key access on result (result['rows'], result['duckdb_path']) have no error handling. If the pipeline fails or returns an unexpected structure, the script will crash with an unhandled exception and no useful error message.

**Suggestion**:
```
Add try/except around the pipeline call and validate the result:
```python
try:
    result = run_pipeline(config, root.resolve(), offline=offline)
except Exception as e:
    print(f"Pipeline failed: {e}", file=sys.stderr)
    sys.exit(1)

if 'rows' not in result or 'duckdb_path' not in result:
    print(f"Unexpected result structure: {result}", file=sys.stderr)
    sys.exit(1)
```
```

---

### [ERROR] Division by zero risk in LAG return calculation

**File**: `bruin/pipelines/equity_ingestion/stg_equity_ohlcv.sql:36`
**Category**: Potential Bug
**Confidence**: 95%

The expression `ln(adj_close / LAG(adj_close) OVER (...))` will produce an error (division by zero) when LAG returns 0, and will return -Infinity or NaN. While the WHERE clause filters adj_close > 0 for the current row, it does not protect against a LAG value of 0 or NULL. If any historical row had adj_close = 0 (which could exist in raw data), this will fail.

**Suggestion**:
```
Add a guard against NULL or zero LAG values:
```sql
CASE
  WHEN LAG(adj_close) OVER (PARTITION BY symbol ORDER BY date) > 0
  THEN ln(adj_close / LAG(adj_close) OVER (PARTITION BY symbol ORDER BY date))
  ELSE NULL
END AS ret_1d
```
```

---

### [ERROR] Missing energy track data as described

**File**: `bruin/pipelines/reporting/rpt_backtest_summary.sql:47-48`
**Category**: Potential Bug
**Confidence**: 95%

The asset metadata claims to 'Combine equity and energy track backtest results into a unified reporting layer', but the SQL only selects 'equity' as the track value and queries only from backtest_daily with fct_alpha_diagnostics. There is no UNION or logic to include energy track data, contradicting the documented purpose.

**Suggestion**:
```
Either add a UNION ALL to include energy track data, e.g.:

SELECT 'equity' AS track, ... FROM backtest_daily AS b JOIN fct_alpha_diagnostics AS d USING (alpha_name) WHERE d.gate_consistency = true
UNION ALL
SELECT 'energy' AS track, ... FROM fct_energy_daily AS e JOIN fct_energy_alpha_diagnostics AS ed USING (alpha_name) WHERE ed.gate_consistency = true

Or update the description to reflect that only equity data is included.
```

---

### [ERROR] Reserved keyword used as column alias without quoting

**File**: `dbt_energy_alpha/models/marts/fct_energy_market_quality.sql:2-3`
**Category**: Potential Bug
**Confidence**: 75%

Using "check" and "column" as string literals for column names is problematic. These are reserved SQL keywords in many databases. The quotes around them make them string literals, not column references, which may cause confusion and potential issues depending on the database engine.

**Suggestion**:
```
Either use the actual column names from the source table or use backticks/brackets if these are actual column names:

```sql
select
    check_col as check_name,
    column_col as column_name,
    keys as key_columns,
    passed,
    nulls,
    duplicates
from {{ source('energy_raw', 'power_market_quality') }}
```

Or if "check" and "column" are actual column names in the source, quote them properly for the target database.
```

---

### [ERROR] Pandas axis deprecation in DataFrame.sum(axis=1)

**File**: `src/quant_alpha/backtest/long_short.py:74`
**Category**: Potential Bug
**Confidence**: 70%

In recent pandas versions, `.sum(axis=1)` on a DataFrame may trigger deprecation warnings or behave unexpectedly with the new copy-on-write semantics. More critically, `wide_weights.diff().abs().sum(axis=1)` could produce NaN for the first row (diff produces NaN), but the subsequent `fillna` handles the first row only. If a date has no prior row but has turnover, the logic is correct, but this relies on index alignment.

**Suggestion**:
```
Ensure index alignment is intentional and consider explicit handling:
```python
turnover = wide_weights.diff().abs().sum(axis=1).fillna(0)  # first day has no prior
# Or for first day, use the absolute weights as initial turnover:
turnover = wide_weights.diff().abs().sum(axis=1)
first_day_mask = turnover.isna()
turnover[first_day_mask] = wide_weights.loc[first_day_mask].abs().sum(axis=1)
```
```

---

### [ERROR] Division by zero risk with log of zero price

**File**: `src/quant_alpha/batch/spark_energy_features.py:28`
**Category**: Potential Bug
**Confidence**: 85%

F.log('spot_price') will produce -infinity or NaN if spot_price is zero or negative, which is possible in energy markets during negative pricing events. The subtraction of two log values compounds this issue.

**Suggestion**:
```
Add null/zero handling: .withColumn('spot_return_1h', F.when((F.col('spot_price') > 0) & (F.lag('spot_price').over(w_market) > 0), F.log('spot_price') - F.log(F.lag('spot_price').over(w_market))).otherwise(F.lit(None)))
```

---

### [ERROR] Missing error handling for invalid YAML content

**File**: `src/quant_alpha/config.py:90-92`
**Category**: Potential Bug
**Confidence**: 80%

yaml.safe_load can raise yaml.YAMLError if the file contains malformed YAML. This exception is not caught or documented, which could lead to unhandled exceptions.

**Suggestion**:
```
Either document the potential exception or add error handling:
```python
def load_yaml(path: Path) -> dict[str, Any]:
    """Load and parse a YAML file.
    
    Raises:
        yaml.YAMLError: If the YAML content is malformed.
    """
    try:
        with path.open("r", encoding="utf-8") as f:
            return yaml.safe_load(f) or {}
    except yaml.YAMLError as e:
        raise ValueError(f"Invalid YAML in {path}: {e}") from e
```
```

---

### [ERROR] Division by zero risk in solar penetration calculation

**File**: `src/quant_alpha/features/energy_alpha.py:87`
**Category**: Potential Bug
**Confidence**: 70%

The expression `df['solar_forecast'] / df['load_forecast'].clip(lower=1.0)` uses a lower clip of 1.0 which assumes load_forecast is in units where 1.0 is a reasonable minimum. If load_forecast is in GW (e.g., values like 1000), a clip of 1.0 is effectively zero protection. If it's in a normalized scale, it may be fine, but this magic number is fragile and unit-dependent. The registry expression `zscore(solar_forecast / load_forecast, 168)` has no clipping at all.

**Suggestion**:
```
Add a configurable minimum threshold or use a more robust protection:
```python
# Use a more robust lower bound based on the data scale
min_load = max(df['load_forecast'].quantile(0.01), 1e-6)
df['_solar_pen'] = df['solar_forecast'] / df['load_forecast'].clip(lower=min_load)
```
Or add a constant with documentation explaining the unit assumption.
```

---

### [ERROR] Division by zero in momentum calculation

**File**: `src/quant_alpha/features/energy_alpha.py:140-143`
**Category**: Potential Bug
**Confidence**: 85%

The expression `(s / s.shift(6) - 1).shift(1)` divides by `s.shift(6)` which can be zero or NaN. When spot_price is zero (e.g., during negative price periods common in energy markets with high renewables), this produces inf values rather than NaN, corrupting downstream zscore/statistics calculations.

**Suggestion**:
```
Replace zeros before division:
```python
df['alpha_energy_price_momentum_6h'] = grouped['spot_price'].transform(
    lambda s: (s / s.shift(6).replace(0, pd.NA) - 1).shift(1)
)
```
```

---

### [ERROR] Inconsistent NaN assignment when gas_price column missing

**File**: `src/quant_alpha/features/energy_alpha.py:148`
**Category**: Potential Bug
**Confidence**: 85%

When `gas_price` is not in columns, `df['alpha_energy_gas_spark_spread'] = pd.NA` assigns a scalar NA to the entire column. This creates an `object` dtype column with `pd.NA` values, whereas all other alpha columns will be `float64` with `np.nan`. This dtype inconsistency can cause issues in downstream consumers that expect uniform numeric columns.

**Suggestion**:
```
Use `np.nan` for consistency with float columns, or explicitly cast:
```python
import numpy as np
df['alpha_energy_gas_spark_spread'] = np.nan
```
Or better, use `pd.Series(np.nan, index=df.index)` to ensure proper alignment.
```

---

### [ERROR] Missing required column 'adj_close' KeyError risk

**File**: `src/quant_alpha/features/registry.py:67`
**Category**: Potential Bug
**Confidence**: 85%

Multiple alpha definitions reference x['adj_close'], x['open'], x['close'], x['high'], x['low'], x['volume'], x['ret_1d']. If the input DataFrame is missing any of these columns, a KeyError will be raised at compute time with no helpful error message indicating which column is expected.

**Suggestion**:
```
Add input validation at the top of each compute function or create a decorator that validates required columns:
```python
def require_columns(*cols):
    def decorator(fn):
        def wrapper(df):
            missing = set(cols) - set(df.columns)
            if missing:
                raise ValueError(f'Missing columns: {missing}')
            return fn(df)
        return wrapper
    return decorator
```
```

---

### [ERROR] No error handling for fetch_prices failure

**File**: `src/quant_alpha/ingestion/dlt_equity.py:23-62`
**Category**: Potential Bug
**Confidence**: 85%

fetch_prices could raise network errors, parsing errors, or return None/empty DataFrame. The code does not handle these cases, which would result in unhandled exceptions propagating to the caller.

**Suggestion**:
```
Add try/except around fetch_prices call and handle empty DataFrame case: `if prices is None or prices.empty: return`
```

---

### [ERROR] Missing error handling for network request

**File**: `src/quant_alpha/ingestion/yahoo.py:84-94`
**Category**: Potential Bug
**Confidence**: 90%

yf.download() can raise various exceptions (ConnectionError, Timeout, JSONDecodeError, etc.) that are not caught. This will result in unhandled exceptions propagating to the caller with potentially confusing stack traces.

**Suggestion**:
```
Wrap the download call in a try-except block:
```python
try:
    data = yf.download(...)
except Exception as e:
    raise RuntimeError(f"Failed to download data from Yahoo Finance: {e}") from e
```
```

---

### [ERROR] KeyError when accessing missing node dependencies

**File**: `src/quant_alpha/platform/bruin_graph.py:98-100`
**Category**: Potential Bug
**Confidence**: 90%

The upstream() method accesses self.nodes[name].depends without checking if 'name' exists in self.nodes. If a node has a dependency that references a non-existent asset, this will raise a KeyError.

**Suggestion**:
```
Add a guard: `if name not in self.nodes: return []` at the start of the method.
```

---

### [ERROR] Missing error handling for GCS upload and BigQuery load

**File**: `src/quant_alpha/storage/gcp.py:29-37`
**Category**: Potential Bug
**Confidence**: 95%

The function doesn't handle errors from blob.upload_from_filename() or load_job.result(). Network failures, permission errors, or BigQuery schema mismatches will raise unhandled exceptions that bypass cleanup of the temporary directory.

**Suggestion**:
```
Add try/except around the upload and load operations, or wrap the entire loop in try/except to ensure proper error reporting and cleanup:
```python
try:
    blob.upload_from_filename(str(local_path))
    load_job.result()
except Exception as exc:
    raise CloudExportError(f"Failed to export {table_name}: {exc}") from exc
```
```

---

### [ERROR] No error handling for write_table call

**File**: `src/quant_alpha/streaming/demo_signals.py:33-35`
**Category**: Potential Bug
**Confidence**: 70%

The `seed_demo_signals` function calls `write_table` without any try/except. If the DuckDB file is locked, the disk is full, or the schema conflicts with existing data, the function will propagate an unhandled exception with no context.

**Suggestion**:
```
Wrap in try/except with a meaningful error message:
```python
try:
    write_table(duckdb_path, "live_energy_signals", frame)
except Exception as e:
    raise RuntimeError(f"Failed to write signals to {duckdb_path}: {e}") from e
```
```

---

### [ERROR] Missing error handling for Kafka consumer operations

**File**: `src/quant_alpha/streaming/redpanda_consumer.py:45-53`
**Category**: Potential Bug
**Confidence**: 95%

The `consume_energy_signals` function creates a Kafka Consumer but doesn't handle potential exceptions during Consumer creation, subscription, or polling. Network issues, authentication failures, or topic not found errors will cause unhandled exceptions that bypass `consumer.close()`, potentially leaving the consumer in an inconsistent state.

**Suggestion**:
```
Wrap consumer operations in a try/finally block:
```python
def consume_energy_signals(...):
    consumer = None
    try:
        consumer = Consumer({...})
        consumer.subscribe([topic])
        # ... polling logic ...
        return messages
    finally:
        if consumer is not None:
            consumer.close()
```
```

---

### [ERROR] No error handling for Kafka produce failures

**File**: `src/quant_alpha/streaming/redpanda_producer.py:36-42`
**Category**: Potential Bug
**Confidence**: 80%

The producer.produce() call doesn't have error handling. If the Kafka broker is unavailable or the produce fails, the error will be silently lost or cause an unhandled exception.

**Suggestion**:
```
Add error callback or use delivery_report:
```python
def delivery_callback(err, msg):
    if err:
        print(f'Message delivery failed: {err}')
    else:
        print(f'Message delivered to {msg.topic()} [{msg.partition()}]')

producer.produce(topic, _serialize(schema, payload), callback=delivery_callback)
```
```

---

### [ERROR] No graceful shutdown handling

**File**: `src/quant_alpha/streaming/risingwave/producer.py:67-72`
**Category**: Potential Bug
**Confidence**: 85%

The producer runs an infinite loop with no signal handling for graceful shutdown. If the process is terminated, in-flight messages may be lost and the Kafka producer may not flush remaining messages.

**Suggestion**:
```
Add signal handling:
```python
import signal
import sys

def _shutdown_handler(signum, frame):
    print('[producer] shutting down...')
    sys.exit(0)

if __name__ == '__main__':
    signal.signal(signal.SIGINT, _shutdown_handler)
    signal.signal(signal.SIGTERM, _shutdown_handler)
    p = _make_producer()
    stream_signals(p)
```
```

---

### [ERROR] Flaky test due to assertion order

**File**: `tests/test_bruin_graph.py:39-52`
**Category**: Potential Bug
**Confidence**: 90%

The test `test_topological_order_respects_dependencies` makes multiple assertions about topological ordering. If the first assertion fails, the test stops there and doesn't check the remaining dependencies. This makes debugging harder as you don't know which specific dependency is violated.

**Suggestion**:
```
Use pytest's `assert` or collect all failures:
```python
def test_topological_order_respects_dependencies() -> None:
    graph = AssetGraph(BRUIN_ROOT)
    order = graph.topological_order()
    
    def pos(name: str) -> int:
        return order.index(name) if name in order else -1
    
    dependencies = [
        ("raw_equity_ohlcv", "stg_equity_ohlcv"),
        ("raw_power_market", "stg_power_market"),
        ("stg_equity_ohlcv", "fct_equity_alpha_panel"),
        ("stg_power_market", "fct_energy_alpha_panel"),
        ("fct_equity_alpha_panel", "fct_alpha_diagnostics"),
    ]
    
    for before, after in dependencies:
        assert pos(before) < pos(after), f"{before} should come before {after}"
```
```

---

## WARNING Issues (190)

### [WARNING] No main guard or entry point protection

**File**: `bruin/pipelines/energy_ingestion/run_energy_ingestion.py:1-16`
**Category**: Readability
**Confidence**: 90%

All code executes at module level without a `if __name__ == "__main__":` guard. This means importing this module will immediately execute the pipeline, making it impossible to import without side effects and difficult to test.

**Suggestion**:
```
Wrap execution logic in a main guard:
```python
def main():
    root = Path(os.environ.get("PROJECT_ROOT", "."))
    config = root / "configs" / "second_foundation_project.yaml"
    source = os.environ.get("ENERGY_SOURCE", None)
    result = run_energy_pipeline(config, root.resolve(), source_override=source)
    print(f"raw_power_market: {result['rows']} rows → {result['duckdb_path']}")
    print(f"data_source: {result['data_source']}")

if __name__ == "__main__":
    main()
```
```

---

### [WARNING] Unused import: load_project_config

**File**: `bruin/pipelines/energy_ingestion/run_energy_ingestion.py:6-7`
**Category**: Code Style
**Confidence**: 95%

The function `load_project_config` is imported from `quant_alpha.config` but never used anywhere in the file.

**Suggestion**:
```
Remove the unused import: delete `from quant_alpha.config import load_project_config`
```

---

### [WARNING] Missing error handling for environment variable fallback

**File**: `bruin/pipelines/energy_ingestion/run_energy_ingestion.py:9`
**Category**: Potential Bug
**Confidence**: 70%

When `PROJECT_ROOT` is not set, the code falls back to the current directory (`.`). This could lead to unexpected behavior if the script is run from an unintended directory, or could silently fail to find the config file.

**Suggestion**:
```
Consider logging a warning when using the default path, or validate that the directory exists:
```python
root = Path(os.environ.get("PROJECT_ROOT", "."))
if not root.exists():
    raise EnvironmentError(f"Project root directory not found: {root}")
```
```

---

### [WARNING] Unicode character in print statement

**File**: `bruin/pipelines/energy_ingestion/run_energy_ingestion.py:14`
**Category**: Code Style
**Confidence**: 60%

The print statement uses a Unicode arrow character (→) which may not render correctly in all terminal environments or log files, potentially causing encoding errors.

**Suggestion**:
```
Use ASCII alternative for broader compatibility:
```python
print(f"raw_power_market: {result['rows']} rows -> {result['duckdb_path']}")
```
```

---

### [WARNING] No validation of result data types

**File**: `bruin/pipelines/energy_ingestion/run_energy_ingestion.py:14-15`
**Category**: Potential Bug
**Confidence**: 65%

The code assumes `result['rows']` is numeric and `result['duckdb_path']` is a string. If these values have unexpected types, the print statements could fail or produce misleading output.

**Suggestion**:
```
Add type validation:
```python
if not isinstance(result.get('rows'), (int, float)):
    raise TypeError(f"Expected numeric rows count, got {type(result['rows'])}")
if not isinstance(result.get('duckdb_path'), str):
    raise TypeError(f"Expected string duckdb_path, got {type(result['duckdb_path'])}")
```
```

---

### [WARNING] BETWEEN filter may exclude valid extreme price data

**File**: `bruin/pipelines/energy_ingestion/stg_power_market.sql:35-36`
**Category**: Potential Bug
**Confidence**: 75%

The filter `spot_price BETWEEN -500 AND 3000` silently drops records with spot prices outside this range. In extreme market conditions (e.g., the 2022 European energy crisis), spot prices have exceeded 3000 €/MWh. Negative prices below -500 are also possible in some markets. These filtered rows are lost without logging, which could bias demand_surprise and gas_spark_spread calculations.

**Suggestion**:
```
Document the rationale for these bounds, consider widening them, or log excluded records for audit:
```sql
-- Reasonable market price bounds; records outside range are extreme outliers or data errors
-- Spot prices: historical range observed [-400, 4000] €/MWh
WHERE spot_price IS NOT NULL
  AND spot_price BETWEEN -1000 AND 5000
```
```

---

### [WARNING] demand_surprise can be misleading when actual_load is imputed

**File**: `bruin/pipelines/energy_ingestion/stg_power_market.sql:35`
**Category**: Potential Bug
**Confidence**: 80%

When actual_load IS NULL, the query substitutes load_forecast (line 30: COALESCE(actual_load, load_forecast)), making demand_surprise always 0. This masks genuine cases where actual load data is unavailable vs. cases where demand exactly matched forecast. Downstream consumers cannot distinguish real surprise=0 from imputed=0.

**Suggestion**:
```
Consider either: (1) leaving demand_surprise as NULL when actual_load is missing, or (2) adding a boolean flag column:
```sql
COALESCE(actual_load, load_forecast)                    AS actual_load,
CASE WHEN actual_load IS NOT NULL THEN actual_load - load_forecast END AS demand_surprise,
(actual_load IS NULL)                                      AS actual_load_imputed,
```
```

---

### [WARNING] Hardcoded magic number for gas price fallback

**File**: `bruin/pipelines/energy_ingestion/stg_power_market.sql:37`
**Category**: Potential Bug
**Confidence**: 85%

The value 35.0 is used as a fallback when gas_price is NULL in the gas_spark_spread calculation. This magic number has no explanation and could produce misleading results if the default doesn't reflect actual market conditions. Changes in gas market fundamentals would silently corrupt downstream analytics.

**Suggestion**:
```
Document the rationale for 35.0 in a comment, or better yet, calculate it from historical data:
```sql
-- Fallback to trailing 90-day median gas price if unavailable
spot_price - COALESCE(gas_price, (SELECT PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY gas_price) FROM power_market_raw WHERE gas_price IS NOT NULL)) AS gas_spark_spread
```
Or at minimum add a comment:
```sql
spot_price - COALESCE(gas_price, 35.0) AS gas_spark_spread  -- 35.0: EU TTF ~median 2023
```
```

---

### [WARNING] QUALIFY window function deduplication may drop valid rows

**File**: `bruin/pipelines/energy_ingestion/stg_power_market.sql:43`
**Category**: Potential Bug
**Confidence**: 90%

The QUALIFY clause uses `ROW_NUMBER() OVER (PARTITION BY timestamp, market ORDER BY timestamp) = 1`. The ORDER BY is on the same columns as PARTITION BY, making the ordering non-deterministic for duplicate (timestamp, market) pairs. This means which duplicate row survives is arbitrary and non-reproducible, potentially causing inconsistent results across runs.

**Suggestion**:
```
Add a meaningful ordering criterion to ensure deterministic deduplication, such as preferring the most recently ingested record:
```sql
QUALIFY ROW_NUMBER() OVER (PARTITION BY timestamp, market ORDER BY ingested_at DESC) = 1
```
Or if no better column exists:
```sql
QUALIFY ROW_NUMBER() OVER (PARTITION BY timestamp, market ORDER BY spot_price DESC) = 1
```
```

---

### [WARNING] ORDER BY may cause unnecessary sorting on large datasets

**File**: `bruin/pipelines/energy_ingestion/stg_power_market.sql:43`
**Category**: Performance
**Confidence**: 65%

The `ORDER BY market, timestamp` at the end of the query forces a full sort of the result set. For staging tables that will be consumed programmatically (not for presentation), this sort adds unnecessary I/O and CPU cost, especially on large time-series datasets.

**Suggestion**:
```
Remove the ORDER BY if the table is only consumed by downstream models/queries (which should handle their own ordering). If ordering is required for incremental processing, consider relying on the table's partitioning/clustering instead:
```sql
-- Remove ORDER BY for staging table; downstream queries should ORDER as needed
```
```

---

### [WARNING] Unused imports load_project_config and load_universe

**File**: `bruin/pipelines/equity_ingestion/run_equity_ingestion.py:7`
**Category**: Code Style
**Confidence**: 99%

load_project_config and load_universe are imported from quant_alpha.config but never used in the code.

**Suggestion**:
```
Remove the unused imports:
```python
from quant_alpha.config import load_project_config, load_universe  # Remove this line
```
```

---

### [WARNING] ORDER BY may be unnecessary in staging table

**File**: `bruin/pipelines/equity_ingestion/stg_equity_ohlcv.sql:34-35`
**Category**: Performance
**Confidence**: 60%

The final ORDER BY symbol, date on lines 34-35 will force a sort on the entire result set. For large equity datasets (thousands of symbols × years of daily data), this can be expensive. If this is a staging table consumed by downstream queries that impose their own ordering, the sort here may be wasted work.

**Suggestion**:
```
Consider removing the ORDER BY clause if downstream consumers don't require pre-sorted output. If ordering is needed for debugging/inspection, document that intent with a comment:
```sql
-- Ordered for deterministic output and downstream debugging
ORDER BY symbol, date
```
```

---

### [WARNING] QUALIFY may not be supported in DuckDB

**File**: `bruin/pipelines/equity_ingestion/stg_equity_ohlcv.sql:37`
**Category**: Potential Bug
**Confidence**: 85%

The QUALIFY clause is a proprietary extension (DuckDB does support it, but it's not standard SQL). If this query is ever ported to another database engine (Postgres, BigQuery, etc.), it will fail. Also, the ROW_NUMBER partition uses only 'date' and 'symbol' but ORDER BY 'date', which means the ordering is redundant within the partition — the row selected is non-deterministic among true duplicates.

**Suggestion**:
```
Make the deduplication deterministic by adding a tiebreaker column (e.g., loaded_at or row hash):
```sql
QUALIFY ROW_NUMBER() OVER (PARTITION BY date, symbol ORDER BY date, loaded_at DESC) = 1
```
Or use a CTE/subquery for portability:
```sql
WITH ranked AS (
  SELECT *, ROW_NUMBER() OVER (PARTITION BY date, symbol ORDER BY date) AS rn
  FROM raw_prices
  WHERE adj_close IS NOT NULL AND adj_close > 0
)
SELECT * EXCEPT (rn) FROM ranked WHERE rn = 1
ORDER BY symbol, date
```
```

---

### [WARNING] Table name mismatch with asset dependency

**File**: `bruin/pipelines/equity_ingestion/stg_equity_ohlcv.sql:38`
**Category**: Potential Bug
**Confidence**: 90%

The asset metadata declares a dependency on 'raw_equity_ohlcv' (line 8), but the FROM clause references 'raw_prices' (line 38). This is likely a mismatch — either the dependency name is wrong or the table reference is wrong, which will cause a runtime error or query the wrong table.

**Suggestion**:
```
Either change line 8 to 'raw_prices' or change line 38 to reference 'raw_equity_ohlcv':
```
FROM raw_equity_ohlcv
```
```

---

### [WARNING] Undeclared dependency on backtest_daily

**File**: `bruin/pipelines/reporting/rpt_backtest_summary.sql:26`
**Category**: Architecture
**Confidence**: 90%

The query references backtest_daily (line 42) but this table is not listed in the depends metadata section. This will cause the dependency graph to be incomplete and may lead to stale data if backtest_daily hasn't been refreshed.

**Suggestion**:
```
Add backtest_daily to the depends list:
```
depends:
  - fct_alpha_diagnostics
  - backtest_daily
```
```

---

### [WARNING] Declared dependency on fct_equity_alpha_panel is unused

**File**: `bruin/pipelines/reporting/rpt_backtest_summary.sql:27`
**Category**: Architecture
**Confidence**: 95%

The asset metadata declares a dependency on fct_equity_alpha_panel, but this table is never referenced in the SQL query. Only fct_alpha_diagnostics and backtest_daily are used. This creates a misleading dependency graph.

**Suggestion**:
```
Remove the unused dependency from the metadata:
```
depends:
  - fct_alpha_diagnostics
```
Or add the missing JOIN to fct_equity_alpha_panel if it should be used.
```

---

### [WARNING] Window function missing frame specification

**File**: `bruin/pipelines/reporting/rpt_backtest_summary.sql:37-41`
**Category**: Potential Bug
**Confidence**: 75%

The SUM() OVER window function uses ORDER BY but no explicit ROWS/RANGE clause. In DuckDB (and standard SQL), this defaults to RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW, which may behave unexpectedly if there are multiple rows with the same date for the same alpha_name (they would all get the same cumulative sum).

**Suggestion**:
```
Add an explicit frame clause if you want a true running sum per-row:
```sql
SUM(b.daily_pnl) OVER (
    PARTITION BY d.alpha_name ORDER BY b.date
    ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
) AS cumulative_pnl,
```
Or document the intended behavior with a comment.
```

---

### [WARNING] Missing NULL handling for daily_pnl

**File**: `bruin/pipelines/reporting/rpt_backtest_summary.sql:39`
**Category**: Potential Bug
**Confidence**: 70%

The cumulative_pnl calculation uses SUM(b.daily_pnl) OVER (...). If daily_pnl contains NULL values, the SUM window function will silently skip them, which may produce cumulative values that don't match expectations. The column metadata also lacks a not_null check on daily_pnl.

**Suggestion**:
```
Add a not_null check on daily_pnl in the column metadata:
```
- name: daily_pnl
  description: Daily strategy P&L
  checks: [not_null]
```
Or use COALESCE:
```sql
SUM(COALESCE(b.daily_pnl, 0)) OVER (...)
```
```

---

### [WARNING] Missing model description/documentation

**File**: `dbt_energy_alpha/models/marts/fct_energy_alpha_decay.sql:1-15`
**Category**: Readability
**Confidence**: 85%

The dbt model lacks a description block explaining its purpose, what 'ic' represents, or the business logic for the ic_regime thresholds (0.02 and -0.02).

**Suggestion**:
```
Add a dbt model description in YAML or a SQL comment block:
```sql
{{ config(materialized="table") }}

-- Model: fct_energy_alpha_decay
-- Description: Transforms energy alpha decay data with information coefficient regime classification.
-- IC thresholds: > 0.02 = positive, < -0.02 = negative, otherwise flat.

select
...
```
```

---

### [WARNING] Hardcoded magic numbers for IC thresholds

**File**: `dbt_energy_alpha/models/marts/fct_energy_alpha_decay.sql:8-10`
**Category**: Potential Bug
**Confidence**: 80%

The thresholds 0.02 and -0.02 for classifying the ic_regime are hardcoded magic numbers. If these thresholds need to be adjusted, it requires modifying the SQL. This also makes the business logic less transparent and harder to audit.

**Suggestion**:
```
Extract these to dbt variables or a configuration table:
```sql
{% set positive_threshold = var('ic_positive_threshold', 0.02) %}
{% set negative_threshold = var('ic_negative_threshold', -0.02) %}

select
    alpha_name,
    horizon_hours,
    ic,
    case
        when ic > {{ positive_threshold }} then 'positive'
        when ic < {{ negative_threshold }} then 'negative'
        else 'flat'
    end as ic_regime,
    current_timestamp as refreshed_at
from {{ source('energy_alpha', 'energy_alpha_decay') }}
where ic is not null
order by alpha_name, horizon_hours
```
```

---

### [WARNING] ORDER BY may be unnecessary in table materialization

**File**: `dbt_energy_alpha/models/marts/fct_energy_alpha_decay.sql:15`
**Category**: Performance
**Confidence**: 70%

Using ORDER BY in a materialized table does not guarantee row ordering when the table is queried later. The ORDER BY only affects the insert order, which may not be meaningful for downstream queries. This could add unnecessary sorting overhead during model build.

**Suggestion**:
```
Remove the ORDER BY unless there's a specific reason for it, and handle ordering in downstream queries or use clustered tables:
```sql
-- Remove this line
-- order by alpha_name, horizon_hours
```
```

---

### [WARNING] Missing model documentation

**File**: `dbt_energy_alpha/models/marts/fct_energy_alpha_diagnostics.sql:1-10`
**Category**: Readability
**Confidence**: 85%

The model lacks dbt documentation block (description) and column descriptions. For a diagnostics model that contains performance metrics like IC means, Sharpe ratios, and drawdowns, documentation is crucial for downstream consumers to understand what each metric represents and how it's calculated.

**Suggestion**:
```
Add a schema.yml file or model description:
```yaml
models:
  - name: fct_energy_alpha_diagnostics
    description: >
      Fact table containing diagnostic metrics for energy alpha strategies.
      Includes in-sample/out-of-sample IC means, consistency and robustness scores.
    columns:
      - name: alpha_name
        description: Unique identifier for the alpha strategy
      - name: is_ic_mean
        description: In-sample information coefficient mean
      - name: oos_ic_mean
        description: Out-of-sample information coefficient mean
      # ... add all column descriptions
```
```

---

### [WARNING] Missing dbt model configuration

**File**: `dbt_energy_alpha/models/marts/fct_energy_backtest_daily.sql:1-9`
**Category**: Convention
**Confidence**: 80%

The model file lacks dbt configuration block. No materialization strategy is specified (table, view, incremental), which will default to 'view'. For a fact table named 'fct_energy_backtest_daily', 'table' or 'incremental' materialization would be more appropriate for performance.

**Suggestion**:
```
Add a config block at the top:
{{
    config(
        materialized='table',
        tags=['fact', 'energy', 'backtest']
    )
}}
```

---

### [WARNING] No precision specification for financial columns

**File**: `dbt_energy_alpha/models/marts/fct_energy_backtest_daily.sql:3`
**Category**: Potential Bug
**Confidence**: 60%

Financial columns (gross_return, transaction_cost, portfolio_return, equity_curve) are selected without explicit data type casting or precision specification. Depending on the source, these may have inconsistent precision that could cause rounding issues in downstream calculations.

**Suggestion**:
```
Consider casting financial columns to appropriate precision:
cast(gross_return as decimal(18,8)) as gross_return,
cast(transaction_cost as decimal(18,8)) as transaction_cost
```

---

### [WARNING] Missing dbt model configuration block

**File**: `dbt_energy_alpha/models/marts/fct_energy_market_quality.sql:1-8`
**Category**: Convention
**Confidence**: 85%

This dbt model lacks a configuration block at the top (e.g., materialization, schema, tags, description). For a mart model (fct_), this is typically expected to have explicit configuration.

**Suggestion**:
```
Add a config block at the top of the file:

```sql
{{
    config(
        materialized='table',
        schema='marts',
        tags=['energy_market', 'quality']
    )
}}

select
    "check" as check_name,
    ...
```
```

---

### [WARNING] Missing model documentation

**File**: `dbt_energy_alpha/models/marts/fct_energy_market_quality.sql:1-8`
**Category**: Convention
**Confidence**: 90%

The dbt model has no description or column-level documentation defined in a YAML file. For a mart-level model, this is important for data discoverability and governance.

**Suggestion**:
```
Create or update a schema YAML file to document this model:

```yaml
models:
  - name: fct_energy_market_quality
    description: "Fact table containing energy market quality checks"
    columns:
      - name: check_name
        description: "Name of the quality check performed"
      - name: column_name
        description: "Column being checked"
      # ... document other columns
```
```

---

### [WARNING] No null handling or data type casting

**File**: `dbt_energy_alpha/models/marts/fct_energy_market_quality.sql:1-8`
**Category**: Potential Bug
**Confidence**: 70%

The model selects columns directly without any NULL handling, type casting, or filtering logic. Columns like 'passed', 'nulls', and 'duplicates' may contain NULLs or unexpected values that could affect downstream consumers.

**Suggestion**:
```
Consider adding COALESCE or explicit type casts where appropriate:

```sql
select
    "check" as check_name,
    "column" as column_name,
    keys as key_columns,
    coalesce(passed, false) as passed,
    coalesce(nulls, 0) as nulls,
    coalesce(duplicates, 0) as duplicates
from {{ source('energy_raw', 'power_market_quality') }}
```
```

---

### [WARNING] Ambiguous column name 'keys'

**File**: `dbt_energy_alpha/models/marts/fct_energy_market_quality.sql:4`
**Category**: Readability
**Confidence**: 60%

The column 'keys' is aliased to 'key_columns' in the select, which is good. However, 'keys' is also a reserved word in some SQL dialects. Verify this is the actual column name in the source table.

**Suggestion**:
```
If 'keys' is a reserved word in the target database, quote it:

```sql
"keys" as key_columns,
```
```

---

### [WARNING] Filter excludes NULL forward_return values

**File**: `dbt_energy_alpha/models/staging/stg_energy_alphas.sql:10`
**Category**: Potential Bug
**Confidence**: 60%

The WHERE clause filters on `alpha_composite IS NOT NULL` which implicitly excludes all rows where forward_return IS NULL as well. This may unintentionally drop valid rows where alpha metrics exist but forward_return is missing. Missing forward_return data could indicate future observations that haven't been realized yet, which could be important for training vs. inference datasets.

**Suggestion**:
```
Consider whether the filter intent is correct. If forward_return can legitimately be NULL for inference rows, add an explicit comment:
```sql
where alpha_composite is not null
-- Note: forward_return may be NULL for future/inference rows; filtered rows have all required fields
```
Or if only non-NULL forward_return rows are needed, make the intent explicit:
```sql
where alpha_composite is not null
  and forward_return is not null
```
```

---

### [WARNING] No NULL handling for critical numeric columns

**File**: `dbt_energy_alpha/models/staging/stg_power_market.sql:9`
**Category**: Potential Bug
**Confidence**: 80%

Columns like spot_price, load_forecast, wind_forecast, solar_forecast, residual_load, and imbalance_price are selected without NULL handling. If these contain NULLs, downstream calculations and aggregations may produce unexpected results or errors.

**Suggestion**:
```
Add COALESCE or NULL handling for numeric columns:
```sql
coalesce(spot_price, 0) as spot_price,
coalesce(load_forecast, 0) as load_forecast,
-- etc.
```
```

---

### [WARNING] Hardcoded magic numbers in CASE statement

**File**: `dbt_quant_alpha/models/marts/fct_alpha_decay.sql:8-12`
**Category**: Readability
**Confidence**: 70%

The threshold values 0.02 and -0.02 are hardcoded directly in the SQL. These appear to be business logic thresholds for classifying IC regimes but lack documentation or configurability.

**Suggestion**:
```
Consider using a dbt variable or referencing a configuration table to make thresholds configurable and documented. Example: {{ var('ic_positive_threshold', 0.02) }} and {{ var('ic_negative_threshold', -0.02) }}
```

---

### [WARNING] Missing dbt model documentation

**File**: `dbt_quant_alpha/models/marts/fct_alpha_diagnostics.sql:1-10`
**Category**: Convention
**Confidence**: 85%

This dbt model has no description or column-level documentation defined in a YAML schema file. dbt best practices recommend documenting all models, especially marts, to maintain data lineage and discoverability.

**Suggestion**:
```
Add a schema.yml entry for this model:
```yaml
models:
  - name: fct_alpha_diagnostics
    description: "Fact table containing alpha diagnostic metrics including IC means, consistency scores, robustness scores, Sharpe ratios, and max drawdowns."
    columns:
      - name: alpha_name
        description: "Unique identifier for the alpha strategy"
      - name: is_ic_mean
        description: "In-sample information coefficient mean"
      # ... add descriptions for all columns
```
```

---

### [WARNING] Missing model configuration block

**File**: `dbt_quant_alpha/models/marts/fct_alpha_diagnostics.sql:1-10`
**Category**: Convention
**Confidence**: 80%

The model lacks a dbt configuration block specifying materialization strategy, schema, or other settings. Mart models typically should be materialized as tables or incremental models for performance.

**Suggestion**:
```
Add a config block at the top:
```sql
{{ config(
    materialized='table',
    schema='marts'
) }}

select
    alpha_name,
    ...
```
```

---

### [WARNING] Missing trailing semicolon

**File**: `dbt_quant_alpha/models/marts/fct_alpha_diagnostics.sql:10`
**Category**: Potential Bug
**Confidence**: 60%

SQL statement is missing a terminating semicolon. While some SQL dialects and dbt may not require it, best practice is to include it for clarity and portability.

**Suggestion**:
```
Add a semicolon at the end of the query:
```sql
from {{ source('quant_alpha_raw', 'alpha_diagnostics') }}
;
```
```

---

### [WARNING] Misleading column name for correlation metric

**File**: `dbt_quant_alpha/models/marts/fct_alpha_panel.sql:11-15`
**Category**: Readability
**Confidence**: 90%

The column is named 'rolling_63d_rank_ic_proxy' but it computes Pearson correlation (corr()), not rank correlation (Spearman). Rank IC typically refers to Spearman rank correlation. This naming could mislead users into thinking it's a rank-based metric when it's actually a linear correlation.

**Suggestion**:
```
Rename the column to accurately reflect the metric:
```sql
    ) as rolling_63d_pearson_ic_proxy
```
Or if rank IC is truly intended, convert to rank first:
```sql
    corr(
        rank() over (order by alpha_composite),
        rank() over (order by forward_return)
    ) over (...)
```
```

---

### [WARNING] Window function missing PARTITION BY clause

**File**: `dbt_quant_alpha/models/marts/fct_alpha_panel.sql:12-15`
**Category**: Potential Bug
**Confidence**: 80%

The rolling correlation window orders by signal_date but doesn't partition by symbol. If the upstream table contains multiple symbols, this will compute correlation across all symbols mixed together in time order, which is likely not the intended behavior. Cross-sectional correlations would pollute the time-series correlation for each symbol.

**Suggestion**:
```
If the intent is per-symbol rolling correlation, add PARTITION BY:
```sql
    corr(alpha_composite, forward_return) over (
        partition by symbol
        order by signal_date
        rows between 62 preceding and current row
    ) as rolling_63d_rank_ic_proxy
```
If cross-sectional correlation is intended, add a comment to clarify.
```

---

### [WARNING] Rolling window may include insufficient data

**File**: `dbt_quant_alpha/models/marts/fct_alpha_panel.sql:14-17`
**Category**: Potential Bug
**Confidence**: 85%

The rolling correlation window uses 'rows between 62 preceding and current row', which requires at least 63 rows of data. Early rows with fewer preceding rows will still compute correlation with incomplete windows, potentially producing misleading values. The first 62 rows will have inflated correlation estimates due to small sample size.

**Suggestion**:
```
Consider filtering to only include rows where sufficient historical data exists, or add a comment acknowledging this edge case:
```sql
-- Note: First 62 rows will have fewer observations in window
-- Consider adding WHERE clause to filter early rows if needed
```
```

---

### [WARNING] Only alpha_composite null check missing forward_return

**File**: `dbt_quant_alpha/models/marts/fct_alpha_panel.sql:17`
**Category**: Potential Bug
**Confidence**: 75%

The WHERE clause filters out rows where alpha_composite IS NULL, but doesn't filter forward_return IS NULL. The corr() function will skip NULL pairs anyway, but having NULL forward_return values in the output alongside non-null alpha_composite values may confuse downstream consumers.

**Suggestion**:
```
Add forward_return to the null filter:
```sql
where alpha_composite is not null
  and forward_return is not null
```
```

---

### [WARNING] Missing null handling and data validation

**File**: `dbt_quant_alpha/models/marts/fct_backtest_daily.sql:1-9`
**Category**: Potential Bug
**Confidence**: 70%

The SELECT statement does not handle NULL values or validate that critical numeric columns (gross_return, transaction_cost, portfolio_return, equity_curve, long_count, short_count) contain valid data. NULL propagation in calculations could cause downstream issues in reporting or analytics.

**Suggestion**:
```
Add NULL handling or COALESCE for critical columns where appropriate:
```sql
select
    cast(date as date) as signal_date,
    coalesce(gross_return, 0) as gross_return,
    coalesce(transaction_cost, 0) as transaction_cost,
    coalesce(portfolio_return, 0) as portfolio_return,
    coalesce(equity_curve, 0) as equity_curve,
    coalesce(long_count, 0) as long_count,
    coalesce(short_count, 0) as short_count
from {{ source('quant_alpha_raw', 'backtest_daily') }}
```
```

---

### [WARNING] Potential null values in date column not handled

**File**: `dbt_quant_alpha/models/staging/stg_factor_panel.sql:2`
**Category**: Potential Bug
**Confidence**: 70%

The CAST(date AS date) operation assumes the 'date' column always contains valid date values. If there are nulls or invalid date formats, this could cause errors or unexpected results in downstream transformations.

**Suggestion**:
```
Consider adding null handling or validation:
```sql
cast(case 
  when date is not null and date != '' then date 
  else null 
end as date) as signal_date,
```
```

---

### [WARNING] Symbol column may contain null or empty values

**File**: `dbt_quant_alpha/models/staging/stg_factor_panel.sql:3`
**Category**: Potential Bug
**Confidence**: 60%

The symbol column is selected without any validation. In financial data, null or empty symbols could indicate data quality issues that should be flagged or filtered.

**Suggestion**:
```
Consider adding validation or filtering:
```sql
where symbol is not null and symbol != ''
```
```

---

### [WARNING] Missing null handling for critical columns

**File**: `dbt_quant_alpha/models/staging/stg_prices.sql:1-10`
**Category**: Potential Bug
**Confidence**: 80%

The model does not handle null values in critical numeric columns (open, high, low, close, adj_close, volume). Null values in price data could cause calculation errors in downstream models that perform arithmetic operations on these fields.

**Suggestion**:
```
Add COALESCE or CASE statements to handle nulls appropriately, e.g.:\nselect\n    cast(date as date) as price_date,\n    symbol,\n    coalesce(open, 0) as open,\n    coalesce(high, 0) as high,\n    coalesce(low, 0) as low,\n    coalesce(close, 0) as close,\n    coalesce(adj_close, close, 0) as adj_close,\n    coalesce(volume, 0) as volume\nfrom {{ source('quant_alpha_raw', 'raw_prices') }}
```

---

### [WARNING] Hardcoded version string

**File**: `src/quant_alpha/__init__.py:5`
**Category**: Potential Bug
**Confidence**: 70%

The version string '0.1.0' is hardcoded in the module. This can lead to version management issues as the package grows and needs to be updated in multiple places.

**Suggestion**:
```
Consider using a version management tool like `setuptools` with `pyproject.toml` or `setup.cfg`, or at minimum store the version in a separate file that can be imported:
```python
# version.py
__version__ = "0.1.0"

# __init__.py
from .version import __version__
```
```

---

### [WARNING] Missing validation of required columns

**File**: `src/quant_alpha/backtest/alpha_decay.py:59`
**Category**: Potential Bug
**Confidence**: 85%

The function `compute_energy_alpha_decay` assumes columns 'timestamp', 'market', 'spot_price' exist without validation, unlike `_ic_at_horizon` which checks for `price_col`. This will raise a KeyError if these columns are missing.

**Suggestion**:
```
Add column validation at the start: `required = {'timestamp', 'market', 'spot_price', alpha_col}; if not required.issubset(panel.columns): return pd.DataFrame(...)` or raise a descriptive error.
```

---

### [WARNING] Forward return calculation may be incorrect

**File**: `src/quant_alpha/backtest/alpha_decay.py:69-73`
**Category**: Potential Bug
**Confidence**: 70%

The forward return calculation `(s.shift(-h) / s.abs().clip(lower=20.0)) - (s / s.abs().clip(lower=20.0))` appears to be calculating `(P_{t+h} / |P_t|_clipped) - (P_t / |P_t|_clipped)` instead of a standard forward return like `(P_{t+h} - P_t) / P_t`. The clipping to absolute value with a floor of 20.0 seems arbitrary and may distort returns when spot_price is negative or near zero.

**Suggestion**:
```
Clarify the intended forward return calculation. If you want standard forward returns: `(s.shift(-h) / s) - 1`. If energy prices can be negative, consider using: `(s.shift(-h) - s) / s.abs().clip(lower=20.0)` for a normalized price change.
```

---

### [WARNING] Magic number 20.0 in clip threshold

**File**: `src/quant_alpha/backtest/alpha_decay.py:69-73`
**Category**: Potential Bug
**Confidence**: 80%

The hardcoded value `20.0` in `s.abs().clip(lower=20.0)` is a magic number without explanation. This threshold determines when price changes are normalized, but its basis is unclear and could be inappropriate for different markets or price regimes.

**Suggestion**:
```
Extract this as a named constant with documentation explaining its derivation: `MIN_PRICE_THRESHOLD = 20.0  # Minimum price for return normalization to avoid extreme values`
```

---

### [WARNING] Unvalidated alpha_col column existence

**File**: `src/quant_alpha/backtest/alpha_decay.py:87`
**Category**: Potential Bug
**Confidence**: 90%

The function `walk_forward_ic` assumes `alpha_col` exists in the panel DataFrame without validation, which will raise a KeyError if it's missing.

**Suggestion**:
```
Add validation: `if alpha_col not in panel.columns: raise ValueError(f'Column {alpha_col} not found in panel')`
```

---

### [WARNING] Repeated pd.to_datetime conversions

**File**: `src/quant_alpha/backtest/alpha_decay.py:95-100`
**Category**: Performance
**Confidence**: 95%

In the walk_forward_ic function, `pd.to_datetime(panel[date_col])` is called twice inside the loop (lines 95-100) for filtering, creating unnecessary overhead. This should be computed once before the loop.

**Suggestion**:
```
Convert dates once before the loop: `panel_dates = pd.to_datetime(panel[date_col])` and reuse it in the filter condition.
```

---

### [WARNING] Missing 'forward_return' column assumption

**File**: `src/quant_alpha/backtest/alpha_decay.py:112`
**Category**: Potential Bug
**Confidence**: 85%

The `_daily_rank_ic` function assumes the panel contains a 'forward_return' column, but this is not documented or validated. If this column is missing, it will raise a KeyError.

**Suggestion**:
```
Add validation at the start: `if 'forward_return' not in panel.columns: raise ValueError('Panel must contain forward_return column')`
```

---

### [WARNING] Missing module and function docstrings

**File**: `src/quant_alpha/backtest/long_short.py:1-95`
**Category**: Convention
**Confidence**: 95%

Neither the module nor the public function `run_long_short_backtest` have docstrings. The function has a non-trivial signature and returns a tuple of results; callers would benefit from documentation explaining parameters, return values, and methodology.

**Suggestion**:
```
Add docstrings:
```python
"""Long-short backtest engine for factor-based portfolios."""

# ...

def run_long_short_backtest(
    factor_panel: pd.DataFrame,
    cfg: BacktestConfig,
    alpha_col: str = "alpha_composite",
) -> tuple[pd.DataFrame, dict[str, float]]:
    """Run a market-neutral long-short backtest.

    Args:
        factor_panel: DataFrame with columns [date, symbol, alpha_col, forward_return].
        cfg: Backtest configuration.
        alpha_col: Column name for the alpha signal.

    Returns:
        Tuple of daily results DataFrame and performance metrics dict.
    """
```
```

---

### [WARNING] Quantile cuts may overlap for uniform data

**File**: `src/quant_alpha/backtest/long_short.py:11-12`
**Category**: Potential Bug
**Confidence**: 75%

When alpha values are uniform or have many ties, top_cut may equal bottom_cut, causing the same assets to appear in both longs and shorts simultaneously. The >= and <= comparisons are inclusive, so assets exactly at the boundary could be selected for both sides.

**Suggestion**:
```
Use strict inequality for one side to avoid overlap:
```python
longs = day[day[alpha_col] > top_cut].copy()  # or
shorts = day[day[alpha_col] < bottom_cut].copy()
```
Or add a guard: `if top_cut <= bottom_cut: continue`
```

---

### [WARNING] Repeated DataFrame copying in loop

**File**: `src/quant_alpha/backtest/long_short.py:19`
**Category**: Performance
**Confidence**: 70%

Inside the groupby loop, `day.copy()` is called on line 12, then `longs.copy()` and `shorts.copy()` are called on lines 14-15. The `pd.concat` on line 18 with `assign(date=dt)` creates yet another copy. For large panels with many trading days and symbols, this creates significant memory pressure.

**Suggestion**:
```
Since `longs` and `shorts` are already copies from boolean indexing, the `.copy()` calls are redundant:
```python
# day = day.copy()  # only needed if modifying day in-place, which we don't
# day is only read, not written to
top_cut = day[alpha_col].quantile(1 - cfg.top_quantile)
bottom_cut = day[alpha_col].quantile(cfg.bottom_quantile)
longs = day[day[alpha_col] >= top_cut]  # already a new DataFrame
shorts = day[day[alpha_col] <= bottom_cut]
```
Only the `longs["weight"] = ...` lines modify in-place, which is fine on the sliced copies.
```

---

### [WARNING] Index alignment risk when assigning cost and counts

**File**: `src/quant_alpha/backtest/long_short.py:68`
**Category**: Potential Bug
**Confidence**: 75%

When assigning `daily['transaction_cost'] = cost` and `daily['long_count'] = counts.get(...)`, pandas aligns on index values. If there are dates in `gross` that are missing from `cost` or `counts` (or vice versa), NaN values will be inserted silently. This could happen if `weights` has dates without any longs or shorts, though the `_daily_weights` function filters for non-empty longs and shorts.

**Suggestion**:
```
Add an explicit reindex to ensure alignment:
```python
daily['transaction_cost'] = cost.reindex(daily.index, fill_value=0)
daily['long_count'] = counts.get('long', pd.Series(dtype=int)).reindex(daily.index, fill_value=0)
daily['short_count'] = counts.get('short', pd.Series(dtype=int)).reindex(daily.index, fill_value=0)
```
```

---

### [WARNING] Division by zero risk in Sortino calculation

**File**: `src/quant_alpha/backtest/long_short.py:84`
**Category**: Potential Bug
**Confidence**: 80%

While there's a check for `downside_std > 0`, `downside_std` could be extremely small (e.g., 1e-300) leading to an astronomically large Sortino ratio that is meaningless. The current guard only prevents exact zero.

**Suggestion**:
```
Use a practical epsilon threshold instead:
```python
sortino = float(ann_ret / (downside_std * np.sqrt(annualization))) if downside_std > 1e-9 else 0.0
```
This matches the pattern already used for the Calmar ratio check on line 85.
```

---

### [WARNING] Spark session config hardcoded for local mode

**File**: `src/quant_alpha/batch/spark_energy_features.py:6-13`
**Category**: Architecture
**Confidence**: 80%

.master('local[*]') is hardcoded, making this unsuitable for production cluster execution. The app_name also references a specific project name that may not match deployment context.

**Suggestion**:
```
Make master configurable: def build_spark_session(app_name: str = 'second-foundation-energy-batch', master: str = 'local[*]'): ... .master(master)
```

---

### [WARNING] Missing docstrings for public functions

**File**: `src/quant_alpha/batch/spark_energy_features.py:16-46`
**Category**: Convention
**Confidence**: 95%

Neither build_spark_session nor compute_energy_features have docstrings. compute_energy_features has non-obvious window calculations and magic numbers that need documentation.

**Suggestion**:
```
Add docstrings:
def compute_energy_features(input_path: str, output_path: str) -> None:
    """Compute rolling energy market features using Spark.
    
    Computes 24h/168h rolling statistics, spot returns, residual load shocks,
    imbalance premium, and scarcity flags.
    """
```

---

### [WARNING] No error handling for missing input path

**File**: `src/quant_alpha/batch/spark_energy_features.py:22`
**Category**: Potential Bug
**Confidence**: 90%

spark.read.parquet(input_path) will throw a cryptic Java exception if the path doesn't exist or is empty. No try/except wrapping provides a user-friendly error message.

**Suggestion**:
```
Add error handling: try:
    frame = spark.read.parquet(input_path)
except Exception as e:
    spark.stop()
    raise FileNotFoundError(f'Input parquet not found: {input_path}') from e
```

---

### [WARNING] Hardcoded relative path assumption fragile

**File**: `src/quant_alpha/batch/spark_energy_features.py:49-52`
**Category**: Potential Bug
**Confidence**: 70%

parents[3] assumes a specific directory depth from the file to the project root. If the file is moved or the project structure changes, this will break silently or point to wrong directories.

**Suggestion**:
```
Consider using a config file, environment variable, or __file__ relative with explicit project root marker (e.g., pyproject.toml lookup).
```

---

### [WARNING] Unused import

**File**: `src/quant_alpha/cli.py:14`
**Category**: Code Style
**Confidence**: 85%

The import 'from quant_alpha.ingestion.dlt_equity import run_dlt_equity_pipeline' is used only inside dlt_equity_command. It should be a lazy import like other internal imports in this file for consistency.

**Suggestion**:
```
Move the import inside the function:
```python
@app.command("dlt-equity")
def dlt_equity_command(...):
    from quant_alpha.ingestion.dlt_equity import run_dlt_equity_pipeline
    from quant_alpha.config import load_project_config, load_universe
    ...
```
```

---

### [WARNING] Missing error handling for run_pipeline

**File**: `src/quant_alpha/cli.py:16-20`
**Category**: Potential Bug
**Confidence**: 85%

The _run function calls run_pipeline but doesn't handle potential exceptions. If run_pipeline fails, the error will propagate without user-friendly context in the CLI output.

**Suggestion**:
```
Add try-except similar to _run_energy:
```python
def _run(config: Path, root: Path, offline: bool) -> None:
    try:
        result = run_pipeline(config, root.resolve(), offline=offline)
    except Exception as exc:
        typer.echo(f"Pipeline failed: {exc}", err=True)
        raise typer.Exit(code=1) from exc
    typer.echo("Pipeline finished.")
    ...
```
```

---

### [WARNING] Hardcoded config path in dlt-energy command

**File**: `src/quant_alpha/cli.py:82-90`
**Category**: Potential Bug
**Confidence**: 90%

The dlt_energy_command hardcodes 'configs/second_foundation_project.yaml' as the config path instead of accepting it as a parameter like other commands. This reduces flexibility and creates inconsistency.

**Suggestion**:
```
Add a config parameter:
```python
def dlt_energy_command(
    config: Path = typer.Option(
        Path("configs/second_foundation_project.yaml"),
        help="Energy project config YAML."
    ),
    root: Path = typer.Option(Path("."), help="Project root."),
    ...
) -> None:
    cfg = load_project_config(config, root=root.resolve())
```
```

---

### [WARNING] Inconsistent spacing in f-string

**File**: `src/quant_alpha/cli.py:87`
**Category**: Readability
**Confidence**: 70%

The f-string 'f"Dataset:  {info['dataset']} in {info['duckdb_path']}"' has double spaces after the colon, which may be intentional but looks inconsistent with other echo statements.

**Suggestion**:
```
Use single space for consistency:
```python
typer.echo(f"Dataset: {info['dataset']} in {info['duckdb_path']}")
```
```

---

### [WARNING] Inconsistent spacing in f-string

**File**: `src/quant_alpha/cli.py:104`
**Category**: Readability
**Confidence**: 70%

The f-string 'f"Dataset:  {info['dataset']} in {info['duckdb_path']}"' has double spaces after the colon, which may be intentional but looks inconsistent with other echo statements.

**Suggestion**:
```
Use single space for consistency:
```python
typer.echo(f"Dataset: {info['dataset']} in {info['duckdb_path']}")
```
```

---

### [WARNING] Line length exceeds recommended limit

**File**: `src/quant_alpha/cli.py:134`
**Category**: Readability
**Confidence**: 70%

The line 'targets: str | None = typer.Option(None, help="Comma-separated asset names to run (with upstream). Default: all.")' is long and may be hard to read in some editors.

**Suggestion**:
```
Break into multiple lines:
```python
targets: str | None = typer.Option(
    None,
    help="Comma-separated asset names to run (with upstream). Default: all."
)
```
```

---

### [WARNING] No file existence check before YAML loading

**File**: `src/quant_alpha/config.py:88`
**Category**: Potential Bug
**Confidence**: 70%

The load_yaml function opens a file without checking if it exists first. While Python will raise a FileNotFoundError, providing a more descriptive error message or handling this explicitly would improve robustness.

**Suggestion**:
```
Add existence check or better error handling:
```python
def load_yaml(path: Path) -> dict[str, Any]:
    if not path.exists():
        raise FileNotFoundError(f"Configuration file not found: {path}")
    with path.open("r", encoding="utf-8") as f:
        return yaml.safe_load(f) or {}
```
```

---

### [WARNING] Inconsistent date type handling

**File**: `src/quant_alpha/config.py:101`
**Category**: Convention
**Confidence**: 75%

start_date and end_date are stored as strings instead of date objects. This inconsistency could lead to parsing errors or manual string manipulation elsewhere in the codebase.

**Suggestion**:
```
Consider using datetime.date type with Pydantic's date parsing:
```python
class ProjectConfig(BaseModel):
    start_date: date = date(2021, 1, 1)
    end_date: date | None = None
```
```

---

### [WARNING] load_universe lacks path validation

**File**: `src/quant_alpha/config.py:109`
**Category**: Potential Bug
**Confidence**: 70%

The load_universe function accepts a Path argument but doesn't validate that the path exists or is a file before attempting to load it. This could result in unclear error messages.

**Suggestion**:
```
Add path validation:
```python
def load_universe(path: Path) -> Universe:
    if not path.is_file():
        raise FileNotFoundError(f"Universe file not found: {path}")
    return Universe(**load_yaml(path))
```
```

---

### [WARNING] Module-level side effect at import time

**File**: `src/quant_alpha/features/alpha_factors.py:10`
**Category**: Potential Bug
**Confidence**: 70%

BASE_FACTOR_COLUMNS calls make_equity_alpha_registry() at module import time. If that function has side effects (database calls, file I/O), it could fail during import or cause unexpected behavior in testing. This also means the list is frozen at import time and won't reflect any dynamic registry changes.

**Suggestion**:
```
Consider making this a lazy property or function call:
```python
def get_base_factor_columns() -> list[str]:
    return [alpha.name for alpha in make_equity_alpha_registry()]
```
```

---

### [WARNING] Unused helper functions

**File**: `src/quant_alpha/features/alpha_factors.py:13-22`
**Category**: Potential Bug
**Confidence**: 65%

Functions _rolling_zscore and _breakout_position are defined but never called within this module. They appear to be dead code or intended for use by alpha definitions but are not exported or referenced.

**Suggestion**:
```
Either remove these functions if unused, or ensure they are imported where needed. If they are utility functions for alpha definitions, consider moving them to a shared utilities module and importing them explicitly.
```

---

### [WARNING] No validation of input DataFrame columns

**File**: `src/quant_alpha/features/alpha_factors.py:25-55`
**Category**: Potential Bug
**Confidence**: 85%

add_alpha_factors assumes 'date', 'symbol', 'adj_close', and 'close' columns exist in the prices DataFrame. If any are missing, it will raise an unhandled KeyError.

**Suggestion**:
```
Add input validation at the start:
```python
def add_alpha_factors(prices: pd.DataFrame, cfg: ProjectConfig) -> pd.DataFrame:
    required = {'date', 'symbol', 'adj_close', 'close'}
    missing = required - set(prices.columns)
    if missing:
        raise ValueError(f"Missing required columns: {missing}")
    df = prices.copy()
```
```

---

### [WARNING] Silent error in alpha compute and reindex

**File**: `src/quant_alpha/features/alpha_factors.py:38-41`
**Category**: Potential Bug
**Confidence**: 60%

If alpha.compute() returns fewer indices than the original DataFrame (e.g., due to NaN handling in the compute function), .reindex(indexed.index) will fill missing values with NaN. This silently degrades data quality without any warning.

**Suggestion**:
```
Consider logging when reindex introduces NaN values:
```python
result = alpha.compute(indexed).reindex(indexed.index)
nan_count = result.isna().sum()
if nan_count > 0:
    logging.warning(f"Alpha '{alpha.name}' has {nan_count} NaN after reindex.")
df[alpha.name] = result.to_numpy()
```
```

---

### [WARNING] Inefficient grouped rank computation loop

**File**: `src/quant_alpha/features/alpha_factors.py:43-47`
**Category**: Performance
**Confidence**: 80%

Each alpha column is ranked individually in a loop with a separate groupby. This creates N separate groupby operations when a single groupby with .rank() on all columns would be more efficient.

**Suggestion**:
```
Batch the ranking:
```python
alpha_names = [alpha.name for alpha in registry]
ranked = df.groupby('date')[alpha_names].rank(pct=True)
ranked.columns = [f'{c}_rank' for c in alpha_names]
df = pd.concat([df, ranked], axis=1)
ranked_cols = list(ranked.columns)
```
```

---

### [WARNING] Division by zero in alpha composite

**File**: `src/quant_alpha/features/alpha_factors.py:48`
**Category**: Potential Bug
**Confidence**: 75%

If ranked_cols is empty (registry is empty), df[ranked_cols].mean(axis=1) will produce NaN for all rows, and the -0.5 subtraction will propagate NaN silently. There's no check for an empty registry.

**Suggestion**:
```
Add a guard:
```python
if not ranked_cols:
    raise ValueError("Alpha registry is empty; cannot compute composite.")
df['alpha_composite'] = df[ranked_cols].mean(axis=1) - 0.5
```
```

---

### [WARNING] _zscore returns pd.NA for zero std, may cause type issues

**File**: `src/quant_alpha/features/energy_alpha.py:49-53`
**Category**: Potential Bug
**Confidence**: 80%

In `_zscore`, `std.replace(0, pd.NA)` replaces zero standard deviations with pd.NA. Division by pd.NA returns pd.NA (not NaN), which creates object-typed results in some pandas versions. Downstream code that expects float columns (e.g., for correlation, ML models) may fail or behave unexpectedly with pd.NA mixed into a float Series.

**Suggestion**:
```
Use np.nan instead for consistency with float operations:
```python
import numpy as np
def _zscore(series: pd.Series, window: int) -> pd.Series:
    mean = series.rolling(window).mean()
    std = series.rolling(window).std()
    return (series - mean) / std.replace(0, np.nan)
```
```

---

### [WARNING] Hardcoded heat rate constant lacks documentation

**File**: `src/quant_alpha/features/energy_alpha.py:57`
**Category**: Potential Bug
**Confidence**: 80%

The magic number `2.0` in `df['spot_price'] - df['gas_price'] * 2.0` represents a simplified thermal efficiency / heat rate conversion but is undocumented. Different gas technologies have vastly different heat rates (CCGT ~1.8-2.0, OCGT ~3.0-4.0). This could silently produce incorrect spark spread calculations for markets dominated by different technologies.

**Suggestion**:
```
Extract to a named constant with documentation:
```python
# Simplified heat rate (MWh gas per MWh electricity) for CCGT
# Real implementations should use market-specific or time-varying heat rates
DEFAULT_HEAT_RATE = 2.0
```
Or pass as a configurable parameter to the function.
```

---

### [WARNING] Missing docstrings for public functions

**File**: `src/quant_alpha/features/energy_alpha.py:62-171`
**Category**: Convention
**Confidence**: 90%

The functions `add_energy_alpha_features` and `energy_alpha_registry_frame` are public APIs but lack docstrings. Given the complexity of the alpha computation (zscore windows, shifts, groupby operations), documentation is essential for maintainability and for users to understand the expected input/output contract.

**Suggestion**:
```
Add docstrings:
```python
def add_energy_alpha_features(frame: pd.DataFrame) -> pd.DataFrame:
    """Compute energy market alpha features from market data.

    Args:
        frame: DataFrame with columns ['market', 'timestamp', 'spot_price', ...]
               plus optional columns for specific alphas.

    Returns:
        DataFrame with original columns plus alpha feature columns.
    """
```
```

---

### [WARNING] Missing validation of required input columns

**File**: `src/quant_alpha/features/energy_alpha.py:74`
**Category**: Architecture
**Confidence**: 90%

The function checks for optional columns (actual_load, gas_price) but never validates that required columns ('market', 'timestamp', 'spot_price', 'load_forecast', 'wind_forecast', 'solar_forecast', 'residual_load', 'imbalance_price') exist. If any required column is missing, the function will fail with an opaque KeyError deep inside groupby operations.

**Suggestion**:
```
Add upfront validation:
```python
required_cols = {'market', 'timestamp', 'spot_price', 'load_forecast', 'wind_forecast', 'solar_forecast', 'residual_load', 'imbalance_price'}
missing = required_cols - set(df.columns)
if missing:
    raise ValueError(f'Missing required columns: {missing}')
```
```

---

### [WARNING] Missing null handling for spot_price in cross-market mean

**File**: `src/quant_alpha/features/energy_alpha.py:78`
**Category**: Potential Bug
**Confidence**: 60%

The `df.groupby('timestamp')['spot_price'].transform('mean')` will propagate NaN correctly, but if an entire group (timestamp) has all NaN spot_prices, the result is NaN. This NaN then flows into `_spread_diff` calculation which may cause issues downstream if market data has gaps. No explicit handling for this edge case.

**Suggestion**:
```
Consider adding a comment documenting the expected data contract, or add a warning for NaN-heavy groups:
```python
cross_market_mean = df.groupby('timestamp')['spot_price'].transform('mean')
if cross_market_mean.isna().any():
    # Log or handle timestamps with no valid spot prices
    pass
df['cross_market_spot_mean'] = cross_market_mean
```
```

---

### [WARNING] Implicit MultiIndex level assumption in cs_rank

**File**: `src/quant_alpha/features/registry.py:24`
**Category**: Potential Bug
**Confidence**: 75%

The function assumes the Series has a MultiIndex with the asset identifier at level=0. If the DataFrame's index is structured differently (e.g., with datetime at level=0), this will compute ranks across assets incorrectly rather than cross-sectionally at each time step. There is no documentation clarifying the expected index structure.

**Suggestion**:
```
Add a docstring explaining the required MultiIndex format, or add validation:
```python
def cs_rank(series: pd.Series) -> pd.Series:
    """Cross-sectional rank: assumes MultiIndex with (date, asset_id) where date is level=0."""
    if not isinstance(series.index, pd.MultiIndex) or series.index.nlevels < 2:
        raise ValueError('Expected MultiIndex with at least 2 levels')
    return series.groupby(level=0).rank(pct=True) - 0.5
```
```

---

### [WARNING] Implicit MultiIndex level assumption in ts_rank

**File**: `src/quant_alpha/features/registry.py:28`
**Category**: Potential Bug
**Confidence**: 75%

Similar to cs_rank, ts_rank assumes level=1 corresponds to the time/asset dimension for time-series operations. The asymmetry (level=0 for cross-section, level=1 for time-series) is implicit and fragile if the MultiIndex levels are ever reordered.

**Suggestion**:
```
Document the expected index structure or use named levels (e.g., series.groupby(level='date')) for clarity and robustness.
```

---

### [WARNING] ts_corr uses groupby().apply() which may be slow

**File**: `src/quant_alpha/features/registry.py:44-48`
**Category**: Performance
**Confidence**: 70%

The ts_corr function uses groupby().apply() with a lambda that computes rolling correlation. The apply() path in pandas is significantly slower than vectorized alternatives and will scale poorly with large datasets.

**Suggestion**:
```
Consider using a multi-indexed rolling approach or numba-accelerated alternative if performance matters. Alternatively, compute the correlation manually using rolling statistics:
```python
# Compute rolling means, then rolling correlation formula
cov = (left * right).rolling(window).mean() - left.rolling(window).mean() * right.rolling(window).mean()
std_left = left.rolling(window).std()
std_right = right.rolling(window).std()
result = cov / (std_left * std_right)
```
```

---

### [WARNING] safe_divide masks division-by-zero with eps, hiding true NaN

**File**: `src/quant_alpha/features/registry.py:50`
**Category**: Potential Bug
**Confidence**: 70%

When right is zero, safe_divide replaces it with eps=1e-9, which produces a valid number instead of NaN. This can create extremely large values (left / 1e-9) that may corrupt downstream ranks or statistics. The caller has no way to distinguish between a true small denominator and a zero denominator.

**Suggestion**:
```
Consider returning NaN for zero denominators instead of replacing with eps, or add a cap to prevent extreme values:
```python
def safe_divide(left: pd.Series, right: pd.Series, eps: float = 1e-9) -> pd.Series:
    result = left / right
    result[right.abs() < eps] = np.nan
    return result
```
```

---

### [WARNING] Division by near-zero in intraday range calculation

**File**: `src/quant_alpha/features/registry.py:96`
**Category**: Potential Bug
**Confidence**: 65%

alpha_wq_003 uses (high - low + 0.001) as denominator. If high ≈ low (a flat bar), the denominator is ~0.001, which will amplify the numerator dramatically. The hardcoded 0.001 also assumes a price scale — for instruments priced in cents or with different tick sizes, this epsilon may be inappropriate.

**Suggestion**:
```
Use safe_divide with a relative epsilon instead of the hardcoded additive constant, or document the assumed price scale. For example: safe_divide(x['close'] - x['open'], x['high'] - x['low']) with a minimum-allowed denominator in safe_divide.
```

---

### [WARNING] Alpha wq_007 missing warmup period specification

**File**: `src/quant_alpha/features/registry.py:110`
**Category**: Potential Bug
**Confidence**: 60%

alpha_wq_007 uses ts_mean with window=60. There is no min_periods argument passed, so rolling().mean() defaults to requiring all 60 observations. This means the first 59 time-series points per asset will be NaN, which is fine, but the behavior differs from other alphas (e.g., wq_009 uses min_periods=5), creating inconsistency in data availability across features.

**Suggestion**:
```
Either consistently specify min_periods across all rolling operations or document the assumed warmup behavior for each alpha definition.
```

---

### [WARNING] ts_corr may fail with insufficient data

**File**: `src/quant_alpha/features/registry.py:132`
**Category**: Potential Bug
**Confidence**: 65%

ts_corr uses rolling(window).corr() with window=6. If any group has fewer than 6 observations after dropping NaN, the result will be all NaN for that group. No min_periods is specified, and no error handling exists for groups that produce all-NaN output.

**Suggestion**:
```
Consider passing min_periods to rolling().corr() to control minimum required observations and add documentation about the expected warmup period.
```

---

### [WARNING] Missing error handling for pipeline execution

**File**: `src/quant_alpha/ingestion/dlt_energy.py:55-65`
**Category**: Potential Bug
**Confidence**: 75%

run_dlt_energy_pipeline calls generate_synthetic_power_market and pipeline.run without any try/except. If the data generation fails or the pipeline encounters a write error (e.g., DuckDB locked, disk full), the exception propagates unhandled, providing no cleanup or informative context.

**Suggestion**:
```
Add error handling with appropriate logging:
```python
try:
    load_info = pipeline.run(source)
except Exception as exc:
    raise RuntimeError(
        f"Energy pipeline failed for {duckdb_path}: {exc}"
    ) from exc
```
```

---

### [WARNING] Side effect modifies global environment variable

**File**: `src/quant_alpha/ingestion/dlt_energy.py:71-72`
**Category**: Architecture
**Confidence**: 85%

build_energy_pipeline modifies os.environ['DESTINATION__DUCKDB__CREDENTIALS'] as a side effect. This pollutes the global process environment and can cause subtle bugs when multiple pipelines run concurrently or when the function is called in tests.

**Suggestion**:
```
Pass credentials directly to the dlt pipeline configuration instead of modifying environment variables, or at minimum document and isolate the side effect:
```python
def build_energy_pipeline(
    duckdb_path: Path,
    dataset_name: str = "dlt_energy_raw",
) -> dlt.Pipeline:
    """Create and return a configured dlt pipeline for energy data."""
    return dlt.pipeline(
        pipeline_name="second_foundation_energy",
        destination="duckdb",
        dataset_name=dataset_name,
        credentials={"connection_string": str(duckdb_path)},
    )
```
```

---

### [WARNING] No validation that duckdb_path parent directory exists

**File**: `src/quant_alpha/ingestion/dlt_energy.py:98`
**Category**: Potential Bug
**Confidence**: 70%

build_energy_pipeline passes duckdb_path to DuckDB credentials without checking that the parent directory exists. DuckDB will fail with a confusing error if the directory tree is missing.

**Suggestion**:
```
Create the parent directory before constructing the pipeline:
```python
duckdb_path.parent.mkdir(parents=True, exist_ok=True)
pipeline = build_energy_pipeline(duckdb_path)
```
```

---

### [WARNING] Hardcoded relative path with parents[3] is fragile

**File**: `src/quant_alpha/ingestion/dlt_energy.py:116`
**Category**: Potential Bug
**Confidence**: 80%

The __main__ block uses `Path(__file__).resolve().parents[3]` to locate the project root. This is fragile and will break if the file is moved to a different directory depth. It also assumes a specific project structure.

**Suggestion**:
```
Use an environment variable or a configuration file for the root/data path, or use a package like `python-dotenv`:
```python
root = Path(os.environ.get("PROJECT_ROOT", Path(__file__).resolve().parents[3]))
```
```

---

### [WARNING] No validation of prices DataFrame columns

**File**: `src/quant_alpha/ingestion/dlt_equity.py:49`
**Category**: Potential Bug
**Confidence**: 70%

The code assumes fetch_prices returns a DataFrame with a 'date' column. If the column is missing or has a different name (e.g., 'Date', 'timestamp'), KeyError will be raised without a helpful error message.

**Suggestion**:
```
Add column validation after fetch_prices: `required = {'date', 'symbol', 'close'}; if not required.issubset(prices.columns): raise ValueError(...)`
```

---

### [WARNING] Potential int overflow or NaN handling for volume

**File**: `src/quant_alpha/ingestion/dlt_equity.py:57`
**Category**: Potential Bug
**Confidence**: 85%

The expression `int(row.get('volume', 0) or 0)` handles None and 0 but will raise ValueError if volume is NaN (float). Pandas often represents missing volume as NaN.

**Suggestion**:
```
Use safer conversion: `row['volume'] = 0 if pd.isna(row.get('volume')) else int(row['volume'])`
```

---

### [WARNING] Side effect via environment variable mutation

**File**: `src/quant_alpha/ingestion/dlt_equity.py:68-69`
**Category**: Architecture
**Confidence**: 80%

build_equity_pipeline sets the DESTINATION__DUCKDB__CREDENTIALS environment variable as a side effect, which is a global mutable state that can affect other processes and tests running in the same environment.

**Suggestion**:
```
Pass credentials directly to the dlt pipeline configuration instead of setting environment variables, or document this side effect clearly and provide cleanup logic.
```

---

### [WARNING] No error handling for pipeline.run failure

**File**: `src/quant_alpha/ingestion/dlt_equity.py:76-103`
**Category**: Potential Bug
**Confidence**: 75%

pipeline.run() can raise exceptions for database connection issues, schema conflicts, or data format problems. These are not caught and will propagate unhandled.

**Suggestion**:
```
Wrap pipeline.run in try/except and log the error, or document that callers should handle pipeline exceptions.
```

---

### [WARNING] Missing function docstring

**File**: `src/quant_alpha/ingestion/energy.py:14-20`
**Category**: Convention
**Confidence**: 90%

The generate_synthetic_power_market() function lacks a docstring explaining its purpose, parameters, return value, and any assumptions about the synthetic data generation model.

**Suggestion**:
```
Add a comprehensive docstring:
```python
def generate_synthetic_power_market(
    markets: list[str],
    start: str,
    end: str,
    freq: str = "h",
) -> pd.DataFrame:
    """Generate synthetic power market data.
    
    Creates deterministic synthetic time series for power market analysis
    including spot prices, load forecasts, renewable generation, and imbalance prices.
    
    Args:
        markets: List of market identifiers (e.g., ['DE', 'FR']).
        start: Start date string in pandas-compatible format.
        end: End date string in pandas-compatible format.
        freq: Frequency string (default 'h' for hourly).
        
    Returns:
        DataFrame with columns: timestamp, market, spot_price, load_forecast,
        actual_load, wind_forecast, solar_forecast, residual_load,
        imbalance_price, gas_price.
        
    Note:
        Uses deterministic random seeds based on market names for reproducibility.
    """
```
```

---

### [WARNING] No validation of input parameters

**File**: `src/quant_alpha/ingestion/energy.py:21`
**Category**: Potential Bug
**Confidence**: 80%

The function doesn't validate input parameters which could lead to silent errors or unexpected behavior. Empty markets list would return empty DataFrame, invalid date strings would cause pandas errors, and negative or zero frequency might produce unexpected results.

**Suggestion**:
```
Add input validation:
```python
def generate_synthetic_power_market(
    markets: list[str],
    start: str,
    end: str,
    freq: str = "h",
) -> pd.DataFrame:
    if not markets:
        raise ValueError("markets list cannot be empty")
    if not start or not end:
        raise ValueError("start and end dates must be provided")
    try:
        timestamps = pd.date_range(start, end, freq=freq)
    except ValueError as e:
        raise ValueError(f"Invalid date range or frequency: {e}")
```
```

---

### [WARNING] Solar generation can be negative

**File**: `src/quant_alpha/ingestion/energy.py:30`
**Category**: Potential Bug
**Confidence**: 85%

Solar generation calculation uses np.maximum(0, ...) for the base component but then adds Gaussian noise which can result in negative values. This violates physical constraints where solar generation cannot be negative.

**Suggestion**:
```
Apply np.maximum(0, ...) to the final solar value:
```python
solar = np.maximum(0, 14 * np.sin((hour - 6) / 12 * np.pi)) + rng.normal(0, 1, len(timestamps))
solar = np.maximum(solar, 0)  # Ensure non-negative
```
```

---

### [WARNING] XML parsing without defusing

**File**: `src/quant_alpha/ingestion/entsoe.py:8`
**Category**: Security
**Confidence**: 60%

Using xml.etree.ElementTree.parse on potentially untrusted XML from an external API. While this is less dangerous than lxml with DTD processing, ElementTree can still be vulnerable to XML bomb attacks (billion laughs) depending on the Python version.

**Suggestion**:
```
Consider using `defusedxml` package for safer XML parsing, or at minimum document the trust assumption:
```python
try:
    import defusedxml.ElementTree as ET
except ImportError:
    import xml.etree.ElementTree as ET
```
```

---

### [WARNING] Security token exposed in URL query parameters

**File**: `src/quant_alpha/ingestion/entsoe.py:47-51`
**Category**: Security
**Confidence**: 75%

The ENTSO-E API security token is passed as a URL query parameter (line 47). This means the token will appear in server access logs, browser history, proxy logs, and any network monitoring tools that capture URLs. While ENTSO-E's API is designed this way, it's still a security concern.

**Suggestion**:
```
Document this limitation clearly. Consider adding a warning in the class docstring. If ENTSO-E ever supports header-based authentication, migrate to that. For now, ensure logs don't capture full URLs.
```

---

### [WARNING] Silently falling back to platform trust store

**File**: `src/quant_alpha/ingestion/entsoe.py:54-57`
**Category**: Potential Bug
**Confidence**: 85%

The broad `except Exception` on line 55 silently swallows all errors when loading certifi certificates, including potential security issues. If certifi is installed but corrupted, or if load_verify_locations fails for a security-related reason, the code silently continues with the platform trust store.

**Suggestion**:
```
Catch only ImportError for the certifi import, and let other certificate loading errors propagate:
```python
try:
    import certifi
    context.load_verify_locations(cafile=certifi.where())
except ImportError:
    pass
```
```

---

### [WARNING] No XML error response detection

**File**: `src/quant_alpha/ingestion/entsoe.py:96-107`
**Category**: Potential Bug
**Confidence**: 90%

The parse_entsoe_timeseries function does not check whether the ENTSO-E API returned an error response instead of valid timeseries data. ENTSO-E returns XML error documents with Reason/code elements when requests fail (e.g., invalid domain, rate limiting). The function will silently return an empty Series instead of raising an error.

**Suggestion**:
```
Add error detection after parsing the root element:
```python
root = ET.parse(BytesIO(xml_payload)).getroot()
# Check for acknowledgement document (error response)
for reason in root.iter():
    if _strip_namespace(reason.tag) == 'Reason':
        text = _first_text(reason, 'text')
        if text and 'No matching data found' not in text:
            raise EntsoeError(f'ENTSO-E API error: {text}')
```
```

---

### [WARNING] Redundant _first_text calls in value lookup

**File**: `src/quant_alpha/ingestion/entsoe.py:116-118`
**Category**: Performance
**Confidence**: 90%

The generator expression calls `_first_text(point, value_name)` twice for each value_name - once in the condition check and once to return the value. This performs redundant XML traversals.

**Suggestion**:
```
Use a walrus operator or extract to a variable:
```python
value_text = next(
    (v for value_name in value_names if (v := _first_text(point, value_name)) is not None),
    None,
)
```
```

---

### [WARNING] Unvalidated position integer conversion

**File**: `src/quant_alpha/ingestion/entsoe.py:120-124`
**Category**: Potential Bug
**Confidence**: 80%

On line 123, `int(position_text)` can raise ValueError if the XML contains non-numeric position values. This would crash the entire parsing without a helpful error message.

**Suggestion**:
```
Add validation or handle the ValueError:
```python
try:
    pos = int(position_text)
except ValueError:
    continue  # or raise EntsoeError with context
records.append({"timestamp": start + delta * (pos - 1), ...})
```
```

---

### [WARNING] Sequential API calls without retry logic

**File**: `src/quant_alpha/ingestion/entsoe.py:160-200`
**Category**: Potential Bug
**Confidence**: 75%

Five separate API calls are made sequentially for each market (spot, load, solar, wind_onshore, wind_offshore). If any call fails partway through, the previous successful calls are wasted and no retry is attempted. Network issues are common with external APIs.

**Suggestion**:
```
Add retry logic with exponential backoff to the request method, or wrap the series queries in a retry decorator. Consider using tenacity or a simple retry loop.
```

---

### [WARNING] Wind forecast addition before resampling

**File**: `src/quant_alpha/ingestion/entsoe.py:188`
**Category**: Potential Bug
**Confidence**: 70%

wind_onshore.add(wind_offshore, fill_value=0) is computed before resampling. If the two wind series have different indices, the add operation will union them, potentially creating unexpected intermediate timestamps that then get resampled differently than expected.

**Suggestion**:
```
Resample each component individually, then add:
```python
"wind_forecast": _resample(wind_onshore, bar_interval).add(
    _resample(wind_offshore, bar_interval), fill_value=0
),
```
```

---

### [WARNING] Silently skipping missing symbols

**File**: `src/quant_alpha/ingestion/yahoo.py:62-65`
**Category**: Potential Bug
**Confidence**: 85%

In _normalize_yfinance_frame, if a symbol is missing from the MultiIndex columns, it is silently skipped with `continue`. This means the caller receives partial data without any warning that some tickers failed.

**Suggestion**:
```
Log a warning or raise an error for missing symbols:
```python
missing = [s for s in symbols if s not in data.columns.get_level_values(0)]
if missing:
    import warnings
    warnings.warn(f"Missing data for symbols: {missing}")
```
```

---

### [WARNING] Single-symbol download produces non-MultiIndex columns

**File**: `src/quant_alpha/ingestion/yahoo.py:67-68`
**Category**: Potential Bug
**Confidence**: 75%

When only one symbol is in universe.symbols, yf.download may return a flat DataFrame (not MultiIndex), but the else branch at line 67 assumes symbols[0] is correct without validation. If symbols list is empty, this will raise an IndexError.

**Suggestion**:
```
Add a guard for empty symbols list:
```python
if not symbols:
    raise ValueError("No symbols provided")
```
```

---

### [WARNING] Deprecated datetime.utcnow() usage

**File**: `src/quant_alpha/ingestion/yahoo.py:97`
**Category**: Potential Bug
**Confidence**: 95%

datetime.utcnow() is deprecated in Python 3.12+ and returns a naive datetime. It may produce incorrect timestamps in edge cases.

**Suggestion**:
```
Use datetime.now(datetime.timezone.utc) instead:
```python
from datetime import datetime, timezone
as_of = datetime.now(timezone.utc).isoformat(timespec='seconds')
```
```

---

### [WARNING] Missing docstring for _write_parquet

**File**: `src/quant_alpha/pipeline.py:17-22`
**Category**: Readability
**Confidence**: 80%

The helper function _write_parquet lacks a docstring explaining its behavior and return value.

**Suggestion**:
```
Add a docstring:
```python
def _write_parquet(frame: pd.DataFrame, path: Path) -> Path:
    """Write a DataFrame to Parquet, creating parent directories as needed."""
    path.parent.mkdir(parents=True, exist_ok=True)
    frame.to_parquet(path, index=False)
    return path
```
```

---

### [WARNING] Missing exception handling in pipeline

**File**: `src/quant_alpha/pipeline.py:20-82`
**Category**: Potential Bug
**Confidence**: 85%

The run_pipeline function performs many I/O operations (file writes, database writes, network fetches) but has no try/except blocks. Any failure in the middle of the pipeline will leave partial state — files written but not database entries, or vice versa — with no cleanup or error reporting.

**Suggestion**:
```
Wrap the pipeline in a try/except block, or use a context manager to track cleanup. Consider adding logging for each stage:
```python
import logging

logger = logging.getLogger(__name__)

def run_pipeline(config_path: Path, root: Path, offline: bool = False) -> dict[str, object]:
    try:
        cfg = load_project_config(config_path, root=root)
        ensure_project_dirs(cfg)
        universe = load_universe(cfg.universe_path)
        # ... rest of pipeline
    except Exception as e:
        logger.exception('Pipeline failed')
        raise
```
```

---

### [WARNING] Missing docstring for run_pipeline

**File**: `src/quant_alpha/pipeline.py:20-82`
**Category**: Readability
**Confidence**: 90%

The public function run_pipeline has no docstring explaining its purpose, parameters, return value, or side effects. This is a complex function with 10+ side effects (file and database writes).

**Suggestion**:
```
Add a docstring:
```python
def run_pipeline(config_path: Path, root: Path, offline: bool = False) -> dict[str, object]:
    """Run the full alpha research pipeline.

    Fetches price data, computes alpha factors, runs backtests and
    diagnostics, and writes results to Parquet files and DuckDB.

    Args:
        config_path: Path to the YAML project configuration.
        root: Project root directory.
        offline: If True, skip network fetching and use cached data.

    Returns:
        Dict with output file paths, row counts, and backtest metrics.
    """
```
```

---

### [WARNING] run_pipeline function exceeds 200 lines of logic

**File**: `src/quant_alpha/pipeline.py:20`
**Category**: Performance
**Confidence**: 70%

The run_pipeline function is 62 lines long but performs 15+ distinct operations (data fetching, feature computation, multiple backtests, multiple diagnostics, multiple database writes). This monolithic structure makes it hard to test individual stages and to resume partial runs after failure.

**Suggestion**:
```
Consider breaking the pipeline into named stages that can be run and tested independently:
```python
def run_pipeline(config_path: Path, root: Path, offline: bool = False) -> dict[str, object]:
    cfg = load_project_config(config_path, root=root)
    ensure_project_dirs(cfg)
    universe = load_universe(cfg.universe_path)
    
    prices = _fetch_and_store(cfg, universe, offline)
    factors = _compute_and_store_factors(cfg, prices)
    metrics = _run_backtests(cfg, factors)
    _run_diagnostics(cfg, factors)
    return _build_summary(cfg, prices, factors, metrics)
```
```

---

### [WARNING] No validation of fetch_prices result

**File**: `src/quant_alpha/pipeline.py:31`
**Category**: Potential Bug
**Confidence**: 75%

fetch_prices could return None or an empty DataFrame if the universe is empty or all tickers fail. The code proceeds to write this to Parquet and DuckDB, and passes it to add_alpha_factors, which may fail or produce incorrect results with empty data.

**Suggestion**:
```
Add a validation check:
```python
prices = fetch_prices(cfg, universe, offline=offline)
if prices is None or prices.empty:
    raise ValueError(f'No price data returned for universe of {len(universe)} tickers')
```
```

---

### [WARNING] No validation of add_alpha_factors result

**File**: `src/quant_alpha/pipeline.py:35`
**Category**: Potential Bug
**Confidence**: 70%

add_alpha_factors could return an empty or malformed DataFrame. The code writes it to parquet and DuckDB without checking, and then passes it to backtest functions that may fail.

**Suggestion**:
```
Add a validation check after add_alpha_factors:
```python
factors = add_alpha_factors(prices, cfg)
if factors.empty:
    raise ValueError('Alpha factor computation returned empty DataFrame')
```
```

---

### [WARNING] walk_forward loop may silently produce empty result

**File**: `src/quant_alpha/pipeline.py:67-74`
**Category**: Potential Bug
**Confidence**: 65%

If all walk_forward_ic calls return empty DataFrames, walk_forward will be an empty DataFrame written to the database. This is handled but not logged — a user wouldn't know all walk-forward analyses failed.

**Suggestion**:
```
Add a warning log when walk_forward is empty:
```python
if walk_forward.empty:
    logger.warning('All walk-forward IC computations returned empty results')
```
```

---

### [WARNING] Missing module docstring

**File**: `src/quant_alpha/pipeline_energy.py:1`
**Category**: Convention
**Confidence**: 90%

The module lacks a docstring explaining its purpose, which is the energy pipeline for quant_alpha.

**Suggestion**:
```
Add a module-level docstring at the top of the file:
```python
"""Energy market data pipeline for alpha generation and backtesting."""
```
```

---

### [WARNING] Missing docstring for _write_parquet

**File**: `src/quant_alpha/pipeline_energy.py:18-20`
**Category**: Convention
**Confidence**: 80%

The helper function _write_parquet lacks a docstring.

**Suggestion**:
```
Add a docstring:
```python
def _write_parquet(frame: pd.DataFrame, path: Path) -> Path:
    """Write DataFrame to parquet, creating parent directories as needed."""
```
```

---

### [WARNING] Missing docstring for _load_power_market

**File**: `src/quant_alpha/pipeline_energy.py:22-46`
**Category**: Convention
**Confidence**: 90%

The function _load_power_market lacks a docstring explaining its parameters, return value, and purpose.

**Suggestion**:
```
Add a docstring:
```python
def _load_power_market(cfg, markets: list[str], universe: dict[str, object]) -> pd.DataFrame:
    """Load power market data from configured source.
    
    Args:
        cfg: Configuration object with data_source, start_date, end_date, etc.
        markets: List of market identifiers to fetch.
        universe: Universe configuration dict containing entsoe_domains.
    
    Returns:
        DataFrame containing power market data.
    
    Raises:
        ValueError: If data_source is unsupported or entsoe_domains invalid.
    """
```
```

---

### [WARNING] Missing type hint for cfg parameter

**File**: `src/quant_alpha/pipeline_energy.py:22`
**Category**: Convention
**Confidence**: 85%

The cfg parameter in _load_power_market lacks a type hint, reducing code clarity and preventing static analysis.

**Suggestion**:
```
Add a type hint for cfg (likely a config dataclass or similar):
```python
def _load_power_market(cfg: EnergyConfig, markets: list[str], universe: dict[str, object]) -> pd.DataFrame:
```
```

---

### [WARNING] Missing docstring for run_energy_pipeline

**File**: `src/quant_alpha/pipeline_energy.py:49`
**Category**: Convention
**Confidence**: 90%

The main public function run_energy_pipeline lacks a docstring explaining its purpose, parameters, and return value.

**Suggestion**:
```
Add a comprehensive docstring:
```python
def run_energy_pipeline(
    config_path: Path,
    root: Path,
    source_override: str | None = None,
) -> dict[str, object]:
    """Execute the full energy market alpha pipeline.
    
    Args:
        config_path: Path to the project YAML configuration.
        root: Project root directory.
        source_override: Optional override for data source (e.g., 'synthetic', 'entsoe').
    
    Returns:
        Dictionary containing pipeline outputs: paths, metrics, row counts, and cloud export status.
    """
```
```

---

### [WARNING] Pipeline function is too long (90+ lines)

**File**: `src/quant_alpha/pipeline_energy.py:49-139`
**Category**: Architecture
**Confidence**: 85%

run_energy_pipeline spans ~90 lines and handles ingestion, feature engineering, backtesting, diagnostics, storage, and cloud export. This makes it hard to test, maintain, and reason about failures.

**Suggestion**:
```
Break into smaller functions: _ingest_and_store(), _compute_features(), _run_backtest(), _export_to_cloud(). Each can be tested independently and the main function becomes an orchestrator.
```

---

### [WARNING] No error handling in pipeline execution

**File**: `src/quant_alpha/pipeline_energy.py:49-139`
**Category**: Potential Bug
**Confidence**: 80%

The entire pipeline has no try/except blocks. If any step fails midway (e.g., network error fetching ENTSOE data, DuckDB write failure), partial state may be left behind with no cleanup or meaningful error context.

**Suggestion**:
```
Add error handling at minimum for external I/O operations, and consider a rollback/cleanup strategy:
```python
try:
    raw = _load_power_market(cfg, markets, universe)
except Exception as e:
    raise RuntimeError(f"Failed to load power market data: {e}") from e
```
```

---

### [WARNING] Potential KeyError on missing universe keys

**File**: `src/quant_alpha/pipeline_energy.py:58`
**Category**: Potential Bug
**Confidence**: 70%

universe.get('markets', ...) handles missing key, but if universe is None or not a dict (e.g., YAML returns a string), this will raise AttributeError.

**Suggestion**:
```
Add validation after load_yaml:
```python
universe = load_yaml(cfg.universe_path)
if not isinstance(universe, dict):
    raise ValueError(f"Universe file {cfg.universe_path} must return a dictionary.")
markets = universe.get("markets", ["DE_LU", "CZ", "FR"])
```
```

---

### [WARNING] Missing NaN handling before division

**File**: `src/quant_alpha/pipeline_energy.py:70`
**Category**: Potential Bug
**Confidence**: 65%

next_spot can contain NaN values (last row per group after shift(-1)). Dividing NaN by denominator produces NaN, which propagates through rank calculations. While pandas handles this, it may silently corrupt downstream aggregations without warning.

**Suggestion**:
```
Consider explicitly handling NaN forward returns, e.g., by documenting expected NaN behavior or dropping/filling:
```python
features["forward_return"] = ((next_spot - features["spot_price"]) / denominator).clip(-0.8, 0.8)
# Note: Last observation per market will have NaN forward_return by design.
```
```

---

### [WARNING] Magic number 0.5 in alpha_composite

**File**: `src/quant_alpha/pipeline_energy.py:78`
**Category**: Readability
**Confidence**: 60%

Subtracting 0.5 from the mean of percentile ranks centers the composite alpha around zero, but this is an implicit assumption not documented.

**Suggestion**:
```
Add a comment explaining the centering:
```python
# Center composite alpha around zero (percentile ranks are [0,1], mean - 0.5 gives [-0.5, 0.5])
features["alpha_composite"] = features[rank_cols].mean(axis=1) - 0.5
```
```

---

### [WARNING] Redundant energy_alpha_registry_frame() call

**File**: `src/quant_alpha/pipeline_energy.py:125`
**Category**: Potential Bug
**Confidence**: 85%

energy_alpha_registry_frame() is called on line 82 and again on line 125. If this function has side effects or is expensive, this is wasteful. Even if not, it's redundant code that could diverge.

**Suggestion**:
```
Reuse the result from the earlier call:
```python
# Store the registry frame once
registry_frame = energy_alpha_registry_frame()
write_table(cfg.duckdb_path, "energy_alpha_registry", registry_frame)
# ... later ...
cloud_tables = {
    ...
    "energy_alpha_registry": registry_frame,
    ...
}
```
```

---

### [WARNING] Empty YAML document returns None

**File**: `src/quant_alpha/platform/bruin_graph.py:55`
**Category**: Potential Bug
**Confidence**: 60%

yaml.safe_load() returns None for empty YAML documents (e.g., empty files or files with only comments). The code checks `if not data` but this would silently skip empty files rather than raising an error.

**Suggestion**:
```
Consider logging a warning when an empty YAML is encountered: `if not data: logging.warning(f"Empty YAML in {path}"); return None`
```

---

### [WARNING] File read errors not handled gracefully

**File**: `src/quant_alpha/platform/bruin_graph.py:67`
**Category**: Potential Bug
**Confidence**: 75%

path.read_text() in _parse_asset_yml and _parse_sql_asset can raise various exceptions (PermissionError, UnicodeDecodeError, etc.) that are not caught, which would crash the entire graph loading.

**Suggestion**:
```
Wrap file reading in try/except: `try: text = path.read_text() except (PermissionError, UnicodeDecodeError) as e: logging.warning(f"Could not read {path}: {e}"); return None`
```

---

### [WARNING] stdout not captured in subprocess

**File**: `src/quant_alpha/platform/bruin_graph.py:72`
**Category**: Potential Bug
**Confidence**: 70%

capture_output=True captures both stdout and stderr, but only stderr is checked on failure. stdout output is lost and not logged anywhere, making debugging difficult.

**Suggestion**:
```
Log stdout on failure: `raise RuntimeError(result.stderr.strip() or result.stdout.strip() or "non-zero exit")`
```

---

### [WARNING] Unvalidated YAML keys may cause KeyError

**File**: `src/quant_alpha/platform/bruin_graph.py:83-84`
**Category**: Potential Bug
**Confidence**: 85%

In _parse_asset_yml, accessing data["name"] without checking if 'name' key exists. If an asset YAML file is malformed or missing the 'name' field, this will crash with a KeyError.

**Suggestion**:
```
Add validation: `if "name" not in in data: raise ValueError(f"Asset YAML at {path} missing required 'name' field")`
```

---

### [WARNING] downstream() has O(n²) complexity

**File**: `src/quant_alpha/platform/bruin_graph.py:122`
**Category**: Performance
**Confidence**: 80%

The downstream() method calls upstream() for every node in the graph, resulting in O(n * (n + e)) complexity. For large graphs with many nodes, this will be very slow.

**Suggestion**:
```
Pre-compute a reverse adjacency map and use a single BFS/DFS from the target node to find all downstream nodes in O(n + e) time.
```

---

### [WARNING] O(n²) complexity in topological sort

**File**: `src/quant_alpha/platform/bruin_graph.py:134`
**Category**: Performance
**Confidence**: 70%

The topological_order() method iterates through all downstream nodes for each node removal, resulting in O(V²) complexity. For large graphs, this could be slow.

**Suggestion**:
```
Use an adjacency list (reverse mapping from dependency → dependents) built once to achieve O(V + E) complexity.
```

---

### [WARNING] Missing dependency warning is silent

**File**: `src/quant_alpha/platform/bruin_graph.py:178`
**Category**: Potential Bug
**Confidence**: 75%

When checking `if d in self.nodes` for dependencies, missing dependencies are silently treated as if the dependency succeeded (not in the generator). This could mask configuration errors.

**Suggestion**:
```
Log a warning when a dependency is not found: `if d not in self.nodes: logging.warning(f"Dependency '{d}' of '{node.name}' not found")`
```

---

### [WARNING] Missing duplicate dataset name validation

**File**: `src/quant_alpha/platform/contracts.py:70`
**Category**: Potential Bug
**Confidence**: 95%

The ALL_DATASETS list combines EQUITY_DATASETS and ENERGY_DATASETS using concatenation. If a dataset name appears in both lists (e.g., 'energy_alpha_diagnostics' in ENERGY_DATASETS but no equivalent in EQUITY_DATASETS), this could lead to ambiguity. The code doesn't validate that all dataset names are unique across both lists.

**Suggestion**:
```
Add validation to ensure no duplicate dataset names exist:
```python
# Add after ALL_DATASETS definition
def _validate_unique_names():
    names = [ds.name for ds in ALL_DATASETS]
    if len(names) != len(set(names)):
        duplicates = [name for name in names if names.count(name) > 1]
        raise ValueError(f"Duplicate dataset names found: {duplicates}")

_validate_unique_names()
```
```

---

### [WARNING] Missing module docstring

**File**: `src/quant_alpha/platform/quality.py:1`
**Category**: Convention
**Confidence**: 90%

The module lacks a docstring explaining its purpose and usage.

**Suggestion**:
```
Add a module docstring:
"""Data quality validation utilities for energy market data."""
```

---

### [WARNING] Missing function docstring

**File**: `src/quant_alpha/platform/quality.py:6-9`
**Category**: Convention
**Confidence**: 90%

The function validate_primary_key lacks a docstring explaining its parameters and return value.

**Suggestion**:
```
Add a docstring:
"""Check for duplicate rows based on specified key columns.

Args:
    frame: DataFrame to validate.
    keys: Column names forming the primary key.

Returns:
    Dictionary with check results including duplicate count."""
```

---

### [WARNING] No error handling for missing columns

**File**: `src/quant_alpha/platform/quality.py:6-9`
**Category**: Potential Bug
**Confidence**: 95%

The function will raise a KeyError if any column in 'keys' doesn't exist in the DataFrame.

**Suggestion**:
```
Add validation:
```python
def validate_primary_key(frame: pd.DataFrame, keys: list[str]) -> dict[str, object]:
    missing = [k for k in keys if k not in frame.columns]
    if missing:
        raise ValueError(f"Columns not found: {missing}")
    duplicates = frame.duplicated(keys).sum()
    return {...}
```
```

---

### [WARNING] Missing function docstring

**File**: `src/quant_alpha/platform/quality.py:12-20`
**Category**: Convention
**Confidence**: 90%

The function validate_non_null lacks a docstring explaining its parameters and return value.

**Suggestion**:
```
Add a docstring:
"""Check for null values in specified columns.

Args:
    frame: DataFrame to validate.
    columns: Column names to check for nulls.

Returns:
    List of dictionaries with check results per column."""
```

---

### [WARNING] No error handling for missing columns

**File**: `src/quant_alpha/platform/quality.py:12-20`
**Category**: Potential Bug
**Confidence**: 95%

The function will raise a KeyError if any column in 'columns' doesn't exist in the DataFrame.

**Suggestion**:
```
Add validation:
```python
def validate_non_null(frame: pd.DataFrame, columns: list[str]) -> list[dict[str, object]]:
    missing = [c for c in columns if c not in frame.columns]
    if missing:
        raise ValueError(f"Columns not found: {missing}")
    return [...]
```
```

---

### [WARNING] Missing function docstring

**File**: `src/quant_alpha/platform/quality.py:23-32`
**Category**: Convention
**Confidence**: 90%

The function run_energy_quality_checks lacks a docstring explaining its purpose and return value.

**Suggestion**:
```
Add a docstring:
"""Run standard quality checks for energy market data.

Args:
    frame: DataFrame with energy market data.

Returns:
    DataFrame summarizing check results."""
```

---

### [WARNING] No error handling for invalid DataFrame

**File**: `src/quant_alpha/platform/quality.py:23-32`
**Category**: Potential Bug
**Confidence**: 90%

The function doesn't validate that 'frame' is a DataFrame or that required columns exist.

**Suggestion**:
```
Add input validation:
```python
def run_energy_quality_checks(frame: pd.DataFrame) -> pd.DataFrame:
    if not isinstance(frame, pd.DataFrame):
        raise TypeError("Expected pandas DataFrame")
    # Validate required columns exist
    required_cols = ["timestamp", "market", "spot_price", "load_forecast", "residual_load"]
    missing = [c for c in required_cols if c not in frame.columns]
    if missing:
        raise ValueError(f"Missing required columns: {missing}")
    ...
```
```

---

### [WARNING] Missing docstrings on all public functions

**File**: `src/quant_alpha/storage/duckdb.py:10-15`
**Category**: Convention
**Confidence**: 90%

None of the three public functions (write_table, write_metrics, table_exists) have docstrings. This makes the API harder to understand for consumers of this module.

**Suggestion**:
```
Add docstrings to all public functions:
```python
def write_table(db_path: Path, table_name: str, frame: pd.DataFrame) -> None:
    """Write a DataFrame as a table to the DuckDB database at db_path.

    Args:
        db_path: Path to the DuckDB database file.
        table_name: Name of the table to create or replace.
        frame: DataFrame to persist.
    """
```
```

---

### [WARNING] Silent exception swallowing hides errors

**File**: `src/quant_alpha/storage/duckdb.py:25-33`
**Category**: Potential Bug
**Confidence**: 90%

The table_exists function catches all exceptions with a bare 'except Exception' and returns False. This silently swallows database corruption errors, permission errors, disk I/O errors, and other unexpected failures, making debugging extremely difficult and masking real problems.

**Suggestion**:
```
Be more specific about which exceptions to catch, or at minimum log the exception:
```python
import logging

logger = logging.getLogger(__name__)

# ...
    except (duckdb.IOException, duckdb.CatalogException) as e:
        logger.debug("Table existence check failed: %s", e)
        return False
```
```

---

### [WARNING] Missing function docstring

**File**: `src/quant_alpha/storage/gcp.py:14-17`
**Category**: Convention
**Confidence**: 95%

The main public function lacks a docstring explaining parameters, return value, and possible exceptions.

**Suggestion**:
```
Add a comprehensive docstring:
"""Export DataFrames to GCS and load them into BigQuery.

Args:
    frames: Dictionary mapping table names to DataFrames.
    config: Cloud export configuration.

Returns:
    Dictionary mapping table names to BigQuery table IDs.

Raises:
    CloudExportError: If configuration is invalid or export fails.
"""
```

---

### [WARNING] gcs_prefix could be empty or None

**File**: `src/quant_alpha/storage/gcp.py:35`
**Category**: Potential Bug
**Confidence**: 80%

If config.gcs_prefix is None or empty string, the blob_name will start with '/' or 'None/', causing unexpected behavior.

**Suggestion**:
```
Add a default or validation for gcs_prefix:
```python
gcs_prefix = config.gcs_prefix.rstrip('/') if config.gcs_prefix else ''
blob_name = f"{gcs_prefix}/{table_name}/{table_name}.parquet" if gcs_prefix else f"{table_name}/{table_name}.parquet"
```
```

---

### [WARNING] BigQuery table_id constructed without escaping

**File**: `src/quant_alpha/storage/gcp.py:38`
**Category**: Potential Bug
**Confidence**: 75%

The table_id is constructed using f-string with project_id, dataset, and table_name. If config values contain dots or special characters, the resulting table_id will be malformed.

**Suggestion**:
```
Use the BigQuery client's Table class to construct proper table references:
```python
from google.cloud.bigquery import Table
table_ref = bigquery.TableReference.from_string(
    f"{config.gcp_project_id}.{config.bigquery_dataset}.{table_name}"
)
```
```

---

### [WARNING] Deprecated pd.Timestamp.utcnow() usage

**File**: `src/quant_alpha/streaming/demo_signals.py:21-22`
**Category**: Potential Bug
**Confidence**: 90%

`pd.Timestamp.utcnow()` is deprecated in newer versions of pandas. This will emit FutureWarning and may be removed in a future release.

**Suggestion**:
```
Use `datetime.now(timezone.utc)` instead:
```python
end = pd.Timestamp(datetime.now(timezone.utc)).floor('h')
```
```

---

### [WARNING] Hardcoded relative path traversal assumption

**File**: `src/quant_alpha/streaming/demo_signals.py:43`
**Category**: Potential Bug
**Confidence**: 75%

The path `parents[3]` assumes a specific directory depth from the file location. If the file is moved or the project structure changes, this will silently resolve to the wrong directory or raise an IndexError.

**Suggestion**:
```
Use an environment variable, a configuration file, or pass the path as a CLI argument instead of relying on relative path traversal:
```python
import os
root = Path(os.environ.get('QUANT_ALPHA_ROOT', Path(__file__).resolve().parents[3]))
```
```

---

### [WARNING] Missing validation that parent directory exists

**File**: `src/quant_alpha/streaming/demo_signals.py:44`
**Category**: Potential Bug
**Confidence**: 85%

The DuckDB file path `data/warehouse/second_foundation.duckdb` is constructed without checking if the `data/warehouse/` directory exists. `write_table` may fail with a confusing error if the directory hasn't been created.

**Suggestion**:
```
Add directory creation before writing:
```python
db = root / 'data/warehouse/second_foundation.duckdb'
db.parent.mkdir(parents=True, exist_ok=True)
```
```

---

### [WARNING] No validation of schema file existence

**File**: `src/quant_alpha/streaming/redpanda_consumer.py:10-11`
**Category**: Potential Bug
**Confidence**: 70%

The `_load_schema` function opens the schema file without checking if it exists first. If the file doesn't exist, a `FileNotFoundError` will be raised with an unclear error message.

**Suggestion**:
```
Add explicit existence check:
```python
def _load_schema(path: Path) -> dict:
    if not path.exists():
        raise FileNotFoundError(f"Schema file not found: {path}")
    # ... rest of function
```
```

---

### [WARNING] Hardcoded Kafka group ID

**File**: `src/quant_alpha/streaming/redpanda_consumer.py:36`
**Category**: Security
**Confidence**: 85%

The Kafka consumer group ID is hardcoded as 'second-foundation-demo'. This prevents multiple instances from running with different group IDs and could cause offset management issues in production.

**Suggestion**:
```
Make the group ID configurable:
```python
def consume_energy_signals(
    bootstrap_servers: str,
    topic: str,
    schema_path: Path,
    max_messages: int = 10,
    max_empty_polls: int = 30,
    group_id: str = "second-foundation-demo",
) -> list[dict]:
    # ...
    consumer = Consumer({
        "bootstrap.servers": bootstrap_servers,
        "group.id": group_id,
        "auto.offset.reset": "earliest",
    })
```
```

---

### [WARNING] Uncaught msg.error() returns KafkaError object

**File**: `src/quant_alpha/streaming/redpanda_consumer.py:44`
**Category**: Potential Bug
**Confidence**: 80%

On line 44, `msg.error()` returns a KafkaError object. The code only checks if it's truthy but doesn't log or handle specific error types (e.g., PARTITION_EOF vs real errors). This could mask critical errors.

**Suggestion**:
```
Log the error for debugging:
```python
if msg is None:
    empty_polls += 1
    continue
if msg.error():
    # Optionally log: logger.warning(f"Kafka error: {msg.error()}")
    if msg.error().code() == KafkaError._PARTITION_EOF:
        empty_polls += 1
        continue
    raise KafkaException(msg.error())
```
```

---

### [WARNING] Missing error handling for DuckDB operations

**File**: `src/quant_alpha/streaming/redpanda_consumer.py:68-79`
**Category**: Potential Bug
**Confidence**: 80%

The DuckDB operations in `consume_and_store` don't handle potential errors such as file permission issues, disk space problems, or table schema mismatches. The function could fail silently or with unhelpful error messages.

**Suggestion**:
```
Add try/except with specific error handling:
```python
try:
    with duckdb.connect(str(duckdb_path)) as con:
        # ... existing code ...
except duckdb.Error as e:
    raise RuntimeError(f"Failed to store data in DuckDB: {e}") from e
```
```

---

### [WARNING] Inefficient table creation using DataFrame reference

**File**: `src/quant_alpha/streaming/redpanda_consumer.py:69`
**Category**: Performance
**Confidence**: 60%

The CREATE TABLE IF NOT EXISTS statement uses `SELECT * FROM frame WHERE false` which references the DataFrame directly. DuckDB may need to scan the entire DataFrame to determine schema even though no rows are inserted. This could be inefficient for large DataFrames.

**Suggestion**:
```
Use explicit column definition or a more efficient approach:
```python
con.execute(
    f"""CREATE TABLE IF NOT EXISTS {table} (
        -- define columns based on schema
    )"""
)
# Or use duckdb's register:
con.register('frame_view', frame)
con.execute(f"CREATE TABLE IF NOT EXISTS {table} AS SELECT * FROM frame_view WHERE false")
con.unregister('frame_view')
```
```

---

### [WARNING] Missing error handling in __main__ block

**File**: `src/quant_alpha/streaming/redpanda_consumer.py:84-88`
**Category**: Potential Bug
**Confidence**: 80%

The `__main__` block calls `consume_and_store` but doesn't handle potential exceptions. Network failures, missing schema files, or DuckDB errors will produce unhandled stack traces.

**Suggestion**:
```
Add try/except block:
```python
if __name__ == "__main__":
    try:
        root = Path(__file__).resolve().parents[3]
        schema_path = root / "schemas/energy_signal.avsc"
        duckdb_path = root / "data/warehouse/second_foundation.duckdb"
        n = consume_and_store("localhost:19092", "energy-signals", schema_path, duckdb_path)
        print(f"Wrote {n} rows to live_energy_signals")
    except Exception as e:
        print(f"Error: {e}", file=sys.stderr)
        sys.exit(1)
```
```

---

### [WARNING] Function lacks docstring

**File**: `src/quant_alpha/streaming/redpanda_producer.py:30-44`
**Category**: Readability
**Confidence**: 90%

The publish_energy_signals function is missing a docstring explaining its purpose, parameters, and return value. This makes it harder for other developers to understand how to use the function.

**Suggestion**:
```
Add a docstring:
```python
def publish_energy_signals(bootstrap_servers: str, topic: str, schema_path: Path, sample_size: int = 100) -> None:
    """Publish energy market signals to Kafka topic.
    
    Args:
        bootstrap_servers: Kafka broker addresses
        topic: Target Kafka topic name
        schema_path: Path to Avro schema file
        sample_size: Number of rows to publish (default: 100)
    """
```
```

---

### [WARNING] Missing error handling for schema file path

**File**: `src/quant_alpha/streaming/redpanda_producer.py:47`
**Category**: Potential Bug
**Confidence**: 85%

The schema file path is constructed as 'root / "schemas/energy_signal.avsc"' but there's no validation that this file exists or is readable. If the file doesn't exist, the program will crash with an unhelpful error.

**Suggestion**:
```
Add existence check or handle FileNotFoundError:
```python
schema_path = root / 'schemas/energy_signal.avsc'
if not schema_path.exists():
    raise FileNotFoundError(f'Schema file not found: {schema_path}')
publish_energy_signals(bootstrap_servers, 'energy-signals', schema_path)
```
```

---

### [WARNING] No validation on INTERVAL env var

**File**: `src/quant_alpha/streaming/risingwave/producer.py:24`
**Category**: Potential Bug
**Confidence**: 80%

If INTERVAL_SECONDS is set to 0 or a negative value, the loop will either spin rapidly (performance issue) or behave unexpectedly. No validation is performed after parsing.

**Suggestion**:
```
Add validation:
```python
INTERVAL = float(os.environ.get('INTERVAL_SECONDS', '60'))
if INTERVAL <= 0:
    raise ValueError('INTERVAL_SECONDS must be positive')
```
```

---

### [WARNING] Missing type hints on function parameters

**File**: `src/quant_alpha/streaming/risingwave/producer.py:33`
**Category**: Potential Bug
**Confidence**: 80%

_make_producer() has no return type hint, and _delivery_report() lacks parameter type hints. The `producer` parameter in stream_signals() also lacks a type hint.

**Suggestion**:
```
Add type hints:
```python
def _make_producer() -> 'Producer':
    ...

def _delivery_report(err: Optional[Exception], msg: Any) -> None:
    ...

def stream_signals(producer: 'Producer', once: bool = False) -> None:
```
```

---

### [WARNING] Missing error handling in stream_signals loop

**File**: `src/quant_alpha/streaming/risingwave/producer.py:43`
**Category**: Potential Bug
**Confidence**: 85%

If generate_synthetic_power_market() raises an exception (e.g., network error, data issue), the entire producer crashes. No try/except wraps the main loop iteration to allow recovery.

**Suggestion**:
```
Add error handling:
```python
while True:
    try:
        now = pd.Timestamp.utcnow().floor('h')
        frame = generate_synthetic_power_market(...)
        ...
    except Exception as e:
        print(f'[producer] error: {e}')
        time.sleep(INTERVAL)
        continue
```
```

---

### [WARNING] Potential KeyError on payload dict access

**File**: `src/quant_alpha/streaming/risingwave/producer.py:49-52`
**Category**: Performance
**Confidence**: 70%

The key construction `f"{payload['market']}:{payload['timestamp']}"` assumes both 'market' and 'timestamp' columns exist in the generated dataframe. If these column names change or are missing, a KeyError will crash the producer.

**Suggestion**:
```
Use .get() with a fallback or validate required keys:
```python
key = f"{payload.get('market', 'unknown')}:{payload.get('timestamp', 'unknown')}"
```
```

---

### [WARNING] time.sleep blocks main thread during long intervals

**File**: `src/quant_alpha/streaming/risingwave/producer.py:58`
**Category**: Potential Bug
**Confidence**: 75%

For large INTERVAL values (e.g., 3600 seconds), the process sleeps for a long time with no way to interrupt or monitor health. Combined with no signal handling, this makes the process unresponsive during sleep.

**Suggestion**:
```
Consider using a shorter sleep in a loop with a stop event:
```python
stop = threading.Event()
def _shutdown(): stop.set()
signal.signal(signal.SIGTERM, lambda *_: _shutdown())
while not stop.wait(INTERVAL):
    ...
```
```

---

### [WARNING] Missing input validation in get_scarcity_alerts

**File**: `src/quant_alpha/streaming/risingwave/simulator.py:80-91`
**Category**: Potential Bug
**Confidence**: 80%

The function assumes the input DataFrame 'panel' contains the column 'alpha_residual_load_rank' and 'alpha_momentum_6h'. If called with a DataFrame missing these columns (e.g., an empty result from build_realtime_alpha_panel or a misconstructed panel), a KeyError will be raised with no useful error message.

**Suggestion**:
```
Add column existence check at the start:
```python
required_cols = {"alpha_residual_load_rank", "alpha_momentum_6h", "timestamp"}
missing = required_cols - set(panel.columns)
if missing:
    raise ValueError(f"Missing required columns: {missing}")
```
```

---

### [WARNING] Missing NOT NULL constraints on source columns

**File**: `src/quant_alpha/streaming/risingwave/views.sql:10`
**Category**: Convention
**Confidence**: 65%

The source definition declares all columns without NOT NULL constraints. In a streaming context, missing or malformed JSON fields will produce NULLs silently. While some downstream expressions handle NULLs (NULLIF, COALESCE), others like spot_price - gas_price in imbalance_premium do not explicitly handle NULL propagation.

**Suggestion**:
```
Consider adding NOT NULL constraints where business logic requires non-null values, or add explicit NULL handling in all downstream expressions.
```

---

### [WARNING] Hardcoded default gas price magic number

**File**: `src/quant_alpha/streaming/risingwave/views.sql:75-76`
**Category**: Potential Bug
**Confidence**: 85%

COALESCE(gas_price, 35.0) uses a hardcoded fallback value of 35.0 for gas price. This magic number may become stale over time, producing misleading gas_spark_spread calculations. If gas_price is NULL, the resulting spread will be systematically biased.

**Suggestion**:
```
Use a configurable default or a rolling average from historical data instead of a hardcoded constant. At minimum, document the source/rationale for 35.0 and add a comment noting when it should be updated.
```

---

### [WARNING] PERCENT_RANK with PARTITION BY timestamp may have single-row partitions

**File**: `src/quant_alpha/streaming/risingwave/views.sql:108-158`
**Category**: Potential Bug
**Confidence**: 65%

All PERCENT_RANK() window functions in mv_realtime_alpha_scores are PARTITION BY timestamp. If a given timestamp has only one market (one row), PERCENT_RANK() returns 0 for that single row. This means early in the stream or for isolated timestamps, all alpha factors will be 0, producing misleading signals. Additionally, PERCENT_RANK() returns values in [0, 1], so alpha_solar_penetration (1.0 - PERCENT_RANK) will also be in [0, 1], but the semantics are inverted correctly.

**Suggestion**:
```
Document this behavior and consider adding a minimum partition size check or using a time-bucketed window approach to ensure enough data points per partition for meaningful rankings.
```

---

### [WARNING] Comment claims 8 alpha factors but only 7 exist

**File**: `src/quant_alpha/streaming/risingwave/views.sql:155`
**Category**: Readability
**Confidence**: 95%

The comment on line 105 states 'Emits one row per (timestamp, market) with all 8 alpha factor values', but the SELECT only computes 7 alpha factors (alpha_residual_load_rank through alpha_gas_spark_spread). This is a documentation discrepancy.

**Suggestion**:
```
Update the comment to say '7 alpha factor values' or add the missing 8th alpha factor.
```

---

### [WARNING] Scarcity alert ELSE 'LOW' is unreachable

**File**: `src/quant_alpha/streaming/risingwave/views.sql:160-167`
**Category**: Potential Bug
**Confidence**: 90%

In mv_scarcity_alerts, the WHERE clause filters for alpha_residual_load_rank > 0.8. The CASE expression has 'WHEN alpha_residual_load_rank > 0.8 THEN MEDIUM' as the second condition, but rows with alpha_residual_load_rank between 0.8 and 0.9 and alpha_momentum_6h <= 0.7 will be classified as 'MEDIUM'. The ELSE 'LOW' branch is unreachable given the WHERE filter, which is dead code and may confuse future maintainers.

**Suggestion**:
```
Remove the ELSE 'LOW' branch or adjust the WHERE clause to allow lower-ranked rows if 'LOW' alerts are desired. Consider adding a comment explaining why 'LOW' is excluded.
```

---

### [WARNING] Missing docstrings for all test functions

**File**: `tests/test_alpha_factors.py:9-69`
**Category**: Convention
**Confidence**: 85%

All five test functions lack docstrings explaining what specific behavior they validate. While pytest doesn't require them, docstrings improve test maintainability and help other developers understand test intent.

**Suggestion**:
```
Add docstrings to each test function, e.g.:
```python
def test_alpha_panel_contains_expected_columns() -> None:
    """Verify that alpha factors DataFrame contains all expected columns."""
```
```

---

### [WARNING] Hardcoded magic number for factor count

**File**: `tests/test_alpha_factors.py:23-24`
**Category**: Readability
**Confidence**: 65%

The assertion `len(BASE_FACTOR_COLUMNS) == 10` uses a hardcoded magic number. If BASE_FACTOR_COLUMNS is intentionally dynamic or changes, this test becomes fragile and provides no context about which 10 factors are expected.

**Suggestion**:
```
Consider asserting against an explicit expected list of factor names, or add a comment explaining why 10 is the canonical count.
```

---

### [WARNING] Test depends on specific factor column name

**File**: `tests/test_alpha_factors.py:27-37`
**Category**: Potential Bug
**Confidence**: 75%

The test `test_factors_are_lagged_at_start` asserts on column `alpha_trend_021_medium_momentum` specifically. If this column is renamed or removed from BASE_FACTOR_COLUMNS, the test will fail with a KeyError rather than a meaningful assertion failure.

**Suggestion**:
```
Use a fixture or constant to define the tested column name, or iterate over all factor columns to verify lagging behavior generically:
```python
for col in BASE_FACTOR_COLUMNS:
    assert first_rows[col].isna().all(), f"Factor {col} not properly lagged at start"
```
```

---

### [WARNING] Slicing may yield fewer than expected factors

**File**: `tests/test_alpha_factors.py:42-44`
**Category**: Potential Bug
**Confidence**: 60%

The line `test_cols = BASE_FACTOR_COLUMNS[:2]` assumes BASE_FACTOR_COLUMNS has at least 2 elements. While test_alpha_panel_has_ten_factors verifies this, test order dependencies are fragile in pytest.

**Suggestion**:
```
Add a local guard: `assert len(BASE_FACTOR_COLUMNS) >= 2, "Need at least 2 base factor columns"`
```

---

### [WARNING] Hardcoded path assumes directory structure

**File**: `tests/test_bruin_graph.py:8`
**Category**: Potential Bug
**Confidence**: 80%

The `BRUIN_ROOT` path is hardcoded relative to the test file location (`Path(__file__).parent.parent / "bruin"`). This assumes a specific directory structure that may not exist in all environments or CI pipelines. If the `bruin` directory is missing or moved, all tests will fail with unclear errors.

**Suggestion**:
```
Use a more robust path resolution approach:
```python
import os

BRUIN_ROOT = Path(os.environ.get('BRUIN_ROOT', Path(__file__).parent.parent / 'bruin'))
```
Or add a fixture that validates the path exists before running tests.
```

---

### [WARNING] Missing error handling for graph initialization

**File**: `tests/test_bruin_graph.py:11-13`
**Category**: Potential Bug
**Confidence**: 80%

The `AssetGraph(BRUIN_ROOT)` constructor is called multiple times without error handling. If the constructor raises an exception (e.g., missing files, invalid configuration), the test will fail with an unclear stack trace rather than a meaningful assertion error.

**Suggestion**:
```
Consider using a pytest fixture that handles graph initialization with proper error messages:
```python
@pytest.fixture
def graph():
    try:
        return AssetGraph(BRUIN_ROOT)
    except Exception as e:
        pytest.fail(f"Failed to initialize AssetGraph: {e}")
```
```

---

### [WARNING] Magic numbers in test assertions

**File**: `tests/test_bruin_graph.py:22-34`
**Category**: Readability
**Confidence**: 70%

The test uses hardcoded numbers (>= 4, > 5, >= 2) without clear documentation of why these specific values are chosen. This makes it unclear what the expected behavior is and makes tests brittle to legitimate changes.

**Suggestion**:
```
Define constants with clear names:
```python
MINIMUM_EXPECTED_ASSETS = 4
MINIMUM_COLUMNS_PER_TABLE = 5
MINIMUM_CUSTOM_CHECKS = 2
```
Then use these constants in assertions with explanatory comments.
```

---

### [WARNING] Unclear test assertion for upstream traversal

**File**: `tests/test_bruin_graph.py:55-60`
**Category**: Readability
**Confidence**: 70%

The test `test_upstream_traversal` checks that specific assets are in the upstream collection, but doesn't verify the total number of upstream assets or that no unexpected assets are included. This makes it unclear what the expected upstream graph looks like.

**Suggestion**:
```
Add more comprehensive assertions:
```python
def test_upstream_traversal() -> None:
    graph = AssetGraph(BRUIN_ROOT)
    upstream = graph.upstream("fct_alpha_diagnostics")
    
    # Verify expected assets are present
    assert "fct_equity_alpha_panel" in upstream
    assert "stg_equity_ohlcv" in upstream
    
    # Verify graph structure is reasonable
    assert len(upstream) >= 2, f"Expected at least 2 upstream assets, got {len(upstream)}"
```
```

---

### [WARNING] Missing docstrings for test functions

**File**: `tests/test_bruin_graph.py:55-60`
**Category**: Readability
**Confidence**: 60%

None of the test functions have docstrings explaining what they test or why specific assertions are made. This makes it harder for other developers to understand the purpose and expected behavior of each test.

**Suggestion**:
```
Add docstrings to each test function:
```python
def test_upstream_traversal() -> None:
    """Verify that upstream traversal returns correct dependency chain."""
    graph = AssetGraph(BRUIN_ROOT)
    upstream = graph.upstream("fct_alpha_diagnostics")
    
    assert "fct_equity_alpha_panel" in upstream
    assert "stg_equity_ohlcv" in upstream
```
```

---

### [WARNING] Hardcoded asset names in contract test

**File**: `tests/test_bruin_graph.py:78-83`
**Category**: Readability
**Confidence**: 80%

The test `test_contracts_cover_all_tables` uses hardcoded asset names (`raw_prices`, `alpha_diagnostics`, `power_market_raw`) that may not match the actual asset names defined elsewhere in the codebase. This creates a maintenance burden and potential for inconsistent naming.

**Suggestion**:
```
Consider using constants or importing the expected names from a central location:
```python
EXPECTED_CONTRACT_TABLES = {
    "raw_prices",
    "alpha_diagnostics",
    "power_market_raw",
}

def test_contracts_cover_all_tables() -> None:
    from quant_alpha.platform.contracts import ALL_DATASETS
    names = {d.name for d in ALL_DATASETS}
    assert EXPECTED_CONTRACT_TABLES.issubset(names)
```
```

---

### [WARNING] Test relies on specific asset metadata

**File**: `tests/test_bruin_graph.py:86-92`
**Category**: Potential Bug
**Confidence**: 70%

The test `test_asset_metadata_fields` assumes specific metadata values for `raw_power_market` (owner, tags, column count, custom checks). If the asset definition changes, this test will fail even if the graph functionality is working correctly.

**Suggestion**:
```
Consider making the test more flexible or adding clear documentation about why these specific values are expected:
```python
def test_asset_metadata_fields() -> None:
    graph = AssetGraph(BRUIN_ROOT)
    node = graph.nodes["raw_power_market"]
    
    # Verify metadata structure exists
    assert hasattr(node, 'owner')
    assert hasattr(node, 'tags')
    assert hasattr(node, 'columns')
    assert hasattr(node, 'custom_checks')
    
    # Verify metadata has reasonable values
    assert isinstance(node.owner, str)
    assert isinstance(node.tags, (list, set))
    assert len(node.columns) > 0
    assert len(node.custom_checks) > 0
```
```

---

### [WARNING] Missing docstring for test function

**File**: `tests/test_cloud_export.py:10-15`
**Category**: Convention
**Confidence**: 70%

The test function 'test_cloud_export_disabled_is_noop' is missing a docstring explaining what it tests and why.

**Suggestion**:
```
Add a docstring to the test function:
```python
def test_cloud_export_disabled_is_noop() -> None:
    """Test that export returns empty dict when cloud export is disabled."""
```
```

---

### [WARNING] Missing docstring for test function

**File**: `tests/test_cloud_export.py:18-24`
**Category**: Convention
**Confidence**: 70%

The test function 'test_cloud_export_requires_destination_config' is missing a docstring explaining what it tests and why.

**Suggestion**:
```
Add a docstring to the test function:
```python
def test_cloud_export_requires_destination_config() -> None:
    """Test that export raises error when required destination config is missing."""
```
```

---

### [WARNING] Test depends on external data generation

**File**: `tests/test_diagnostics.py:11-13`
**Category**: Potential Bug
**Confidence**: 70%

Test uses generate_synthetic_prices() which may produce non-deterministic or system-dependent results, potentially causing flaky tests across different environments or runs.

**Suggestion**:
```
Consider seeding random number generators or using fixed test data fixtures to ensure deterministic test outcomes.
```

---

### [WARNING] Missing newline at end of file

**File**: `tests/test_dlt_pipelines.py:85`
**Category**: Potential Bug
**Confidence**: 90%

The file does not end with a newline character, which violates PEP 8 and can cause issues with some tools.

**Suggestion**:
```
Add a newline at the end of the file after the last line.
```

---

### [WARNING] Test depends on external synthetic data generation

**File**: `tests/test_energy_alpha.py:9`
**Category**: Potential Bug
**Confidence**: 65%

All tests call generate_synthetic_power_market() which likely uses random/seeded data. If the synthetic data generation is not deterministic (e.g., no fixed random seed), tests may produce non-reproducible results, especially test_energy_alpha_module_produces_expression_columns which only checks > 0 NaN count.

**Suggestion**:
```
Ensure generate_synthetic_power_market uses a fixed random seed internally, or pass a seed parameter. For the notna() assertion on line 15, consider checking for a minimum threshold rather than just > 0:
```python
assert panel[list(ENERGY_ALPHA_EXPRESSIONS)].notna().sum().sum() > some_minimum
```
```

---

### [WARNING] Magic number in test assertion

**File**: `tests/test_energy_alpha.py:20-21`
**Category**: Convention
**Confidence**: 60%

The expected count of 8 factors in ENERGY_ALPHA_REGISTRY is a hardcoded magic number. If the registry changes, this test will break without clear documentation of what the actual factors are.

**Suggestion**:
```
Consider either documenting which 8 factors are expected, or using the actual known list for comparison:
```python
EXPECTED_FACTORS = [...]  # List all 8 expected factor names
assert len(ENERGY_ALPHA_REGISTRY) == len(EXPECTED_FACTORS)
```
```

---

### [WARNING] Test assertion relies on timezone-sensitive comparison

**File**: `tests/test_entsoe.py:39-43`
**Category**: Potential Bug
**Confidence**: 80%

The assertion `assert list(series.index) == list(pd.date_range("2024-01-01", periods=3, freq="h"))` compares DatetimeIndex values. If `parse_entsoe_timeseries` returns timezone-aware timestamps (UTC, as the XML data suggests), but `pd.date_range` returns timezone-naive timestamps, this comparison may fail in different environments or pandas versions. The test does not explicitly specify tz-aware or tz-naive behavior.

**Suggestion**:
```
Make timezone expectations explicit in the test:
```python
expected = pd.date_range("2024-01-01", periods=3, freq="h", tz="UTC")
assert list(series.index) == list(expected)
```
Or if the function returns naive timestamps, document that behavior clearly.
```

---

### [WARNING] Fragile assertion on missing key

**File**: `tests/test_quality.py:10`
**Category**: Potential Bug
**Confidence**: 75%

The test directly accesses `checks["passed"]` without verifying the key exists or that `checks` is a DataFrame/Series. If `run_energy_quality_checks` returns an unexpected structure (e.g., None, empty dict, or different key name), the test will fail with a confusing KeyError rather than a clear assertion failure.

**Suggestion**:
```
Add a defensive assertion before the final check:
```python
def test_energy_quality_checks_pass_for_synthetic_data() -> None:
    frame = generate_synthetic_power_market(["DE_LU", "CZ"], "2024-01-01", "2024-01-03")
    checks = run_energy_quality_checks(frame)
    
    assert checks is not None, "run_energy_quality_checks returned None"
    assert "passed" in checks, f"'passed' key missing from checks: {checks}"
    assert checks["passed"].all()
```
```

---

### [WARNING] Magic number assertion without explanation

**File**: `tests/test_risingwave_simulator.py:10`
**Category**: Potential Bug
**Confidence**: 75%

The assertion `len(panel) >= 20` uses a magic number that doesn't clearly correspond to the expected 24 rows mentioned in the comment. This makes the test brittle and unclear about what boundary conditions are acceptable.

**Suggestion**:
```
Define a constant with a clear name to explain the tolerance:
```python
EXPECTED_ROWS = 2 * 12  # 2 markets × 12 hours
ROW_TOLERANCE = 4  # Allow for boundary variations
assert len(panel) >= EXPECTED_ROWS - ROW_TOLERANCE
```
```

---

### [WARNING] Inconsistent threshold comparison

**File**: `tests/test_risingwave_simulator.py:56-58`
**Category**: Potential Bug
**Confidence**: 80%

In test_scarcity_alerts_subset_of_panel(), the threshold is 0.8 but the assertion checks for values > 0.8. If the function is supposed to return rows where alpha_residual_load_rank >= threshold, this test would incorrectly pass rows with exactly 0.8.

**Suggestion**:
```
Verify the threshold behavior and use consistent comparison:
```python
# If threshold should be inclusive:
assert (alerts["alpha_residual_load_rank"] >= 0.8).all()
# Or update the threshold to match the assertion:
alerts = get_scarcity_alerts(panel, threshold=0.80001)
```
```

---

### [WARNING] Test depends on relative file path

**File**: `tests/test_risingwave_simulator.py:70-78`
**Category**: Potential Bug
**Confidence**: 85%

test_views_sql_parses() uses a relative path construction that assumes a specific project structure. This test will fail if the test runner changes working directory or if the project structure is reorganized.

**Suggestion**:
```
Use importlib.resources or pkg_resources for more robust path handling:
```python
from importlib import resources
# Or use a fixture that provides the path
sql_path = resources.files('quant_alpha.streaming.risingwave').joinpath('views.sql')
```
```

---

### [WARNING] File existence not validated

**File**: `tests/test_risingwave_simulator.py:70-78`
**Category**: Potential Bug
**Confidence**: 80%

The test assumes the SQL file exists but doesn't handle the case where it's missing. If the file doesn't exist, read_text() will raise FileNotFoundError, causing an unhelpful test failure.

**Suggestion**:
```
Add explicit file existence check:
```python
assert sql_path.exists(), f"SQL file not found: {sql_path}"
sql = sql_path.read_text()
```
```

---

## INFO Issues (155)

### [INFO] Missing type hints and docstrings

**File**: `bruin/pipelines/energy_ingestion/run_energy_ingestion.py:1-16`
**Category**: Convention
**Confidence**: 70%

The module lacks type hints for variables and functions, and while there's a module docstring, there's no documentation of expected environment variables or the pipeline's behavior.

**Suggestion**:
```
Add comprehensive documentation:
```python
"""Bruin asset runner: raw_power_market.

Environment Variables:
    PROJECT_ROOT: Root directory of the project (default: current directory)
    ENERGY_SOURCE: Optional override for energy data source

Raises:
    FileNotFoundError: If config file doesn't exist
    KeyError: If pipeline result is malformed
"""
```
```

---

### [INFO] No NULL handling for load-based fields

**File**: `bruin/pipelines/energy_ingestion/stg_power_market.sql:28-37`
**Category**: Readability
**Confidence**: 70%

The query returns load_forecast, actual_load (COALESCE'd), wind_forecast, solar_forecast, residual_load, imbalance_price, and gas_price without any documentation of which are expected to be nullable and which are required. While actual_load is handled via COALESCE, other fields like wind_forecast, solar_forecast, residual_load, and imbalance_price could also be NULL, potentially causing issues in downstream calculations.

**Suggestion**:
```
Consider adding NOT NULL checks or COALESCE for fields used in downstream computations, and document expected nullability in the column metadata section of the header comment.
```

---

### [INFO] gas_spark_spread null when gas_price is null but default differs from 0

**File**: `bruin/pipelines/energy_ingestion/stg_power_market.sql:37`
**Category**: Potential Bug
**Confidence**: 70%

When gas_price IS NULL, the COALESCE substitutes 35.0, so gas_spark_spread becomes spot_price - 35.0. This produces a synthetic spread value that looks real but is derived from an assumed price. If downstream analytics aggregate across markets with varying gas_price NULL rates, results will be biased.

**Suggestion**:
```
Consider documenting or flagging imputed values, or leaving gas_spark_spread as NULL when gas_price is absent:
```sql
CASE WHEN gas_price IS NOT NULL THEN spot_price - gas_price END AS gas_spark_spread,
(gas_price IS NULL) AS gas_price_imputed,
```
```

---

### [INFO] Module-level variables lack type hints

**File**: `bruin/pipelines/equity_ingestion/run_equity_ingestion.py:9-11`
**Category**: Readability
**Confidence**: 75%

Variables root, config, and offline are defined at module level without type hints, reducing code clarity.

**Suggestion**:
```
Add type hints:
```python
root: Path = Path(os.environ.get("PROJECT_ROOT", "."))
config: Path = root / "configs" / "project.yaml"
offline: bool = os.environ.get("OFFLINE", "true").lower() == "true"
```
```

---

### [INFO] Fallback to current directory may cause path issues

**File**: `bruin/pipelines/equity_ingestion/run_equity_ingestion.py:10`
**Category**: Potential Bug
**Confidence**: 70%

If PROJECT_ROOT is not set, the script falls back to "." (current working directory), which may not be the expected project root depending on where the script is invoked from, leading to config file not found errors.

**Suggestion**:
```
Consider making PROJECT_ROOT required or documenting the expected working directory:
```python
root = Path(os.environ.get("PROJECT_ROOT", "."))
if not (root / "configs" / "project.yaml").exists():
    raise FileNotFoundError(f"Config not found. Set PROJECT_ROOT or run from project root.")
```
```

---

### [INFO] Missing positive check for ret_1d column

**File**: `bruin/pipelines/equity_ingestion/stg_equity_ohlcv.sql:22`
**Category**: Code Style
**Confidence**: 60%

The asset metadata declares that adj_close has a 'positive' check, but ret_1d (daily log return) can legitimately be negative (price decreases). However, there is no documentation or validation on reasonable bounds for ret_1d (e.g., |ret_1d| < 1.0 would flag unrealistic single-day moves).

**Suggestion**:
```
Consider adding a reasonable bounds check or documentation:
```yaml
- name: ret_1d
  description: Daily log return (can be negative for price decreases)
  checks: [not_null]
```
```

---

### [INFO] Missing column comments for open/high/low/close/volume

**File**: `bruin/pipelines/equity_ingestion/stg_equity_ohlcv.sql:27-35`
**Category**: Readability
**Confidence**: 80%

The asset metadata (lines 17-24) documents checks for date, symbol, adj_close, and a description for ret_1d, but provides no documentation or checks for open, high, low, close, or volume columns. These are important financial data points that should be validated (e.g., not null, non-negative, high >= low, etc.).

**Suggestion**:
```
Add column metadata with appropriate checks:
```yaml
columns:
  - name: open
    checks: [not_null, positive]
  - name: high
    checks: [not_null, positive]
  - name: low
    checks: [not_null, positive]
  - name: close
    checks: [not_null, positive]
  - name: volume
    checks: [not_null]
```
Also consider adding a check that high >= low.
```

---

### [INFO] Deduplication ordering is non-deterministic

**File**: `bruin/pipelines/equity_ingestion/stg_equity_ohlcv.sql:37`
**Category**: Readability
**Confidence**: 85%

ROW_NUMBER() OVER (PARTITION BY date, symbol ORDER BY date) — since date is the partition key, ordering by date within the same date group provides no meaningful ordering. If there are true duplicates with the same date and symbol but different prices/volumes, the selected row is arbitrary and non-reproducible.

**Suggestion**:
```
Add a meaningful tiebreaker to ensure deterministic deduplication, such as a load timestamp or an id column:
```sql
ROW_NUMBER() OVER (PARTITION BY date, symbol ORDER BY loaded_at DESC)
```
```

---

### [INFO] backtest_daily alias source is undocumented

**File**: `bruin/pipelines/reporting/rpt_backtest_summary.sql:42`
**Category**: Readability
**Confidence**: 80%

The query references backtest_daily AS b but this table is not documented in the depends section or in any comments. It's unclear where this table comes from, what its schema is, or whether it's a staging/fact table.

**Suggestion**:
```
Add backtest_daily to the depends section and consider adding a comment explaining its origin and expected schema.
```

---

### [INFO] ORDER BY in a table materialization may be unnecessary

**File**: `bruin/pipelines/reporting/rpt_backtest_summary.sql:48`
**Category**: Performance
**Confidence**: 60%

The query ends with ORDER BY d.alpha_name, b.date. If this is used to create a materialized table (duckdb.table), the sort order is not guaranteed to be preserved in the table storage. This adds computation cost without guaranteed benefit.

**Suggestion**:
```
If sort order is needed for downstream consumers, document this expectation. Otherwise, consider removing the ORDER BY or moving it to the consumer query.
```

---

### [INFO] current_timestamp may vary across databases

**File**: `dbt_energy_alpha/models/marts/fct_energy_alpha_decay.sql:14`
**Category**: Potential Bug
**Confidence**: 60%

The behavior of current_timestamp can differ between database platforms (some return UTC, others local time). For a data warehouse refresh timestamp, this should be consistent.

**Suggestion**:
```
Consider using a dbt macro for consistent timestamp handling, or explicitly use UTC:
```sql
-- Use a consistent timestamp function
{{ dbt_utils.current_timestamp() }} as refreshed_at
-- Or explicitly:
CONVERT_TZ(NOW(), @@session.time_zone, '+00:00') as refreshed_at
```
```

---

### [INFO] No data tests defined

**File**: `dbt_energy_alpha/models/marts/fct_energy_alpha_diagnostics.sql:1-10`
**Category**: Code Style
**Confidence**: 90%

The model doesn't appear to have any data quality tests defined. For a diagnostics table containing financial metrics (IC, Sharpe, drawdown), implementing tests for null values, accepted ranges, and uniqueness of alpha_name would catch data quality issues early.

**Suggestion**:
```
Add tests in schema.yml:
```yaml
models:
  - name: fct_energy_alpha_diagnostics
    columns:
      - name: alpha_name
        tests:
          - unique
          - not_null
      - name: consistency_score
        tests:
          - not_null
          - dbt_utils.accepted_range:
              min_value: 0
              max_value: 1
```
```

---

### [INFO] No NULL handling for numeric metrics

**File**: `dbt_energy_alpha/models/marts/fct_energy_alpha_diagnostics.sql:1-10`
**Category**: Convention
**Confidence**: 60%

The model passes through numeric columns (IC means, scores, Sharpe, drawdown) without explicit NULL handling. Downstream consumers may not handle NULLs properly, leading to unexpected results in aggregations or comparisons.

**Suggestion**:
```
Consider adding COALESCE for critical metrics:
```sql
select
    alpha_name,
    is_ic_mean,
    oos_ic_mean,
    is_oos_ic_same_sign,
    coalesce(consistency_score, 0) as consistency_score,
    coalesce(robustness_score, 0) as robustness_score,
    coalesce(oos_sharpe, 0) as oos_sharpe,
    coalesce(oos_max_drawdown, 0) as oos_max_drawdown
from {{ source('energy_raw', 'energy_alpha_diagnostics') }}
```
```

---

### [INFO] Missing column comments or aliases

**File**: `dbt_energy_alpha/models/marts/fct_energy_backtest_daily.sql:8`
**Category**: Readability
**Confidence**: 60%

Columns like 'equity_curve', 'long_count', 'short_count' lack documentation. In a financial context, it's unclear what 'equity_curve' represents (cumulative return, actual dollar value, etc.) without comments or documentation.

**Suggestion**:
```
Add SQL comments for complex columns:
equity_curve,  -- cumulative portfolio value in dollars
long_count,    -- number of long positions held
short_count    -- number of short positions held
```

---

### [INFO] Missing dbt model configuration block

**File**: `dbt_energy_alpha/models/staging/stg_energy_alphas.sql:1-10`
**Category**: Convention
**Confidence**: 70%

This staging model has no config block specifying materialization, schema, or tags. While staging models often inherit defaults, explicit configuration is a dbt best practice for clarity and maintainability.

**Suggestion**:
```
Add a config block at the top:
```sql
{{
    config(
        materialized='view',
        tags=['staging', 'energy', 'alphas']
    )
}}

select
    ...
```
```

---

### [INFO] Missing model description and column documentation

**File**: `dbt_energy_alpha/models/staging/stg_energy_alphas.sql:1-10`
**Category**: Convention
**Confidence**: 80%

The staging model has no description or column-level documentation. dbt best practices recommend adding YAML-based descriptions and column comments to enable auto-generated documentation and improve team understanding.

**Suggestion**:
```
Add a corresponding schema YAML file with model and column descriptions:
```yaml
version: 2
models:
  - name: stg_energy_alphas
    description: >
      Staging model for energy alpha signals from the power market features source.
    columns:
      - name: market_ts
        description: 'Timestamp of the market observation'
      - name: alpha_composite
        description: 'Composite alpha signal'
      # ... remaining columns
```
```

---

### [INFO] Missing model documentation

**File**: `dbt_energy_alpha/models/staging/stg_power_market.sql:1-10`
**Category**: Convention
**Confidence**: 80%

The staging model lacks documentation including a description of its purpose, data freshness requirements, and expected row counts. In dbt projects, staging models should have documentation for maintainability.

**Suggestion**:
```
Add a YAML documentation file or inline comments:
```sql
-- Staging model for power market features
-- Source: energy_raw.power_market_features
-- Description: Cleans and standardizes market data for downstream models
select
    ...
```
```

---

### [INFO] No data quality tests defined

**File**: `dbt_energy_alpha/models/staging/stg_power_market.sql:1-10`
**Category**: Architecture
**Confidence**: 85%

The staging model doesn't include any data quality tests for critical business columns like spot_price or timestamp. Without tests, data quality issues could propagate to downstream models.

**Suggestion**:
```
Add tests in the corresponding YAML file:
```yaml
models:
  - name: stg_power_market
    columns:
      - name: market_ts
        tests:
          - not_null
      - name: spot_price
        tests:
          - not_null
          - dbt_utils.accepted_range:
              min_value: -1000
              max_value: 10000
```
```

---

### [INFO] Missing model description/documentation

**File**: `dbt_quant_alpha/models/marts/fct_alpha_decay.sql:1`
**Category**: Convention
**Confidence**: 80%

The dbt model lacks a description in the config block or a schema.yml file. For a 'fct_' prefixed fact table, documentation about the grain, what each row represents, and business context is important.

**Suggestion**:
```
Add documentation in schema.yml describing the model's purpose, grain (one row per alpha_name/horizon_days combination), and the business meaning of ic_regime classifications.
```

---

### [INFO] Missing ORDER BY documentation

**File**: `dbt_quant_alpha/models/marts/fct_alpha_decay.sql:14`
**Category**: Convention
**Confidence**: 60%

The ORDER BY clause is included in a table materialization. While this doesn't affect the stored table (tables aren't guaranteed to maintain order in most databases), it adds unnecessary processing overhead and may mislead readers about data ordering guarantees.

**Suggestion**:
```
Either remove the ORDER BY clause if ordering isn't required for downstream consumption, or add a comment explaining why ordering is necessary (e.g., for deterministic output in testing).
```

---

### [INFO] Missing model documentation or column descriptions

**File**: `dbt_quant_alpha/models/marts/fct_alpha_panel.sql:1-17`
**Category**: Convention
**Confidence**: 65%

dbt best practices recommend adding column descriptions using schema.yml or doc blocks. This model has many alpha signal columns with no documentation of what each represents, their expected ranges, or data types.

**Suggestion**:
```
Create or update a schema.yml file with column descriptions:
```yaml
models:
  - name: fct_alpha_panel
    columns:
      - name: alpha_composite
        description: 'Composite alpha signal score'
      - name: rolling_63d_rank_ic_proxy
        description: '63-day rolling Pearson correlation between alpha_composite and forward_return'
```
```

---

### [INFO] Missing dbt model configuration

**File**: `dbt_quant_alpha/models/marts/fct_alpha_panel.sql:17`
**Category**: Convention
**Confidence**: 70%

The model lacks a configuration block specifying materialization strategy, schema, or other dbt configuration. For a mart-level table, this should typically be materialized as a table or incremental model with appropriate configuration.

**Suggestion**:
```
Add a config block at the top of the file:
```sql
{{ config(
    materialized='table',
    schema='marts',
    tags=['alpha', 'panel']
) }}

select
    ...
```
```

---

### [INFO] Missing column documentation

**File**: `dbt_quant_alpha/models/marts/fct_backtest_daily.sql:2`
**Category**: Convention
**Confidence**: 85%

The model lacks documentation for column definitions, data types, and business logic. In dbt projects, columns should be documented in a YAML schema file to ensure data catalog completeness and team understanding.

**Suggestion**:
```
Add a corresponding schema YAML file with column descriptions:
```yaml
version: 2
models:
  - name: fct_backtest_daily
    description: 'Daily backtest performance metrics'
    columns:
      - name: signal_date
        description: 'Date of the trading signal'
      - name: gross_return
        description: 'Gross return before costs'
      # ... add all column descriptions
```
```

---

### [INFO] Missing trailing semicolon

**File**: `dbt_quant_alpha/models/marts/fct_backtest_daily.sql:8`
**Category**: Convention
**Confidence**: 60%

The SQL statement does not end with a semicolon. While dbt often handles this, it's best practice to include it for clarity and compatibility with different SQL engines.

**Suggestion**:
```
Add a semicolon at the end of the SQL statement:
```sql
from {{ source('quant_alpha_raw', 'backtest_daily') }};
```
```

---

### [INFO] Missing dbt config block

**File**: `dbt_quant_alpha/models/staging/stg_factor_panel.sql:1-14`
**Category**: Convention
**Confidence**: 65%

The model does not include a dbt configuration block specifying materialization strategy, schema, or other settings. This means it will use default settings which may not be optimal.

**Suggestion**:
```
Add appropriate configuration:
```sql
{{
  config(
    materialized='view',
    schema='staging'
  )
}}

select
    ...
```
```

---

### [INFO] Missing semicolon at end of SQL statement

**File**: `dbt_quant_alpha/models/staging/stg_factor_panel.sql:14`
**Category**: Code Style
**Confidence**: 60%

The SQL statement does not end with a semicolon. While dbt typically handles this, it's best practice to include it for SQL standards compliance and clarity.

**Suggestion**:
```
Add a semicolon at the end of the statement:
```sql
from {{ source('quant_alpha_raw', 'factor_panel') }};
```
```

---

### [INFO] Missing column alias for date transformation

**File**: `dbt_quant_alpha/models/staging/stg_prices.sql:2`
**Category**: Code Style
**Confidence**: 60%

The cast(date as date) transformation creates a column named 'price_date', but the original column name is not documented. This could cause confusion about whether 'date' is a reserved word or if there's a reason for the aliasing.

**Suggestion**:
```
Consider adding a comment explaining why the column is being aliased, e.g.:\n-- Rename date column to avoid reserved word conflicts\ncast(date as date) as price_date,
```

---

### [INFO] Missing type hints for module

**File**: `src/quant_alpha/__init__.py:1-5`
**Category**: Convention
**Confidence**: 60%

While __version__ is a simple string, adding type hints to the module-level variables would improve code documentation and enable better IDE support.

**Suggestion**:
```
Consider adding type hints:
```python
__all__: list[str] = ["__version__"]
__version__: str = "0.1.0"
```
```

---

### [INFO] Missing module-level docstring format

**File**: `src/quant_alpha/backtest/__init__.py:1`
**Category**: Convention
**Confidence**: 60%

The module docstring is minimal and doesn't follow standard Python documentation practices. For a backtesting utilities module, it should describe the package contents, usage examples, or at least list the main components.

**Suggestion**:
```
Expand the docstring to provide more context:
```python
"""Backtesting utilities.

This module contains utilities for backtesting trading strategies.
Includes modules for data handling, performance metrics, and execution simulation.
"""
```

---

### [INFO] Magic number 10 for minimum observations

**File**: `src/quant_alpha/backtest/alpha_decay.py:27`
**Category**: Readability
**Confidence**: 80%

The threshold `if len(clean) < 10` uses a magic number for minimum observations for correlation calculation. This should be documented or made configurable.

---

### [INFO] Inconsistent date handling between functions

**File**: `src/quant_alpha/backtest/alpha_decay.py:78-80`
**Category**: Code Style
**Confidence**: 60%

The `walk_forward_ic` function uses `date_col` parameter and converts it, while `_daily_rank_ic` uses `date_col` but the enclosing context shows `compute_energy_alpha_decay` uses 'timestamp' and 'market'. This inconsistency could cause confusion about expected column names.

**Suggestion**:
```
Document the expected column naming conventions for each function or standardize them.
```

---

### [INFO] Potential off-by-one in OOS end date

**File**: `src/quant_alpha/backtest/alpha_decay.py:79-80`
**Category**: Potential Bug
**Confidence**: 60%

The OOS end date calculation `dates[min(idx + is_days + oos_days - 1, len(dates) - 1)]` uses `- 1` which suggests the window should include exactly `oos_days` dates. However, the filter condition uses `<= oos_end`, which could include an extra day if dates are not perfectly contiguous.

**Suggestion**:
```
Verify that the OOS window includes exactly the intended number of trading days, especially if there are gaps in the date sequence.
```

---

### [INFO] Missing validation of required columns

**File**: `src/quant_alpha/backtest/long_short.py:10`
**Category**: Potential Bug
**Confidence**: 85%

The function assumes `factor_panel` contains 'date', 'symbol', 'forward_return', and the specified `alpha_col`, but never validates their presence. Missing columns would produce cryptic pandas KeyErrors rather than clear error messages.

**Suggestion**:
```
Add input validation at the start:
```python
required = {'date', 'symbol', 'forward_return', alpha_col}
missing = required - set(factor_panel.columns)
if missing:
    raise ValueError(f"factor_panel missing required columns: {missing}")
```
```

---

### [INFO] Unnecessary sort before pivot operations

**File**: `src/quant_alpha/backtest/long_short.py:55`
**Category**: Performance
**Confidence**: 60%

The `weights.sort_values(['date', 'symbol'])` on line 46 is used to ensure deterministic ordering, but `pivot_table` does not depend on input order. The sort is only needed if the output `daily` DataFrame must be ordered by date (which `set_index` and groupby already ensure).

**Suggestion**:
```
Remove the sort if determinism is not required, or move it to after the final calculations on line 71:
```python
# Remove line 46:
# weights = weights.sort_values(['date', 'symbol'])
# Add at end before return:
daily = daily.sort_values('date').reset_index(drop=True)
```
```

---

### [INFO] Win rate may be misleading with transaction costs

**File**: `src/quant_alpha/backtest/long_short.py:89`
**Category**: Potential Bug
**Confidence**: 60%

The win rate is calculated on `portfolio_return` (net of costs), but a position could have positive gross return yet negative net return after costs. If win rate should reflect strategy alpha quality, it might be more useful to compute on gross returns or on per-position returns.

**Suggestion**:
```
Consider documenting the intent or offering both:
```python
"gross_win_rate": float((gross.set_index('date')['gross_return'] > 0).mean()),
"net_win_rate": float((ret > 0).mean()),
```
```

---

### [INFO] Metric 'observations' cast to float unnecessarily

**File**: `src/quant_alpha/backtest/long_short.py:95`
**Category**: Convention
**Confidence**: 70%

The 'observations' metric is `float(len(daily))` but an observation count is inherently an integer. Casting to float loses clarity and may confuse callers expecting a natural number.

**Suggestion**:
```
Use integer instead:
```python
"observations": len(daily),
```
Or if the return type requires float, document why.
```

---

### [INFO] Magic numbers for window sizes

**File**: `src/quant_alpha/batch/spark_energy_features.py:24-25`
**Category**: Readability
**Confidence**: 75%

The window sizes -23, -167 (representing 24h and 168h windows with 0-based offset) are magic numbers. -167 represents 7 days * 24 hours - 1.

**Suggestion**:
```
Define named constants: WINDOW_24H = 24; WINDOW_168H = 168; w_24 = w_market.rowsBetween(-(WINDOW_24H-1), 0)
```

---

### [INFO] Hardcoded threshold for scarcity flag

**File**: `src/quant_alpha/batch/spark_energy_features.py:35`
**Category**: Readability
**Confidence**: 70%

The scarcity_flag threshold of 5 for residual_load_shock is a magic number with no units or context. This should be documented or configurable.

**Suggestion**:
```
Extract to constant: SCARCITY_THRESHOLD_GW = 5 (or appropriate unit) and add comment explaining the rationale.
```

---

### [INFO] Import consistency issue

**File**: `src/quant_alpha/cli.py:11`
**Category**: Code Style
**Confidence**: 75%

The import 'from quant_alpha.ingestion.entsoe import EntsoeError' is at module level but only used inside _run_energy. Other imports use lazy loading for consistency.

**Suggestion**:
```
Move the import inside the function:
```python
def _run_energy(config: Path, root: Path, source: str | None = None) -> None:
    from quant_alpha.ingestion.entsoe import EntsoeError
    ...
```
```

---

### [INFO] Import consistency issue

**File**: `src/quant_alpha/cli.py:13`
**Category**: Code Style
**Confidence**: 80%

The import 'from quant_alpha.ingestion.dlt_energy import run_dlt_energy_pipeline' is at module level but only used inside dlt_energy_command. Other internal imports (like AssetGraph, load_project_config) use lazy imports within functions for consistency.

**Suggestion**:
```
Move the import inside the function:
```python
@app.command("dlt-energy")
def dlt_energy_command(...):
    from quant_alpha.ingestion.dlt_energy import run_dlt_energy_pipeline
    from quant_alpha.config import load_project_config
    ...
```
```

---

### [INFO] Missing docstring for _run_energy function

**File**: `src/quant_alpha/cli.py:24`
**Category**: Readability
**Confidence**: 75%

The _run_energy helper function lacks a docstring explaining its purpose and parameters.

**Suggestion**:
```
Add docstring:
```python
def _run_energy(config: Path, root: Path, source: str | None = None) -> None:
    """Run the energy research pipeline.
    
    Args:
        config: Path to energy project configuration YAML.
        root: Project root directory.
        source: Optional override for energy data source.
    """
    ...
```
```

---

### [INFO] Missing blank line between functions

**File**: `src/quant_alpha/cli.py:36`
**Category**: Code Style
**Confidence**: 80%

There's no blank line between the _run and _run_energy function definitions, which violates PEP 8 style guidelines for top-level function separation.

**Suggestion**:
```
Add blank line between functions:
```python
def _run(config: Path, root: Path, offline: bool) -> None:
    ...


def _run_energy(config: Path, root: Path, source: str | None = None) -> None:
    ...
```
```

---

### [INFO] Missing docstring for _run function

**File**: `src/quant_alpha/cli.py:37`
**Category**: Readability
**Confidence**: 75%

The _run helper function lacks a docstring explaining its purpose and parameters.

**Suggestion**:
```
Add docstring:
```python
def _run(config: Path, root: Path, offline: bool) -> None:
    """Run the main data pipeline.
    
    Args:
        config: Path to project configuration YAML.
        root: Project root directory.
        offline: If True, use deterministic synthetic prices.
    """
    ...
```
```

---

### [INFO] Missing error handling in dlt_energy_command

**File**: `src/quant_alpha/cli.py:82-90`
**Category**: Potential Bug
**Confidence**: 80%

The dlt_energy_command doesn't handle exceptions from load_project_config or run_dlt_energy_pipeline. Any error will show a raw traceback to the user.

**Suggestion**:
```
Add try-except block with user-friendly error messages:
```python
try:
    cfg = load_project_config(...)
    info = run_dlt_energy_pipeline(...)
except Exception as exc:
    typer.echo(f"DLT energy pipeline failed: {exc}", err=True)
    raise typer.Exit(code=1) from exc
```
```

---

### [INFO] Missing error handling in dlt_equity_command

**File**: `src/quant_alpha/cli.py:98-106`
**Category**: Potential Bug
**Confidence**: 80%

Similar to dlt_energy_command, the dlt_equity_command doesn't handle exceptions from load_project_config, load_universe, or run_dlt_equity_pipeline.

**Suggestion**:
```
Add try-except block with user-friendly error messages:
```python
try:
    cfg = load_project_config(config, root=root.resolve())
    universe = load_universe(cfg.universe_path)
    info = run_dlt_equity_pipeline(...)
except Exception as exc:
    typer.echo(f"DLT equity pipeline failed: {exc}", err=True)
    raise typer.Exit(code=1) from exc
```
```

---

### [INFO] Missing error handling in bruin_lineage_command

**File**: `src/quant_alpha/cli.py:109-121`
**Category**: Potential Bug
**Confidence**: 75%

The bruin_lineage_command doesn't handle exceptions from AssetGraph instantiation. If the bruin_root doesn't exist or is malformed, the error will propagate without context.

**Suggestion**:
```
Add try-except for AssetGraph instantiation:
```python
try:
    graph = AssetGraph(bruin_root.resolve())
except Exception as exc:
    typer.echo(f"Failed to load asset graph: {exc}", err=True)
    raise typer.Exit(code=1) from exc
```
```

---

### [INFO] Missing error handling in bruin_run_command

**File**: `src/quant_alpha/cli.py:124-143`
**Category**: Potential Bug
**Confidence**: 75%

The bruin_run_command doesn't handle exceptions from AssetGraph instantiation or graph.run(). If the bruin_root doesn't exist or run fails, the error will propagate without context.

**Suggestion**:
```
Add try-except for AssetGraph instantiation and graph.run():
```python
try:
    graph = AssetGraph(bruin_root.resolve())
    ...
    results = graph.run(...)
except Exception as exc:
    typer.echo(f"Bruin run failed: {exc}", err=True)
    raise typer.Exit(code=1) from exc
```
```

---

### [INFO] top_quantile + bottom_quantile could exceed 1.0

**File**: `src/quant_alpha/config.py:12`
**Category**: Readability
**Confidence**: 65%

The BacktestConfig allows top_quantile and bottom_quantile to be configured independently. If their sum exceeds 1.0, this could create overlapping quantiles. No validation is present.

**Suggestion**:
```
Add a Pydantic validator to ensure quantiles don't overlap:
```python
from pydantic import validator

class BacktestConfig(BaseModel):
    @validator('bottom_quantile')
    def validate_quantiles(cls, v, values):
        if 'top_quantile' in values and v + values['top_quantile'] > 1.0:
            raise ValueError('Sum of quantiles cannot exceed 1.0')
        return v
```
```

---

### [INFO] bigquery_location should be validated

**File**: `src/quant_alpha/config.py:38`
**Category**: Convention
**Confidence**: 60%

bigquery_location accepts any string but BigQuery has a limited set of valid locations. Invalid locations could cause runtime errors.

**Suggestion**:
```
Consider adding validation or at minimum documentation:
```python
# Valid BigQuery locations: US, EU, asia-northeast1, etc.
bigquery_location: str = "EU"
```
```

---

### [INFO] Missing validation for write_disposition enum

**File**: `src/quant_alpha/config.py:42`
**Category**: Convention
**Confidence**: 80%

CloudExportConfig.write_disposition accepts any string but should only allow valid BigQuery write dispositions (WRITE_TRUNCATE, WRITE_APPEND, WRITE_EMPTY).

**Suggestion**:
```
Use Pydantic's Literal type for validation:
```python
from typing import Literal

class CloudExportConfig(BaseModel):
    write_disposition: Literal["WRITE_TRUNCATE", "WRITE_APPEND", "WRITE_EMPTY"] = "WRITE_TRUNCATE"
```
```

---

### [INFO] Magic numbers in default values

**File**: `src/quant_alpha/config.py:76`
**Category**: Readability
**Confidence**: 70%

Default values like 252.0 (periods_per_year), 5.0 (transaction_cost_bps), 21 (momentum) etc. are not documented. These financial/trading constants would benefit from comments explaining their origin.

**Suggestion**:
```
Add comments explaining the constants:
```python
class BacktestConfig(BaseModel):
    # Number of trading days in a year
    periods_per_year: float = 252.0
    # Transaction cost in basis points (0.05%)
    transaction_cost_bps: float = 5.0
```
```

---

### [INFO] Missing docstrings for public functions

**File**: `src/quant_alpha/config.py:85`
**Category**: Code Style
**Confidence**: 90%

Public functions load_yaml, resolve_path, load_project_config, load_universe, and ensure_project_dirs lack docstrings, reducing code documentation and maintainability.

**Suggestion**:
```
Add docstrings to all public functions:
```python
def load_yaml(path: Path) -> dict[str, Any]:
    """Load and parse a YAML file.
    
    Args:
        path: Path to the YAML file.
    
    Returns:
        Parsed YAML content as a dictionary.
    """
    ...
```
```

---

### [INFO] Repeated resolve_path calls could be refactored

**File**: `src/quant_alpha/config.py:112-116`
**Category**: Readability
**Confidence**: 60%

The load_project_config function calls resolve_path multiple times for different paths. This repetitive pattern could be made more maintainable.

**Suggestion**:
```
Consider refactoring to reduce repetition:
```python
for field in ['raw_dir', 'processed_dir', 'duckdb_path', 'universe_path']:
    setattr(cfg, field, resolve_path(root, getattr(cfg, field)))
```
```

---

### [INFO] Missing docstring for _rolling_zscore

**File**: `src/quant_alpha/features/alpha_factors.py:13-16`
**Category**: Readability
**Confidence**: 60%

The private helper function _rolling_zscore lacks a docstring explaining its purpose, parameters, and return value.

**Suggestion**:
```
Add a docstring:
```python
def _rolling_zscore(series: pd.Series, window: int) -> pd.Series:
    """Compute rolling z-score normalization.
    
    Replaces zero std with NaN to avoid division by zero.
    """
```
```

---

### [INFO] Missing docstring for _breakout_position

**File**: `src/quant_alpha/features/alpha_factors.py:19-22`
**Category**: Readability
**Confidence**: 60%

The private helper function _breakout_position lacks a docstring explaining its purpose and the -0.5 offset behavior.

**Suggestion**:
```
Add a docstring:
```python
def _breakout_position(series: pd.Series, window: int) -> pd.Series:
    """Compute breakout position within rolling range [-0.5, 0.5].
    
    Returns position relative to midpoint of rolling min/max window.
    """
```
```

---

### [INFO] Missing docstring for add_alpha_factors

**File**: `src/quant_alpha/features/alpha_factors.py:25-55`
**Category**: Readability
**Confidence**: 70%

The main public function add_alpha_factors lacks a docstring explaining its purpose, parameters, and return value.

**Suggestion**:
```
Add a docstring:
```python
def add_alpha_factors(prices: pd.DataFrame, cfg: ProjectConfig) -> pd.DataFrame:
    """Compute alpha factors and composite score for equity prices.
    
    Args:
        prices: DataFrame with date, symbol, adj_close, close columns.
        cfg: Project configuration.
    
    Returns:
        DataFrame with added alpha factors, ranks, and composite score.
    """
```
```

---

### [INFO] Missing docstring for alpha_registry_frame

**File**: `src/quant_alpha/features/alpha_factors.py:58-69`
**Category**: Readability
**Confidence**: 70%

The public function alpha_registry_frame lacks a docstring explaining its purpose and return value.

**Suggestion**:
```
Add a docstring:
```python
def alpha_registry_frame(registry: list[AlphaDefinition] | None = None) -> pd.DataFrame:
    """Return a DataFrame summarizing all registered alpha definitions.
    
    Args:
        registry: Optional list of alpha definitions. Uses default registry if None.
    
    Returns:
        DataFrame with alpha_name, expression, family, hypothesis, expected_direction.
    """
```
```

---

### [INFO] Unused import: __future__ annotations

**File**: `src/quant_alpha/features/energy_alpha.py:1`
**Category**: Code Style
**Confidence**: 70%

The `from __future__ import annotations` import is present but no forward references or PEP 604 union types are used in this file. All type annotations use existing types (str, pd.Series, pd.DataFrame). This import has no effect.

**Suggestion**:
```
Remove the unused import:
```python
# Remove: from __future__ import annotations
```
```

---

### [INFO] Temporary columns use inconsistent naming

**File**: `src/quant_alpha/features/energy_alpha.py:62-171`
**Category**: Readability
**Confidence**: 60%

Temporary columns use underscore prefix (`_imbalance_diff`, `_spread_diff`, `_solar_pen`, `_demand_surprise`, `_gas_spark`) which is good, but the naming convention is inconsistent. `_demand_surprise` and `_gas_spark` are created inside conditionals and dropped inline, while the others are created unconditionally and dropped at the end. This scattered cleanup pattern is error-prone.

**Suggestion**:
```
Consider using a context manager or consistent pattern for temporary columns:
```python
temp_cols = ['_imbalance_diff', '_spread_diff', '_solar_pen']
try:
    # ... compute alphas ...
finally:
    df.drop(columns=[c for c in temp_cols if c in df.columns], inplace=True)
```
```

---

### [INFO] Window sizes (168, 72, 24, 6) are magic numbers

**File**: `src/quant_alpha/features/energy_alpha.py:62-171`
**Category**: Readability
**Confidence**: 75%

The rolling window sizes 168 (1 week of hourly data), 72 (3 days), 24 (1 day), and 6 (6 hours) appear throughout the function as magic numbers. While energy practitioners may recognize these, they're not documented in code.

**Suggestion**:
```
Define named constants at module level:
```python
WINDOW_WEEK = 168   # 7 days * 24 hours (hourly data)
WINDOW_3D = 72      # 3 days * 24 hours
WINDOW_DAY = 24     # 1 day * 24 hours
WINDOW_6H = 6       # 6 hours
```
```

---

### [INFO] Missing module-level __all__ export list

**File**: `src/quant_alpha/features/registry.py:1`
**Category**: Convention
**Confidence**: 70%

The module defines public symbols (cs_rank, ts_rank, delta, delay, etc.) but does not define __all__. This makes it unclear which symbols are intended to be part of the public API versus internal helpers.

**Suggestion**:
```
Add __all__ to explicitly define the public interface:
```python
__all__ = [
    'AlphaFn', 'AlphaDefinition', 'cs_rank', 'ts_rank', 'delta',
    'delay', 'ts_corr', 'ts_std', 'ts_mean', 'safe_divide',
    'make_equity_alpha_registry',
]
```
```

---

### [INFO] Missing docstrings for module and public functions

**File**: `src/quant_alpha/features/registry.py:14`
**Category**: Convention
**Confidence**: 90%

The module-level docstring and docstrings for cs_rank, ts_rank, delta, delay, ts_corr, ts_std, ts_mean, safe_divide, and make_equity_alpha_registry are all missing. These are public API functions that would benefit from documentation.

**Suggestion**:
```
Add module-level docstring and function docstrings explaining purpose, parameters, return values, and expected index structure:
```python
def cs_rank(series: pd.Series) -> pd.Series:
    """Compute cross-sectional percentile rank centered around zero.
    
    Assumes MultiIndex with (date, asset_id) where date is level=0.
    """
```
```

---

### [INFO] Missing docstrings on AlphaDefinition dataclass

**File**: `src/quant_alpha/features/registry.py:16-22`
**Category**: Convention
**Confidence**: 85%

The AlphaDefinition dataclass has no docstring or field-level documentation. The 'expected_direction' field (int) is unclear — does 1 mean long, -1 mean short? The relationship between 'expression' (human-readable formula) and 'compute' (actual lambda) should be documented.

**Suggestion**:
```
Add a class docstring:
```python
@dataclass(frozen=True)
class AlphaDefinition:
    """Definition of a quantitative alpha factor.
    
    Attributes:
        name: Unique identifier for the alpha.
        expression: Human-readable mathematical expression.
        family: Category grouping (e.g., 'short_reversal').
        hypothesis: Economic rationale for the alpha.
        expected_direction: 1 if higher values predict higher returns, -1 otherwise.
        compute: Function that takes a DataFrame and returns the alpha Series.
    """
```
```

---

### [INFO] Hardcoded magic numbers for MultiIndex levels

**File**: `src/quant_alpha/features/registry.py:24-51`
**Category**: Readability
**Confidence**: 70%

The functions use level=0 and level=1 as magic numbers throughout (cs_rank uses level=0, ts_rank/delta/delay use level=1). These integer literals make the code harder to understand without knowing the assumed MultiIndex layout.

**Suggestion**:
```
Define constants for the index levels at module level:
```python
_DATE_LEVEL = 0
_ASSET_LEVEL = 1
```
Then use these constants in all groupby calls for clarity.
```

---

### [INFO] Module docstring lacks detailed description

**File**: `src/quant_alpha/ingestion/__init__.py:1`
**Category**: Convention
**Confidence**: 70%

The module docstring is minimal (only one line). For a package __init__.py file, it would benefit from a more comprehensive description that outlines the module's purpose, main classes/functions, and usage examples.

**Suggestion**:
```
Consider expanding the docstring to include more details about the ingestion adapters, such as:
```python
"""Market data ingestion adapters.

This module provides adapters for ingesting market data from various sources.
Adapters handle data fetching, transformation, and storage.

Example:
    from quant_alpha.ingestion import MarketDataAdapter
    adapter = MarketDataAdapter(source='bloomberg')
    data = adapter.fetch_data('AAPL', start_date='2024-01-01')
"""
```
```

---

### [INFO] Unused import: Path from pathlib

**File**: `src/quant_alpha/ingestion/dlt_energy.py:8`
**Category**: Code Style
**Confidence**: 60%

The import `from pathlib import Path` is used, but `os` is imported but `os.environ` usage could be avoided (see architecture issue). The `Path` import is used, but worth noting that `os` usage creates a dependency that could be eliminated.

**Suggestion**:
```
This is informational. If the os.environ side effect is removed, the `import os` line can be removed.
```

---

### [INFO] No bounds check on frame after slicing by cutoff

**File**: `src/quant_alpha/ingestion/dlt_energy.py:46`
**Category**: Potential Bug
**Confidence**: 60%

After filtering the dataframe by the cutoff timestamp, if frame is empty the loop simply yields nothing. This is not a bug per se, but there is no logging or visibility into whether zero records were loaded because there truly are no new records or because of a data issue.

**Suggestion**:
```
Consider adding a debug log:
```python
import logging
logger = logging.getLogger(__name__)
# ... in the resource function:
logger.debug(f"Yielding {len(frame)} records after incremental filter (cutoff={cutoff})")
```
```

---

### [INFO] Use of deprecated pd.Timestamp.utcnow()

**File**: `src/quant_alpha/ingestion/dlt_energy.py:63-64`
**Category**: Code Style
**Confidence**: 90%

pd.Timestamp.utcnow() is deprecated in newer versions of pandas. It will trigger a FutureWarning and may be removed in a future release.

**Suggestion**:
```
Use datetime directly:
```python
from datetime import datetime, timezone
if end is None:
    end = datetime.now(timezone.utc).date().isoformat()
```
```

---

### [INFO] No return type annotation on run_dlt_energy_pipeline

**File**: `src/quant_alpha/ingestion/dlt_energy.py:96-112`
**Category**: Potential Bug
**Confidence**: 85%

The function returns a dict but has no return type annotation. While the parameter type hints are present, the return type is missing for consistency.

**Suggestion**:
```
Add return type annotation:
```python
def run_dlt_energy_pipeline(
    duckdb_path: Path,
    markets: list[str] | None = None,
    start: str = "2023-01-01",
    end: str | None = None,
) -> dict:
```
```

---

### [INFO] Redundant pd.Timestamp conversion

**File**: `src/quant_alpha/ingestion/dlt_equity.py:55-57`
**Category**: Potential Bug
**Confidence**: 60%

Row values from to_dict(orient='records') may already be pandas Timestamp objects after the earlier conversion on line 49. Wrapping in pd.Timestamp again is safe but redundant, and could hide unexpected input types.

**Suggestion**:
```
Consider casting once during the DataFrame phase: `prices['date'] = prices['date'].dt.strftime('%Y-%m-%d')` and remove per-row conversion.
```

---

### [INFO] Missing duckdb_path validation

**File**: `src/quant_alpha/ingestion/dlt_equity.py:68-69`
**Category**: Potential Bug
**Confidence**: 70%

The function does not validate that the parent directory of duckdb_path exists. If the parent directory does not exist, DuckDB will fail to create the database file.

**Suggestion**:
```
Add validation: `duckdb_path.parent.mkdir(parents=True, exist_ok=True)` before setting credentials.
```

---

### [INFO] Missing type hints on return dict

**File**: `src/quant_alpha/ingestion/dlt_equity.py:93-101`
**Category**: Convention
**Confidence**: 60%

The function returns a dict but does not specify the structure via TypedDict or more specific type hints, making it harder for callers to know what keys to expect.

**Suggestion**:
```
Define a TypedDict for the return value: `class PipelineResult(TypedDict): pipeline: str; dataset: str; ...` and use it as the return type.
```

---

### [INFO] Missing type hints for return value

**File**: `src/quant_alpha/ingestion/energy.py:9-10`
**Category**: Convention
**Confidence**: 70%

The _seed() function has return type annotation but no docstring explaining the purpose of this helper function.

**Suggestion**:
```
Add a brief docstring:
```python
def _seed(name: str) -> int:
    """Generate deterministic seed from market name string.
    
    Args:
        name: Market identifier string.
        
    Returns:
        Integer seed for numpy random generator.
    """
    return int(hashlib.sha256(name.encode("utf-8")).hexdigest()[:8], 16)
```
```

---

### [INFO] SHA256 used for non-security purpose

**File**: `src/quant_alpha/ingestion/energy.py:11`
**Category**: Security
**Confidence**: 70%

SHA256 is being used to generate deterministic seeds for random number generation, not for security purposes. While SHA256 is cryptographically secure, it's overkill for seed generation and adds unnecessary computation overhead.

**Suggestion**:
```
Consider using a simpler hash function like hashlib.md5() with a comment explaining it's not for security, or use a dedicated seed generation approach:
```python
def _seed(name: str) -> int:
    # Using MD5 for non-security seed generation
    return int(hashlib.md5(name.encode('utf-8')).hexdigest()[:8], 16)
```
Alternatively, use Python's built-in hash():
```python
def _seed(name: str) -> int:
    return hash(name) % (2**32)
```
```

---

### [INFO] Magic numbers in synthetic model

**File**: `src/quant_alpha/ingestion/energy.py:31`
**Category**: Readability
**Confidence**: 60%

The synthetic model uses many hardcoded numeric constants (55, 12, 18, 8, 14, 45, etc.) without documentation of what they represent or their units.

**Suggestion**:
```
Extract constants with descriptive names:
```python
# Base load and amplitude parameters (MW)
BASE_LOAD = 55  # Average base load
LOAD_AMPLITUDE = 12  # Diurnal variation amplitude

# Renewable parameters (MW)
BASE_WIND = 18  # Average wind generation
WIND_AMPLITUDE = 8  # Seasonal variation amplitude
MAX_SOLAR = 14  # Peak solar capacity

# Price parameters (€/MWh)
BASE_SPOT_PRICE = 45  # Base electricity price
SPOT_LOAD_COEFFICIENT = 1.4  # Price sensitivity to residual load
SPOT_SCARCITY_COEFFICIENT = 2.0  # Scarcity premium coefficient
```
```

---

### [INFO] Missing module and function docstrings

**File**: `src/quant_alpha/ingestion/entsoe.py:1-258`
**Category**: Convention
**Confidence**: 95%

The module and all public functions (parse_entsoe_timeseries, fetch_entsoe_power_market) lack docstrings. The EntsoeClient class also lacks a docstring explaining its purpose and usage.

**Suggestion**:
```
Add docstrings:
```python
"""ENTSO-E Transparency Platform API client for European power market data."""

class EntsoeClient:
    """Client for ENTSO-E Transparency Platform REST API.
    
    Requires a valid API token from https://transparency.entsoe.eu/
    """
```
```

---

### [INFO] Base URL default duplicated

**File**: `src/quant_alpha/ingestion/entsoe.py:39`
**Category**: Readability
**Confidence**: 80%

The default base_url 'https://web-api.tp.entsoe.eu/api' is defined both in the dataclass field default (line 22) and in the from_env classmethod parameter default (line 30). This creates a maintenance burden if the URL needs to change.

**Suggestion**:
```
Define the default URL as a class-level constant:
```python
DEFAULT_BASE_URL = "https://web-api.tp.entsoe.eu/api"

@dataclass(frozen=True)
class EntsoeClient:
    base_url: str = DEFAULT_BASE_URL
    ...
    @classmethod
    def from_env(cls, ..., base_url: str = DEFAULT_BASE_URL, ...):
```
```

---

### [INFO] Unbounded response size from API

**File**: `src/quant_alpha/ingestion/entsoe.py:43`
**Category**: Potential Bug
**Confidence**: 60%

response.read() reads the entire response into memory without size limits. A malformed or extremely large response from the API could cause memory exhaustion.

**Suggestion**:
```
Consider adding a size limit:
```python
MAX_RESPONSE_SIZE = 100 * 1024 * 1024  # 100MB
payload = response.read(MAX_RESPONSE_SIZE + 1)
if len(payload) > MAX_RESPONSE_SIZE:
    raise EntsoeError('Response too large')
```
```

---

### [INFO] Magic number 10.0 in imbalance calculation

**File**: `src/quant_alpha/ingestion/entsoe.py:196-200`
**Category**: Potential Bug
**Confidence**: 85%

The scarcity factor multiplied by 10.0 on line 200 is a hardcoded magic number with no explanation of its derivation or units. This appears to be a synthetic field rather than real data from the API.

**Suggestion**:
```
Extract to a named constant and document its purpose:
```python
SCARCITY_PREMIUM_FACTOR = 10.0  # EUR/MW scaling factor for synthetic imbalance price
market_frame["imbalance_price"] = market_frame["spot_price"] + scarcity * SCARCITY_PREMIUM_FACTOR
```
```

---

### [INFO] Missing module and function docstrings

**File**: `src/quant_alpha/ingestion/yahoo.py:1-102`
**Category**: Convention
**Confidence**: 95%

The module and all public functions (generate_synthetic_prices, fetch_prices) lack docstrings. This makes the code harder to understand and maintain.

**Suggestion**:
```
Add docstrings:
```python
"""Yahoo Finance price data ingestion with offline synthetic fallback."""

def fetch_prices(cfg: ProjectConfig, universe: Universe, offline: bool = False) -> pd.DataFrame:
    """Fetch OHLCV price data for the configured universe.
    
    Args:
        cfg: Project configuration with date range and interval.
        universe: Ticker universe to fetch.
        offline: If True, generate synthetic data instead of fetching.
    
    Returns:
        DataFrame with columns defined in PRICE_COLUMNS.
    """
```
```

---

### [INFO] Potential low/negative price values in synthetic data

**File**: `src/quant_alpha/ingestion/yahoo.py:34-37`
**Category**: Potential Bug
**Confidence**: 60%

The random walk can theoretically produce very large negative shocks that drive close prices extremely low or cause high < low violations due to independent random multipliers for high and low.

**Suggestion**:
```
Add clamping to ensure high >= max(open, close) and low <= min(open, close) with valid positive values, or add assertion checks.
```

---

### [INFO] Redundant rename_map entry

**File**: `src/quant_alpha/ingestion/yahoo.py:58-71`
**Category**: Readability
**Confidence**: 95%

The rename_map includes `"adj_close": "adj_close"` which is a no-op identity mapping. This is confusing and suggests a possible copy-paste error or incomplete implementation.

**Suggestion**:
```
Remove the redundant entry:
```python
rename_map = {"datetime": "date"}
```
```

---

### [INFO] Consider using __all__ for public API

**File**: `src/quant_alpha/pipeline.py:15`
**Category**: Code Style
**Confidence**: 65%

This module defines both a private helper (_write_parquet) and a public function (run_pipeline) but does not define __all__ to clarify the module's public API.

**Suggestion**:
```
Add `__all__ = ['run_pipeline']` at module level to clearly document the public interface.
```

---

### [INFO] Duplicate data written to Parquet and DuckDB

**File**: `src/quant_alpha/pipeline.py:20-82`
**Category**: Performance
**Confidence**: 60%

The pipeline writes the same DataFrames to both Parquet files and DuckDB tables, effectively doubling storage and write time. This may be intentional for the architecture but is worth noting as a performance consideration for large datasets.

**Suggestion**:
```
If both storage backends are always needed, consider documenting this design decision. If one is optional, add configuration to skip either. For large datasets, consider writing to DuckDB only and providing an export function for Parquet.
```

---

### [INFO] Magic number 20.0 in denominator clip

**File**: `src/quant_alpha/pipeline_energy.py:71`
**Category**: Readability
**Confidence**: 75%

The clip lower bound of 20.0 for the denominator is a magic number without explanation of why 20.0 was chosen or what it represents in the domain.

**Suggestion**:
```
Extract to a named constant with a comment:
```python
# Minimum absolute price threshold to avoid extreme returns from near-zero prices.
MIN_PRICE_THRESHOLD = 20.0  # EUR/MWh
denominator = features["spot_price"].abs().clip(lower=MIN_PRICE_THRESHOLD)
```
```

---

### [INFO] Magic numbers -0.8 and 0.8 in return clipping

**File**: `src/quant_alpha/pipeline_energy.py:72`
**Category**: Readability
**Confidence**: 70%

The clip bounds -0.8 and 0.8 for forward_return are unexplained magic numbers.

**Suggestion**:
```
Extract to named constants:
```python
MAX_RETURN_MAGNITUDE = 0.8  # ±80% max single-period return
features["forward_return"] = (...).clip(-MAX_RETURN_MAGNITUDE, MAX_RETURN_MAGNITUDE)
```
```

---

### [INFO] Loop over alpha_cols for rank computation

**File**: `src/quant_alpha/pipeline_energy.py:75`
**Category**: Performance
**Confidence**: 70%

The loop `for col in alpha_cols` computes groupby rank one column at a time. This could be vectorized using groupby().rank() on multiple columns at once.

**Suggestion**:
```
Vectorize the rank computation:
```python
rank_cols = [f"{col}_rank" for col in alpha_cols]
features[rank_cols] = features.groupby("timestamp")[alpha_cols].rank(pct=True)
```
```

---

### [INFO] Undocumented date type conversion for turnover

**File**: `src/quant_alpha/pipeline_energy.py:96`
**Category**: Readability
**Confidence**: 60%

Line 96 converts 'date' column to string with `.astype(str)` before passing to alpha_turnover, but no comment explains why this is necessary for turnover but not for other diagnostics.

**Suggestion**:
```
Add a comment explaining the conversion:
```python
# Convert date to string for turnover calculation compatibility
energy_turnover_panel["date"] = energy_turnover_panel["date"].astype(str)
```
```

---

### [INFO] Missing module-level docstring format

**File**: `src/quant_alpha/platform/__init__.py:1`
**Category**: Convention
**Confidence**: 60%

The docstring is present but minimal. For a module labeled 'Platform contracts and quality checks', consider expanding to describe what contracts and checks are provided, following PEP 257 conventions for module docstrings.

**Suggestion**:
```
Expand the docstring to be more descriptive:
"""Platform contracts and quality checks.

This module defines interfaces and validation logic for platform
integrations, including data quality assertions and contract testing.
"""
```

---

### [INFO] Missing module-level docstring with metadata

**File**: `src/quant_alpha/platform/bruin_graph.py:1`
**Category**: Convention
**Confidence**: 60%

The module has a one-line docstring but lacks standard metadata like author, version, license, or creation date that would be expected for a production module.

**Suggestion**:
```
Add comprehensive module docstring with author, version, and purpose.
```

---

### [INFO] Missing logging module usage

**File**: `src/quant_alpha/platform/bruin_graph.py:1`
**Category**: Convention
**Confidence**: 80%

The module uses print() for all output instead of the logging module. This makes it difficult to control output levels, redirect to files, or integrate with logging frameworks.

**Suggestion**:
```
Replace print() calls with logging.info(), logging.warning(), etc. using a configured logger.
```

---

### [INFO] AssetNode docstring missing

**File**: `src/quant_alpha/platform/bruin_graph.py:41`
**Category**: Convention
**Confidence**: 70%

The AssetNode dataclass has no docstring explaining its purpose or the meaning of its fields.

**Suggestion**:
```
Add a docstring explaining the dataclass purpose and important field semantics.
```

---

### [INFO] Magic number 99 for default depth

**File**: `src/quant_alpha/platform/bruin_graph.py:61`
**Category**: Readability
**Confidence**: 70%

The default depth=99 in upstream() is a magic number with no explanation. It's unclear why 99 was chosen or if it's sufficient for all use cases.

**Suggestion**:
```
Define a constant: `MAX_UPSTREAM_DEPTH = 99` and use it as the default, or consider using None to mean unlimited depth.
```

---

### [INFO] Silent import inside method

**File**: `src/quant_alpha/platform/bruin_graph.py:158`
**Category**: Readability
**Confidence**: 80%

The `import os` statement is inside the run() method rather than at the top of the module. This is unusual and may confuse developers about the module's dependencies.

**Suggestion**:
```
Move `import os` to the top of the file with the other imports.
```

---

### [INFO] upstream() KeyError on missing targets

**File**: `src/quant_alpha/platform/bruin_graph.py:168`
**Category**: Potential Bug
**Confidence**: 90%

In the run() method, if a target name doesn't exist in self.nodes, calling self.upstream(t) will raise a KeyError since upstream() accesses self.nodes[name].depends without checking existence.

**Suggestion**:
```
Add validation: `for t in targets: if t not in self.nodes: raise ValueError(f"Target '{t}' not found in graph")`
```

---

### [INFO] Missing module docstring

**File**: `src/quant_alpha/platform/contracts.py:1-70`
**Category**: Convention
**Confidence**: 90%

The module lacks a docstring explaining its purpose, which is to define data contracts for equity and energy datasets. This makes it harder for developers to understand the module's role in the system at a glance.

**Suggestion**:
```
Add a module-level docstring at the beginning of the file:
```python
"""Data contract definitions for equity and energy datasets.

This module defines the DatasetContract dataclass and pre-defined contracts
for datasets used in quantitative research and alpha development.
"""
```

---

### [INFO] No runtime validation of contract data

**File**: `src/quant_alpha/platform/contracts.py:1-70`
**Category**: Architecture
**Confidence**: 80%

The DatasetContract class uses frozen=True dataclass, which prevents mutation but doesn't validate the content of the fields. For example, owner could be empty string, primary_keys could be empty tuple, or name could be empty.

**Suggestion**:
```
Add __post_init__ validation to ensure data integrity:
```python
@dataclass(frozen=True)
class DatasetContract:
    # ... fields ...
    
    def __post_init__(self):
        if not self.name:
            raise ValueError("Dataset name cannot be empty")
        if not self.primary_keys:
            raise ValueError("Primary keys cannot be empty")
        if not self.owner:
            raise ValueError("Owner cannot be empty")
```
```

---

### [INFO] Missing class docstring for DatasetContract

**File**: `src/quant_alpha/platform/contracts.py:8-13`
**Category**: Convention
**Confidence**: 90%

The DatasetContract dataclass lacks a docstring describing its purpose, fields, and usage. This reduces code readability and makes it harder for other developers to understand how to use this contract class.

**Suggestion**:
```
Add a docstring to the DatasetContract class:
```python
@dataclass(frozen=True)
class DatasetContract:
    """Represents a data contract for a specific dataset.
    
    Attributes:
        name: Unique identifier for the dataset
        grain: Temporal and dimensional granularity
        owner: Team responsible for the dataset
        primary_keys: Tuple of columns forming the primary key
        freshness_expectation: How often data should be updated
    """
    name: str
    grain: str
    owner: str
    primary_keys: tuple[str, ...]
    freshness_expectation: str
```
```

---

### [INFO] Hardcoded dataset definitions without validation

**File**: `src/quant_alpha/platform/contracts.py:16-21`
**Category**: Readability
**Confidence**: 70%

The DatasetContract instances are created with string values that represent grain and freshness expectations (e.g., 'daily', 'hourly'). These values are not validated against an enum or predefined set, which could lead to typos or inconsistencies when used programmatically.

**Suggestion**:
```
Consider using enums for grain and freshness_expectation fields to ensure consistency and enable IDE support:
```python
from enum import Enum

class Grain(Enum):
    DAILY_SYMBOL = "daily x symbol"
    PER_ALPHA = "per alpha"
    DAILY_ALPHA = "daily x alpha"
    HOURLY_MARKET = "hourly x market"

class FreshnessExpectation(Enum):
    DAILY = "daily"
    HOURLY = "hourly"
```
```

---

### [INFO] Missing comments explaining dataset relationships

**File**: `src/quant_alpha/platform/contracts.py:58-63`
**Category**: Readability
**Confidence**: 80%

The EQUITY_DATASETS and ENERGY_DATASETS lists contain related datasets but there are no comments explaining their relationships, dependencies, or the rationale behind the grouping.

**Suggestion**:
```
Add comments explaining the dataset groupings:
```python
# Equity datasets: Core datasets for equity alpha research
EQUITY_DATASETS = [
    # Raw market price data ingested daily
    DatasetContract(...)
    # ... other equity datasets
]
```
```

---

### [INFO] Magic number for duplicate check

**File**: `src/quant_alpha/platform/quality.py:6-9`
**Category**: Readability
**Confidence**: 60%

The comparison 'duplicates == 0' uses an implicit magic number. While clear in context, explicit naming improves readability.

**Suggestion**:
```
Consider using a named constant: MAX_DUPLICATES = 0, then 'passed': duplicates <= MAX_DUPLICATES
```

---

### [INFO] Consider using a dataclass for results

**File**: `src/quant_alpha/platform/quality.py:6-9`
**Category**: Code Style
**Confidence**: 60%

The function returns a plain dictionary. Using a dataclass would provide better structure and type safety.

**Suggestion**:
```
Define a result dataclass:
```python
from dataclasses import dataclass

@dataclass
class QualityCheckResult:
    check: str
    keys: str
    passed: bool
    duplicates: int
```
```

---

### [INFO] Hardcoded column names

**File**: `src/quant_alpha/platform/quality.py:23-32`
**Category**: Readability
**Confidence**: 70%

The column names for null checks are hardcoded strings scattered in the code. If column names change, multiple places need updating.

**Suggestion**:
```
Consider defining required columns as module-level constants:
```python
ENERGY_REQUIRED_COLUMNS = ["timestamp", "market", "spot_price", "load_forecast", "residual_load"]
ENERGY_KEY_COLUMNS = ["timestamp", "market"]
```
```

---

### [INFO] Consider adding more quality checks

**File**: `src/quant_alpha/platform/quality.py:23-32`
**Category**: Readability
**Confidence**: 80%

The function only checks primary key and null values. For energy market data, additional checks might be valuable (e.g., price ranges, timestamp validity, data freshness).

**Suggestion**:
```
Consider extending with additional checks:
- Price range validation (spot_price > 0)
- Timestamp ordering and completeness
- Data freshness check
- Outlier detection
```

---

### [INFO] Missing module-level docstring

**File**: `src/quant_alpha/storage/duckdb.py:1-6`
**Category**: Convention
**Confidence**: 80%

The module lacks a docstring explaining its purpose. This is a standard Python convention (PEP 257) that helps with documentation generation and code understanding.

**Suggestion**:
```
Add a module-level docstring:
```python
"""DuckDB storage utilities for persisting DataFrames and metrics."""
from __future__ import annotations
```
```

---

### [INFO] mkdir call could fail with permission errors

**File**: `src/quant_alpha/storage/duckdb.py:11`
**Category**: Readability
**Confidence**: 60%

The db_path.parent.mkdir(parents=True, exist_ok=True) call on line 11 has no error handling. If the parent directory cannot be created (e.g., due to permission issues), an unhandled OSError will be raised. While this might be acceptable behavior, it's worth noting for defensive programming.

**Suggestion**:
```
Consider adding explicit error handling or documenting the expected exception behavior:
```python
def write_table(db_path: Path, table_name: str, frame: pd.DataFrame) -> None:
    """Write DataFrame to DuckDB. Raises OSError if directory cannot be created."""
    db_path.parent.mkdir(parents=True, exist_ok=True)
```
```

---

### [INFO] Empty DataFrame creates table with no schema

**File**: `src/quant_alpha/storage/duckdb.py:13`
**Category**: Potential Bug
**Confidence**: 75%

When write_metrics is called with an empty dict, it creates an empty DataFrame with no columns (line 21). This is passed to write_table which will attempt to create a table from an empty, schema-less DataFrame, potentially resulting in an empty table with no columns or an error depending on the DuckDB version.

**Suggestion**:
```
Consider handling the empty case explicitly:
```python
def write_table(db_path: Path, table_name: str, frame: pd.DataFrame) -> None:
    if frame.empty and len(frame.columns) == 0:
        raise ValueError(f"Cannot write empty DataFrame with no columns as table '{table_name}'")
```
```

---

### [INFO] Case sensitivity in table name lookup

**File**: `src/quant_alpha/storage/duckdb.py:29`
**Category**: Potential Bug
**Confidence**: 85%

DuckDB normalizes unquoted identifiers to lowercase. The information_schema query uses a case-sensitive comparison with `where table_name = ?`. If table_name contains uppercase characters (e.g., 'BacktestMetrics'), it will not match the lowercased version stored by DuckDB, causing false negatives.

**Suggestion**:
```
Normalize the table_name to lowercase in the query:
```python
result = con.execute(
    "select count(*) from information_schema.tables where lower(table_name) = lower(?)",
    [table_name],
).fetchone()[0]
```
Or normalize it at function entry: `table_name = table_name.lower()`
```

---

### [INFO] Missing module docstring

**File**: `src/quant_alpha/storage/gcp.py:1-12`
**Category**: Convention
**Confidence**: 90%

The module lacks a docstring explaining its purpose and usage.

**Suggestion**:
```
Add a module docstring:
"""Google Cloud Storage and BigQuery export utilities."""
```

---

### [INFO] Missing docstring for CloudExportError

**File**: `src/quant_alpha/storage/gcp.py:11`
**Category**: Convention
**Confidence**: 85%

The custom exception class lacks a docstring explaining when it is raised.

**Suggestion**:
```
Add a docstring:
"""Exception raised for cloud export configuration or runtime errors."""
```

---

### [INFO] No parallel upload for multiple tables

**File**: `src/quant_alpha/storage/gcp.py:43`
**Category**: Performance
**Confidence**: 60%

The function processes tables sequentially in a loop. For large datasets with many tables, this could be slow as each upload and load job waits for completion before starting the next.

**Suggestion**:
```
Consider using concurrent futures or asyncio for parallel uploads if performance is a concern:
```python
from concurrent.futures import ThreadPoolExecutor
with ThreadPoolExecutor() as executor:
    futures = {executor.submit(_upload_table, ...): name for name, frame in frames.items()}
```
```

---

### [INFO] Consider logging for observability

**File**: `src/quant_alpha/storage/gcp.py:46`
**Category**: Readability
**Confidence**: 85%

The function performs cloud operations but has no logging, making it difficult to debug issues in production.

**Suggestion**:
```
Add logging statements:
```python
import logging
logger = logging.getLogger(__name__)

# In the loop:
logger.info("Uploading %s to GCS: %s", table_name, blob_name)
logger.info("Loading %s into BigQuery: %s", table_name, table_id)
```
```

---

### [INFO] No type hint for generate_synthetic_power_market return

**File**: `src/quant_alpha/streaming/demo_signals.py:24`
**Category**: Readability
**Confidence**: 60%

The `raw` variable's type depends on the return type of `generate_synthetic_power_market`, which is not visible here. This makes it harder to reason about what `features` will contain.

**Suggestion**:
```
Add a comment or type annotation clarifying the expected type:
```python
raw: pd.DataFrame = generate_synthetic_power_market(markets, start.isoformat(), end.isoformat(), freq="h")
```
```

---

### [INFO] Silent column dropping may hide data issues

**File**: `src/quant_alpha/streaming/demo_signals.py:26-28`
**Category**: Potential Bug
**Confidence**: 75%

The `keep` list filters to only columns that exist in the features DataFrame (`if c in features.columns`). While defensive, this silently drops missing columns. If an expected column like `spot_price` or `timestamp` is missing due to an upstream change, no error is raised and the output may be silently incomplete.

**Suggestion**:
```
Consider logging a warning for missing expected columns:
```python
missing = [c for c in keep if c not in features.columns]
if missing:
    import logging
    logging.warning(f"Expected columns missing from features: {missing}")
```
```

---

### [INFO] No error handling in __main__ block

**File**: `src/quant_alpha/streaming/demo_signals.py:45-46`
**Category**: Potential Bug
**Confidence**: 70%

The `if __name__ == '__main__'` block calls `seed_demo_signals` without any error handling. A failure will print an unformatted traceback to stderr, which is not ideal for a demo script.

**Suggestion**:
```
Add basic error handling:
```python
if __name__ == "__main__":
    try:
        root = Path(__file__).resolve().parents[3]
        db = root / "data/warehouse/second_foundation.duckdb"
        n = seed_demo_signals(db)
        print(f"Seeded {n} rows into live_energy_signals")
    except Exception as e:
        print(f"Error seeding demo signals: {e}")
        raise SystemExit(1)
```
```

---

### [INFO] Missing module-level docstring

**File**: `src/quant_alpha/streaming/redpanda_consumer.py:1`
**Category**: Convention
**Confidence**: 90%

The module lacks a docstring explaining its purpose, usage, and configuration requirements. This makes it harder for other developers to understand the module's role in the system.

**Suggestion**:
```
Add a module docstring:
```python
"""Redpanda consumer for energy signal streaming.

This module provides functions to consume Avro-encoded messages from
Redpanda (Kafka-compatible) and store them in DuckDB for analytics.
"""
```
```

---

### [INFO] Missing docstring for _load_schema function

**File**: `src/quant_alpha/streaming/redpanda_consumer.py:61`
**Category**: Readability
**Confidence**: 90%

The `_load_schema` helper function lacks a docstring explaining its purpose and parameters.

**Suggestion**:
```
Add a docstring:
```python
def _load_schema(path: Path) -> dict:
    """Load and parse an Avro schema from a JSON file.
    
    Args:
        path: Path to the .avsc schema file.
    
    Returns:
        Parsed Avro schema object.
    
    Raises:
        FileNotFoundError: If schema file doesn't exist.
        json.JSONDecodeError: If file contains invalid JSON.
    """
```
```

---

### [INFO] Redundant str() conversion of Path object

**File**: `src/quant_alpha/streaming/redpanda_consumer.py:66`
**Category**: Performance
**Confidence**: 75%

Line 66 calls `str(duckdb_path)` but DuckDB's `connect` method can accept Path objects directly. The explicit conversion is unnecessary.

**Suggestion**:
```
Remove the str() call:
```python
with duckdb.connect(duckdb_path) as con:
```
```

---

### [INFO] Function lacks docstring

**File**: `src/quant_alpha/streaming/redpanda_producer.py:11-13`
**Category**: Code Style
**Confidence**: 70%

The _load_schema function is missing a docstring explaining what it does and returns.

**Suggestion**:
```
Add a docstring:
```python
def _load_schema(path: Path) -> dict:
    """Load and parse Avro schema from file."""
```
```

---

### [INFO] Import inside function body

**File**: `src/quant_alpha/streaming/redpanda_producer.py:11-13`
**Category**: Performance
**Confidence**: 60%

The fastavro import is inside the _load_schema function. While this might be intentional to delay import, it's generally better to have imports at the top of the file for clarity and to avoid repeated import overhead if the function is called multiple times.

**Suggestion**:
```
Move import to top of file if fastavro is a core dependency:
```python
import json
from pathlib import Path
import pandas as pd
from fastavro import parse_schema
from quant_alpha.ingestion.energy import generate_synthetic_power_market
```
```

---

### [INFO] Function lacks docstring

**File**: `src/quant_alpha/streaming/redpanda_producer.py:19-25`
**Category**: Code Style
**Confidence**: 70%

The _serialize function is missing a docstring explaining what it does and returns.

**Suggestion**:
```
Add a docstring:
```python
def _serialize(schema: dict, payload: dict) -> bytes:
    """Serialize payload using Avro schema."""
```
```

---

### [INFO] Import inside function body

**File**: `src/quant_alpha/streaming/redpanda_producer.py:19-25`
**Category**: Performance
**Confidence**: 60%

The fastavro and io imports are inside the _serialize function. While this might be intentional to delay import, it's generally better to have imports at the top of the file for clarity.

**Suggestion**:
```
Move imports to top of file if fastavro is a core dependency:
```python
import json
import io
from pathlib import Path
import pandas as pd
from fastavro import parse_schema, schemaless_writer
from quant_alpha.ingestion.energy import generate_synthetic_power_market
```
```

---

### [INFO] Import inside function body

**File**: `src/quant_alpha/streaming/redpanda_producer.py:30-44`
**Category**: Performance
**Confidence**: 60%

The confluent_kafka import is inside the publish_energy_signals function. This adds import overhead every time the function is called.

**Suggestion**:
```
Move import to top of file or use lazy loading pattern:
```python
# At top of file
def _get_kafka_producer(bootstrap_servers: str):
    from confluent_kafka import Producer
    return Producer({"bootstrap.servers": bootstrap_servers})
```
```

---

### [INFO] Module docstring missing version and author info

**File**: `src/quant_alpha/streaming/risingwave/__init__.py:1`
**Category**: Convention
**Confidence**: 60%

The module docstring only describes the module purpose but lacks standard metadata like version, author, and module-level variables that are common in well-documented Python packages.

**Suggestion**:
```
Consider adding standard module metadata:
"""
RisingWave streaming SQL — materialized views over live energy signals.

Module: quant_alpha.streaming.risingwave
Version: 1.0.0
Author: [Author Name]
Description: Provides streaming SQL capabilities for real-time energy signal processing using RisingWave.
"""
```

---

### [INFO] Empty __init__.py may indicate missing public API

**File**: `src/quant_alpha/streaming/risingwave/__init__.py:1`
**Category**: Architecture
**Confidence**: 70%

The __init__.py file only contains a docstring with no actual module exports or imports. This suggests the package may be missing a clear public API definition, making it unclear what functionality is available to users of this package.

**Suggestion**:
```
Consider defining the package's public API by importing and exposing key classes/functions, or add a comment explaining that this is an empty package for future use:
"""...
"""

# Public API
# from .module import SomeClass
# __all__ = ['SomeClass']

# Note: This package is currently empty and will be populated with RisingWave streaming functionality in future releases.
```

---

### [INFO] Unused datetime import (partially)

**File**: `src/quant_alpha/streaming/risingwave/producer.py:18`
**Category**: Code Style
**Confidence**: 60%

The `datetime` class is imported but only `datetime.now(timezone.utc).isoformat()` is used. While not unused, `timezone` is correctly imported alongside it. This is minor but worth noting for consistency.

---

### [INFO] Delivery errors only logged to stdout

**File**: `src/quant_alpha/streaming/risingwave/producer.py:35-37`
**Category**: Code Style
**Confidence**: 85%

The delivery callback only prints errors to stdout. In production, these should be logged via Python's logging module for proper log level control and log aggregation.

**Suggestion**:
```
Use logging:
```python
import logging
logger = logging.getLogger(__name__)

def _delivery_report(err, msg):
    if err:
        logger.error(f'[{__name__}] delivery failed: {err}')
```
```

---

### [INFO] Complex dict comprehension is hard to read

**File**: `src/quant_alpha/streaming/risingwave/producer.py:46-52`
**Category**: Readability
**Confidence**: 80%

The dictionary comprehension on lines 46-52 uses nested ternary expressions inline, making it difficult to quickly understand the logic for type conversion.

**Suggestion**:
```
Extract to a helper function for clarity:
```python
def _convert_value(v):
    if isinstance(v, pd.Timestamp):
        return v.isoformat()
    elif pd.notna(v):
        return float(v)
    return None

payload = {k: _convert_value(v) for k, v in row.items()}
```
```

---

### [INFO] Missing module-level type hints and public API annotations

**File**: `src/quant_alpha/streaming/risingwave/simulator.py:1-91`
**Category**: Convention
**Confidence**: 60%

While the function signatures have return type hints, the module lacks __all__ to define its public API. This is a minor convention issue for a module that is intended to be used by other packages.

**Suggestion**:
```
Add at module level:
```python
__all__ = ["build_realtime_alpha_panel", "get_scarcity_alerts"]
```
```

---

### [INFO] Deprecation warning for utcnow usage

**File**: `src/quant_alpha/streaming/risingwave/simulator.py:27`
**Category**: Potential Bug
**Confidence**: 85%

pd.Timestamp.utcnow() is deprecated in newer pandas versions (>= 2.0). The recommended replacement is pd.Timestamp.now(tz='UTC'). Using deprecated APIs may cause warnings or breakage in future pandas releases.

**Suggestion**:
```
Replace with:
```python
end = pd.Timestamp.now(tz="UTC").floor("h")
```
```

---

### [INFO] Floor to hour may produce no data if hours=0

**File**: `src/quant_alpha/streaming/risingwave/simulator.py:27-28`
**Category**: Potential Bug
**Confidence**: 70%

If hours=0 is passed, start == end and the time range has zero duration. Depending on how generate_synthetic_power_market handles this, it may return an empty dataset, leading to unexpected empty results downstream.

**Suggestion**:
```
Add validation:
```python
if hours <= 0:
    raise ValueError(f"hours must be positive, got {hours}")
```
```

---

### [INFO] Hardcoded magic number 35.0 in SQL

**File**: `src/quant_alpha/streaming/risingwave/simulator.py:33`
**Category**: Potential Bug
**Confidence**: 70%

The value 35.0 is used as a fallback for gas_price in the SQL query (COALESCE(s.gas_price, 35.0)). This hardcoded magic number is a default assumption about gas prices that may become stale or incorrect in different market conditions or geographies. It should be documented or parameterized.

**Suggestion**:
```
Extract to a named parameter or constant:
```python
def build_realtime_alpha_panel(
    markets: list[str],
    hours: int = 48,
    db_path: str = ":memory:",
    default_gas_price: float = 35.0,
) -> pd.DataFrame:
```
Then use it in the query via string formatting or a DuckDB parameter.
```

---

### [INFO] No handling of empty markets list

**File**: `src/quant_alpha/streaming/risingwave/simulator.py:38-39`
**Category**: Potential Bug
**Confidence**: 75%

If an empty list is passed as 'markets', generate_synthetic_power_market may return an empty DataFrame, which could cause downstream SQL operations or feature computation to behave unexpectedly (division by zero in SQL, empty join results, etc.).

**Suggestion**:
```
Add early validation:
```python
if not markets:
    raise ValueError("markets list must not be empty")
```
```

---

### [INFO] Derived columns lack documentation

**File**: `src/quant_alpha/streaming/risingwave/views.sql:70-77`
**Category**: Readability
**Confidence**: 80%

Several derived columns in mv_energy_momentum_6h (gas_spark_spread, demand_surprise, solar_penetration, imbalance_premium) are business-domain calculations but have no inline comments explaining their economic meaning or expected units/scales.

**Suggestion**:
```
Add inline comments explaining each derived column, e.g.: -- gas_spark_spread: difference between spot electricity price and gas-equivalent price (EUR/MWh)
```

---

### [INFO] Unnecessary __future__ import

**File**: `tests/test_alpha_factors.py:1`
**Category**: Code Style
**Confidence**: 70%

The `from __future__ import annotations` import is unnecessary in Python 3.10+ and this is a test file where forward reference string annotations aren't needed.

**Suggestion**:
```
Remove the import if targeting Python 3.10+, or keep it only if maintaining Python 3.7-3.9 compatibility.
```

---

### [INFO] Repeated synthetic data generation across tests

**File**: `tests/test_alpha_factors.py:11-12`
**Category**: Performance
**Confidence**: 80%

Each test function independently calls `generate_synthetic_prices` and `add_alpha_factors`. This duplicates expensive computation across tests, especially with larger date ranges (2021-01-01 to 2022-12-31).

**Suggestion**:
```
Consider using pytest fixtures with appropriate scope to share generated test data:
```python
@pytest.fixture(scope="module")
def test_panel():
    cfg = ProjectConfig(start_date="2021-01-01", end_date="2021-06-30")
    universe = Universe(name="test", symbols=["AAA", "BBB", "CCC", "DDD", "EEE"])
    prices = generate_synthetic_prices(cfg, universe)
    return add_alpha_factors(prices, cfg)
```
```

---

### [INFO] Unused import statement

**File**: `tests/test_bruin_graph.py:1-2`
**Category**: Code Style
**Confidence**: 70%

The `from __future__ import annotations` import is present but not needed for this code. The annotations are basic types (None, int, str, set) that don't require deferred evaluation. This is a style preference but adds unnecessary complexity.

**Suggestion**:
```
Remove the `from __future__ import annotations` import unless there's a specific need for forward references or string annotations.
```

---

### [INFO] Import inside test function

**File**: `tests/test_bruin_graph.py:71-75`
**Category**: Code Style
**Confidence**: 60%

The import `from quant_alpha.platform.contracts import ALL_DATASETS` is inside the test function rather than at the module level. While this can be useful to avoid import errors when the module isn't available, it's inconsistent with the other imports at the top of the file.

**Suggestion**:
```
Move the import to the top of the file for consistency:
```python
from quant_alpha.platform.contracts import ALL_DATASETS
```
Or add a comment explaining why it's imported locally.
```

---

### [INFO] Unnecessary future import

**File**: `tests/test_cloud_export.py:1`
**Category**: Code Style
**Confidence**: 60%

The `from __future__ import annotations` import is unnecessary as the code uses Python 3.9+ syntax (dict[str, pd.DataFrame]) or doesn't use any features requiring this import in the test file itself.

**Suggestion**:
```
Consider removing `from __future__ import annotations` unless it's required by the project's style guide or for consistency with other files.
```

---

### [INFO] Magic tuple unpacking without validation

**File**: `tests/test_diagnostics.py:16`
**Category**: Readability
**Confidence**: 65%

evaluate_alpha_suite() returns a 3-tuple (diagnostics, metrics, backtests) but there's no assertion that backtests is a DataFrame (assumed from .empty call). A TypeError would occur if the return type changes.

**Suggestion**:
```
Add type validation: `assert isinstance(backtests, pd.DataFrame)` before checking `.empty`.
```

---

### [INFO] Test correlation assertion unclear

**File**: `tests/test_diagnostics.py:21`
**Category**: Readability
**Confidence**: 60%

Asserting len(corr) == len(BASE_FACTOR_COLUMNS) ** 2 assumes correlation is returned as a flattened Series/list. This assumption should be documented or the assertion made more explicit about the expected shape (e.g., DataFrame shape assertion).

**Suggestion**:
```
Consider using: `assert corr.shape == (len(BASE_FACTOR_COLUMNS), len(BASE_FACTOR_COLUMNS))` if it returns a DataFrame, or add a comment explaining the expected format.
```

---

### [INFO] Magic number 300 without explanation

**File**: `tests/test_dlt_pipelines.py:27-28`
**Category**: Readability
**Confidence**: 70%

The assertion `n > 300` uses a magic number. While there is a comment explaining the expected count, the specific threshold is not clearly derived from the expected calculation (169 hours × 2 markets = 338 rows).

**Suggestion**:
```
Consider using a named constant or a more precise assertion based on the expected calculation:
```python
expected_min_rows = 169 * 2  # 169 hours × 2 markets
assert n >= expected_min_rows, f"Expected >= {expected_min_rows} rows, got {n}"
```
```

---

### [INFO] Missing docstring for test function

**File**: `tests/test_dlt_pipelines.py:44`
**Category**: Convention
**Confidence**: 60%

The function test_dlt_equity_pipeline_creates_table lacks a docstring explaining what it tests.

**Suggestion**:
```
Add a docstring:
```python
def test_dlt_equity_pipeline_creates_table() -> None:
    """Test that equity pipeline creates table with data."""
```
```

---

### [INFO] Another magic number 300 without explanation

**File**: `tests/test_dlt_pipelines.py:55-56`
**Category**: Readability
**Confidence**: 70%

The assertion `n > 300` in test_dlt_energy_incremental_loads_new_dates also uses a magic number. The comment mentions 337 hours but the threshold is 300.

**Suggestion**:
```
Use a named constant or a more precise assertion:
```python
expected_min_rows = 337  # 14 days × 24 hours + 1 hour
assert n >= expected_min_rows, f"Expected >= {expected_min_rows} rows after extension, got {n}"
```
```

---

### [INFO] Missing docstring for test function

**File**: `tests/test_dlt_pipelines.py:59`
**Category**: Convention
**Confidence**: 60%

The function test_dlt_equity_schema_has_required_columns lacks a docstring explaining what it tests.

**Suggestion**:
```
Add a docstring:
```python
def test_dlt_equity_schema_has_required_columns() -> None:
    """Test that equity table schema contains required columns."""
```
```

---

### [INFO] Missing trailing newline at end of file

**File**: `tests/test_energy_alpha.py:57`
**Category**: Code Style
**Confidence**: 70%

The file appears to end without a trailing newline character, which is a common style guideline (PEP 8 recommends ending files with a newline).

**Suggestion**:
```
Add a blank line at the end of the file after line 57.
```

---

### [INFO] Test function missing docstring

**File**: `tests/test_entsoe.py:39`
**Category**: Convention
**Confidence**: 60%

The test function `test_parse_entsoe_timeseries_handles_namespaced_price_points` lacks a docstring explaining what scenario is being tested. While test names should be descriptive, a brief docstring helps document the expected behavior being verified.

**Suggestion**:
```
Add a docstring:
```python
def test_parse_entsoe_timeseries_handles_namespaced_price_points() -> None:
    """Verify parser extracts price points from namespaced XML elements."""
```
```

---

### [INFO] Test function missing docstring

**File**: `tests/test_entsoe.py:47`
**Category**: Convention
**Confidence**: 60%

The test function `test_period_params_are_utc_entsoe_format` lacks a docstring. Adding one would clarify that this tests the internal helper's date formatting behavior.

**Suggestion**:
```
Add a docstring:
```python
def test_period_params_are_utc_entsoe_format() -> None:
    """Verify _period_params returns dates in ENTSO-E UTC format (YYYYMMDDHHMM)."""
```
```

---

### [INFO] Testing private/internal function _period_params

**File**: `tests/test_entsoe.py:47`
**Category**: Code Style
**Confidence**: 65%

The test imports and directly tests `_period_params`, which has a leading underscore indicating it's a private/internal function. Testing private functions tightly couples tests to implementation details, making refactoring harder.

**Suggestion**:
```
Consider testing `_period_params` indirectly through its public API (i.e., through the function that calls it). If direct testing is necessary for thorough coverage, document why:
```python
# Direct test of internal helper to verify ENTSO-E date formatting
def test_period_params_are_utc_entsoe_format() -> None:
```
```

---

### [INFO] Test function missing docstring

**File**: `tests/test_entsoe.py:53-56`
**Category**: Convention
**Confidence**: 60%

The test function `test_parser_rejects_unsupported_resolution` lacks a docstring explaining the expected failure scenario.

**Suggestion**:
```
Add a docstring:
```python
def test_parser_rejects_unsupported_resolution() -> None:
    """Verify parser raises EntsoeError for unsupported time resolutions."""
```
```

---

### [INFO] Byte replacement may be fragile

**File**: `tests/test_entsoe.py:53-56`
**Category**: Readability
**Confidence**: 70%

The test uses `SAMPLE_PRICE_XML.replace(...)` to create a modified XML with an unsupported resolution. This approach is fragile — if the XML structure changes or whitespace differs, the replacement may silently fail and the test could pass for the wrong reason.

**Suggestion**:
```
Consider verifying the replacement succeeded, or use a dedicated fixture:
```python
def test_parser_rejects_unsupported_resolution() -> None:
    """Verify parser raises EntsoeError for unsupported time resolutions."""
    xml = SAMPLE_PRICE_XML.replace(b"<resolution>PT60M</resolution>", b"<resolution>P1Y</resolution>")
    assert b"P1Y" in xml, "Test fixture modification failed"
    with pytest.raises(EntsoeError):
        parse_entsoe_timeseries(xml, ("price.amount",))
```
```

---

### [INFO] Missing test docstring

**File**: `tests/test_quality.py:7`
**Category**: Convention
**Confidence**: 70%

The test function lacks a docstring explaining what behavior is being verified. Good test documentation helps future developers understand test intent.

**Suggestion**:
```
Add a docstring:
```python
def test_energy_quality_checks_pass_for_synthetic_data() -> None:
    """Verify that synthetic power market data passes all quality checks."""
    frame = generate_synthetic_power_market(["DE_LU", "CZ"], "2024-01-01", "2024-01-03")
    ...
```
```

---

### [INFO] Test logic could be more explicit

**File**: `tests/test_risingwave_simulator.py:37-41`
**Category**: Readability
**Confidence**: 65%

The test iterates over columns starting with 'alpha_' but the required columns are already defined in test_realtime_alpha_panel_columns(). This test dynamically discovers alpha columns which could mask issues if column naming changes.

**Suggestion**:
```
Consider using an explicit list of expected alpha columns to make the test more predictable:
```python
alpha_cols = [
    "alpha_residual_load_rank", "alpha_imbalance_premium",
    "alpha_cross_market_spread", "alpha_demand_surprise",
    "alpha_solar_penetration", "alpha_momentum_6h", "alpha_gas_spark_spread"
]
```
```

---

### [INFO] Missing docstring for test function

**File**: `tests/test_risingwave_simulator.py:63-66`
**Category**: Convention
**Confidence**: 70%

test_scarcity_level_values() lacks a docstring explaining what scarcity levels are being validated and why only HIGH and MEDIUM are valid.

**Suggestion**:
```
Add a docstring:
```python
def test_scarcity_level_values() -> None:
    """Verify scarcity alerts only contain valid scarcity levels (HIGH, MEDIUM)."""
```
```

---

### [INFO] SQL comment handling unclear in test

**File**: `tests/test_risingwave_simulator.py:80-88`
**Category**: Readability
**Confidence**: 65%

The test SQL contains a comment (`-- comment`) but doesn't verify how the _split_statements function handles comments. If comments are stripped, the test still passes but doesn't validate comment preservation.

**Suggestion**:
```
Add an assertion to document expected comment handling:
```python
# Verify comments are handled (either preserved or stripped)
assert len(stmts) == 2  # Comments should be stripped
# Or if comments should be preserved:
# assert len(stmts) == 3  # Including comment as statement
```
```

---

