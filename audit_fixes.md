# Audit Fixes — Quant Alpha Foundation

Based on `audit_report_v1.md`. All fixes were applied across two sessions. Each entry explains the original defect, the root cause, and the observed/expected effect after the fix.

---

## 1. SQL 注入防护 — `storage/duckdb.py`

**原始问题**
`write_table()` 直接将外部传入的 `table_name` 拼接进 f-string SQL：
```python
con.execute(f"create or replace table {table_name} as select * from _frame")
```
攻击者可传入 `foo; DROP TABLE backtest_metrics` 等字符串，执行任意 SQL。

**修复方式**
新增 `_safe_identifier()` 函数，校验名称只含字母、数字、下划线及一个点（schema.table），否则抛 `ValueError`：
```python
def _safe_identifier(name: str) -> str:
    if not name.replace(".", "_").replace("_", "a").isalnum():
        raise ValueError(f"Unsafe SQL identifier: {name!r}")
    return name
```
`write_table()` 在拼接 SQL 前先调用该函数。

**修复效果**
非法标识符立即被拒绝，SQL 注入路径关闭。合法名称（如 `backtest_metrics`、`raw.equity_ohlcv`）不受影响。

---

## 2. SQL 注入防护 — `storage/gcp.py`

**原始问题**
`export_frames_to_gcs_bigquery()` 中 `table_name` 直接用于构造 BigQuery 表 ID 字符串，无任何校验。

**修复方式**
在 export 循环内加入 `_validate_table_name()` 正则校验：
```python
def _validate_table_name(name: str) -> None:
    if not re.match(r'^[a-zA-Z_][a-zA-Z0-9_]*$', name):
        raise CloudExportError(f"Invalid table name for cloud export: {name!r}")
```

**修复效果**
非法表名在上传 GCS/写入 BigQuery 之前即被拦截，不会产生无效或危险的云资源操作。

---

## 3. GCS / BigQuery 错误捕获 — `storage/gcp.py`

**原始问题**
`blob.upload_from_filename()` 和 `load_job.result()` 若抛出异常，堆栈会直接向上透传，调用方无法区分是上传失败还是 BQ 加载失败。

**修复方式**
分别用 `try/except` 包裹两段操作，统一抛出 `CloudExportError` 并附原始异常信息：
```python
try:
    blob.upload_from_filename(str(local_path))
except Exception as exc:
    raise CloudExportError(f"GCS upload failed for {table_name}: {exc}") from exc
```

**修复效果**
错误信息更清晰，调用方可根据 `CloudExportError` 单独处理云导出失败，不影响本地 DuckDB 流程。

---

## 4. Kafka Consumer 资源泄漏 — `streaming/redpanda_consumer.py`

**原始问题**
`consume_energy_signals()` 在抛出异常时不调用 `consumer.close()`，导致 Kafka 消费者连接及 group 协调资源永久泄漏：
```python
consumer = Consumer(...)
# 若下方代码抛异常，close() 永远不被调用
messages.append(...)
return messages
consumer.close()  # 实际从未执行
```

**修复方式**
改为 `try/finally` 结构，无论是否异常都保证调用 `consumer.close()`。

**修复效果**
消费者连接在异常和正常退出两种路径下均被正确释放，避免 Kafka broker 端积压孤立连接。

---

## 5. SQL 注入防护 — `streaming/redpanda_consumer.py`

**原始问题**
`consume_and_store()` 中 `table` 参数直接拼入 DuckDB SQL：
```python
con.execute(f"CREATE TABLE IF NOT EXISTS {table} AS ...")
con.execute(f"INSERT INTO {table} SELECT * FROM frame")
```

**修复方式**
在执行 SQL 前用正则校验表名：
```python
if not re.match(r'^[a-zA-Z_][a-zA-Z0-9_]*$', table):
    raise ValueError(f"Invalid table name: {table!r}")
```

**修复效果**
与 `duckdb.py` 修复等效，封闭 Redpanda 消费写入路径的 SQL 注入风险。

---

## 6. Spark Session 未被 finally 保护 — `batch/spark_energy_features.py`

**原始问题**
`spark.stop()` 在 `compute_energy_features()` 末尾直接调用。若 Parquet 读取、窗口计算或写入中途抛出异常，`spark.stop()` 不会被执行，Spark Driver 进程和所有 Executor 资源持续占用。

**修复方式**
```python
spark = build_spark_session()
try:
    ...
finally:
    spark.stop()
```

**修复效果**
无论执行成功还是失败，Spark 进程都会被正确终止，不留僵尸进程。

---

## 7. `F.log()` 零/负数防护 — `batch/spark_energy_features.py`

**原始问题**
```python
frame.withColumn("spot_return_1h", F.log("spot_price") - F.log(F.lag("spot_price").over(w_market)))
```
`F.log(0)` 返回 `-Infinity`，`F.log(负数)` 返回 `NaN`，均会静默污染下游特征，且 `LAG` 首行为 `NULL` 时 `F.log(NULL)` 虽返回 NULL，但与正常 NULL 行为混在一起难以追踪。

**修复方式**
用 `F.when(col > 0, F.log(col))` 显式保护：
```python
safe_log = lambda col: F.when(F.col(col) > 0, F.log(F.col(col)))
lag_spot = F.lag("spot_price").over(w_market)
frame.withColumn(
    "spot_return_1h",
    F.when(lag_spot > 0, safe_log("spot_price") - F.log(lag_spot)),
)
```

**修复效果**
零/负 spot_price 时 `spot_return_1h` 为 `NULL` 而非 `±Infinity`/`NaN`，下游聚合函数（`AVG`、`STDDEV`）不被污染。

---

## 8. `coalesce(1)` 硬编码 — `batch/spark_energy_features.py`

**原始问题**
输出强制写入 1 个分区，大数据量场景下单个 Parquet 文件过大，成为读取瓶颈且无法并行写入。

**修复方式**
新增函数参数 `output_partitions: int = 1`，默认行为不变，大数据量时可覆盖：
```python
def compute_energy_features(input_path, output_path, output_partitions=1):
    ...
    enriched.coalesce(output_partitions).write.mode("overwrite").parquet(output_path)
```

**修复效果**
本地测试沿用默认值 `1`，生产环境可传入合理分区数（如 `4`），避免单文件过大。

---

## 9. `upstream()` KeyError — `platform/bruin_graph.py`

**原始问题**
```python
def upstream(self, name, depth=99):
    queue = list(self.nodes[name].depends)  # 若 name 不在 nodes 中直接抛 KeyError
```
当外部传入不存在的资产名（如 `bruin-lineage` CLI 误操作），整个 CLI 进程崩溃。

**修复方式**
```python
if name not in self.nodes:
    return []
```

**修复效果**
对不存在的节点名称返回空列表，CLI 输出友好提示而非堆栈崩溃。

---

## 10. 路径穿越防护 — `platform/bruin_graph.py`

**原始问题**
`_execute_node()` 直接执行 `run_file` 字段中的路径，若 YAML 资产文件被恶意修改为 `../../../../etc/passwd` 等路径，则会执行项目根目录外的脚本。

**修复方式**
```python
run_path = Path(node.run_file).resolve()
if not str(run_path).startswith(str(self.root.resolve())):
    raise RuntimeError(f"run_file escapes project root: {node.run_file!r}")
```

**修复效果**
所有 `run_file` 路径均被限制在 `bruin/` 项目根目录内，目录穿越路径在执行前即被拒绝。

---

## 11. subprocess 无超时 — `platform/bruin_graph.py`

**原始问题**
```python
subprocess.run([sys.executable, node.run_file], env=env, capture_output=True, text=True)
```
无 `timeout` 参数。若某个资产脚本死锁或无限等待（如网络挂起），`bruin-run` 命令永远不返回。

**修复方式**
```python
subprocess.run(..., timeout=300)
```

**修复效果**
单个资产执行超过 5 分钟时自动抛出 `subprocess.TimeoutExpired`，被 `run()` 捕获并标记为 `FAILED`，不阻塞后续资产执行。

---

## 12. 动量因子除零 — `features/energy_alpha.py`

**原始问题**
```python
lambda s: (s / s.shift(6) - 1).shift(1)
```
当 `spot_price` 在 6 小时前为 `0`（综合数据集或数据清洗边界可能发生），`s / 0` 产生 `inf`，后续 `rank()` 计算被污染。

**修复方式**
```python
lambda s: (s / s.shift(6).replace(0, np.nan) - 1).shift(1)
```

**修复效果**
前值为 0 时动量因子返回 `NaN` 而非 `inf`，`rank()` 计算忽略 `NaN`，不产生异常排名。

---

## 13. `pd.NA` 混入 float 列 — `features/energy_alpha.py`

**原始问题**
```python
df["alpha_energy_gas_spark_spread"] = pd.NA
```
`pd.NA` 是 pandas ExtensionArray 缺失值标记，赋给 float64 列时可能引发 `TypeError` 或隐式类型提升为 `object` dtype，破坏后续 `zscore` 等数值运算。

**修复方式**
```python
df["alpha_energy_gas_spark_spread"] = np.nan
```

**修复效果**
列保持 `float64` dtype，所有依赖该列的数值运算正常执行。

---

## 14. Bruin Runner 脚本无入口保护 — `bruin/pipelines/*/run_*.py`

**原始问题**
两个 runner 脚本（`run_energy_ingestion.py`、`run_equity_ingestion.py`）的顶层代码在 `import` 时直接执行，测试或工具链 `import` 这些模块时会触发真实 pipeline 运行。

**修复方式**
用 `if __name__ == '__main__':` 包裹业务代码，并加 `try/except` + `sys.exit(1)` 处理错误：
```python
if __name__ == "__main__":
    try:
        result = run_energy_pipeline(...)
        print(...)
    except Exception as exc:
        print(f"ERROR: {exc}", file=sys.stderr)
        sys.exit(1)
```

**修复效果**
脚本被 `import` 时不再执行副作用；运行失败时返回非零退出码，Bruin 调度器和 Kestra 任务可正确感知失败。

---

## 15. `demand_surprise` 恒为零 — `streaming/risingwave/views.sql`

**原始问题**
```sql
COALESCE(actual_load, load_forecast) - load_forecast AS demand_surprise
```
当 `actual_load IS NULL` 时，`COALESCE` 返回 `load_forecast`，结果 `load_forecast - load_forecast = 0`，信号完全失效。

**修复方式**
```sql
CASE WHEN actual_load IS NOT NULL
     THEN actual_load - load_forecast
     ELSE NULL
END AS demand_surprise
```
同时将 `solar_forecast / NULLIF(load_forecast, 0)` 改为 `COALESCE(solar_forecast, 0.0) / NULLIF(load_forecast, 0)`，避免 `solar_forecast` 为 NULL 时整列变 NULL。

**修复效果**
`demand_surprise` 在 actual_load 缺失时正确输出 NULL（而非 0），下游 `PERCENT_RANK()` 不会将所有无数据时段误排为同一分位。

---

## 16. 能源赛道缺失 — `bruin/pipelines/reporting/rpt_backtest_summary.sql`

**原始问题**
报告层 SQL 只包含 `equity` track，`description` 注释声称"合并 equity 和 energy 两个赛道"，但实际只有一侧，Streamlit 仪表盘能量研究页永远无数据。

**修复方式**
追加 `UNION ALL` 查询 `energy_backtest_daily` 表：
```sql
UNION ALL
SELECT 'energy' AS track, e.alpha_name, e.date, e.daily_pnl,
       SUM(e.daily_pnl) OVER (...) AS cumulative_pnl,
       e.sharpe_ann, e.max_drawdown
FROM energy_backtest_daily AS e
WHERE e.gate_consistency = true
```

**修复效果**
`rpt_backtest_summary` 同时包含 equity 和 energy 两个赛道的每日 P&L，仪表盘 track 筛选功能生效。

---

## 17. LAG 除零 — `bruin/pipelines/equity_ingestion/stg_equity_ohlcv.sql`

**原始问题**
```sql
ln(adj_close / LAG(adj_close) OVER (...)) AS ret_1d
```
`LAG` 在分组首行返回 NULL（结果为 NULL，可接受），但若上游数据存在 `adj_close = 0` 的脏数据，`ln(x / 0)` 产生 `±Infinity`，污染 `ret_1d` 列。

**修复方式**
```sql
ln(adj_close / NULLIF(LAG(adj_close) OVER (...), 0)) AS ret_1d
```

**修复效果**
前值为 0 时 `NULLIF` 将除数转为 NULL，结果为 NULL 而非 Infinity，所有依赖 `ret_1d` 的因子计算不被污染。

---

## 修复汇总

| # | 文件 | 类别 | 严重程度 |
|---|---|---|---|
| 1 | `storage/duckdb.py` | 安全 / SQL 注入 | 高 |
| 2 | `storage/gcp.py` | 安全 / SQL 注入 | 高 |
| 3 | `storage/gcp.py` | 健壮性 / 错误处理 | 中 |
| 4 | `streaming/redpanda_consumer.py` | 资源泄漏 | 中 |
| 5 | `streaming/redpanda_consumer.py` | 安全 / SQL 注入 | 高 |
| 6 | `batch/spark_energy_features.py` | 资源泄漏 | 中 |
| 7 | `batch/spark_energy_features.py` | 数据正确性 / 数值稳定性 | 中 |
| 8 | `batch/spark_energy_features.py` | 可配置性 | 低 |
| 9 | `platform/bruin_graph.py` | 健壮性 / 防崩溃 | 低 |
| 10 | `platform/bruin_graph.py` | 安全 / 路径穿越 | 高 |
| 11 | `platform/bruin_graph.py` | 健壮性 / 超时 | 中 |
| 12 | `features/energy_alpha.py` | 数据正确性 / 数值稳定性 | 中 |
| 13 | `features/energy_alpha.py` | 数据正确性 / dtype | 低 |
| 14 | `bruin/pipelines/*/run_*.py` | 健壮性 / 副作用 | 中 |
| 15 | `streaming/risingwave/views.sql` | 数据正确性 / 逻辑错误 | 高 |
| 16 | `bruin/rpt_backtest_summary.sql` | 功能缺失 | 高 |
| 17 | `bruin/stg_equity_ohlcv.sql` | 数据正确性 / 数值稳定性 | 低 |
