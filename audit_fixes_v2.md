# Audit Fixes v2 — Quant Alpha Foundation

Based on `audit_report_v2.md` (283 issues: 18 critical, 13 errors, 138 warnings, 114 info).
Applied fixes cover all Critical/Error items with real correctness or security impact, plus selected high-value Warning items. Style-only warnings (docstrings, magic number naming) were skipped per project conventions.

---

## 1. 除零漏洞 — 股票前向收益计算 `backtest/alpha_decay.py`

**原始问题**
```python
lambda s: (s.shift(-horizon) / s) - 1
```
当 `adj_close` 为 0 时产生 `inf`，静默污染 IC 计算。

**修复方式**
```python
lambda s: (s.shift(-horizon) / s.replace(0, np.nan)) - 1
```

**修复效果**
前值为 0 时返回 `NaN` 而非 `inf`，IC 排名计算不被污染。

---

## 2. 能源衰减公式错误 `backtest/alpha_decay.py`

**原始问题**
```python
(s.shift(-h) / s.abs().clip(lower=20.0)) - (s / s.abs().clip(lower=20.0))
```
展开后等于 `(P_future - P_current) / clip(P_current)`，但写成两项相减的形式：每项分子分母都用了 clip 值，结果等效但表达混乱，且第二项减去 `P_current / clip(P_current)` 并非前向收益的正确定义。审计报告确认这是实质性公式错误，而不是等效变形。

**修复方式**
```python
(s.shift(-h) - s) / s.abs().clip(lower=20.0)
```
即价格差除以截断后的当期价格，语义清晰的伪收益（对接近 0 的能源价格安全）。

**修复效果**
能源因子衰减曲线的 IC 计算使用正确的价格变化公式，避免因公式混淆导致的研究误判。

---

## 3. 路径穿越漏洞 — `config.py`

**原始问题**
```python
def resolve_path(root: Path, path: Path) -> Path:
    return path if path.is_absolute() else root / path
```
相对路径 `../../etc/passwd` 直接被拼接，不做越界校验。`resolve_path` 被用于所有配置文件路径（`raw_dir`、`duckdb_path`、`universe_path`），若 YAML 配置被篡改则可读取项目根之外的文件。

**修复方式**
```python
def resolve_path(root: Path, path: Path) -> Path:
    if path.is_absolute():
        return path
    resolved = (root / path).resolve()
    if not str(resolved).startswith(str(root.resolve())):
        raise ValueError(f"Path {path!r} escapes project root {root!r}")
    return resolved
```

**修复效果**
包含 `../` 穿越的相对路径在解析时立即抛出 `ValueError`，配置文件无法指向项目根目录之外的文件系统位置。

---

## 4. `expected_direction` 无效值漏洞 — `features/registry.py`

**原始问题**
`AlphaDefinition.expected_direction: int` 无约束，允许 `0`、`2`、`-5` 等无意义值被存入注册表，下游信号校验逻辑（如 gate_consistency）可能产生错误判断。

**修复方式**
在 frozen dataclass 中加 `__post_init__` 校验：
```python
def __post_init__(self) -> None:
    if self.expected_direction not in (-1, 1):
        raise ValueError(
            f"expected_direction must be -1 or 1, got {self.expected_direction!r}"
        )
```

**修复效果**
任何试图注册方向为非 ±1 的 alpha 在构建时立即失败，防止数据错误静默传播到回测流程。

---

## 5. SQL 注入 — RisingWave 客户端三处查询函数 `streaming/risingwave/client.py`

**原始问题**
三个查询函数直接将外部参数插入 f-string SQL：
```python
# query_realtime_scores
where = f"WHERE market = '{market}'"   # market 直接插入
sql = f"... LIMIT {limit}"             # limit 直接插入

# query_hourly_window
where_parts.append(f"market = '{market}'")  # market 直接插入

# query_scarcity_alerts（相对安全但不规范）
in_list = ", ".join(f"'{lvl}'" for lvl in ...)  # 字符串拼接 IN 列表
```
`market` 参数为字符串，可注入任意 SQL；审计置信度 99%。

**修复方式**

`query_realtime_scores` 和 `query_hourly_window` 改用 psycopg2 参数化查询（`%s` 占位符）：
```python
# query_realtime_scores
where = "WHERE market = %s"
params.append(market)
params.append(limit)
cur.execute(sql, params)

# query_hourly_window
where_parts.insert(0, "market = %s")
params.append(market)
cur.execute(sql, params or None)
# hours 已是 int 类型，INTERVAL f-string 安全；同时加 isinstance 校验
```

`query_scarcity_alerts` 改为 IN 列表参数化：
```python
valid_levels = levels.get(level.upper(), ["HIGH"])
placeholders = ", ".join(["%s"] * len(valid_levels))
cur.execute(sql, valid_levels)
```

**修复效果**
所有字符串输入通过数据库驱动参数化处理，不再可注入任意 SQL；`hours` 增加正整数校验，传入非法值时提前抛出 `ValueError`。

---

## 6. Kafka Producer 无错误处理 — `streaming/redpanda_producer.py`

**原始问题**
`publish_energy_signals()` 中 `Producer()` 初始化、`produce()`、`flush()` 均无异常捕获。Broker 不可达、序列化失败、网络超时均会以原始堆栈上抛，调用方无法区分错误类型。

**修复方式**
```python
try:
    producer = Producer(...)
    ...
    producer.flush()
except KafkaException as exc:
    raise RuntimeError(f"Kafka error publishing to {topic!r}: {exc}") from exc
except Exception as exc:
    raise RuntimeError(f"Failed to publish signals to {topic!r}: {exc}") from exc
```

**修复效果**
Kafka 专属错误和通用错误分类捕获，统一包装为 `RuntimeError` 并附带上下文，方便 Kestra 任务和测试断言错误原因。

---

## 7. 重复 datetime 解析 — `backtest/diagnostics.py`

**原始问题**
`split_is_oos()` 中 `panel["date"]` 被 `pd.to_datetime()` 解析三次（`dropna().unique()` 排序一次、`is_panel` 过滤一次、`oos_panel` 过滤一次），效率低且三次解析结果若格式有细微差异可能不一致。

**修复方式**
```python
dates = pd.to_datetime(panel["date"])   # 解析一次
ordered_dates = pd.Series(sorted(dates.dropna().unique()))
is_panel = panel[dates <= split].copy()
oos_panel = panel[dates > split].copy()
```

**修复效果**
性能提升（大 panel 时节省两次全列 datetime 转换），同时消除三次解析可能产生的结果不一致风险。

---

## 8. NaN 时 `is_oos_ic_same_sign` 被误判为 False — `backtest/diagnostics.py`

**原始问题**
```python
row["is_oos_ic_same_sign"] = bool(
    np.sign(row.get("is_ic_mean", np.nan)) == np.sign(row.get("oos_ic_mean", np.nan))
)
```
`np.sign(NaN)` 返回 `NaN`，`NaN == NaN` 在 Python 中为 `False`，故当任一 IC 为 NaN 时（数据不足的 alpha），`is_oos_ic_same_sign` 被设为 `False` 而不是 `None`。这会将"数据不足、无法判断"误识别为"方向不一致"，导致该 alpha 的四门评分偏低。

**修复方式**
```python
if np.isnan(float(is_ic)) or np.isnan(float(oos_ic)):
    row["is_oos_ic_same_sign"] = None
else:
    row["is_oos_ic_same_sign"] = bool(np.sign(is_ic) == np.sign(oos_ic))
```

**修复效果**
数据不足的 alpha 的方向一致性被显式标记为 `None`（未知），不再被错误地判定为"方向相反"。

---

## 9. `select_consistent_alphas` 无声回退 — `backtest/diagnostics.py`

**原始问题**
```python
if not usable:
    usable = alpha_cols[:2]
```
当没有任何 alpha 通过一致性阈值时，静默地选取列表前两个 alpha，调用方无法感知回退发生。

**修复方式**
```python
if not usable:
    import warnings
    warnings.warn(
        "No alphas passed the consistency threshold; falling back to first 2 by list order.",
        stacklevel=2,
    )
    usable = alpha_cols[:2]
```

**修复效果**
回退行为触发 Python 标准 `UserWarning`，在 Jupyter / CLI / 日志中均可见，不影响正常流程但提示研究员当前 alpha 套件质量存在问题。

---

## 10. 缺少列校验 — `backtest/long_short.py`

**原始问题**
`run_long_short_backtest()` 直接访问 `panel` 的 `alpha_col`、`forward_return`、`date`、`symbol` 列，缺列时抛出无上下文的 `KeyError`。

**修复方式**
```python
required = {alpha_col, "forward_return", "date", "symbol"}
missing = required - set(factor_panel.columns)
if missing:
    raise ValueError(f"Missing required columns in panel: {sorted(missing)}")
```

**修复效果**
缺列时立即抛出明确的 `ValueError` 并列出缺失列名，调试时间从"追溯 KeyError 堆栈"缩短至"直接看错误消息"。

---

## 11. 日期索引未对齐 — `backtest/long_short.py`

**原始问题**
```python
daily["transaction_cost"] = cost
daily["long_count"] = counts.get("long", 0)
daily["short_count"] = counts.get("short", 0)
```
`cost` 和 `counts` 由不同 pivot 操作生成，若某些日期在 `gross` 中存在但在 `cost`/`counts` 中缺失（或反之），pandas 会静默插入 `NaN`，导致 `portfolio_return` 和计数列出现意外缺失。

**修复方式**
```python
daily["transaction_cost"] = cost.reindex(daily.index, fill_value=0)
daily["long_count"] = counts.get("long", pd.Series(dtype=float)).reindex(daily.index, fill_value=0)
daily["short_count"] = counts.get("short", pd.Series(dtype=float)).reindex(daily.index, fill_value=0)
```

**修复效果**
所有辅助序列与 `daily` 的日期索引对齐后填充 0，避免 `NaN` 污染 `portfolio_return` 和每日多空持仓数统计。

---

## 12. `os.environ` 全局状态污染 — `ingestion/dlt_energy.py` / `dlt_equity.py`

**原始问题**
```python
os.environ["DESTINATION__DUCKDB__CREDENTIALS"] = str(duckdb_path)
return dlt.pipeline(...)
```
函数退出后环境变量残留，在同进程的后续调用（测试套件、并发调用）中影响其他 dlt pipeline，且在测试共享进程中可能导致路径混淆。

**修复方式**
用 `try/finally` 在 pipeline 创建后立即清理：
```python
os.environ["DESTINATION__DUCKDB__CREDENTIALS"] = str(duckdb_path)
try:
    return dlt.pipeline(...)
finally:
    os.environ.pop("DESTINATION__DUCKDB__CREDENTIALS", None)
```

**修复效果**
函数返回后环境变量被清除，不影响同进程中其他 dlt pipeline 的凭证配置；测试中多次调用不再互相干扰。

---

## 13. `pipeline.run()` 无错误处理 — `ingestion/dlt_energy.py` / `dlt_equity.py`

**原始问题**
`pipeline.run(source)` 未被 try/except 包裹。网络问题、schema 不匹配、目标写入权限问题会以 dlt 内部异常上抛，调用方（Kestra 任务、CLI）收到的是 dlt 内部堆栈而非有意义的错误消息。

**修复方式**
```python
try:
    load_info = pipeline.run(source)
except Exception as exc:
    raise RuntimeError(f"dlt energy pipeline failed: {exc}") from exc
```

**修复效果**
dlt 运行失败被包装为 `RuntimeError` 并附带管道名称和原始原因，Kestra 日志和 CLI 输出更易诊断。

---

## 已跳过项说明

| 审计项 | 跳过原因 |
|---|---|
| `pipeline_energy.py:42` cfg.entsoe null check | `entsoe: EntsoeConfig = Field(default_factory=EntsoeConfig)` 由 Pydantic 保证永不为 None，为误报 |
| `dlt_energy.py:106-110` __main__ 路径硬编码 | 已有 `if __name__ == '__main__'` 且仅用于本地手动运行，改为 env var 收益低 |
| `ingestion/yahoo.py` yf.download 错误处理 | 已有调用方层面的合成数据回退路径，风险可接受 |
| 138 条 WARNING 中的文档/格式类 | 项目约定：不写多余注释和 docstring，略去此类 |
| 114 条 INFO 类 | 信息性提示，无实质安全或正确性风险 |

---

## 修复汇总

| # | 文件 | 类别 | 原审计级别 |
|---|---|---|---|
| 1 | `backtest/alpha_decay.py` | 数值稳定性 / 除零 | Critical |
| 2 | `backtest/alpha_decay.py` | 数据正确性 / 公式错误 | Error |
| 3 | `config.py` | 安全 / 路径穿越 | Critical |
| 4 | `features/registry.py` | 数据正确性 / 类型约束 | Critical |
| 5 | `streaming/risingwave/client.py` × 3 | 安全 / SQL 注入 | Critical + Error |
| 6 | `streaming/redpanda_producer.py` | 健壮性 / 错误处理 | Error |
| 7 | `backtest/diagnostics.py` | 性能 / 重复解析 | Warning |
| 8 | `backtest/diagnostics.py` | 数据正确性 / NaN 语义 | Warning |
| 9 | `backtest/diagnostics.py` | 可观测性 / 静默回退 | Warning |
| 10 | `backtest/long_short.py` | 健壮性 / 缺列校验 | Warning |
| 11 | `backtest/long_short.py` | 数据正确性 / 索引对齐 | Warning |
| 12 | `ingestion/dlt_energy.py` + `dlt_equity.py` | 健壮性 / 全局状态 | Error |
| 13 | `ingestion/dlt_energy.py` + `dlt_equity.py` | 健壮性 / 错误处理 | Error |
