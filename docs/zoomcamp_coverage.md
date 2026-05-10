# Zoomcamp Coverage Audit

Maps each DataTalksClub Data Engineering Zoomcamp module to the implementation in this repository.

## Coverage Matrix

| Zoomcamp Module | Knowledge Points | Status | Key Evidence |
|---|---|---|---|
| **M1 — Docker + Terraform** | Containerization, Docker Compose, IaC, GCP provisioning, remote state, Workload Identity | ✅ Complete | `Dockerfile`, `docker-compose.yml`, `docker-compose.risingwave.yml`, `infra/terraform/` (6 files: main/backend/WI/secrets/iam/outputs) |
| **M2 — Workflow Orchestration** | DAG structure, scheduling, backfills, multi-task flows, error handling, cross-flow triggers, cloud deployment | ✅ Complete | 5 Kestra flows, K8s Kestra Deployment, `flows/kestra/README.md` (cloud deploy guide) |
| **Workshop 1 — dlt Ingestion** | `@dlt.source`, `@dlt.resource`, `dlt.sources.incremental`, DuckDB destination, stateful cursor, schema hints | ✅ Complete | `ingestion/dlt_energy.py`, `ingestion/dlt_equity.py`, `tests/test_dlt_pipelines.py` (5 tests), CI smoke step |
| **M3 — Data Warehouse** | Partitioning, clustering, warehouse modeling, 3-layer schema, BQ table schema declarations | ✅ Complete | DuckDB warehouse, BigQuery datasets (raw/staging/marts) in Terraform, `storage/duckdb.py`, `storage/gcp.py` |
| **M4 — Analytics Engineering** | dbt models, sources, staging, marts, schema tests, CI `dbt build` | ✅ Complete | `dbt_quant_alpha/` (10 models), `dbt_energy_alpha/` (9 models), 13+ schema tests, CI step |
| **M5 — Data Platforms** | Asset graph, lineage tracking, declarative asset definitions, data quality, schema contracts | ✅ Complete | `bruin/` (8 asset YAML/SQL files), `platform/bruin_graph.py` (topological runner), `platform/contracts.py`, `tests/test_bruin_graph.py` (9 tests) |
| **M6 — Batch Processing** | Spark DataFrames, window functions, joins, Parquet I/O, `spark-submit` | ✅ Complete | `batch/spark_energy_features.py` (7 rolling features) |
| **M7 — Streaming** | Kafka/Redpanda, Avro schema, producer, consumer, schema management | ✅ Complete | `streaming/redpanda_producer.py`, `redpanda_consumer.py`, `schemas/energy_signal.avsc` |
| **Workshop 2 — RisingWave** | Streaming SQL, materialized views, Kafka source connector, real-time alpha | ✅ Complete | `streaming/risingwave/views.sql` (5 MViews), `client.py`, `simulator.py`, `docker-compose.risingwave.yml`, `tests/test_risingwave_simulator.py` (8 tests) |
| **Cloud / Kubernetes** | Container registry, Helm chart, CronJobs, Secrets, PVC, HPA, K8s overlays | ✅ Complete | `infra/helm/quant-alpha/` (full Helm chart, dev/prod values), `infra/k8s/base/` (9 manifests incl. Kestra), live cluster deploy |
| **CI/CD** | Lint, test, build, deploy, alpha regression gate, Trivy, Helm lint, Terraform validate | ✅ Complete | `.github/workflows/ci.yml` (7-stage pipeline) |
| **Capstone** | End-to-end pipeline, dashboard, reproducibility, documentation | ✅ Complete | Full equity + energy pipelines, Streamlit dashboard, dbt marts, Kestra orchestration, 40 tests |

---

## Detailed Coverage

### M1 — Docker + Terraform

**Docker:**
- `docker-compose.yml` — Redpanda, Console, Kestra, Kestra-Postgres, Streamlit dashboard
- `docker-compose.risingwave.yml` — RisingWave, Redpanda, signal producer (Workshop 2 stack)
- Single `Dockerfile` used by all compute workloads and K8s CronJobs

**Terraform** (`infra/terraform/` — 6 files):
- `main.tf` — GCS lake (versioned, lifecycle), 3× BigQuery datasets, BQ table schemas, Service Account, Artifact Registry, GKE Autopilot with Workload Identity
- `backend.tf` — GCS remote state (prevents local drift)
- `workload_identity.tf` — WI Pool, GitHub Actions OIDC provider, GKE pod WI binding (keyless auth)
- `secrets.tf` — Secret Manager resources (ENTSO-E key, SA key) + accessor IAM
- `iam.tf` — All IAM role bindings consolidated (BQ/GCS/AR/GKE/Secrets/WI)
- `outputs.tf` — Actionable outputs: WI provider URL, Helm annotation, image repo, `kubectl` command

### M2 — Workflow Orchestration (Kestra)

Five flows covering all orchestration patterns:

| Flow | Pattern | Schedule |
|---|---|---|
| `daily_alpha_pipeline` | Single-task, scheduled | 22:00 UTC Mon–Fri |
| `second_foundation_energy_pipeline` | Multi-task, timeout, error handler | Hourly at :05 |
| `dlt_ingestion_pipeline` | Sequential, verification task | 22:30 UTC Mon–Fri |
| `bruin_asset_refresh` | Cross-flow trigger (from dlt), validation | Mon 03:00 + auto |
| `risingwave_view_init` | Idempotent DDL, fallback seeder | Daily 00:05 |

Cloud deployment: `infra/k8s/base/kestra-deployment.yaml` + init container that pre-loads flow YAMLs.

### Workshop 1 — dlt Framework

Declarative incremental ingestion using dlt v1.26.0:

```
@dlt.source → @dlt.resource → dlt.sources.incremental (cursor)
    │
    ▼
DuckDB destination (dlt_energy_raw / dlt_equity_raw schemas)
    │
    └── _dlt_pipeline_state  ← cursor survives restarts
```

Verified incremental behavior: same range → 0 packages; extended range → 1 package with only new rows.

### M3 — Data Warehouse

Three-layer BigQuery architecture (declared in Terraform):

```
second_foundation_quant          ← raw (sources)
second_foundation_quant_staging  ← dbt staging models
second_foundation_quant_marts    ← dbt mart + Terraform-declared table schemas
```

Local DuckDB mirrors the same structure for offline development.

### M4 — Analytics Engineering (dbt)

| Project | Staging Models | Mart Models | Schema Tests |
|---|---|---|---|
| `dbt_quant_alpha` | 3 | 7 | 7 not_null checks |
| `dbt_energy_alpha` | 4 | 5 | 6 not_null + freshness |

CI runs `dbt build --profiles-dir .` in the `pipeline-smoke` job.

### M5 — Data Platforms (Bruin)

Full Bruin-style asset graph with 8 assets across 4 pipelines:

```
raw_equity_ohlcv  →  stg_equity_ohlcv  →  fct_equity_alpha_panel  →  fct_alpha_diagnostics
raw_power_market  →  stg_power_market  →  fct_energy_alpha_panel               │
                                                                          rpt_backtest_summary
```

- YAML asset definitions with column schemas and custom SQL quality checks
- `AssetGraph.topological_order()` — Kahn's algorithm for dependency-safe execution
- `upstream()` / `downstream()` — lineage traversal
- CLI: `quant-alpha bruin-lineage`, `quant-alpha bruin-run --dry-run`

### M6 — Batch Processing (Spark)

`spark_energy_features.py` computes 7 rolling features over hourly energy Parquet using Spark window functions. Runs locally by default; submit to a cluster via `spark-submit` without code changes.

### M7 — Streaming (Redpanda + RisingWave)

**Redpanda (M7):**
- Avro schema (`schemas/energy_signal.avsc`) parsed by `fastavro`
- Producer serializes energy signals; consumer upserts to DuckDB
- Demo seeder (`demo_signals.py`) for offline/CI use

**RisingWave (Workshop 2):**
- 5 materialized views: `source → hourly_window → momentum_6h → cross_market_spread → realtime_alpha_scores → scarcity_alerts`
- DuckDB simulator reproduces the same window-function logic offline
- `docker-compose.risingwave.yml` — one-command local RisingWave stack

### Cloud / Kubernetes

**Helm chart** (`infra/helm/quant-alpha/`):
- 11 templates, `_helpers.tpl` with shared macros
- `values.dev.yaml` — local image, NodePort, 5 Gi PVC, minimal resources
- `values.prod.yaml` — GCP Artifact Registry, LoadBalancer, HPA (2–5 replicas), 50 Gi, Workload Identity annotation
- Live Helm release deployed to `quant-alpha` namespace (Revision 1)

**CI Helm validation:** lint (3 value sets) + `helm template` smoke render asserting Deployment/CronJob/PVC presence.

**Kestra on K8s:** `kestra-deployment.yaml` with init container for flow loading; Helm chart instructions in `flows/kestra/README.md`.

### CI/CD (GitHub Actions)

Seven-stage pipeline:

```
lint-and-test ──► pipeline-smoke ──► k8s-validate ──► docker-build ──► security-scan
                       │                  │
                  alpha-gate         helm-lint
                  dlt smoke          terraform-validate
                  dbt build
```

---

## Honest Gaps

These require external credentials and cannot be fully verified locally:

| Item | Current State | Needed |
|---|---|---|
| GKE live deployment | Manifests + Helm chart complete; local Kind cluster deployed | GCP project with billing |
| BigQuery production data | Terraform declares tables; export path implemented | `cloud.enabled: true` + GCP SA credentials |
| ENTSO-E live data | Client implemented (`entsoe.py`) | Free API token from transparency.entsoe.eu |
| RisingWave live streaming | Views SQL + simulator complete | Docker daemon (for `docker-compose.risingwave.yml`) |
| Kestra cloud (postgres) | K8s manifest complete | Running cluster + PVC |

---

## Test Coverage

```
40 tests — all passing
  test_ingestion.py          6 tests   (Yahoo Finance / synthetic)
  test_alpha_factors.py      7 tests   (equity factors, diagnostics)
  test_energy_alpha.py       4 tests   (energy factors)
  test_dlt_pipelines.py      5 tests   (dlt incremental behavior)
  test_bruin_graph.py        9 tests   (asset graph, lineage, dry-run)
  test_risingwave_simulator.py  8 tests (streaming views, scarcity alerts)
  test_batch.py              1 test    (Spark features)
```
