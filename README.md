# Quant Alpha Foundation

A Second Foundation-inspired quantitative energy research platform with a parallel US equities alpha demo. Built to cover the [DataTalksClub Data Engineering Zoomcamp](https://github.com/DataTalksClub/data-engineering-zoomcamp) syllabus in semester-project form, with WorldQuant-style formulaic alpha research and a production-ready data engineering stack.

## Tracks

| Track | Universe | Factors | Data Source |
|---|---|---|---|
| Second Foundation Energy | European power markets | 8 energy alphas | ENTSO-E / synthetic |
| US Equities Demo | Configurable equity universe | 10 equity alphas | Yahoo Finance / synthetic |

## Architecture

```
ENTSO-E API / Yahoo Finance / Synthetic
        │
        ▼
   dlt incremental ingestion  ──────────────────────────────► DuckDB raw layer
        │                                                           │
        ▼                                                           ▼
   Kestra orchestration                               Bruin asset graph (M5)
        │                                                           │
        ▼                                                           ▼
   Raw Parquet (data lake)  ──────────────► GCS bucket (cloud)   dbt staging
        │                                                           │
        ▼                                                           ▼
   Python factor engine → alpha panel ──────────────────────► dbt marts
        │                                                           │
        ▼                                                           ▼
   DuckDB warehouse  ──────────────────────────────────► BigQuery (cloud)
        │                │
        ▼                ▼
   Spark batch     Redpanda / RisingWave (streaming SQL)
        │                │
        └────────────────┘
                 │
                 ▼
        Streamlit research dashboard
```

## Zoomcamp Module Coverage

| Module | Technology | Status |
|---|---|---|
| M1 — Containerization + IaC | Docker, Docker Compose, Terraform (GCS/BQ/GKE/WI/Secrets) | ✅ Complete |
| M2 — Orchestration | Kestra (5 flows, K8s deployment, cross-flow triggers) | ✅ Complete |
| Workshop 1 — dlt Ingestion | dlt v1.26 (`@dlt.source`, incremental cursor, DuckDB dest) | ✅ Complete |
| M3 — Data Warehouse | DuckDB (local), BigQuery 3-layer schema (Terraform-declared) | ✅ Complete |
| M4 — Analytics Engineering | dbt (2 projects, 19 models, staging + marts, schema tests) | ✅ Complete |
| M5 — Data Platforms | Bruin asset graph (8 assets, lineage, topological runner) | ✅ Complete |
| M6 — Batch Processing | Apache Spark (7 rolling features, window functions) | ✅ Complete |
| M7 — Streaming | Redpanda + Avro + RisingWave (5 materialized views) | ✅ Complete |
| Workshop 2 — RisingWave | Streaming SQL, Kafka source, real-time alpha scores | ✅ Complete |
| Cloud + Kubernetes | Helm chart (dev/prod overlays, HPA), K8s live deploy | ✅ Complete |
| CI/CD | GitHub Actions (7-stage: lint/test/dlt/dbt/helm/terraform/docker/trivy) | ✅ Complete |

Full coverage audit: [docs/zoomcamp_coverage.md](docs/zoomcamp_coverage.md)

**Test suite: 40 tests, all passing.**

## Alpha Research

### Four Research Gates

| Gate | Description |
|---|---|
| Robustness | Stable OOS evidence beats many fragile factors |
| Uniqueness | Each expression captures one phenomenon; correlation matrix flags overlap |
| Value-added | Composite OOS Sharpe must exceed the best single-factor |
| Consistency | IS and OOS IC must share sign and comparable magnitude |

Full factor catalog and validation checklist: [docs/alpha_research.md](docs/alpha_research.md)

## Quick Start

```bash
cd quant-alpha-foundation
python -m venv .venv && source .venv/bin/activate
pip install -e ".[dev,dlt]"

# Equity pipeline (offline, no API key)
quant-alpha run --offline

# Energy pipeline (synthetic)
quant-alpha energy-run

# dlt incremental ingestion
quant-alpha dlt-energy --start 2024-01-01
quant-alpha dlt-equity --offline

# Bruin asset graph
quant-alpha bruin-lineage
quant-alpha bruin-run --dry-run

# Dashboard
streamlit run streamlit_app/app.py
```

## With Real Data

```bash
# ENTSO-E live data (free token from transparency.entsoe.eu)
export ENTSOE_API_KEY=your-token
quant-alpha energy-run --source entsoe

# Cloud export (requires GCP credentials)
# Set cloud.enabled: true in configs/second_foundation_project.yaml
quant-alpha energy-run
```

## Docker

```bash
# Research stack (pipeline + dashboard)
docker compose up --build quant-alpha dashboard

# Streaming + orchestration stack
docker compose up -d redpanda redpanda-console kestra

# RisingWave streaming SQL stack (Workshop 2)
docker compose -f docker-compose.risingwave.yml up -d

# Seed live signals demo (no broker needed)
python -m quant_alpha.streaming.demo_signals
```

## Kubernetes (Helm)

```bash
# Local dev cluster (Docker Desktop / Kind)
docker build -t quant-alpha-foundation:latest .
helm upgrade --install quant-alpha infra/helm/quant-alpha/ \
  -f infra/helm/quant-alpha/values.dev.yaml \
  --namespace quant-alpha --create-namespace \
  --set image.tag=latest --set image.pullPolicy=IfNotPresent

# Access dashboard
kubectl port-forward -n quant-alpha svc/quant-alpha-dashboard 8501:8501
```

## Terraform (GCP)

```bash
cd infra/terraform
cp terraform.tfvars.example terraform.tfvars   # fill in project_id + github_repo
# Create remote state bucket first (once):
# gsutil mb gs://<project_id>-tf-state && gsutil versioning set on gs://<project_id>-tf-state
terraform init && terraform plan && terraform apply
```

## dbt

```bash
cd dbt_quant_alpha && dbt build --profiles-dir .      # equity marts
cd dbt_energy_alpha && dbt build --profiles-dir .     # energy marts
```

## Module Documentation

| Module | README |
|---|---|
| Data ingestion (Yahoo Finance, ENTSO-E, dlt, synthetic) | [src/quant_alpha/ingestion/README.md](src/quant_alpha/ingestion/README.md) |
| Alpha factor engine (10 equity + 8 energy factors) | [src/quant_alpha/features/README.md](src/quant_alpha/features/README.md) |
| Backtesting (long-short, decay, walk-forward) | [src/quant_alpha/backtest/README.md](src/quant_alpha/backtest/README.md) |
| Streaming (Redpanda + RisingWave) | [src/quant_alpha/streaming/README.md](src/quant_alpha/streaming/README.md) |
| Storage (DuckDB, GCS, BigQuery) | [src/quant_alpha/storage/README.md](src/quant_alpha/storage/README.md) |
| Batch processing (Spark) | [src/quant_alpha/batch/README.md](src/quant_alpha/batch/README.md) |
| Data platform (Bruin asset graph, contracts, quality) | [src/quant_alpha/platform/README.md](src/quant_alpha/platform/README.md) |
| Bruin asset definitions | [bruin/README.md](bruin/README.md) |
| Kestra orchestration flows | [flows/kestra/README.md](flows/kestra/README.md) |
| Helm chart (dev/prod overlays) | [infra/helm/quant-alpha/README.md](infra/helm/quant-alpha/README.md) |
| Terraform IaC (GCP, Workload Identity) | [infra/terraform/README.md](infra/terraform/README.md) |
| Kubernetes manifests | [infra/k8s/README.md](infra/k8s/README.md) |
| dbt — equity marts | [dbt_quant_alpha/README.md](dbt_quant_alpha/README.md) |
| dbt — energy marts | [dbt_energy_alpha/README.md](dbt_energy_alpha/README.md) |
| Streamlit dashboard | [streamlit_app/README.md](streamlit_app/README.md) |

## Research Documentation

| Document | Description |
|---|---|
| [docs/zoomcamp_coverage.md](docs/zoomcamp_coverage.md) | Module-by-module Zoomcamp coverage audit (full matrix) |
| [docs/alpha_research.md](docs/alpha_research.md) | Research methodology, factor catalog, four-gate validation |
| [docs/architecture.md](docs/architecture.md) | System layers and production boundary |
| [docs/cloud_kubernetes.md](docs/cloud_kubernetes.md) | Cloud deployment guide |

## Project Layout

```
bruin/                Bruin asset graph (8 YAML/SQL asset definitions)
configs/              Universe and pipeline parameters
data/raw/             Raw immutable Parquet extracts
data/warehouse/       DuckDB databases
dbt_quant_alpha/      dbt project — equity warehouse (10 models)
dbt_energy_alpha/     dbt project — energy warehouse (9 models)
docs/                 Architecture, research notes, coverage audit
flows/kestra/         Kestra orchestration flows (5 flows)
infra/helm/           Helm chart with dev/prod value overlays
infra/k8s/            Kubernetes manifests (Kustomize base, 9 resources)
infra/terraform/      GCP IaC (6 files: main/backend/WI/secrets/iam/outputs)
schemas/              Avro schema definitions
src/quant_alpha/      Python package
  ingestion/          Yahoo Finance, ENTSO-E, dlt, synthetic generators
  features/           Equity + energy alpha factor engines
  backtest/           Long-short backtest, alpha decay, walk-forward IC
  streaming/          Redpanda producer/consumer + RisingWave streaming SQL
  batch/              Apache Spark feature computation
  platform/           Bruin graph runner, data contracts, quality checks
  storage/            DuckDB, GCS, BigQuery writers
streamlit_app/        Streamlit research dashboard
tests/                40-test suite
```

## References

- DataTalksClub Data Engineering Zoomcamp: [github.com/DataTalksClub/data-engineering-zoomcamp](https://github.com/DataTalksClub/data-engineering-zoomcamp)
- Second Foundation: [second-foundation.eu](https://www.second-foundation.eu)
- Kakushadze, Z. (2016). *101 Formulaic Alphas*. Wilmott.
- ENTSO-E Transparency Platform: [transparency.entsoe.eu](https://transparency.entsoe.eu)
- RisingWave: [risingwave.com](https://risingwave.com)
- Bruin: [bruin-data.github.io/bruin](https://bruin-data.github.io/bruin/)
- dlt: [dlthub.com](https://dlthub.com)
