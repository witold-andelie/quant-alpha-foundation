# Kestra Orchestration

Kestra workflow definitions for the platform. Covers the zoomcamp Module 2 (Workflow Orchestration) knowledge points: DAG execution, scheduling, backfills, multi-task flows, cross-flow triggers, and cloud deployment.

## Flows

| Flow | Schedule | Purpose | Module |
|---|---|---|---|
| `daily_alpha_pipeline.yaml` | 22:00 UTC Mon–Fri | Equity factor pipeline | M2 |
| `second_foundation_energy_pipeline.yaml` | Hourly at :05 | Energy pipeline → Spark → dbt | M2 |
| `dlt_ingestion_pipeline.yaml` | 22:30 UTC Mon–Fri | dlt incremental ingestion (equity + energy) | Workshop 1 |
| `bruin_asset_refresh.yaml` | 03:00 Mon + after dlt | Bruin asset graph execution | M5 |
| `risingwave_view_init.yaml` | 00:05 UTC daily | RisingWave streaming SQL view init | Workshop 2 |

### Flow Dependency Chain

```
22:00  daily_alpha_pipeline        (equity)
22:30  dlt_ingestion_pipeline      (incremental load)
           │
           └─► bruin_asset_refresh (triggered automatically via Flow trigger)

00:05  risingwave_view_init        (streaming views)
00:05  second_foundation_energy_pipeline  (hourly)
```

---

## Running Locally (Docker)

```bash
docker compose up -d kestra-postgres kestra
# UI: http://localhost:8080
```

Register all flows at once:

```bash
for f in flows/kestra/*.yaml; do
  curl -s -X POST http://localhost:8080/api/v1/flows \
    -H "Content-Type: application/x-yaml" \
    --data-binary "@$f" | python3 -c "import sys,json; d=json.load(sys.stdin); print(d.get('id','?'), d.get('namespace','?'))"
done
```

---

## Cloud Deployment (GKE)

The `infra/k8s/base/kestra-deployment.yaml` manifest deploys Kestra as a K8s Deployment in standalone mode. The init container copies flow YAMLs from the application image into Kestra's flow directory at startup.

### Deploy via Kustomize

```bash
# Kustomize applies all base manifests including kestra-deployment.yaml
kubectl apply -k infra/k8s/base/

# Wait for Kestra to be ready
kubectl rollout status deployment/kestra -n quant-alpha

# Access the UI
kubectl port-forward -n quant-alpha svc/kestra 8080:8080
open http://localhost:8080
```

### Deploy via Helm (Official Kestra Chart)

For production workloads, use the official Kestra Helm chart with PostgreSQL:

```bash
helm repo add kestra https://helm.kestra.io/
helm repo update

helm upgrade --install kestra kestra/kestra \
  --namespace quant-alpha \
  --set configuration.kestra.storage.type=gcs \
  --set configuration.kestra.storage.gcs.bucket=$(terraform -chdir=infra/terraform output -raw lake_bucket_name) \
  --set postgresql.enabled=true \
  --set postgresql.auth.password=REPLACE_PG_PASSWORD
```

### Register flows via CI

The CI pipeline registers flows to a running Kestra instance when `KESTRA_URL` is set:

```bash
# Add to GitHub repository secrets:
KESTRA_URL=http://<kestra-service-ip>:8080

# The CI step (already in ci.yml) runs:
for f in flows/kestra/*.yaml; do
  curl -X POST $KESTRA_URL/api/v1/flows \
    -H "Content-Type: application/x-yaml" \
    --data-binary "@$f"
done
```

---

## Flow Details

### `dlt_ingestion_pipeline.yaml`

| Property | Value |
|---|---|
| Module | Workshop 1 (dlt framework) |
| Schedule | 22:30 UTC Mon–Fri |
| Tasks | dlt-energy → dlt-equity → row count verify |
| Trigger downstream | `bruin_asset_refresh` (Flow trigger) |

Runs `quant-alpha dlt-energy` and `quant-alpha dlt-equity`. The dlt cursor state persists in `_dlt_pipeline_state` inside DuckDB — subsequent runs load only new records.

### `bruin_asset_refresh.yaml`

| Property | Value |
|---|---|
| Module | M5 (Data Platforms / Bruin) |
| Schedule | 03:00 UTC Mon + auto after dlt |
| Tasks | bruin-lineage → bruin-run → table validation |
| Trigger | Flow trigger from `dlt_ingestion_pipeline` |

Executes the Bruin asset DAG via `quant-alpha bruin-run`. Assets run in topological order; if an upstream asset fails, downstream assets are automatically skipped.

### `risingwave_view_init.yaml`

| Property | Value |
|---|---|
| Module | Workshop 2 (RisingWave streaming SQL) |
| Schedule | 00:05 UTC daily |
| Tasks | wait-for-RW → apply views.sql → validate → seed demo |
| Fallback | Seeds DuckDB `live_energy_signals` if RisingWave unavailable |

Applies `streaming/risingwave/views.sql` via the Python client. All DDL uses `CREATE ... IF NOT EXISTS` — safe to re-run. Falls back to the DuckDB demo seeder when RisingWave is not in the stack.

---

## Parameterization

All flows accept inputs that can be overridden at trigger time via the UI or API:

| Flow | Input | Default | Description |
|---|---|---|---|
| All | `project_root` | `/app` | Absolute project path inside the container |
| `second_foundation_energy_pipeline` | `energy_source` | `synthetic` | `synthetic` or `entsoe` |
| `dlt_ingestion_pipeline` | `energy_markets` | `DE_LU,CZ,FR` | Comma-separated bidding zones |
| `dlt_ingestion_pipeline` | `offline_equity` | `true` | Use synthetic prices |
| `bruin_asset_refresh` | `targets` | `` | Specific assets to run (empty = all) |
| `risingwave_view_init` | `risingwave_host` | `risingwave` | RisingWave service hostname |

---

## Backfill

Trigger a flow manually for a historical execution date via the Kestra UI:

1. Open http://localhost:8080 → Flows → select flow
2. Click **Execute** → set execution date → Submit
3. Monitor in **Executions** tab

---

## Adding a New Flow

1. Create `flows/kestra/<name>.yaml` with `id`, `namespace`, `description`, `tasks`
2. Add a `triggers` block (schedule or Flow trigger)
3. Register via the API: `curl -X POST .../api/v1/flows --data-binary @<file>`
4. The CI pipeline auto-registers all flows on deploy when `KESTRA_URL` is set
