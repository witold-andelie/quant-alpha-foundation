# Cloud and Kubernetes Module

This module covers the cloud-computing and Kubernetes part of the semester project.

## Cloud Architecture

The default cloud target is GCP because it maps cleanly to the Data Engineering Zoomcamp path:

- Cloud Storage as the data lake.
- BigQuery as the analytical warehouse.
- Artifact Registry for Docker images.
- GKE Autopilot for scheduled research jobs and the dashboard.
- A dedicated service account for pipeline execution.

The Terraform entry point is `infra/terraform/main.tf`.

## Kubernetes Architecture

The Kubernetes manifests live in `infra/k8s/base` and deploy:

- `energy-alpha-refresh`: hourly CronJob for the Second Foundation energy pipeline.
- `equity-alpha-refresh`: weekday CronJob for the US equities demo.
- `quant-alpha-dashboard`: Streamlit dashboard deployment.
- `quant-alpha-data`: persistent volume claim for local DuckDB and Parquet artifacts.

## Commands

```bash
cd infra/terraform
cp terraform.tfvars.example terraform.tfvars
terraform init
terraform plan
```

```bash
kubectl apply -k infra/k8s/base
kubectl get cronjobs -n quant-alpha
kubectl get pods -n quant-alpha
kubectl port-forward -n quant-alpha service/quant-alpha-dashboard 8501:80
```

## Deployment Notes

Before applying to a real cluster, replace `PROJECT_ID` in `infra/k8s/base/kustomization.yaml` with your GCP project ID or override the image in an environment-specific overlay.

For a production deployment, move DuckDB and Parquet state to GCS/BigQuery instead of relying on a single persistent volume. The PVC is intentionally simple for a semester-project deployment path.

## BigQuery dbt Path

The real-data cloud path is:

```text
ENTSO-E API -> local Parquet -> GCS Parquet objects -> BigQuery raw tables -> dbt_energy_alpha marts -> Streamlit
```

Enable export in `configs/second_foundation_project.yaml`:

```yaml
data_source: entsoe
cloud:
  enabled: true
  gcp_project_id: your-project
  gcs_bucket: your-project-second-foundation-lake
  bigquery_dataset: second_foundation_quant
  bigquery_location: EU
```

Then run:

```bash
export ENTSOE_API_KEY=your-token
export GOOGLE_APPLICATION_CREDENTIALS=/path/to/service-account.json
quant-alpha energy-run --source entsoe
```

The pipeline uploads each Parquet table to GCS and uses a BigQuery load job from that
GCS URI. The source table names match the DuckDB tables:

- `power_market_raw`
- `power_market_features`
- `power_market_quality`
- `energy_alpha_registry`
- `energy_backtest_daily`
- `energy_backtest_metrics`
- `energy_alpha_diagnostics`
- `energy_alpha_metrics`
- `energy_alpha_backtest_daily`
- `energy_alpha_value_added`

The energy dbt project includes a BigQuery profile target:

```bash
cd dbt_energy_alpha
GCP_PROJECT_ID=your-project \
BQ_DATASET=second_foundation_quant \
BQ_LOCATION=EU \
ENERGY_SOURCE_SCHEMA=second_foundation_quant \
GOOGLE_APPLICATION_CREDENTIALS=/path/to/key.json \
dbt build --profiles-dir . --target bigquery
```

Streamlit can read those BigQuery tables after dbt has built marts:

```bash
STREAMLIT_DATA_BACKEND=bigquery \
GCP_PROJECT_ID=your-project \
BQ_DATASET=second_foundation_quant \
streamlit run streamlit_app/app.py
```

The local default still uses DuckDB so the project remains runnable without ENTSO-E or
cloud credentials.
