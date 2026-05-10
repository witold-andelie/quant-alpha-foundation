# Kubernetes Deployment

This directory contains Kubernetes manifests for deploying the platform to a GKE (or any Kubernetes) cluster. It uses Kustomize for base/overlay layering.

## Directory Structure

```
infra/k8s/
└── base/
    ├── kustomization.yaml         Kustomize resource list and image overrides
    ├── namespace.yaml             quant-alpha namespace
    ├── service-account.yaml       Workload Identity service account
    ├── configmap.yaml             Non-sensitive environment variables
    ├── secrets.yaml               Secret template (API keys, GCP credentials)
    ├── persistent-volume-claim.yaml  ReadWriteOnce PVC for DuckDB and Parquet
    ├── energy-cronjob.yaml        Hourly energy pipeline CronJob
    ├── equity-cronjob.yaml        Daily equity pipeline CronJob
    ├── dashboard-deployment.yaml  Streamlit dashboard Deployment
    └── dashboard-service.yaml     LoadBalancer service for the dashboard
```

## Workloads

| Resource | Kind | Schedule | Description |
|---|---|---|---|
| `energy-alpha-refresh` | CronJob | `15 * * * *` | Hourly energy pipeline |
| `equity-alpha-refresh` | CronJob | `0 6 * * 1-5` | Daily equity pipeline (weekdays) |
| `dashboard` | Deployment | Always-on | Streamlit research dashboard |

## Secrets

`secrets.yaml` is a placeholder. **Do not commit real credentials.**

In production, provision secrets via one of:

- [External Secrets Operator](https://external-secrets.io) pulling from GCP Secret Manager
- [Sealed Secrets](https://github.com/bitnami-labs/sealed-secrets) for GitOps-safe encrypted secrets
- Direct `kubectl create secret` before applying manifests

```bash
# Manual secret creation (development only)
kubectl create secret generic quant-alpha-secrets \
  --namespace quant-alpha \
  --from-literal=ENTSOE_API_KEY=your-token \
  --from-file=GCP_SA_KEY_JSON=path/to/key.json
```

## Applying Manifests

```bash
# Preview what will be applied
kubectl kustomize infra/k8s/base

# Apply to the current cluster context
kubectl apply -k infra/k8s/base

# Check status
kubectl -n quant-alpha get all
```

## Image References

The image tag is managed in `kustomization.yaml`:

```yaml
images:
  - name: quant-alpha-foundation
    newName: europe-west3-docker.pkg.dev/PROJECT_ID/second-foundation-containers/quant-alpha-foundation
    newTag: "0.1.0"
```

Update `newTag` to the SHA or version tag built by CI/CD:

```bash
kubectl kustomize infra/k8s/base | sed "s/0.1.0/${GITHUB_SHA}/g" | kubectl apply -f -
```

## Resource Budgets

| Container | CPU Request | CPU Limit | Memory Request | Memory Limit |
|---|---|---|---|---|
| energy-alpha | 500m | 2 | 1Gi | 4Gi |
| equity-alpha | 250m | 1 | 512Mi | 2Gi |
| dashboard | 100m | 500m | 256Mi | 1Gi |

## Persistent Storage

A single `ReadWriteOnce` PVC (`quant-alpha-data`) is mounted at `/app/data` in all job containers. This stores DuckDB files and Parquet outputs. For multi-node clusters, replace with a `ReadWriteMany` storage class (e.g., Filestore on GKE).

## Cluster Setup (GKE)

```bash
# Create cluster
gcloud container clusters create quant-alpha \
  --region europe-west3 \
  --num-nodes 2 \
  --machine-type e2-standard-4

# Configure kubectl
gcloud container clusters get-credentials quant-alpha --region europe-west3

# Create namespace and apply
kubectl apply -k infra/k8s/base
```
