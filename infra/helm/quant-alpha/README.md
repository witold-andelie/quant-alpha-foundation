# Helm Chart — quant-alpha

Production-ready Helm chart for the Quant Alpha Foundation platform. Deploys the Streamlit research dashboard plus equity and energy ingestion CronJobs into any Kubernetes cluster.

## Chart Structure

```
infra/helm/quant-alpha/
├── Chart.yaml              ← chart metadata (name, version, appVersion)
├── values.yaml             ← default values (prod-like baseline)
├── values.dev.yaml         ← development overlay (local cluster, low resources)
├── values.prod.yaml        ← production overlay (GKE, GCP Artifact Registry, HPA)
└── templates/
    ├── _helpers.tpl        ← shared template helpers (labels, image ref, volume blocks)
    ├── namespace.yaml
    ├── serviceaccount.yaml
    ├── configmap.yaml
    ├── secret.yaml         ← Opaque secret with helm.sh/resource-policy: keep
    ├── pvc.yaml            ← PVC with helm.sh/resource-policy: keep (never deleted on uninstall)
    ├── deployment.yaml     ← Streamlit dashboard (replicas, liveness/readiness probes)
    ├── service.yaml        ← ClusterIP (prod) or NodePort (dev)
    ├── cronjob-energy.yaml ← hourly energy ingestion (schedule configurable per env)
    ├── cronjob-equity.yaml ← daily equity ingestion (--offline flag togglable)
    ├── hpa.yaml            ← HorizontalPodAutoscaler (enabled only in prod overlay)
    └── NOTES.txt           ← post-install instructions rendered by helm install
```

---

## Quick Start

### Prerequisites

```bash
# Install Helm (macOS)
brew install helm

# Verify cluster access
kubectl cluster-info
```

### Development (local Kind / Docker Desktop)

```bash
# 1. Build the image locally
docker build -t quant-alpha-foundation:latest .

# 2. Install with dev values
helm upgrade --install quant-alpha infra/helm/quant-alpha/ \
  -f infra/helm/quant-alpha/values.dev.yaml \
  --namespace quant-alpha \
  --create-namespace \
  --set image.repository=quant-alpha-foundation \
  --set image.tag=latest \
  --set image.pullPolicy=IfNotPresent

# 3. Access the dashboard
kubectl port-forward -n quant-alpha svc/quant-alpha-dashboard 8501:8501
open http://localhost:8501
```

### Production (GKE)

```bash
# Pass the GIT SHA as image tag and secrets as --set flags (or use Vault/External Secrets)
helm upgrade --install quant-alpha infra/helm/quant-alpha/ \
  -f infra/helm/quant-alpha/values.prod.yaml \
  --namespace quant-alpha \
  --create-namespace \
  --set image.tag=$GIT_SHA \
  --set "secrets.data.ENTSOE_API_KEY=$(base64 -w0 <<< "$ENTSOE_API_KEY")"
```

---

## Environment Overlays

| Setting | Dev | Prod |
|---|---|---|
| Image | `quant-alpha-foundation:latest` (local) | GCP Artifact Registry + `$GIT_SHA` tag |
| Pull policy | `Never` | `Always` |
| Dashboard replicas | 1 | 2 (min) with HPA up to 5 |
| Service type | `NodePort` | `LoadBalancer` |
| PVC size | 5 Gi | 50 Gi (`standard-rwo` StorageClass) |
| Energy CronJob | every 30 min | every 60 min at :15 |
| Equity CronJob | every hour, offline | 23:00 UTC Mon–Fri, live yfinance |
| Resource limits (energy) | 500m CPU / 1 Gi | 4 CPU / 8 Gi |
| Workload Identity | — | `iam.gke.io/gcp-service-account` annotation |

---

## Key Design Decisions

### `helm.sh/resource-policy: keep`

The PVC and Secret carry this annotation, so `helm uninstall` **never** deletes your data volume or credentials — you must manually remove them if you truly want a clean slate.

### Secrets handling

By default, `secrets.create=true` creates an Opaque Secret from `values.yaml` (empty placeholders). For production:
- Set `secrets.external=true` and `secrets.secretName=my-external-secret` to point at an externally-managed secret (e.g., from External Secrets Operator / GCP Secret Manager).
- Or pass values via `--set secrets.data.ENTSOE_API_KEY=<base64>` in CI.

### CronJob `offline` flag

`equityCronJob.offline: true` (dev default) adds `--offline` to the equity pipeline command, using deterministic synthetic prices instead of live yfinance. Set to `false` in prod values.

---

## Useful Commands

```bash
# Check release status
helm status quant-alpha -n quant-alpha

# Inspect computed values for a release
helm get values quant-alpha -n quant-alpha

# Upgrade with new image tag
helm upgrade quant-alpha infra/helm/quant-alpha/ \
  -f infra/helm/quant-alpha/values.prod.yaml \
  --set image.tag=abc1234

# Render templates locally (no cluster needed)
helm template quant-alpha infra/helm/quant-alpha/ \
  -f infra/helm/quant-alpha/values.dev.yaml

# Lint the chart
helm lint infra/helm/quant-alpha/ -f infra/helm/quant-alpha/values.dev.yaml

# Uninstall (PVC and Secret are preserved due to resource-policy annotation)
helm uninstall quant-alpha -n quant-alpha

# View all resources under the release
kubectl get all -n quant-alpha -l app.kubernetes.io/instance=quant-alpha
```

---

## Relationship to Kustomize Base

This Helm chart replaces the static Kustomize manifests in `infra/k8s/base/` with a fully-parameterised release. The Kustomize base is preserved as reference but is **not** the primary deployment mechanism.

| Concern | Kustomize base | Helm chart |
|---|---|---|
| Environment overlays | Manual patches | `values.dev.yaml` / `values.prod.yaml` |
| Resource defaults | Hard-coded | `values.yaml` |
| Image tag management | `kustomization.yaml` `images:` block | `--set image.tag=…` |
| Rollback | `kubectl apply` re-apply | `helm rollback quant-alpha <revision>` |
| Release history | None | `helm history quant-alpha -n quant-alpha` |
