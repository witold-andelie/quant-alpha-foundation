# Terraform IaC — GCP Infrastructure

Manages all GCP resources for the Quant Alpha Foundation platform using Terraform. Covers the zoomcamp IaC requirement: declarative cloud infrastructure, remote state, least-privilege IAM, and Workload Identity for keyless authentication.

## Resources Managed

| File | Resources |
|---|---|
| `main.tf` | GCS lake, BigQuery datasets (raw/staging/marts) + mart tables, Service Account, Artifact Registry, GKE Autopilot |
| `workload_identity.tf` | WI Pool, GitHub Actions OIDC provider, K8s pod WI binding |
| `secrets.tf` | Secret Manager secrets (ENTSO-E key, SA key), access IAM |
| `iam.tf` | All IAM role bindings for the pipeline SA (BQ, GCS, AR, GKE, Secrets) |
| `backend.tf` | Remote state in GCS |
| `variables.tf` | All input variables with descriptions |
| `outputs.tf` | Actionable outputs: bucket name, WI provider, Helm annotations |

---

## Architecture

```
GitHub Actions CI ──OIDC──► WI Pool ──impersonate──► pipeline-sa@project.iam
                                                           │
GKE pod (quant-alpha-runner) ──WI Federation──────────────┘
                                                           │
                    ┌──────────────────────────────────────┤
                    ▼          ▼           ▼               ▼
               GCS bucket   BigQuery   Artifact       Secret Manager
               (data lake)  (warehouse) Registry      (ENTSO-E key)
```

**Workload Identity** eliminates long-lived service account keys:
- CI/CD: GitHub Actions uses OIDC tokens — no key JSON in secrets
- GKE pods: K8s SA `quant-alpha-runner` is bound to the GCP SA via WI Federation

---

## Quick Start

### 1. Bootstrap remote state bucket (once)

```bash
export PROJECT_ID=your-gcp-project-id

gsutil mb -l europe-west3 gs://${PROJECT_ID}-tf-state
gsutil versioning set on gs://${PROJECT_ID}-tf-state
```

### 2. Configure variables

```bash
cp terraform.tfvars.example terraform.tfvars
# Edit terraform.tfvars with your project_id and github_repo
```

### 3. Authenticate and apply

```bash
gcloud auth application-default login

terraform init
terraform plan
terraform apply
```

### 4. Wire up CI secrets (GitHub)

After `terraform apply`, copy the outputs into GitHub repository secrets:

```bash
terraform output workload_identity_provider   # → GCP_WIF_PROVIDER secret
terraform output pipeline_service_account     # → GCP_SA_EMAIL secret
echo $PROJECT_ID                              # → GCP_PROJECT_ID secret
```

### 5. Wire up Helm prod values

```bash
terraform output helm_prod_image_repository   # → values.prod.yaml image.repository
terraform output helm_prod_wi_annotation      # → values.prod.yaml serviceAccount.annotations
```

### 6. Configure kubectl

```bash
$(terraform output -raw gke_get_credentials_cmd)
```

---

## BigQuery Schema

Three datasets form the warehouse layers:

| Dataset | Purpose | dbt layer |
|---|---|---|
| `second_foundation_quant` | Raw ingested data | sources |
| `second_foundation_quant_staging` | Cleaned, typed models | staging |
| `second_foundation_quant_marts` | Production alpha panels | marts |

BQ table schemas are declared in Terraform (`fct_alpha_diagnostics`, `fct_backtest_daily`) with partitioning and clustering to enforce structure before dbt writes.

---

## CI/CD Integration

The `terraform-validate` CI job runs on every PR:
1. `terraform fmt -check` — enforces formatting
2. `terraform init -backend=false` + `terraform validate` — always runs (no GCP needed)
3. `terraform plan` — runs only when `GCP_PROJECT_ID` secret is set, posts plan summary to the PR

`terraform apply` is intentionally **not** automated — apply is a manual step to prevent accidental infrastructure changes from PRs.

---

## Workload Identity — How It Works

### GitHub Actions → GCP

```yaml
# In CI workflow (already configured):
- uses: google-github-actions/auth@v2
  with:
    workload_identity_provider: ${{ secrets.GCP_WIF_PROVIDER }}
    service_account: ${{ secrets.GCP_SA_EMAIL }}
```

GitHub's OIDC token is exchanged for a short-lived GCP access token via the WI Pool. No JSON key required.

### GKE Pods → GCP

```yaml
# In Helm values.prod.yaml (already configured):
serviceAccount:
  annotations:
    iam.gke.io/gcp-service-account: second-foundation-pipeline@PROJECT.iam.gserviceaccount.com
```

After `terraform apply`, annotate the K8s SA:

```bash
kubectl annotate serviceaccount quant-alpha-runner \
  -n quant-alpha \
  iam.gke.io/gcp-service-account=$(terraform output -raw pipeline_service_account)
```

---

## Useful Commands

```bash
# Preview changes
terraform plan

# Apply (interactive confirmation required)
terraform apply

# Destroy all resources (DESTRUCTIVE — loses all data)
terraform destroy

# Show all outputs
terraform output

# Format all .tf files
terraform fmt -recursive

# Check state
terraform state list
```
