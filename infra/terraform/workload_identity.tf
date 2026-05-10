# ---------------------------------------------------------------------------
# Workload Identity — keyless authentication for two principals:
#   1. GitHub Actions CI/CD  (OIDC token → GCP SA)
#   2. GKE pods              (K8s SA → GCP SA via WI Federation)
# ---------------------------------------------------------------------------

# Shared pool used by both GitHub Actions and GKE pod identity
resource "google_iam_workload_identity_pool" "main" {
  workload_identity_pool_id = "quant-alpha-wi-pool"
  display_name              = "Quant Alpha WI Pool"
  description               = "Federated identities for CI and GKE pods"
  project                   = var.project_id
}

# --------------------------------------------------------------------------- #
# GitHub Actions OIDC provider
# --------------------------------------------------------------------------- #
resource "google_iam_workload_identity_pool_provider" "github" {
  workload_identity_pool_id          = google_iam_workload_identity_pool.main.workload_identity_pool_id
  workload_identity_pool_provider_id = "github-provider"
  display_name                       = "GitHub Actions OIDC"
  project                            = var.project_id

  attribute_mapping = {
    "google.subject"       = "assertion.sub"
    "attribute.repository" = "assertion.repository"
    "attribute.ref"        = "assertion.ref"
  }

  # Only allow tokens from the specific GitHub repo
  attribute_condition = "attribute.repository == '${var.github_repo}'"

  oidc {
    issuer_uri = "https://token.actions.githubusercontent.com"
  }
}

# Allow GitHub Actions jobs on this repo to impersonate the pipeline SA
resource "google_service_account_iam_member" "github_actions_impersonation" {
  service_account_id = google_service_account.pipeline_runner.name
  role               = "roles/iam.workloadIdentityUser"
  member             = "principalSet://iam.googleapis.com/${google_iam_workload_identity_pool.main.name}/attribute.repository/${var.github_repo}"
}

# --------------------------------------------------------------------------- #
# GKE Pod Workload Identity
# Binds the Kubernetes ServiceAccount (quant-alpha/quant-alpha-runner)
# to the GCP ServiceAccount so pods get short-lived GCP credentials without
# storing a key file in a Secret.
# --------------------------------------------------------------------------- #
resource "google_service_account_iam_member" "gke_pod_identity" {
  service_account_id = google_service_account.pipeline_runner.name
  role               = "roles/iam.workloadIdentityUser"
  member             = "serviceAccount:${var.project_id}.svc.id.goog[quant-alpha/quant-alpha-runner]"
}

# The GKE Autopilot cluster must have Workload Identity enabled (it is by default).
# After terraform apply, annotate the K8s SA:
#   kubectl annotate serviceaccount quant-alpha-runner \
#     -n quant-alpha \
#     iam.gke.io/gcp-service-account=<pipeline_runner_email>
# The Helm prod values already carry this annotation — just substitute the email.
