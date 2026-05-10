# ---------------------------------------------------------------------------
# Secret Manager — stores runtime secrets, eliminating K8s Secret YAML files
# with base64 values in the repo.
# ---------------------------------------------------------------------------

resource "google_secret_manager_secret" "entsoe_api_key" {
  secret_id = "entsoe-api-key"
  project   = var.project_id

  replication {
    auto {}
  }

  labels = {
    project = "second-foundation-quant"
    managed = "terraform"
  }
}

resource "google_secret_manager_secret" "gcp_sa_key" {
  secret_id = "pipeline-sa-key"
  project   = var.project_id

  replication {
    auto {}
  }

  labels = {
    project = "second-foundation-quant"
    managed = "terraform"
  }
}

# Grant the pipeline SA access to read its own secrets
resource "google_secret_manager_secret_iam_member" "entsoe_key_access" {
  secret_id = google_secret_manager_secret.entsoe_api_key.id
  role      = "roles/secretmanager.secretAccessor"
  member    = "serviceAccount:${google_service_account.pipeline_runner.email}"
  project   = var.project_id
}

resource "google_secret_manager_secret_iam_member" "sa_key_access" {
  secret_id = google_secret_manager_secret.gcp_sa_key.id
  role      = "roles/secretmanager.secretAccessor"
  member    = "serviceAccount:${google_service_account.pipeline_runner.email}"
  project   = var.project_id
}

# Grant GitHub Actions SA (CI) access to push secret versions (e.g., rotate key)
resource "google_secret_manager_secret_iam_member" "github_entsoe_access" {
  secret_id = google_secret_manager_secret.entsoe_api_key.id
  role      = "roles/secretmanager.secretVersionAdder"
  member    = "serviceAccount:${google_service_account.pipeline_runner.email}"
  project   = var.project_id
}
