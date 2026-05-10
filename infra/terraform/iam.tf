# ---------------------------------------------------------------------------
# IAM — all role bindings for the pipeline service account.
# Consolidated here so permissions are auditable in one place.
# ---------------------------------------------------------------------------

locals {
  pipeline_sa = "serviceAccount:${google_service_account.pipeline_runner.email}"
}

# BigQuery
resource "google_project_iam_member" "bq_data_editor" {
  project = var.project_id
  role    = "roles/bigquery.dataEditor"
  member  = local.pipeline_sa
}

resource "google_project_iam_member" "bq_job_user" {
  project = var.project_id
  role    = "roles/bigquery.jobUser"
  member  = local.pipeline_sa
}

# GCS — raw data lake
resource "google_storage_bucket_iam_member" "lake_object_admin" {
  bucket = google_storage_bucket.quant_alpha_lake.name
  role   = "roles/storage.objectAdmin"
  member = local.pipeline_sa
}

# Artifact Registry — pull images in GKE
resource "google_project_iam_member" "artifact_reader" {
  project = var.project_id
  role    = "roles/artifactregistry.reader"
  member  = local.pipeline_sa
}

# Artifact Registry — push images from CI
resource "google_project_iam_member" "artifact_writer" {
  project = var.project_id
  role    = "roles/artifactregistry.writer"
  member  = local.pipeline_sa
}

# Secret Manager — read secrets at runtime
resource "google_project_iam_member" "secret_accessor" {
  project = var.project_id
  role    = "roles/secretmanager.secretAccessor"
  member  = local.pipeline_sa
}

# GKE — deploy workloads from CI
resource "google_project_iam_member" "gke_developer" {
  project = var.project_id
  role    = "roles/container.developer"
  member  = local.pipeline_sa
}

# Workload Identity — allow SA to be impersonated via WI
resource "google_project_iam_member" "workload_identity_user" {
  project = var.project_id
  role    = "roles/iam.workloadIdentityUser"
  member  = local.pipeline_sa
}
