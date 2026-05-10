output "lake_bucket_name" {
  description = "GCS data lake bucket name."
  value       = google_storage_bucket.quant_alpha_lake.name
}

output "bigquery_dataset_raw" {
  description = "BigQuery raw dataset ID."
  value       = google_bigquery_dataset.quant_alpha.dataset_id
}

output "bigquery_dataset_staging" {
  description = "BigQuery staging dataset ID."
  value       = google_bigquery_dataset.quant_alpha_staging.dataset_id
}

output "bigquery_dataset_marts" {
  description = "BigQuery marts dataset ID."
  value       = google_bigquery_dataset.quant_alpha_marts.dataset_id
}

output "pipeline_service_account" {
  description = "Email of the pipeline GCP service account."
  value       = google_service_account.pipeline_runner.email
}

output "artifact_registry_url" {
  description = "Full Artifact Registry URL for the Docker repository."
  value       = "${var.region}-docker.pkg.dev/${var.project_id}/${google_artifact_registry_repository.containers.repository_id}"
}

output "gke_cluster_name" {
  description = "GKE Autopilot cluster name."
  value       = google_container_cluster.autopilot.name
}

output "gke_get_credentials_cmd" {
  description = "Command to configure local kubectl for this cluster."
  value       = "gcloud container clusters get-credentials ${google_container_cluster.autopilot.name} --region ${var.region} --project ${var.project_id}"
}

output "workload_identity_provider" {
  description = "Workload Identity Provider resource name — used in GitHub Actions 'workload_identity_provider' secret."
  value       = google_iam_workload_identity_pool_provider.github.name
}

output "workload_identity_pool" {
  description = "Workload Identity Pool resource name."
  value       = google_iam_workload_identity_pool.main.name
}

output "entsoe_secret_name" {
  description = "Secret Manager secret name for the ENTSO-E API key."
  value       = google_secret_manager_secret.entsoe_api_key.name
}

output "helm_prod_image_repository" {
  description = "Image repository string to paste into values.prod.yaml."
  value       = "${var.region}-docker.pkg.dev/${var.project_id}/${google_artifact_registry_repository.containers.repository_id}/quant-alpha-foundation"
}

output "helm_prod_wi_annotation" {
  description = "Workload Identity annotation for the Helm SA in values.prod.yaml."
  value       = "iam.gke.io/gcp-service-account: ${google_service_account.pipeline_runner.email}"
}
