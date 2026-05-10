variable "project_id" {
  type        = string
  description = "GCP project ID."
}

variable "region" {
  type        = string
  default     = "europe-west3"
  description = "Primary GCP region (Frankfurt)."
}

variable "bigquery_location" {
  type        = string
  default     = "EU"
  description = "BigQuery dataset location (multi-region EU)."
}

variable "dataset_id" {
  type        = string
  default     = "second_foundation_quant"
  description = "Base BigQuery dataset ID. Staging and mart datasets are derived from this."
}

variable "bucket_suffix" {
  type        = string
  default     = "second-foundation-lake"
  description = "GCS bucket name suffix (full name = project_id-suffix)."
}

variable "artifact_repository_id" {
  type        = string
  default     = "second-foundation-containers"
  description = "Artifact Registry repository name for Docker images."
}

variable "gke_cluster_name" {
  type        = string
  default     = "second-foundation-quant"
  description = "GKE Autopilot cluster name."
}

variable "github_repo" {
  type        = string
  description = "GitHub repo in owner/name format (e.g. 'andelie1892/quant-alpha-foundation'). Used to scope the Workload Identity binding."
}
