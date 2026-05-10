terraform {
  required_version = ">= 1.6.0"
  required_providers {
    google = {
      source  = "hashicorp/google"
      version = "~> 6.0"
    }
  }
}

provider "google" {
  project = var.project_id
  region  = var.region
}

# --------------------------------------------------------------------------- #
# GCS Data Lake
# --------------------------------------------------------------------------- #
resource "google_storage_bucket" "quant_alpha_lake" {
  name                        = "${var.project_id}-${var.bucket_suffix}"
  location                    = var.region
  uniform_bucket_level_access = true

  versioning {
    enabled = true
  }

  lifecycle_rule {
    condition { age = 30 }
    action {
      type          = "SetStorageClass"
      storage_class = "NEARLINE"
    }
  }

  lifecycle_rule {
    condition { age = 365 }
    action {
      type          = "SetStorageClass"
      storage_class = "COLDLINE"
    }
  }

  labels = {
    project = "second-foundation-quant"
    managed = "terraform"
  }
}

# GCS folders (objects) that define the lake structure
resource "google_storage_bucket_object" "lake_folders" {
  for_each = toset(["raw/equity/", "raw/energy/", "processed/equity/", "processed/energy/", "models/"])
  name     = each.value
  bucket   = google_storage_bucket.quant_alpha_lake.name
  content  = " "
}

# --------------------------------------------------------------------------- #
# BigQuery — multi-schema warehouse
# --------------------------------------------------------------------------- #
resource "google_bigquery_dataset" "quant_alpha" {
  dataset_id  = var.dataset_id
  description = "Second Foundation quant alpha research warehouse"
  location    = var.bigquery_location
  project     = var.project_id

  labels = {
    project = "second-foundation-quant"
    managed = "terraform"
  }
}

resource "google_bigquery_dataset" "quant_alpha_staging" {
  dataset_id  = "${var.dataset_id}_staging"
  description = "Staging layer — dbt intermediate models"
  location    = var.bigquery_location
  project     = var.project_id

  labels = {
    project = "second-foundation-quant"
    layer   = "staging"
    managed = "terraform"
  }
}

resource "google_bigquery_dataset" "quant_alpha_marts" {
  dataset_id  = "${var.dataset_id}_marts"
  description = "Mart layer — production-ready alpha panels and backtest results"
  location    = var.bigquery_location
  project     = var.project_id

  labels = {
    project = "second-foundation-quant"
    layer   = "marts"
    managed = "terraform"
  }
}

# Core mart tables — schema declared here so BQ enforces types before dbt runs
resource "google_bigquery_table" "alpha_diagnostics" {
  dataset_id = google_bigquery_dataset.quant_alpha_marts.dataset_id
  table_id   = "fct_alpha_diagnostics"
  project    = var.project_id

  deletion_protection = false

  schema = jsonencode([
    { name = "alpha_name", type = "STRING", mode = "REQUIRED" },
    { name = "oos_ic_mean", type = "FLOAT64", mode = "NULLABLE" },
    { name = "oos_ic_ir", type = "FLOAT64", mode = "NULLABLE" },
    { name = "consistency_score", type = "FLOAT64", mode = "NULLABLE" },
    { name = "gate_robustness", type = "BOOL", mode = "NULLABLE" },
    { name = "gate_uniqueness", type = "BOOL", mode = "NULLABLE" },
    { name = "gate_value_added", type = "BOOL", mode = "NULLABLE" },
    { name = "gate_consistency", type = "BOOL", mode = "NULLABLE" },
    { name = "gates_passed", type = "INT64", mode = "NULLABLE" },
    { name = "run_date", type = "DATE", mode = "NULLABLE" },
  ])

  labels = {
    managed = "terraform"
    layer   = "marts"
  }
}

resource "google_bigquery_table" "backtest_daily" {
  dataset_id = google_bigquery_dataset.quant_alpha_marts.dataset_id
  table_id   = "fct_backtest_daily"
  project    = var.project_id

  deletion_protection = false
  time_partitioning {
    type  = "DAY"
    field = "date"
  }
  clustering = ["alpha_name"]

  schema = jsonencode([
    { name = "date", type = "DATE", mode = "REQUIRED" },
    { name = "alpha_name", type = "STRING", mode = "REQUIRED" },
    { name = "daily_pnl", type = "FLOAT64", mode = "NULLABLE" },
    { name = "sharpe_ann", type = "FLOAT64", mode = "NULLABLE" },
    { name = "max_drawdown", type = "FLOAT64", mode = "NULLABLE" },
  ])

  labels = {
    managed = "terraform"
    layer   = "marts"
  }
}

# --------------------------------------------------------------------------- #
# Service Account
# --------------------------------------------------------------------------- #
resource "google_service_account" "pipeline_runner" {
  account_id   = "second-foundation-pipeline"
  display_name = "Second Foundation Pipeline Runner"
  description  = "Used by GKE pods and GitHub Actions CI via Workload Identity"
  project      = var.project_id
}

# --------------------------------------------------------------------------- #
# Artifact Registry
# --------------------------------------------------------------------------- #
resource "google_artifact_registry_repository" "containers" {
  location      = var.region
  repository_id = var.artifact_repository_id
  description   = "Container images for the Second Foundation semester project"
  format        = "DOCKER"
  project       = var.project_id

  labels = {
    project = "second-foundation-quant"
    managed = "terraform"
  }
}

# --------------------------------------------------------------------------- #
# GKE Autopilot Cluster
# --------------------------------------------------------------------------- #
resource "google_container_cluster" "autopilot" {
  name     = var.gke_cluster_name
  location = var.region
  project  = var.project_id

  enable_autopilot = true

  release_channel {
    channel = "REGULAR"
  }

  workload_identity_config {
    workload_pool = "${var.project_id}.svc.id.goog"
  }
}
