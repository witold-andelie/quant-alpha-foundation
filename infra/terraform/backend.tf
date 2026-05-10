terraform {
  # Remote state stored in GCS — prevents local state drift across team members and CI.
  # Bootstrap: create the bucket manually ONCE before `terraform init`:
  #   gsutil mb -l europe-west3 gs://<project_id>-tf-state
  #   gsutil versioning set on gs://<project_id>-tf-state
  backend "gcs" {
    bucket = "REPLACE_PROJECT_ID-tf-state"
    prefix = "quant-alpha/state"
  }
}
