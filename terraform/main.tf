terraform {
  required_providers {
    google = {
      source  = "hashicorp/google"
      version = "~> 5.0"
    }
  }

  # REMOTE BACKEND CONFIGURATION
  # This stores your state in the cloud so a team can collaborate safely.
  # (Note: You must manually create this bucket in GCP first to hold the state file)
  backend "gcs" {
    bucket = "theta-sunlight-tf-state-bucket" 
    prefix = "terraform/olist-pipeline/state"
  }
}

# 1. Authenticate and define your project
provider "google" {
  project = var.project_id
  region  = var.region
}

# 2. Create the Data Lake (GCS Bucket)
resource "google_storage_bucket" "data_lake" {
  name          = var.bucket_name
  location      = "US"
  
  #I only allow force_destroy if i declare this is a 'dev' environment
  force_destroy = var.is_development 
  
  #Security policy to prevent accidental public data leaks
  public_access_prevention = "enforced"

  #Resource tagging for Cost Tracking (FinOps)
  labels = var.tags

  lifecycle_rule {
    condition {
      age = 30
    }
    action {
      type = "Delete"
    }
  }
}

# 3. Create the Data Warehouse (BigQuery Dataset)
resource "google_bigquery_dataset" "data_warehouse" {
  dataset_id                 = var.dataset_id
  project                    = var.project_id
  location                   = "US"
  delete_contents_on_destroy = var.is_development

  #Resource tagging for Cost Tracking
  labels = var.tags
}

# 4. Create a Service Account for the Data Pipeline
resource "google_service_account" "pipeline_runner" {
  account_id   = "ecommerce-pipeline-sa"
  display_name = "E-commerce Pipeline Service Account"
  description  = "Used by Prefect and dbt to securely run the automated data pipeline"
}

# 5. Grant precise IAM permissions on the Data Lake (GCS)
resource "google_storage_bucket_iam_member" "bucket_access" {
  bucket = google_storage_bucket.data_lake.name
  role   = "roles/storage.objectAdmin"
  member = "serviceAccount:${google_service_account.pipeline_runner.email}"
}

# 6. Grant precise IAM permissions on the Data Warehouse (BigQuery)
resource "google_bigquery_dataset_iam_member" "dataset_access" {
  dataset_id = google_bigquery_dataset.data_warehouse.dataset_id
  role       = "roles/bigquery.dataEditor"
  member     = "serviceAccount:${google_service_account.pipeline_runner.email}"
}