terraform {
  required_providers {
    google = {
      source  = "hashicorp/google"
      version = "~> 5.0"
    }
  }
}

# 1. Authenticate and define your project
provider "google" {
  project = "theta-sunlight-491314-n8"
  region  = "us-central1" # You can change this to your preferred GCP region
}

# 2. Create the Data Lake (GCS Bucket)
resource "google_storage_bucket" "data_lake" {
  name          = "ecommerce-datalake-gk"
  location      = "US"
  force_destroy = true # Allows Terraform to delete the bucket later even if it has data inside

  lifecycle_rule {
    condition {
      age = 30 # Optional: Automatically deletes files older than 30 days to save money
    }
    action {
      type = "Delete"
    }
  }
}

# 3. Create the Data Warehouse (BigQuery Dataset)
resource "google_bigquery_dataset" "data_warehouse" {
  dataset_id                 = "ecommerce_dw"
  project                    = "theta-sunlight-491314-n8"
  location                   = "US"
  delete_contents_on_destroy = true
}