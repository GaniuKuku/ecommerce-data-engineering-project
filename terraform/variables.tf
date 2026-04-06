variable "project_id" {
  description = "The ID of the Google Cloud project"
  type        = string
}

variable "region" {
  description = "The default compute region"
  type        = string
  default     = "us-central1"
}

variable "bucket_name" {
  description = "The name of the GCS Data Lake bucket"
  type        = string
}

variable "dataset_id" {
  description = "The name of the BigQuery Dataset"
  type        = string
  default     = "ecommerce_dw"
}

variable "is_development" {
  description = "Set to true to allow Terraform to forcefully delete buckets/datasets"
  type        = bool
  default     = false
}

variable "tags" {
  description = "Resource tags for FinOps and tracking"
  type        = map(string)
  default = {
    environment = "dev"
    project     = "olist_pipeline"
    owner       = "data_engineering_team"
  }
}