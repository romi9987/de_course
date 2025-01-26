terraform {
  required_providers {
    google = {
      source  = "hashicorp/google"
      version = "6.17.0"
    }
  }
}

provider "google" {
  credentials = file(var.credentials)
  project     = var.project
  region      = var.region
}

resource "google_storage_bucket" "rolirad-bucket" {
  name          = var.gcs_bucket_name
  location      = var.location
  force_destroy = true

#   lifecycle_rule {
#     condition {
#       age = 3 #in days
#     }
#     action {
#       type = "Delete" #delete in 3 days
#     }
#   }

  lifecycle_rule {
    condition {
      age = 1
    }
    action {
      type = "AbortIncompleteMultipartUpload" #if you upload a very large file, it will abort it after 1 day
    }
  }
}

resource "google_bigquery_dataset" "demo_dataset" {
  dataset_id                  = var.bg_dataset_name
  location                    = var.location
  }