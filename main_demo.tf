terraform {
  required_providers {
    google = {
      source  = "hashicorp/google"
      version = "6.17.0"
    }
  }
}

provider "google" {
  credentials = "./conf/gcp_secrets.json"
  project     = "dtc-de-448913"
  region      = "europe-central12"
}

resource "google_storage_bucket" "rolirad-bucket" {
  name          = "dtc-de-448913-rolirad-bucket"
  location      = "EU"
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

