variable "credentials" {
  description = "GCP Credentials"
  default = "./conf/gcp_secrets.json"
}

variable "project" {
  description = "Project Name"
  default = "dtc-de-448913"
}

variable "region" {
  description = "Project Region"
  default = "europe-central12"
}


variable "location" {
  description = "Project Location"
  default = "EU"
}


variable "bg_dataset_name" {
  description = "My BigQuery Dataset Name"
  default = "demo_dataset"
}

variable "gcs_bucket_name" {
  description = "My Storage Bucket Name"
  default = "dtc-de-448913-rolirad-bucket"
}

variable "gsc_storage_class" {
  description = "Bucket Storage Class"
  default = "STANDARD"
}