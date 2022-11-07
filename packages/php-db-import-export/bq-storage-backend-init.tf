terraform {
  required_providers {
    google = {
      source = "hashicorp/google"
      version = "4.0"
    }
  }
}

provider "google" {
  # Configuration options
}

variable "organization_id" {
  type = string
}

variable "backend_prefix" {
  type = string
}

variable "billing_account_id" {
  type = string
}

locals {
  backend_folder_display_name = "${var.backend_prefix}-bq-import-export"
  service_project_name = "${var.backend_prefix}-bq-import-export"
  service_file_project_name = "${var.backend_prefix}-bq-file-import-export"
  service_project_id = "${var.backend_prefix}-bq-import-export"
  service_file_project_id = "${var.backend_prefix}-bq-file-import-export"
  service_account_id = "${var.backend_prefix}-main-service-acc"
}

variable services {
  type        = list
  default     = [
    "bigquery.googleapis.com"
  ]
}

resource "google_folder" "storage_backend_folder" {
  display_name = local.backend_folder_display_name
  parent       = "organizations/${var.organization_id}"
}

resource "google_project" "service_project_in_a_folder" {
  name       = local.service_project_name
  project_id = local.service_project_id
  folder_id  = google_folder.storage_backend_folder.id
  billing_account = var.billing_account_id
}

resource "google_project" "service_file_project_in_a_folder" {
  name       = local.service_file_project_name
  project_id = local.service_file_project_id
  folder_id  = google_folder.storage_backend_folder.id
  billing_account = var.billing_account_id
}

resource "google_project_service" "services" {
  for_each = toset(var.services)
  project                    = google_project.service_project_in_a_folder.project_id
  service                    = each.key
  disable_dependent_services = false
  disable_on_destroy         = false
  depends_on = [google_project.service_project_in_a_folder]
}

resource "google_service_account" "service_account" {
  account_id = local.service_account_id
  project = google_project.service_project_in_a_folder.project_id
}

resource "google_project_iam_binding" "project_service_acc_owner" {
  project  = google_project.service_project_in_a_folder.project_id
  role    = "roles/owner"

  members = [
    "serviceAccount:${google_service_account.service_account.email}",
  ]
}

output "folder_id" {
  value = google_folder.storage_backend_folder.id
}

output "service_project_id" {
  value = google_project.service_project_in_a_folder.project_id
}

resource "google_storage_bucket" "kbc_file_storage_backend" {
  name = "${var.backend_prefix}-files-bucket"
  project = google_project.service_file_project_in_a_folder.name
  location = "US"
  storage_class = "STANDARD"
  # public_access_prevention = "enforced" - not available yet
  versioning {
    enabled = false
  }
  uniform_bucket_level_access = true
}

output "file_storage_bucket_id" {
  value = google_storage_bucket.kbc_file_storage_backend.id
}

resource "google_service_account" "gcp_file_storage_service_account" {
  account_id = "${var.backend_prefix}-file-storage"
  display_name = "${var.backend_prefix} File Storage Service Account"
  project = google_project.service_file_project_in_a_folder.name
}

resource "google_storage_bucket_iam_member" "member_creator_fs_bucket" {
  bucket = google_storage_bucket.kbc_file_storage_backend.name
  role = "roles/storage.objectAdmin"
  member = "serviceAccount:${google_service_account.gcp_file_storage_service_account.email}"
}