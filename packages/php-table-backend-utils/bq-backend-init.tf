terraform {
  required_providers {
    google = {
      source  = "hashicorp/google"
      version = "4.49.0"
    }
  }
}

provider "google" {
  # Configuration options
}

variable "folder_id" {
  type = string
}

variable "backend_prefix" {
  type = string
}

variable "billing_account_id" {
  type = string
}

locals {
  backend_folder_display_name = "${var.backend_prefix}-bq-backend-utils"
  service_project_name = "${var.backend_prefix}-bq-backend-utils"
  service_project_id = "${var.backend_prefix}-bq-backend-utils"
  service_account_id = "${var.backend_prefix}-main-service-acc"
}

variable services {
  type        = list
  default     = [
    "cloudresourcemanager.googleapis.com",
    "serviceusage.googleapis.com",
    "iam.googleapis.com",
    "bigquery.googleapis.com"
  ]
}

data "google_folder" "storage_backend_folder" {
  folder = "folders/${var.folder_id}"
}

resource "google_project" "service_project_in_a_folder" {
  name       = local.service_project_name
  project_id = local.service_project_id
  folder_id  =  data.google_folder.storage_backend_folder.folder_id
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
  description = "Service account to managing keboola backend projects"
  project = google_project.service_project_in_a_folder.project_id
}

resource "google_folder_iam_binding" "folder_service_acc_project_creator_role" {
  folder  = data.google_folder.storage_backend_folder.folder_id
  role    = "roles/resourcemanager.projectCreator"

  members = [
    "serviceAccount:${google_service_account.service_account.email}",
  ]
}

resource "google_project_iam_binding" "service_acc_project_owner" {
  project  = google_project.service_project_in_a_folder.name
  role    = "roles/owner"

  members = [
    "serviceAccount:${google_service_account.service_account.email}",
  ]
}

resource "google_service_account_key" "key_principal" {
  service_account_id = google_service_account.service_account.name
}

resource "local_file" "key_principal" {
  content  = base64decode(google_service_account_key.key_principal.private_key)
  filename = "big_query_key.json"
}

output "service_project_id" {
  value = google_project.service_project_in_a_folder.project_id
}
