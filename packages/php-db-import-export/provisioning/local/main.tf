terraform {
  required_providers {
    aws = {
      source  = "hashicorp/aws"
      version = "~> 3.74"
    }

    azurerm = {
      source  = "hashicorp/azurerm"
      version = "~> 3.0.0"
    }

    azuread = {
      source  = "hashicorp/azuread"
      version = "~> 2.24"
    }

    google = {
      source  = "hashicorp/google"
      version = "~> 4.0"
    }
  }
}

variable "name_prefix" {
  type = string
}

locals {
  serviceName = "${var.name_prefix}-php-db-import-export"
}

output "KEBOOLA_STORAGE_API__CLIENT_DB_PREFIX" {
  value = var.name_prefix
}
