terraform {
  required_providers {
    aws = {
      source  = "hashicorp/aws"
      version = "~> 3.74"
    }
  }
}

variable "name_prefix" {
  type = string
}

locals {
  serviceName = "${var.name_prefix}-php-storage-driver-snowflake"
}

output "KEBOOLA_STORAGE_API__CLIENT_DB_PREFIX" {
  value = var.name_prefix
}
