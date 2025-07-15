provider "aws" {
  allowed_account_ids = ["532553470754"] //Dev-Connection-Team
  region              = "eu-central-1"
  profile             = "Keboola-Dev-Connection-Team-AWSAdministratorAccess"
}

data "aws_region" "current" {}
data "aws_caller_identity" "current" {}

output "AWS_REGION" {
  value = data.aws_region.current.id
}

output "AWS_ACCESS_KEY_ID" {
  value = aws_iam_access_key.UserAccessKey.id
}

output "AWS_SECRET_ACCESS_KEY" {
  value     = aws_iam_access_key.UserAccessKey.secret
  sensitive = true
}

// file storage
output "S3FileStorageRegion" {
  value = data.aws_region.current.id
}

output "FileStorageBucket" {
  value = aws_s3_bucket.S3FilesBucket.bucket
}

output "S3FileStorageAwsKey" {
  value = aws_iam_access_key.S3FileStorageUserAccessKey.id
}

output "S3FileStorageAwsSecret" {
  value     = aws_iam_access_key.S3FileStorageUserAccessKey.secret
  sensitive = true
}
