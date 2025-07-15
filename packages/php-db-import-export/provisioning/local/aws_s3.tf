// File storage
resource "aws_s3_bucket" "S3FilesBucket" {
  bucket = "${local.serviceName}-s3-files-storage-bucket"

  tags = {
    Name = "keboola-file-storage"
  }

  force_destroy = true
}

resource "aws_s3_bucket_cors_configuration" "S3FilesBucketCorsConfiguration" {
  bucket = aws_s3_bucket.S3FilesBucket.bucket

  cors_rule {
    allowed_headers = ["*"]
    allowed_methods = [
      "GET",
      "PUT",
      "POST",
      "DELETE"
    ]
    allowed_origins = ["*"]
    max_age_seconds = 3600
  }
}

resource "aws_s3_bucket_lifecycle_configuration" "S3FilesBucketLifecycleConfig" {
  bucket = aws_s3_bucket.S3FilesBucket.bucket
  rule {
    id = "After 30 days IA, 180 days to glacier and 270 delete"
    filter {
      prefix = "exp-180"
    }

    expiration {
      days = 270
    }

    transition {
      storage_class = "STANDARD_IA"
      days          = 30
    }

    transition {
      storage_class = "GLACIER"
      days          = 180
    }

    status = "Enabled"
  }

  rule {
    id     = "Delete after 30 days"
    status = "Enabled"

    filter {
      prefix = "exp-30"
    }

    expiration {
      days = 30
    }
  }

  rule {
    id     = "Delete after 15 days"
    status = "Enabled"

    filter {
      prefix = "exp-15"
    }

    expiration {
      days = 15
    }
  }

  rule {
    id     = "Delete after 48 hours"
    status = "Enabled"

    filter {
      prefix = "exp-2"
    }
    expiration {
      days = 2
    }
  }

  rule {
    id     = "Delete incomplete multipart uploads"
    status = "Enabled"

    abort_incomplete_multipart_upload {
      days_after_initiation = 7
    }
  }
}

resource "aws_iam_user" "S3FileStorageUser" {
  name = "${local.serviceName}-s3-file-storage-user"

  path = "/"
}

data "aws_iam_policy_document" "S3AccessDocument" {
  statement {
    effect = "Allow"
    actions = [
      "s3:*"
    ]
    resources = [
      "${aws_s3_bucket.S3FilesBucket.arn}/*"
    ]
  }

  statement {
    sid = "AllowListingOfUserFolder"
    actions = [
      "s3:ListBucket",
      "s3:GetBucketLocation"
    ]
    effect = "Allow"
    resources = [
      aws_s3_bucket.S3FilesBucket.arn
    ]
  }
}

data "aws_iam_policy_document" "STSAccessDocument" {
  statement {
    effect = "Allow"
    actions = [
      "sts:GetFederationToken"
    ]

    resources = ["*"]
  }
}

resource "aws_iam_user_policy" "S3Access" {
  name   = "S3Access"
  user   = aws_iam_user.S3FileStorageUser.name
  policy = data.aws_iam_policy_document.S3AccessDocument.json
}

resource "aws_iam_user_policy" "STSAccess" {
  name   = "STSAccess"
  user   = aws_iam_user.S3FileStorageUser.name
  policy = data.aws_iam_policy_document.STSAccessDocument.json
}

resource "aws_iam_access_key" "S3FileStorageUserAccessKey" {
  user = aws_iam_user.S3FileStorageUser.name
}
