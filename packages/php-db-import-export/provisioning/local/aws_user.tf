resource "aws_iam_user" "KbcServiceUser" {
  name = local.serviceName
}

resource "aws_iam_access_key" "UserAccessKey" {
  user = aws_iam_user.KbcServiceUser.name
}

data "aws_iam_policy_document" "ElasticSearchSnapshotRolePolicy" {
  statement {
    effect = "Allow"
    principals {
      identifiers = ["es.amazonaws.com"]
      type        = "Service"
    }
    actions = [
      "sts:AssumeRole"
    ]
  }
}
