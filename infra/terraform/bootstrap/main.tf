# Applied once per account, with local state: the bucket that holds every other layer's state,
# and the monthly budget alert. Run this before anything else costs money.
#
#   cd infra/terraform/bootstrap
#   terraform init && terraform apply -var alert_email=you@example.com

terraform {
  required_version = ">= 1.10"
  required_providers {
    aws = {
      source  = "hashicorp/aws"
      version = "~> 6.0"
    }
  }
}

provider "aws" {
  region = var.region
  default_tags {
    tags = {
      project    = "ghost-bus-radar"
      managed_by = "terraform"
      layer      = "bootstrap"
    }
  }
}

variable "region" {
  type    = string
  default = "eu-north-1"
}

variable "alert_email" {
  type        = string
  description = "Where budget alerts go."
}

variable "monthly_budget_usd" {
  type        = number
  default     = 20
  description = "Alerts at 25%, 50% and 100% of this (actual), and when the forecast passes it."
}

data "aws_caller_identity" "me" {}

resource "aws_s3_bucket" "state" {
  bucket = "ghostbus-tfstate-${data.aws_caller_identity.me.account_id}"
  lifecycle {
    prevent_destroy = true
  }
}

resource "aws_s3_bucket_versioning" "state" {
  bucket = aws_s3_bucket.state.id
  versioning_configuration {
    status = "Enabled"
  }
}

resource "aws_s3_bucket_server_side_encryption_configuration" "state" {
  bucket = aws_s3_bucket.state.id
  rule {
    apply_server_side_encryption_by_default {
      sse_algorithm = "AES256"
    }
  }
}

resource "aws_s3_bucket_public_access_block" "state" {
  bucket                  = aws_s3_bucket.state.id
  block_public_acls       = true
  block_public_policy     = true
  ignore_public_acls      = true
  restrict_public_buckets = true
}

resource "aws_s3_bucket_ownership_controls" "state" {
  bucket = aws_s3_bucket.state.id
  rule {
    object_ownership = "BucketOwnerEnforced"
  }
}

data "aws_iam_policy_document" "state" {
  statement {
    sid       = "DenyInsecureTransport"
    effect    = "Deny"
    actions   = ["s3:*"]
    resources = [aws_s3_bucket.state.arn, "${aws_s3_bucket.state.arn}/*"]
    principals {
      type        = "*"
      identifiers = ["*"]
    }
    condition {
      test     = "Bool"
      variable = "aws:SecureTransport"
      values   = ["false"]
    }
  }
}

resource "aws_s3_bucket_policy" "state" {
  bucket = aws_s3_bucket.state.id
  policy = data.aws_iam_policy_document.state.json
}

resource "aws_s3_bucket_lifecycle_configuration" "state" {
  bucket = aws_s3_bucket.state.id
  rule {
    id     = "old-state-versions"
    status = "Enabled"
    filter {}
    noncurrent_version_expiration {
      noncurrent_days = 90
    }
    abort_incomplete_multipart_upload {
      days_after_initiation = 1
    }
  }
}

# One budget (the first two per account are free).
resource "aws_budgets_budget" "monthly" {
  name         = "ghostbus-monthly"
  budget_type  = "COST"
  limit_amount = tostring(var.monthly_budget_usd)
  limit_unit   = "USD"
  time_unit    = "MONTHLY"

  dynamic "notification" {
    for_each = [25, 50, 100]
    content {
      comparison_operator        = "GREATER_THAN"
      threshold                  = notification.value
      threshold_type             = "PERCENTAGE"
      notification_type          = "ACTUAL"
      subscriber_email_addresses = [var.alert_email]
    }
  }

  notification {
    comparison_operator        = "GREATER_THAN"
    threshold                  = 100
    threshold_type             = "PERCENTAGE"
    notification_type          = "FORECASTED"
    subscriber_email_addresses = [var.alert_email]
  }
}

output "state_bucket" {
  value = aws_s3_bucket.state.bucket
}

output "backend_hcl" {
  description = "Paste into infra/terraform/core/backend.hcl"
  value       = "bucket = \"${aws_s3_bucket.state.bucket}\"\nregion = \"${var.region}\"\n"
}
