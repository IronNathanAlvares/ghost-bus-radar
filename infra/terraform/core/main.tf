# Always-on, cheap layer (ADR-0009): the raw archive and the collector Lambdas.
#
#   uv run python infra/lambda/build_collector.py
#   cd infra/terraform/core
#   terraform init -backend-config=backend.hcl
#   terraform apply -var alert_email=you@example.com                       # schedules disabled
#   terraform apply -var alert_email=you@example.com -var collector_enabled=true   # after the laptop poller is stopped

terraform {
  required_version = ">= 1.10"
  required_providers {
    aws = {
      source  = "hashicorp/aws"
      version = "~> 6.0"
    }
  }
  backend "s3" {
    key          = "core/terraform.tfstate"
    use_lockfile = true
    encrypt      = true
  }
}

provider "aws" {
  region = var.region
  default_tags {
    tags = {
      project    = "ghost-bus-radar"
      managed_by = "terraform"
      layer      = "core"
    }
  }
}

data "aws_caller_identity" "me" {}

locals {
  account        = data.aws_caller_identity.me.account_id
  archive_bucket = "ghostbus-archive-${local.account}"
  nta_key_arn    = "arn:aws:ssm:${var.region}:${local.account}:parameter${var.nta_key_parameter}"
  collector_zip  = "${path.module}/../../lambda/dist/collector.zip"
}

output "archive_bucket" {
  value = aws_s3_bucket.archive.bucket
}

output "archive_uri" {
  value = "s3://${aws_s3_bucket.archive.bucket}/"
}

output "poller_function" {
  value = aws_lambda_function.poller.function_name
}

output "static_watcher_function" {
  value = aws_lambda_function.static_watcher.function_name
}

output "alerts_topic" {
  value = aws_sns_topic.alerts.arn
}
