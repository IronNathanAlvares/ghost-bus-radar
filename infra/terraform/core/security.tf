# Guardrails on the data and an audit trail (roadmap Phase 0, steps 0.2 and 0.3).

# --- bucket policies ------------------------------------------------------------------------------

data "aws_iam_policy_document" "archive_bucket" {
  statement {
    sid       = "DenyInsecureTransport"
    effect    = "Deny"
    actions   = ["s3:*"]
    resources = [aws_s3_bucket.archive.arn, "${aws_s3_bucket.archive.arn}/*"]
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

  # The raw archive is the system of record (ADR-0001): nobody deletes from it, admins included.
  # Lifecycle rules still apply (S3 performs those itself). Removing data on purpose means
  # changing this policy first, which is a reviewed Terraform change and a CloudTrail event.
  statement {
    sid       = "DenyDeletingRawData"
    effect    = "Deny"
    actions   = ["s3:DeleteObject", "s3:DeleteObjectVersion"]
    resources = ["${aws_s3_bucket.archive.arn}/raw/*", "${aws_s3_bucket.archive.arn}/provenance/*"]
    principals {
      type        = "*"
      identifiers = ["*"]
    }
  }
}

resource "aws_s3_bucket_policy" "archive" {
  bucket = aws_s3_bucket.archive.id
  policy = data.aws_iam_policy_document.archive_bucket.json
}

data "aws_iam_policy_document" "work_bucket" {
  statement {
    sid       = "DenyInsecureTransport"
    effect    = "Deny"
    actions   = ["s3:*"]
    resources = [aws_s3_bucket.work.arn, "${aws_s3_bucket.work.arn}/*"]
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

resource "aws_s3_bucket_policy" "work" {
  bucket = aws_s3_bucket.work.id
  policy = data.aws_iam_policy_document.work_bucket.json
}

# --- CloudTrail -----------------------------------------------------------------------------------

resource "aws_s3_bucket" "trail" {
  bucket = "ghostbus-trail-${local.account}"
}

resource "aws_s3_bucket_public_access_block" "trail" {
  bucket                  = aws_s3_bucket.trail.id
  block_public_acls       = true
  block_public_policy     = true
  ignore_public_acls      = true
  restrict_public_buckets = true
}

resource "aws_s3_bucket_ownership_controls" "trail" {
  bucket = aws_s3_bucket.trail.id
  rule {
    object_ownership = "BucketOwnerEnforced"
  }
}

resource "aws_s3_bucket_server_side_encryption_configuration" "trail" {
  bucket = aws_s3_bucket.trail.id
  rule {
    apply_server_side_encryption_by_default {
      sse_algorithm = "AES256"
    }
  }
}

resource "aws_s3_bucket_lifecycle_configuration" "trail" {
  bucket = aws_s3_bucket.trail.id
  rule {
    id     = "keep-a-year"
    status = "Enabled"
    filter {}
    expiration {
      days = 365
    }
    noncurrent_version_expiration {
      noncurrent_days = 365
    }
    abort_incomplete_multipart_upload {
      days_after_initiation = 1
    }
  }
}

# Versioned, so an overwritten log file keeps its original (CloudTrail also signs digests).
resource "aws_s3_bucket_versioning" "trail" {
  bucket = aws_s3_bucket.trail.id
  versioning_configuration {
    status = "Enabled"
  }
}

locals {
  trail_name = "ghostbus-trail"
  trail_arn  = "arn:aws:cloudtrail:${var.region}:${local.account}:trail/${local.trail_name}"
}

data "aws_iam_policy_document" "trail_bucket" {
  statement {
    sid       = "CloudTrailAclCheck"
    actions   = ["s3:GetBucketAcl"]
    resources = [aws_s3_bucket.trail.arn]
    principals {
      type        = "Service"
      identifiers = ["cloudtrail.amazonaws.com"]
    }
    condition {
      test     = "StringEquals"
      variable = "aws:SourceArn"
      values   = [local.trail_arn]
    }
  }
  statement {
    sid       = "CloudTrailWrite"
    actions   = ["s3:PutObject"]
    resources = ["${aws_s3_bucket.trail.arn}/AWSLogs/${local.account}/*"]
    principals {
      type        = "Service"
      identifiers = ["cloudtrail.amazonaws.com"]
    }
    condition {
      test     = "StringEquals"
      variable = "aws:SourceArn"
      values   = [local.trail_arn]
    }
  }
  statement {
    sid       = "DenyInsecureTransport"
    effect    = "Deny"
    actions   = ["s3:*"]
    resources = [aws_s3_bucket.trail.arn, "${aws_s3_bucket.trail.arn}/*"]
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

resource "aws_s3_bucket_policy" "trail" {
  bucket = aws_s3_bucket.trail.id
  policy = data.aws_iam_policy_document.trail_bucket.json
}

# Management events (who created, changed or deleted what), in this region. The first copy of
# management events is free. Multi-region trails are not allowed on this account's plan.
resource "aws_cloudtrail" "main" {
  name                          = local.trail_name
  s3_bucket_name                = aws_s3_bucket.trail.id
  is_multi_region_trail         = false
  include_global_service_events = true
  enable_log_file_validation    = true

  depends_on = [aws_s3_bucket_policy.trail]
}

# --- tripwires --------------------------------------------------------------------------------------
# CloudTrail management events -> EventBridge -> the alerts email. Each one means "a human should
# look": expected right after a deploy you made, alarming otherwise. IAM and console sign-in events
# are global (logged in us-east-1), which this account's SCPs do not allow; see docs/07-security.md.

locals {
  tripwires = {
    secret-read = {
      description = "Someone other than the poller read the NTA API key"
      # Read-only events reach EventBridge only with this rule state.
      state = "ENABLED_WITH_ALL_CLOUDTRAIL_MANAGEMENT_EVENTS"
      pattern = {
        source        = ["aws.ssm"]
        "detail-type" = ["AWS API Call via CloudTrail"]
        detail = {
          eventSource = ["ssm.amazonaws.com"]
          eventName   = ["GetParameter", "GetParameters", "GetParameterHistory"]
          userIdentity = {
            arn = [{ "anything-but" = { prefix = "arn:aws:sts::${local.account}:assumed-role/ghostbus-poller/" } }]
          }
          "$or" = [
            { requestParameters = { name = [var.nta_key_parameter] } },
            { requestParameters = { names = [var.nta_key_parameter] } },
          ]
        }
      }
    }
    audit-trail = {
      description = "The audit trail was stopped, deleted or reconfigured"
      state       = "ENABLED"
      pattern = {
        source        = ["aws.cloudtrail"]
        "detail-type" = ["AWS API Call via CloudTrail"]
        detail = {
          eventName = ["StopLogging", "DeleteTrail", "UpdateTrail", "PutEventSelectors", "PutInsightSelectors"]
        }
      }
    }
    bucket-guardrails = {
      description = "A bucket policy, versioning or public-access setting changed on a ghostbus bucket"
      state       = "ENABLED"
      pattern = {
        source        = ["aws.s3"]
        "detail-type" = ["AWS API Call via CloudTrail"]
        detail = {
          eventName = [
            "PutBucketPolicy", "DeleteBucketPolicy", "PutBucketVersioning",
            "PutBucketPublicAccessBlock", "DeleteBucketPublicAccessBlock",
          ]
          requestParameters = { bucketName = [{ prefix = "ghostbus-" }] }
        }
      }
    }
  }
}

resource "aws_cloudwatch_event_rule" "tripwire" {
  for_each      = local.tripwires
  name          = "ghostbus-tripwire-${each.key}"
  description   = each.value.description
  state         = each.value.state
  event_pattern = jsonencode(each.value.pattern)
}

resource "aws_cloudwatch_event_target" "tripwire" {
  for_each = local.tripwires
  rule     = aws_cloudwatch_event_rule.tripwire[each.key].name
  arn      = aws_sns_topic.alerts.arn

  input_transformer {
    input_paths = {
      event  = "$.detail.eventName"
      who    = "$.detail.userIdentity.arn"
      ip     = "$.detail.sourceIPAddress"
      agent  = "$.detail.userAgent"
      time   = "$.time"
      region = "$.region"
    }
    input_template = jsonencode(
      "TRIPWIRE ${each.key}: ${each.value.description}. <event> by <who> from <ip> (<agent>) at <time> in <region>. Expected if you just deployed or ran this yourself; otherwise rotate the CLI key and check CloudTrail (docs/07-security.md)."
    )
  }
}
