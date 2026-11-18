# The collector: the GTFS-R poller (every minute) and the static watcher (hourly), outside any
# VPC so there is no NAT to pay for. Archive only; Kafka is a burst-mode extra (ADR-0009).

data "aws_iam_policy_document" "lambda_assume" {
  statement {
    actions = ["sts:AssumeRole"]
    principals {
      type        = "Service"
      identifiers = ["lambda.amazonaws.com"]
    }
  }
}

# --- poller --------------------------------------------------------------------------------------

resource "aws_cloudwatch_log_group" "poller" {
  name              = "/aws/lambda/ghostbus-poller"
  retention_in_days = var.log_retention_days
}

resource "aws_iam_role" "poller" {
  name               = "ghostbus-poller"
  assume_role_policy = data.aws_iam_policy_document.lambda_assume.json
}

data "aws_iam_policy_document" "poller" {
  statement {
    sid       = "ListArchive"
    actions   = ["s3:ListBucket"]
    resources = [aws_s3_bucket.archive.arn]
  }
  statement {
    sid     = "ReadWriteSnapshotsHeartbeatsClaims"
    actions = ["s3:GetObject", "s3:PutObject"]
    resources = [
      "${aws_s3_bucket.archive.arn}/raw/*",
      "${aws_s3_bucket.archive.arn}/claims/*",
    ]
  }
  statement {
    sid       = "NtaKey"
    actions   = ["ssm:GetParameter"]
    resources = [local.nta_key_arn]
  }
  statement {
    sid       = "Logs"
    actions   = ["logs:CreateLogStream", "logs:PutLogEvents"]
    resources = ["${aws_cloudwatch_log_group.poller.arn}:*"]
  }
}

resource "aws_iam_role_policy" "poller" {
  role   = aws_iam_role.poller.id
  policy = data.aws_iam_policy_document.poller.json
}

resource "aws_lambda_function" "poller" {
  function_name    = "ghostbus-poller"
  description      = "One NTA GTFS-R call per minute into the raw archive (ADR-0005)"
  role             = aws_iam_role.poller.arn
  handler          = "ghostbus_ingest.lambda_handlers.poll"
  runtime          = "python3.11"
  architectures    = ["arm64"]
  filename         = local.collector_zip
  source_code_hash = filebase64sha256(local.collector_zip)
  memory_size      = 256
  # Up to ~60 s waiting for the planned second, plus the call (NTA client timeout) and writes.
  timeout = 100

  environment {
    variables = {
      ARCHIVE_URI       = "s3://${aws_s3_bucket.archive.bucket}/"
      NTA_KEY_PARAMETER = var.nta_key_parameter
      POLL_OFFSET_S     = tostring(var.poll_offset_s)
    }
  }

  logging_config {
    log_format = "Text"
    log_group  = aws_cloudwatch_log_group.poller.name
  }

  depends_on = [aws_iam_role_policy.poller]
}

# --- static watcher ------------------------------------------------------------------------------

resource "aws_cloudwatch_log_group" "static_watcher" {
  name              = "/aws/lambda/ghostbus-static-watcher"
  retention_in_days = var.log_retention_days
}

resource "aws_iam_role" "static_watcher" {
  name               = "ghostbus-static-watcher"
  assume_role_policy = data.aws_iam_policy_document.lambda_assume.json
}

data "aws_iam_policy_document" "static_watcher" {
  statement {
    sid       = "ListArchive"
    actions   = ["s3:ListBucket"]
    resources = [aws_s3_bucket.archive.arn]
  }
  statement {
    sid       = "ReadWriteStaticVersions"
    actions   = ["s3:GetObject", "s3:PutObject"]
    resources = ["${aws_s3_bucket.archive.arn}/raw/feed=static/*"]
  }
  statement {
    sid       = "Logs"
    actions   = ["logs:CreateLogStream", "logs:PutLogEvents"]
    resources = ["${aws_cloudwatch_log_group.static_watcher.arn}:*"]
  }
}

resource "aws_iam_role_policy" "static_watcher" {
  role   = aws_iam_role.static_watcher.id
  policy = data.aws_iam_policy_document.static_watcher.json
}

resource "aws_lambda_function" "static_watcher" {
  function_name    = "ghostbus-static-watcher"
  description      = "Archives each new static GTFS version by SHA-256 (hourly HEAD check)"
  role             = aws_iam_role.static_watcher.arn
  handler          = "ghostbus_ingest.lambda_handlers.watch_static"
  runtime          = "python3.11"
  architectures    = ["arm64"]
  filename         = local.collector_zip
  source_code_hash = filebase64sha256(local.collector_zip)
  memory_size      = 512
  timeout          = 600

  # The static zip is about 100 MB; it is downloaded to /tmp only when the headers change.
  ephemeral_storage {
    size = 1024
  }

  environment {
    variables = {
      ARCHIVE_URI = "s3://${aws_s3_bucket.archive.bucket}/"
    }
  }

  logging_config {
    log_format = "Text"
    log_group  = aws_cloudwatch_log_group.static_watcher.name
  }

  depends_on = [aws_iam_role_policy.static_watcher]
}

# --- schedules -----------------------------------------------------------------------------------

data "aws_iam_policy_document" "scheduler_assume" {
  statement {
    actions = ["sts:AssumeRole"]
    principals {
      type        = "Service"
      identifiers = ["scheduler.amazonaws.com"]
    }
    condition {
      test     = "StringEquals"
      variable = "aws:SourceAccount"
      values   = [local.account]
    }
  }
}

resource "aws_iam_role" "scheduler" {
  name               = "ghostbus-scheduler"
  assume_role_policy = data.aws_iam_policy_document.scheduler_assume.json
}

data "aws_iam_policy_document" "scheduler" {
  statement {
    actions = ["lambda:InvokeFunction"]
    resources = [
      aws_lambda_function.poller.arn,
      aws_lambda_function.static_watcher.arn,
    ]
  }
}

resource "aws_iam_role_policy" "scheduler" {
  role   = aws_iam_role.scheduler.id
  policy = data.aws_iam_policy_document.scheduler.json
}

resource "aws_scheduler_schedule" "poller" {
  name                = "ghostbus-poller"
  description         = "Start of every minute; the function waits for its planned second"
  schedule_expression = "cron(* * * * ? *)"
  state               = var.collector_enabled ? "ENABLED" : "DISABLED"

  flexible_time_window {
    mode = "OFF"
  }

  target {
    arn      = aws_lambda_function.poller.arn
    role_arn = aws_iam_role.scheduler.arn
    # A retried invocation would land in a later minute, which has its own invocation.
    retry_policy {
      maximum_retry_attempts       = 0
      maximum_event_age_in_seconds = 60
    }
  }
}

resource "aws_scheduler_schedule" "static_watcher" {
  name                = "ghostbus-static-watcher"
  description         = "Hourly HEAD check of the static GTFS zip"
  schedule_expression = "cron(23 * * * ? *)"
  state               = var.collector_enabled ? "ENABLED" : "DISABLED"

  flexible_time_window {
    mode = "OFF"
  }

  target {
    arn      = aws_lambda_function.static_watcher.arn
    role_arn = aws_iam_role.scheduler.arn
    retry_policy {
      maximum_retry_attempts       = 2
      maximum_event_age_in_seconds = 1800
    }
  }
}
