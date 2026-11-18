# The lake (ADR-0010): S3 Tables (Iceberg) written by nightly Glue jobs from the raw archive,
# queried with Athena through the Glue Data Catalog's federated s3tablescatalog.
#
#   uv run python infra/glue/build_jobs.py     # wheels + pinned modules, before plan/apply

locals {
  table_bucket_name = "ghostbus-lake"
  work_bucket       = "ghostbus-work-${local.account}"
  glue_dist         = "${path.module}/../../glue/dist"
  wheels            = sort(fileset(local.glue_dist, "*.whl"))
  pinned_modules    = compact(split("\n", trimspace(file("${local.glue_dist}/modules.txt"))))
}

# --- tables ---------------------------------------------------------------------------------------

resource "aws_s3tables_table_bucket" "lake" {
  name = local.table_bucket_name
}

# IAM-mode integration: Athena and Glue see every table bucket in this account and region as
# s3tablescatalog/<bucket>, with access decided by IAM alone (no Lake Formation grants).
resource "aws_glue_catalog" "s3tables" {
  name                                  = "s3tablescatalog"
  allow_full_table_external_data_access = "True"

  federated_catalog {
    identifier      = "arn:aws:s3tables:${var.region}:${local.account}:bucket/*"
    connection_name = "aws:s3tables"
  }

  create_database_default_permissions {
    permissions = ["ALL"]
    principal {
      data_lake_principal_identifier = "IAM_ALLOWED_PRINCIPALS"
    }
  }

  create_table_default_permissions {
    permissions = ["ALL"]
    principal {
      data_lake_principal_identifier = "IAM_ALLOWED_PRINCIPALS"
    }
  }
}

# --- work bucket: Glue script, wheels and temp files; Athena results ----------------------------

resource "aws_s3_bucket" "work" {
  bucket        = local.work_bucket
  force_destroy = true # nothing here is irreplaceable: all of it is rebuilt or re-derivable
}

resource "aws_s3_bucket_server_side_encryption_configuration" "work" {
  bucket = aws_s3_bucket.work.id
  rule {
    apply_server_side_encryption_by_default {
      sse_algorithm = "AES256"
    }
    bucket_key_enabled = true
  }
}

resource "aws_s3_bucket_public_access_block" "work" {
  bucket                  = aws_s3_bucket.work.id
  block_public_acls       = true
  block_public_policy     = true
  ignore_public_acls      = true
  restrict_public_buckets = true
}

resource "aws_s3_bucket_ownership_controls" "work" {
  bucket = aws_s3_bucket.work.id
  rule {
    object_ownership = "BucketOwnerEnforced"
  }
}

resource "aws_s3_bucket_lifecycle_configuration" "work" {
  bucket = aws_s3_bucket.work.id
  rule {
    id     = "scratch"
    status = "Enabled"
    filter {
      prefix = "tmp/"
    }
    expiration {
      days = 7
    }
  }
  rule {
    id     = "athena-results"
    status = "Enabled"
    filter {
      prefix = "athena/"
    }
    expiration {
      days = 7
    }
  }
  rule {
    id     = "multipart"
    status = "Enabled"
    filter {}
    abort_incomplete_multipart_upload {
      days_after_initiation = 1
    }
  }
}

resource "aws_s3_object" "glue_entry" {
  bucket = aws_s3_bucket.work.id
  key    = "glue/entry.py"
  source = "${path.module}/../../glue/entry.py"
  etag   = filemd5("${path.module}/../../glue/entry.py")
}

resource "aws_s3_object" "wheel" {
  for_each = toset(local.wheels)
  bucket   = aws_s3_bucket.work.id
  key      = "glue/wheels/${each.value}"
  source   = "${local.glue_dist}/${each.value}"
  etag     = filemd5("${local.glue_dist}/${each.value}")
}

# --- Glue jobs ------------------------------------------------------------------------------------

data "aws_iam_policy_document" "glue_assume" {
  statement {
    actions = ["sts:AssumeRole"]
    principals {
      type        = "Service"
      identifiers = ["glue.amazonaws.com"]
    }
  }
}

resource "aws_iam_role" "glue" {
  name               = "ghostbus-glue"
  assume_role_policy = data.aws_iam_policy_document.glue_assume.json
}

# No AWSGlueServiceRole: that AWS-managed policy grants glue:*, broad S3 and EC2 access. The jobs
# need only what is listed in data.aws_iam_policy_document.glue (verified with a full Glue run).

data "aws_iam_policy_document" "glue" {
  statement {
    sid       = "ReadArchive"
    actions   = ["s3:ListBucket", "s3:GetObject", "s3:GetBucketLocation"]
    resources = [aws_s3_bucket.archive.arn, "${aws_s3_bucket.archive.arn}/raw/*"]
  }
  statement {
    sid       = "SealArchiveDays"
    actions   = ["s3:GetObject", "s3:PutObject"]
    resources = ["${aws_s3_bucket.archive.arn}/provenance/*"]
  }
  statement {
    sid = "GlueJobLogs"
    actions = [
      "logs:CreateLogGroup",
      "logs:CreateLogStream",
      "logs:PutLogEvents",
      "logs:AssociateKmsKey",
    ]
    resources = ["arn:aws:logs:${var.region}:${local.account}:log-group:/aws-glue/*"]
  }
  statement {
    sid       = "GlueJobMetrics"
    actions   = ["cloudwatch:PutMetricData"]
    resources = ["*"]
    condition {
      test     = "StringEquals"
      variable = "cloudwatch:namespace"
      values   = ["Glue"]
    }
  }
  statement {
    sid       = "WorkBucket"
    actions   = ["s3:ListBucket", "s3:GetObject", "s3:PutObject", "s3:DeleteObject", "s3:GetBucketLocation"]
    resources = [aws_s3_bucket.work.arn, "${aws_s3_bucket.work.arn}/*"]
  }
  statement {
    sid       = "PublishWebsiteData"
    actions   = ["s3:ListBucket", "s3:GetObject", "s3:PutObject"]
    resources = [aws_s3_bucket.site.arn, "${aws_s3_bucket.site.arn}/data/*"]
  }
  statement {
    sid       = "DailySummaryEmail"
    actions   = ["sns:Publish"]
    resources = [aws_sns_topic.alerts.arn]
  }
  statement {
    sid       = "BatchCostInTheSummary"
    actions   = ["glue:GetJobRuns"]
    resources = ["arn:aws:glue:${var.region}:${local.account}:job/ghostbus-*"]
  }
  statement {
    sid     = "LakeTables"
    actions = ["s3tables:*"]
    resources = [
      aws_s3tables_table_bucket.lake.arn,
      "${aws_s3tables_table_bucket.lake.arn}/*",
    ]
  }
}

resource "aws_iam_role_policy" "glue" {
  role   = aws_iam_role.glue.id
  policy = data.aws_iam_policy_document.glue.json
}

locals {
  glue_default_arguments = {
    "--job-language"                     = "python"
    "--datalake-formats"                 = "iceberg"
    "--conf"                             = "spark.sql.extensions=org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions"
    "--additional-python-modules"        = join(",", concat([for w in local.wheels : "s3://${aws_s3_bucket.work.bucket}/glue/wheels/${w}"], local.pinned_modules))
    "--python-modules-installer-option"  = "--no-deps"
    "--enable-continuous-cloudwatch-log" = "true"
    "--TempDir"                          = "s3://${aws_s3_bucket.work.bucket}/tmp/glue/"
    "--LAKE_MODE"                        = "s3tables"
    "--LAKE_TABLE_BUCKET_ARN"            = aws_s3tables_table_bucket.lake.arn
    "--AWS_REGION"                       = var.region
    "--ARCHIVE_URI"                      = "s3://${aws_s3_bucket.archive.bucket}/"
    # Executors cannot read the driver's /tmp: unzipped timetables are staged here for Spark.
    "--STATIC_STAGE_URI" = "s3://${aws_s3_bucket.work.bucket}/tmp/static/"
    "--ALERTS_TOPIC_ARN" = aws_sns_topic.alerts.arn
    "--EXPORT_URI"       = "s3://${aws_s3_bucket.site.bucket}/data/v1/"
  }
  glue_jobs = {
    timetable_load = "Load new static GTFS versions from the archive into lake.dim (SCD2)"
    reconcile      = "Final trip outcomes for yesterday from the raw archive (backfill: --ARGS)"
    quality        = "Data-quality checks for yesterday into gold.data_quality, and the summary email"
    export         = "Public JSON for the website (data/v1), only from days that passed the quality gates"
  }
}

# Glue writes temporary and output files with S3 server-side encryption. CloudWatch-log and
# bookmark encryption would need a customer KMS key (about $1 a month each); logs hold no secrets
# and bookmarks are unused.
resource "aws_glue_security_configuration" "jobs" {
  name = "ghostbus-jobs"
  encryption_configuration {
    cloudwatch_encryption {
      cloudwatch_encryption_mode = "DISABLED"
    }
    job_bookmarks_encryption {
      job_bookmarks_encryption_mode = "DISABLED"
    }
    s3_encryption {
      s3_encryption_mode = "SSE-S3"
    }
  }
}

resource "aws_glue_job" "batch" {
  for_each          = local.glue_jobs
  name              = "ghostbus-${replace(each.key, "_", "-")}"
  description       = each.value
  role_arn          = aws_iam_role.glue.arn
  glue_version      = "5.1"
  worker_type       = "G.1X"
  number_of_workers = var.glue_workers
  execution_class   = "FLEX" # spare capacity at about two thirds of the price; may start later
  timeout           = 60
  max_retries       = 0

  security_configuration = aws_glue_security_configuration.jobs.name

  command {
    name            = "glueetl"
    python_version  = "3"
    script_location = "s3://${aws_s3_bucket.work.bucket}/${aws_s3_object.glue_entry.key}"
  }

  default_arguments = merge(local.glue_default_arguments, { "--TASK" = each.key })

  depends_on = [aws_s3_object.wheel, aws_iam_role_policy.glue]
}

# Nightly: new static versions first (NTA uploads around 23:30 UTC), then yesterday's outcomes.
# Yesterday's last trips (GTFS times past 24:00) close by about 03:00 UTC.
resource "aws_glue_trigger" "nightly" {
  name     = "ghostbus-nightly"
  type     = "SCHEDULED"
  schedule = "cron(15 4 * * ? *)"
  enabled  = var.batch_enabled

  actions {
    job_name = aws_glue_job.batch["timetable_load"].name
  }
}

resource "aws_glue_trigger" "after_timetable" {
  name    = "ghostbus-after-timetable"
  type    = "CONDITIONAL"
  enabled = var.batch_enabled

  predicate {
    conditions {
      job_name = aws_glue_job.batch["timetable_load"].name
      state    = "SUCCEEDED"
    }
  }

  actions {
    job_name = aws_glue_job.batch["reconcile"].name
  }
}

resource "aws_glue_trigger" "after_reconcile" {
  name    = "ghostbus-after-reconcile"
  type    = "CONDITIONAL"
  enabled = var.batch_enabled

  predicate {
    conditions {
      job_name = aws_glue_job.batch["reconcile"].name
      state    = "SUCCEEDED"
    }
  }

  actions {
    job_name = aws_glue_job.batch["quality"].name
  }
}

resource "aws_glue_trigger" "after_quality" {
  name    = "ghostbus-after-quality"
  type    = "CONDITIONAL"
  enabled = var.batch_enabled

  predicate {
    conditions {
      job_name = aws_glue_job.batch["quality"].name
      state    = "SUCCEEDED"
    }
  }

  actions {
    job_name = aws_glue_job.batch["export"].name
  }
}

# A failed nightly job emails the alerts topic.
resource "aws_cloudwatch_event_rule" "glue_failed" {
  name        = "ghostbus-glue-failed"
  description = "A Ghost Bus Radar Glue job failed, timed out or was stopped"
  event_pattern = jsonencode({
    source      = ["aws.glue"]
    detail-type = ["Glue Job State Change"]
    detail = {
      jobName = [for j in aws_glue_job.batch : j.name]
      state   = ["FAILED", "TIMEOUT", "ERROR"]
    }
  })
}

resource "aws_cloudwatch_event_target" "glue_failed" {
  rule = aws_cloudwatch_event_rule.glue_failed.name
  arn  = aws_sns_topic.alerts.arn
}

data "aws_iam_policy_document" "alerts_topic" {
  statement {
    sid       = "EventBridgePublish"
    actions   = ["sns:Publish"]
    resources = [aws_sns_topic.alerts.arn]
    principals {
      type        = "Service"
      identifiers = ["events.amazonaws.com"]
    }
    condition {
      test     = "ArnEquals"
      variable = "aws:SourceArn"
      values = concat(
        [aws_cloudwatch_event_rule.glue_failed.arn],
        [for r in aws_cloudwatch_event_rule.tripwire : r.arn],
      )
    }
  }
  statement {
    sid       = "CloudWatchAlarms"
    actions   = ["sns:Publish"]
    resources = [aws_sns_topic.alerts.arn]
    principals {
      type        = "Service"
      identifiers = ["cloudwatch.amazonaws.com"]
    }
    condition {
      test     = "StringEquals"
      variable = "aws:SourceAccount"
      values   = [local.account]
    }
  }
}

resource "aws_sns_topic_policy" "alerts" {
  arn    = aws_sns_topic.alerts.arn
  policy = data.aws_iam_policy_document.alerts_topic.json
}

# --- Athena ---------------------------------------------------------------------------------------

resource "aws_athena_workgroup" "ghostbus" {
  name          = "ghostbus"
  description   = "Queries over the lake; at most 1 GB scanned per query (about half a cent)"
  force_destroy = true

  configuration {
    enforce_workgroup_configuration    = true
    publish_cloudwatch_metrics_enabled = false
    bytes_scanned_cutoff_per_query     = 1073741824

    result_configuration {
      output_location = "s3://${aws_s3_bucket.work.bucket}/athena/"
      encryption_configuration {
        encryption_option = "SSE_S3"
      }
    }
  }
}

output "table_bucket_arn" {
  value = aws_s3tables_table_bucket.lake.arn
}

output "glue_jobs" {
  value = [for j in aws_glue_job.batch : j.name]
}

# --- public website data (roadmap 2.1, ADR-0011) -----------------------------------------------------
# Private: the website (roadmap 2.2) will serve it through CloudFront with origin access control.
# Not versioned: everything here is rebuilt nightly from the lake.

resource "aws_s3_bucket" "site" {
  bucket = "ghostbus-site-${local.account}"
}

resource "aws_s3_bucket_public_access_block" "site" {
  bucket                  = aws_s3_bucket.site.id
  block_public_acls       = true
  block_public_policy     = true
  ignore_public_acls      = true
  restrict_public_buckets = true
}

resource "aws_s3_bucket_ownership_controls" "site" {
  bucket = aws_s3_bucket.site.id
  rule {
    object_ownership = "BucketOwnerEnforced"
  }
}

resource "aws_s3_bucket_server_side_encryption_configuration" "site" {
  bucket = aws_s3_bucket.site.id
  rule {
    apply_server_side_encryption_by_default {
      sse_algorithm = "AES256"
    }
    bucket_key_enabled = true
  }
}

resource "aws_s3_bucket_lifecycle_configuration" "site" {
  bucket = aws_s3_bucket.site.id
  rule {
    id     = "multipart"
    status = "Enabled"
    filter {}
    abort_incomplete_multipart_upload {
      days_after_initiation = 1
    }
  }
}

data "aws_iam_policy_document" "site_bucket" {
  statement {
    sid       = "DenyInsecureTransport"
    effect    = "Deny"
    actions   = ["s3:*"]
    resources = [aws_s3_bucket.site.arn, "${aws_s3_bucket.site.arn}/*"]
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

resource "aws_s3_bucket_policy" "site" {
  bucket = aws_s3_bucket.site.id
  policy = data.aws_iam_policy_document.site_bucket.json
}

output "site_data_uri" {
  value = "s3://${aws_s3_bucket.site.bucket}/data/v1/"
}
