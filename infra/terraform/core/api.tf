# "Will my bus come?" (roadmap 2.3, ADR-0012): one Lambda behind an HTTP API. It reads only S3:
# the stop index, the newest archived live snapshots and the routes' published history. It never
# calls the NTA (one poller per API key) and cannot read the NTA key.

resource "aws_cloudwatch_log_group" "api" {
  name              = "/aws/lambda/ghostbus-api"
  retention_in_days = var.log_retention_days
}

resource "aws_cloudwatch_log_group" "api_access" {
  name              = "/aws/apigateway/ghostbus-api"
  retention_in_days = var.log_retention_days
}

resource "aws_iam_role" "api" {
  name               = "ghostbus-api"
  assume_role_policy = data.aws_iam_policy_document.lambda_assume.json
}

# Read-only, and only the exact prefixes it needs. ListBucket is limited by prefix too: without
# it a missing key reads as AccessDenied instead of "not found".
data "aws_iam_policy_document" "api" {
  statement {
    sid       = "ListLiveSnapshots"
    actions   = ["s3:ListBucket"]
    resources = [aws_s3_bucket.archive.arn]
    condition {
      test     = "StringLike"
      variable = "s3:prefix"
      values   = ["raw/feed=trip_updates/*", "raw/feed=vehicles/*"]
    }
  }
  statement {
    sid     = "ReadLiveSnapshots"
    actions = ["s3:GetObject"]
    resources = [
      "${aws_s3_bucket.archive.arn}/raw/feed=trip_updates/*",
      "${aws_s3_bucket.archive.arn}/raw/feed=vehicles/*",
    ]
  }
  statement {
    sid       = "ListStopIndex"
    actions   = ["s3:ListBucket"]
    resources = [aws_s3_bucket.work.arn]
    condition {
      test     = "StringLike"
      variable = "s3:prefix"
      values   = ["api/v1/*"]
    }
  }
  statement {
    sid       = "ReadStopIndex"
    actions   = ["s3:GetObject"]
    resources = ["${aws_s3_bucket.work.arn}/api/v1/*"]
  }
  statement {
    sid       = "ListRouteHistory"
    actions   = ["s3:ListBucket"]
    resources = [aws_s3_bucket.site.arn]
    condition {
      test     = "StringLike"
      variable = "s3:prefix"
      values   = ["data/v1/routes/*", "data/v1/model.json"]
    }
  }
  statement {
    sid       = "ReadRouteHistory"
    actions   = ["s3:GetObject"]
    resources = ["${aws_s3_bucket.site.arn}/data/v1/routes/*", "${aws_s3_bucket.site.arn}/data/v1/model.json"]
  }
  statement {
    sid       = "Logs"
    actions   = ["logs:CreateLogStream", "logs:PutLogEvents"]
    resources = ["${aws_cloudwatch_log_group.api.arn}:*"]
  }
}

resource "aws_iam_role_policy" "api" {
  role   = aws_iam_role.api.id
  policy = data.aws_iam_policy_document.api.json
}

resource "aws_lambda_function" "api" {
  function_name    = "ghostbus-api"
  description      = "Will my bus come? Next departures at a stop with a probability (ADR-0012)"
  role             = aws_iam_role.api.arn
  handler          = "ghostbus_api.handler.handler"
  runtime          = "python3.11"
  architectures    = ["arm64"]
  filename         = local.api_zip
  source_code_hash = filebase64sha256(local.api_zip)
  memory_size      = 512 # more memory is more CPU: faster cold starts and decoding
  timeout          = 10

  environment {
    variables = {
      INDEX_URI     = "s3://${aws_s3_bucket.work.bucket}/api/v1/"
      ARCHIVE_URI   = "s3://${aws_s3_bucket.archive.bucket}/"
      SITE_DATA_URI = "s3://${aws_s3_bucket.site.bucket}/data/v1/"
    }
  }

  logging_config {
    log_format = "Text"
    log_group  = aws_cloudwatch_log_group.api.name
  }

  depends_on = [aws_iam_role_policy.api]
}

resource "aws_apigatewayv2_api" "api" {
  name          = "ghostbus-api"
  description   = "Ghost Bus Radar public API (ADR-0012)"
  protocol_type = "HTTP"

  # Public, read-only data: any website may call it, with GET only and no credentials.
  cors_configuration {
    allow_origins = ["*"]
    allow_methods = ["GET"]
    max_age       = 3600
  }
}

resource "aws_apigatewayv2_integration" "api" {
  api_id                 = aws_apigatewayv2_api.api.id
  integration_type       = "AWS_PROXY"
  integration_uri        = aws_lambda_function.api.invoke_arn
  payload_format_version = "2.0"
  timeout_milliseconds   = 10000
}

resource "aws_apigatewayv2_route" "api" {
  #checkov:skip=CKV_AWS_309:Public read-only open data, by design; abuse is capped by stage throttling
  for_each  = toset(["GET /v1/health", "GET /v1/stops/{code}/departures"])
  api_id    = aws_apigatewayv2_api.api.id
  route_key = each.value
  target    = "integrations/${aws_apigatewayv2_integration.api.id}"
}

resource "aws_apigatewayv2_stage" "api" {
  api_id      = aws_apigatewayv2_api.api.id
  name        = "$default"
  auto_deploy = true

  # A cap on the bill as much as on abuse ("denial of wallet"): 10 requests a second, bursts of 20.
  default_route_settings {
    throttling_rate_limit  = var.api_rate_limit
    throttling_burst_limit = var.api_burst_limit
  }

  # Route, status and latency; deliberately no caller IP address (personal data, not needed).
  access_log_settings {
    destination_arn = aws_cloudwatch_log_group.api_access.arn
    format = jsonencode({
      requestId = "$context.requestId"
      time      = "$context.requestTime"
      routeKey  = "$context.routeKey"
      path      = "$context.path"
      status    = "$context.status"
      latencyMs = "$context.responseLatency"
      lambdaMs  = "$context.integrationLatency"
      error     = "$context.integrationErrorMessage"
    })
  }
}

resource "aws_lambda_permission" "api" {
  statement_id  = "AllowHttpApi"
  action        = "lambda:InvokeFunction"
  function_name = aws_lambda_function.api.function_name
  principal     = "apigateway.amazonaws.com"
  source_arn    = "${aws_apigatewayv2_api.api.execution_arn}/*/*"
}

output "api_url" {
  value = aws_apigatewayv2_stage.api.invoke_url
}
