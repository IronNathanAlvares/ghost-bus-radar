# The website (roadmap 2.2): static pages from web/ and the nightly data/v1/ export, both in the
# private site bucket, served by CloudFront through origin access control. /v1/* goes to the API,
# so the site calls it on its own origin (no CORS needed, and repeats are cached for 30 s).
# Security headers on every response come from infra/site/security_headers.json, which the local
# preview (infra/site/preview.py) also sends, so a policy violation shows up before deploying.

locals {
  web_root     = "${path.module}/../../../web"
  web_files    = fileset(local.web_root, "**")
  site_headers = jsondecode(file("${path.module}/../../site/security_headers.json"))
  content_types = {
    html = "text/html; charset=utf-8"
    css  = "text/css; charset=utf-8"
    js   = "text/javascript; charset=utf-8"
    svg  = "image/svg+xml"
    txt  = "text/plain; charset=utf-8"
    json = "application/json"
  }
  api_domain = trimsuffix(trimprefix(aws_apigatewayv2_api.api.api_endpoint, "https://"), "/")
}

resource "aws_s3_object" "web" {
  for_each      = local.web_files
  bucket        = aws_s3_bucket.site.id
  key           = each.value
  source        = "${local.web_root}/${each.value}"
  etag          = filemd5("${local.web_root}/${each.value}")
  content_type  = lookup(local.content_types, reverse(split(".", each.value))[0], "application/octet-stream")
  cache_control = "public, max-age=300"
}

resource "aws_cloudfront_origin_access_control" "site" {
  name                              = "ghostbus-site"
  description                       = "CloudFront reads the private site bucket, signed with SigV4"
  origin_access_control_origin_type = "s3"
  signing_behavior                  = "always"
  signing_protocol                  = "sigv4"
}

# The nightly data: cached up to 5 minutes by default (files change once a night).
resource "aws_cloudfront_cache_policy" "data" {
  name        = "ghostbus-data"
  min_ttl     = 0
  default_ttl = 300
  max_ttl     = 3600
  parameters_in_cache_key_and_forwarded_to_origin {
    enable_accept_encoding_brotli = true
    enable_accept_encoding_gzip   = true
    cookies_config {
      cookie_behavior = "none"
    }
    headers_config {
      header_behavior = "none"
    }
    query_strings_config {
      query_string_behavior = "none"
    }
  }
}

# The API: follows the API's own cache-control (max-age=30). Every query string is in the cache
# key: with only the known ones, "?minutes=60&junk=1" (a 400) could be cached under the same key as
# a valid "?minutes=60" and served to everyone for a few seconds (cache poisoning).
resource "aws_cloudfront_cache_policy" "api" {
  name        = "ghostbus-api"
  min_ttl     = 0
  default_ttl = 0
  max_ttl     = 60
  parameters_in_cache_key_and_forwarded_to_origin {
    enable_accept_encoding_brotli = true
    enable_accept_encoding_gzip   = true
    cookies_config {
      cookie_behavior = "none"
    }
    headers_config {
      header_behavior = "none"
    }
    query_strings_config {
      query_string_behavior = "all"
    }
  }
}

resource "aws_cloudfront_response_headers_policy" "site" {
  #checkov:skip=CKV_AWS_259:HSTS is set (1 year, subdomains); "preload" only means something for a domain we own, so it waits for a custom domain
  name    = "ghostbus-site"
  comment = "Strict security headers for every response (infra/site/security_headers.json)"

  security_headers_config {
    content_security_policy {
      content_security_policy = local.site_headers.content_security_policy
      override                = true
    }
    strict_transport_security {
      access_control_max_age_sec = local.site_headers.hsts_max_age_s
      include_subdomains         = true
      override                   = true
    }
    content_type_options {
      override = true
    }
    frame_options {
      frame_option = "DENY"
      override     = true
    }
    referrer_policy {
      referrer_policy = local.site_headers.referrer_policy
      override        = true
    }
  }

  custom_headers_config {
    items {
      header   = "Permissions-Policy"
      value    = local.site_headers.permissions_policy
      override = true
    }
  }

  remove_headers_config {
    items {
      header = "Server"
    }
  }
}

data "aws_cloudfront_cache_policy" "optimized" {
  name = "Managed-CachingOptimized"
}

data "aws_cloudfront_origin_request_policy" "all_but_host" {
  name = "Managed-AllViewerExceptHostHeader"
}

resource "aws_cloudfront_distribution" "site" {
  #checkov:skip=CKV2_AWS_42:The free *.cloudfront.net address by choice (roadmap 2.2); a custom domain with an ACM certificate is a later option
  #checkov:skip=CKV_AWS_68:WAF costs about $8/month minimum; throttling at the API and a static site cover the risk at this size
  #checkov:skip=CKV2_AWS_47:Same: no WAF, so no Log4j rule; nothing here runs Java
  #checkov:skip=CKV_AWS_86:Access logs would hold every visitor's IP address; not needed, and personal data
  #checkov:skip=CKV_AWS_174:The default *.cloudfront.net certificate fixes the minimum TLS version; a custom domain would set TLSv1.2_2021
  #checkov:skip=CKV_AWS_310:One origin per behaviour by design; an S3 or API outage is shown honestly, not hidden
  #checkov:skip=CKV_AWS_374:No geo restriction: public open data
  enabled             = true
  comment             = "Ghost Bus Radar website (roadmap 2.2)"
  default_root_object = "index.html"
  http_version        = "http2and3"
  is_ipv6_enabled     = true
  price_class         = "PriceClass_100" # Europe and North America edges: cheapest, and the audience is in Ireland

  origin {
    origin_id                = "site"
    domain_name              = aws_s3_bucket.site.bucket_regional_domain_name
    origin_access_control_id = aws_cloudfront_origin_access_control.site.id
  }

  origin {
    origin_id   = "api"
    domain_name = local.api_domain
    custom_origin_config {
      http_port              = 80
      https_port             = 443
      origin_protocol_policy = "https-only"
      origin_ssl_protocols   = ["TLSv1.2"]
    }
  }

  default_cache_behavior {
    target_origin_id           = "site"
    viewer_protocol_policy     = "redirect-to-https"
    allowed_methods            = ["GET", "HEAD"]
    cached_methods             = ["GET", "HEAD"]
    compress                   = true
    cache_policy_id            = data.aws_cloudfront_cache_policy.optimized.id
    response_headers_policy_id = aws_cloudfront_response_headers_policy.site.id
  }

  ordered_cache_behavior {
    path_pattern               = "/data/*"
    target_origin_id           = "site"
    viewer_protocol_policy     = "redirect-to-https"
    allowed_methods            = ["GET", "HEAD"]
    cached_methods             = ["GET", "HEAD"]
    compress                   = true
    cache_policy_id            = aws_cloudfront_cache_policy.data.id
    response_headers_policy_id = aws_cloudfront_response_headers_policy.site.id
  }

  ordered_cache_behavior {
    path_pattern               = "/v1/*"
    target_origin_id           = "api"
    viewer_protocol_policy     = "redirect-to-https"
    allowed_methods            = ["GET", "HEAD"]
    cached_methods             = ["GET", "HEAD"]
    compress                   = true
    cache_policy_id            = aws_cloudfront_cache_policy.api.id
    origin_request_policy_id   = data.aws_cloudfront_origin_request_policy.all_but_host.id
    response_headers_policy_id = aws_cloudfront_response_headers_policy.site.id
  }

  # S3 answers 403 for a missing key (CloudFront may not list the bucket). The API never answers
  # 403, so mapping it to the 404 page affects only missing site files.
  custom_error_response {
    error_code            = 403
    response_code         = 404
    response_page_path    = "/404.html"
    error_caching_min_ttl = 60
  }

  restrictions {
    geo_restriction {
      restriction_type = "none"
    }
  }

  viewer_certificate {
    cloudfront_default_certificate = true
  }
}

output "site_url" {
  value = "https://${aws_cloudfront_distribution.site.domain_name}/"
}
