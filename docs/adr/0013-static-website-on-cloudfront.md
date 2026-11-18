# ADR-0013: The website: static pages on CloudFront, one origin, strict headers

**Status:** accepted, 18 November 2026. Roadmap 2.2. Code: `web/` (pages), `infra/terraform/core/site.tf`,
`infra/site/` (security headers, local preview).

## Context

The project needs a public face: the league table, a page per route, "will my bus come?" at a
stop, and a methodology page that every number links to. It must cost close to nothing, keep
working when every compute part is off, show nothing the quality gates rejected (ADR-0011), and
carry the NTA attribution. It is also the part of the project anyone on the internet can reach,
so it should be a good example of web security, not an exception to it.

## Decision

- **Static pages, no framework, no build step.** Four HTML pages and small ES modules in
  `web/assets/`. They read the nightly JSON (`data/v1/`, ADR-0011) and the API (ADR-0012). With
  no build there is no dependency tree to audit or keep patched, and the files deployed are the
  files reviewed.
- **One CloudFront distribution, one origin for the browser.** Paths route to:

  | Path | Origin | Cache |
  |---|---|---|
  | `/` (pages, assets) | S3 site bucket | `max-age=300` set on each object |
  | `/data/*` | S3 site bucket | 5 minutes by default (the files change once a night) |
  | `/v1/*` | the HTTP API | follows the API's `max-age=30`; every query string in the cache key |

  The page calls the API on its own origin, so `connect-src 'self'` is enough and no CORS is
  involved. Repeat questions about a busy stop are answered by CloudFront for 30 seconds.
- **The bucket stays private.** CloudFront reads it through origin access control (SigV4); the
  bucket policy allows `s3:GetObject` to the CloudFront service only for this distribution's ARN.
  Block Public Access stays on.
- **Strict security headers on every response**, defined once in
  `infra/site/security_headers.json` and sent both by CloudFront and by the local preview:
  `default-src 'none'`, scripts, styles and fetches from the site only, `frame-ancestors 'none'`,
  `base-uri 'none'`; HSTS for a year; `nosniff`; `X-Frame-Options: DENY`; a Referrer-Policy; and a
  Permissions-Policy that turns off camera, microphone and location.
- **The code can't create markup from data.** Every element is built through the DOM (`h()` in
  `common.js`), never `innerHTML`. A unit test (`tests/unit/test_web.py`) fails the build if a
  page gains an inline script, inline style or event-handler attribute, if a script writes HTML
  from strings, if a definition link points nowhere, or if a page loses the attribution.
- **Honest empty states.** Until a day passes the quality gates the league is empty, the headline
  says why, and a route page says "no published days for this route yet". The local preview
  (`infra/site/preview.py`) fills the pages with clearly labelled random demo data for review;
  demo data is never uploaded.
- **The free `*.cloudfront.net` address.** A custom domain costs about $15 a year plus an ACM
  certificate in `us-east-1`; it can be added later without changing the design.
- **No access logs, no analytics.** Both would collect visitors' IP addresses, which this project
  has no use for.

## Consequences

- Cost: the CloudFront always-free allowance (1 TB and 10 million requests a month) covers it; S3
  storage is a few MB.
- The scanner exceptions are written on the resources with their reasons: no WAF (about $8 a month
  minimum), no access logs (privacy), the default certificate's TLS floor, and HSTS `preload`
  (meaningful only on a domain we own).
- Page changes deploy with `terraform apply` (each file is an `aws_s3_object` with its MD5), and
  reach visitors within 5 minutes without cache invalidations.
- Charts follow one-hue sequential colour for magnitude, with icon and label for every status, a
  table view for every chart and separate light and dark steps.
