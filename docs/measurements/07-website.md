# Measurement 07: the website

**Date:** 18 November 2026. **Roadmap:** 2.2. Design: ADR-0013. CloudFront (PriceClass_100) in
front of the private site bucket and the API.

## 1. The live site, checked by hand

| Check | Result |
|---|---|
| `/`, `/route.html`, `/stop.html`, `/methodology.html`, `/assets/*` | 200, correct content types |
| `/data/v1/meta.json` | 200, from the bucket the export writes |
| `/v1/health`, `/v1/stops/{code}/departures` | 200, routed to the API on the same origin |
| A page that doesn't exist | 404 with the site's 404 page |
| The bucket directly, anonymously | 403: the bucket stays private |
| `http://` | 301 to `https://` |
| Compression | Brotli on HTML, CSS, JS and JSON; `stops.json` 964 KB becomes 179 KB |
| Security headers | CSP, HSTS (1 year), `nosniff`, `X-Frame-Options: DENY`, Referrer-Policy, Permissions-Policy on every response |
| API caching | the same URL twice: Miss, then Hit |
| Cache key | `?minutes=50&junk=1` answers 400; `?minutes=50` straight after still answers 200 (no poisoning) |

## 2. Lighthouse

Mobile, simulated slow 4G, Lighthouse 12. Second run, after the fixes below:

| Page | Performance | Accessibility | Best practices | SEO |
|---|---|---|---|---|
| Home | (see below) | 100 | 100 | 100 |
| Route | (see below) | 100 | 100 | 100 |
| Stop | (see below) | 100 | 100 | 100 |
| Methodology | (see below) | 100 | 100 | 100 |

**Fixed after the first run:**

- *Contrast:* white text on the dark theme's accent blue measured 3.6:1. Buttons and route pills now
  use their own fill (6.6:1 light, 5.4:1 dark). Every text and background pair was then checked
  at 4.5:1 or better in both themes.
- *A console error on the route page:* before any day is published the route file doesn't exist,
  so the browser logged a 404. The page now checks the 28-day league for the route first.
- *Layout shift:* content arriving after load (the departures board, the charts) pushed the footer
  up the screen: CLS 0.176 on the stop page. The main area is now at least one screen tall.

**Performance could not be measured on this laptop.** The trace shows antivirus HTTPS inspection
on every request: a 172 KB script injected into each page (render-blocking), the HTML arriving
after 1.8 s and uncompressed although CloudFront serves it brotli-compressed to `curl`. The clearest
sign: the methodology page, which runs none of the site's JavaScript, scored 41 with "total
blocking time" from the injected script. These numbers measure the antivirus, not the site.

The clean measurement runs on a GitHub runner (`.github/workflows/lighthouse.yml`, started by hand
with the site URL, which is not stored in the repository); its results belong in this table.

## 3. Weight

| Page | Requests before data | Transfer (compressed) |
|---|---|---|
| Home | HTML, CSS, 2 modules, `meta.json`, `league/7d.json` (preloaded in parallel) | about 15 KB |
| Stop with a code | HTML, CSS, 2 modules, one API call | about 12 KB |
| Stop search | as above plus `stops.json` once, on the first search | about 190 KB |

No fonts, no images beyond four small SVG icons, no third-party requests.
