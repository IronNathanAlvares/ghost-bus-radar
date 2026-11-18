"""The website's own rules (roadmap 2.2), checked on the source files in web/.

- Every number links to its definition: every ``methodology.html#x`` link (in HTML and in the
  page scripts) must point at an ``id`` that exists on the methodology page.
- The Content Security Policy allows scripts and styles from the site only, so no page may carry
  an inline script, an inline style or an event-handler attribute, and no script may write HTML
  from strings (``innerHTML`` and friends), which is how stop names could ever become markup.
- Every local file a page references exists.
- Every page carries the NTA attribution and the licence.
"""

from __future__ import annotations

import json
import re
from html.parser import HTMLParser
from pathlib import Path

import pytest

WEB = Path(__file__).resolve().parents[2] / "web"
PAGES = sorted(WEB.glob("*.html"))
SCRIPTS = sorted((WEB / "assets").glob("*.js"))
HEADERS = json.loads(
    (WEB.parent / "infra" / "site" / "security_headers.json").read_text(encoding="utf-8")
)


class Page(HTMLParser):
    def __init__(self) -> None:
        super().__init__()
        self.ids: set[str] = set()
        self.links: list[str] = []
        self.inline: list[str] = []
        self.has_lang = False
        self.has_description = False

    def handle_starttag(self, tag: str, attrs: list[tuple[str, str | None]]) -> None:
        a = dict(attrs)
        if tag == "html" and a.get("lang"):
            self.has_lang = True
        if tag == "meta" and a.get("name") == "description" and a.get("content"):
            self.has_description = True
        if a.get("id"):
            self.ids.add(a["id"] or "")
        for key in ("href", "src"):
            if a.get(key):
                self.links.append(a[key] or "")
        if "style" in a:
            self.inline.append(f"style attribute on <{tag}>")
        for name in a:
            if name.startswith("on"):
                self.inline.append(f"{name} attribute on <{tag}>")
        if tag == "script" and not a.get("src"):
            self.inline.append("inline <script>")
        if tag == "style":
            self.inline.append("inline <style>")


def parse(path: Path) -> Page:
    p = Page()
    p.feed(path.read_text(encoding="utf-8"))
    return p


METHODOLOGY_IDS = parse(WEB / "methodology.html").ids


@pytest.mark.parametrize("page", PAGES, ids=lambda p: p.name)
def test_pages_have_no_inline_code_and_say_who_they_are(page: Path) -> None:
    p = parse(page)
    assert p.inline == [], f"{page.name}: CSP would block {p.inline}"
    assert p.has_lang and p.has_description


@pytest.mark.parametrize("page", PAGES, ids=lambda p: p.name)
def test_every_local_reference_exists(page: Path) -> None:
    for link in parse(page).links:
        if re.match(r"^(https?:|mailto:|#|data/|\./?$|/$)", link):
            continue
        target = link.split("#")[0].split("?")[0].lstrip("/")
        if target:
            assert (WEB / target).exists(), f"{page.name} links to missing {link}"


def test_every_definition_link_points_at_a_definition() -> None:
    sources = [*PAGES, *SCRIPTS]
    anchors = set()
    for path in sources:
        text = path.read_text(encoding="utf-8")
        anchors |= {(path.name, a) for a in re.findall(r"methodology\.html#([a-z0-9-]+)", text)}
        # methodology("anchor", ...) in the page scripts
        anchors |= {(path.name, a) for a in re.findall(r'methodology\("([a-z0-9-]+)"', text)}
    missing = sorted((f, a) for f, a in anchors if a not in METHODOLOGY_IDS)
    assert len(anchors) >= 10
    assert missing == [], f"links to definitions that don't exist: {missing}"


@pytest.mark.parametrize("script", SCRIPTS, ids=lambda p: p.name)
def test_scripts_never_write_html_from_strings(script: Path) -> None:
    text = script.read_text(encoding="utf-8")
    bad = re.compile(
        r"\.(innerHTML|outerHTML)\b|insertAdjacentHTML|document\.write|\beval\(|new Function"
    )
    assert bad.findall(text) == [], f"{script.name} writes HTML from strings"


@pytest.mark.parametrize("page", [p for p in PAGES if p.name != "404.html"], ids=lambda p: p.name)
def test_every_page_carries_the_attribution(page: Path) -> None:
    text = page.read_text(encoding="utf-8")
    assert "National Transport Authority" in text or "NTA" in text
    assert "CC BY 4.0" in text


def test_the_policy_is_strict() -> None:
    csp = dict(
        (d.split()[0], d.split()[1:]) for d in HEADERS["content_security_policy"].split("; ")
    )
    assert csp["default-src"] == ["'none'"]
    for directive in ("script-src", "style-src", "connect-src"):
        assert csp[directive] == ["'self'"], directive
    assert csp["frame-ancestors"] == ["'none'"] and csp["base-uri"] == ["'none'"]
    assert HEADERS["hsts_max_age_s"] >= 31536000
