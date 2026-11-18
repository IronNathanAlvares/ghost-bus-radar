// Shared helpers. Every element is built through the DOM (never innerHTML), so text from the data
// (stop names, headsigns) can never become markup. The page's Content Security Policy allows
// scripts from this site only.

export const DATA = "data/v1/";
export const API = "/v1/";

export async function getJSON(path, { signal } = {}) {
  const r = await fetch(path, { headers: { accept: "application/json" }, signal });
  if (!r.ok) {
    const e = new Error(`${r.status} for ${path}`);
    e.status = r.status;
    try { e.body = await r.json(); } catch { /* not JSON */ }
    throw e;
  }
  return r.json();
}

/** h("a", {href: "x", class: "y"}, "text", child) */
export function h(tag, attrs = {}, ...children) {
  const el = document.createElement(tag);
  for (const [k, v] of Object.entries(attrs || {})) {
    if (v === null || v === undefined || v === false) continue;
    if (k === "class") el.className = v;
    else if (k.startsWith("on") && typeof v === "function") el.addEventListener(k.slice(2), v);
    else if (k === "style") for (const [p, val] of Object.entries(v)) el.style.setProperty(p, val);
    else el.setAttribute(k, v === true ? "" : String(v));
  }
  for (const c of children.flat()) {
    if (c === null || c === undefined || c === false) continue;
    el.append(c instanceof Node ? c : document.createTextNode(String(c)));
  }
  return el;
}

const SVGNS = "http://www.w3.org/2000/svg";
export function s(tag, attrs = {}, ...children) {
  const el = document.createElementNS(SVGNS, tag);
  for (const [k, v] of Object.entries(attrs)) if (v !== null && v !== undefined) el.setAttribute(k, String(v));
  for (const c of children.flat()) if (c !== null && c !== undefined) el.append(c instanceof Node ? c : document.createTextNode(String(c)));
  return el;
}

export const qs = (name) => new URLSearchParams(location.search).get(name);

export function pct(x, digits = 1) {
  if (x === null || x === undefined || Number.isNaN(x)) return "–";
  return `${(x * 100).toFixed(digits)}%`;
}

/** "8.6%" plus "95% interval 6.9–10.7%" from a rate object {k, n, rate, lo, hi}. */
export function rateParts(r) {
  if (!r) return { value: "–", ci: "no trips" };
  return { value: pct(r.rate), ci: `95% interval ${pct(r.lo)}–${pct(r.hi)} · ${r.k.toLocaleString("en-IE")} of ${r.n.toLocaleString("en-IE")}` };
}

/** 95% Wilson score interval, the same as ghostbus_core.outcomes.wilson. */
export function wilson(k, n, z = 1.96) {
  if (n <= 0) return [0, 1];
  const p = k / n;
  const den = 1 + (z * z) / n;
  const centre = (p + (z * z) / (2 * n)) / den;
  const half = (z * Math.sqrt((p * (1 - p)) / n + (z * z) / (4 * n * n))) / den;
  return [Math.max(0, centre - half), Math.min(1, centre + half)];
}

const QUALITY = {
  ok: ["ok", "passed the checks"],
  warn: ["warn", "passed, with warnings"],
  fail: ["fail", "did not pass"],
};
export function qualityBadge(q) {
  const [cls, label] = QUALITY[q] || ["", "not checked yet"];
  return h("span", { class: `status ${cls}` }, label);
}

const dayFmt = new Intl.DateTimeFormat("en-IE", { weekday: "short", day: "numeric", month: "short", timeZone: "UTC" });
export const dayLabel = (iso) => dayFmt.format(new Date(`${iso}T00:00:00Z`));

const timeFmt = new Intl.DateTimeFormat("en-IE", { hour: "2-digit", minute: "2-digit", timeZone: "Europe/Dublin" });
export const clock = (iso) => timeFmt.format(new Date(iso));

const stampFmt = new Intl.DateTimeFormat("en-IE", {
  weekday: "short", day: "numeric", month: "short", hour: "2-digit", minute: "2-digit", timeZone: "Europe/Dublin",
});
export const stamp = (iso) => stampFmt.format(new Date(iso));

export const methodology = (anchor, text) => h("a", { href: `methodology.html#${anchor}` }, text);

/** One tooltip for the page: show(el, title, lines) on hover and keyboard focus. */
let tipEl = null;
export function tip(target, title, lines) {
  const show = () => {
    if (!tipEl) { tipEl = h("div", { class: "tip", role: "tooltip", hidden: true }); document.body.append(tipEl); }
    tipEl.replaceChildren(h("b", {}, title), ...lines.map((l) => h("div", {}, l)));
    tipEl.hidden = false;
    const r = target.getBoundingClientRect();
    const w = tipEl.offsetWidth, hgt = tipEl.offsetHeight;
    let x = r.left + r.width / 2 - w / 2;
    x = Math.max(8, Math.min(x, window.innerWidth - w - 8));
    let y = r.top - hgt - 8;
    if (y < 8) y = r.bottom + 8;
    tipEl.style.setProperty("left", `${x}px`);
    tipEl.style.setProperty("top", `${y}px`);
  };
  const hide = () => { if (tipEl) tipEl.hidden = true; };
  target.addEventListener("pointerenter", show);
  target.addEventListener("pointerleave", hide);
  target.addEventListener("focus", show);
  target.addEventListener("blur", hide);
}

export function fail(container, message) {
  container.replaceChildren(h("div", { class: "notice warn", role: "status" }, message));
}

/** The run of days the site covers, newest first, from meta.json. */
export function publishedDays(meta) {
  return meta.days.filter((d) => d.quality !== "fail").map((d) => d.date);
}
