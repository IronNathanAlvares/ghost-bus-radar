import { DATA, dayLabel, fail, getJSON, h, methodology, pct, qs, rateParts, s, tip } from "./common.js";

const DAY_TYPES = [["weekday", "Weekdays"], ["saturday", "Saturdays"], ["sunday", "Sundays"]];
// Untracked share -> one of 8 steps of the blue ramp (site.css --s1..--s8).
const STEPS = [0.02, 0.04, 0.06, 0.08, 0.12, 0.16, 0.25];
const STEP_LABELS = ["<2%", "2–4", "4–6", "6–8", "8–12", "12–16", "16–25", "≥25%"];
const THIN = 10;

const step = (rate) => 1 + STEPS.filter((t) => rate >= t).length;

async function main() {
  const slug = qs("r") || "";
  if (!/^[a-z0-9-]{1,80}$/.test(slug)) {
    document.getElementById("title").textContent = "Route not found";
    fail(document.getElementById("sub"), "That link doesn't name a route. Pick one from the league table.");
    return;
  }
  let doc;
  try {
    // A route file exists only for routes with a published day; the 28-day league lists them
    // all, so check it first rather than ask for a file that isn't there.
    const league = await getJSON(`${DATA}league/28d.json`);
    const known = [...league.routes, ...league.not_ranked].some((r) => r.slug === slug);
    if (!known) throw Object.assign(new Error("not published"), { status: 404 });
    doc = await getJSON(`${DATA}routes/${slug}.json`);
  } catch (e) {
    document.getElementById("title").textContent = "Route not found";
    fail(document.getElementById("sub"), e.status === 404 || e.status === 403
      ? "No published days for this route yet. Routes appear once a day that includes them passes the quality checks."
      : "The route could not be loaded. Please try again later.");
    return;
  }
  document.title = `${doc.agency} ${doc.route} · Ghost Bus Radar`;
  document.getElementById("title").textContent = `${doc.agency} ${doc.route}`;
  const days = doc.daily.length;
  document.getElementById("sub").replaceChildren(
    `${doc.total.trips_in_rates.toLocaleString("en-IE")} `, methodology("in-rates", "fully collected"),
    ` trips over ${days} published day${days === 1 ? "" : "s"}, to ${dayLabel(doc.to)}.`);
  renderTiles(doc.total);
  renderHeat(doc.by_hour);
  renderDaily(doc.daily);
}

function renderTiles(t) {
  const tiles = [
    ["untracked", "Untracked", t.untracked],
    ["vanished", "Vanished mid-route", t.vanished],
    ["cancelled-early", "Cancelled with notice", t.cancelled_early],
    ["cancelled-late", "Cancelled late", t.cancelled_late],
    ["ran", "Ran", t.ran],
  ];
  document.getElementById("tiles").replaceChildren(...tiles.map(([anchor, label, r]) => {
    const p = rateParts(r);
    return h("div", { class: "card tile" },
      h("div", { class: "label" }, methodology(anchor, label)),
      h("div", { class: "value" }, p.value),
      h("div", { class: "ci" }, p.ci));
  }));
}

function renderHeat(byHour) {
  const box = document.getElementById("heat");
  const cells = new Map(byHour.map((c) => [`${c.day_type}|${c.hour}`, c]));
  const grid = h("div", { class: "heat", role: "img", "aria-label": "Untracked share by day type and hour; the table below has the same numbers" });
  grid.append(h("div"));
  for (let hr = 0; hr < 24; hr++) grid.append(h("div", { class: "hour" }, hr % 3 === 0 ? String(hr).padStart(2, "0") : ""));
  const rows = [];
  for (const [key, label] of DAY_TYPES) {
    grid.append(h("div", { class: "rowlabel" }, label));
    for (let hr = 0; hr < 24; hr++) {
      const c = cells.get(`${key}|${hr}`);
      if (!c || !c.untracked) { grid.append(h("div", { class: "cell none" })); continue; }
      const n = c.trips_in_rates;
      const cls = n < THIN ? "cell thin" : `cell s${step(c.untracked.rate)}`;
      const el = h("button", { type: "button", class: cls, "aria-label": `${label} ${hr}:00, ${pct(c.untracked.rate)} untracked of ${n} trips` });
      tip(el, `${label}, ${String(hr).padStart(2, "0")}:00–${String(hr).padStart(2, "0")}:59`, [
        `${pct(c.untracked.rate)} untracked (${c.untracked.k} of ${n} trips)`,
        `95% interval ${pct(c.untracked.lo)}–${pct(c.untracked.hi)}`,
        n < THIN ? "Too few trips to read much into" : "",
      ].filter(Boolean));
      grid.append(el);
      rows.push([label, hr, c]);
    }
  }
  const legend = h("div", { class: "legend" },
    h("span", {}, "Untracked:"),
    h("span", { class: "ramp", "aria-hidden": "true" }, STEP_LABELS.map((_, i) => h("span", { class: `s${i + 1}` }))),
    h("span", {}, `${STEP_LABELS[0]} to ${STEP_LABELS[7]}`),
    h("span", {}, h("span", { class: "sw thin" }), `fewer than ${THIN} trips`),
    h("span", {}, h("span", { class: "sw none" }), "no trips"));
  box.replaceChildren(grid, legend, tableView(
    ["Day type", "Hour", "Untracked", "95% interval", "Trips"],
    rows.map(([label, hr, c]) => [label, `${String(hr).padStart(2, "0")}:00`, pct(c.untracked.rate),
      `${pct(c.untracked.lo)}–${pct(c.untracked.hi)}`, c.trips_in_rates]),
  ));
}

function renderDaily(daily) {
  const box = document.getElementById("daily");
  const pts = daily.filter((d) => d.untracked);
  if (!pts.length) { box.replaceChildren(h("p", { class: "muted" }, "No published days.")); return; }
  const W = 720, H = 220, L = 44, R = 8, T = 10, B = 28;
  const top = Math.max(0.1, Math.ceil(Math.max(...pts.map((d) => d.untracked.hi)) * 20) / 20);
  const y = (v) => T + (H - T - B) * (1 - v / top);
  const band = (W - L - R) / pts.length;
  const bw = Math.max(3, Math.min(28, band * 0.6));
  const svg = s("svg", { class: "chart", viewBox: `0 0 ${W} ${H}`, role: "img", "aria-label": "Untracked share per day; the table below has the same numbers" });
  for (const v of [0, top / 2, top]) {
    svg.append(s("line", { class: v === 0 ? "baseline" : "gridline", x1: L, x2: W - R, y1: y(v), y2: y(v) }));
    svg.append(s("text", { class: "tick", x: L - 6, y: y(v) + 4, "text-anchor": "end" }, pct(v, 0)));
  }
  const every = Math.ceil(pts.length / 8);
  pts.forEach((d, i) => {
    const cx = L + band * i + band / 2;
    const u = d.untracked;
    const g = s("g", {});
    const hit = s("rect", { class: "hit", x: cx - band / 2, y: T, width: band, height: H - T - B, tabindex: 0,
      "aria-label": `${dayLabel(d.date)}: ${pct(u.rate)} untracked of ${u.n} trips` });
    const barTop = y(u.rate);
    g.append(hit,
      s("rect", { class: "bar", x: cx - bw / 2, y: barTop, width: bw, height: Math.max(0, y(0) - barTop), rx: 2 }),
      s("line", { class: "whisker", x1: cx, x2: cx, y1: y(u.lo), y2: y(u.hi) }));
    if (i % every === 0) g.append(s("text", { class: "tick", x: cx, y: H - 8, "text-anchor": "middle" }, dayLabel(d.date).replace(/^\w+,? /, "")));
    tip(hit, dayLabel(d.date), [`${pct(u.rate)} untracked (${u.k} of ${u.n} trips)`, `95% interval ${pct(u.lo)}–${pct(u.hi)}`]);
    svg.append(g);
  });
  box.replaceChildren(svg, tableView(
    ["Day", "Untracked", "95% interval", "Vanished", "Trips"],
    pts.map((d) => [dayLabel(d.date), pct(d.untracked.rate), `${pct(d.untracked.lo)}–${pct(d.untracked.hi)}`,
      d.vanished ? pct(d.vanished.rate) : "–", d.trips_in_rates]),
  ));
}

function tableView(head, rows) {
  return h("details", {}, h("summary", {}, "Show as a table"),
    h("div", { class: "table-scroll" }, h("table", {},
      h("thead", {}, h("tr", {}, head.map((c, i) => h("th", { scope: "col", class: i ? "num" : null }, c)))),
      h("tbody", {}, rows.map((r) => h("tr", {}, r.map((c, i) => h("td", { class: i ? "num" : null }, typeof c === "number" ? c.toLocaleString("en-IE") : c))))))));
}

main();
