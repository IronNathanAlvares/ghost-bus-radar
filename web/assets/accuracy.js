import { h, methodology, pct, s, tip } from "./common.js";

// "When the board says 5 minutes...": real-time countdowns vs when buses really arrived, with the
// timetable alongside (ADR-0016). Median and the 10th-90th percentile range per lead time.

const mins = (x) => (x === null || x === undefined ? "–" : `${x > 0 ? "+" : ""}${x.toFixed(1)} min`);
const label = (b) => `${b.replace("-", "–")} min`;

function chart(rows) {
  const W = 720, H = 260, L = 48, R = 12, T = 12, B = 34;
  const lo = Math.min(-2, ...rows.flatMap((r) => [r.p10_min, r.timetable.p10_min]));
  const hi = Math.max(2, ...rows.flatMap((r) => [r.p90_min, r.timetable.p90_min]));
  const top = Math.ceil(hi / 2) * 2, bottom = Math.floor(lo / 2) * 2;
  const y = (v) => T + (H - T - B) * (top - v) / (top - bottom);
  const band = (W - L - R) / rows.length;
  const svg = s("svg", { class: "chart accuracy", viewBox: `0 0 ${W} ${H}`, role: "img",
    "aria-label": "Error of the countdown by lead time, real-time against timetable; the table below has the same numbers" });
  for (let v = bottom; v <= top; v += 2) {
    svg.append(s("line", { class: v === 0 ? "baseline" : "gridline", x1: L, x2: W - R, y1: y(v), y2: y(v) }));
    svg.append(s("text", { class: "tick", x: L - 6, y: y(v) + 4, "text-anchor": "end" }, `${v > 0 ? "+" : ""}${v}`));
  }
  svg.append(s("text", { class: "tick", x: 10, y: (T + H - B) / 2, "text-anchor": "middle",
    transform: `rotate(-90 10 ${(T + H - B) / 2})` }, "minutes late (+) or early (−)"));
  rows.forEach((r, i) => {
    const cx = L + band * i + band / 2;
    svg.append(s("text", { class: "tick", x: cx, y: H - 12, "text-anchor": "middle" }, label(r.lead_min)));
    for (const [series, d, dx] of [["realtime", r, -9], ["timetable", r.timetable, 9]]) {
      if (d.median_min === null || d.median_min === undefined) continue;
      const g = s("g", { class: `series-${series}` });
      g.append(s("line", { class: "range", x1: cx + dx, x2: cx + dx, y1: y(d.p10_min), y2: y(d.p90_min) }));
      const dot = s("circle", { class: "dot", cx: cx + dx, cy: y(d.median_min), r: 5.5, tabindex: 0,
        "aria-label": `${series === "realtime" ? "Real-time" : "Timetable"}, board says ${label(r.lead_min)}: median ${mins(d.median_min)}` });
      g.append(dot);
      tip(dot, `${series === "realtime" ? "Real-time countdown" : "Timetable"}, ${label(r.lead_min)} ahead`, [
        `Typical: ${mins(d.median_min)}`,
        `8 in 10 buses between ${mins(d.p10_min)} and ${mins(d.p90_min)}`,
        `Within 2 minutes: ${pct(d.within_2_min.share, 0)}`,
      ]);
      svg.append(g);
    }
  });
  return svg;
}

export function renderAccuracy(box, doc) {
  const rows = doc.by_lead.filter((r) => r.predictions);
  if (!rows.length) {
    box.replaceChildren(h("p", { class: "muted" }, "No measurements yet."));
    return;
  }
  const five = rows.find((r) => r.lead_min === "5-10") || rows[0];
  const n = rows.reduce((a, r) => a + r.predictions, 0);
  box.replaceChildren(
    h("p", { class: "lede" },
      `When the board said ${label(five.lead_min)}, the bus came within 2 minutes of it `,
      h("strong", {}, pct(five.within_2_min.share, 0)), ` of the time `,
      `(timetable: ${pct(five.timetable.within_2_min.share, 0)}). Typically it was ${mins(five.median_min)} off.`),
    h("figure", { class: "accuracy-figure" },
      h("div", { class: "legend" },
        h("span", {}, h("span", { class: "sw realtime" }), "Real-time countdown"),
        h("span", {}, h("span", { class: "sw timetable" }), "Timetable"),
        h("span", { class: "muted" }, "dot: typical (median); line: 8 in 10 buses")),
      chart(rows),
      h("figcaption", { class: "small muted" },
        `${n.toLocaleString("en-IE")} countdowns from ${doc.days.length} day(s), checked against when each bus actually reached the stop. `,
        methodology("accuracy", "How we measure this"))),
    h("details", {}, h("summary", {}, "Show as a table"), h("div", { class: "table-scroll" }, h("table", {},
      h("thead", {}, h("tr", {}, ["Board says", "Countdowns", "Trips", "Typical", "8 in 10 between", "Within 2 min (95% interval)", "Timetable within 2 min"]
        .map((c, i) => h("th", { scope: "col", class: i ? "num" : null }, c)))),
      h("tbody", {}, rows.map((r) => h("tr", {},
        h("td", {}, label(r.lead_min)),
        h("td", { class: "num" }, r.predictions.toLocaleString("en-IE")),
        h("td", { class: "num" }, r.trips.toLocaleString("en-IE")),
        h("td", { class: "num" }, mins(r.median_min)),
        h("td", { class: "num" }, `${mins(r.p10_min)} to ${mins(r.p90_min)}`),
        h("td", { class: "num" }, `${pct(r.within_2_min.share, 0)} (${pct(r.within_2_min.lo, 0)}–${pct(r.within_2_min.hi, 0)})`),
        h("td", { class: "num" }, pct(r.timetable.within_2_min.share, 0)))))))),
  );
}
