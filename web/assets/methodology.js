import { DATA, dayLabel, getJSON, h, pct, s, tip } from "./common.js";

// "How good is the chance?": the backtest of the probability (ADR-0014), from calibration.json.

const num = (x, d = 3) => (x === null || x === undefined ? "–" : x.toFixed(d));
const skill = (x) => (x === null || x === undefined ? "–" : `${x > 0 ? "+" : ""}${(x * 100).toFixed(0)}%`);

function verdict(doc) {
  const o = doc.overall;
  if (!o.scored) return "Not enough history yet to score any prediction.";
  const parts = [`Over ${doc.test_days.length} day(s), ${o.scored.toLocaleString("en-IE")} predictions were scored.`];
  parts.push(doc.beats_always_comes
    ? `The chance beats “the bus always comes” (Brier ${num(o.brier)} against ${num(o.brier_always_comes)}).`
    : `It does not yet beat “the bus always comes” (Brier ${num(o.brier)} against ${num(o.brier_always_comes)}).`);
  parts.push(doc.beats_base_rate
    ? `It also beats a single base rate for every bus (${num(o.brier_base_rate)}).`
    : `It does not yet beat a single base rate for every bus (${num(o.brier_base_rate)}).`);
  return parts.join(" ");
}

function reliability(rows) {
  const W = 360, H = 360, L = 44, R = 12, T = 12, B = 40;
  const x = (v) => L + (W - L - R) * v;
  const y = (v) => T + (H - T - B) * (1 - v);
  const svg = s("svg", { class: "chart reliability", viewBox: `0 0 ${W} ${H}`, role: "img",
    "aria-label": "Reliability diagram: predicted chance against the share of buses that ran; the table below has the same numbers" });
  for (const v of [0, 0.25, 0.5, 0.75, 1]) {
    svg.append(s("line", { class: v === 0 ? "baseline" : "gridline", x1: L, x2: W - R, y1: y(v), y2: y(v) }));
    svg.append(s("text", { class: "tick", x: L - 6, y: y(v) + 4, "text-anchor": "end" }, pct(v, 0)));
    svg.append(s("text", { class: "tick", x: x(v), y: H - B + 16, "text-anchor": "middle" }, pct(v, 0)));
  }
  svg.append(s("text", { class: "tick", x: (L + W - R) / 2, y: H - 6, "text-anchor": "middle" }, "Predicted chance"));
  svg.append(s("text", { class: "tick", x: 12, y: (T + H - B) / 2, "text-anchor": "middle", transform: `rotate(-90 12 ${(T + H - B) / 2})` }, "Share that ran"));
  svg.append(s("line", { class: "diagonal", x1: x(0), y1: y(0), x2: x(1), y2: y(1) }));
  const most = Math.max(...rows.map((r) => r.n));
  for (const r of rows) {
    const cx = x(r.mean_predicted), cy = y(r.share_ran);
    const radius = 4 + 6 * Math.sqrt(r.n / most);
    const g = s("g", {});
    g.append(s("line", { class: "whisker", x1: cx, x2: cx, y1: y(r.lo), y2: y(r.hi) }));
    const dot = s("circle", { class: "dot", cx, cy, r: radius, tabindex: 0,
      "aria-label": `Predicted ${pct(r.mean_predicted)}, ran ${pct(r.share_ran)}, ${r.n} predictions` });
    g.append(dot);
    tip(dot, `Predicted ${pct(r.from, 0)}–${pct(r.to, 0)}`, [
      `${r.n.toLocaleString("en-IE")} predictions, average ${pct(r.mean_predicted)}`,
      `${pct(r.share_ran)} of those buses ran (95% interval ${pct(r.lo)}–${pct(r.hi)})`,
    ]);
    svg.append(g);
  }
  return svg;
}

function table(head, rows) {
  return h("div", { class: "table-scroll" }, h("table", {},
    h("thead", {}, h("tr", {}, head.map((c, i) => h("th", { scope: "col", class: i ? "num" : null }, c)))),
    h("tbody", {}, rows.map((r) => h("tr", {}, r.map((c, i) => h("td", { class: i ? "num" : null }, c)))))));
}

async function main() {
  const box = document.getElementById("calibration-results");
  if (!box) return;
  let doc;
  try {
    doc = await getJSON(`${DATA}calibration.json`);
  } catch {
    box.replaceChildren(h("p", { class: "muted" }, "The backtest results are not available yet."));
    return;
  }
  const days = doc.test_days;
  const out = [
    h("p", {}, verdict(doc)),
    h("p", { class: "small muted" }, days.length
      ? `Test days ${dayLabel(days[0])} to ${dayLabel(days[days.length - 1])}; model ${doc.model}; calibration error ${pct(doc.overall.calibration_error)}.`
      : `Model ${doc.model}.`),
  ];
  if (doc.reliability.length) {
    out.push(h("figure", { class: "reliability-figure" }, reliability(doc.reliability),
      h("figcaption", { class: "small muted" },
        "Each dot is a group of predictions; on the dashed line, the chance said exactly what happened. Bigger dots are bigger groups; lines are 95% intervals.")));
    out.push(h("details", {}, h("summary", {}, "Show as a table"), table(
      ["Predicted", "Predictions", "Average predicted", "Share that ran", "95% interval"],
      doc.reliability.map((r) => [`${pct(r.from, 0)}–${pct(r.to, 0)}`, r.n.toLocaleString("en-IE"),
        pct(r.mean_predicted), pct(r.share_ran), `${pct(r.lo)}–${pct(r.hi)}`]))));
  }
  out.push(h("h3", {}, "By how far ahead the chance was given"));
  out.push(table(
    ["Minutes before", "Scored", "Brier", "Always comes", "Base rate", "Better than always comes", "Better than base rate"],
    doc.by_lead.map((r) => [String(r.lead_min), r.scored.toLocaleString("en-IE"), num(r.brier), num(r.brier_always_comes),
      num(r.brier_base_rate), skill(r.skill_vs_always_comes), skill(r.skill_vs_base_rate)])));
  out.push(h("h3", {}, "By live status at the time"));
  out.push(table(
    ["Status", "Scored", "Average predicted", "Share that ran", "Brier"],
    doc.by_status.map((r) => [r.status.replace(/_/g, " "), r.scored.toLocaleString("en-IE"), pct(r.mean_predicted),
      pct(r.share_ran), num(r.brier)])));
  box.replaceChildren(...out);
}

main();
