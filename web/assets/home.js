import { DATA, dayLabel, fail, getJSON, h, methodology, pct, qualityBadge, rateParts, stamp, wilson } from "./common.js";

const SHOW = 25;

async function main() {
  let meta;
  try {
    meta = await getJSON(`${DATA}meta.json`);
  } catch {
    for (const id of ["headline", "league", "days"]) fail(document.getElementById(id), "The data could not be loaded. Please try again later.");
    return;
  }
  document.getElementById("updated").textContent = `Updated ${stamp(meta.generated_at)}. Rules version ${meta.params_version}.`;
  renderDays(meta);
  const leagues = {};
  const load = async (n) => (leagues[n] ||= await getJSON(`${DATA}league/${n}d.json`));
  try {
    const week = await load(7);
    renderHeadline(week).catch(() => fail(document.getElementById("headline"), "The headline figure could not be loaded."));
    renderLeague(week);
  } catch {
    fail(document.getElementById("league"), "The league table could not be loaded.");
  }
  for (const b of document.querySelectorAll("[data-window]")) {
    b.addEventListener("click", async () => {
      for (const o of document.querySelectorAll("[data-window]")) o.setAttribute("aria-pressed", String(o === b));
      renderLeague(await load(Number(b.dataset.window)));
    });
  }
}

async function renderHeadline(league) {
  const box = document.getElementById("headline");
  const title = box.querySelector("h2");
  if (!league.days_published.length) {
    box.replaceChildren(
      title,
      h("p", { class: "hero-figure" }, "–"),
      h("p", {}, "No day has passed the quality checks yet, so there is nothing to publish. "
        + "We collect every minute; the first complete day appears here the morning after."),
    );
    return;
  }
  // The network figure: the sum over the published days' own documents.
  const docs = await Promise.all(league.days_published.map((d) => getJSON(`${DATA}days/${d}.json`).catch(() => null)));
  let k = 0, n = 0;
  for (const d of docs) if (d && d.published && d.untracked) { k += d.untracked.k; n += d.untracked.n; }
  const [lo, hi] = wilson(k, n);
  box.replaceChildren(
    title,
    h("p", { class: "hero-figure" }, n ? pct(k / n) : "–", " ", h("small", {}, "untracked")),
    h("p", {}, `${k.toLocaleString("en-IE")} of ${n.toLocaleString("en-IE")} fully collected bus trips never showed up on the live map `,
      `(95% interval ${pct(lo)}–${pct(hi)}), `, `${dayLabel(league.from)} to ${dayLabel(league.to)}.`),
    h("p", { class: "small muted" }, "What counts: ", methodology("untracked", "untracked"), " · ",
      methodology("in-rates", "fully collected"), " · ", methodology("interval", "95% interval")),
  );
}

function renderLeague(league) {
  const box = document.getElementById("league");
  if (!league.routes.length && !league.not_ranked.length) {
    box.replaceChildren(h("div", { class: "notice", role: "status" },
      `No published days between ${dayLabel(league.from)} and ${dayLabel(league.to)} yet. `
      + "The table fills in as soon as a day passes the quality checks."));
    return;
  }
  const rows = league.routes.map((r) => {
    const u = rateParts(r.untracked);
    return h("tr", {},
      h("td", { class: "num" }, r.rank),
      h("td", {}, h("a", { href: `route.html?r=${encodeURIComponent(r.slug)}` }, r.route)),
      h("td", {}, r.agency),
      h("td", { class: "num", title: u.ci }, u.value, h("div", { class: "small muted" }, `${pct(r.untracked.lo)}–${pct(r.untracked.hi)}`)),
      h("td", { class: "num" }, r.vanished ? pct(r.vanished.rate) : "–"),
      h("td", { class: "num" }, r.trips_in_rates.toLocaleString("en-IE")),
    );
  });
  const table = (body) => h("div", { class: "table-scroll" }, h("table", {},
    h("caption", {}, `${dayLabel(league.from)} to ${dayLabel(league.to)} · ${league.days_published.length} published day(s)`),
    h("thead", {}, h("tr", {},
      h("th", { class: "num", scope: "col" }, "Rank"),
      h("th", { scope: "col" }, "Route"),
      h("th", { scope: "col" }, "Operator"),
      h("th", { class: "num", scope: "col" }, methodology("untracked", "Untracked"), h("div", { class: "small muted" }, "95% interval")),
      h("th", { class: "num", scope: "col" }, methodology("vanished", "Vanished")),
      h("th", { class: "num", scope: "col" }, methodology("in-rates", "Trips")),
    )),
    h("tbody", {}, body)));
  const out = [table(rows.slice(0, SHOW))];
  if (rows.length > SHOW) {
    const more = h("button", { type: "button", class: "link" }, `Show all ${rows.length} ranked routes`);
    more.addEventListener("click", () => { out[0].replaceWith(table(rows)); more.remove(); });
    out.push(more);
  }
  if (league.not_ranked.length) {
    out.push(h("details", {},
      h("summary", {}, `${league.not_ranked.length} route(s) with fewer than ${league.min_trips_to_rank} trips (not ranked)`),
      h("ul", {}, league.not_ranked.map((r) => h("li", {},
        h("a", { href: `route.html?r=${encodeURIComponent(r.slug)}` }, `${r.agency} ${r.route}`),
        ` · ${r.trips_in_rates} trips`)))));
  }
  box.replaceChildren(...out);
}

function renderDays(meta) {
  const box = document.getElementById("days");
  const days = [...meta.days].reverse().slice(0, 28);
  if (!days.length) { box.replaceChildren(h("p", { class: "muted" }, "No days yet.")); return; }
  box.replaceChildren(h("div", { class: "table-scroll" }, h("table", {},
    h("thead", {}, h("tr", {}, h("th", { scope: "col" }, "Service day"), h("th", { scope: "col" }, "Quality checks"))),
    h("tbody", {}, days.map((d) => h("tr", {}, h("td", {}, dayLabel(d.date)), h("td", {}, qualityBadge(d.quality))))))));
}

main();
