import { API, DATA, clock, fail, getJSON, h, methodology, qs } from "./common.js";

const CODE = /^[0-9A-Za-z]{1,12}$/;
const REFRESH_MS = 60_000;
const MAX_RESULTS = 20;

const STATUS = {
  tracked: "Tracked live",
  assigned: "Bus assigned, not moving yet",
  scheduled: "Timetable only so far",
  cancelled: "Cancelled",
  skips_stop: "Not stopping here",
  no_live_data: "No live data",
};

let stopsPromise = null;
const stops = () => (stopsPromise ||= getJSON(`${DATA}stops.json`).then((d) => d.stops));

const fold = (t) => t.normalize("NFKD").replace(/[̀-ͯ]/g, "").toLowerCase();

async function search(q) {
  const box = document.getElementById("results");
  q = q.trim();
  if (!q) { box.replaceChildren(); return; }
  let all;
  try { all = await stops(); } catch { fail(box, "The stop list could not be loaded."); return; }
  const exact = all.find((st) => st.code === q);
  if (exact) { location.search = `?code=${encodeURIComponent(exact.code)}`; return; }
  const needle = fold(q);
  const hits = all.filter((st) => st.code.startsWith(q) || fold(st.name).includes(needle)).slice(0, MAX_RESULTS);
  if (!hits.length) { box.replaceChildren(h("p", { class: "muted" }, `No stop matches “${q}”.`)); return; }
  box.replaceChildren(h("ul", { class: "results" }, hits.map((st) => h("li", {},
    h("a", { href: `stop.html?code=${encodeURIComponent(st.code)}` },
      h("span", { class: "code" }, st.code), st.name,
      h("span", { class: "small muted" }, ` · ${st.routes.slice(0, 8).join(", ")}${st.routes.length > 8 ? "…" : ""}`))))));
}

function minutesAway(iso, now) {
  const m = Math.round((Date.parse(iso) - now) / 60000);
  if (m <= 0) return "Due";
  return `${m} min`;
}

function delayText(d) {
  if (d.delay_s === null || d.delay_s === undefined) return null;
  const m = Math.round(d.delay_s / 60);
  if (m === 0) return "on time";
  return m > 0 ? `${m} min late` : `${-m} min early`;
}

function probability(d) {
  if (d.probability === null || d.probability === undefined) {
    return h("span", { class: "small muted", title: d.basis && d.basis.reason ? d.basis.reason : "" },
      "Chance: not enough history yet");
  }
  const p = Math.round(d.probability * 100);
  return h("span", { class: "prob", title: "Chance this bus really comes, from the route's history and its live status" },
    h("span", { class: "meter", "aria-hidden": "true" }, h("span", { style: { "--p": `${p}%` } })),
    `${p}% chance`);
}

/** A cancelled bus is not always gone: say how often this operator's cancelled buses still ran. */
function cancelledChance(d) {
  if (!d.probability) return null;
  const p = Math.round(d.probability * 100);
  return h("span", { class: "small", title: "From this operator's record of cancelled buses that ran anyway" },
    `but ${p}% of this operator's cancelled buses still ran`);
}

function renderBoard(doc) {
  const box = document.getElementById("stop");
  document.title = `${doc.stop.name} · Ghost Bus Radar`;
  document.getElementById("title").textContent = doc.stop.name;
  const live = doc.live;
  const liveNote = live.status === "fresh"
    ? `Live data from ${clock(live.trip_updates_at)}.`
    : live.status === "stale"
      ? `Live data is ${Math.round(live.age_s / 60)} minutes old, so treat it with care.`
      : "Live data is unavailable right now: showing the timetable only.";
  const now = Date.parse(doc.generated_at);
  const items = doc.departures.map((d) => {
    const when = d.expected || d.scheduled;
    const delay = delayText(d);
    return h("li", {},
      h("span", { class: "route-pill" }, d.route),
      h("div", { class: "dest" },
        h("div", { class: "head" }, d.headsign || d.route),
        h("div", { class: "meta" },
          h("span", { class: `live ${d.status}` }, STATUS[d.status] || d.status),
          delay ? h("span", {}, delay) : null,
          d.status === "skips_stop" ? null
            : d.status === "cancelled" ? cancelledChance(d) : probability(d))),
      h("div", { class: "when" },
        h("span", { class: "big" }, d.status === "cancelled" ? "–" : minutesAway(when, now)),
        h("span", { class: "small muted" }, d.expected ? `${clock(d.expected)}, due ${clock(d.scheduled)}` : `due ${clock(d.scheduled)}`)));
  });
  box.replaceChildren(
    h("p", { class: "muted small" }, `Stop ${doc.stop.code} · `, liveNote, " ",
      methodology("live-status", "What the statuses mean"), " · ",
      h("a", { href: `https://www.openstreetmap.org/?mlat=${doc.stop.lat}&mlon=${doc.stop.lon}#map=18/${doc.stop.lat}/${doc.stop.lon}` }, "Map")),
    items.length
      ? h("ol", { class: "board", "aria-label": `Next departures at ${doc.stop.name}` }, items)
      : h("div", { class: "notice" }, `No buses due in the next ${doc.window_minutes} minutes.`),
    h("p", { class: "small muted" }, `Updated ${clock(doc.generated_at)}; refreshes every minute.`));
}

async function loadBoard(code) {
  const box = document.getElementById("stop");
  try {
    renderBoard(await getJSON(`${API}stops/${encodeURIComponent(code)}/departures?limit=12`));
  } catch (e) {
    const msg = e.status === 404 ? `There is no bus stop with the number ${code}.`
      : e.status === 429 ? "Lots of people are asking right now; please try again in a few seconds."
        : e.status === 503 ? "Departures are temporarily unavailable while the timetable is refreshed."
          : "Departures could not be loaded. Please try again.";
    fail(box, msg);
  }
}

function main() {
  const form = document.getElementById("search");
  const input = document.getElementById("q");
  form.addEventListener("submit", (ev) => { ev.preventDefault(); search(input.value); });
  let timer = null;
  input.addEventListener("input", () => { clearTimeout(timer); timer = setTimeout(() => search(input.value), 250); });
  const q = qs("q");
  if (q) { input.value = q; search(q); }
  const code = qs("code");
  if (code && CODE.test(code)) {
    input.value = code;
    document.getElementById("stop").replaceChildren(h("p", { class: "muted" }, "Loading departures…"));
    loadBoard(code);
    setInterval(() => { if (document.visibilityState === "visible") loadBoard(code); }, REFRESH_MS);
  } else if (code) {
    fail(document.getElementById("stop"), "A stop number is letters and digits only.");
  }
}

main();
