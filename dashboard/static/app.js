/* Smart City Mobility dashboard - page logic.
   A small single-page app: the sidebar changes the page (#overview, #traffic ...),
   each page fetches its data from the FastAPI backend and draws KPIs, charts,
   tables and the map. In live mode the data is refreshed every 10 seconds. */

const state = { page: "overview", hours: 1, meta: { mode: "live" }, charts: {}, map: null, mapLayers: {}, timer: null };
const PAGES = ["overview", "traffic", "map", "transit", "pipeline", "quality"];

/* ================================================================ helpers */
const $ = (sel, root = document) => root.querySelector(sel);
const nf = (d = 0) => new Intl.NumberFormat(LANG === "fr" ? "fr-FR" : "en-GB", { maximumFractionDigits: d, minimumFractionDigits: d });
const num = (v, d = 0) => (v === null || v === undefined ? "—" : nf(d).format(v));
const pct = (v, d = 1) => (v === null || v === undefined ? "—" : `${nf(d).format(v)} %`);
const esc = (s) => String(s ?? "").replace(/[&<>"]/g, (c) => ({ "&": "&amp;", "<": "&lt;", ">": "&gt;", '"': "&quot;" }[c]));

function hhmm(iso) {
  const d = new Date(iso);
  return d.toLocaleTimeString(LANG === "fr" ? "fr-FR" : "en-GB", { hour: "2-digit", minute: "2-digit" });
}
function dateTime(iso) {
  if (!iso) return "—";
  return new Date(iso).toLocaleString(LANG === "fr" ? "fr-FR" : "en-GB", { day: "2-digit", month: "short", hour: "2-digit", minute: "2-digit" });
}
/* seconds -> "+1 min 20 s" */
function delay(s) {
  if (s === null || s === undefined) return "—";
  const sign = s < 0 ? "−" : "+", a = Math.abs(Math.round(s));
  return a >= 60 ? `${sign}${Math.floor(a / 60)} min ${String(a % 60).padStart(2, "0")} s` : `${sign}${a} s`;
}
function rangeLabel() { return state.hours === 1 ? t("last_hour") : t("last_hours", { h: state.hours }); }

async function api(path) {
  const res = await fetch(path, { cache: "no-store" });
  if (!res.ok) {
    let msg = `${res.status}`;
    try { msg = (await res.json()).detail || msg; } catch (e) { /* not JSON */ }
    throw new Error(msg);
  }
  return res.json();
}
function showError(err) {
  const box = $("#error-banner");
  if (!err) { box.hidden = true; return; }
  box.hidden = false;
  const msg = String(err.message || err);
  // right after start-up the Airflow tables do not exist yet: say it in the user's language
  box.textContent = msg.includes("analytics tables") ? t("waiting_airflow") : t("error_banner", { msg });
}

/* ================================================================ layout builders */
const MARKS = '<div class="marks"><i></i><i></i><i></i><i></i><i></i></div>';
const row = (inner, extra = "") => `<div class="row ${extra}">${MARKS}${inner}</div>`;

function hero(n, titleKey, leadKey, noteKey) {
  return `<section class="hero">
      <div class="scroll-hint">${t("scroll")}<i></i></div>
      <div class="eyebrow" data-reveal><span class="tag">.0${n}</span><span>${esc(t("nav_" + PAGES[n - 1]))}</span></div>
      <h1 class="display" data-split>${esc(t(titleKey))}</h1>
      <div class="hero-bottom">
        <p class="lead" data-reveal>${t(leadKey)}</p>
        <div class="hero-note" data-reveal>${esc(t(noteKey))}</div>
      </div>
    </section>`;
}

function sectionHead(numLabel, titleKey, subKey, textKey) {
  return `<div class="section-head" data-reveal>
      <span class="num">${numLabel}</span>
      <h2>${esc(t(titleKey))}<em>${esc(t(subKey))}</em></h2>
      ${textKey ? `<p>${esc(t(textKey))}</p>` : ""}
    </div>`;
}

function kpi(id, labelKey, icon, unit = "") {
  return `<div class="cell kpi" data-reveal>
      <div class="kpi-label">${ICONS[icon] || ""}<span>${esc(t(labelKey))}</span></div>
      <div><div class="kpi-value"><span id="${id}">—</span>${unit ? `<small>${unit}</small>` : ""}</div>
      <div class="kpi-sub" id="${id}-sub">&nbsp;</div>
      <div class="kpi-bar"><i id="${id}-bar"></i></div></div>
    </div>`;
}

function card(id, titleKey, subKey, body, meta = "") {
  return `<div class="card" data-reveal>
      <div class="card-head"><div class="card-title">${esc(t(titleKey))}<em>${esc(t(subKey))}</em></div>
      <div class="card-meta" id="${id}-meta">${meta}</div></div>
      ${body}
    </div>`;
}

const chartBox = (id, size = "") => `<div class="chart-box ${size}"><canvas id="${id}"></canvas></div>`;

function footer() {
  return `<div class="marquee" aria-hidden="true"><div class="marquee-track">${
    [...t("marquee"), ...t("marquee")].map((w, i) => `<span class="${i % 2 ? "outline" : ""}">${esc(w)}</span>`).join("")
  }</div></div>
  <div class="footer"><span>${esc(t("footer_data"))}</span><span>${esc(t("footer_built"))}</span><span>Chengho Fongang Marie Louise · 2026</span></div>`;
}

const ICONS = {
  speed: '<svg viewBox="0 0 24 24"><path d="M4 16a8 8 0 1 1 16 0M12 16l4-5"/></svg>',
  car: '<svg viewBox="0 0 24 24"><path d="M4 15h16v-3l-2-5H6l-2 5zM7 18v-3M17 18v-3"/></svg>',
  clock: '<svg viewBox="0 0 24 24"><circle cx="12" cy="12" r="8"/><path d="M12 8v4l3 2"/></svg>',
  sensor: '<svg viewBox="0 0 24 24"><path d="M12 13v8M8 9a5 5 0 0 1 8 0M5 6a9 9 0 0 1 14 0"/></svg>',
  bolt: '<svg viewBox="0 0 24 24"><path d="M13 3L5 14h6l-1 7 8-11h-6z"/></svg>',
  alert: '<svg viewBox="0 0 24 24"><path d="M12 4l9 16H3zM12 10v4M12 17v1"/></svg>',
  layers: '<svg viewBox="0 0 24 24"><path d="M12 4l9 5-9 5-9-5zM3 14l9 5 9-5"/></svg>',
  db: '<svg viewBox="0 0 24 24"><ellipse cx="12" cy="6" rx="7" ry="3"/><path d="M5 6v12c0 1.7 3.1 3 7 3s7-1.3 7-3V6M5 12c0 1.7 3.1 3 7 3s7-1.3 7-3"/></svg>',
  copy: '<svg viewBox="0 0 24 24"><rect x="8" y="8" width="12" height="12"/><path d="M16 8V4H4v12h4"/></svg>',
  shield: '<svg viewBox="0 0 24 24"><path d="M12 3l8 3v6c0 5-3.5 8-8 9-4.5-1-8-4-8-9V6z"/><path d="M9 12l2 2 4-4"/></svg>',
  stream: '<svg viewBox="0 0 24 24"><path d="M3 7h12M3 12h18M3 17h9"/></svg>',
  cpu: '<svg viewBox="0 0 24 24"><rect x="6" y="6" width="12" height="12"/><path d="M9 2v4M15 2v4M9 18v4M15 18v4M2 9h4M2 15h4M18 9h4M18 15h4"/></svg>',
  flow: '<svg viewBox="0 0 24 24"><path d="M4 6h6v6H4zM14 12h6v6h-6zM10 9h4v6"/></svg>',
  screen: '<svg viewBox="0 0 24 24"><rect x="3" y="4" width="18" height="12"/><path d="M8 20h8M12 16v4"/></svg>',
  arrow: '<svg viewBox="0 0 24 24"><path d="M5 12h14M13 6l6 6-6 6"/></svg>',
};

/* ================================================================ charts */
const COLORS = { white: "#ffffff", ink: "#111111", green: "#1b4d3e", greenSoft: "#3f7d69", amber: "#e0b04f", red: "#e2553a", grey: "rgba(255,255,255,0.45)" };
const CLASS_COLORS = { boulevard: COLORS.white, arterial: COLORS.ink, local: COLORS.green };
const QUERY_COLORS = { traffic_raw: COLORS.white, traffic_windows: COLORS.grey, transit_raw: COLORS.ink, transit_windows: COLORS.green };

function chartDefaults() {
  Chart.defaults.color = "rgba(255,255,255,0.78)";
  Chart.defaults.font.family = "'Geist Mono', monospace";
  Chart.defaults.font.size = 10;
  Chart.defaults.borderColor = "rgba(255,255,255,0.14)";
  Chart.defaults.plugins.legend.display = false;
  Chart.defaults.plugins.tooltip.backgroundColor = "#111";
  Chart.defaults.plugins.tooltip.borderColor = "#333";
  Chart.defaults.plugins.tooltip.borderWidth = 1;
  Chart.defaults.plugins.tooltip.cornerRadius = 3;
  Chart.defaults.plugins.tooltip.padding = 10;
  Chart.defaults.animation.duration = REDUCED ? 0 : 900;
  Chart.defaults.elements.line.tension = 0.3;
  Chart.defaults.elements.point.radius = 0;
  Chart.defaults.elements.point.hoverRadius = 4;
  Chart.defaults.interaction.mode = "index";
  Chart.defaults.interaction.intersect = false;
  Chart.defaults.maintainAspectRatio = false;
}

/* Create the chart the first time, update it in place afterwards (no flicker on refresh). */
function setChart(id, config) {
  const canvas = document.getElementById(id);
  if (!canvas) return;
  const existing = state.charts[id];
  if (existing && existing.canvas === canvas) {
    existing.data = config.data;
    existing.update("none");
    return;
  }
  if (existing) existing.destroy();
  state.charts[id] = new Chart(canvas, config);
}

function axes(yTitle, extra = {}) {
  return {
    x: { grid: { display: false }, ticks: { maxTicksLimit: 8, maxRotation: 0 } },
    y: { beginAtZero: true, title: { display: !!yTitle, text: yTitle }, ticks: { maxTicksLimit: 5 }, ...extra },
  };
}

/* turn [{t, key, value}] into labels + one series per key */
function pivot(rows, keyField, valueField) {
  const times = [...new Set(rows.map((r) => r.t))].sort();
  const keys = [...new Set(rows.map((r) => r[keyField]))].sort();
  const index = Object.fromEntries(rows.map((r) => [`${r.t}|${r[keyField]}`, r[valueField]]));
  return { labels: times.map(hhmm), keys, series: keys.map((k) => times.map((tm) => index[`${tm}|${k}`] ?? null)) };
}

const legend = (items) => `<div class="legend">${items.map(([c, l]) => `<span><i style="background:${c}"></i>${esc(l)}</span>`).join("")}</div>`;

/* ================================================================ pages */
const Pages = {};

/* ---------------------------------------------------------------- overview */
Pages.overview = {
  layout() {
    return hero(1, "ov_title", "ov_lead", "ov_note")
      + row(kpi("k-speed", "k_speed", "speed", "%") + kpi("k-vehicles", "k_vehicles", "car") + kpi("k-ontime", "k_ontime", "clock", "%") + kpi("k-sensors", "k_sensors", "sensor"))
      + row(`<div class="cell span-1">${sectionHead(".A", "s_trend", "s_trend_sub", "s_trend_p")}</div>
             <div class="span-3">${card("c-trend", "vehicles", "speed_ratio", chartBox("ch-trend", "tall") + legend([[COLORS.ink, t("vehicles")], [COLORS.white, t("speed_ratio")]]))}</div>`)
      + row(`<div class="span-2">${card("c-transit", "s_transit", "s_transit_sub", chartBox("ch-ontime"))}</div>
             <div class="cell span-1">${sectionHead(".B", "s_transit", "s_transit_sub", "s_transit_p")}</div>
             <div class="cell span-1 kpi" data-reveal><div class="kpi-label">${ICONS.alert}<span>${esc(t("k_congested"))}</span></div>
               <div><div class="kpi-value"><span id="k-congested">—</span><small>/ 84</small></div><div class="kpi-sub">${esc(t("k_congested_sub"))}</div>
               <a class="pill" href="#map" data-cursor="${esc(t("open_map"))}" style="margin-top:14px">${esc(t("open_map"))}</a></div></div>`)
      + row(`<div class="cell span-1">${sectionHead(".C", "s_health", "s_health_sub")}</div>`
        + kpi("k-eps", "k_events", "bolt") + kpi("k-latency", "k_latency", "clock", "s") + kpi("k-lag", "k_lag", "stream"))
      + footer();
  },
  async load() {
    const d = await api(`/api/overview?hours=${state.hours}`);
    const k = d.kpi;
    countUp($("#k-speed"), k.speed_ratio_now == null ? null : k.speed_ratio_now * 100, 0);
    $("#k-speed-sub").textContent = t("k_speed_sub");
    $("#k-speed-bar").style.width = `${Math.min(100, (k.speed_ratio_now || 0) * 100)}%`;
    countUp($("#k-vehicles"), k.vehicles, 0, (v) => num(v));
    $("#k-vehicles-sub").textContent = `${num(k.readings)} ${t("k_readings").toLowerCase()} · ${rangeLabel()}`;
    countUp($("#k-ontime"), k.on_time_pct, 1, (v) => nf(1).format(v));
    $("#k-ontime-sub").textContent = t("k_ontime_sub", { d: delay(k.avg_delay_s) });
    $("#k-ontime-bar").style.width = `${k.on_time_pct || 0}%`;
    countUp($("#k-sensors"), k.sensors_ok, 0, (v) => `${Math.round(v)}/${k.sensors_total}`);
    $("#k-sensors-sub").textContent = t("k_sensors_sub", { n: k.sensors_silent, i: k.open_incidents });
    $("#k-sensors-bar").style.width = `${(100 * k.sensors_ok) / (k.sensors_total || 1)}%`;
    countUp($("#k-congested"), k.congested_segments, 0);
    countUp($("#k-eps"), k.events_per_s, 1, (v) => nf(1).format(v));
    $("#k-eps-sub").textContent = "Kafka → Spark";
    countUp($("#k-latency"), k.latency_p50_s, 2, (v) => nf(2).format(v));
    $("#k-latency-sub").textContent = t("k_latency_sub");
    countUp($("#k-lag"), d.health.kafka_lag, 0, (v) => num(v));
    $("#k-lag-sub").textContent = t("health_" + d.health.status);
    updateHealth(d.health);

    $("#c-trend-meta").textContent = rangeLabel();
    setChart("ch-trend", {
      type: "bar",
      data: {
        labels: d.trend.map((r) => hhmm(r.t)),
        datasets: [
          { type: "bar", label: t("vehicles"), data: d.trend.map((r) => r.vehicles), backgroundColor: "rgba(17,17,17,0.55)", yAxisID: "y", barPercentage: 0.9, categoryPercentage: 1 },
          { type: "line", label: t("speed_ratio"), data: d.trend.map((r) => r.speed_ratio * 100), borderColor: COLORS.white, borderWidth: 2, yAxisID: "y1" },
        ],
      },
      options: { scales: { ...axes(t("vehicles")), y1: { position: "right", min: 0, max: 110, grid: { display: false }, ticks: { callback: (v) => `${v}%`, maxTicksLimit: 5 } } } },
    });
    $("#c-transit-meta").textContent = rangeLabel();
    setChart("ch-ontime", {
      type: "line",
      data: { labels: d.transit_trend.map((r) => hhmm(r.t)), datasets: [
        { label: t("on_time"), data: d.transit_trend.map((r) => r.on_time_pct), borderColor: COLORS.white, borderWidth: 2, fill: { target: "origin", above: "rgba(255,255,255,0.08)" } },
      ] },
      // max 105: a flat 100 % line stays visible instead of hiding on the top edge
      options: { scales: axes("%", { max: 105, ticks: { maxTicksLimit: 5, callback: (v) => (v <= 100 ? v : "") } }) },
    });
  },
};

/* ---------------------------------------------------------------- traffic */
Pages.traffic = {
  layout() {
    const classLegend = legend(Object.entries(CLASS_COLORS).map(([k, c]) => [c, t(k)]));
    return hero(2, "tr_title", "tr_lead", "tr_note")
      + row(`<div class="span-2">${card("c-class", "s_class", "s_class_sub", chartBox("ch-class") + classLegend)}</div>
             <div class="span-2">${card("c-speed", "s_speed", "s_speed_sub", chartBox("ch-speed") + classLegend)}</div>`)
      + row(`<div class="cell span-1">${sectionHead(".A", "s_profile", "s_profile_sub", "s_profile_p")}</div>
             <div class="span-3">${card("c-profile", "s_profile", "s_profile_sub", chartBox("ch-profile", "tall") + classLegend, "24 h")}</div>`)
      + row(`<div class="span-2">${card("c-districts", "s_districts", "s_districts_sub", '<div class="table-wrap" id="t-districts"></div>')}</div>
             <div class="span-2">${card("c-slow", "s_slowest", "s_slowest_sub", '<div class="table-wrap" id="t-slow"></div>')}</div>`)
      + footer();
  },
  async load() {
    const d = await api(`/api/traffic?hours=${state.hours}`);
    ["c-class", "c-speed"].forEach((id) => { $(`#${id}-meta`).textContent = rangeLabel(); });
    const vol = pivot(d.by_class, "road_class", "vehicles");
    setChart("ch-class", { type: "line", data: { labels: vol.labels, datasets: vol.keys.map((k, i) => ({
      label: t(k), data: vol.series[i], borderColor: CLASS_COLORS[k], backgroundColor: CLASS_COLORS[k] + "33", borderWidth: 2, fill: true })) },
      options: { scales: axes(t("vehicles")) } });
    const sp = pivot(d.by_class, "road_class", "avg_speed_kmh");
    setChart("ch-speed", { type: "line", data: { labels: sp.labels, datasets: sp.keys.map((k, i) => ({
      label: t(k), data: sp.series[i], borderColor: CLASS_COLORS[k], borderWidth: 2 })) },
      options: { scales: axes("km/h") } });
    const hours = [...Array(24).keys()];
    const classes = Object.keys(CLASS_COLORS);
    setChart("ch-profile", { type: "bar", data: { labels: hours.map((h) => `${String(h).padStart(2, "0")}h`), datasets: classes.map((c) => ({
      label: t(c), backgroundColor: CLASS_COLORS[c],
      data: hours.map((h) => (d.profile.find((p) => p.hour_of_day === h && p.road_class === c) || {}).avg_vehicles_per_min ?? null) })) },
      options: { scales: axes(t("vehicles") + " / min") } });

    const maxV = Math.max(1, ...d.districts.map((r) => r.vehicles));
    $("#t-districts").innerHTML = `<table class="table"><thead><tr><th>${t("district")}</th><th>${t("vehicles")}</th><th class="r">${t("speed")}</th><th class="r">${t("speed_ratio")}</th></tr></thead><tbody>${
      d.districts.map((r) => `<tr><td>${esc(r.district)}</td>
        <td><div class="bar-cell"><span>${num(r.vehicles)}</span><div class="bar"><i style="width:${(100 * r.vehicles) / maxV}%"></i></div></div></td>
        <td class="r">${num(r.avg_speed_kmh, 1)} km/h</td><td class="r">${pct(r.speed_ratio * 100, 0)}</td></tr>`).join("")}</tbody></table>`;
    $("#t-slow").innerHTML = `<table class="table"><thead><tr><th>${t("segment")}</th><th class="r">${t("speed")}</th><th class="r">${t("level")}</th></tr></thead><tbody>${
      d.slowest_segments.map((r) => `<tr><td>${esc(r.road_name)}<br><span style="color:var(--muted)">${esc(r.segment_id)} · ${esc(t(r.road_class))}</span></td>
        <td class="r">${num(r.avg_speed_kmh, 1)} / ${r.speed_limit_kmh}</td><td class="r"><span class="pill ${r.congestion_level}">${esc(t(r.congestion_level))}</span></td></tr>`).join("")}</tbody></table>`;
  },
};

/* ---------------------------------------------------------------- map */
const LEVEL_COLORS = { free_flow: "#ededed", dense: COLORS.amber, congested: COLORS.red, no_data: "#555" };

Pages.map = {
  layout() {
    return hero(3, "map_title", "map_lead", "map_note")
      + `<div class="band-dark">${row(`<div class="span-4" style="padding:22px 0 0">
          <div class="map-wrap" data-reveal="clip">
            <div id="city-map"></div>
            <div class="map-panel">
              <h3>${esc(t("map_panel"))}</h3>
              <div class="hint">${esc(t("map_hint"))}</div>
              <div class="layer-toggles">
                <button class="on" data-layer="roads">${esc(t("layer_roads"))}</button>
                <button class="on" data-layer="sensors">${esc(t("layer_sensors"))}</button>
                <button class="on" data-layer="transit">${esc(t("layer_transit"))}</button>
                <button data-layer="districts">${esc(t("layer_districts"))}</button>
              </div>
              <div class="map-legend">
                ${["free_flow", "dense", "congested", "no_data"].map((l) => `<span><i class="lg-line" style="background:${LEVEL_COLORS[l]}"></i>${esc(t(l))}</span>`).join("")}
                <span><i class="lg-line" style="height:8px;width:8px;background:#f2f2f2"></i>${esc(t("layer_sensors"))} · ${esc(t("st_OK"))}</span>
                <span><i class="lg-line" style="height:8px;width:8px;background:${COLORS.red}"></i>${esc(t("layer_sensors"))} · ${esc(t("st_SILENT"))}</span>
              </div>
              <div class="map-info" id="map-info"></div>
            </div>
          </div></div>`)}</div>`
      + footer();
  },
  init() {
    state.map = L.map("city-map", { zoomControl: false, scrollWheelZoom: false, attributionControl: true }).setView([45.8336, 1.2611], 14);
    L.control.zoom({ position: "topright" }).addTo(state.map);   // the layer panel sits on the left
    L.tileLayer("https://tile.openstreetmap.org/{z}/{x}/{y}.png", {
      maxZoom: 19,
      attribution: '&copy; <a href="https://www.openstreetmap.org/copyright">OpenStreetMap</a>',
    }).addTo(state.map);
    state.mapLayers = { districts: L.layerGroup(), roads: L.layerGroup(), transit: L.layerGroup(), sensors: L.layerGroup() };
    ["roads", "transit", "sensors"].forEach((k) => state.mapLayers[k].addTo(state.map));
    document.querySelectorAll(".layer-toggles button").forEach((b) => b.addEventListener("click", () => {
      const layer = state.mapLayers[b.dataset.layer];
      b.classList.toggle("on");
      if (b.classList.contains("on")) layer.addTo(state.map); else state.map.removeLayer(layer);
    }));
    state.map.on("click", () => state.map.scrollWheelZoom.enable());   // zoom with the wheel after a click
  },
  info(html) { $("#map-info").innerHTML = html; },
  async load() {
    if (!state.map) this.init();
    const d = await api("/api/map");
    const Ls = state.mapLayers;
    Object.values(Ls).forEach((g) => g.clearLayers());

    L.geoJSON(d.districts, {
      style: (f) => ({ color: "#ffffff", weight: 1, dashArray: "4 4", fillColor: "#ffffff", fillOpacity: 0.04 + 0.12 * (1 - (f.properties.speed_ratio || 1)) }),
      onEachFeature: (f, layer) => layer.on("mouseover", () => this.info(`<b>${esc(f.properties.name)}</b>${t("speed_ratio")} : ${pct((f.properties.speed_ratio || 0) * 100, 0)}`)),
    }).addTo(Ls.districts);

    L.geoJSON(d.roads, {
      style: (f) => ({ color: LEVEL_COLORS[f.properties.congestion_level], weight: f.properties.road_class === "boulevard" ? 6 : f.properties.road_class === "arterial" ? 5 : 3, opacity: 0.95, lineCap: "butt" }),
      onEachFeature: (f, layer) => {
        const p = f.properties;
        layer.on("mouseover", () => { layer.setStyle({ weight: 9 }); this.info(`<b>${esc(p.name)}</b>${esc(p.segment_id)} · ${esc(t(p.road_class))} · ${p.speed_limit_kmh} km/h<br>${t("speed")} : ${num(p.avg_speed_kmh, 1)} km/h · <span class="pill ${p.congestion_level}">${esc(t(p.congestion_level))}</span>`); });
        layer.on("mouseout", () => layer.setStyle({ weight: p.road_class === "boulevard" ? 6 : p.road_class === "arterial" ? 5 : 3 }));
      },
    }).addTo(Ls.roads);

    L.geoJSON(d.lines, { style: (f) => ({ color: f.properties.color, weight: 2, opacity: 0.9, dashArray: f.properties.mode === "tram" ? null : "6 5", offset: 4 }) }).addTo(Ls.transit);
    const lineColor = Object.fromEntries(d.lines.features.map((f) => [f.properties.line_id, f.properties.color]));
    L.geoJSON(d.stops, {
      pointToLayer: (f, latlng) => L.circleMarker(latlng, { radius: 5, color: "#fff", weight: 1.5, fillColor: lineColor[f.properties.line_id], fillOpacity: 1 }),
      onEachFeature: (f, layer) => layer.on("mouseover", () => this.info(`<b>${esc(f.properties.name)}</b>${t("line")} ${esc(f.properties.line_id)} · ${esc(f.properties.stop_id)}<br>${t("avg_delay")} (1 h) : ${delay(f.properties.avg_delay_s)} · ${num(f.properties.stop_events)} ${t("stop_events").toLowerCase()}`)),
    }).addTo(Ls.transit);

    L.geoJSON(d.sensors, {
      pointToLayer: (f, latlng) => L.marker(latlng, { icon: L.divIcon({ className: "", html: `<div class="sensor-icon ${f.properties.status}"></div>`, iconSize: [9, 9], iconAnchor: [5, 5] }) }),
      onEachFeature: (f, layer) => layer.on("mouseover", () => {
        const p = f.properties;
        this.info(`<b>${esc(p.sensor_id)} · ${esc(p.road_name || "")}</b><span class="pill ${String(p.status).toLowerCase()}">${esc(t("st_" + p.status))}</span> ${p.seconds_since_last != null ? t("last_reading", { s: num(p.seconds_since_last) }) : ""}<br>${esc(p.district_name || "")} · ${t("snap", { m: num(p.snap_distance_m, 1) })}`);
      }),
    }).addTo(Ls.sensors);
    setTimeout(() => state.map && state.map.invalidateSize(), 300);
  },
  leave() { if (state.map) { state.map.remove(); state.map = null; } },
};

/* ---------------------------------------------------------------- transit */
Pages.transit = {
  layout() {
    return hero(4, "tt_title", "tt_lead", "tt_note")
      + row(`<div class="span-4"><div class="line-cards" id="line-cards"></div></div>`)
      + row(`<div class="cell span-1">${sectionHead(".A", "s_delay", "s_delay_sub", "s_delay_p")}</div>
             <div class="span-3">${card("c-delay", "s_delay", "s_delay_sub", chartBox("ch-delay", "tall") + '<div id="delay-legend"></div>')}</div>`)
      + row(`<div class="span-3">${card("c-stops", "s_stops", "s_stops_sub", '<div class="table-wrap" id="t-stops"></div>')}</div>
             <div class="cell span-1">${sectionHead(".B", "s_stops", "s_stops_sub", "s_stops_p")}</div>`)
      + footer();
  },
  async load() {
    const d = await api(`/api/transit?hours=${state.hours}`);
    $("#line-cards").innerHTML = d.lines.map((l) => `<div class="line-card" data-reveal>
        <div class="code" style="color:${l.color}">${esc(l.line_id)}</div>
        <div class="name">${esc(l.name)}</div>
        <dl><dt>${t("on_time")}</dt><dd>${pct(l.on_time_pct)}</dd>
            <dt>${t("avg_delay")}</dt><dd>${delay(l.avg_delay_s)}</dd>
            <dt>${t("p90")}</dt><dd>${delay(l.p90_delay_s)}</dd>
            <dt>${t("trips")}</dt><dd>${num(l.trips)}</dd>
            <dt>${t("passengers")}</dt><dd>${num(l.avg_passengers)}</dd></dl></div>`).join("") || `<div class="empty">—</div>`;
    enableReveal($("#line-cards"));
    $("#c-delay-meta").textContent = rangeLabel();
    const colors = Object.fromEntries(d.lines.map((l) => [l.line_id, l.color]));
    const p = pivot(d.trend, "line_id", "avg_delay_s");
    setChart("ch-delay", { type: "line", data: { labels: p.labels, datasets: p.keys.map((k, i) => ({
      label: k, data: p.series[i].map((v) => (v == null ? null : v / 60)), borderColor: colors[k] || COLORS.white, borderWidth: 2, spanGaps: true })) },
      options: { scales: axes("min", { beginAtZero: false }) } });
    $("#delay-legend").innerHTML = legend(p.keys.map((k) => [colors[k] || COLORS.white, k]));
    $("#t-stops").innerHTML = `<table class="table"><thead><tr><th>${t("stop")}</th><th>${t("line")}</th><th>${t("district")}</th><th class="r">${t("avg_delay")}</th><th class="r">${t("on_time")}</th><th class="r">${t("nearby_speed")}</th></tr></thead><tbody>${
      d.worst_stops.map((s) => `<tr><td>${esc(s.stop_name)}<br><span style="color:var(--muted)">${esc(s.stop_id)}</span></td>
        <td><span class="swatch" style="background:${colors[s.line_id] || "#fff"}"></span>${esc(s.line_id)}</td><td>${esc(s.district_name)}</td>
        <td class="r">${delay(s.avg_delay_s)}</td><td class="r">${pct(s.on_time_pct)}</td>
        <td class="r">${s.nearby_speed_ratio != null ? `${pct(s.nearby_speed_ratio * 100, 0)} <span style="color:var(--muted)">(${esc(s.nearest_sensor_id)}, ${num(s.nearest_sensor_m)} m)</span>` : "—"}</td></tr>`).join("")}</tbody></table>`;
  },
};

/* ---------------------------------------------------------------- pipeline */
Pages.pipeline = {
  layout() {
    const steps = [["screen", "f_producer"], ["stream", "f_kafka"], ["cpu", "f_spark"], ["db", "f_postgis"], ["flow", "f_airflow"], ["layers", "f_api"]];
    return hero(5, "pl_title", "pl_lead", "pl_note")
      + row(`<div class="span-4 cell"><div class="flow" data-reveal>${steps.map(([i, k]) =>
          `<div class="flow-step"><div class="box">${ICONS[i]}</div><b>${esc(t(k))}</b><span>${esc(t(k + "_s"))}</span></div>`).join("")}</div></div>`)
      + row(kpi("k-p50", "k_latency", "clock", "s") + kpi("k-p95", "k_p95", "clock", "s") + kpi("k-batches", "k_batches", "layers") + kpi("k-runs", "k_runs", "cpu"))
      + row(`<div class="span-2">${card("c-tp", "s_throughput", "s_throughput_sub", chartBox("ch-tp") + legend(Object.entries(QUERY_COLORS).map(([k, c]) => [c, k])))}</div>
             <div class="span-2">${card("c-lag", "s_lag", "s_lag_sub", chartBox("ch-lag") + legend(Object.entries(QUERY_COLORS).map(([k, c]) => [c, k])))}</div>`)
      + row(`<div class="span-2">${card("c-batch", "s_batch", "s_batch_sub", chartBox("ch-batch"))}</div>
             <div class="span-2">${card("c-checks", "s_checks", "s_checks_sub", '<div class="table-wrap" id="t-checks"></div>')}</div>`)
      + row(`<div class="cell span-1">${sectionHead(".A", "s_log", "s_log_sub")}</div>
             <div class="span-3">${card("c-log", "s_log", "s_log_sub", '<ul class="log" id="log"></ul>')}</div>`)
      + footer();
  },
  async load() {
    const d = await api(`/api/pipeline?hours=${state.hours}`);
    updateHealth(d.health);
    countUp($("#k-p50"), d.latency.p50_s, 2, (v) => nf(2).format(v));
    $("#k-p50-sub").textContent = t("k_latency_sub");
    countUp($("#k-p95"), d.latency.p95_s, 2, (v) => nf(2).format(v));
    $("#k-p95-sub").textContent = `max ${num(d.latency.max_s, 1)} s`;
    countUp($("#k-batches"), d.totals.batches, 0, (v) => num(v));
    $("#k-batches-sub").textContent = `${num(d.totals.messages_read)} messages · ${rangeLabel()}`;
    countUp($("#k-runs"), d.totals.spark_runs, 0);
    $("#k-runs-sub").textContent = t("k_runs_sub");
    ["c-tp", "c-lag", "c-batch"].forEach((id) => { $(`#${id}-meta`).textContent = rangeLabel(); });

    const tp = pivot(d.series, "query_name", "events_per_s");
    setChart("ch-tp", { type: "line", data: { labels: tp.labels, datasets: tp.keys.map((k, i) => ({ label: k, data: tp.series[i], borderColor: QUERY_COLORS[k], borderWidth: 2 })) }, options: { scales: axes("ev/s") } });
    const lag = pivot(d.series, "query_name", "kafka_lag");
    setChart("ch-lag", { type: "line", data: { labels: lag.labels, datasets: lag.keys.map((k, i) => ({ label: k, data: lag.series[i], borderColor: QUERY_COLORS[k], borderWidth: 2, stepped: true })) }, options: { scales: axes("messages") } });
    const bt = pivot(d.series.filter((r) => r.query_name === "traffic_raw"), "query_name", "batch_ms");
    setChart("ch-batch", { type: "bar", data: { labels: bt.labels, datasets: [{ label: "traffic_raw", data: bt.series[0] || [], backgroundColor: "rgba(255,255,255,0.7)" }] }, options: { scales: axes("ms") } });

    $("#t-checks").innerHTML = d.checks.length ? `<table class="table"><tbody>${d.checks.map((c) => `<tr><td>${esc(c.check_name)}</td><td class="r">${num(c.failed_rows)}</td>
      <td class="r"><span class="pill ${c.passed ? "pass" : "fail"}">${c.passed ? t("pass") : t("fail")}</span></td></tr>`).join("")}</tbody></table>
      <div class="legend">${dateTime(d.checks[0].checked_at)}</div>` : `<div class="empty">—</div>`;
    $("#log").innerHTML = d.events.map((e) => `<li><time>${dateTime(e.logged_at)}</time><div><span class="pill">${esc(e.event_type)}</span> ${esc(e.query_name || "")}<span class="details">${esc(e.details || "")}</span></div></li>`).join("");
  },
};

/* ---------------------------------------------------------------- data quality */
Pages.quality = {
  layout() {
    return hero(6, "q_title", "q_lead", "q_note")
      + row(kpi("k-invalid", "k_invalid", "alert") + kpi("k-dupes", "k_dupes", "copy") + kpi("k-late", "k_late", "clock") + kpi("k-compl", "k_completeness", "shield", "%"))
      + row(`<div class="span-2">${card("c-invalid", "s_invalid", "s_invalid_sub", '<div class="table-wrap" id="t-invalid"></div>')}</div>
             <div class="span-1">${card("c-versions", "s_versions", "s_versions_sub", chartBox("ch-versions", "short"))}</div>
             <div class="span-1">${card("c-sstatus", "s_sensor_status", "s_sensor_status_sub", '<div id="t-sstatus"></div>')}</div>`)
      + row(`<div class="span-2">${card("c-compl", "s_completeness", "s_completeness_sub", chartBox("ch-compl"))}</div>
             <div class="span-2">${card("c-inc", "s_incidents", "s_incidents_sub", '<div class="table-wrap" id="t-inc"></div>')}</div>`)
      + footer();
  },
  async load() {
    const d = await api(`/api/quality?hours=${state.hours}`);
    const totalInvalid = d.invalid.reduce((s, r) => s + r.records, 0);
    countUp($("#k-invalid"), totalInvalid, 0, (v) => num(v));
    $("#k-invalid-sub").textContent = rangeLabel();
    countUp($("#k-dupes"), d.batches.duplicates_skipped, 0, (v) => num(v));
    $("#k-dupes-sub").textContent = `${pct((100 * d.batches.duplicates_skipped) / (d.batches.valid_received || 1))} · ${rangeLabel()}`;
    countUp($("#k-late"), d.late.late, 0, (v) => num(v));
    $("#k-late-sub").textContent = t("k_late_sub", { p: num(d.late.beyond_watermark) });
    const last = d.completeness[d.completeness.length - 1];
    countUp($("#k-compl"), last ? last.completeness_pct : null, 1, (v) => nf(1).format(v));
    $("#k-compl-sub").textContent = t("k_completeness_sub");
    if (last) $("#k-compl-bar").style.width = `${last.completeness_pct}%`;

    $("#c-invalid-meta").textContent = rangeLabel();
    const maxI = Math.max(1, ...d.invalid.map((r) => r.records));
    $("#t-invalid").innerHTML = d.invalid.length ? `<table class="table"><thead><tr><th>${t("reason")}</th><th>${t("source")}</th><th>${t("records")}</th></tr></thead><tbody>${
      d.invalid.map((r) => `<tr><td>${esc(r.error_reason)}</td><td>${esc(r.source)}</td>
        <td><div class="bar-cell"><span>${num(r.records)}</span><div class="bar"><i style="width:${(100 * r.records) / maxI}%"></i></div></div></td></tr>`).join("")}</tbody></table>` : `<div class="empty">0</div>`;
    setChart("ch-versions", { type: "bar", data: { labels: d.schema_versions.map((v) => `v${v.schema_version}`), datasets: [{ data: d.schema_versions.map((v) => v.readings), backgroundColor: [COLORS.white, COLORS.ink, COLORS.green] }] },
      options: { indexAxis: "y", scales: { x: { beginAtZero: true, ticks: { maxTicksLimit: 3 } }, y: { grid: { display: false } } } } });
    $("#t-sstatus").innerHTML = `<table class="table"><tbody>${d.sensor_status.map((s) => `<tr><td><span class="pill ${s.status.toLowerCase()}">${esc(t("st_" + s.status))}</span></td><td class="r" style="font-family:var(--display);font-size:34px">${num(s.sensors)}</td></tr>`).join("")}</tbody></table>`;
    setChart("ch-compl", { type: "line", data: { labels: d.completeness.map((r) => hhmm(r.t)), datasets: [
      { label: "%", data: d.completeness.map((r) => r.completeness_pct), borderColor: COLORS.white, borderWidth: 2, fill: { target: "origin", above: "rgba(255,255,255,0.08)" } }] },
      options: { scales: axes("%", { min: 80, max: 100, beginAtZero: false }) } });
    $("#t-inc").innerHTML = d.incidents.length ? `<table class="table"><thead><tr><th>${t("sensors")}</th><th>${t("opened")}</th><th>${t("closed")}</th><th class="r">${t("duration")}</th></tr></thead><tbody>${
      d.incidents.map((i) => `<tr><td>${esc(i.sensor_id)}</td><td>${dateTime(i.opened_at)}</td>
        <td>${i.closed_at ? dateTime(i.closed_at) : `<span class="pill open">${t("still_open")}</span>`}</td><td class="r">${num(i.minutes)} ${t("minutes")}</td></tr>`).join("")}</tbody></table>` : `<div class="empty">—</div>`;
  },
};

/* ================================================================ app shell */
function updateHealth(h) {
  if (!h) return;
  ["#health-dot", "#health-dot-mobile"].forEach((s) => { $(s).className = `health-dot ${h.status}`; });
  $("#health-text").textContent = t("health_" + h.status);
}

function updateMode() {
  const m = state.meta;
  $("#mode-text").textContent = m.mode === "demo" ? `${t("mode_demo")} ${dateTime(m.snapshot_at)}` : t("mode_live");
  const banner = $("#demo-banner");
  banner.hidden = m.mode !== "demo";
  if (m.mode === "demo") banner.textContent = t("demo_banner", { date: dateTime(m.snapshot_at) });
}

async function loadPage() {
  try {
    await Pages[state.page].load();
    showError(null);
  } catch (err) {
    console.error(err);
    showError(err);
  }
}

function render() {
  const view = $("#view");
  view.innerHTML = `<div class="page">${Pages[state.page].layout()}</div>`;
  view.querySelectorAll("[data-split]").forEach(splitWords);
  enableReveal(view);
  enableTilt(view);
  document.querySelectorAll(".nav a").forEach((a) => a.classList.toggle("active", a.dataset.page === state.page));
  return loadPage();
}

async function go(page, animate = true) {
  if (!PAGES.includes(page)) page = "overview";
  if (page === state.page && $("#view").children.length) return;
  const previous = Pages[state.page];
  const swap = () => {
    if (previous && previous.leave) previous.leave();
    Object.values(state.charts).forEach((c) => c.destroy());
    state.charts = {};
    state.page = page;
    render();
  };
  $("#sidebar").classList.remove("open");
  if (animate) await pageTransition(t("nav_" + page), swap); else swap();
}

function startRefresh() {
  clearInterval(state.timer);
  if (state.meta.mode !== "live") return;
  state.timer = setInterval(async () => {
    if (document.hidden) return;
    loadPage();
    if (!["overview", "pipeline"].includes(state.page)) api("/api/health").then(updateHealth).catch(() => {});
  }, 10000);
}

async function init() {
  const finishLoader = runLoader();
  chartDefaults();
  applyStaticTexts();
  Cursor.init();
  Scroll.init();

  document.querySelectorAll("#lang button").forEach((b) => b.classList.toggle("active", b.dataset.lang === LANG));
  document.querySelectorAll("#range button").forEach((b) => b.addEventListener("click", () => {
    document.querySelectorAll("#range button").forEach((x) => x.classList.toggle("active", x === b));
    state.hours = Number(b.dataset.hours);
    loadPage();
  }));
  document.querySelectorAll("#lang button").forEach((b) => b.addEventListener("click", () => {
    LANG = b.dataset.lang;
    try { localStorage.setItem("lang", LANG); } catch (e) { /* storage blocked */ }
    document.querySelectorAll("#lang button").forEach((x) => x.classList.toggle("active", x === b));
    applyStaticTexts();
    updateMode();
    const page = state.page;
    if (Pages[page].leave) Pages[page].leave();
    Object.values(state.charts).forEach((c) => c.destroy());
    state.charts = {};
    render();
  }));
  $("#menu-open").addEventListener("click", () => $("#sidebar").classList.add("open"));
  $("#menu-close").addEventListener("click", () => $("#sidebar").classList.remove("open"));
  window.addEventListener("hashchange", () => go(location.hash.slice(1)));

  try { state.meta = await api("/api/meta"); } catch (e) { showError(e); }
  updateMode();
  api("/api/health").then(updateHealth).catch(() => {});
  state.page = null;
  await go(location.hash.slice(1) || "overview", false);
  finishLoader();
  startRefresh();
}

init();
