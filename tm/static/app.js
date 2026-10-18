"use strict";
// tm monitor: polls /api/state, draws runners and jobs; a job's card polls /api/job.
const STATES = ["queued", "starting", "running", "done", "failed", "cancelled", "lost"];
const LABEL = {queued: "queued", starting: "starting", running: "running", done: "done",
               failed: "failed", cancelled: "cancelled", lost: "lost"};
const POLL = 4000;
let state = null, openKey = null, detailTimer = null;
const sel = {runner: "", states: new Set(["queued", "starting", "running", "failed", "lost"]), text: ""};

const $ = (s) => document.querySelector(s);
const el = (tag, attrs = {}, ...kids) => {
  const e = document.createElement(tag);
  for (const [k, v] of Object.entries(attrs)) {
    if (k === "class") e.className = v; else if (k.startsWith("on")) e.addEventListener(k.slice(2), v);
    else if (v !== null && v !== undefined && v !== false) e.setAttribute(k, v);
  }
  for (const k of kids.flat(Infinity)) if (k !== null && k !== undefined) e.append(k instanceof Node ? k : String(k));
  return e;
};

function loadPrefs() {
  try {
    const p = JSON.parse(localStorage.getItem("tm.filters") || "null");
    if (p) { sel.runner = p.runner || ""; sel.states = new Set(p.states || []); }
  } catch (e) { /* private window */ }
}
function savePrefs() {
  try { localStorage.setItem("tm.filters", JSON.stringify({runner: sel.runner, states: [...sel.states]})); } catch (e) {}
}

function dur(sec) {
  if (sec == null || sec < 0) return "";
  sec = Math.round(sec);
  const d = Math.floor(sec / 86400), h = Math.floor(sec % 86400 / 3600), m = Math.floor(sec % 3600 / 60), s = sec % 60;
  if (d) return `${d}d ${h}h`;
  if (h) return `${h}h ${String(m).padStart(2, "0")}m`;
  if (m) return `${m}m ${String(s).padStart(2, "0")}s`;
  return `${s}s`;
}
function when(ts) {
  if (!ts) return "";
  const d = new Date(ts * 1000);
  return d.toLocaleString(undefined, {day: "2-digit", month: "2-digit", hour: "2-digit", minute: "2-digit"});
}
function gb(b) { return b == null ? "?" : (b / 2 ** 30).toFixed(1) + " GB"; }
function elapsed(j, now) {
  const start = j.started || null;
  if (!start) return j.state === "queued" ? "waiting " + dur(now - j.submitted) : "";
  return dur((j.finished || now) - start);
}
function stateBadge(j) {
  const t = j.state === "failed" && j.exit ? `failed (exit ${j.exit})` : LABEL[j.state] || j.state;
  return el("span", {class: "badge s-" + j.state, title: t}, t);
}
function progressBar(p) {
  if (!p) return "";
  const pct = p.total ? Math.min(100, Math.round(100 * p.done / p.total)) : null;
  return el("div", {class: "prog", title: p.total ? `${p.done}/${p.total}` : `${p.done} files`},
    el("div", {class: "prog-bar", style: pct == null ? "width:100%;opacity:.25" : `width:${pct}%`}),
    el("span", {}, p.total ? `${p.done}/${p.total} · ${pct}%` : `${p.done}`));
}

// ETA from the monitor: seconds left, pieces an hour and what the rate was read from
function etaText(j, now) {
  const e = j.eta;
  if (!e) return "";
  const at = new Date((now + e.eta) * 1000);
  const sameDay = at.toDateString() === new Date(now * 1000).toDateString();
  const clock = at.toLocaleString(undefined, sameDay ? {hour: "2-digit", minute: "2-digit"} : {day: "2-digit", month: "2-digit", hour: "2-digit", minute: "2-digit"});
  return `≈ ${dur(e.eta)}, at ${clock}`;
}
function etaTitle(e) {
  return e ? `${e.rate} an hour, ${e.basis === "recent" ? "over the last 30 min" : "since the start"}` : "";
}

async function api(path, opts = {}) {
  const r = await fetch(path, Object.assign({credentials: "same-origin", headers: {"X-TM": "1"}}, opts));
  if (r.status === 401) { location.href = "/login"; throw new Error("sign in"); }
  const ct = r.headers.get("Content-Type") || "";
  const body = ct.includes("json") ? await r.json() : await r.text();
  if (!r.ok) throw new Error(body.error || body || r.status);
  return body;
}

function drawRunners() {
  const box = $("#runners");
  box.replaceChildren(...state.runners.map((r) => {
    const cores = el("div", {class: "cores"}, r.cores.map((c) => {
      const busy = c.busy == null ? 0 : c.busy;
      const tip = `core ${c.cpu}: ${c.busy == null ? "?" : busy + "%"}` + (c.jobs.length ? "\n" + c.jobs.map((k) => k.split("/").pop()).join("\n") : "") + (c.pool ? "" : "\n(outside the tm pool)");
      return el("div", {class: "core" + (c.jobs.length ? " claimed" : "") + (c.pool ? "" : " outside"), title: tip,
                        onclick: c.jobs.length ? () => openJob(c.jobs[0]) : null},
        el("div", {class: "core-fill", style: `height:${busy}%`}), el("span", {}, c.cpu));
    }));
    const memUsed = r.mem_total && r.mem_available != null ? r.mem_total - r.mem_available : null;
    return el("div", {class: "runner" + (r.online ? "" : " offline")},
      el("div", {class: "runner-head"},
        el("strong", {}, r.name), " ",
        el("span", {class: "muted"}, r.online ? `${r.hostname || ""} · load ${(r.load || []).map((x) => x.toFixed(1)).join(" ")} · memory ${gb(memUsed)} of ${gb(r.mem_total)}` : "offline: " + (r.error || "not polled yet")),
        r.queued ? el("span", {class: "badge s-queued"}, `queued ${r.queued}`) : ""),
      cores);
  }));
}

function drawFilters() {
  const rs = $("#f-runner");
  const have = [...rs.options].map((o) => o.value);
  for (const r of state.runners) if (!have.includes(r.name)) rs.append(el("option", {value: r.name}, r.name));
  rs.value = sel.runner;
  const counts = {};
  for (const j of state.jobs) if (!sel.runner || j.runner === sel.runner) counts[j.state] = (counts[j.state] || 0) + 1;
  $("#f-state").replaceChildren(...STATES.map((s) => el("button", {
    class: "chip s-" + s + (sel.states.has(s) ? " on" : ""), type: "button", "aria-pressed": sel.states.has(s) ? "true" : "false",
    onclick: () => { sel.states.has(s) ? sel.states.delete(s) : sel.states.add(s); savePrefs(); draw(); },
  }, `${LABEL[s]} ${counts[s] || 0}`)));
}

function drawJobs() {
  const now = state.now;
  const t = sel.text.toLowerCase();
  const rows = state.jobs.filter((j) => (!sel.runner || j.runner === sel.runner) && sel.states.has(j.state) &&
    (!t || (j.name || "").toLowerCase().includes(t) || (j.cmd || "").toLowerCase().includes(t) || j.id.toLowerCase().includes(t)));
  $("#empty").hidden = rows.length > 0;
  $("#jobs tbody").replaceChildren(...rows.map((j) => el("tr", {onclick: () => openJob(j.key), class: "s-" + j.state},
    el("td", {class: "c-name"}, el("div", {class: "name"}, j.name, el("span", {class: "kind"}, j.kind)),
      el("div", {class: "muted small id"}, j.id),
      j.tail && j.state === "running" ? el("div", {class: "tail small"}, j.tail.split("\n").pop()) : ""),
    el("td", {"data-l": "state"}, stateBadge(j)),
    el("td", {"data-l": "progress"}, progressBar(j.progress),
      j.eta ? el("div", {class: "muted small eta", title: etaTitle(j.eta)}, etaText(j, now)) : ""),
    el("td", {"data-l": "time"}, elapsed(j, now), el("div", {class: "muted small"}, when(j.started || j.submitted))),
    el("td", {"data-l": "cores"}, j.cpus || (j.cores != null ? j.cores : ""),
      j.cpu_now != null ? el("div", {class: "muted small"}, `using ${j.cpu_now}`) : ""),
    el("td", {"data-l": "runner"}, j.runner || "any"))));
}

function draw() {
  if (!state) return;
  drawRunners(); drawFilters(); drawJobs();
  $("#updated").textContent = "updated " + new Date(state.now * 1000).toLocaleTimeString(undefined);
}

async function refresh() {
  try { state = await api("/api/state"); draw(); $("#updated").classList.remove("stale"); }
  catch (e) { $("#updated").textContent = "no answer: " + e.message; $("#updated").classList.add("stale"); }
}

// ---- job card ----
function openJob(key) {
  openKey = key;
  history.replaceState(null, "", "#job=" + encodeURIComponent(key));
  $("#detail").hidden = false;
  document.body.classList.add("noscroll");
  const j = state && state.jobs.find((x) => x.key === key);
  $("#d-title").textContent = j ? j.name : key;
  $("#d-body").replaceChildren(el("p", {class: "muted"}, "loading…"));
  loadDetail();
}
function closeJob() {
  openKey = null; clearTimeout(detailTimer);
  history.replaceState(null, "", location.pathname);
  $("#detail").hidden = true; document.body.classList.remove("noscroll");
}
async function act(what, key, confirmText) {
  if (confirmText && !confirm(confirmText)) return;
  try {
    const r = await api(`/api/${what}?key=${encodeURIComponent(key)}`, {method: "POST"});
    if (what === "retry" && r.id) alert("Queued again: " + r.id);
    await refresh(); loadDetail();
  } catch (e) { alert(e.message); }
}
async function loadDetail() {
  clearTimeout(detailTimer);
  const key = openKey;
  if (!key) return;
  let d;
  try { d = await api(`/api/job?key=${encodeURIComponent(key)}&lines=300`); }
  catch (e) { $("#d-body").replaceChildren(el("p", {class: "error"}, e.message)); detailTimer = setTimeout(loadDetail, POLL); return; }
  if (key !== openKey) return;
  const pre = $("#d-log");
  const atBottom = !pre || pre.scrollTop + pre.clientHeight >= pre.scrollHeight - 20;
  const now = Date.now() / 1000;
  const facts = [
    ["state", stateBadge(d)], ["runner", d.runner || "any"], ["kind", d.kind],
    ["commit", (d.repo ? d.repo + "@" : "") + (d.sha || "") + (d.tree === "dirty" ? " (with local changes)" : "")],
    ["cores", (d.cpus || "") + (d.cores != null ? ` (${d.cores})` : "")], ["memory", d.mem || ""],
    ["priority", d.prio != null ? d.prio : ""], ["submitted", when(d.submitted)],
    ["started", when(d.started)], ["finished", when(d.finished)], ["run time", elapsed(d, now)],
    ["left", d.eta ? etaText(d, now) : ""], ["rate", d.eta ? etaTitle(d.eta) : ""],
    ["by", d.by || ""],
  ].filter(([, v]) => v !== "" && v != null);
  const actions = el("div", {class: "actions"},
    ["queued", "starting", "running", "lost"].includes(d.state) ? el("button", {class: "danger", onclick: () => act("cancel", key, `Cancel ${d.id}?`)}, "Cancel") : "",
    ["done", "failed", "cancelled", "lost"].includes(d.state) ? el("button", {onclick: () => act("retry", key, `Run ${d.id} again?`)}, "Retry") : "",
    d.runner && d.state !== "queued" ? el("a", {class: "button", href: `/api/fetch?key=${encodeURIComponent(key)}`}, "Download out/") : "");
  $("#d-title").textContent = d.name;
  $("#d-body").replaceChildren(
    el("div", {class: "muted small id"}, d.id),
    el("dl", {class: "facts"}, facts.map(([k, v]) => [el("dt", {}, k), el("dd", {}, v)])),
    d.progress ? el("div", {class: "dprog"}, progressBar(d.progress)) : "",
    actions,
    el("h3", {}, "Command"), el("pre", {class: "cmd"}, d.cmd || ""),
    el("h3", {}, "Log ", el("span", {class: "muted small"}, "last 300 lines")),
    el("pre", {id: "d-log", class: "log"}, d.tail || "(empty)"),
    // replaceChildren does not flatten an array: it would print the two nodes as "[object …]"
    ...(d.out && d.out.length ? [el("h3", {}, `out/ (${d.out.length})`),
      el("ul", {class: "out"}, d.out.map(([n, s]) => el("li", {}, n, el("span", {class: "muted"}, " " + (s > 1e6 ? (s / 1e6).toFixed(1) + " MB" : s + " B")))))] : []));
  const log = $("#d-log");
  if (atBottom) log.scrollTop = log.scrollHeight;
  if (["queued", "starting", "running"].includes(d.state)) detailTimer = setTimeout(loadDetail, POLL);
}

document.addEventListener("DOMContentLoaded", () => {
  loadPrefs();
  $("#f-runner").addEventListener("change", (e) => { sel.runner = e.target.value; savePrefs(); draw(); });
  $("#f-text").addEventListener("input", (e) => { sel.text = e.target.value; draw(); });
  $("#d-close").addEventListener("click", closeJob);
  $("#detail").addEventListener("click", (e) => { if (e.target.id === "detail") closeJob(); });
  document.addEventListener("keydown", (e) => { if (e.key === "Escape" && openKey) closeJob(); });
  refresh().then(() => {
    const m = location.hash.match(/^#job=(.+)$/);
    if (m) openJob(decodeURIComponent(m[1]));
  });
  setInterval(() => { if (!document.hidden) refresh(); }, POLL);
});
