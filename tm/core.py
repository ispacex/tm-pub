"""The queue, the runners' picture and the scheduler.

The queue (jobs submitted through tm and not yet on a runner) lives in SQLite. Everything that is
on a runner is read from the runner itself every few seconds (tm-agent snapshot), whoever started
it — tm itself (kind "tm") or your own scripts writing job directories of another kind in the same
layout. A job's state is worked out from its directory: exit file, else an active unit, else lost.

Jobs of another kind than "tm" (SUBMIT_KIND) take that kind's name as their repository and, when
rerun, as their $JOB_CACHE, unless their meta says otherwise.
"""
import base64
import json
import os
import re
import sqlite3
import threading
import time

from .config import format_cpus, parse_cpus, parse_mem
from .remote import Remote, RunnerError

ID_RE = re.compile(r"^[A-Za-z0-9._-]{1,120}$")
NAME_RE = re.compile(r"^[A-Za-z0-9_-]{1,40}$")
SUBMIT_KIND = "tm"  # the agent's submit_kind: the kind tm's own jobs live under
STATES = ("queued", "starting", "running", "done", "failed", "cancelled", "lost")

SCHEMA = """
CREATE TABLE IF NOT EXISTS jobs (
  id TEXT PRIMARY KEY,
  runner TEXT,            -- where it went (or is pinned to)
  pinned INTEGER DEFAULT 0,
  name TEXT, cmd TEXT, repo TEXT, sha TEXT, tree TEXT,
  cores INTEGER, mem TEXT, prio INTEGER DEFAULT 0, nice INTEGER, cache TEXT,
  progress_glob TEXT, progress_total INTEGER,
  by TEXT, submitted REAL,
  state TEXT,             -- queued | dispatched | cancelled | failed
  cpus TEXT, dispatched REAL, error TEXT,
  tar TEXT, from_kind TEXT, from_id TEXT, retry_of TEXT
);
CREATE TABLE IF NOT EXISTS events (
  at REAL, who TEXT, action TEXT, job TEXT, detail TEXT
);
CREATE TABLE IF NOT EXISTS progress_samples (  -- (job, when, how far): what the ETA is read from
  job TEXT, at REAL, done INTEGER
);
CREATE INDEX IF NOT EXISTS progress_samples_job ON progress_samples (job, at);
"""

# The ETA: the rate of the last RECENT seconds when they saw at least two steps, else the rate
# since the job started. A job whose pieces are uneven (later chunks heavier than earlier ones) is better
# told by the recent rate; a job just restarted after a pause, by the overall one.
RECENT = 1800
KEEP = 2 * 86400
PURGE_EVERY = 3600  # how often the sweep of stale failed/lost jobs runs (cfg.purge_after_seconds)


def estimate(done, total, started, samples, now):
    """{"eta": seconds left, "rate": pieces per hour, "basis": "recent" | "overall"} or None.

    samples: [(at, done)] oldest first, the job's own history of its progress."""
    if not total or done is None or done >= total or started is None or done <= 0:
        return None
    rate, basis = None, None
    window = [(t, d) for t, d in samples if t >= now - RECENT]
    if len(window) >= 2:
        (t0, d0), (t1, d1) = window[0], window[-1]
        if d1 - d0 >= 2 and t1 - t0 >= 120:
            rate, basis = (d1 - d0) / (now - t0), "recent"
    if rate is None:
        span = now - started
        if span < 60:   # a first step seen seconds after the start says nothing yet
            return None
        rate, basis = done / span, "overall"
    if rate <= 0:
        return None
    return {"eta": round((total - done) / rate), "rate": round(rate * 3600, 2), "basis": basis}



def job_name(jid):
    m = re.match(r"^\d{8}-\d{6}-(.+)-[0-9a-f]{7,}$", jid)
    return m.group(1) if m else jid


def iso_to_ts(s):
    if not s:
        return None
    try:
        from datetime import datetime
        return datetime.fromisoformat(s).timestamp()
    except ValueError:
        return None


def runner_state(j):
    """State of a job found on a runner, as the scripts' status sees it."""
    ex = j.get("exit")
    if ex is not None:
        if ex == "0":
            return "done", None
        if ex == "cancelled":
            return "cancelled", None
        return "failed", ex
    if j.get("unit") in ("active", "activating", "reloading"):
        return "running", None
    return "lost", None


class Store:
    def __init__(self, path):
        os.makedirs(os.path.dirname(path) or ".", exist_ok=True)
        self.db = sqlite3.connect(path, check_same_thread=False, isolation_level=None)
        self.db.row_factory = sqlite3.Row
        self.lock = threading.RLock()
        with self.lock:
            self.db.executescript(SCHEMA)

    def q(self, sql, *args):
        with self.lock:
            return [dict(r) for r in self.db.execute(sql, args).fetchall()]

    def x(self, sql, *args):
        with self.lock:
            self.db.execute(sql, args)

    def event(self, who, action, job, detail=""):
        self.x("INSERT INTO events VALUES (?,?,?,?,?)", time.time(), who, action, job, detail)


class Monitor:
    def __init__(self, cfg, store=None, remote_factory=None):
        self.cfg = cfg
        self.store = store or Store(os.path.join(cfg.data_dir, "tm.db"))
        os.makedirs(os.path.join(cfg.data_dir, "trees"), exist_ok=True)
        os.makedirs(os.path.join(cfg.data_dir, "uploads"), exist_ok=True)
        mk = remote_factory or (lambda r: Remote(r, cfg.ssh_key, cfg.ssh_known_hosts))
        self.remotes = {r.name: mk(r) for r in cfg.runners}
        self.snap = {}       # runner -> {"at", "data", "error", "prev"}
        self.lock = threading.RLock()
        self.wake = threading.Event()
        self.samples = {}    # job key -> [(at, done)], loaded from progress_samples
        self.last_purge = 0.0
        self.purge_refused = set()  # keys the agent refused to purge: told once in events, not hourly
        for r in self.store.q("SELECT job, at, done FROM progress_samples WHERE at > ? ORDER BY at", time.time() - KEEP):
            self.samples.setdefault(r["job"], []).append((r["at"], r["done"]))
        self.store.x("DELETE FROM progress_samples WHERE at <= ?", time.time() - KEEP)

    # ---- polling ---------------------------------------------------------------------------

    def poll(self, name):
        try:
            data = self.remotes[name].json("snapshot", timeout=40)
            with self.lock:
                old = self.snap.get(name, {})
                self.snap[name] = {"at": time.time(), "data": data, "error": None,
                                   "prev": old.get("data") if old.get("data") else None}
        except RunnerError as e:
            with self.lock:
                old = self.snap.get(name, {})
                self.snap[name] = dict(old, error=str(e), failed_at=time.time())

    def poll_all(self):
        threads = [threading.Thread(target=self.poll, args=(r.name,), daemon=True)
                   for r in self.cfg.runners if not r.disabled]
        for t in threads:
            t.start()
        for t in threads:
            t.join(60)

    def loop(self, stop):
        while not stop.is_set():
            t0 = time.time()
            self.poll_all()
            try:
                self.schedule()
            except Exception as e:  # never let the loop die
                self.store.event("tm", "scheduler-error", "", repr(e))
            if time.time() - self.last_purge >= PURGE_EVERY:
                self.last_purge = time.time()
                try:
                    self.purge_stale()
                except Exception as e:
                    self.store.event("tm", "purge-error", "", repr(e))
            self.wake.wait(max(0.5, self.cfg.poll_seconds - (time.time() - t0)))
            self.wake.clear()

    # ---- the merged picture ----------------------------------------------------------------

    def runner_jobs(self):
        out = []
        with self.lock:
            snaps = dict(self.snap)
        for name, s in snaps.items():
            data = s.get("data")
            if not data:
                continue
            prev = {(j["kind"], j["id"]): j for j in (s.get("prev") or {}).get("jobs", [])}
            dt = data["host"]["time"] - (s.get("prev") or {}).get("host", {}).get("time", 0)
            for j in data["jobs"]:
                state, code = runner_state(j)
                meta = j.get("meta", {})
                # the scripts write meta before systemd-run starts the unit: not lost yet, starting
                sub = iso_to_ts(meta.get("submitted"))
                if state == "lost" and sub and time.time() - sub < 90:
                    state = "starting"
                p = prev.get((j["kind"], j["id"]))
                cpu_now = None
                if state == "running" and p and j.get("cpu_ns") is not None and p.get("cpu_ns") is not None and 0 < dt < 120:
                    cpu_now = round((j["cpu_ns"] - p["cpu_ns"]) / 1e9 / dt, 2)
                started = iso_to_ts(j.get("started"))
                finished = iso_to_ts(j.get("finished"))
                cpus = meta.get("cpus", "")
                try:
                    ncores = len(parse_cpus(cpus)) if cpus else None
                except ValueError:
                    ncores = None
                out.append({
                    "key": "%s/%s/%s" % (name, j["kind"], j["id"]),
                    "runner": name, "kind": j["kind"], "id": j["id"],
                    "name": meta.get("name") or job_name(j["id"]),
                    "cmd": meta.get("cmd", ""), "sha": meta.get("sha"), "tree": meta.get("tree"),
                    "repo": meta.get("repo") or (j["kind"] if j["kind"] != SUBMIT_KIND else None),
                    "by": meta.get("by") or ("script" if meta.get("started_by") != "tm" else None),
                    "submitted": iso_to_ts(meta.get("submitted")), "started": started, "finished": finished,
                    "cpus": cpus, "cores": int(meta["cores"]) if meta.get("cores", "").isdigit() else ncores,
                    "claims": meta.get("cores") != "0",
                    "mem": meta.get("mem"), "prio": int(meta["prio"]) if re.match(r"^-?\d+$", meta.get("prio", "")) else None,
                    "state": state, "exit": code, "unit": j.get("unit"),
                    "progress": j.get("progress"), "tail": j.get("tail"),
                    "cpu_now": cpu_now, "mem_bytes": j.get("mem_bytes"),
                    "log_mtime": j.get("log_mtime"), "log_size": j.get("log_size"),
                    "tm": meta.get("started_by") == "tm",
                })
                self.with_eta(out[-1])
        return out

    def with_eta(self, rec):
        """Write down a running job's progress when it moves, and put its ETA on the record."""
        p = rec.get("progress")
        rec["eta"] = None
        if rec["state"] != "running" or not p or p.get("done") is None:
            return
        now = time.time()
        with self.lock:
            hist = self.samples.setdefault(rec["key"], [])
            if not hist or hist[-1][1] != p["done"]:
                hist.append((now, p["done"]))
                self.store.x("INSERT INTO progress_samples VALUES (?,?,?)", rec["key"], now, p["done"])
                del hist[:-500]
            hist = list(hist)
        rec["eta"] = estimate(p["done"], p.get("total"), rec.get("started"), hist, now)

    def queue_jobs(self, seen):
        """tm's own rows that are not (yet, or any more) visible on a runner."""
        out = []
        now = time.time()
        for r in self.store.q("SELECT * FROM jobs"):
            if r["runner"] and (r["runner"], "tm", r["id"]) in seen:
                continue
            state = r["state"]
            if state == "dispatched":
                with self.lock:
                    s = self.snap.get(r["runner"], {})
                fresh = s.get("at", 0) > (r["dispatched"] or 0)
                state = "lost" if fresh and now - (r["dispatched"] or 0) > self.cfg.lost_after_seconds else "starting"
            out.append({
                "key": "%s/tm/%s" % (r["runner"] or "-", r["id"]),
                "runner": r["runner"], "kind": "tm", "id": r["id"], "name": r["name"], "cmd": r["cmd"],
                "sha": r["sha"], "tree": r["tree"], "repo": r["repo"], "by": r["by"],
                "submitted": r["submitted"], "started": None, "finished": None,
                "cpus": r["cpus"] or "", "cores": r["cores"], "claims": r["cores"] != 0, "mem": r["mem"], "prio": r["prio"],
                "state": state, "exit": ("dispatch" if (r["error"] or "").startswith("dispatch") else r["error"]) if state == "failed" else None,
                "pinned": bool(r["pinned"]), "progress": None, "tail": r["error"], "tm": True,
            })
        return out

    def jobs(self):
        rj = self.runner_jobs()
        seen = {(j["runner"], j["kind"], j["id"]) for j in rj}
        all_jobs = rj + self.queue_jobs(seen)
        all_jobs.sort(key=lambda j: -(j.get("submitted") or 0))
        return all_jobs

    def runners(self, jobs=None):
        jobs = jobs if jobs is not None else self.jobs()
        out = []
        for r in self.cfg.runners:
            with self.lock:
                s = dict(self.snap.get(r.name, {}))
            data = s.get("data") or {}
            host = data.get("host", {})
            prev = (s.get("prev") or {}).get("host", {})
            busy = {}
            for c, (tot, idle) in (host.get("cpu_ticks") or {}).items():
                pt = (prev.get("cpu_ticks") or {}).get(c)
                if pt and tot > pt[0]:
                    busy[int(c)] = round(100 * (1 - (idle - pt[1]) / (tot - pt[0])))
            claim = {}
            for j in jobs:
                if j["runner"] == r.name and j["state"] in ("running", "starting") and j.get("claims", True) and j["cpus"]:
                    for c in parse_cpus(j["cpus"]):
                        claim.setdefault(c, []).append(j["key"])
            ncpu = host.get("ncpu") or (max(r.cpus) + 1 if r.cpus else 0)
            out.append({
                "name": r.name, "ssh": r.ssh.split("@")[-1], "disabled": r.disabled,
                "online": bool(s.get("at")) and not s.get("error"),
                "seen": s.get("at"), "error": s.get("error"),
                "hostname": host.get("hostname"), "load": host.get("load"),
                "mem_total": host.get("mem_total"), "mem_available": host.get("mem_available"),
                "pool": format_cpus(r.cpus), "mem_pool": r.mem,
                "cores": [{"cpu": c, "busy": busy.get(c), "pool": c in r.cpus, "jobs": claim.get(c, [])}
                          for c in range(ncpu)],
                "queued": sum(1 for j in jobs if j["state"] == "queued" and j.get("runner") in (None, r.name)),
            })
        return out

    def state(self):
        jobs = self.jobs()
        return {"now": time.time(), "runners": self.runners(jobs), "jobs": jobs}

    def find(self, key):
        for j in self.jobs():
            if j["key"] == key:
                return j
        return None

    # ---- submitting ------------------------------------------------------------------------

    def uploads_dir(self):
        return os.path.join(self.cfg.data_dir, "uploads")

    def check(self, spec):
        """A spec's refusal as ValueError, before anything is written or fetched."""
        name = spec.get("name") or "job"
        if not NAME_RE.match(name):
            raise ValueError("name: letters, digits, - and _ only")
        cmd = spec.get("cmd") or ""
        if not cmd.strip():
            raise ValueError("no command")
        cores = int(spec.get("cores", 1))
        if cores < 0 or cores > 256:
            raise ValueError("bad cores")
        mem = str(spec.get("mem") or "4G")
        parse_mem(mem)
        if not re.match(r"^\d+[KMGT]?$", mem):
            raise ValueError("mem like 4G")
        runner = spec.get("runner") or None
        if runner and not self.cfg.runner(runner):
            raise ValueError("no runner %s" % runner)
        if runner is None and not any(self.fits_ever(r, cores, mem) for r in self.cfg.runners if not r.disabled):
            raise ValueError("no runner has %d cores and %s" % (cores, mem))
        if runner and not self.fits_ever(self.cfg.runner(runner), cores, mem):
            raise ValueError("%s has fewer than %d cores or %s" % (runner, cores, mem))
        sha = (spec.get("sha") or "nogit")[:12]
        if not re.match(r"^[0-9a-zA-Z]+$", sha):
            raise ValueError("bad sha")
        nice = int(spec.get("nice", self.cfg.default_nice))
        if not 0 <= nice <= 19:
            raise ValueError("nice 0..19")
        pg = spec.get("progress_glob") or None
        if pg and (pg.startswith("/") or ".." in pg):
            raise ValueError("progress_glob is relative to the job directory, e.g. out/*.json")
        return name, cmd, cores, mem, runner, sha, nice, pg

    def submit(self, who, spec, tar_path=None):
        name, cmd, cores, mem, runner, sha, nice, pg = self.check(spec)
        jid = "%s-%s-%s" % (time.strftime("%Y%m%d-%H%M%S", time.gmtime()), name, sha)
        base, n = jid, 1
        while self.store.q("SELECT id FROM jobs WHERE id=?", jid):
            n += 1
            jid = "%s.%d" % (base, n)
        tar = None
        if tar_path:
            tar = os.path.join(self.cfg.data_dir, "trees", jid + ".tar.gz")
            os.replace(tar_path, tar)
        frm = spec.get("from") or {}
        self.store.x("""INSERT INTO jobs (id, runner, pinned, name, cmd, repo, sha, tree, cores, mem, prio, nice, cache,
                        progress_glob, progress_total, by, submitted, state, tar, from_kind, from_id, retry_of)
                        VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)""",
                     jid, runner, 1 if runner else 0, name, cmd, spec.get("repo"), sha, spec.get("tree") or "clean",
                     cores, mem, int(spec.get("prio", 0)), nice, spec.get("cache") or spec.get("repo"),
                     pg, spec.get("progress_total"), who, time.time(), "queued", tar,
                     frm.get("kind"), frm.get("id"), spec.get("retry_of"))
        self.store.event(who, "submit", jid, cmd[:200])
        self.wake.set()
        return jid

    def fits_ever(self, r, cores, mem):
        return cores <= len(r.cpus) and parse_mem(mem) <= r.mem

    # ---- scheduling ------------------------------------------------------------------------

    def schedule(self):
        queued = self.store.q("SELECT * FROM jobs WHERE state='queued' ORDER BY prio DESC, submitted ASC")
        if not queued:
            return
        jobs = self.jobs()
        free, mem_free = {}, {}
        for r in self.cfg.runners:
            with self.lock:
                s = self.snap.get(r.name, {})
            if r.disabled or not s.get("data") or s.get("error"):
                continue
            claimed = set()
            for j in jobs:
                if j["runner"] == r.name and j["state"] in ("running", "starting") and j.get("claims", True) and j["cpus"]:
                    claimed.update(parse_cpus(j["cpus"]))
            free[r.name] = [c for c in r.cpus if c not in claimed]
            host = s["data"]["host"]
            mem_free[r.name] = min(r.mem, (host.get("mem_available") or r.mem) - (1 << 30))
        for q in queued:
            want_mem = parse_mem(q["mem"])
            for r in self.cfg.runners:
                if r.name not in free or (q["pinned"] and q["runner"] != r.name):
                    continue
                if q["from_kind"] and not q["tar"] and q["runner"] != r.name:
                    continue
                if want_mem > mem_free[r.name]:
                    continue
                if q["cores"] == 0:
                    cpus = list(r.cpus)
                elif len(free[r.name]) >= q["cores"]:
                    cpus = free[r.name][:q["cores"]]
                else:
                    continue
                if self.dispatch(q, r, cpus):
                    if q["cores"]:
                        free[r.name] = [c for c in free[r.name] if c not in cpus]
                    mem_free[r.name] -= want_mem
                break

    def dispatch(self, q, r, cpus):
        cpus_s = format_cpus(cpus)
        self.store.x("UPDATE jobs SET state='dispatched', runner=?, cpus=?, dispatched=? WHERE id=? AND state='queued'",
                     r.name, cpus_s, time.time(), q["id"])
        meta = {k: q[k] for k in ("sha", "tree", "name", "repo", "prio", "by", "cores", "retry_of",
                                  "progress_glob", "progress_total")}
        meta["runner"] = r.name
        meta["submitted"] = time.strftime("%Y-%m-%dT%H:%M:%S+00:00", time.gmtime(q["submitted"]))
        args = ["start", q["id"], "--cpus", cpus_s, "--mem", q["mem"], "--nice", str(q["nice"] if q["cores"] else 19),
                "--cmd", base64.b64encode(q["cmd"].encode()).decode(),
                "--meta", base64.b64encode(json.dumps(meta).encode()).decode()]
        if q["cache"]:
            args += ["--cache", q["cache"]]
        stdin = None
        if q["tar"] and os.path.exists(q["tar"]):
            with open(q["tar"], "rb") as f:
                stdin = f.read()
        elif q["from_kind"]:
            args += ["--from", "%s/%s" % (q["from_kind"], q["from_id"])]
        else:
            stdin = b""
        try:
            self.remotes[r.name].call(*args, stdin=stdin, timeout=300)
        except RunnerError as e:
            self.store.x("UPDATE jobs SET state='failed', error=? WHERE id=?", "dispatch: %s" % e, q["id"])
            self.store.event("tm", "dispatch-failed", q["id"], str(e))
            return False
        self.store.event("tm", "dispatch", q["id"], "%s cpus %s" % (r.name, cpus_s))
        return True

    # ---- cleanup ---------------------------------------------------------------------------

    def purge_stale(self, now=None):
        """Remove failed and lost jobs nobody has touched for cfg.purge_after_seconds.

        On a runner: tm-agent purge, which checks the job's state and age again by itself. In the
        queue: failed rows (dispatch errors) and dispatched rows that never showed up (lost), with
        their tree archive. Only runners whose last snapshot succeeded count: when a runner does not
        answer, nothing of it is judged lost or failed. Returns the keys removed."""
        limit = self.cfg.purge_after_seconds
        if not limit or limit <= 0:
            return []
        now = now or time.time()
        with self.lock:
            ok = {n for n, s in self.snap.items() if s.get("data") and not s.get("error")}
        purged = []
        rj = self.runner_jobs()
        for j in rj:
            if j["runner"] not in ok or j["state"] not in ("failed", "lost"):
                continue
            last = max((t for t in (j.get("finished"), j.get("log_mtime"), j.get("started"), j.get("submitted")) if t),
                       default=None)
            if last is None or now - last <= limit:
                continue
            try:
                self.remotes[j["runner"]].call("purge", j["kind"], j["id"], int(limit), timeout=120)
            except RunnerError as e:
                if j["key"] not in self.purge_refused:
                    self.purge_refused.add(j["key"])
                    self.store.event("tm", "purge-refused", j["key"], str(e))
                continue
            with self.lock:
                s = self.snap.get(j["runner"])
                if s and s.get("data"):
                    s["data"] = dict(s["data"], jobs=[x for x in s["data"]["jobs"]
                                                      if (x["kind"], x["id"]) != (j["kind"], j["id"])])
            if j["kind"] == "tm":
                self.drop_row(j["id"], ("dispatched", "failed"))
            self.store.event("tm", "purge", j["key"], "%s%s, last update %s" % (
                j["state"], " (exit %s)" % j["exit"] if j.get("exit") else "", time.strftime("%Y-%m-%d %H:%M", time.gmtime(last))))
            purged.append(j["key"])
        seen = {(j["runner"], j["kind"], j["id"]) for j in rj}
        for q in self.queue_jobs(seen):
            if q["state"] not in ("failed", "lost") or (q["runner"] and q["runner"] not in ok):
                continue
            row = self.store.q("SELECT * FROM jobs WHERE id=?", q["id"])
            if not row:
                continue
            r = row[0]
            last = max((t for t in (r["submitted"], r["dispatched"]) if t), default=None)
            if last is None or now - last <= limit:
                continue
            if self.drop_row(r["id"], (r["state"],)):
                self.store.event("tm", "purge", q["key"], "%s in the queue%s" % (
                    q["state"], ": " + r["error"][:200] if r["error"] else ""))
                purged.append(q["key"])
        return purged

    def drop_row(self, jid, states):
        """Delete a queue row in one of these states, and its tree archive. True if it was there."""
        with self.store.lock:
            row = self.store.q("SELECT * FROM jobs WHERE id=?", jid)
            if not row or row[0]["state"] not in states:
                return False
            self.store.x("DELETE FROM jobs WHERE id=? AND state=?", jid, row[0]["state"])
        tar = row[0]["tar"]
        trees = os.path.realpath(os.path.join(self.cfg.data_dir, "trees"))
        if tar and os.path.dirname(os.path.realpath(tar)) == trees:
            try:
                os.remove(tar)
            except OSError:
                pass
        return True

    # ---- actions ---------------------------------------------------------------------------

    def cancel(self, who, key):
        j = self.find(key)
        if not j:
            raise KeyError(key)
        if j["state"] == "queued":
            self.store.x("UPDATE jobs SET state='cancelled' WHERE id=? AND state='queued'", j["id"])
            self.store.event(who, "cancel", key, "queued")
            return "cancelled"
        if j["state"] not in ("running", "starting", "lost"):
            raise ValueError("job is %s" % j["state"])
        out = self.remotes[j["runner"]].call("cancel", j["kind"], j["id"]).decode().strip()
        self.store.event(who, "cancel", key, out)
        self.wake.set()
        return out

    def retry(self, who, key):
        j = self.find(key)
        if not j:
            raise KeyError(key)
        if j["state"] in ("queued", "starting", "running"):
            raise ValueError("job is %s" % j["state"])
        row = self.store.q("SELECT * FROM jobs WHERE id=?", j["id"]) if j["kind"] == "tm" else []
        spec = {"name": (j["name"] or "job")[:40], "cmd": j["cmd"], "cores": j.get("cores") if j.get("cores") is not None else 1,
                "mem": j.get("mem") or "4G", "prio": j.get("prio") or 0, "sha": j.get("sha") or "nogit",
                "tree": j.get("tree"), "repo": j.get("repo"), "retry_of": key}
        if not NAME_RE.match(spec["name"]):
            spec["name"] = "retry"
        if row:
            r0 = row[0]
            spec.update(cache=r0["cache"], nice=r0["nice"], progress_glob=r0["progress_glob"],
                        progress_total=r0["progress_total"])
        tar = None
        if row and row[0]["tar"] and os.path.exists(row[0]["tar"]):
            tar = os.path.join(self.cfg.data_dir, "uploads", "retry-%d.tar.gz" % time.time_ns())
            with open(row[0]["tar"], "rb") as a, open(tar, "wb") as b:
                b.write(a.read())
        elif row and not row[0]["tar"] and not row[0]["from_kind"]:
            pass  # submitted with --no-tree: runs in an empty tree again
        elif j.get("runner"):
            spec["runner"] = j["runner"]
            spec["from"] = {"kind": j["kind"], "id": j["id"]}
            if j["kind"] != SUBMIT_KIND:
                spec.setdefault("cache", j["kind"])
        else:
            raise ValueError("nothing to rerun from")
        if spec.get("cache") is None and j["kind"] != SUBMIT_KIND:
            spec["cache"] = j["kind"]
        jid = self.submit(who, spec, tar)
        self.store.event(who, "retry", key, jid)
        return jid

    def show(self, key, lines=200):
        j = self.find(key)
        if not j:
            raise KeyError(key)
        if j["state"] in ("queued", "starting", "cancelled", "failed", "lost") and j["kind"] == "tm" and not j.get("started"):
            row = self.store.q("SELECT * FROM jobs WHERE id=?", j["id"])
            if row and row[0]["state"] != "dispatched":
                return dict(j, out=[], tail=j.get("tail") or "")
        d = self.remotes[j["runner"]].json("show", j["kind"], j["id"], lines)
        return dict(j, tail=d.get("tail"), out=d.get("out", []), cmd=d.get("meta", {}).get("cmd") or j["cmd"],
                    progress=d.get("progress"))

    def log(self, key, lines=200):
        j = self.find(key)
        if not j or not j.get("runner"):
            raise KeyError(key)
        return self.remotes[j["runner"]].call("log", j["kind"], j["id"], lines).decode("utf-8", "replace")

    def fetch_proc(self, key):
        j = self.find(key)
        if not j or not j.get("runner"):
            raise KeyError(key)
        return j, self.remotes[j["runner"]].stream("fetch", j["kind"], j["id"])
