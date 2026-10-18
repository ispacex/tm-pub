"""MCP over Streamable HTTP at /mcp: the jobs, the runners and the queue as tools.

Plain JSON-RPC 2.0, stateless: every POST carries one message (or a batch) and is answered with
one JSON body — no SSE stream and no session id, which the transport allows. The caller is whoever
the Bearer token is (the same API token as the CLI's); the rights are the web's.
"""
import json
import os
import time
import urllib.parse

from . import gitsrc

VERSIONS = ("2025-11-25", "2025-06-18", "2025-03-26", "2024-11-05")
SERVER = {"name": "tm", "title": "tm — compute jobs", "version": "1.0"}
INSTRUCTIONS = (
    "tm runs long computations on the runners and shows them in its web monitor. A job is named by its key 'runner/kind/id'; any tool also takes the id or "
    "a unique part of it. States: queued -> starting -> running -> done | failed | cancelled | lost. "
    "submit_job takes the tree from GitHub by commit (only the repositories list_runners names under "
    "git_repos); a job writes results to $JOB_OUT and may report progress as 'done/total' in "
    "$JOB_ROOT/progress. Outputs are not streamed through MCP: fetch_outputs gives the file list and "
    "a download link.")
FINISHED = ("done", "cancelled")
LOG_MAX_CHARS = 200_000


class ToolError(Exception):
    """A tool's refusal: returned as a result with isError, so the model reads it."""


def iso(ts):
    return time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime(ts)) if ts else None


def dur(s):
    if s is None:
        return None
    s = int(s)
    return "%dh%02dm" % (s // 3600, s % 3600 // 60) if s >= 3600 else "%dm%02ds" % (s // 60, s % 60)


def schema(props, required=()):
    return {"type": "object", "properties": props, "required": list(required), "additionalProperties": False}


JOB = {"type": "string", "description": "job key runner/kind/id, the id, or a unique part of the id"}

TOOLS = [
    {"name": "list_jobs", "title": "List jobs",
     "description": "Jobs on the runners and in the queue, newest first: state, progress, ETA, cores, run time. "
                    "By default unfinished and failed ones (as `tm ls`); all=true adds done and cancelled.",
     "inputSchema": schema({
         "runner": {"type": "string", "description": "only this runner"},
         "status": {"type": "string", "enum": ["queued", "starting", "running", "done", "failed", "cancelled", "lost"]},
         "text": {"type": "string", "description": "substring of the key, name, command, repo or submitter"},
         "all": {"type": "boolean", "default": False},
         "limit": {"type": "integer", "minimum": 1, "maximum": 500, "default": 50}}),
     "annotations": {"readOnlyHint": True}},
    {"name": "get_job", "title": "Get a job",
     "description": "One job: its meta (command, commit, cores, memory, who, times), progress, ETA, the tail of its "
                    "log and the files in out/.",
     "inputSchema": schema({"job": JOB, "lines": {"type": "integer", "minimum": 0, "maximum": 500, "default": 40}}, ["job"]),
     "annotations": {"readOnlyHint": True}},
    {"name": "get_log", "title": "Job log",
     "description": "The last N lines of a job's log.",
     "inputSchema": schema({"job": JOB, "lines": {"type": "integer", "minimum": 1, "maximum": 5000, "default": 100}}, ["job"]),
     "annotations": {"readOnlyHint": True}},
    {"name": "list_runners", "title": "List runners",
     "description": "The runners: online or not, cores in tm's pool (claimed by jobs and free), per-core load, "
                    "memory, load average, queued jobs; and which repositories submit_job may take a tree from.",
     "inputSchema": schema({}), "annotations": {"readOnlyHint": True}},
    {"name": "submit_job", "title": "Submit a job",
     "description": "Queue a job. The tree is fetched by the monitor from GitHub at repo@commit (only allowlisted "
                    "repositories, see list_runners) and the command runs in it, under bash, with $JOB_OUT, $JOB_ROOT "
                    "and $JOB_CACHE set. Without repo the command runs in an empty directory. Answers with the new "
                    "job's key.",
     "inputSchema": schema({
         "command": {"type": "string", "description": "shell command, run in the tree's root"},
         "repo": {"type": "string", "description": "owner/name on GitHub, e.g. example-org/example-project"},
         "commit": {"type": "string", "description": "sha (7-40 hex) or a branch/tag; recorded as the full sha"},
         "name": {"type": "string", "description": "letters, digits, - and _; default: job"},
         "cores": {"type": "integer", "minimum": 0, "maximum": 256, "default": 1,
                   "description": "cores to claim; 0 = a light job that claims none and runs at nice 19"},
         "mem": {"type": "string", "default": "4G", "description": "MemoryMax, e.g. 4G"},
         "priority": {"type": "integer", "default": 0, "description": "higher goes first"},
         "runner": {"type": "string", "description": "pin to one runner"},
         "progress_glob": {"type": "string", "description": "progress = files matching this, relative to the job dir, e.g. out/*.json"},
         "progress_total": {"type": "integer", "minimum": 1},
         "cache": {"type": "string", "description": "whose $JOB_CACHE (a kind or repository name); default: the repository's name"},
         "nice": {"type": "integer", "minimum": 0, "maximum": 19}}, ["command"]),
     "annotations": {"readOnlyHint": False, "destructiveHint": False, "openWorldHint": True}},
    {"name": "cancel_job", "title": "Cancel a job",
     "description": "Stop a running job, or take a queued one out of the queue.",
     "inputSchema": schema({"job": JOB}, ["job"]),
     "annotations": {"readOnlyHint": False, "destructiveHint": True}},
    {"name": "retry_job", "title": "Retry a job",
     "description": "Run a finished, failed, cancelled or lost job again: the same tree and command, a new job.",
     "inputSchema": schema({"job": JOB}, ["job"]),
     "annotations": {"readOnlyHint": False, "destructiveHint": False}},
    {"name": "fetch_outputs", "title": "Job outputs",
     "description": "The files in a job's out/ with their sizes, and a link to download out/, log, meta and exit as "
                    "one .tar.gz (it needs the same Bearer token). The bytes are not sent through MCP.",
     "inputSchema": schema({"job": JOB}, ["job"]),
     "annotations": {"readOnlyHint": True}},
]


class Mcp:
    def __init__(self, cfg, monitor, github=None):
        self.cfg, self.monitor = cfg, monitor
        self.github = github or gitsrc.GitHub(cfg.github_token)

    # ---- JSON-RPC ------------------------------------------------------------------------

    def handle(self, msg, who, base_url):
        """One parsed message -> the response dict, or None for a notification or a response."""
        if not isinstance(msg, dict) or msg.get("jsonrpc") != "2.0":
            return err(None, -32600, "not a JSON-RPC 2.0 message")
        if "method" not in msg:
            return None  # a response to something we never ask
        mid, method, params = msg.get("id"), msg["method"], msg.get("params") or {}
        if "id" not in msg:
            return None  # notifications/initialized, notifications/cancelled, ...
        if not isinstance(params, dict):
            return err(mid, -32602, "params must be an object")
        try:
            if method == "initialize":
                return ok(mid, self.initialize(params))
            if method == "ping":
                return ok(mid, {})
            if method == "tools/list":
                return ok(mid, {"tools": TOOLS})
            if method == "tools/call":
                return ok(mid, self.call(params, who, base_url))
            if method in ("resources/list", "prompts/list"):
                return ok(mid, {method.split("/")[0]: []})
            return err(mid, -32601, "no method %s" % method)
        except InvalidParams as e:
            return err(mid, -32602, str(e))

    def initialize(self, params):
        want = params.get("protocolVersion")
        return {"protocolVersion": want if want in VERSIONS else VERSIONS[0],
                "capabilities": {"tools": {"listChanged": False}},
                "serverInfo": SERVER, "instructions": INSTRUCTIONS}

    def call(self, params, who, base_url):
        name, args = params.get("name"), params.get("arguments") or {}
        fn = getattr(self, "t_" + str(name), None)
        if not fn or not any(t["name"] == name for t in TOOLS):
            raise InvalidParams("no tool %s" % name)
        if not isinstance(args, dict):
            raise InvalidParams("arguments must be an object")
        try:
            data = fn(args, who=who, base_url=base_url)
        except ToolError as e:
            return {"content": [{"type": "text", "text": str(e)}], "isError": True}
        except KeyError as e:
            return {"content": [{"type": "text", "text": "no such job: %s" % e.args[0]}], "isError": True}
        except (ValueError, TypeError) as e:  # the monitor's own refusals (bad cores, job is running, ...)
            return {"content": [{"type": "text", "text": str(e)}], "isError": True}
        except Exception as e:  # noqa: BLE001 — a runner that does not answer, and the like
            return {"content": [{"type": "text", "text": "%s: %s" % (type(e).__name__, e)}], "isError": True}
        if isinstance(data, str):
            return {"content": [{"type": "text", "text": data}]}
        return {"content": [{"type": "text", "text": json.dumps(data, ensure_ascii=False, indent=1)}],
                "structuredContent": data}

    # ---- helpers ---------------------------------------------------------------------------

    def resolve(self, ref, jobs=None):
        ref = str(ref or "").strip()
        if not ref:
            raise ToolError("job: a key, an id or a part of an id")
        jobs = jobs if jobs is not None else self.monitor.jobs()
        for j in jobs:
            if j["key"] == ref or j["id"] == ref:
                return j
        hits = [j for j in jobs if ref in j["id"]]
        if len(hits) == 1:
            return hits[0]
        if not hits:
            raise ToolError("no job matches %r" % ref)
        raise ToolError("%r matches %d jobs: %s" % (ref, len(hits), ", ".join(j["key"] for j in hits[:8])))

    @staticmethod
    def brief(j, now):
        p = j.get("progress")
        e = j.get("eta")
        run = ((j.get("finished") or now) - j["started"]) if j.get("started") else None
        return {
            "key": j["key"], "name": j.get("name"), "state": j["state"], "exit": j.get("exit"),
            "runner": j.get("runner"), "repo": j.get("repo"), "sha": j.get("sha"),
            "cores": j.get("cores"), "cpus": j.get("cpus") or None, "mem": j.get("mem"), "priority": j.get("prio"),
            "by": j.get("by"),
            "submitted": iso(j.get("submitted")), "started": iso(j.get("started")), "finished": iso(j.get("finished")),
            "run_time": dur(run), "run_seconds": int(run) if run is not None else None,
            "progress": {"done": p.get("done"), "total": p.get("total")} if p else None,
            "eta": {"seconds": e["eta"], "left": dur(e["eta"]), "at": iso(now + e["eta"]),
                    "rate_per_hour": e["rate"], "basis": e["basis"]} if e else None,
            "cpu_now": j.get("cpu_now"),
            "command": (j.get("cmd") or "")[:300],
        }

    # ---- tools -----------------------------------------------------------------------------

    def t_list_jobs(self, a, **_):
        now = time.time()
        text = str(a.get("text") or "").lower()
        limit = max(1, min(int(a.get("limit") or 50), 500))
        out, total = [], 0
        for j in self.monitor.jobs():
            if a.get("runner") and j.get("runner") != a["runner"]:
                continue
            if a.get("status") and j["state"] != a["status"]:
                continue
            if not a.get("all") and not a.get("status") and j["state"] in FINISHED:
                continue
            if text and not any(text in str(j.get(k) or "").lower() for k in ("key", "name", "cmd", "repo", "by")):
                continue
            total += 1
            if len(out) < limit:
                out.append(self.brief(j, now))
        return {"jobs": out, "count": total, "shown": len(out), "now": iso(now)}

    def t_get_job(self, a, **_):
        j = self.resolve(a.get("job"))
        lines = max(0, min(int(a.get("lines", 40)), 500))
        d = self.monitor.show(j["key"], max(lines, 1))
        rec = self.brief(dict(j, **{k: d[k] for k in ("progress", "cmd") if d.get(k) is not None}), time.time())
        rec["command"] = d.get("cmd") or j.get("cmd")
        rec.update(kind=j["kind"], id=j["id"], tree=j.get("tree"), tm=j.get("tm"),
                   out_files=len(d.get("out") or []), out=[{"path": p, "bytes": n} for p, n in (d.get("out") or [])[:50]],
                   log_tail=d.get("tail") if lines else None)
        return rec

    def t_get_log(self, a, **_):
        j = self.resolve(a.get("job"))
        lines = max(1, min(int(a.get("lines", 100)), 5000))
        text = self.monitor.log(j["key"], lines)
        if len(text) > LOG_MAX_CHARS:
            text = "[… first %d characters cut]\n" % (len(text) - LOG_MAX_CHARS) + text[-LOG_MAX_CHARS:]
        return text

    def t_list_runners(self, a, **_):
        jobs = self.monitor.jobs()
        out = []
        for r in self.monitor.runners(jobs):
            pool = [c for c in r["cores"] if c["pool"]]
            claimed = [c["cpu"] for c in pool if c["jobs"]]
            busy = [c["busy"] for c in r["cores"] if c["busy"] is not None]
            out.append({
                "name": r["name"], "online": r["online"], "disabled": r["disabled"], "error": r["error"],
                "hostname": r["hostname"], "load": r["load"],
                "cores_total": len(r["cores"]), "pool": r["pool"], "pool_cores": len(pool),
                "cores_claimed": len(claimed), "cores_free": len(pool) - len(claimed),
                "cpu_busy_percent": round(sum(busy) / len(busy)) if busy else None,
                "cores": [{"cpu": c["cpu"], "busy": c["busy"], "pool": c["pool"], "jobs": c["jobs"]} for c in r["cores"]],
                "mem_total": r["mem_total"], "mem_available": r["mem_available"], "mem_pool": r["mem_pool"],
                "running": sum(1 for j in jobs if j.get("runner") == r["name"] and j["state"] == "running"),
                "queued": r["queued"],
            })
        return {"runners": out, "queued_total": sum(1 for j in jobs if j["state"] == "queued"),
                "git_repos": [gitsrc.normalize(x) for x in self.cfg.git_repos]}

    def t_submit_job(self, a, who, **_):
        cmd = str(a.get("command") or "")
        if not cmd.strip():
            raise ToolError("command is empty")
        spec = {"name": a.get("name") or "job", "cmd": cmd, "cores": a.get("cores", 1), "mem": a.get("mem") or "4G",
                "prio": a.get("priority", 0), "runner": a.get("runner"), "progress_glob": a.get("progress_glob"),
                "progress_total": a.get("progress_total"), "cache": a.get("cache")}
        if a.get("nice") is not None:
            spec["nice"] = a["nice"]
        tar = None
        if a.get("repo"):
            if not a.get("commit"):
                raise ToolError("repo needs a commit")
            # cheap checks first: no download for a job the monitor would refuse anyway
            self.monitor.check(dict(spec))
            try:
                tar, sha, norm, short = gitsrc.fetch_tree(a["repo"], a["commit"], self.cfg.git_repos,
                                                          self.monitor.uploads_dir(), self.github)
            except gitsrc.GitError as e:
                raise ToolError(str(e))
            spec.update(sha=sha[:7], tree="clean", repo=short)
        elif a.get("commit"):
            raise ToolError("commit without repo")
        try:
            jid = self.monitor.submit(who, spec, tar)
        finally:
            if tar and os.path.exists(tar):  # submit moves it into trees/; left only by a refusal
                os.remove(tar)
        key = "%s/tm/%s" % (spec["runner"] or "-", jid)
        return {"id": jid, "key": key, "state": "queued", "commit": sha if tar else None,
                "note": "the key's runner part is '-' until the scheduler places it; the id stays"}

    def t_cancel_job(self, a, who, **_):
        j = self.resolve(a.get("job"))
        return {"key": j["key"], "result": self.monitor.cancel(who, j["key"])}

    def t_retry_job(self, a, who, **_):
        j = self.resolve(a.get("job"))
        jid = self.monitor.retry(who, j["key"])
        return {"retry_of": j["key"], "id": jid, "state": "queued"}

    def t_fetch_outputs(self, a, base_url, **_):
        j = self.resolve(a.get("job"))
        if not j.get("runner"):
            raise ToolError("%s has not reached a runner: nothing to fetch" % j["key"])
        d = self.monitor.show(j["key"], 1)
        files = d.get("out") or []
        url = "%s/api/fetch?key=%s" % (base_url, urllib.parse.quote(j["key"], safe=""))
        return {
            "key": j["key"], "state": d.get("state", j["state"]),
            "files": [{"path": p, "bytes": n} for p, n in files], "count": len(files),
            "listed_max": 300, "bytes": sum(n for _, n in files),
            "download": {"url": url, "format": "tar.gz of out/, log, meta, exit, progress",
                         "curl": 'curl -fH "Authorization: Bearer $TM_TOKEN" -o %s.tar.gz "%s"' % (j["id"], url),
                         "cli": "tm fetch %s" % j["id"]},
        }


class InvalidParams(Exception):
    pass


def ok(mid, result):
    return {"jsonrpc": "2.0", "id": mid, "result": result}


def err(mid, code, message):
    return {"jsonrpc": "2.0", "id": mid, "error": {"code": code, "message": message}}
