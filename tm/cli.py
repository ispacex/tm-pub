"""tm — the command line for people and agents. Talks to the monitor's API with a token.

    tm submit [--name N] [--cores 1] [--mem 4G] [--prio 0] [--runner R] [--untracked] -- COMMAND...
    tm ls [-a] [--runner R] [--state S]
    tm show JOB          tm log JOB [-n 100]       tm cancel JOB
    tm retry JOB         tm fetch JOB [DEST]       tm runners

JOB is a key (runner/kind/id), an id, or any unique part of an id.
Settings: TM_URL and TM_TOKEN, else ~/.config/tm/config (url = ..., token = ...).
Exit codes: 0 fine; 1 the job failed / is not finished; 2 nothing ran.
"""
import argparse
import io
import json
import os
import secrets
import subprocess
import sys
import tarfile
import time
import tomllib
import urllib.error
import urllib.parse
import urllib.request

CHUNK = 512 * 1024


def not_run(msg):
    print("NOT RUN: " + msg, file=sys.stderr)
    sys.exit(2)


def settings():
    conf = {}
    path = os.path.expanduser(os.environ.get("TM_CLI_CONFIG", "~/.config/tm/config"))
    if os.path.exists(path):
        with open(path, "rb") as f:
            conf = tomllib.load(f)
    url = os.environ.get("TM_URL") or conf.get("url")
    if not url:
        not_run("no monitor URL: set TM_URL or put url = \"https://...\" into " + path)
    token = os.environ.get("TM_TOKEN") or conf.get("token")
    if not token:
        not_run("no token: set TM_TOKEN or put token = \"...\" into " + path)
    return url.rstrip("/"), token


class Api:
    def __init__(self):
        self.url, self.token = settings()

    def req(self, method, path, data=None, ctype="application/json", raw=False, timeout=120):
        r = urllib.request.Request(self.url + path, data=data, method=method)
        r.add_header("Authorization", "Bearer " + self.token)
        if data is not None:
            r.add_header("Content-Type", ctype)
        try:
            resp = urllib.request.urlopen(r, timeout=timeout)
        except urllib.error.HTTPError as e:
            body = e.read().decode("utf-8", "replace")
            try:
                body = json.loads(body).get("error", body)
            except ValueError:
                pass
            if e.code == 404:
                print("NOT FOUND: " + body, file=sys.stderr)
                sys.exit(2)
            not_run("%s %s: %s %s" % (method, path, e.code, body))
        except urllib.error.URLError as e:
            not_run("no connection to %s: %s" % (self.url, e.reason))
        if raw:
            return resp
        body = resp.read()
        return json.loads(body) if resp.headers.get("Content-Type", "").startswith("application/json") else body.decode()

    def state(self):
        return self.req("GET", "/api/state")

    def resolve(self, ref):
        jobs = self.state()["jobs"]
        for j in jobs:
            if j["key"] == ref or j["id"] == ref:
                return j
        hits = [j for j in jobs if ref in j["id"]]
        if len(hits) == 1:
            return hits[0]
        if not hits:
            print("NOT FOUND: no job matches %r" % ref, file=sys.stderr)
            sys.exit(2)
        not_run("%r matches %d jobs: %s" % (ref, len(hits), ", ".join(j["id"] for j in hits[:6])))


def git(*args, cwd=None):
    return subprocess.run(["git"] + list(args), cwd=cwd, capture_output=True, check=True).stdout


def pack_tree(untracked):
    try:
        top = git("rev-parse", "--show-toplevel").decode().strip()
    except subprocess.CalledProcessError:
        not_run("not in a git repository")
    sha = git("rev-parse", "--short=7", "HEAD", cwd=top).decode().strip()
    dirty = "dirty" if git("status", "--porcelain", "--untracked-files=no", cwd=top).strip() else "clean"
    try:
        origin = git("remote", "get-url", "origin", cwd=top).decode().strip()
        repo = origin.rstrip("/").rsplit("/", 1)[-1].rsplit(":", 1)[-1].removesuffix(".git")
    except subprocess.CalledProcessError:
        repo = os.path.basename(top)
    files = [f for f in git("ls-files", "-z", cwd=top).decode().split("\0") if f]
    others = [f for f in git("ls-files", "-z", "--others", "--exclude-standard", cwd=top).decode().split("\0") if f]
    if others and not untracked:
        print("%d file(s) outside the index — NOT sent (git add them, or pass --untracked):" % len(others))
        for f in others[:20]:
            print("  " + f)
    if untracked:
        files += others
    buf = io.BytesIO()
    with tarfile.open(fileobj=buf, mode="w:gz") as tar:
        for f in files:
            p = os.path.join(top, f)
            if os.path.lexists(p):  # a deleted-but-tracked file is skipped, as tar would complain
                tar.add(p, arcname=f, recursive=False)
    return buf.getvalue(), sha, dirty, repo


def cmd_submit(api, a):
    if not a.command:
        not_run("no command after --")
    cmd = " ".join(a.command)
    spec = {"name": a.name, "cmd": cmd, "cores": a.cores, "mem": a.mem, "prio": a.prio, "runner": a.runner,
            "progress_glob": a.progress_glob, "progress_total": a.progress_total, "cache": a.cache}
    if a.nice is not None:
        spec["nice"] = a.nice
    if not a.no_tree:
        data, sha, dirty, repo = pack_tree(a.untracked)
        spec.update(sha=sha, tree=dirty, repo=repo)
        up = secrets.token_urlsafe(12)
        for i in range(0, max(len(data), 1), CHUNK):
            api.req("POST", "/api/upload?id=%s&part=%d" % (up, i // CHUNK), data[i:i + CHUNK], "application/gzip")
        spec["upload"] = up
        print("submitting %s: %s (cores %d, mem %s, %s@%s %s, %d KB)" % (a.name, cmd, a.cores, a.mem, repo, sha, dirty, len(data) >> 10))
    r = api.req("POST", "/api/submit", json.dumps(spec).encode())
    print("SUBMITTED: " + r["id"])
    if a.wait:
        return wait(api, r["id"])
    return 0


def wait(api, jid):
    last = None
    while True:
        j = api.resolve(jid)
        if j["state"] != last:
            print("%s  %s" % (time.strftime("%H:%M:%S"), j["state"]), flush=True)
            last = j["state"]
        if j["state"] in ("done", "failed", "cancelled", "lost"):
            return 0 if j["state"] == "done" else 1
        time.sleep(5)


def fmt_dur(s):
    if s is None:
        return ""
    s = int(s)
    return "%dh%02dm" % (s // 3600, s % 3600 // 60) if s >= 3600 else "%dm%02ds" % (s // 60, s % 60)


def state_text(j):
    return "FAILED (exit %s)" % j["exit"] if j["state"] == "failed" and j.get("exit") else j["state"].upper()


def cmd_ls(api, a):
    st = api.state()
    now = st["now"]
    for r in st["runners"]:
        claimed = sum(1 for c in r["cores"] if c["jobs"])
        print("# %s %s  cores claimed %d/%d  load %s" % (r["name"], "online" if r["online"] else "OFFLINE " + (r["error"] or ""),
                                                          claimed, len(r["cores"]), " ".join("%.1f" % x for x in r.get("load") or [])))
    for j in st["jobs"]:
        if a.runner and j["runner"] != a.runner:
            continue
        if a.state and j["state"] != a.state:
            continue
        if not a.all and not a.state and j["state"] in ("done", "cancelled"):
            continue
        p = j.get("progress")
        prog = "%s/%s" % (p["done"], p["total"] or "?") if p else ""
        t = fmt_dur(((j.get("finished") or now) - j["started"]) if j.get("started") else None)
        eta = "eta " + fmt_dur(j["eta"]["eta"]) if j.get("eta") else ""
        print("%-55s %-18s %-9s %-9s %-12s %-7s %s" % (j["key"], state_text(j), prog, t, eta, j.get("cpus") or j.get("cores") or "",
                                                       j["name"]))
    return 0


def cmd_show(api, a):
    j = api.resolve(a.job)
    d = api.req("GET", "/api/job?key=%s&lines=%d" % (urllib.parse.quote(j["key"]), a.lines))
    for k in ("key", "state", "exit", "runner", "kind", "repo", "sha", "tree", "cpus", "cores", "mem", "prio", "by"):
        if d.get(k) not in (None, ""):
            print("%-9s %s" % (k, d[k]))
    for k in ("submitted", "started", "finished"):
        if d.get(k):
            print("%-9s %s" % (k, time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(d[k]))))
    if d.get("progress"):
        print("progress  %s/%s" % (d["progress"]["done"], d["progress"]["total"] or "?"))
    if d.get("eta"):
        e = d["eta"]
        print("eta       %s (at %s; %s/h, %s rate)" % (fmt_dur(e["eta"]), time.strftime("%Y-%m-%d %H:%M", time.localtime(time.time() + e["eta"])),
                                                   e["rate"], "last 30 min" if e["basis"] == "recent" else "overall"))
    print("cmd       " + (d.get("cmd") or ""))
    print("--- log tail:")
    print(d.get("tail") or "")
    return 1 if d["state"] in ("failed", "lost") else 0


def cmd_log(api, a):
    j = api.resolve(a.job)
    sys.stdout.write(api.req("GET", "/api/log?key=%s&lines=%d" % (urllib.parse.quote(j["key"]), a.n)))
    return 0


def cmd_cancel(api, a):
    j = api.resolve(a.job)
    r = api.req("POST", "/api/cancel?key=" + urllib.parse.quote(j["key"]))
    print("CANCELLED: %s (%s)" % (j["key"], r["result"]))
    return 0


def cmd_retry(api, a):
    j = api.resolve(a.job)
    r = api.req("POST", "/api/retry?key=" + urllib.parse.quote(j["key"]))
    print("SUBMITTED: " + r["id"])
    return 0


def cmd_fetch(api, a):
    j = api.resolve(a.job)
    dest = a.dest or os.path.join("remote-results", j["id"])
    os.makedirs(dest, exist_ok=True)
    resp = api.req("GET", "/api/fetch?key=" + urllib.parse.quote(j["key"]), raw=True, timeout=600)
    with tarfile.open(fileobj=resp, mode="r|gz") as tar:
        tar.extractall(dest, filter="data")
    ex = os.path.join(dest, "exit")
    if os.path.exists(ex):
        rc = open(ex).read().strip()
        if rc == "0":
            print("DONE: %s → %s" % (j["id"], dest))
            return 0
        print("FAILED (exit %s): %s → %s" % (rc, j["id"], dest))
        return 1
    print("NOT FINISHED: %s — partial results in %s" % (j["id"], dest))
    return 1


def cmd_runners(api, a):
    for r in api.state()["runners"]:
        cores = " ".join("%d%s" % (c["cpu"], "*" if c["jobs"] else "") for c in r["cores"])
        print("%-10s %-8s pool %-8s cores %s" % (r["name"], "online" if r["online"] else "OFFLINE", r["pool"], cores))
    return 0


def main(argv=None):
    ap = argparse.ArgumentParser(prog="tm", description="computation jobs on the runners")
    sub = ap.add_subparsers(dest="cmd", required=True)
    s = sub.add_parser("submit", help="queue a job: this git tree as it is now, and a command")
    s.add_argument("--name", default="job")
    s.add_argument("--cores", type=int, default=1, help="cores to claim; 0 = a light job that claims none (nice 19)")
    s.add_argument("--mem", default="4G")
    s.add_argument("--prio", type=int, default=0, help="higher goes first")
    s.add_argument("--runner", default=None, help="pin to one runner")
    s.add_argument("--nice", type=int, default=None)
    s.add_argument("--cache", default=None, help="whose $JOB_CACHE to use (a kind or repository name); default: the repository's name")
    s.add_argument("--progress-glob", default=None, help="progress = files matching this, e.g. 'out/*.json'")
    s.add_argument("--progress-total", type=int, default=None)
    s.add_argument("--untracked", action="store_true", help="send files outside the index too")
    s.add_argument("--no-tree", action="store_true", help="send no files: the command runs in an empty tree/")
    s.add_argument("--wait", action="store_true", help="wait for the end; exit 0 done, 1 otherwise")
    s.add_argument("command", nargs=argparse.REMAINDER)
    s.set_defaults(fn=cmd_submit)
    s = sub.add_parser("ls", help="jobs (unfinished and failed; -a for all)")
    s.add_argument("-a", "--all", action="store_true")
    s.add_argument("--runner")
    s.add_argument("--state")
    s.set_defaults(fn=cmd_ls)
    for name, fn, h in (("show", cmd_show, "one job and its log tail"), ("cancel", cmd_cancel, "stop a job"),
                        ("retry", cmd_retry, "run a finished job again")):
        s = sub.add_parser(name, help=h)
        s.add_argument("job")
        if name == "show":
            s.add_argument("--lines", type=int, default=20)
        s.set_defaults(fn=fn)
    s = sub.add_parser("log", help="tail of the log")
    s.add_argument("job")
    s.add_argument("-n", type=int, default=100)
    s.set_defaults(fn=cmd_log)
    s = sub.add_parser("fetch", help="out/, log, meta, exit → remote-results/ID/")
    s.add_argument("job")
    s.add_argument("dest", nargs="?")
    s.set_defaults(fn=cmd_fetch)
    s = sub.add_parser("runners", help="runners and their cores")
    s.set_defaults(fn=cmd_runners)
    a = ap.parse_args(argv)
    if getattr(a, "command", None) and a.command[:1] == ["--"]:
        a.command = a.command[1:]
    sys.exit(a.fn(Api(), a))


if __name__ == "__main__":
    main()
