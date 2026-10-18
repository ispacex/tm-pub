"""python3 -m unittest discover -s tests"""
import hashlib
import hmac
import importlib.machinery
import importlib.util
import io
import json
import os
import sys
import tarfile
import tempfile
import threading
import time
import unittest
import urllib.error
import urllib.request
from http.server import ThreadingHTTPServer

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from tm import auth, config, gitsrc, server  # noqa: E402
from tm.mcp import Mcp  # noqa: E402
from tm.core import Monitor, Store, estimate, runner_state  # noqa: E402
from tm.remote import RunnerError  # noqa: E402

HERE = os.path.dirname(__file__)


def load_agent():
    path = os.path.join(HERE, "..", "agent", "tm-agent")
    loader = importlib.machinery.SourceFileLoader("tm_agent", path)
    spec = importlib.util.spec_from_loader("tm_agent", loader)
    mod = importlib.util.module_from_spec(spec)
    loader.exec_module(mod)
    return mod


def tg_sign(fields, token):
    check = "\n".join("%s=%s" % (k, fields[k]) for k in sorted(fields))
    return hmac.new(hashlib.sha256(token.encode()).digest(), check.encode(), hashlib.sha256).hexdigest()


class AuthTest(unittest.TestCase):
    TOKEN = "123:abc"

    def test_password(self):
        h = auth.hash_password("pw", iterations=1000)
        self.assertTrue(auth.check_password("pw", h))
        self.assertFalse(auth.check_password("px", h))
        self.assertFalse(auth.check_password("pw", "garbage"))

    def test_session(self):
        s = auth.make_session(b"k", "alice")
        self.assertEqual(auth.read_session(b"k", s), "alice")
        self.assertIsNone(auth.read_session(b"other", s))
        self.assertIsNone(auth.read_session(b"k", s[:-2] + "xx"))
        self.assertIsNone(auth.read_session(b"k", auth.make_session(b"k", "v", now=1)))

    def test_telegram_good(self):
        f = {"id": "123456789", "first_name": "S", "username": "someone", "auth_date": str(int(time.time()))}
        f["hash"] = tg_sign(f, self.TOKEN)
        self.assertEqual(auth.check_telegram(dict(f), self.TOKEN, [123456789]), (123456789, None))

    def test_telegram_forged_hash(self):
        f = {"id": "123456789", "auth_date": str(int(time.time()))}
        f["hash"] = tg_sign(f, "999:other-bot")
        uid, why = auth.check_telegram(f, self.TOKEN, [123456789])
        self.assertIsNone(uid)
        self.assertEqual(why, "bad signature")

    def test_telegram_tampered_field(self):
        f = {"id": "123456789", "auth_date": str(int(time.time()))}
        f["hash"] = tg_sign(f, self.TOKEN)
        f["id"] = "1"
        self.assertEqual(auth.check_telegram(f, self.TOKEN, [123456789, 1])[1], "bad signature")

    def test_telegram_stranger(self):
        f = {"id": "42", "auth_date": str(int(time.time()))}
        f["hash"] = tg_sign(f, self.TOKEN)
        self.assertEqual(auth.check_telegram(f, self.TOKEN, [123456789])[1], "this Telegram account is not on the list")

    def test_telegram_old(self):
        f = {"id": "123456789", "auth_date": str(int(time.time()) - 2 * 86400)}
        f["hash"] = tg_sign(f, self.TOKEN)
        self.assertEqual(auth.check_telegram(f, self.TOKEN, [123456789])[1], "sign-in is older than a day")

    def test_telegram_unconfigured(self):
        self.assertIsNone(auth.check_telegram({"id": "1", "hash": "x"}, "", [1])[0])


class ConfigTest(unittest.TestCase):
    def test_cpus(self):
        self.assertEqual(config.parse_cpus("0-3,7"), [0, 1, 2, 3, 7])
        self.assertEqual(config.format_cpus([0, 1, 2, 7, 9, 8]), "0-2,7-9")
        self.assertEqual(config.parse_mem("13G"), 13 << 30)

    def test_repo_config(self):
        cfg = config.load(os.path.join(HERE, "..", "tm.example.toml"), env={})
        self.assertEqual(cfg.runners[0].name, "runner1")
        self.assertEqual(cfg.runners[0].user, "tm")
        self.assertEqual(set(cfg.runners[0].jobs), {"tm", "batch"})
        self.assertEqual(cfg.allowed_telegram_ids, [])
        self.assertEqual(cfg.purge_after_seconds, 172800)


class AgentTest(unittest.TestCase):
    def test_meta_multiline_cmd(self):
        agent = load_agent()
        m = agent.parse_meta("id=x\nsha=abc\ncpus=7-9\ncmd=echo 1\nfoo=bar\necho 2\n")
        self.assertEqual(m["cpus"], "7-9")
        self.assertEqual(m["cmd"], "echo 1\nfoo=bar\necho 2")

    def test_progress(self):
        agent = load_agent()
        with tempfile.TemporaryDirectory() as d:
            os.makedirs(os.path.join(d, "out"))
            for i in range(3):
                open(os.path.join(d, "out", "%d.json" % i), "w").close()
            self.assertEqual(agent.progress(d, {"progress_glob": "out/*.json", "progress_total": "10"})["done"], 3)
            with open(os.path.join(d, "progress"), "w") as f:
                f.write("7/20\n")
            self.assertEqual(agent.progress(d, {})["total"], 20)
            self.assertIsNone(agent.progress(d + "/nope", {"progress_glob": "../*"}))


class FakeRemote:
    def __init__(self, runner, snapshot):
        self.runner, self.snapshot, self.calls, self.stdins = runner, snapshot, [], []

    def json(self, *args, **kw):
        return self.snapshot

    def call(self, *args, **kw):
        self.calls.append(args)
        self.stdins.append(kw.get("stdin"))
        return b'{"started": "x"}\n'


def snap(jobs, ncpu=10, avail=16 << 30):
    return {"host": {"ncpu": ncpu, "time": time.time(), "mem_total": 19 << 30, "mem_available": avail,
                     "cpu_ticks": {}}, "jobs": jobs}


def running(jid, cpus, kind="batch"):
    return {"kind": kind, "id": jid, "meta": {"cpus": cpus, "submitted": "2026-10-15T19:56:45+00:00"},
            "exit": None, "unit": "active"}


class SchedulerTest(unittest.TestCase):
    def monitor(self, jobs, **kw):
        self.tmp = tempfile.TemporaryDirectory()
        cfg = config.Config(runners=[config.Runner("runner1", "root@x", list(range(10)), 18 << 30, {})],
                            data_dir=self.tmp.name)
        fakes = {}

        def mk(r):
            fakes[r.name] = FakeRemote(r, snap(jobs, **kw))
            return fakes[r.name]
        m = Monitor(cfg, Store(os.path.join(self.tmp.name, "t.db")), mk)
        m.poll_all()
        return m, fakes["runner1"]

    def tearDown(self):
        if hasattr(self, "tmp"):
            self.tmp.cleanup()

    def test_states_from_runner(self):
        self.assertEqual(runner_state({"exit": "0"})[0], "done")
        self.assertEqual(runner_state({"exit": "3"}), ("failed", "3"))
        self.assertEqual(runner_state({"exit": "cancelled"})[0], "cancelled")
        self.assertEqual(runner_state({"exit": None, "unit": "active"})[0], "running")
        self.assertEqual(runner_state({"exit": None, "unit": "gone"})[0], "lost")

    def test_takes_free_cores_only(self):
        m, fake = self.monitor([running("a", "0-6")])
        jid = m.submit("t", {"name": "x", "cmd": "true", "cores": 2, "mem": "1G"})
        m.schedule()
        start = fake.calls[-1]
        self.assertEqual(start[:2], ("start", jid))
        self.assertEqual(start[start.index("--cpus") + 1], "7-8")

    def test_waits_when_full(self):
        m, fake = self.monitor([running("a", "0-6"), running("b", "7-9", "sim")])
        m.submit("t", {"name": "x", "cmd": "true", "cores": 1, "mem": "1G"})
        m.schedule()
        self.assertEqual(fake.calls, [])
        self.assertEqual([j["state"] for j in m.jobs() if j["kind"] == "tm"], ["queued"])

    def test_other_kinds_name_their_repo(self):
        m, fake = self.monitor([running("a", "0-1", "sim"), dict(running("b", "2-3", "sim"), meta={"cpus": "2-3", "repo": "r"})])
        self.assertEqual({j["id"]: j["repo"] for j in m.jobs()}, {"a": "sim", "b": "r"})

    def test_light_job_runs_on_a_full_box(self):
        m, fake = self.monitor([running("a", "0-9")])
        m.submit("t", {"name": "x", "cmd": "true", "cores": 0, "mem": "1G"})
        m.schedule()
        start = fake.calls[-1]
        self.assertEqual(start[start.index("--nice") + 1], "19")

    def test_priority_and_backfill(self):
        m, fake = self.monitor([running("a", "0-6")])
        low = m.submit("t", {"name": "low", "cmd": "true", "cores": 1, "mem": "1G", "prio": 0})
        big = m.submit("t", {"name": "big", "cmd": "true", "cores": 5, "mem": "1G", "prio": 9})
        high = m.submit("t", {"name": "high", "cmd": "true", "cores": 2, "mem": "1G", "prio": 5})
        m.schedule()
        started = [c[1] for c in fake.calls]
        self.assertEqual(started, [high, low])
        self.assertNotIn(big, started)

    def test_memory(self):
        m, fake = self.monitor([], avail=3 << 30)
        m.submit("t", {"name": "x", "cmd": "true", "cores": 1, "mem": "4G"})
        m.schedule()
        self.assertEqual(fake.calls, [])

    def test_cancel_queued(self):
        m, fake = self.monitor([running("a", "0-9")])
        jid = m.submit("t", {"name": "x", "cmd": "true", "cores": 1, "mem": "1G"})
        m.cancel("t", "-/tm/" + jid)
        self.assertEqual([j["state"] for j in m.jobs() if j["id"] == jid], ["cancelled"])
        self.assertEqual(fake.calls, [])

    def test_too_big_refused(self):
        m, _ = self.monitor([])
        with self.assertRaises(ValueError):
            m.submit("t", {"name": "x", "cmd": "true", "cores": 11, "mem": "1G"})

    def test_retry_script_job_reuses_its_tree(self):
        m, fake = self.monitor([{"kind": "batch", "id": "20261015-133957-g64-0f8490e",
                                 "meta": {"cpus": "7-9", "mem": "13G", "sha": "0f8490e", "cmd": "echo"},
                                 "exit": "1", "unit": "gone"}])
        jid = m.retry("t", "runner1/batch/20261015-133957-g64-0f8490e")
        m.schedule()
        start = fake.calls[-1]
        self.assertEqual(start[1], jid)
        self.assertEqual(start[start.index("--from") + 1], "batch/20261015-133957-g64-0f8490e")
        self.assertEqual(start[start.index("--cache") + 1], "batch")


def iso_ago(seconds):
    return time.strftime("%Y-%m-%dT%H:%M:%S+00:00", time.gmtime(time.time() - seconds))


def runner_job(jid, exit, unit="gone", age=3 * 86400, kind="batch", **extra):
    """A job as the snapshot shows it, last touched `age` seconds ago."""
    j = {"kind": kind, "id": jid, "exit": exit, "unit": unit,
         "meta": {"cpus": "0", "submitted": iso_ago(age + 3600)}, "started": iso_ago(age + 3000),
         "finished": iso_ago(age) if exit is not None else None, "log_mtime": int(time.time() - age)}
    j.update(extra)
    return j


class PurgeRemote(FakeRemote):
    def __init__(self, runner, snapshot, refuse=False):
        super().__init__(runner, snapshot)
        self.refuse = refuse

    def call(self, *args, **kw):
        if args and args[0] == "purge" and self.refuse:
            self.calls.append(args)
            raise RunnerError("runner1: tm-agent: refused")
        return super().call(*args, **kw)


class PurgeTest(unittest.TestCase):
    def monitor(self, jobs, purge_after=2 * 86400, refuse=False):
        self.tmp = tempfile.TemporaryDirectory()
        cfg = config.Config(runners=[config.Runner("runner1", "root@x", list(range(10)), 18 << 30, {})],
                            data_dir=self.tmp.name, purge_after_seconds=purge_after)
        self.fake = None

        def mk(r):
            self.fake = PurgeRemote(r, snap(jobs), refuse)
            return self.fake
        m = Monitor(cfg, Store(os.path.join(self.tmp.name, "t.db")), mk)
        m.poll_all()
        return m

    def tearDown(self):
        if hasattr(self, "tmp"):
            self.tmp.cleanup()

    def purges(self):
        return [c[1:] for c in self.fake.calls if c[0] == "purge"]

    def test_stale_failed_and_lost_purged(self):
        m = self.monitor([runner_job("f", "1"), runner_job("l", None)])
        keys = m.purge_stale()
        self.assertEqual(sorted(keys), ["runner1/batch/f", "runner1/batch/l"])
        self.assertEqual(sorted(self.purges()), [("batch", "f", 172800), ("batch", "l", 172800)])
        self.assertEqual([j["id"] for j in m.jobs()], [])  # gone from the picture at once
        ev = m.store.q("SELECT who, action, job FROM events WHERE action='purge' ORDER BY job")
        self.assertEqual(ev, [{"who": "tm", "action": "purge", "job": "runner1/batch/f"},
                              {"who": "tm", "action": "purge", "job": "runner1/batch/l"}])

    def test_fresh_failed_kept(self):
        # submitted long ago, but the log was written an hour ago: the last update counts
        m = self.monitor([runner_job("f", "1", log_mtime=int(time.time() - 3600)),
                          runner_job("g", "1", age=86400)])
        self.assertEqual(m.purge_stale(), [])
        self.assertEqual(self.purges(), [])

    def test_done_cancelled_running_never(self):
        m = self.monitor([runner_job("d", "0", age=30 * 86400), runner_job("c", "cancelled", age=30 * 86400),
                          runner_job("r", None, unit="active", age=30 * 86400)])
        self.assertEqual(m.purge_stale(), [])
        self.assertEqual(self.purges(), [])

    def test_disabled_when_zero(self):
        m = self.monitor([runner_job("f", "1", age=30 * 86400)], purge_after=0)
        self.assertEqual(m.purge_stale(), [])
        self.assertEqual(self.purges(), [])

    def test_unreachable_runner_skipped(self):
        m = self.monitor([runner_job("f", "1"), runner_job("l", None)])
        jid = m.submit("t", {"name": "x", "cmd": "true", "cores": 1, "mem": "1G", "runner": "runner1"})
        m.store.x("UPDATE jobs SET state='dispatched', dispatched=?, submitted=? WHERE id=?",
                  time.time() - 3 * 86400, time.time() - 3 * 86400, jid)
        m.snap["runner1"]["error"] = "runner1: no answer in 40s"
        self.assertEqual(m.purge_stale(), [])
        self.assertEqual(self.purges(), [])
        self.assertEqual(len(m.store.q("SELECT id FROM jobs")), 1)

    def test_refused_is_kept_and_told_once(self):
        m = self.monitor([runner_job("f", "1")], refuse=True)
        self.assertEqual(m.purge_stale(), [])
        self.assertEqual(m.purge_stale(), [])
        self.assertEqual(len(self.purges()), 2)
        self.assertEqual(len(m.store.q("SELECT * FROM events WHERE action='purge-refused'")), 1)
        self.assertEqual([j["id"] for j in m.jobs()], ["f"])

    def test_queue_rows_purged(self):
        m = self.monitor([])
        old = time.time() - 3 * 86400
        failed = m.submit("t", {"name": "failed", "cmd": "true", "cores": 1, "mem": "1G"})
        lost = m.submit("t", {"name": "lost", "cmd": "true", "cores": 1, "mem": "1G"})
        fresh = m.submit("t", {"name": "fresh", "cmd": "true", "cores": 1, "mem": "1G"})
        queued = m.submit("t", {"name": "queued", "cmd": "true", "cores": 1, "mem": "1G"})
        cancelled = m.submit("t", {"name": "cancelled", "cmd": "true", "cores": 1, "mem": "1G"})
        tar = os.path.join(self.tmp.name, "trees", failed + ".tar.gz")
        open(tar, "wb").close()
        m.store.x("UPDATE jobs SET state='failed', runner='runner1', error='dispatch: boom', tar=?, submitted=?, dispatched=? WHERE id=?",
                  tar, old, old, failed)
        m.store.x("UPDATE jobs SET state='dispatched', runner='runner1', submitted=?, dispatched=? WHERE id=?", old, old, lost)
        m.store.x("UPDATE jobs SET state='failed', runner='runner1', error='dispatch: boom', submitted=?, dispatched=? WHERE id=?",
                  old, time.time() - 3600, fresh)
        m.store.x("UPDATE jobs SET submitted=? WHERE id IN (?, ?)", old, queued, cancelled)
        m.store.x("UPDATE jobs SET state='cancelled' WHERE id=?", cancelled)
        m.poll_all()  # the runner's picture is newer than the dispatch
        self.assertEqual(sorted(m.purge_stale()), sorted(["runner1/tm/" + failed, "runner1/tm/" + lost]))
        self.assertEqual(sorted(r["id"] for r in m.store.q("SELECT id FROM jobs")), sorted([fresh, queued, cancelled]))
        self.assertFalse(os.path.exists(tar))
        self.assertEqual(len(m.store.q("SELECT * FROM events WHERE action='purge' AND who='tm'")), 2)

    def test_runner_tm_job_drops_its_row(self):
        m = self.monitor([runner_job("20261010-000000-x-abc", "2", kind="tm")])
        m.store.x("INSERT INTO jobs (id, runner, state, submitted, dispatched) VALUES (?,?,?,?,?)",
                  "20261010-000000-x-abc", "runner1", "dispatched", time.time() - 4 * 86400, time.time() - 4 * 86400)
        self.assertEqual(m.purge_stale(), ["runner1/tm/20261010-000000-x-abc"])
        self.assertEqual(m.store.q("SELECT id FROM jobs"), [])


class AgentPurgeTest(unittest.TestCase):
    def setUp(self):
        self.agent = load_agent()
        self.tmp = tempfile.TemporaryDirectory()
        self.root = os.path.join(self.tmp.name, "batch-jobs")
        os.makedirs(self.root)
        self.conf = {"user": "u", "submit_kind": "tm", "kinds": {"batch": self.root}}
        self.unit = "inactive"
        self.agent.unit_active_state = lambda kind, jid: self.unit

    def tearDown(self):
        self.tmp.cleanup()

    def job(self, jid, exit=None, age=3 * 86400, root=None):
        d = os.path.join(root or self.root, jid)
        os.makedirs(os.path.join(d, "out"))
        old = time.time() - age
        with open(os.path.join(d, "meta"), "w") as f:
            f.write("id=%s\nsubmitted=%s\ncmd=true\n" % (jid, iso_ago(age + 60)))
        with open(os.path.join(d, "log"), "w") as f:
            f.write("started %s on box\n" % iso_ago(age + 30))
        names = ["meta", "log", "out"]
        if exit is not None:
            with open(os.path.join(d, "exit"), "w") as f:
                f.write(exit + "\n")
            names.append("exit")
        for n in names:
            os.utime(os.path.join(d, n), (old, old))
        os.utime(d, (old, old))
        return d

    def purge(self, *args):
        out = io.StringIO()
        old = sys.stdout, sys.stderr
        sys.stdout, sys.stderr = out, io.StringIO()
        try:
            self.agent.cmd_purge(self.conf, [str(a) for a in args])
        except SystemExit as e:
            return None, e.code
        finally:
            sys.stdout, sys.stderr = old
        return json.loads(out.getvalue()), 0

    def test_failed_and_lost_removed(self):
        f, lost = self.job("f", "1"), self.job("l")
        out, code = self.purge("batch", "f", 172800)
        self.assertEqual((out["state"], code), ("failed", 0))
        self.assertEqual(self.purge("batch", "l", 172800)[0]["state"], "lost")
        self.assertFalse(os.path.exists(f) or os.path.exists(lost))

    def test_refuses_done_cancelled_running_young(self):
        keep = [self.job("d", "0"), self.job("c", "cancelled"), self.job("y", "1", age=3600)]
        for jid in ("d", "c", "y"):
            self.assertNotEqual(self.purge("batch", jid, 0)[1], 0, jid)
        self.unit = "active"
        keep.append(self.job("r"))
        keep.append(self.job("r2", "1"))  # an exit file, but its unit still runs
        self.assertNotEqual(self.purge("batch", "r", 0)[1], 0)
        self.assertNotEqual(self.purge("batch", "r2", 0)[1], 0)
        self.unit = None  # systemctl could not tell
        self.assertNotEqual(self.purge("batch", "r", 0)[1], 0)
        self.assertTrue(all(os.path.isdir(d) for d in keep))

    def test_floor_on_the_age(self):
        d = self.job("f", "1", age=3600)
        self.assertNotEqual(self.purge("batch", "f", 1)[1], 0)  # the monitor asking for 1 s does not lower it
        self.assertTrue(os.path.isdir(d))

    def test_refuses_bad_paths(self):
        outside = os.path.join(self.tmp.name, "elsewhere")
        os.makedirs(outside)
        target = self.job("victim", "1", root=outside)
        os.symlink(target, os.path.join(self.root, "link"))
        cache = os.path.join(self.root, "cache")
        os.makedirs(cache)
        with open(os.path.join(cache, "meta"), "w") as f:
            f.write("x\n")
        self.job("runs", "1")
        for kind, jid in (("batch", "../elsewhere/victim"), ("batch", ".."), ("batch", "."), ("batch", "link"),
                          ("batch", "cache"), ("batch", "runs"), ("batch", "nope"), ("tm", "victim"), ("x/..", "f")):
            self.assertNotEqual(self.purge(kind, jid, 172800)[1], 0, (kind, jid))
        self.assertTrue(os.path.isdir(target) and os.path.isdir(cache) and os.path.isdir(os.path.join(self.root, "runs")))
        self.assertNotEqual(self.purge("batch", "f")[1], 0)  # no age given

    def test_symlink_inside_not_followed(self):
        outside = os.path.join(self.tmp.name, "precious")
        os.makedirs(outside)
        open(os.path.join(outside, "keep"), "w").close()
        d = self.job("f", "1")
        os.symlink(outside, os.path.join(d, "out", "link"))
        old = time.time() - 3 * 86400
        os.utime(os.path.join(d, "out"), (old, old))
        self.assertEqual(self.purge("batch", "f", 172800)[1], 0)
        self.assertTrue(os.path.exists(os.path.join(outside, "keep")))


class EtaTest(unittest.TestCase):
    def test_overall_rate(self):
        # 100 of 400 in an hour, no history: 300 more at 100/h
        e = estimate(100, 400, started=0, samples=[], now=3600)
        self.assertEqual((e["eta"], e["rate"], e["basis"]), (10800, 100.0, "overall"))

    def test_recent_rate_wins(self):
        # slow start (100 in the first 3 h), then 60 in the last 20 min: 180/h now
        now = 4 * 3600
        e = estimate(160, 340, started=0, samples=[(now - 1200, 100), (now - 600, 130), (now - 60, 160)], now=now)
        self.assertEqual(e["basis"], "recent")
        self.assertEqual(e["rate"], 180.0)
        self.assertEqual(e["eta"], 3600)

    def test_stalled_since_last_step(self):
        # the recent rate is measured up to now, so a stall stretches the ETA
        now = 3600
        a = estimate(20, 40, 0, [(now - 1500, 10), (now - 900, 20)], now)
        b = estimate(20, 40, 0, [(now - 1500, 10), (now - 900, 20)], now + 600)
        self.assertGreater(b["eta"], a["eta"])

    def test_none(self):
        self.assertIsNone(estimate(0, 10, 0, [], 100))          # nothing done yet
        self.assertIsNone(estimate(10, 10, 0, [], 100))         # finished
        self.assertIsNone(estimate(5, None, 0, [], 100))        # total unknown
        self.assertIsNone(estimate(5, 10, None, [], 100))       # not started
        self.assertIsNone(estimate(1, 16, 0, [], 4))            # too early to tell


def github_tarball(path, files, top="example-org-demo-0123456"):
    """A tarball shaped like GitHub's: a pax global header, then everything under one directory."""
    with tarfile.open(path, "w:gz", format=tarfile.PAX_FORMAT,
                      pax_headers={"comment": "0123456789abcdef0123456789abcdef01234567"}) as tar:
        d = tarfile.TarInfo(top)
        d.type = tarfile.DIRTYPE
        tar.addfile(d)
        for name, data in files.items():
            ti = tarfile.TarInfo(top + "/" + name)
            ti.size = len(data)
            tar.addfile(ti, io.BytesIO(data))


class FakeGitHub:
    SHA = "0123456789abcdef0123456789abcdef01234567"

    def __init__(self, files=None):
        self.files = files or {"run.sh": b"echo hi\n", "src/a.g": b"1;\n"}
        self.asked = []

    def resolve(self, owner_name, ref):
        self.asked.append(("resolve", owner_name, ref))
        if ref == "nope":
            raise gitsrc.GitError("no such commit")
        return self.SHA

    def tarball(self, owner_name, sha, dest):
        self.asked.append(("tarball", owner_name, sha))
        github_tarball(dest, self.files)


class GitSrcTest(unittest.TestCase):
    ALLOW = ["github.com/example-org/example-project", "example-org/other-project"]

    def test_normalize(self):
        for r in ("example-org/Example-Project", "https://github.com/example-org/example-project.git",
                  "git@github.com:example-org/example-project.git", "github.com/example-org/example-project/"):
            self.assertEqual(gitsrc.normalize(r), "github.com/example-org/example-project")
        for bad in ("", "gitlab.com/example-org/tm", "../x", "https://evil.example/example-org/x", "a/b/c"):
            with self.assertRaises(gitsrc.GitError):
                gitsrc.normalize(bad)

    def test_allowlist(self):
        self.assertEqual(gitsrc.allowed("example-org/other-project", self.ALLOW), "github.com/example-org/other-project")
        with self.assertRaises(gitsrc.GitError) as e:
            gitsrc.allowed("example-org/not-listed", self.ALLOW)
        self.assertIn("not allowed", str(e.exception))

    def test_repack_strips_the_top_directory(self):
        with tempfile.TemporaryDirectory() as d:
            src, dst = os.path.join(d, "gh.tar.gz"), os.path.join(d, "t.tar.gz")
            github_tarball(src, {"run.sh": b"x", "sub/b.txt": b"yy"})
            self.assertEqual(gitsrc.repack(src, dst), 2)
            with tarfile.open(dst) as t:
                self.assertEqual(sorted(t.getnames()), ["run.sh", "sub/b.txt"])

    def test_fetch_tree_resolves_a_short_sha(self):
        with tempfile.TemporaryDirectory() as d:
            gh = FakeGitHub()
            tar, sha, norm, short = gitsrc.fetch_tree("example-org/example-project", "0123456", self.ALLOW, d, gh)
            self.assertEqual((sha, norm, short), (FakeGitHub.SHA, "github.com/example-org/example-project", "example-project"))
            self.assertEqual(gh.asked[0], ("resolve", "example-org/example-project", "0123456"))
            self.assertTrue(tar.startswith(d) and os.path.exists(tar))
            self.assertEqual(sorted(os.listdir(d)), [os.path.basename(tar)])  # the raw tarball is gone
            # a full sha is taken as it is, without asking
            gh2 = FakeGitHub()
            gitsrc.fetch_tree("example-org/example-project", FakeGitHub.SHA, self.ALLOW, d, gh2)
            self.assertEqual([a[0] for a in gh2.asked], ["tarball"])

    def test_fetch_tree_refuses_before_downloading(self):
        gh = FakeGitHub()
        with tempfile.TemporaryDirectory() as d:
            for repo, ref in (("example-org/not-listed", "abc1234"), ("example-org/other-project", "a..b"), ("example-org/other-project", "-x")):
                with self.assertRaises(gitsrc.GitError):
                    gitsrc.fetch_tree(repo, ref, self.ALLOW, d, gh)
        self.assertEqual(gh.asked, [])


class ShowRemote(FakeRemote):
    """Answers show/log as the agent would, from the snapshot."""

    def json(self, *args, **kw):
        if args[0] == "show":
            return {"tail": "line 1\nline 2", "out": [["a.json", 10], ["b.json", 20]], "meta": {"cmd": "./run.sh"},
                    "progress": {"done": 3, "total": 10}}
        return self.snapshot

    def call(self, *args, **kw):
        if args[0] == "log":
            return b"l1\nl2\nl3\n"
        if args[0] == "cancel":
            self.calls.append(args)
            return b"cancelled\n"
        return super().call(*args, **kw)


class McpTest(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.cfg = config.Config(runners=[config.Runner("runner1", "root@x", list(range(10)), 18 << 30, {})],
                                 data_dir=self.tmp.name, git_repos=["github.com/example-org/example-project"])
        self.fakes = {}
        job = dict(running("20261016-114010-demo-abc1234", "0-6", kind="tm"), progress={"done": 3, "total": 10},
                   started="2026-10-16T11:40:12+00:00")
        job["meta"].update(cmd="./run.sh", cores="7", name="demo")
        done = {"kind": "batch", "id": "20261015-100000-old-0f8490e", "meta": {"cpus": "7"}, "exit": "0", "unit": "gone"}

        def mk(r):
            self.fakes[r.name] = ShowRemote(r, snap([job, done]))
            return self.fakes[r.name]
        self.monitor = Monitor(self.cfg, Store(os.path.join(self.tmp.name, "t.db")), mk)
        self.monitor.poll_all()
        self.gh = FakeGitHub()
        self.mcp = Mcp(self.cfg, self.monitor, self.gh)

    def tearDown(self):
        self.tmp.cleanup()

    def rpc(self, method, params=None, mid=1):
        return self.mcp.handle({"jsonrpc": "2.0", "id": mid, "method": method, "params": params or {}}, "mcp", "https://t")

    def tool(self, tool_name, **args):
        return self.rpc("tools/call", {"name": tool_name, "arguments": args})["result"]

    def test_initialize_negotiates_the_version(self):
        r = self.rpc("initialize", {"protocolVersion": "2025-03-26", "capabilities": {}, "clientInfo": {"name": "t"}})
        self.assertEqual(r["result"]["protocolVersion"], "2025-03-26")
        self.assertIn("tools", r["result"]["capabilities"])
        r = self.rpc("initialize", {"protocolVersion": "1999-01-01"})
        self.assertEqual(r["result"]["protocolVersion"], "2025-11-25")

    def test_notifications_and_unknowns(self):
        self.assertIsNone(self.mcp.handle({"jsonrpc": "2.0", "method": "notifications/initialized"}, "mcp", ""))
        self.assertEqual(self.rpc("ping")["result"], {})
        self.assertEqual(self.rpc("nope")["error"]["code"], -32601)
        self.assertEqual(self.rpc("tools/call", {"name": "rm_rf"})["error"]["code"], -32602)
        self.assertEqual(self.mcp.handle({"id": 1}, "mcp", "")["error"]["code"], -32600)

    def test_tools_list(self):
        names = [t["name"] for t in self.rpc("tools/list")["result"]["tools"]]
        self.assertEqual(names, ["list_jobs", "get_job", "get_log", "list_runners", "submit_job", "cancel_job",
                                 "retry_job", "fetch_outputs"])

    def test_list_jobs(self):
        r = self.tool("list_jobs")["structuredContent"]
        self.assertEqual([j["key"] for j in r["jobs"]], ["runner1/tm/20261016-114010-demo-abc1234"])
        j = r["jobs"][0]
        self.assertEqual((j["state"], j["cores"], j["progress"]), ("running", 7, {"done": 3, "total": 10}))
        self.assertIsNotNone(j["run_seconds"])
        self.assertEqual(self.tool("list_jobs", all=True)["structuredContent"]["count"], 2)
        self.assertEqual(self.tool("list_jobs", all=True, text="OLD")["structuredContent"]["count"], 1)
        self.assertEqual(self.tool("list_jobs", status="done")["structuredContent"]["count"], 1)
        self.assertEqual(self.tool("list_jobs", runner="other")["structuredContent"]["count"], 0)

    def test_get_job_and_log(self):
        r = self.tool("get_job", job="demo")["structuredContent"]
        self.assertEqual((r["key"], r["command"], r["log_tail"], r["out_files"]),
                         ("runner1/tm/20261016-114010-demo-abc1234", "./run.sh", "line 1\nline 2", 2))
        self.assertEqual(self.tool("get_log", job="demo", lines=3)["content"][0]["text"], "l1\nl2\nl3\n")
        r = self.tool("get_job", job="2026")
        self.assertTrue(r["isError"])
        self.assertIn("matches 2 jobs", r["content"][0]["text"])

    def test_list_runners(self):
        r = self.tool("list_runners")["structuredContent"]
        k = r["runners"][0]
        self.assertEqual((k["name"], k["pool_cores"], k["cores_claimed"], k["cores_free"]), ("runner1", 10, 7, 3))  # the done job's core 7 is free
        self.assertEqual(r["git_repos"], ["github.com/example-org/example-project"])

    def test_submit_from_git(self):
        r = self.tool("submit_job", command="bash run.sh", repo="example-org/example-project", commit="0123456",
                      cores=1, mem="1G", priority=3, name="t1")
        self.assertNotIn("isError", r)
        jid = r["structuredContent"]["id"]
        self.assertTrue(jid.endswith("-t1-0123456"))
        row = self.monitor.store.q("SELECT * FROM jobs WHERE id=?", jid)[0]
        self.assertEqual((row["by"], row["repo"], row["sha"], row["prio"], row["tree"]), ("mcp", "example-project", "0123456", 3, "clean"))
        self.assertEqual(os.listdir(self.monitor.uploads_dir()), [])
        self.monitor.schedule()
        fake = self.fakes["runner1"]
        self.assertEqual(fake.calls[-1][:2], ("start", jid))
        with tarfile.open(fileobj=io.BytesIO(fake.stdins[-1])) as t:
            self.assertEqual(sorted(t.getnames()), ["run.sh", "src/a.g"])

    def test_submit_refusals(self):
        r = self.tool("submit_job", command="x", repo="example-org/not-listed", commit="abc1234")
        self.assertTrue(r["isError"])
        self.assertIn("not allowed", r["content"][0]["text"])
        r = self.tool("submit_job", command="x", repo="example-org/example-project", commit="abc1234", cores=99)
        self.assertTrue(r["isError"])
        self.assertEqual(self.gh.asked, [])  # refused before anything was downloaded
        self.assertTrue(self.tool("submit_job", command="x", commit="abc1234")["isError"])
        self.assertTrue(self.tool("submit_job", command=" ")["isError"])

    def test_submit_without_repo_cancel_and_retry(self):
        jid = self.tool("submit_job", command="echo hi", cores=0, mem="1G")["structuredContent"]["id"]
        self.assertEqual(self.tool("cancel_job", job=jid)["structuredContent"]["result"], "cancelled")
        r = self.tool("retry_job", job=jid)["structuredContent"]
        self.assertEqual(r["retry_of"], "-/tm/" + jid)
        r = self.tool("cancel_job", job="demo")["structuredContent"]
        self.assertEqual(r["result"], "cancelled")
        self.assertEqual(self.fakes["runner1"].calls[-1], ("cancel", "tm", "20261016-114010-demo-abc1234"))

    def test_fetch_outputs_is_a_list_and_a_link(self):
        r = self.tool("fetch_outputs", job="demo")["structuredContent"]
        self.assertEqual((r["count"], r["bytes"]), (2, 30))
        self.assertEqual(r["download"]["url"], "https://t/api/fetch?key=runner1%2Ftm%2F20261016-114010-demo-abc1234")
        self.assertIn("$TM_TOKEN", r["download"]["curl"])


class McpHttpTest(unittest.TestCase):
    TOKEN = "tm_test-token"

    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        cfg = config.Config(runners=[config.Runner("runner1", "root@x", list(range(10)), 18 << 30, {})],
                            data_dir=self.tmp.name, api_token_hashes=[auth.token_hash(self.TOKEN)], secret=b"k")
        mon = Monitor(cfg, Store(os.path.join(self.tmp.name, "t.db")), lambda r: ShowRemote(r, snap([])))
        mon.poll_all()
        self.srv = ThreadingHTTPServer(("127.0.0.1", 0), server.make_handler(server.App(cfg, mon)))
        self.srv.RequestHandlerClass.log_message = lambda *a: None
        threading.Thread(target=self.srv.serve_forever, daemon=True).start()
        self.url = "http://127.0.0.1:%d/mcp" % self.srv.server_address[1]

    def tearDown(self):
        self.srv.shutdown()
        self.srv.server_close()
        self.tmp.cleanup()

    def post(self, body, token=TOKEN, method="POST", headers=None):
        r = urllib.request.Request(self.url, data=json.dumps(body).encode() if body is not None else None, method=method)
        r.add_header("Content-Type", "application/json")
        r.add_header("Accept", "application/json, text/event-stream")
        if token:
            r.add_header("Authorization", "Bearer " + token)
        for k, v in (headers or {}).items():
            r.add_header(k, v)
        try:
            with urllib.request.urlopen(r, timeout=10) as resp:
                raw = resp.read()
                return resp.status, dict(resp.headers), json.loads(raw) if raw else None
        except urllib.error.HTTPError as e:
            raw = e.read()
            e.close()
            return e.code, dict(e.headers), json.loads(raw) if raw.startswith(b"{") else raw

    def test_token_only(self):
        init = {"jsonrpc": "2.0", "id": 1, "method": "initialize", "params": {"protocolVersion": "2025-06-18"}}
        code, h, _ = self.post(init, token=None)
        self.assertEqual(code, 401)
        self.assertIn("Bearer", h["WWW-Authenticate"])
        self.assertEqual(self.post(init, token="tm_wrong")[0], 401)
        # a signed-in browser's cookie is not enough
        cookie = "tm_session=" + auth.make_session(b"k", "alice")
        self.assertEqual(self.post(init, token=None, headers={"Cookie": cookie})[0], 401)

    def test_round_trip(self):
        code, h, r = self.post({"jsonrpc": "2.0", "id": 1, "method": "initialize", "params": {"protocolVersion": "2025-06-18"}})
        self.assertEqual((code, r["result"]["protocolVersion"], h["MCP-Protocol-Version"]), (200, "2025-06-18", "2025-06-18"))
        self.assertEqual(self.post({"jsonrpc": "2.0", "method": "notifications/initialized"})[0], 202)
        code, _, r = self.post({"jsonrpc": "2.0", "id": 2, "method": "tools/call", "params": {"name": "list_jobs", "arguments": {}}})
        self.assertEqual(r["result"]["structuredContent"]["count"], 0)
        code, _, r = self.post([{"jsonrpc": "2.0", "id": 3, "method": "ping"}, {"jsonrpc": "2.0", "method": "notifications/x"}])
        self.assertEqual(r, [{"jsonrpc": "2.0", "id": 3, "result": {}}])
        self.assertEqual(self.post(None, method="GET")[0], 405)
        self.assertEqual(self.post({"jsonrpc": "2.0", "id": 1, "method": "ping"}, headers={"Origin": "https://evil.example"})[0], 403)


if __name__ == "__main__":
    unittest.main()
