#!/usr/bin/env python3
"""Install tm-agent on a runner and let the monitor's key in — for that command only.

    deploy/install-agent.py RUNNER --pubkey tm-monitor.pub --from 203.0.113.10,2001:db8::10

(--from: the monitor's own addresses; the examples are documentation ranges.)

Runs from a machine with root ssh to the runner (your own). It:
  * copies agent/tm-agent to /usr/local/bin/tm-agent (root, 0755);
  * writes /etc/tm-agent.json from the runner's line in tm.toml (its job roots) — the agent
    takes kinds and directories from there, never from the caller;
  * makes the tm job root, owned by the jobs' user (the runner's `user` in tm.toml, default "tm");
  * puts one line into root's authorized_keys, replacing an earlier tm line:
      command="/usr/local/bin/tm-agent",restrict,from="..." <key> tm-monitor
    restrict = no pty, no forwarding of any kind; command= = whatever the client asks, sshd runs
    tm-agent, and the request is only an argument to it;
  * prints the runner's host key line for the monitor's known_hosts.
"""
import argparse
import json
import os
import re
import shlex
import subprocess
import sys

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))
from tm import config  # noqa: E402

MARK = "tm-monitor"


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("runner")
    ap.add_argument("--pubkey", required=True)
    ap.add_argument("--from", dest="from_", required=True, help="addresses the monitor connects from")
    ap.add_argument("--user", default=None, help="user the jobs run as (default: the runner's user in tm.toml)")
    ap.add_argument("--config", default=os.path.join(os.path.dirname(__file__), "..", "tm.toml"))
    a = ap.parse_args()
    cfg = config.load(a.config, env={})
    r = cfg.runner(a.runner)
    if not r:
        sys.exit("no runner %s in %s" % (a.runner, a.config))
    pub = open(a.pubkey).read().strip()
    parts = pub.split()
    if len(parts) < 2 or not parts[0].startswith("ssh-"):
        sys.exit("not a public key: " + a.pubkey)
    agent = open(os.path.join(os.path.dirname(__file__), "..", "agent", "tm-agent")).read()
    user = a.user or r.user
    if not re.match(r"^[a-z_][a-z0-9_-]{0,31}$", user):
        sys.exit("bad user name %r" % user)
    conf = json.dumps({"user": user, "submit_kind": "tm", "kinds": r.jobs}, indent=2)
    line = 'command="/usr/local/bin/tm-agent",restrict,from="%s" %s %s %s' % (a.from_, parts[0], parts[1], MARK)
    tm_root = r.jobs.get("tm")
    script = """set -e
cat > /usr/local/bin/tm-agent.new <<'AGENT_EOF'
%s
AGENT_EOF
install -m 0755 -o root -g root /usr/local/bin/tm-agent.new /usr/local/bin/tm-agent && rm /usr/local/bin/tm-agent.new
cat > /etc/tm-agent.json <<'CONF_EOF'
%s
CONF_EOF
chmod 0644 /etc/tm-agent.json
%s
mkdir -p /root/.ssh && touch /root/.ssh/authorized_keys && chmod 600 /root/.ssh/authorized_keys
cp /root/.ssh/authorized_keys /root/.ssh/authorized_keys.tm-bak
grep -v ' %s$' /root/.ssh/authorized_keys.tm-bak > /root/.ssh/authorized_keys || true
echo %s >> /root/.ssh/authorized_keys
echo installed
""" % (agent, conf,
       ("install -d -o %s -g %s %s" % (user, user, shlex.quote(tm_root))) if tm_root else "",
       MARK, shlex.quote(line))
    subprocess.run(["ssh", "-o", "BatchMode=yes", "-p", str(r.port), r.ssh, "bash -s"], input=script.encode(), check=True)
    host = r.ssh.split("@")[-1]
    keys = subprocess.run(["ssh-keyscan", "-t", "ed25519", "-p", str(r.port), host], capture_output=True, text=True).stdout
    print("known_hosts line for the monitor:")
    print(keys.strip())


if __name__ == "__main__":
    main()
