"""Talking to a runner's tm-agent over ssh. The monitor's key can do nothing else there."""
import json
import shlex
import subprocess


class RunnerError(Exception):
    pass


class Remote:
    def __init__(self, runner, key="", known_hosts="", control_dir="/tmp"):
        self.runner = runner
        self.base = ["ssh", "-o", "BatchMode=yes", "-o", "ConnectTimeout=15", "-o", "LogLevel=ERROR",
                     "-o", "ServerAliveInterval=10", "-o", "ServerAliveCountMax=3",
                     "-o", "ControlMaster=auto", "-o", "ControlPath=%s/tm-ssh-%%C" % control_dir,
                     "-o", "ControlPersist=120", "-p", str(runner.port)]
        if key:
            self.base += ["-i", key, "-o", "IdentitiesOnly=yes"]
        if known_hosts:
            self.base += ["-o", "StrictHostKeyChecking=yes", "-o", "UserKnownHostsFile=" + known_hosts]

    def argv(self, *args):
        # The agent reads SSH_ORIGINAL_COMMAND with shlex, so every argument is quoted here.
        return self.base + [self.runner.ssh, "tm-agent " + " ".join(shlex.quote(str(a)) for a in args)]

    def call(self, *args, stdin=None, timeout=60):
        try:
            r = subprocess.run(self.argv(*args), input=stdin, capture_output=True, timeout=timeout)
        except subprocess.TimeoutExpired:
            raise RunnerError("%s: no answer in %ds" % (self.runner.name, timeout))
        if r.returncode != 0:
            err = r.stderr.decode("utf-8", "replace").strip().splitlines()
            msg = err[-1] if err else "exit %d" % r.returncode
            raise RunnerError("%s: %s" % (self.runner.name, msg))
        return r.stdout

    def json(self, *args, **kw):
        out = self.call(*args, **kw)
        try:
            return json.loads(out)
        except ValueError:
            raise RunnerError("%s: not JSON: %r" % (self.runner.name, out[:200]))

    def stream(self, *args):
        """A process whose stdout is the agent's answer (for fetch)."""
        return subprocess.Popen(self.argv(*args), stdout=subprocess.PIPE, stderr=subprocess.PIPE)
