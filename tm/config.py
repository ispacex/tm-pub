"""The monitor's configuration: tm.toml (runners, whitelist) plus secrets from the environment."""
import os
import re
import tomllib
from dataclasses import dataclass, field


def parse_cpus(spec):
    """'0-3,7' -> [0, 1, 2, 3, 7]"""
    cpus = []
    for part in str(spec).split(","):
        part = part.strip()
        if not part:
            continue
        if "-" in part:
            a, b = part.split("-", 1)
            cpus.extend(range(int(a), int(b) + 1))
        else:
            cpus.append(int(part))
    return sorted(set(cpus))


def format_cpus(cpus):
    """[0, 1, 2, 5] -> '0-2,5'"""
    cpus = sorted(set(cpus))
    out, i = [], 0
    while i < len(cpus):
        j = i
        while j + 1 < len(cpus) and cpus[j + 1] == cpus[j] + 1:
            j += 1
        out.append(str(cpus[i]) if i == j else "%d-%d" % (cpus[i], cpus[j]))
        i = j + 1
    return ",".join(out)


_UNITS = {"": 1, "K": 1 << 10, "M": 1 << 20, "G": 1 << 30, "T": 1 << 40}


def parse_mem(spec):
    m = re.match(r"^\s*(\d+(?:\.\d+)?)\s*([KMGT]?)i?B?\s*$", str(spec), re.I)
    if not m:
        raise ValueError("bad memory size %r" % spec)
    return int(float(m.group(1)) * _UNITS[m.group(2).upper()])


@dataclass
class Runner:
    name: str
    ssh: str
    cpus: list            # cores tm may hand out
    mem: int              # bytes tm may hand out (also checked against MemAvailable)
    jobs: dict            # kind -> job root, as the agent on the runner has them
    port: int = 22
    disabled: bool = False
    user: str = "tm"      # the account jobs run as there (deploy/install-agent.py)


@dataclass
class Config:
    runners: list
    allowed_telegram_ids: list = field(default_factory=list)
    telegram_bot: str = ""
    poll_seconds: float = 4.0
    lost_after_seconds: float = 90.0
    purge_after_seconds: float = 2 * 86400.0  # failed/lost jobs untouched this long are removed; 0: never
    default_nice: int = 15
    users: dict = field(default_factory=dict)   # username -> pbkdf2 hash (from env)
    api_token_hashes: list = field(default_factory=list)
    secret: bytes = b""
    telegram_token: str = ""
    cookie_secure: bool = True
    data_dir: str = "data"
    ssh_key: str = ""
    ssh_known_hosts: str = ""
    git_repos: list = field(default_factory=list)  # what MCP submit_job may fetch by commit
    github_token: str = ""                          # read-only, for private ones among them

    def runner(self, name):
        for r in self.runners:
            if r.name == name:
                return r
        return None


def load(path=None, env=None):
    env = os.environ if env is None else env
    path = path or env.get("TM_CONFIG", "tm.toml")
    with open(path, "rb") as f:
        raw = tomllib.load(f)
    runners = []
    for r in raw.get("runners", []):
        runners.append(Runner(name=r["name"], ssh=r["ssh"], cpus=parse_cpus(r["cpus"]), mem=parse_mem(r["mem"]),
                              jobs=dict(r.get("jobs", {})), port=int(r.get("port", 22)),
                              disabled=bool(r.get("disabled", False)),
                              user=str(r.get("user", "tm"))))
    users = {}
    for item in filter(None, env.get("TM_USERS", "").split(",")):
        name, _, h = item.partition(":")
        users[name.strip()] = h.strip()
    tg_token = env.get("TM_TELEGRAM_TOKEN", "")
    tg_file = env.get("TM_TELEGRAM_TOKEN_FILE", "")
    if not tg_token and tg_file and os.path.exists(tg_file):
        tg_token = open(tg_file).read().strip()
    return Config(
        runners=runners,
        allowed_telegram_ids=[int(x) for x in raw.get("allowed_telegram_ids", [])],
        telegram_bot=raw.get("telegram_bot", "").lstrip("@"),
        poll_seconds=float(raw.get("poll_seconds", 4)),
        lost_after_seconds=float(raw.get("lost_after_seconds", 90)),
        purge_after_seconds=float(raw.get("purge_after_seconds", 2 * 86400)),
        default_nice=int(raw.get("default_nice", 15)),
        users=users,
        api_token_hashes=[h.strip() for h in env.get("TM_API_TOKEN_HASHES", "").split(",") if h.strip()],
        secret=env.get("TM_SECRET", "").encode(),
        telegram_token=tg_token,
        cookie_secure=env.get("TM_COOKIE_SECURE", "1") != "0",
        data_dir=env.get("TM_DATA", "data"),
        ssh_key=env.get("TM_SSH_KEY", ""),
        ssh_known_hosts=env.get("TM_SSH_KNOWN_HOSTS", ""),
        git_repos=[str(x) for x in raw.get("git_repos", [])],
        github_token=github_token(env),
    )


def github_token(env):
    path = env.get("TM_GITHUB_TOKEN_FILE", "")
    if path and os.path.exists(path):
        with open(path) as f:
            return f.read().strip()
    return env.get("TM_GITHUB_TOKEN", "")
