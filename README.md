# tm — a small job tracker for long computations

tm queues long-running computations (algebra systems, lattice enumerations, proof-assistant
builds, anything that is a shell command), hands them out to the cores of a few runner machines,
and shows what is going on in a web monitor. People use a CLI; AI agents use the same API or an
MCP server built into the monitor.

It is deliberately small: Python standard library only, no database server, no message broker,
no agent daemon on the runners.

```
 laptop / agent              monitor host                         runner(s)
 ──────────────             ──────────────────────               ─────────────────────────────
 tm submit ──HTTPS+token──► tm.server: queue (SQLite)  ──ssh──►  tm-agent (forced command)
 browser ───HTTPS+login───► scheduler, polls every 4 s           <job roots>/<kind>/<ID>/
 MCP client ─HTTPS+token──► /mcp                                 systemd-run: unit <kind>-<ID>
```

## How it works

- **The monitor** (`tm/server.py`) is one process: a web page, a JSON API for the CLI, an MCP
  endpoint, and a scheduler thread. Its own state is a SQLite file holding only the *queue*:
  jobs submitted through tm that have not reached a runner yet.
- **The runner side** is a single script, `agent/tm-agent`, installed as the *forced command*
  of the monitor's ssh key in root's `authorized_keys`. Whatever the monitor asks for over ssh,
  sshd runs `tm-agent` and passes the request as an argument; there is no shell. The agent knows
  exactly seven verbs: `snapshot`, `show`, `log`, `fetch`, `cancel`, `purge`, `start`.
- **A job's state is read from the runner, not from a database.** Every few seconds the monitor
  calls `tm-agent snapshot`. A job is a directory `<root>/<ID>/` with `meta`, `log`, `exit`,
  `out/`, `tree/` and a transient systemd unit `<kind>-<ID>`. Its state: an `exit` file means
  done, failed or cancelled; otherwise an active unit means running; otherwise lost.
- **Cores are handed out as cpusets.** Each runner has a pool of cores (`cpus`) and memory
  (`mem`) tm may use. A job asks for N cores and gets N free ones from the pool, enforced by
  systemd (`AllowedCPUs`, `MemoryMax`, `Nice`, idle I/O class). Memory is also checked against
  the runner's `MemAvailable`.
- **Kinds.** A runner's job roots are grouped by *kind*. The kind `tm` holds tm's own jobs. Any
  other kind is a directory where *your own* scripts create job directories in the same layout
  (`meta` with `cpus=`, `cmd=`, …, a `log`, an `exit` file, a unit named `<kind>-<ID>`). tm shows
  such jobs, counts their cores as taken, and can cancel, retry and purge them. A job of another
  kind takes the kind's name as its repository name and, when rerun, as its `$JOB_CACHE`.

States: `queued` (waiting for cores or memory) → `starting` (handed to a runner, not yet in a
snapshot) → `running` → `done` / `failed` (exit N) / `cancelled` / `lost` (no unit, no exit file).

## Quick start

Requirements: Python ≥ 3.11 everywhere; on runners, systemd and root ssh for the installer.

```bash
git clone https://github.com/<you>/tm-pub.git tm && cd tm
cp tm.example.toml tm.toml          # describe your runners (tm.toml is git-ignored)
python3 -m unittest discover -s tests
```

Run a monitor locally (no TLS, so the session cookie must not be `Secure`):

```bash
export TM_SECRET=$(python3 -c 'import secrets; print(secrets.token_urlsafe(48))')
export TM_USERS="alice:$(python3 -c 'import getpass, sys; sys.path.insert(0, "."); from tm import auth; print(auth.hash_password(getpass.getpass()))')"
export TM_API_TOKEN_HASHES=$(python3 -c 'import secrets, sys; sys.path.insert(0, "."); from tm import auth; t = "tm_" + secrets.token_urlsafe(32); print("token:", t, file=sys.stderr); print(auth.token_hash(t))')
TM_COOKIE_SECURE=0 TM_SSH_KEY=~/.ssh/tm-monitor python3 -m tm.server --listen 127.0.0.1:8080
```

The third command prints the API token once to stderr; only its SHA-256 is kept by the server.

Install the agent on a runner and let the monitor's key in (from a machine with root ssh to the
runner):

```bash
ssh-keygen -t ed25519 -N "" -f ~/.ssh/tm-monitor -C tm-monitor   # the monitor's own key
deploy/install-agent.py runner1 --pubkey ~/.ssh/tm-monitor.pub --from 203.0.113.10
#   --from: the address(es) the monitor connects from. The script prints the runner's host key
#   line: add it to the monitor's known_hosts (TM_SSH_KNOWN_HOSTS).
```

The runner needs the job user (`user` in `tm.toml`, default `tm`) and `python3`.

Set up the CLI:

```bash
ln -s "$PWD/bin/tm" ~/.local/bin/tm
mkdir -p ~/.config/tm && chmod 700 ~/.config/tm
cat > ~/.config/tm/config <<'EOF'
url = "https://tm.example.org"
token = "tm_..."
EOF
chmod 600 ~/.config/tm/config
```

Instead of the file you can set `TM_URL` and `TM_TOKEN`.

## CLI

`tm submit` packs the git tree of the current directory (tracked files, including uncommitted
changes) and runs the command in it on a runner.

```bash
tm submit --name sweep --cores 4 --mem 8G -- './run.sh 1 100 "$JOB_OUT/result.jsonl"'
tm ls                 # unfinished and failed jobs; -a for all; --runner R, --state running
tm show sweep         # one job and its log tail
tm log sweep -n 200
tm cancel sweep
tm retry sweep        # again: the same tree and command, a new job
tm fetch sweep        # out/, log, meta, exit -> remote-results/<ID>/
tm runners            # runners and their cores (* = claimed)
```

A job is named by its key `runner/kind/id`, its id, or any unique part of the id.

| `submit` option | meaning |
|---|---|
| `--cores N` | cores to claim (default 1). `0`: a light job that claims none and runs on the whole pool at nice 19 |
| `--mem 4G` | the unit's `MemoryMax`; the scheduler waits until that much is free |
| `--prio N` | higher goes first; a smaller job may backfill when the first in line does not fit |
| `--runner R` | pin to one runner |
| `--cache NAME` | whose `$JOB_CACHE` to use; default: the repository's name |
| `--progress-glob 'out/*.json' --progress-total 858` | progress as a count of matching files |
| `--untracked` | also send files outside the git index |
| `--no-tree` | send nothing: the command runs in an empty `tree/` |
| `--wait` | wait for the end; exit 0 if done, 1 otherwise |

Inside a job: `$JOB_OUT` (results), `$JOB_ROOT` (the job directory), `$JOB_CACHE` (a per-project
cache shared between jobs; `$JOB_CACHE/bin` is on `PATH`).

**Progress and ETA.** A job may write `done/total` to `$JOB_ROOT/progress`, e.g.
`echo "$i/858" > "$JOB_ROOT/progress"`; otherwise progress is the number of files matching
`progress_glob`. The monitor records every change of progress and estimates the time left from
the rate over the last 30 minutes (if it saw at least two steps there) or else from the rate since
the start. Without a total there is no ETA.

Exit codes: `0` fine; `1` the job failed or is not finished; `2` nothing ran.

## Web monitor

- per-runner core map: fill = the core's load at the last poll, frame = claimed by a job (click
  opens it); memory and load average;
- the job table with filters by runner, state and text (cards on a phone);
- a job's card: command, commit, cores, times, progress, ETA, live log tail, `out/` listing, and
  Cancel / Retry / Download buttons.

## MCP

The monitor is also an MCP server at `https://<monitor>/mcp` (Streamable HTTP, stateless, JSON
answers only). Connect with the same API token as the CLI, e.g. in Claude Code:

```bash
claude mcp add --transport http tm https://tm.example.org/mcp --header "Authorization: Bearer <token>"
```

| tool | what it does |
|---|---|
| `list_jobs` | jobs, newest first: state, progress, ETA, cores, run time; filters `runner`, `status`, `text`, `all` |
| `get_job` | one job: meta, progress, ETA, log tail (`lines`), files in `out/` |
| `get_log` | the last N lines of the log (up to 5000) |
| `list_runners` | runners: online, pool cores claimed/free, per-core load, memory, queue; and `git_repos` |
| `submit_job` | queue a job: `command`, `repo` + `commit`, `cores`, `mem`, `priority`, `runner`, `progress_glob` / `progress_total`, `cache`, `nice` |
| `cancel_job`, `retry_job` | as the buttons in the web card |
| `fetch_outputs` | the `out/` listing with sizes and a download link for a `.tar.gz` (same Bearer token); the bytes do not go through MCP |

**Where `submit_job` gets the tree.** An MCP client has no working tree to upload, so the
monitor fetches GitHub's tarball of `repo` at `commit`, repacks it without its top directory, and
queues it as if `tm submit` had uploaded it. Only repositories listed in `git_repos` are fetched;
a private one needs a read-only, fine-grained token (Contents: read) in `TM_GITHUB_TOKEN_FILE`.
Submodules are not included. Without `repo` the command runs in an empty tree.

Supported methods: `initialize` (protocol versions 2025-11-25, 2025-06-18, 2025-03-26,
2024-11-05), `ping`, `tools/list`, `tools/call`; notifications get 202. A tool's refusal comes back
as a result with `isError`, so the model reads it.

## Configuration

Non-secret settings live in `tm.toml` (see `tm.example.toml`; the file is git-ignored):

| key | meaning |
|---|---|
| `runners` | `{ name, ssh, cpus, mem, jobs = { kind = root, ... }, user, port, disabled }` per runner |
| `allowed_telegram_ids`, `telegram_bot` | optional Telegram sign-in: numeric ids allowed in, the bot's username |
| `git_repos` | repositories MCP `submit_job` may fetch, e.g. `github.com/example-org/example-project` |
| `poll_seconds`, `lost_after_seconds` | polling interval; when a dispatched job that never appeared counts as lost |
| `purge_after_seconds` | failed/lost jobs untouched this long are removed (hourly; `0` = never; default two days) |
| `default_nice` | nice of normal jobs (light `--cores 0` jobs always get 19) |

Secrets and paths come from the environment:

| variable | meaning |
|---|---|
| `TM_CONFIG` | path of `tm.toml` (default `./tm.toml`) |
| `TM_DATA` | data directory: `tm.db`, uploaded trees (default `./data`) |
| `TM_LISTEN` | `host:port` (default `0.0.0.0:8080`) |
| `TM_SECRET` | key that signs session cookies (random per process if unset) |
| `TM_USERS` | `name:pbkdf2-hash,...` for password sign-in |
| `TM_API_TOKEN_HASHES` | comma-separated SHA-256 hashes of API tokens |
| `TM_SSH_KEY`, `TM_SSH_KNOWN_HOSTS` | the monitor's private key for the runners and its known_hosts |
| `TM_TELEGRAM_TOKEN_FILE` (or `TM_TELEGRAM_TOKEN`) | the Telegram bot token |
| `TM_GITHUB_TOKEN_FILE` (or `TM_GITHUB_TOKEN`) | read-only GitHub token for private `git_repos` |
| `TM_COOKIE_SECURE` | `0` to drop the `Secure` cookie flag (local HTTP only) |

On a runner, `/etc/tm-agent.json` (written by `deploy/install-agent.py`) holds the job user and
the kinds with their roots: `{"user": "tm", "submit_kind": "tm", "kinds": {"tm": "/srv/tm-jobs/tm"}}`.

**Cleanup.** Every hour the monitor removes failed and lost jobs untouched for
`purge_after_seconds`: on a runner through `tm-agent purge`, which re-checks everything itself
(never a done, cancelled or running job, never one touched less than a day ago whatever the
monitor asks, never anything but a plain directory right under the kind's root, never the shared
`cache` or `runs`); in the queue, failed and never-seen rows with their tree archives. A runner
that did not answer the last poll is skipped. Each removal is logged in the `events` table.

## Deployment

`deploy/docker-compose.yml` runs the monitor as one container behind a reverse proxy (it assumes
nginx-proxy + acme-companion on an external network `proxy`; adapt it to yours). Layout on the
host, under `TM_DIR` (default `/srv/tm`):

```
$TM_DIR/app/          code (deploy/deploy.sh copies it with rsync)
$TM_DIR/tm.toml       your configuration
$TM_DIR/tm.env   600  TM_SECRET, TM_USERS, TM_API_TOKEN_HASHES
$TM_DIR/ssh/     700  id_ed25519 (the monitor's key to the runners), known_hosts
$TM_DIR/secrets/ 700  telegram_bot_token, github_token (both optional, 600)
$TM_DIR/data/         tm.db, trees/
```

The container runs as uid 10001, so those directories belong to 10001. Write `tm.env` values
without `$`: compose substitutes variables in env files too (the password hash format has no `$`).
Then:

```bash
TM_PUBLIC_HOST=tm.example.org deploy/deploy.sh root@monitor.example.org
```

## Security notes

- **Web sign-in**: a password (only a PBKDF2-SHA256 hash with 600 000 iterations is stored; ten
  failures in 15 minutes block the address) or the Telegram Login Widget (the signature is
  checked on the server, `auth_date` must be under a day old, and only numeric ids from
  `allowed_telegram_ids` are let in). The session is an HMAC-signed cookie, `HttpOnly; Secure;
  SameSite=Lax`, 14 days. Browser writes need an `X-TM: 1` header, which a cross-site form cannot
  send. Pages carry a CSP, `X-Frame-Options: DENY`, `Referrer-Policy: no-referrer`. Without
  signing in only `/login`, `/static/*` and `/healthz` are served.
- **API and MCP**: `Authorization: Bearer <token>`; the server stores only SHA-256 hashes.
  `/mcp` accepts the token only, never the browser cookie, so another site's page cannot call
  tools as a signed-in user; a foreign `Origin` gets 403 (DNS rebinding). Request logs contain
  the path only, never the query string.
- **The monitor's key on a runner** is a dedicated key whose `authorized_keys` line is
  ```
  command="/usr/local/bin/tm-agent",restrict,from="<monitor addresses>" ssh-ed25519 AAAA... tm-monitor
  ```
  `command=` means sshd runs `tm-agent` whatever the client asks; the request arrives in
  `SSH_ORIGINAL_COMMAND`, parsed with `shlex`, no shell. `restrict` turns off pty and all
  forwarding; `from=` limits the key to the monitor's addresses. The kinds and directories come
  from the agent's own `/etc/tm-agent.json`, never from the caller; job ids are checked against
  a strict pattern. `start` runs the command as the unprivileged job user in a unit with CPU,
  memory and priority limits. The key therefore grants what `tm submit` grants — running code
  as the job user — and not a root shell. (A non-root agent would not help: `systemd-run --uid`
  and `systemctl stop` need root anyway, so it would need sudo for the same commands.)
- No keys or tokens are ever sent to the browser. Keep `tm.toml`, `tm.env` and the key files out
  of version control (`.gitignore` already covers `tm.toml` and `*.env`).

## Development

```bash
python3 -m unittest discover -s tests
```

Files: `tm/server.py` (HTTP, sign-in), `tm/mcp.py` (MCP at `/mcp`), `tm/gitsrc.py` (a tree from
GitHub by commit), `tm/core.py` (queue, scheduler, the merged picture), `tm/remote.py` (ssh to the
agent), `tm/auth.py`, `tm/config.py`, `tm/cli.py`, `tm/static/` (the page), `agent/tm-agent`
(the runner side), `deploy/` (compose, deploy, agent installer).

## License

MIT, see [LICENSE](LICENSE).
