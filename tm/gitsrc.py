"""A job's tree taken from git by commit, on the monitor's side (MCP submit_job).

An MCP client has no working tree to upload, so the monitor fetches the commit itself: GitHub's
tarball of the repository at that commit, repacked without its top directory, which makes it the
same archive `tm submit` uploads (files at the root of tree/). Only repositories listed in tm.toml
(`git_repos`) are fetched. A private one needs a read-only GitHub token in TM_GITHUB_TOKEN_FILE;
without it only public repositories work.
"""
import os
import re
import secrets
import shutil
import tarfile
import tempfile
import urllib.error
import urllib.request

API = "https://api.github.com"
MAX_TARBALL = 200 * 1024 * 1024
REPO_RE = re.compile(r"^(?:https?://|git@|ssh://git@)?(?:www\.)?github\.com[/:]([A-Za-z0-9_.-]+)/([A-Za-z0-9_.-]+?)(?:\.git)?/?$")
SHORT_RE = re.compile(r"^([A-Za-z0-9_.-]+)/([A-Za-z0-9_.-]+?)(?:\.git)?$")
REF_RE = re.compile(r"^[A-Za-z0-9][A-Za-z0-9._/-]{0,99}$")
SHA_RE = re.compile(r"^[0-9a-f]{40}$")


class GitError(ValueError):
    pass


def normalize(repo):
    """'owner/x', 'https://github.com/owner/x.git', 'git@github.com:owner/x' -> 'github.com/owner/x'."""
    repo = (repo or "").strip()
    m = REPO_RE.match(repo) or SHORT_RE.match(repo)
    if not m or m.group(1) in (".", "..") or m.group(2) in (".", ".."):
        raise GitError("repo: owner/name or a github.com address, e.g. example-org/example-project")
    return "github.com/%s/%s" % (m.group(1).lower(), m.group(2).lower())


def allowed(repo, allowlist):
    """The normalized repository if it is on the list, else GitError naming the list."""
    norm = normalize(repo)
    names = {normalize(x) for x in allowlist}
    if norm not in names:
        raise GitError("repo %s is not allowed; allowed: %s" % (norm, ", ".join(sorted(names)) or "none"))
    return norm


class GitHub:
    """The two calls the monitor makes: a ref to a full sha, and the tarball at that sha."""

    def __init__(self, token="", timeout=120):
        self.token, self.timeout = token, timeout

    def open(self, url, accept):
        r = urllib.request.Request(url, headers={"Accept": accept, "User-Agent": "tm-monitor"})
        if self.token:
            # unredirected: the tarball answer is a redirect to codeload, which gets its own signed URL
            r.add_unredirected_header("Authorization", "Bearer " + self.token)
        try:
            return urllib.request.urlopen(r, timeout=self.timeout)
        except urllib.error.HTTPError as e:
            if e.code in (404, 422):
                raise GitError("%s: no such repository or commit%s" % (
                    url.split("/repos/", 1)[-1], "" if self.token else " (private repositories need TM_GITHUB_TOKEN_FILE)"))
            raise GitError("github: %s %s" % (e.code, e.reason))
        except urllib.error.URLError as e:
            raise GitError("github: %s" % e.reason)

    def resolve(self, owner_name, ref):
        with self.open("%s/repos/%s/commits/%s" % (API, owner_name, ref), "application/vnd.github.sha") as resp:
            sha = resp.read(100).decode().strip()
        if not SHA_RE.match(sha):
            raise GitError("github: unexpected answer for %s@%s" % (owner_name, ref))
        return sha

    def tarball(self, owner_name, sha, dest):
        with self.open("%s/repos/%s/tarball/%s" % (API, owner_name, sha), "application/vnd.github+json") as resp, \
                open(dest, "wb") as f:
            n = 0
            while True:
                chunk = resp.read(1 << 16)
                if not chunk:
                    break
                n += len(chunk)
                if n > MAX_TARBALL:
                    raise GitError("repository tarball over %d MB" % (MAX_TARBALL >> 20))
                f.write(chunk)


def repack(src, dest):
    """GitHub's 'owner-name-sha/...' tarball -> the same files at the archive's root, as `tm submit` sends."""
    count = 0
    with tarfile.open(src, mode="r|gz") as tin, tarfile.open(dest, mode="w:gz") as tout:
        for m in tin:
            if m.type in (tarfile.XGLTYPE, tarfile.XHDTYPE) or "/" not in m.name.strip("/"):
                continue  # the pax global header (the commit id) and the top directory itself
            name = m.name.split("/", 1)[1]
            if not name or name.startswith("/") or ".." in name.split("/"):
                continue
            m.name = name
            if m.isfile():
                tout.addfile(m, tin.extractfile(m))
                count += 1
            elif m.isdir() or m.issym():
                tout.addfile(m)
    return count


def fetch_tree(repo, ref, allowlist, uploads_dir, github=None):
    """(path of the packed tree in uploads_dir, full sha, normalized repo, short name).

    The packed tree is what Monitor.submit takes as tar_path (it moves it into trees/)."""
    norm = allowed(repo, allowlist)
    ref = (ref or "").strip()
    if not REF_RE.match(ref) or ".." in ref:
        raise GitError("commit: a sha (7-40 hex) or a branch/tag name")
    gh = github or GitHub()
    owner_name = norm.split("/", 1)[1]
    sha = ref if SHA_RE.match(ref) else gh.resolve(owner_name, ref)
    os.makedirs(uploads_dir, exist_ok=True)
    tmp = tempfile.mkdtemp(dir=uploads_dir, prefix="git-")
    try:
        raw = os.path.join(tmp, "github.tar.gz")
        gh.tarball(owner_name, sha, raw)
        out = os.path.join(uploads_dir, "git-%s.tar.gz" % secrets.token_hex(8))
        try:
            if repack(raw, out) == 0:
                raise GitError("%s@%s: no files" % (norm, sha[:7]))
        except (tarfile.TarError, OSError, EOFError) as e:
            if os.path.exists(out):
                os.remove(out)
            raise GitError("bad tarball from github: %s" % e)
        except GitError:
            if os.path.exists(out):
                os.remove(out)
            raise
        return out, sha, norm, owner_name.split("/")[1]
    finally:
        shutil.rmtree(tmp, ignore_errors=True)

