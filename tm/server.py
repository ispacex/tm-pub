"""The web monitor and the API the CLI talks to. Python stdlib only.

    python -m tm.server [--listen 0.0.0.0:8080]

Everything but /login, /auth/telegram, /static/* and /healthz needs a session (password or
Telegram) or an API token (Authorization: Bearer ...). /mcp (tm/mcp.py) takes the token only.
"""
import argparse
import html
import json
import os
import re
import secrets
import threading
import time
import urllib.parse
from http import cookies
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

from . import auth, config
from .core import Monitor
from .mcp import Mcp

STATIC = os.path.join(os.path.dirname(__file__), "static")
COOKIE = "tm_session"
MAX_UPLOAD = 200 * 1024 * 1024
UPLOAD_ID = re.compile(r"^[A-Za-z0-9_-]{8,64}$")
CSP = ("default-src 'self'; script-src 'self' https://telegram.org; frame-src https://oauth.telegram.org; "
       "img-src 'self' data: https://t.me https://*.telegram.org; style-src 'self' 'unsafe-inline'; "
       "connect-src 'self'; frame-ancestors 'none'; base-uri 'none'; form-action 'self'")


class Failures:
    """Failed sign-ins per address: ten in fifteen minutes and the address waits."""

    def __init__(self):
        self.lock = threading.Lock()
        self.seen = {}

    def blocked(self, ip):
        with self.lock:
            now = time.time()
            hits = [t for t in self.seen.get(ip, []) if now - t < 900]
            self.seen[ip] = hits
            return len(hits) >= 10

    def add(self, ip):
        with self.lock:
            self.seen.setdefault(ip, []).append(time.time())


class App:
    def __init__(self, cfg, monitor):
        self.cfg = cfg
        self.monitor = monitor
        self.failures = Failures()
        self.mcp = Mcp(cfg, monitor)


def make_handler(app):
    cfg = app.cfg

    class H(BaseHTTPRequestHandler):
        server_version = "tm"
        sys_version = ""

        def log_message(self, fmt, *args):  # one line per request, never a query string
            print("%s %s %s" % (self.client_ip(), self.command, self.path.split("?")[0]), *args[1:2], flush=True)

        def base_url(self):
            host = self.headers.get("Host", "localhost")
            if not re.match(r"^[A-Za-z0-9.:-]+$", host):
                host = "localhost"
            proto = self.headers.get("X-Forwarded-Proto", "http")
            return "%s://%s" % ("https" if proto == "https" else "http", host)

        def client_ip(self):
            return self.headers.get("X-Real-IP") or self.client_address[0]

        # ---- responses ----
        def send(self, code, body=b"", ctype="text/plain; charset=utf-8", headers=None):
            if isinstance(body, str):
                body = body.encode()
            self.send_response(code)
            self.send_header("Content-Type", ctype)
            self.send_header("Content-Length", str(len(body)))
            self.send_header("Content-Security-Policy", CSP)
            self.send_header("X-Content-Type-Options", "nosniff")
            self.send_header("Referrer-Policy", "no-referrer")
            self.send_header("X-Frame-Options", "DENY")
            if not ctype.startswith(("text/css", "application/javascript")):
                self.send_header("Cache-Control", "no-store")
            for k, v in (headers or {}).items():
                self.send_header(k, v)
            self.end_headers()
            if self.command != "HEAD":
                self.wfile.write(body)

        def json_out(self, obj, code=200):
            self.send(code, json.dumps(obj, ensure_ascii=False), "application/json; charset=utf-8")

        def redirect(self, to, headers=None):
            h = {"Location": to}
            h.update(headers or {})
            self.send(303, b"", headers=h)

        def cookie(self, value, max_age):
            c = "%s=%s; Path=/; HttpOnly; SameSite=Lax; Max-Age=%d" % (COOKIE, value, max_age)
            return c + ("; Secure" if cfg.cookie_secure else "")

        # ---- who ----
        def who(self):
            h = self.headers.get("Authorization", "")
            if h.startswith("Bearer "):
                return "token" if auth.check_token(h[7:].strip(), cfg.api_token_hashes) else None
            c = cookies.SimpleCookie(self.headers.get("Cookie", ""))
            if COOKIE in c:
                return auth.read_session(cfg.secret, c[COOKIE].value)
            return None

        def need(self, write=False):
            w = self.who()
            if not w:
                if self.path.startswith("/api/"):
                    self.json_out({"error": "sign in"}, 401)
                else:
                    self.redirect("/login")
                return None
            # A write from a browser must carry our own header: a form on another site cannot.
            if write and not self.headers.get("Authorization") and self.headers.get("X-TM") != "1":
                self.json_out({"error": "missing X-TM header"}, 403)
                return None
            return w

        def query(self):
            return {k: v[-1] for k, v in urllib.parse.parse_qs(urllib.parse.urlsplit(self.path).query).items()}

        def body(self, limit=1 << 20):
            n = int(self.headers.get("Content-Length") or 0)
            if n > limit:
                raise ValueError("too large")
            return self.rfile.read(n) if n else b""

        # ---- routes ----
        def do_HEAD(self):
            self.do_GET()

        def do_GET(self):
            path = urllib.parse.urlsplit(self.path).path
            try:
                if path == "/healthz":
                    return self.send(200, "ok")
                if path == "/mcp":  # no server-initiated stream: the transport allows 405 here
                    return self.send(405, "POST only", headers={"Allow": "POST"})
                if path.startswith("/static/"):
                    return self.static(path[len("/static/"):])
                if path == "/login":
                    return self.login_page()
                if path == "/auth/telegram":
                    return self.telegram()
                if path in ("/", "/index.html") or path.startswith("/job/"):
                    if not self.need():
                        return
                    return self.static("index.html")
                if not path.startswith("/api/"):
                    return self.send(404, "not found")
                who = self.need()
                if not who:
                    return
                q = self.query()
                if path == "/api/state":
                    return self.json_out(app.monitor.state())
                if path == "/api/job":
                    return self.json_out(app.monitor.show(q.get("key", ""), int(q.get("lines", 200))))
                if path == "/api/log":
                    return self.send(200, app.monitor.log(q.get("key", ""), min(int(q.get("lines", 200)), 20000)))
                if path == "/api/fetch":
                    return self.fetch(q.get("key", ""))
                if path == "/api/me":
                    return self.json_out({"who": who})
                return self.json_out({"error": "no such call"}, 404)
            except KeyError:
                self.json_out({"error": "no such job"}, 404)
            except Exception as e:  # noqa: BLE001
                self.json_out({"error": str(e)}, 500)

        def do_POST(self):
            path = urllib.parse.urlsplit(self.path).path
            try:
                if path == "/login":
                    return self.login_post()
                if path == "/logout":
                    return self.redirect("/login", {"Set-Cookie": self.cookie("", 0)})
                if path == "/mcp":
                    return self.mcp_post()
                who = self.need(write=True)
                if not who:
                    return
                q = self.query()
                if path == "/api/cancel":
                    return self.json_out({"result": app.monitor.cancel(who, q.get("key", ""))})
                if path == "/api/retry":
                    return self.json_out({"id": app.monitor.retry(who, q.get("key", ""))})
                if path == "/api/upload":
                    return self.upload(q)
                if path == "/api/submit":
                    spec = json.loads(self.body() or b"{}")
                    tar = None
                    up = spec.pop("upload", None)
                    if up:
                        if not UPLOAD_ID.match(up):
                            raise ValueError("bad upload id")
                        tar = os.path.join(cfg.data_dir, "uploads", up + ".tar.gz")
                        if not os.path.exists(tar):
                            raise ValueError("no such upload")
                    return self.json_out({"id": app.monitor.submit(who, spec, tar)})
                return self.json_out({"error": "no such call"}, 404)
            except KeyError:
                self.json_out({"error": "no such job"}, 404)
            except ValueError as e:
                self.json_out({"error": str(e)}, 400)
            except Exception as e:  # noqa: BLE001
                self.json_out({"error": str(e)}, 500)

        def do_DELETE(self):  # a client ending its session: there is none to end
            self.send(405, "POST only", headers={"Allow": "POST"})

        def mcp_post(self):
            """Streamable HTTP, stateless, JSON answers only. A Bearer token and nothing else: a browser's
            cookie must not be able to call tools from another site's page."""
            h = self.headers.get("Authorization", "")
            if not (h.startswith("Bearer ") and auth.check_token(h[7:].strip(), cfg.api_token_hashes)):
                return self.json_out_h({"jsonrpc": "2.0", "id": None, "error": {"code": -32001, "message": "Bearer token required"}},
                                       401, {"WWW-Authenticate": 'Bearer realm="tm"'})
            origin = self.headers.get("Origin")
            if origin and origin != self.base_url():  # DNS rebinding: a page elsewhere talking to us
                return self.json_out({"error": "origin not allowed"}, 403)
            try:
                msg = json.loads(self.body(1 << 20) or b"null")
            except ValueError:
                return self.json_out({"jsonrpc": "2.0", "id": None, "error": {"code": -32700, "message": "parse error"}}, 400)
            base = self.base_url()
            if isinstance(msg, list):
                out = [r for r in (app.mcp.handle(m, "mcp", base) for m in msg) if r is not None]
            else:
                out = app.mcp.handle(msg, "mcp", base)
            if not out:  # notifications and responses only
                return self.send(202, b"")
            hdr = {}
            if isinstance(msg, dict) and msg.get("method") == "initialize" and "result" in out:
                hdr["MCP-Protocol-Version"] = out["result"]["protocolVersion"]
            return self.json_out_h(out, 200, hdr)

        def json_out_h(self, obj, code, headers):
            self.send(code, json.dumps(obj, ensure_ascii=False), "application/json; charset=utf-8", headers)

        # ---- pieces ----
        def static(self, name):
            if not re.match(r"^[a-z0-9_.-]+$", name):
                return self.send(404, "not found")
            p = os.path.join(STATIC, name)
            if not os.path.isfile(p):
                return self.send(404, "not found")
            ctype = {"html": "text/html; charset=utf-8", "js": "application/javascript; charset=utf-8",
                     "css": "text/css; charset=utf-8", "svg": "image/svg+xml"}.get(name.rsplit(".", 1)[-1], "application/octet-stream")
            with open(p, "rb") as f:
                self.send(200, f.read(), ctype, {"Cache-Control": "no-cache"} if not name.endswith(".html") else None)

        def login_page(self, error=""):
            with open(os.path.join(STATIC, "login.html"), encoding="utf-8") as f:
                page = f.read()
            tg = ""
            if cfg.telegram_bot and cfg.telegram_token:
                tg = ('<script async src="https://telegram.org/js/telegram-widget.js?22" '
                      'data-telegram-login="%s" data-size="large" data-userpic="false" '
                      'data-auth-url="%s" data-request-access="read"></script>') % (
                    html.escape(cfg.telegram_bot), html.escape(self.base_url() + "/auth/telegram"))
            page = page.replace("{{telegram}}", tg).replace("{{error}}", html.escape(error))
            self.send(401 if error else 200, page, "text/html; charset=utf-8")

        def login_post(self):
            ip = self.client_ip()
            if app.failures.blocked(ip):
                return self.login_page("Too many attempts, wait 15 minutes.")
            form = urllib.parse.parse_qs(self.body(16 * 1024).decode())
            user = (form.get("user") or [""])[0]
            pw = (form.get("password") or [""])[0]
            stored = cfg.users.get(user)
            if stored and auth.check_password(pw, stored):
                return self.redirect("/", {"Set-Cookie": self.cookie(auth.make_session(cfg.secret, user), auth.SESSION_TTL)})
            app.failures.add(ip)
            time.sleep(1)
            return self.login_page("Wrong user name or password.")

        def telegram(self):
            ip = self.client_ip()
            if app.failures.blocked(ip):
                return self.login_page("Too many attempts, wait 15 minutes.")
            fields = self.query()
            uid, why = auth.check_telegram(fields, cfg.telegram_token, cfg.allowed_telegram_ids)
            if not uid:
                app.failures.add(ip)
                print("telegram sign-in refused: %s (id %s)" % (why, fields.get("id")), flush=True)
                return self.login_page("Telegram sign-in refused: %s." % why)
            who = "tg:%d" % uid
            return self.redirect("/", {"Set-Cookie": self.cookie(auth.make_session(cfg.secret, who), auth.SESSION_TTL)})

        def upload(self, q):
            up = q.get("id", "")
            if not UPLOAD_ID.match(up):
                raise ValueError("bad upload id")
            part = int(q.get("part", "0"))
            path = os.path.join(cfg.data_dir, "uploads", up + ".tar.gz")
            data = self.body(1 << 20)
            if part == 0 and os.path.exists(path):
                os.remove(path)
            size = os.path.getsize(path) if os.path.exists(path) else 0
            if size + len(data) > MAX_UPLOAD:
                raise ValueError("upload over %d MB" % (MAX_UPLOAD >> 20))
            with open(path, "ab") as f:
                f.write(data)
            return self.json_out({"size": size + len(data)})

        def fetch(self, key):
            j, proc = app.monitor.fetch_proc(key)
            self.send_response(200)
            self.send_header("Content-Type", "application/gzip")
            self.send_header("Content-Disposition", 'attachment; filename="%s.tar.gz"' % j["id"])
            self.send_header("Cache-Control", "no-store")
            self.end_headers()
            try:
                while True:
                    chunk = proc.stdout.read(64 * 1024)
                    if not chunk:
                        break
                    self.wfile.write(chunk)
            finally:
                proc.kill()
                proc.wait()
            self.close_connection = True

    return H


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--listen", default=os.environ.get("TM_LISTEN", "0.0.0.0:8080"))
    ap.add_argument("--config", default=None)
    args = ap.parse_args()
    cfg = config.load(args.config)
    if not cfg.secret:
        cfg.secret = secrets.token_bytes(32)
        print("TM_SECRET is not set: sessions end with the process", flush=True)
    monitor = Monitor(cfg)
    stop = threading.Event()
    threading.Thread(target=monitor.loop, args=(stop,), daemon=True).start()
    host, port = args.listen.rsplit(":", 1)
    srv = ThreadingHTTPServer((host, int(port)), make_handler(App(cfg, monitor)))
    srv.daemon_threads = True
    print("tm monitor on %s, runners: %s" % (args.listen, ", ".join(r.name for r in cfg.runners)), flush=True)
    srv.serve_forever()


if __name__ == "__main__":
    main()
