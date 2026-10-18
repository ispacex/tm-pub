"""Sign-in: a password (PBKDF2), Telegram Login Widget, a signed session cookie, API tokens."""
import base64
import hashlib
import hmac
import json
import os
import time

SESSION_TTL = 14 * 24 * 3600
TELEGRAM_MAX_AGE = 24 * 3600
PBKDF2_ITER = 600_000


def hash_password(password, salt=None, iterations=PBKDF2_ITER):
    salt = salt or os.urandom(16)
    dk = hashlib.pbkdf2_hmac("sha256", password.encode(), salt, iterations)
    # No "$" in it: docker compose would read "$..." in an env file as a variable.
    return "pbkdf2_sha256:%d:%s:%s" % (iterations, base64.b64encode(salt).decode(), base64.b64encode(dk).decode())


def check_password(password, stored):
    try:
        algo, iterations, salt, dk = stored.replace("$", ":").split(":")
        if algo != "pbkdf2_sha256":
            return False
        got = hashlib.pbkdf2_hmac("sha256", password.encode(), base64.b64decode(salt), int(iterations))
        return hmac.compare_digest(got, base64.b64decode(dk))
    except (ValueError, TypeError):
        return False


def token_hash(token):
    return hashlib.sha256(token.encode()).hexdigest()


def check_token(token, hashes):
    h = token_hash(token)
    return any(hmac.compare_digest(h, x) for x in hashes)


def _b64(b):
    return base64.urlsafe_b64encode(b).rstrip(b"=").decode()


def _unb64(s):
    return base64.urlsafe_b64decode(s + "=" * (-len(s) % 4))


def make_session(secret, who, now=None):
    body = _b64(json.dumps({"who": who, "exp": int((now or time.time()) + SESSION_TTL)}).encode())
    sig = _b64(hmac.new(secret, body.encode(), hashlib.sha256).digest())
    return body + "." + sig


def read_session(secret, value, now=None):
    """The signed-in name, or None."""
    if not secret or not value or "." not in value:
        return None
    body, sig = value.rsplit(".", 1)
    want = _b64(hmac.new(secret, body.encode(), hashlib.sha256).digest())
    if not hmac.compare_digest(sig, want):
        return None
    try:
        data = json.loads(_unb64(body))
    except ValueError:
        return None
    if data.get("exp", 0) < (now or time.time()):
        return None
    return data.get("who")


def check_telegram(fields, bot_token, allowed_ids, now=None):
    """Telegram Login Widget data -> (user id, None) or (None, reason).

    https://core.telegram.org/widgets/login#checking-authorization: the data-check-string is every
    received field but hash, sorted, as key=value joined by \\n; the key is SHA256(bot token).
    """
    if not bot_token:
        return None, "telegram sign-in is not configured"
    fields = {k: v for k, v in fields.items() if v is not None}
    got = fields.pop("hash", None)
    if not got:
        return None, "no hash"
    check = "\n".join("%s=%s" % (k, fields[k]) for k in sorted(fields))
    key = hashlib.sha256(bot_token.encode()).digest()
    want = hmac.new(key, check.encode(), hashlib.sha256).hexdigest()
    if not hmac.compare_digest(want, got):
        return None, "bad signature"
    try:
        auth_date = int(fields.get("auth_date", "0"))
        uid = int(fields.get("id", ""))
    except ValueError:
        return None, "bad fields"
    if (now or time.time()) - auth_date > TELEGRAM_MAX_AGE:
        return None, "sign-in is older than a day"
    if uid not in allowed_ids:
        return None, "this Telegram account is not on the list"
    return uid, None
