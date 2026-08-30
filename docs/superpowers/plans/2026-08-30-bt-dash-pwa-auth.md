# bt-dash PWA + Session Auth Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Make the bt-dash PWA installable from the browser (native "Install" button + in-app button + assetlinks), and replace Apache HTTP Basic Auth with a one-time login + long-lived Flask session cookie served by bt-api.

**Architecture:** bt-api (Flask, entrypoint `server.py`, run by gunicorn under `systemctl --user bt-api@prod`) gains an `app/auth.py` module: a blueprint (`/auth/login|logout|status`) plus `init_auth(app)` which installs a signed-cookie session config and a global `before_request` guard that returns 401 for any path outside an allowlist when there is no session. The Quasar SPA gets a `/login` page and an axios 401 interceptor that redirects there. Apache stops doing auth and just reverse-proxies `/api/` and serves `/dash/` static. Deploy order guarantees `/api/` is never unprotected: backend guard goes live (still behind Basic Auth) first, Basic Auth is removed last.

**Tech Stack:** Flask 3.1, Werkzeug `pbkdf2:sha256` password hashing, Flask signed session cookie, pytest (new dev dep for bt-api), Quasar/Vue 3 (`@quasar/app-webpack` 4), workbox `GenerateSW`, Apache 2.4 reverse proxy.

**Spec:** `docs/superpowers/specs/2026-08-30-bt-dash-pwa-auth-design.md`

## Global Constraints

- Backend edits go in **`bt-api/server.py`** (gunicorn `server:app`, prod, proxied by Apache on `127.0.0.1:19090`). `bt-api/app.py` is a stale dev-only duplicate — **do not touch it**; dev instance is localhost-only and out of scope.
- Password hashing: **`generate_password_hash(pw, method='pbkdf2:sha256')`** / `check_password_hash` (avoid the scrypt default — platform OpenSSL dependency).
- Session cookie: `PERMANENT_SESSION_LIFETIME = timedelta(days=90)`, `SESSION_REFRESH_EACH_REQUEST = True`, `SESSION_COOKIE_SECURE = True`, `SESSION_COOKIE_HTTPONLY = True`, `SESSION_COOKIE_SAMESITE = 'Lax'`, `SESSION_COOKIE_PATH = '/'`.
- Guard allowlist (exact paths, no prefix match): `/auth/login`, `/auth/status`, `/github-webhook`. Plus: never block `OPTIONS`.
- `BT_DASH_SECRET_KEY` and `BT_DASH_PW_HASH` are **required** env vars — bt-api must fail to start if `BT_DASH_SECRET_KEY` is missing. They live in `env/server-prod` (git-ignored; loaded via `BT_SRV_ENV_FILE` by `bt-api/start.sh`). Never commit real values.
- Manifest: `id: '/dash/'`, `scope: '/dash/'`, `start_url: '/dash/'`; icons 192 and 512 each also offered with `"purpose": "maskable"`.
- bt-dash production build: `scripts/bt-dash-build-prod.sh` (runs `npx quasar build -m pwa`, needs node v20 via the nvm PATH the script sets). Output `bt-dash/dist/pwa/`, served by Apache `Alias /dash`.
- No lockout / rate-limiting, no CSRF tokens, no multi-user — explicitly out of scope per spec.
- Commits: bt-api changes commit in the `bt-api` submodule; bt-dash changes commit in the `bt-dash` submodule; the spec/plan and `env/*.example` commit in the workspace root repo. End commit messages with the `Co-Authored-By` / `Claude-Session` trailers used in this repo.

---

### Task 1: bt-api auth module (`app/auth.py`) + pytest harness

**Files:**
- Create: `bt-api/app/auth.py`
- Create: `bt-api/requirements-dev.txt`
- Create: `bt-api/tests/__init__.py` (empty)
- Create: `bt-api/tests/conftest.py`
- Create: `bt-api/tests/test_auth.py`

**Interfaces:**
- Produces:
  - `app.auth.auth_bp` — Flask `Blueprint` with routes `POST /auth/login`, `POST /auth/logout`, `GET /auth/status` (registered with **no** `url_prefix`).
  - `app.auth.init_auth(app: flask.Flask) -> None` — updates `app.config` with the session settings, registers `auth_bp`, installs a `before_request` guard. Reads `os.environ['BT_DASH_SECRET_KEY']` (raises `KeyError` if missing) and `os.environ['BT_DASH_PW_HASH']` at request time.
  - `python -m app.auth hash` — reads a password twice from a TTY, prints a `pbkdf2:sha256` hash to stdout.

- [ ] **Step 1: Add the pytest dev dependency and install it**

Create `bt-api/requirements-dev.txt`:

```
pytest>=8,<9
```

Run:

```bash
cd /home/htpc/backtrader/bt-api
.venv/bin/python -m pip install -r requirements-dev.txt
```

Expected: pytest installs into `bt-api/.venv`.

- [ ] **Step 2: Write the failing tests**

Create `bt-api/tests/__init__.py` (empty file).

Create `bt-api/tests/conftest.py`:

```python
import os

from werkzeug.security import generate_password_hash

# Must be set BEFORE app.auth is imported / init_auth is called.
os.environ.setdefault("BT_DASH_SECRET_KEY", "test-secret-key-not-for-prod")
os.environ.setdefault(
    "BT_DASH_PW_HASH", generate_password_hash("testpw", method="pbkdf2:sha256")
)

import pytest
from flask import Flask, jsonify

from app.auth import init_auth


@pytest.fixture
def app():
    app = Flask(__name__)
    app.config["TESTING"] = True
    init_auth(app)
    # test client speaks http:// -> don't let Secure suppress the cookie
    app.config["SESSION_COOKIE_SECURE"] = False

    @app.get("/dyn/protected")
    def _protected():
        return jsonify(ok=True)

    return app


@pytest.fixture
def client(app):
    return app.test_client()
```

Create `bt-api/tests/test_auth.py`:

```python
def test_status_unauthed(client):
    r = client.get("/auth/status")
    assert r.status_code == 200
    assert r.get_json() == {"authed": False}


def test_protected_requires_auth(client):
    r = client.get("/dyn/protected")
    assert r.status_code == 401
    assert r.get_json() == {"error": "auth required"}


def test_login_wrong_password(client):
    r = client.post("/auth/login", json={"password": "wrong"})
    assert r.status_code == 401
    assert r.get_json() == {"ok": False}


def test_login_missing_body(client):
    r = client.post("/auth/login")
    assert r.status_code == 401


def test_login_then_access_protected(client):
    r = client.post("/auth/login", json={"password": "testpw"})
    assert r.status_code == 200
    assert r.get_json() == {"ok": True}

    r = client.get("/dyn/protected")
    assert r.status_code == 200
    assert r.get_json() == {"ok": True}

    r = client.get("/auth/status")
    assert r.get_json() == {"authed": True}


def test_logout_clears_session(client):
    client.post("/auth/login", json={"password": "testpw"})
    r = client.post("/auth/logout")
    assert r.status_code == 204

    r = client.get("/dyn/protected")
    assert r.status_code == 401


def test_options_not_blocked(client):
    r = client.open("/dyn/protected", method="OPTIONS")
    assert r.status_code != 401


def test_allowlisted_path_not_forced_to_401(client):
    # /github-webhook is not registered on this bare test app, so it 404s;
    # the guard must NOT turn it into a 401.
    r = client.post("/github-webhook")
    assert r.status_code != 401
```

- [ ] **Step 3: Run the tests to verify they fail**

Run:

```bash
cd /home/htpc/backtrader/bt-api
.venv/bin/python -m pytest tests/test_auth.py -v
```

Expected: collection error / `ModuleNotFoundError: No module named 'app.auth'`.

- [ ] **Step 4: Implement `app/auth.py`**

Create `bt-api/app/auth.py`:

```python
import os
from datetime import timedelta

from flask import Blueprint, jsonify, request, session
from werkzeug.security import check_password_hash, generate_password_hash

auth_bp = Blueprint("auth", __name__)

# Exact paths that bypass the auth guard.
_PUBLIC_PATHS = {"/auth/login", "/auth/status", "/github-webhook"}


@auth_bp.post("/auth/login")
def login():
    data = request.get_json(silent=True) or {}
    password = data.get("password", "")
    pw_hash = os.environ["BT_DASH_PW_HASH"]
    if not password or not check_password_hash(pw_hash, password):
        return jsonify(ok=False), 401
    session.permanent = True
    session["authed"] = True
    return jsonify(ok=True)


@auth_bp.post("/auth/logout")
def logout():
    session.clear()
    return "", 204


@auth_bp.get("/auth/status")
def status():
    return jsonify(authed=bool(session.get("authed")))


def init_auth(app):
    """Install session config, register the auth blueprint, add the guard."""
    app.config.update(
        SECRET_KEY=os.environ["BT_DASH_SECRET_KEY"],  # required: KeyError if unset
        PERMANENT_SESSION_LIFETIME=timedelta(days=90),
        SESSION_REFRESH_EACH_REQUEST=True,
        SESSION_COOKIE_SECURE=True,
        SESSION_COOKIE_HTTPONLY=True,
        SESSION_COOKIE_SAMESITE="Lax",
        SESSION_COOKIE_PATH="/",
    )
    app.register_blueprint(auth_bp)

    @app.before_request
    def _require_auth():
        if request.method == "OPTIONS":
            return None
        if request.path in _PUBLIC_PATHS:
            return None
        if not session.get("authed"):
            return jsonify(error="auth required"), 401
        return None


def _main():
    import getpass
    import sys

    if len(sys.argv) != 2 or sys.argv[1] != "hash":
        print("usage: python -m app.auth hash", file=sys.stderr)
        sys.exit(2)
    pw1 = getpass.getpass("Password: ")
    pw2 = getpass.getpass("Confirm : ")
    if pw1 != pw2:
        print("passwords do not match", file=sys.stderr)
        sys.exit(1)
    print(generate_password_hash(pw1, method="pbkdf2:sha256"))


if __name__ == "__main__":
    _main()
```

- [ ] **Step 5: Run the tests to verify they pass**

Run:

```bash
cd /home/htpc/backtrader/bt-api
.venv/bin/python -m pytest tests/test_auth.py -v
```

Expected: all 8 tests PASS.

- [ ] **Step 6: Smoke-test the password helper**

Run:

```bash
cd /home/htpc/backtrader/bt-api
printf 'hunter2\nhunter2\n' | .venv/bin/python -m app.auth hash
```

Expected: one line starting `pbkdf2:sha256:` printed to stdout. (If `python -m app.auth` fails because `app/__init__.py` pulls heavy imports, fall back to:
`.venv/bin/python -c "import getpass;from werkzeug.security import generate_password_hash as g;print(g(getpass.getpass(),method='pbkdf2:sha256'))"`.)

- [ ] **Step 7: Commit (in the bt-api submodule)**

```bash
cd /home/htpc/backtrader/bt-api
git add app/auth.py requirements-dev.txt tests/
git commit -m "$(cat <<'EOF'
feat(auth): session-cookie auth module + guard (app/auth.py)

Blueprint /auth/login|logout|status + init_auth(app) installing a
90-day rolling signed-cookie session and a before_request guard that
401s any non-allowlisted path without a session. pytest harness added
as a dev dep. Not wired into server.py yet.

Co-Authored-By: Claude Sonnet 5 <noreply@anthropic.com>
Claude-Session: https://claude.ai/code/session_01EgXuopiM8anWBQWHsCS9aT
EOF
)"
```

---

### Task 2: Wire `init_auth` into `server.py`

**Files:**
- Modify: `bt-api/server.py` (add import near the other `from app.* import ...` lines; add `init_auth(app)` immediately after the last `app.register_blueprint(obs_bp, ...)` call, before `@app.route('/')`)
- Create: `bt-api/tests/test_server_integration.py`

**Interfaces:**
- Consumes: `app.auth.init_auth` (Task 1).
- Produces: `server.app` is a fully-wired Flask app where every route except the allowlist requires a session.

- [ ] **Step 1: Write the failing integration test**

Create `bt-api/tests/test_server_integration.py`:

```python
import pytest

# conftest.py has already set BT_DASH_SECRET_KEY / BT_DASH_PW_HASH.
try:
    import server
except Exception as exc:  # pragma: no cover - env/data not present on this box
    pytest.skip(f"cannot import server: {exc}", allow_module_level=True)


@pytest.fixture
def client():
    server.app.config["TESTING"] = True
    server.app.config["SESSION_COOKIE_SECURE"] = False
    return server.app.test_client()


def test_dyn_route_blocked_without_session(client):
    r = client.get("/dyn/sc/index")
    assert r.status_code == 401
    assert r.get_json() == {"error": "auth required"}


def test_dyn_route_reachable_after_login(client):
    r = client.post("/auth/login", json={"password": "testpw"})
    assert r.status_code == 200
    r = client.get("/dyn/sc/index")
    assert r.status_code != 401


def test_github_webhook_not_blocked_by_guard(client):
    # No signature -> handler itself rejects (403) or 503 if unconfigured,
    # but NOT 401 from the guard.
    r = client.post("/github-webhook", data=b"{}")
    assert r.status_code != 401
```

- [ ] **Step 2: Run it to verify it fails**

Run:

```bash
cd /home/htpc/backtrader/bt-api
.venv/bin/python -m pytest tests/test_server_integration.py -v
```

Expected: `test_dyn_route_blocked_without_session` FAILS (gets 200, not 401) because the guard isn't wired in yet. (If the module is skipped instead, run pytest on the host where bt-api normally runs.)

- [ ] **Step 3: Wire it into `server.py`**

In `bt-api/server.py`, add to the import block (next to `from app.watchtower import obs_bp`):

```python
from app.auth import init_auth
```

Immediately after this existing line:

```python
# Watchtower / watchdog
app.register_blueprint(obs_bp, url_prefix=f'{url_prefix}/obs')
```

add:

```python
# Session auth: config + /auth/* blueprint + before_request guard.
# Replaces the Apache Basic Auth that used to gate /api and /dash.
init_auth(app)
```

- [ ] **Step 4: Run the tests to verify they pass**

Run:

```bash
cd /home/htpc/backtrader/bt-api
.venv/bin/python -m pytest tests/ -v
```

Expected: all tests from Task 1 and Task 2 PASS (or `test_server_integration.py` skipped on a box without bt-api's data — Task 1 tests must still pass).

- [ ] **Step 5: Commit (in the bt-api submodule)**

```bash
cd /home/htpc/backtrader/bt-api
git add server.py tests/test_server_integration.py
git commit -m "$(cat <<'EOF'
feat(auth): enforce session auth in server.py via init_auth(app)

Every /api route except /auth/login, /auth/status and /github-webhook
now returns 401 without a session cookie.

Co-Authored-By: Claude Sonnet 5 <noreply@anthropic.com>
Claude-Session: https://claude.ai/code/session_01EgXuopiM8anWBQWHsCS9aT
EOF
)"
```

---

### Task 3: Provision prod secrets and restart bt-api (Basic Auth still in front)

**Files:**
- Modify (git-ignored, edit on disk only): `/home/htpc/backtrader/env/server-prod`
- Modify: `/home/htpc/backtrader/env/server-prod.example` (commented placeholders only)

**Interfaces:**
- Consumes: `python -m app.auth hash` (Task 1), `init_auth` reading `BT_DASH_SECRET_KEY` / `BT_DASH_PW_HASH` (Task 2).
- Produces: a running prod bt-api that enforces the session guard. Verified by curl through the still-present Apache Basic Auth.

- [ ] **Step 1: Generate the secret key**

Run:

```bash
/home/htpc/backtrader/bt-api/.venv/bin/python -c "import secrets; print(secrets.token_hex(32))"
```

Copy the 64-char hex output.

- [ ] **Step 2: Generate the password hash**

Run (replace the two lines with your chosen password):

```bash
cd /home/htpc/backtrader/bt-api
printf 'YOUR-PASSWORD\nYOUR-PASSWORD\n' | .venv/bin/python -m app.auth hash
```

Copy the `pbkdf2:sha256:...` output.

- [ ] **Step 3: Append both to `env/server-prod`**

Append to `/home/htpc/backtrader/env/server-prod` (use the Edit tool or `>>`; do not disturb existing lines):

```sh

# bt-dash session auth (added 2026-08-30). Keep stable: changing the key
# invalidates all sessions and forces re-login.
BT_DASH_SECRET_KEY=<hex from Step 1>
BT_DASH_PW_HASH=<pbkdf2 hash from Step 2>
```

- [ ] **Step 4: Document the vars in the tracked example file**

Edit `/home/htpc/backtrader/env/server-prod.example`, appending:

```sh

# bt-dash session auth. Generate with:
#   python -c "import secrets; print(secrets.token_hex(32))"
#   python -m app.auth hash   # (from bt-api/, prints a pbkdf2:sha256 hash)
# BT_DASH_SECRET_KEY=
# BT_DASH_PW_HASH=
```

- [ ] **Step 5: Restart prod bt-api and confirm it came up**

Run:

```bash
systemctl --user restart bt-api@prod.service
sleep 2
systemctl --user is-active bt-api@prod.service
journalctl --user -u bt-api@prod.service -n 20 --no-pager
```

Expected: `active`. If it is `failed` with `KeyError: 'BT_DASH_SECRET_KEY'`, the env var did not reach the process — check that `env/server-prod` is the file `BT_SRV_ENV_FILE` points to in `env/bt-api-prod`.

- [ ] **Step 6: Verify the guard through Apache Basic Auth**

Basic Auth is still active, so pass `-u USER:PASS` for the htpasswd credentials in every call. `-c`/`-b` keep a cookie jar.

```bash
BASE=https://ilz.duckdns.org:1443
J=/tmp/btdash.cookies

# no session yet -> guard 401 (JSON, not the Basic Auth challenge)
curl -sk -u USER:PASS "$BASE/api/auth/status"
# expect: {"authed": false}

curl -sk -u USER:PASS "$BASE/api/dyn/sc/index" -o /dev/null -w '%{http_code}\n'
# expect: 401

# log in -> Set-Cookie: session=...
curl -sk -u USER:PASS -c "$J" -X POST "$BASE/api/auth/login" \
  -H 'Content-Type: application/json' -d '{"password":"YOUR-PASSWORD"}' \
  -w '\n%{http_code}\n'
# expect: {"ok": true} then 200

# with the session cookie -> protected route no longer 401
curl -sk -u USER:PASS -b "$J" "$BASE/api/dyn/sc/index" -o /dev/null -w '%{http_code}\n'
# expect: 200 (or whatever that endpoint normally returns; NOT 401)
```

If all three expectations hold, the backend is ready for the Apache cutover.

- [ ] **Step 7: Commit the example file (workspace root repo)**

```bash
cd /home/htpc/backtrader
git add env/server-prod.example
git commit -m "$(cat <<'EOF'
chore(env): document BT_DASH_SECRET_KEY / BT_DASH_PW_HASH for bt-dash auth

Real values live in env/server-prod (git-ignored).

Co-Authored-By: Claude Sonnet 5 <noreply@anthropic.com>
Claude-Session: https://claude.ai/code/session_01EgXuopiM8anWBQWHsCS9aT
EOF
)"
```

---

### Task 4: bt-dash — installable manifest, in-app Install button, assetlinks

**Files:**
- Modify: `bt-dash/quasar.conf.js` (the `pwa.manifest` object, ~lines 156-190)
- Create: `bt-dash/src/components/InstallPwaButton.vue`
- Modify: `bt-dash/src/layouts/MainLayout.vue` (toolbar `<div class="q-gutter-sm row items-center no-wrap">`, ~line 20)
- Create: `bt-dash/public/.well-known/assetlinks.json`

**Interfaces:**
- Produces: `dist/pwa/manifest.json` with `id`/`scope`/`purpose`; a mounted `<InstallPwaButton>`; `dist/pwa/.well-known/assetlinks.json` reachable at `/dash/.well-known/assetlinks.json` (root alias added in Task 6).

- [ ] **Step 1: Update the manifest in `quasar.conf.js`**

In the `pwa.manifest` object: add `id` and `scope` keys next to `name`, and add maskable variants for the 192 and 512 icons. Result:

```js
      manifest: {
        id: '/dash/',
        scope: '/dash/',
        name: `Backtrader Watchtower`,
        short_name: `Watchtower`,
        description: `Monitoraggio strategie di trading Backtrader/Alpaca`,
        display: 'standalone',
        orientation: 'portrait',
        background_color: '#ffffff',
        theme_color: '#027be3',
        icons: [
          { src: 'icons/icon-128x128.png', sizes: '128x128', type: 'image/png' },
          { src: 'icons/icon-192x192.png', sizes: '192x192', type: 'image/png' },
          { src: 'icons/icon-192x192.png', sizes: '192x192', type: 'image/png', purpose: 'maskable' },
          { src: 'icons/icon-256x256.png', sizes: '256x256', type: 'image/png' },
          { src: 'icons/icon-384x384.png', sizes: '384x384', type: 'image/png' },
          { src: 'icons/icon-512x512.png', sizes: '512x512', type: 'image/png' },
          { src: 'icons/icon-512x512.png', sizes: '512x512', type: 'image/png', purpose: 'maskable' }
        ]
      }
```

- [ ] **Step 2: Create the Install button component**

Create `bt-dash/src/components/InstallPwaButton.vue`:

```vue
<template>
  <q-btn
    v-if="canInstall"
    dense
    flat
    color="white"
    icon="install_mobile"
    label="Installa app"
    @click="install"
  />
  <q-btn
    v-else-if="showIosHint"
    dense
    flat
    round
    color="white"
    icon="ios_share"
    @click="iosDialog = true"
  >
    <q-dialog v-model="iosDialog">
      <q-card>
        <q-card-section class="text-subtitle1">Installa su iPhone/iPad</q-card-section>
        <q-card-section>
          Apri il menu <b>Condividi</b> di Safari e scegli
          <b>«Aggiungi a Home»</b>.
        </q-card-section>
        <q-card-actions align="right">
          <q-btn flat label="OK" v-close-popup />
        </q-card-actions>
      </q-card>
    </q-dialog>
  </q-btn>
</template>

<script>
export default {
  name: 'InstallPwaButton',
  data () {
    return {
      deferredPrompt: null,
      canInstall: false,
      iosDialog: false
    }
  },
  computed: {
    isStandalone () {
      return window.matchMedia('(display-mode: standalone)').matches ||
        window.navigator.standalone === true
    },
    isIos () {
      return /iphone|ipad|ipod/i.test(window.navigator.userAgent)
    },
    showIosHint () {
      return this.isIos && !this.isStandalone
    }
  },
  mounted () {
    if (this.isStandalone) return
    window.addEventListener('beforeinstallprompt', this.onPrompt)
    window.addEventListener('appinstalled', this.onInstalled)
  },
  beforeUnmount () {
    window.removeEventListener('beforeinstallprompt', this.onPrompt)
    window.removeEventListener('appinstalled', this.onInstalled)
  },
  methods: {
    onPrompt (e) {
      e.preventDefault()
      this.deferredPrompt = e
      this.canInstall = true
    },
    onInstalled () {
      this.canInstall = false
      this.deferredPrompt = null
    },
    async install () {
      if (!this.deferredPrompt) return
      this.deferredPrompt.prompt()
      await this.deferredPrompt.userChoice
      this.deferredPrompt = null
      this.canInstall = false
    }
  }
}
</script>
```

- [ ] **Step 3: Mount it in the layout toolbar**

In `bt-dash/src/layouts/MainLayout.vue`, inside `<div class="q-gutter-sm row items-center no-wrap">`, add as the first child:

```vue
          <install-pwa-button />
```

And register the component in that file's `<script>` (`export default { ... }`):

```js
import InstallPwaButton from 'src/components/InstallPwaButton.vue'
// ...
  components: { InstallPwaButton },
```

(If the file already has a `components:` key, add `InstallPwaButton` to it rather than duplicating the key.)

- [ ] **Step 4: Create the assetlinks file**

Create `bt-dash/public/.well-known/assetlinks.json`:

```json
[
  {
    "relation": ["delegate_permission/common.handle_all_urls"],
    "target": {
      "namespace": "android_app",
      "package_name": "REPLACE_WITH_ANDROID_PACKAGE_NAME",
      "sha256_cert_fingerprints": ["REPLACE_WITH_SIGNING_CERT_SHA256"]
    }
  }
]
```

- [ ] **Step 5: Build and verify the output**

Run:

```bash
/home/htpc/backtrader/scripts/bt-dash-build-prod.sh
```

Then check:

```bash
cd /home/htpc/backtrader/bt-dash
grep -o '"purpose":"maskable"' dist/pwa/manifest.json | head -1        # expect a match
grep -o '"id":"/dash/"' dist/pwa/manifest.json                          # expect a match
test -f dist/pwa/.well-known/assetlinks.json && echo assetlinks-ok      # expect: assetlinks-ok
```

Expected: build completes, all three checks succeed.

- [ ] **Step 6: Commit (in the bt-dash submodule)**

```bash
cd /home/htpc/backtrader/bt-dash
git add quasar.conf.js src/components/InstallPwaButton.vue src/layouts/MainLayout.vue public/.well-known/assetlinks.json
git commit -m "$(cat <<'EOF'
feat(pwa): installable manifest + in-app Install button + assetlinks

manifest id/scope + maskable 192/512 icons; InstallPwaButton
(beforeinstallprompt, iOS Add-to-Home hint) in the toolbar;
public/.well-known/assetlinks.json placeholder for a future TWA.

Co-Authored-By: Claude Sonnet 5 <noreply@anthropic.com>
Claude-Session: https://claude.ai/code/session_01EgXuopiM8anWBQWHsCS9aT
EOF
)"
```

---

### Task 5: bt-dash — login page + route + axios 401 interceptor

**Files:**
- Create: `bt-dash/src/bt/pages/LoginPage.vue`
- Modify: `bt-dash/src/router/routes.js` (add a top-level route, sibling of the `MainLayout` route)
- Modify: `bt-dash/src/boot/axios.js` (add a response interceptor; use the `router` arg of `boot()`)

**Interfaces:**
- Consumes: `api` axios instance from `src/boot/axios.js`; backend `POST /auth/login`, `GET /auth/status` (Task 2).
- Produces: route `/login`; any `401` from an `/api/*` call (except `/auth/*`) redirects the SPA to `/login?redirect=<path>`.

- [ ] **Step 1: Create the login page**

Create `bt-dash/src/bt/pages/LoginPage.vue`:

```vue
<template>
  <q-page class="flex flex-center bg-primary">
    <q-card style="width: 320px; max-width: 90vw">
      <q-card-section class="text-h6 text-center">Backtrader Watchtower</q-card-section>
      <q-form @submit="submit">
        <q-card-section class="q-gutter-md">
          <input
            type="text"
            name="username"
            autocomplete="username"
            value="watchtower"
            hidden
          />
          <q-input
            v-model="password"
            type="password"
            label="Password"
            autocomplete="current-password"
            autofocus
            :error="!!error"
            :error-message="error"
          />
        </q-card-section>
        <q-card-actions>
          <q-btn
            type="submit"
            color="primary"
            class="full-width"
            label="Entra"
            :loading="loading"
          />
        </q-card-actions>
      </q-form>
    </q-card>
  </q-page>
</template>

<script>
import { api } from 'boot/axios'

export default {
  name: 'LoginPage',
  data () {
    return { password: '', error: '', loading: false }
  },
  methods: {
    async submit () {
      this.loading = true
      this.error = ''
      try {
        await api.post('/auth/login', { password: this.password })
        const redirect = this.$route.query.redirect || '/'
        this.$router.replace(redirect)
      } catch (e) {
        this.error = e.response && e.response.status === 401
          ? 'Password errata'
          : 'Errore di connessione'
      } finally {
        this.loading = false
      }
    }
  }
}
</script>
```

(The hidden username input is there only so mobile password managers offer to save/fill the entry and expose biometric autofill.)

- [ ] **Step 2: Add the `/login` route**

In `bt-dash/src/router/routes.js`, add this object **before** the `path: '/details/:ticker'` route (so it is a sibling of the `MainLayout` route, not a child of it):

```js
  {
    path: '/login',
    component: () => import('src/bt/pages/LoginPage.vue')
  },
```

- [ ] **Step 3: Add the axios 401 interceptor**

In `bt-dash/src/boot/axios.js`, change the boot callback signature to receive `router` and register an interceptor. Replace:

```js
export default boot(({ app }) => {
```

with:

```js
export default boot(({ app, router }) => {
  api.interceptors.response.use(
    (response) => response,
    (error) => {
      const url = (error.config && error.config.url) || ''
      if (error.response && error.response.status === 401 && !url.includes('/auth/')) {
        const current = router.currentRoute.value
        if (current.path !== '/login') {
          router.replace({ path: '/login', query: { redirect: current.fullPath } })
        }
      }
      return Promise.reject(error)
    }
  )
```

(Keep the existing body of the callback — the `app.config.globalProperties` assignments — after this block, and keep the closing `})`.)

- [ ] **Step 4: Build to verify it compiles**

Run:

```bash
/home/htpc/backtrader/scripts/bt-dash-build-prod.sh
```

Expected: build completes with no errors; `dist/pwa/` is regenerated. (Functional check happens end-to-end in Task 7.)

- [ ] **Step 5: Commit (in the bt-dash submodule)**

```bash
cd /home/htpc/backtrader/bt-dash
git add src/bt/pages/LoginPage.vue src/router/routes.js src/boot/axios.js
git commit -m "$(cat <<'EOF'
feat(auth): SPA login page + axios 401 -> /login redirect

/login route (outside MainLayout); response interceptor sends any
401 from a non-/auth API call to /login?redirect=<path>.

Co-Authored-By: Claude Sonnet 5 <noreply@anthropic.com>
Claude-Session: https://claude.ai/code/session_01EgXuopiM8anWBQWHsCS9aT
EOF
)"
```

---

### Task 6: Apache cutover — remove Basic Auth, add assetlinks alias

**Files:**
- Modify (root-owned, `sudo` required): `/etc/apache2/sites-available/bt-dash.conf`

**Interfaces:**
- Consumes: backend guard live (Task 3 verified); frontend built with `/login` + interceptor (Task 5).
- Produces: `/api/*` protected only by the Flask session; `/dash/` static public; `https://ilz.duckdns.org:1443/.well-known/assetlinks.json` served.

- [ ] **Step 1: Back up the current config**

```bash
sudo cp -a /etc/apache2/sites-available/bt-dash.conf \
           /etc/apache2/sites-available/bt-dash.conf.pre-auth-$(date +%F)
```

- [ ] **Step 2: Edit the vhost**

In `/etc/apache2/sites-available/bt-dash.conf`:

**Remove** this block entirely:

```apache
    <Location />
        AuthType Basic
        AuthName "Backtrader"
        AuthUserFile /etc/apache2/backtrader.htpasswd
        Require valid-user
    </Location>
```

**Add**, just before the `ErrorLog` line:

```apache
    # Digital Asset Links for the installable PWA / future Android TWA.
    Alias "/.well-known/assetlinks.json" "/home/htpc/backtrader/bt-dash/dist/pwa/.well-known/assetlinks.json"
    <Files "assetlinks.json">
        Require all granted
    </Files>
```

Leave everything else (`ProxyPass /api/`, `Alias /dash`, the `<Directory>` grant, the `sw.js` no-cache `FilesMatch`, `RedirectMatch`) unchanged.

- [ ] **Step 3: Syntax-check and reload**

```bash
sudo apache2ctl configtest && sudo systemctl reload apache2
```

Expected: `Syntax OK`, reload with no error.

- [ ] **Step 4: Verify the cutover**

```bash
BASE=https://ilz.duckdns.org:1443
J=/tmp/btdash.cookies; rm -f "$J"

# no Basic Auth prompt anymore; guard answers with JSON 401
curl -sk "$BASE/api/dyn/sc/index" -o /dev/null -w '%{http_code}\n'      # expect 401
curl -sk "$BASE/api/auth/status"                                        # expect {"authed": false}

# static shell is public
curl -sk "$BASE/dash/" -o /dev/null -w '%{http_code}\n'                 # expect 200

# assetlinks at domain root
curl -sk "$BASE/.well-known/assetlinks.json" -o /dev/null -w '%{http_code}\n'   # expect 200

# full login round-trip, no -u
curl -sk -c "$J" -X POST "$BASE/api/auth/login" -H 'Content-Type: application/json' \
  -d '{"password":"YOUR-PASSWORD"}' -w '\n%{http_code}\n'               # expect {"ok": true} 200
curl -sk -b "$J" "$BASE/api/dyn/sc/index" -o /dev/null -w '%{http_code}\n'  # expect not 401
```

Expected: every line matches. If `/api/...` still shows a Basic Auth challenge (`401` with `WWW-Authenticate: Basic`), the `<Location />` block was not fully removed or another vhost/`.htaccess` still enforces it.

- [ ] **Step 5: Record the cutover (workspace root repo)**

Append a dated note to the spec's Apache section and commit (no system file goes in git):

```bash
cd /home/htpc/backtrader
# edit docs/superpowers/specs/2026-08-30-bt-dash-pwa-auth-design.md:
#   under "5. Apache", add: "DONE <date>: Basic Auth removed, assetlinks Alias added,
#   backup at bt-dash.conf.pre-auth-<date>. /etc/apache2/backtrader.htpasswd now dead."
git add docs/superpowers/specs/2026-08-30-bt-dash-pwa-auth-design.md
git commit -m "$(cat <<'EOF'
docs(bt-dash): record Apache Basic Auth removal / assetlinks alias

Co-Authored-By: Claude Sonnet 5 <noreply@anthropic.com>
Claude-Session: https://claude.ai/code/session_01EgXuopiM8anWBQWHsCS9aT
EOF
)"
```

---

### Task 7: End-to-end acceptance

**Files:** none (verification only). Re-run `scripts/bt-dash-build-prod.sh` only if Task 5's build was not the last one deployed.

**Interfaces:**
- Consumes: everything from Tasks 1-6.

- [ ] **Step 1: Fresh-session login flow**

In a private/incognito browser window, open `https://ilz.duckdns.org:1443/dash/`.
Expected: redirected to `/dash/#/login`. Enter the password → land on the dashboard, data loads. No Basic Auth dialog at any point.

- [ ] **Step 2: Session persistence**

Close the browser entirely, reopen, go to `https://ilz.duckdns.org:1443/dash/`.
Expected: dashboard loads directly, no login prompt. (`GET /api/auth/status` → `{"authed": true}` in DevTools → Network.)

- [ ] **Step 3: 401 handling**

In DevTools → Application → Cookies, delete the `session` cookie. Trigger any data reload in the app.
Expected: app redirects to `/login`.

- [ ] **Step 4: Installability**

DevTools → Application → Manifest: no errors, "Installable" (or the "Add to home screen" link works). The **Install app** button is visible in the toolbar on desktop Chrome/Edge; clicking it shows the native install dialog. On an Android phone, Chrome offers to install; the installed app opens standalone (no URL bar).

- [ ] **Step 5: iOS check (if an iPhone/iPad is available)**

Open the URL in Safari, log in, tap the share button in the toolbar → the dialog explains "Aggiungi a Home". After adding, the icon opens the app standalone and the session is still valid.

- [ ] **Step 6: Final report**

Confirm all six steps. Note any deviation (e.g. maskable icon cropping, install button not appearing on a given browser) as a follow-up — none should block acceptance.

---

## Self-Review

**Spec coverage:**

| Spec section | Task |
|---|---|
| §3 A1 manifest id/scope/purpose | Task 4 Step 1 |
| §3 A2 InstallPwaButton + iOS hint | Task 4 Steps 2-3 |
| §3 A3 assetlinks.json | Task 4 Step 4; served at root by Task 6 Step 2 |
| §3 A4 rebuild | Task 4 Step 5 / Task 5 Step 4 |
| §4 B1 auth blueprint `/auth/login|logout|status` | Task 1 Step 4 |
| §4 B2 before_request guard + allowlist | Task 1 Step 4 (`init_auth`), wired Task 2 |
| §4 B3 session config | Task 1 Step 4 (`init_auth`) |
| §4 B4 secrets in env/server-prod | Task 3 Steps 1-4 |
| §4 B5 password helper | Task 1 Step 4 (`_main`), smoke-tested Step 6 |
| §4 B6 LoginPage + route + interceptor | Task 5 |
| §4 B7 CORS unchanged | no task needed (explicitly "no change") |
| §5 Apache remove Basic Auth + assetlinks alias | Task 6 |
| §6 deploy order | Task ordering 1→2→3 (backend live behind Basic Auth) → 4→5 (frontend) → 6 (remove Basic Auth) → 7 |
| §7 backend tests | Task 1 Step 2, Task 2 Step 1 |

No gaps.

**Placeholder scan:** `assetlinks.json` contains `REPLACE_WITH_*` tokens — intentional and documented (the APK does not exist yet; the file is inert until a TWA is generated). `YOUR-PASSWORD` / `USER:PASS` in curl commands are operator inputs, not plan placeholders. No `TBD`/`TODO`/"add error handling"-style gaps.

**Type consistency:** `init_auth(app)` — defined Task 1, called Task 2. `auth_bp` name consistent. Session key `session['authed']` consistent across `auth.py`, tests, and status endpoint. Login response bodies `{"ok": true/false}` consistent between `auth.py` and Task 1/Task 3 tests. Guard response `{"error": "auth required"}` consistent between `auth.py` and Task 1/Task 2 tests. `_PUBLIC_PATHS` matches the spec allowlist exactly.
