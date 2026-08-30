# bt-dash — PWA installabile + auth a sessione (rimpiazzo Basic Auth)

**Data:** 2026-08-30
**Repo toccati:** `bt-dash`, `bt-api`, config Apache (`/etc/apache2/sites-available/bt-dash.conf`), `env/` (workspace)
**Stato:** design approvato in chat, pronto per il piano di implementazione

---

## 1. Obiettivo

1. **Parte A — PWA installabile:** far comparire il pulsante nativo "Installa" del
   browser per `https://ilz.duckdns.org:1443/dash/`, aggiungere un pulsante
   "Installa app" in-app, e predisporre `assetlinks.json` per una futura
   TWA/APK.
2. **Parte B — auth:** sostituire la HTTP Basic Auth di Apache (che rompe
   cookie/sessione e obbliga a rifare login) con un login **una tantum** +
   cookie di sessione a lunga durata gestito da `bt-api`. L'autofill
   biometrico ("impronta") arriva dal gestore password del dispositivo.

**Non obiettivi:** WebAuthn/passkey vere, multi-utente, lockout/rate-limit,
token CSRF, generazione effettiva dell'APK.

---

## 2. Stato attuale (rilevato)

- `bt-dash`: app Quasar (Vue 3, `@quasar/app-webpack` 4.4.5), router in **hash
  mode**, build `quasar build -m pwa` → `dist/pwa/`, servita **staticamente**
  da Apache come `Alias /dash`. Service worker workbox `GenerateSW` (`sw.js`)
  già prodotto dalla build.
- Manifest PWA generato: `display: standalone`, `start_url: /dash/`,
  icone 128–512. **Mancano:** `purpose: "maskable"`, `id`/`scope` espliciti.
- Nessun handler `beforeinstallprompt` nella SPA.
- `bt-api`: app **Flask**. Entrypoint reale = **`server.py`** (gunicorn
  `server:app`); `app.py` è un **duplicato stantìo** usato solo dal
  fallback `python app.py` — **tutte le modifiche vanno in `server.py`**
  (valutare a parte se allineare o cancellare `app.py`, fuori scope).
  gunicorn `workers=2`, in ascolto su `127.0.0.1:19090`, proxied da Apache
  su `/api/`. Blueprint sotto `/dyn/*` e `/fs/*`. Endpoint speciale
  `POST /github-webhook` (firma HMAC; `503` se `GITHUB_WEBHOOK_SECRET`
  non impostato). Servizio lanciato via **systemd `--user`**.
  **Nessuna autenticazione propria.**
- Apache vhost `:1443`: blocco `<Location />` con `AuthType Basic` +
  `Require valid-user` (file `/etc/apache2/backtrader.htpasswd`) — copre
  **sia** `/dash/` **sia** `/api/`. È l'unica protezione esistente.
- SPA → API: `axios` con `baseURL = constants.API_BASE_URL = /api` →
  **stessa origine**, i cookie viaggiano automaticamente senza `withCredentials`.
- `bt-api/start.sh` fa `source` di `env/pa2`, `env/bt-live-events`,
  `env/server`. `env/*` è git-ignored → posto adatto ai segreti.

---

## 3. Parte A — PWA installabile

### A1. Manifest (`bt-dash/quasar.conf.js` → `pwa.manifest`)

- Aggiungere `id: '/dash/'` e `scope: '/dash/'` espliciti (`start_url`
  resta `/dash/`, già corretto via `publicPath`).
- Alle icone **192x192** e **512x512** aggiungere una variante
  `"purpose": "maskable"` (le sorgenti PNG esistenti vanno bene; se il
  soggetto viene tagliato dalla safe-zone si rigenerano più avanti, fuori
  scope).

Criterio di verifica: DevTools → Application → Manifest = "Installable",
`dist/pwa/manifest.json` contiene `purpose`.

### A2. Pulsante "Installa app" in-app

Nuovo file `bt-dash/src/components/InstallPwaButton.vue`:

- `window.addEventListener('beforeinstallprompt', e => { e.preventDefault();
  deferredPrompt = e; visible = true })`.
- `q-btn` "Installa app" → `deferredPrompt.prompt()`; a `userChoice`
  risolto, nascondere il pulsante.
- `window.addEventListener('appinstalled', …)` → nascondere.
- Se `navigator.standalone === true` o display-mode standalone → non
  mostrare nulla (già installata).
- **iOS/Safari** (nessun `beforeinstallprompt`): se UA è iOS e non
  standalone, mostrare una nota "Aggiungi a Home dal menu Condividi".

Montaggio: nella `MainLayout.vue`, nella toolbar in alto (accanto ai
controlli esistenti).

### A3. `assetlinks.json`

Nuovo file `bt-dash/public/.well-known/assetlinks.json` (Quasar copia
`public/` → `dist/pwa/`):

```json
[{
  "relation": ["delegate_permission/common.handle_all_urls"],
  "target": {
    "namespace": "android_app",
    "package_name": "PLACEHOLDER.da.riempire",
    "sha256_cert_fingerprints": ["PLACEHOLDER:SHA256:del:cert:di:firma"]
  }
}]
```

`package_name` e fingerprint si compilano quando si genererà l'APK
(PWABuilder o Bubblewrap). Fino ad allora il file è inerte.

Per una TWA il file va servito su **`/.well-known/assetlinks.json`** alla
**root del dominio**, non sotto `/dash/`. Vedi §5 (Alias Apache).

### A4. Rebuild

`scripts/bt-dash-build-prod.sh` (invariato) → `npx quasar build -m pwa`.

---

## 4. Parte B — auth a sessione

### B1. Nuovo blueprint `bt-api/app/auth.py`

Registrato in `server.py` **senza** `url_prefix` (path raggiungibili come
`/api/auth/*` dal browser):

| Metodo | Path | Comportamento |
|---|---|---|
| `POST` | `/auth/login` | body JSON `{ "password": "..." }`. Verifica con `werkzeug.security.check_password_hash` contro `BT_DASH_PW_HASH`. OK → `session['authed'] = True`, `session.permanent = True`, `200 {ok:true}`. KO → `401 {ok:false}`. |
| `POST` | `/auth/logout` | `session.clear()`, `204`. |
| `GET` | `/auth/status` | `200 { authed: bool }` (legge `session.get('authed', False)`). |

Nessuno store utente: singolo utente, il "chi" è implicito.

### B2. Guard globale (`bt-api/server.py`)

```python
_PUBLIC_PATHS = {'/auth/login', '/auth/status', '/github-webhook'}

@app.before_request
def _require_auth():
    if request.method == 'OPTIONS':
        return
    if request.path in _PUBLIC_PATHS:
        return
    if not session.get('authed'):
        return jsonify(error='auth required'), 401
```

- `/github-webhook` resta pubblico: già protetto da firma HMAC.
- `/fs/*`, `/dyn/*` e tutto il resto → protetti.
- La route `GET /` ("Ciao, mondo!") diventa protetta: irrilevante.

### B3. Config sessione (`bt-api/server.py`, dopo `app = Flask(__name__)`)

```python
app.config.update(
    SECRET_KEY=os.environ['BT_DASH_SECRET_KEY'],          # obbligatorio, no default
    PERMANENT_SESSION_LIFETIME=timedelta(days=90),
    SESSION_REFRESH_EACH_REQUEST=True,                    # rolling
    SESSION_COOKIE_SECURE=True,
    SESSION_COOKIE_HTTPONLY=True,
    SESSION_COOKIE_SAMESITE='Lax',
    SESSION_COOKIE_PATH='/',                              # inviato a /dash e /api
)
```

- Il cookie è il **cookie di sessione firmato di Flask** (stateless): con
  `SECRET_KEY` stabile lo validano tutti e 2 i worker gunicorn, nessuno
  stato condiviso necessario.
- `SECRET_KEY` **deve** venire dall'ambiente ed essere stabile: se cambia o
  è casuale per-processo, tutte le sessioni cadono → si rifà login (viola
  l'obiettivo).
- Avvio: se `BT_DASH_SECRET_KEY` manca → l'app non parte (fail-fast), così
  non si finisce con un default insicuro in prod.

### B4. Segreti — `env/server`

Aggiungere a `/home/htpc/backtrader/env/server` (git-ignored, già
sourced da `bt-api/start.sh`):

```sh
BT_DASH_SECRET_KEY=<32+ byte random, es. python -c "import secrets;print(secrets.token_hex(32))">
BT_DASH_PW_HASH=<output di werkzeug.security.generate_password_hash>
```

### B5. Helper password — `bt-api/app/auth.py` come `__main__`

```
python -m app.auth hash        # legge la password da stdin (getpass), stampa l'hash
```

`generate_password_hash` (scrypt di default in werkzeug ≥3, nessuna
dipendenza nuova). L'output si incolla in `env/server`.

### B6. SPA — login view + interceptor

**`bt-dash/src/bt/pages/LoginPage.vue`** (nuovo): route **top-level**
(fuori da `MainLayout`, niente chrome di navigazione):

```js
{ path: '/login', component: () => import('src/bt/pages/LoginPage.vue') }
```

- `<form>` con un solo campo `<input type="password" autocomplete="current-password">`
  + submit → così iOS/Android offrono l'**autofill in biometria**.
- submit → `api.post('/auth/login', { password })`. OK → `router.replace(redirect || '/')`. KO → messaggio "Password errata".

**`bt-dash/src/boot/axios.js`**: interceptor di risposta:

```js
api.interceptors.response.use(r => r, err => {
  if (err.response?.status === 401 &&
      !err.config.url.includes('/auth/')) {
    const redirect = router.currentRoute.value.fullPath
    router.replace({ path: '/login', query: { redirect } })
  }
  return Promise.reject(err)
})
```

(Opzionale, innocuo same-origin: `api.defaults.withCredentials = true`.)

**Avvio app:** un boot/guard chiama `GET /api/auth/status`; se
`authed === false` e la route non è `/login` → redirect a `/login`.
In alternativa affidarsi solo all'interceptor 401 (più semplice, prima
chiamata dati fallisce e reindirizza). **Scelta: solo interceptor** —
meno codice, la prima `q-page` fa comunque una fetch.

### B7. CORS

SPA e API sono **stessa origine** → CORS non entra in gioco per il
browser. Lasciare `ALLOWED_ORIGINS` invariato; **non** serve
`supports_credentials`. Il file morto `bt-api/.htaccess`
(`Access-Control-Allow-Origin *`) non è usato da gunicorn — si può
ignorare o rimuovere (fuori scope).

---

## 5. Apache (`/etc/apache2/sites-available/bt-dash.conf`) — richiede sudo

1. **Rimuovere** l'intero blocco:
   ```apache
   <Location />
       AuthType Basic
       AuthName "Backtrader"
       AuthUserFile /etc/apache2/backtrader.htpasswd
       Require valid-user
   </Location>
   ```
2. **Aggiungere** l'alias per l'asset-links alla root del dominio (per la
   futura TWA; non collide con `acme-challenge` di certbot perché mappa un
   singolo file):
   ```apache
   Alias "/.well-known/assetlinks.json" "/home/htpc/backtrader/bt-dash/dist/pwa/.well-known/assetlinks.json"
   <Files "assetlinks.json">
       Require all granted
   </Files>
   ```
3. `/dash/` statico resta pubblico: è solo la shell JS/CSS, **zero dati**;
   tutti i dati stanno dietro `/api/` ora protetto dalla sessione.
4. `/etc/apache2/backtrader.htpasswd` resta su disco **inutilizzato**
   (documentare come morto; non cancellare in questo intervento).
5. `sudo apache2ctl configtest && sudo systemctl reload apache2`.

---

## 6. Ordine di deploy (evitare finestra con `/api` aperto)

1. **bt-api**: aggiungere `env/server` (SECRET_KEY, PW_HASH); deployare
   `auth.py` + guard + config sessione; riavviare gunicorn.
2. **Verifica**: `curl -s https://ilz.duckdns.org:1443/api/dyn/sc/index`
   con Basic Auth ancora attiva ma **senza** cookie di sessione → deve dare
   `401 {"error":"auth required"}` (attraverso la Basic Auth, autenticandosi
   con le vecchie credenziali htpasswd). `POST /api/auth/login` con la
   password giusta → `200` + `Set-Cookie: session=...`.
3. **bt-dash**: build con LoginPage + interceptor + manifest `purpose` +
   `public/.well-known/assetlinks.json`; pubblicare `dist/pwa/`.
4. **Apache**: rimuovere Basic Auth + aggiungere Alias assetlinks;
   `configtest` + `reload`.
5. **Verifica finale**: browser pulito su `/dash/` → redirect a `/login` →
   login → dashboard OK. Chiudere il browser, riaprire → ancora dentro.
   `/api/dyn/...` senza cookie → `401`. `/.well-known/assetlinks.json` →
   `200`. DevTools → "Installable"; pulsante "Installa app" visibile.

---

## 7. Test automatici (`bt-api`)

- `POST /auth/login` password corretta → `200`, `session` settata.
- `POST /auth/login` password errata → `401`.
- `GET /dyn/...` senza sessione → `401`.
- `GET /dyn/...` con sessione valida → passa al handler.
- `POST /github-webhook` senza sessione → **non** bloccato dal guard
  (arriva al controllo firma HMAC).
- `GET /auth/status` riflette lo stato sessione.

Manuale: persistenza cookie a distanza di giorni; `logout` → torna al form;
autofill password manager mobile.

---

## 8. Rischi e mitigazioni

| Rischio | Mitigazione |
|---|---|
| `SECRET_KEY` assente/instabile → logout a catena | Fail-fast all'avvio se manca; documentato in `env/server`. |
| Finestra con `/api` non protetto durante il deploy | Ordine §6: guard prima, rimozione Basic Auth per ultima. |
| Brute-force sul login (no lockout) | Accettato dall'utente. Difesa = password forte + hash scrypt lento. Endpoint su HTTPS, porta non standard. |
| CSRF | `SameSite=Lax` + API JSON (form cross-site non può settare `Content-Type: application/json`); tool monoutente. Nessun token. |
| Icone `maskable` con soggetto tagliato | Cosmetico, non blocca l'install; rigenerazione icone fuori scope. |
| `assetlinks.json` con placeholder | Inerte finché non si genera l'APK; nessun effetto sulla PWA. |

---

## 9. Elenco file

**Nuovi**
- `bt-api/app/auth.py`
- `bt-dash/src/bt/pages/LoginPage.vue`
- `bt-dash/src/components/InstallPwaButton.vue`
- `bt-dash/public/.well-known/assetlinks.json`

**Modificati**
- `bt-api/server.py` — registra blueprint, config sessione, `before_request` guard
- `bt-dash/quasar.conf.js` — manifest `id`/`scope`/`purpose`
- `bt-dash/src/boot/axios.js` — interceptor 401 → `/login`
- `bt-dash/src/router/routes.js` — route `/login` top-level
- `bt-dash/src/layouts/MainLayout.vue` — monta `<InstallPwaButton>`
- `env/server` — `BT_DASH_SECRET_KEY`, `BT_DASH_PW_HASH` (non in git)
- `/etc/apache2/sites-available/bt-dash.conf` — via sudo (§5)

**Documentare come morto:** `/etc/apache2/backtrader.htpasswd`, `bt-api/.htaccess`
