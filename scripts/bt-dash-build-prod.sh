#!/usr/bin/env bash
# Rebuilds bt-dash/dist/pwa/ for production -- run this after any bt-dash
# change that should reach https://ilz.duckdns.org:1443/dash/.
#
# bt-dash-prod is served as a static PWA build by Apache (see
# /etc/apache2/sites-available/bt-dash.conf, Alias /dash -> dist/pwa), not
# by a continuously-running bt-dash@prod.service -- that unit was
# deliberately stopped/disabled once static serving replaced it (Quasar's
# dev server never serves a working service worker, even with -m pwa;
# verified empirically -- only `quasar build` produces one, which is the
# whole reason PWA installability requires a real build here). This script
# is the "build on deploy" step to run in its place; Apache needs no
# restart afterwards, it just serves whatever is currently on disk.
#
# bt-dash@dev is untouched by this -- it keeps running as the continuous
# dev server for development, unrelated to this script.
set -euo pipefail

cd "$(dirname "$0")/../bt-dash"

# Same env file the old bt-dash@prod.service used (PORT/BT_DASH_HOST are
# dev-server-only and irrelevant to a static build; VUE_APP_API_URL is the
# one setting that actually gets baked into the bundle at build time).
set -a
# shellcheck source=/dev/null
source ../env/bt-dash-prod
set +a

export PATH="/home/htpc/.nvm/versions/node/v20.20.0/bin:$PATH"

echo "Building bt-dash (pwa mode) with VUE_APP_API_URL=${VUE_APP_API_URL}"
npx quasar build -m pwa

echo "Done. Output: $(pwd)/dist/pwa"
echo "Nothing to restart -- Apache serves dist/pwa/ directly."
