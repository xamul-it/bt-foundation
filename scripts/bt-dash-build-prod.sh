#!/usr/bin/env bash
# Build the production PWA in isolation before replacing Apache's static files.
set -euo pipefail

root=$(cd "$(dirname "$0")/.." && pwd)
dash_dir="$root/bt-dash"
dist_dir="$dash_dir/dist"
mkdir -p "$dist_dir"
exec 9>"$dist_dir/.pwa-deploy.lock"
flock -n 9 || { echo 'Pubblicazione PWA già in corso.' >&2; exit 1; }
stage=$(mktemp -d "$dist_dir/.pwa-build.XXXXXX")
backup="$dist_dir/.pwa-previous.$$"
cleanup() {
  if [[ -d "$backup" && ! -e "$dist_dir/pwa" ]]; then mv "$backup" "$dist_dir/pwa"; fi
  rm -rf "$stage"
}
trap cleanup EXIT

cd "$dash_dir"
set -a
# shellcheck source=/dev/null
source "$root/env/bt-dash-prod"
set +a
export PATH="/home/htpc/.nvm/versions/node/v20.20.0/bin:$PATH"
export BT_DASH_DIST_DIR="$stage"

echo 'Compilazione PWA in directory temporanea...'
npx quasar build -m pwa
for artifact in index.html manifest.json sw.js; do
  [[ -s "$stage/$artifact" ]] || { echo "Artefatto mancante: $artifact" >&2; exit 1; }
done
[[ -d "$stage/js" && -d "$stage/css" ]] || { echo 'Asset JS/CSS mancanti.' >&2; exit 1; }

# Keep hashed assets from the previous release for clients with an open tab.
if [[ -d "$dist_dir/pwa" ]]; then
  for assets in js css; do
    [[ ! -d "$dist_dir/pwa/$assets" ]] || cp -an "$dist_dir/pwa/$assets/." "$stage/$assets/"
  done
  mv "$dist_dir/pwa" "$backup"
fi
mv "$stage" "$dist_dir/pwa"
rm -rf "$backup"
echo "PWA pubblicata: $dist_dir/pwa (Apache non richiede riavvio)."
