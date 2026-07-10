#!/usr/bin/env bash
set -euo pipefail

ROOT=$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)
TMP=$(mktemp -d)
trap 'rm -rf "$TMP"' EXIT
mkdir -p "$TMP/profiles" "$TMP/state"
: >"$TMP/account.env"

sed \
    -e "s|^CODE_ROOT=.*|CODE_ROOT=$ROOT|" \
    -e "s|^ACCOUNT_ENV=.*|ACCOUNT_ENV=$TMP/account.env|" \
    "$ROOT/config-common/scheduled/profiles/development.env.example" \
    >"$TMP/profiles/development.env"

export BT_SCHEDULED_CONFIG_DIR=$TMP/profiles
export BT_SCHEDULED_STATE_DIR=$TMP/state

"$ROOT/scripts/scheduled-job.sh" --check development entry
for phase in entry exit exit-fallback; do
    output=$("$ROOT/scripts/scheduled-job.sh" --dry-run development "$phase")
    [[ "$output" == *"profile=development phase=$phase mode=paper"* ]]
    [[ "$output" == *"run:"* ]]
done

echo "scheduled job tests passed"
