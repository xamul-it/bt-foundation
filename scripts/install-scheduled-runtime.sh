#!/usr/bin/env bash
# Install missing runtime templates. Existing profiles and secrets are preserved.
set -euo pipefail

ROOT=$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)
CONFIG_HOME=${XDG_CONFIG_HOME:-$HOME/.config}/backtrader
PROFILE_DIR=$CONFIG_HOME/scheduled
ACCOUNT_DIR=$CONFIG_HOME/accounts
BIN_DIR=${BT_USER_BIN_DIR:-$HOME/bin}

mkdir -p "$PROFILE_DIR" "$ACCOUNT_DIR" "$BIN_DIR"
for src in "$ROOT"/config-common/scheduled/profiles/*.env.example; do
    name=$(basename "$src" .example)
    if [[ ! -e "$PROFILE_DIR/$name" ]]; then
        cp "$src" "$PROFILE_DIR/$name"
        echo "installed $PROFILE_DIR/$name"
    else
        echo "preserved $PROFILE_DIR/$name"
    fi
done

cat <<EOF

Profile templates are installed. Account files were not created or copied.
Create these manually with mode 0600 after verifying each Alpaca account:
  $ACCOUNT_DIR/live.env
  $ACCOUNT_DIR/mirror.env
  $ACCOUNT_DIR/challenger.env
  $ACCOUNT_DIR/development.env

After profiles have been verified, install a stable launcher with:
  ln -s $ROOT/scripts/scheduled-job.sh $BIN_DIR/bt-scheduled
EOF
