#!/usr/bin/env bash
# Wrapper shell per dump_windows_download.py.
# Scarica finestre temporali dai dump indicizzati, con provider opzionali.

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

# Optional override via env without flags.
PROVIDERS_ARG=()
if [[ -n "${DUMP_PROVIDERS:-}" ]]; then
  PROVIDERS_ARG=(--providers "$DUMP_PROVIDERS")
fi

python3 "$SCRIPT_DIR/dump_windows_download.py" "${PROVIDERS_ARG[@]}"
