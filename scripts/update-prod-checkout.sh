#!/usr/bin/env bash
# Fast-forward a clean production checkout and materialize pinned submodules.
set -euo pipefail

CHECKOUT=${1:-/home/htpc/backtrader-prod}
CHECKOUT=$(realpath -e "$CHECKOUT")
[[ $(git -C "$CHECKOUT" branch --show-current) == prod ]] || {
    echo "$CHECKOUT is not on branch prod" >&2
    exit 78
}
[[ -z $(git -C "$CHECKOUT" status --porcelain --untracked-files=no) ]] || {
    echo "$CHECKOUT is not clean; refusing to update" >&2
    exit 78
}
git -C "$CHECKOUT" submodule foreach --quiet --recursive '
    test -z "$(git status --porcelain --untracked-files=no)" || { echo "$name is dirty" >&2; exit 1; }
'

git -C "$CHECKOUT" fetch origin prod --tags
git -C "$CHECKOUT" merge --ff-only origin/prod
git -C "$CHECKOUT" submodule sync --recursive
git -C "$CHECKOUT" submodule update --init --recursive

echo "deployed_commit=$(git -C "$CHECKOUT" rev-parse HEAD)"
git -C "$CHECKOUT" describe --tags --exact-match 2>/dev/null || echo "warning: deployed commit has no exact tag" >&2
git -C "$CHECKOUT" submodule status --recursive
