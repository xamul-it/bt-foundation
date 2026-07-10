#!/usr/bin/env bash
# Validate and optionally tag the exact commit checked out on the prod branch.
set -euo pipefail

usage() {
    echo "Usage: $0 [--create] [--push] prod-YYYY.MM.DD-N" >&2
}

CREATE=0
PUSH=0
while [[ "${1:-}" == --* ]]; do
    case "$1" in
        --create) CREATE=1 ;;
        --push) CREATE=1; PUSH=1 ;;
        -h|--help) usage; exit 0 ;;
        *) usage; exit 64 ;;
    esac
    shift
done
[[ $# == 1 ]] || { usage; exit 64; }
TAG=$1
[[ "$TAG" =~ ^prod-[0-9]{4}\.[0-9]{2}\.[0-9]{2}-[1-9][0-9]*$ ]] || {
    echo "Invalid production tag: $TAG" >&2
    exit 64
}

ROOT=$(git rev-parse --show-toplevel)
[[ $(git -C "$ROOT" branch --show-current) == prod ]] || {
    echo "Production releases must be tagged from branch prod" >&2
    exit 78
}
[[ -z $(git -C "$ROOT" status --porcelain --untracked-files=no) ]] || {
    echo "The production worktree is not clean" >&2
    exit 78
}
git -C "$ROOT" submodule foreach --quiet --recursive '
    test -z "$(git status --porcelain --untracked-files=no)" || { echo "$name is dirty" >&2; exit 1; }
'
git -C "$ROOT" fetch origin prod --tags
[[ $(git -C "$ROOT" rev-parse HEAD) == $(git -C "$ROOT" rev-parse origin/prod) ]] || {
    echo "Local prod is not exactly origin/prod" >&2
    exit 78
}
! git -C "$ROOT" rev-parse -q --verify "refs/tags/$TAG" >/dev/null || {
    echo "Tag already exists: $TAG" >&2
    exit 65
}

echo "release_commit=$(git -C "$ROOT" rev-parse HEAD)"
git -C "$ROOT" submodule status --recursive
if (( CREATE )); then
    git -C "$ROOT" tag -a "$TAG" -m "Production release $TAG"
    echo "created $TAG"
fi
if (( PUSH )); then
    git -C "$ROOT" push origin "$TAG"
    echo "pushed $TAG"
fi
if (( ! CREATE )); then
    echo "validation complete; rerun with --create or --push"
fi
