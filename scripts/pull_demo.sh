#!/usr/bin/env bash
#
# Pull a demo down locally and get it running. One command.
#
#   ./scripts/pull_demo.sh <company_slug>          # from main, or newest branch
#   ./scripts/pull_demo.sh <company_slug> --dev    # ...and start dg dev
#   ./scripts/pull_demo.sh <company_slug> --pr 2   # from a specific PR
#
# Factory merges to main once a deploy is confirmed, so most of the time the
# demo is already on main and this just syncs and installs. When a build is
# still on a branch (merge blocked, or you want a superseded version), this
# finds it.

set -euo pipefail

SLUG="${1:?usage: pull_demo.sh <company_slug> [--dev] [--pr <number>]}"
shift || true
START_DEV=0
PR_NUM=""
while [[ $# -gt 0 ]]; do
  case "$1" in
    --dev) START_DEV=1; shift ;;
    --pr)  PR_NUM="${2:?--pr needs a number}"; shift 2 ;;
    *) echo "unknown flag: $1" >&2; exit 1 ;;
  esac
done

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$REPO_ROOT"
export PATH="$HOME/.local/bin:$PATH"

echo "==> Fetching"
git fetch origin --prune

if [[ -n "$PR_NUM" ]]; then
  command -v gh >/dev/null || { echo "gh CLI needed for --pr" >&2; exit 1; }
  echo "==> Checking out PR #$PR_NUM"
  gh pr checkout "$PR_NUM"
elif git ls-tree -d --name-only origin/main "demos/$SLUG" 2>/dev/null | grep -q .; then
  echo "==> Found on main"
  git checkout main -q && git pull -q
else
  # Newest matching branch by commit date, not by name — dates in branch names
  # sort lexically but revisions (-r2, -r3) don't.
  BRANCH="$(git for-each-ref --sort=-committerdate --format='%(refname:short)' \
            "refs/remotes/origin/claude/demo-$SLUG-*" | head -1 | sed 's|^origin/||')"
  if [[ -z "$BRANCH" ]]; then
    echo "ERROR: no demo found for '$SLUG' on main or any claude/demo-$SLUG-* branch" >&2
    echo "Available:" >&2
    git for-each-ref --format='  %(refname:short)' 'refs/remotes/origin/claude/demo-*' >&2
    exit 1
  fi
  echo "==> Checking out branch $BRANCH"
  git checkout -q "$BRANCH" 2>/dev/null || git checkout -q -b "$BRANCH" "origin/$BRANCH"
  git pull -q origin "$BRANCH" 2>/dev/null || true
fi

PROJECT_DIR="$REPO_ROOT/demos/$SLUG"
[[ -d "$PROJECT_DIR" ]] || { echo "ERROR: $PROJECT_DIR not present on this ref" >&2; exit 1; }

echo "==> Installing dependencies"
cd "$PROJECT_DIR"
uv sync

cat <<EOF

==> Ready: $PROJECT_DIR

  source .venv/bin/activate
  dg dev            # NOT 'dagster dev'

Useful:
  dg list defs
  dg launch --assets '*'
  dg launch --assets '*' --partition <key>
EOF

if [[ "$START_DEV" == "1" ]]; then
  echo
  echo "==> Starting dg dev"
  # shellcheck disable=SC1091
  source .venv/bin/activate
  exec dg dev
fi
