#!/usr/bin/env bash
#
# Catch packaging failures BEFORE spending a multi-minute deploy cycle.
#
#   ./scripts/preflight_deploy.sh <company_slug> [package_name]
#
# The 2026-08-24 Northwind run needed seven deploy attempts. Every failure was
# packaging, and every one of them is checkable locally in seconds. Run this
# first; only deploy when it exits 0.

set -uo pipefail

SLUG="${1:?usage: preflight_deploy.sh <company_slug> [package_name]}"
PKG="${2:-$SLUG}"
REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
PROJECT_DIR="$REPO_ROOT/demos/$SLUG"

export PATH="$HOME/.local/bin:$PATH"
cd "$PROJECT_DIR" || { echo "No project at $PROJECT_DIR" >&2; exit 1; }
# shellcheck disable=SC1091
source .venv/bin/activate 2>/dev/null || true

FAILED=0
ok()   { echo "  ✓ $1"; }
bad()  { echo "  ✗ $1" >&2; FAILED=1; }
warn() { echo "  ! $1"; }

echo "==> [1/7] pex installed (required by --build-method local)"
if python -c "import pex" 2>/dev/null || command -v pex >/dev/null; then
  ok "pex present"
else
  bad "pex missing — run: uv pip install pex"
fi

echo "==> [2/7] dagster-cloud is a project dependency"
if grep -qE '^\s*"?dagster-cloud' pyproject.toml; then
  ok "dagster-cloud in pyproject.toml"
else
  bad "dagster-cloud not in pyproject.toml dependencies (CLI on PATH is not enough)"
fi

echo "==> [3/7] package name resolves to a real module with Definitions"
if python -c "import $PKG" 2>/dev/null; then
  ok "import $PKG works"
  if python -c "
import importlib, dagster as dg
m = importlib.import_module('$PKG')
found = any(isinstance(getattr(m, a, None), dg.Definitions) for a in dir(m))
raise SystemExit(0 if found else 1)
" 2>/dev/null; then
    ok "Definitions object found in $PKG"
  else
    warn "no top-level Definitions in $PKG — fine if defined via defs/ discovery, otherwise check --package-name"
  fi
else
  bad "cannot import '$PKG' — --package-name is probably wrong"
fi

echo "==> [4/7] dbt project lives inside the package directory"
DBT_DIRS="$(find . -name dbt_project.yml -not -path "./.venv/*" 2>/dev/null)"
if [[ -z "$DBT_DIRS" ]]; then
  ok "no dbt project (skipping)"
else
  while IFS= read -r d; do
    if [[ "$d" == ./src/$PKG/* || "$d" == ./$PKG/* ]]; then
      ok "dbt project inside package: $d"
    else
      bad "dbt project at $d is OUTSIDE the package — it will not ship in the wheel"
    fi
  done <<< "$DBT_DIRS"
fi

echo "==> [5/7] defs state generated (state-backed components)"
if dg utils refresh-defs-state >/dev/null 2>&1; then
  ok "refresh-defs-state succeeded"
else
  warn "refresh-defs-state failed or not applicable — verify if using Fivetran/dbt components"
fi

echo "==> [6/7] definitions load"
if dg check defs >/dev/null 2>&1; then ok "dg check defs"; else bad "dg check defs FAILED"; fi
if dg check yaml >/dev/null 2>&1; then ok "dg check yaml"; else bad "dg check yaml FAILED"; fi

echo "==> [7/7] wheel actually CONTAINS the runtime files"
# The one that cost the most time on 2026-08-24: gitignored build artifacts are
# silently excluded from the wheel and only fail once deployed.
rm -rf dist
if python -m build --wheel >/dev/null 2>&1 || uv build --wheel >/dev/null 2>&1; then
  WHL="$(ls dist/*.whl 2>/dev/null | head -1)"
  if [[ -z "$WHL" ]]; then
    bad "wheel build produced no artifact"
  else
    CONTENTS="$(unzip -l "$WHL")"
    if [[ -n "$DBT_DIRS" ]]; then
      echo "$CONTENTS" | grep -q "dbt_project.yml" \
        && ok "dbt_project.yml in wheel" \
        || bad "dbt_project.yml NOT in wheel — add a force-include in pyproject.toml"
      echo "$CONTENTS" | grep -q "manifest.json" \
        && ok "dbt manifest.json in wheel" \
        || bad "manifest.json NOT in wheel — it is gitignored; force-include it"
    fi
    if compgen -G "**/defs_state*" >/dev/null 2>&1 || echo "$CONTENTS" | grep -qi "defs_state"; then
      echo "$CONTENTS" | grep -qi "defs_state" \
        && ok "defs state in wheel" \
        || bad "defs state NOT in wheel — force-include it"
    fi
    echo "$CONTENTS" | grep -q "$PKG/" && ok "package contents present" || bad "package dir missing from wheel"
  fi
else
  bad "wheel build failed — fix before deploying"
fi
rm -rf dist

echo
if [[ "$FAILED" == "1" ]]; then
  echo "PREFLIGHT FAILED — fix the ✗ items above before deploying." >&2
  echo "Each one costs a full deploy cycle to discover the hard way." >&2
  exit 1
fi
echo "PREFLIGHT PASSED — safe to deploy."
