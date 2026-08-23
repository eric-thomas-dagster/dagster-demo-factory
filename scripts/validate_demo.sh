#!/usr/bin/env bash
#
# The gate. Nothing gets published or deployed unless this exits 0.
#
#   ./scripts/validate_demo.sh <company_slug> [package_name]
#
# `dg check defs` passing only proves the project LOADS. A demo that loads and
# then crashes on materialize is worse than no demo -- it fails live, on a
# shared screen, in front of the people we're selling to. So this also runs a
# full materialize.

set -euo pipefail

SLUG="${1:?usage: validate_demo.sh <company_slug> [package_name]}"
PKG="${2:-$SLUG}"
REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
PROJECT_DIR="$REPO_ROOT/demos/$SLUG"

export PATH="$HOME/.local/bin:$PATH"
cd "$PROJECT_DIR"
# shellcheck disable=SC1091
source .venv/bin/activate

fail() { echo "FAILED: $1" >&2; exit 1; }

echo "==> [1/5] dg check defs"
dg check defs || fail "definitions did not load"

echo "==> [2/5] dg check yaml"
dg check yaml || fail "component YAML is malformed"

echo "==> [3/5] dg list defs"
dg list defs || fail "could not list definitions"
ASSET_COUNT="$(dg list defs --json 2>/dev/null | grep -c '"assetKey"' || true)"
echo "    assets discovered: ${ASSET_COUNT:-unknown}"

echo "==> [4/5] dg list components"
# Custom components missing here means the registry_modules entry point didn't
# get wired -- they'd be invisible in the Dagster UI's Components tab, which is
# exactly the tab we want to show off.
dg list components || echo "    WARNING: component listing failed; check the registry entry point"

echo "==> [5/5] full materialize (the real test)"
dagster asset materialize --select '*' -m "$PKG" \
  || fail "assets loaded but did not materialize -- do NOT deploy this"

echo
echo "==> PASSED. Safe to publish and deploy."
