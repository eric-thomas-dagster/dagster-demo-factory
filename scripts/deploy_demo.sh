#!/usr/bin/env bash
#
# Deploy a validated demo to Dagster+ Serverless.
#
#   ./scripts/deploy_demo.sh <company_slug> [package_name]
#
# Requires in the environment:
#   DAGSTER_CLOUD_ORGANIZATION   e.g. ericthomas-dagster
#   DAGSTER_CLOUD_API_TOKEN
#   DAGSTER_CLOUD_DEPLOYMENT     e.g. demos  (prod only if you mean it)
#
# Uses `deploy-python-executable`, NOT `serverless deploy`. The plain deploy
# command builds a Docker image; there is no Docker daemon in a Claude Code
# cloud sandbox. `--build-method local` builds PEX files from the current
# environment only.
#
# PEX-local can only bundle packages that publish wheels. If a dependency is
# source-only the build fails -- pin to a wheel-publishing version or drop it
# and substitute something demo-mode-only.

set -euo pipefail

SLUG="${1:?usage: deploy_demo.sh <company_slug> [package_name]}"
PKG="${2:-$SLUG}"
REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
PROJECT_DIR="$REPO_ROOT/demos/$SLUG"
LOCATION="demo-$SLUG"

: "${DAGSTER_CLOUD_ORGANIZATION:?not set}"
: "${DAGSTER_CLOUD_API_TOKEN:?not set}"
: "${DAGSTER_CLOUD_DEPLOYMENT:?not set}"

export PATH="$HOME/.local/bin:$PATH"

echo "==> Re-validating before deploy"
"$REPO_ROOT/scripts/validate_demo.sh" "$SLUG" "$PKG"

echo "==> Deploying $LOCATION to $DAGSTER_CLOUD_ORGANIZATION/$DAGSTER_CLOUD_DEPLOYMENT"
if ! dagster-cloud serverless deploy-python-executable "$PROJECT_DIR" \
      --location-name "$LOCATION" \
      --module-name "$PKG.definitions" \
      --python-version 3.12 \
      --build-method local \
      --organization "$DAGSTER_CLOUD_ORGANIZATION" \
      --deployment "$DAGSTER_CLOUD_DEPLOYMENT" \
      --api-token "$DAGSTER_CLOUD_API_TOKEN"; then
  echo "ERROR: deploy failed. Cleaning up any partial location." >&2
  dagster-cloud deployment delete-location "$LOCATION" \
    --organization "$DAGSTER_CLOUD_ORGANIZATION" \
    --deployment "$DAGSTER_CLOUD_DEPLOYMENT" \
    --api-token "$DAGSTER_CLOUD_API_TOKEN" 2>/dev/null || true
  exit 1
fi

# A successful deploy command does NOT mean the location loaded. Confirm it,
# or we email "ready!" about a red banner.
echo "==> Waiting for code location to report loaded (up to 5 min)"
for i in $(seq 1 30); do
  # `dagster-cloud deployment list-locations` prints names/images only, no
  # status -- use `dg api code-location list --json` instead, which returns
  # a real `status` field (LOADED / FAILED / etc.) per location. (2026-08-24)
  STATUS_JSON="$(dg api code-location list --json 2>/dev/null \
    | python3 -c "
import json, sys
data = json.load(sys.stdin)
items = data.get('items', data) if isinstance(data, dict) else data
for loc in items:
    if loc.get('location_name') == '$LOCATION':
        print(loc.get('status', 'UNKNOWN'))
        break
" || true)"
  if [[ "$STATUS_JSON" == "LOADED" ]]; then
    echo "    location is live"
    echo
    echo "==> URL: https://${DAGSTER_CLOUD_ORGANIZATION}.dagster.cloud/${DAGSTER_CLOUD_DEPLOYMENT}/locations/${LOCATION}"
    exit 0
  fi
  if [[ "$STATUS_JSON" == "FAILED" ]]; then
    echo "ERROR: location failed to load." >&2
    exit 1
  fi
  sleep 10
done

echo "WARNING: location did not confirm loaded within 5 minutes." >&2
echo "Report this as UNCONFIRMED, not as success." >&2
exit 1
