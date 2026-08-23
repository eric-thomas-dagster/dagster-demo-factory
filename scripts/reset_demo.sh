#!/usr/bin/env bash
#
# Requeue a prospect so Demo Factory rebuilds it from scratch.
#
#   ./scripts/reset_demo.sh <company_slug> [--keep-location] [--keep-project]
#
# By default this removes the generated project and deletes the Dagster+ code
# location, then flips the ledger entry back to "brief-ready" so the next
# Factory run picks it up. The brief itself is never touched — edit that by
# hand if the build directives should change.

set -euo pipefail

SLUG="${1:?usage: reset_demo.sh <company_slug> [--keep-location] [--keep-project]}"
shift || true
KEEP_LOCATION=0
KEEP_PROJECT=0
for arg in "$@"; do
  case "$arg" in
    --keep-location) KEEP_LOCATION=1 ;;
    --keep-project)  KEEP_PROJECT=1 ;;
    *) echo "unknown flag: $arg" >&2; exit 1 ;;
  esac
done

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
LEDGER="$REPO_ROOT/state/ledger.json"
PROJECT_DIR="$REPO_ROOT/demos/$SLUG"
LOCATION="demo-$SLUG"

export PATH="$HOME/.local/bin:$PATH"

echo "==> Resetting $SLUG"

if [[ "$KEEP_PROJECT" == "0" && -d "$PROJECT_DIR" ]]; then
  # build_demo.sh refuses to clobber an existing project dir, so a stale one
  # silently blocks the next rebuild.
  echo "    removing $PROJECT_DIR"
  rm -rf "$PROJECT_DIR"
else
  echo "    keeping project directory"
fi

if [[ "$KEEP_LOCATION" == "0" ]]; then
  if [[ -n "${DAGSTER_CLOUD_API_TOKEN:-}" ]]; then
    echo "    deleting Dagster+ location $LOCATION"
    dagster-cloud deployment delete-location \
      --location-name "$LOCATION" \
      --organization "${DAGSTER_CLOUD_ORGANIZATION:?not set}" \
      --deployment "${DAGSTER_CLOUD_DEPLOYMENT:?not set}" \
      --api-token "$DAGSTER_CLOUD_API_TOKEN" 2>/dev/null \
      || echo "    (location absent or already removed)"
  else
    echo "    DAGSTER_CLOUD_API_TOKEN not set locally — skipping location delete"
    echo "    (the next deploy will overwrite the location anyway)"
  fi
else
  echo "    keeping Dagster+ location"
fi

echo "    flipping ledger entry to brief-ready"
python3 - "$LEDGER" "$SLUG" <<'PY'
import json, sys, datetime
ledger_path, slug = sys.argv[1], sys.argv[2]
with open(ledger_path) as f:
    data = json.load(f)
entries = data.get("built", data if isinstance(data, list) else [])
hit = None
for e in entries:
    if e.get("slug") == slug:
        hit = e
        break
if hit is None:
    print(f"    WARNING: no ledger entry for '{slug}' — Factory will find nothing to build")
    sys.exit(0)
hit["status"] = "brief-ready"
hit["reset_at"] = datetime.datetime.now().isoformat(timespec="seconds")
hit.pop("dagster_plus_url", None)
with open(ledger_path, "w") as f:
    json.dump(data, f, indent=2)
    f.write("\n")
print(f"    ledger entry for '{slug}' is now brief-ready")
PY

cat <<EOF

==> Reset complete.

Commit and push the ledger change, then either wait for the 2am run or fire
the Factory routine on demand:

  git add state/ledger.json && git commit -m "Requeue $SLUG" && git push

EOF
