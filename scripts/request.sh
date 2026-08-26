#!/usr/bin/env bash
#
# Tell the routines to do something with a prospect.
#
#   ./scripts/request.sh <slug> <action> ["optional notes"]
#
# Actions:
#   rebuild-brief   Recon rewrites the brief from scratch
#   rebuild-demo    Factory deletes the project and rebuilds from the brief
#   enhance         Factory modifies the existing project; notes describe how
#   build-poc       POC Builder builds a POC; notes name the scenario doc
#
# The file's presence is the instruction. Both routines check requests/ before
# any other selection logic. Commit and push, then run the routine.

set -euo pipefail

SLUG="${1:?usage: request.sh <slug> <action> [\"notes\"]}"
ACTION="${2:?missing action: rebuild-brief | rebuild-demo | enhance | build-poc}"
NOTES="${3:-}"

case "$ACTION" in
  rebuild-brief|rebuild-demo|enhance|build-poc) ;;
  *) echo "unknown action: $ACTION" >&2
     echo "expected: rebuild-brief | rebuild-demo | enhance | build-poc" >&2
     exit 1 ;;
esac

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
mkdir -p "$REPO_ROOT/requests/done"
FILE="$REPO_ROOT/requests/$SLUG.md"

{
  echo "---"
  echo "action: $ACTION"
  echo "requested: $(date +%Y-%m-%d)"
  echo "---"
  echo
  if [[ -n "$NOTES" ]]; then
    echo "$NOTES"
  else
    echo "<!-- Optional: describe what should change. Anything here overrides"
    echo "     what the brief currently says. -->"
  fi
} > "$FILE"

echo "Wrote $FILE"
echo
cat "$FILE"
cat <<EOF

Next:
  git add requests/ && git commit -m "Request: $ACTION $SLUG" && git push

Then run the relevant routine. It will honour this before checking the calendar
or the ledger, and move the file to requests/done/ when finished.
EOF
