#!/usr/bin/env bash
#
# Scaffold a new demo project and wire in the community component registry.
#
#   ./scripts/build_demo.sh <company_slug>
#
# Order matters: `dagster-component init` does NOT create a project. It writes
# AI-tool config (CLAUDE.md, .cursorrules, copilot-instructions.md) and injects
# the `dagster_dg_cli.registry_modules` entry point + editable install into a
# project that already exists. So create-dagster runs first.
#
# Leaves you in an activated venv inside demos/<slug> with a project that
# passes `dg check defs`. Writing assets is the routine's job, not this script's.

set -euo pipefail

SLUG="${1:?usage: build_demo.sh <company_slug>}"
REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
PROJECT_DIR="$REPO_ROOT/demos/$SLUG"

export PATH="$HOME/.local/bin:$PATH"

if [[ -d "$PROJECT_DIR" ]]; then
  echo "ERROR: $PROJECT_DIR already exists. Refusing to clobber an existing demo." >&2
  echo "Delete it deliberately, or pick a different slug." >&2
  exit 1
fi

echo "==> Scaffolding Dagster project: $SLUG"
mkdir -p "$REPO_ROOT/demos"
cd "$REPO_ROOT/demos"
uvx create-dagster@latest project "$SLUG" --uv-sync

cd "$PROJECT_DIR"
# shellcheck disable=SC1091
source .venv/bin/activate

echo "==> Wiring in the community component registry"
# --auto-install skips the editable-install confirmation prompt, which would
# otherwise hang forever in an unattended run.
uvx --from dagster-community-components-cli dagster-component init \
    --auto-install --force

echo "==> Baseline validation (empty project must load before we add anything)"
dg check defs
dg list defs

cat <<EOF

==> Ready.

  Project:  $PROJECT_DIR
  Venv:     active

Next, inside this project:
  uvx --from dagster-community-components-cli dagster-component search <term>
  uvx --from dagster-community-components-cli dagster-component info <id>
  uvx --from dagster-community-components-cli dagster-component add <id> --auto-install

If component deps don't get picked up:
  uvx --from dagster-community-components-cli dagster-component sync-deps

If \`dg list components\` doesn't show your custom components, the registry
entry point didn't take -- re-run \`dagster-component init --force\`.

Validate before publishing:
  ./scripts/validate_demo.sh $SLUG
EOF
