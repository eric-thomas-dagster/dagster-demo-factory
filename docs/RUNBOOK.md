# Demo Factory — Operations Runbook

How this thing runs day to day, and where to change it when you want it to
behave differently.

---

## What it does

Two Claude Code Routines running in the cloud, against this repo.

**Prospect Recon** — weeknights 6:30pm. Reads your calendar for upcoming
external demos, picks the soonest one not already built, hunts down the AE's
discovery doc in Drive, researches the company publicly, and writes a brief to
`briefs/`. Opens a PR and notifies you.

**Demo Factory** — nightly 2:00am. Takes the newest brief, scaffolds a Dagster
project, builds the asset graph with real components in demo mode, validates it,
opens a PR, deploys to Dagster+, and notifies you.

The gap between them is deliberate. Recon tells you at 6:30pm who it's building
for; you have until 2am to edit the brief. That edit is the cheapest possible
intervention — one paragraph in a markdown file redirects the entire build.

---

## The normal rhythm

**Evening.** Push notification: *"Tonight's build: Acme Health — demo Thursday."*
Read it. If the prospect is wrong or the thesis is off, edit the brief in the
PR. If the demo isn't worth building for, do nothing — or reset the ledger entry
so Factory skips it.

**Morning.** Push notification with the Dagster+ link and the run-of-show. A
Gmail draft holds the long version. Open the deployed location, walk the money
shots once, then go give the demo.

**Before any real demo, rehearse it yourself:**

```bash
git fetch origin
git checkout claude/demo-<slug>-<date>
cd demos/<slug>
uv sync && source .venv/bin/activate
dg dev
```

A green build report is not a rehearsed demo. You want to know where the graph
view sits and what the check text actually reads like before you're sharing a
screen.

---

## Firing on demand

Set up once: Factory routine → Edit → Add another trigger → API → Generate
token. Then keep this in your shell profile:

```bash
demo-fire() {
  curl -sS -X POST "https://api.anthropic.com/v1/claude_code/routines/$ROUTINE_TRIGGER_ID/fire" \
    -H "Authorization: Bearer $ROUTINE_TRIGGER_TOKEN" \
    -H "anthropic-beta: experimental-cc-routine-2026-04-01" \
    -H "anthropic-version: 2023-06-01" \
    -H "Content-Type: application/json" \
    -d "{\"text\": \"$*\"}"
}
```

Usage:

```bash
demo-fire "Enhance northwind-logistics: make the check failure message more legible on a shared screen"
demo-fire "Build for acme-health now, demo moved up to tomorrow 9am"
demo-fire "Rebuild northwind-logistics from scratch"
```

The fire text reaches the routine as a `<routine-fire-payload>` block. Section 1
of the Factory prompt opts into reading it — without that, fired text is ignored.

---

## Iterating: enhance vs rebuild

**ENHANCE** is the default for changes. Factory checks out the existing branch,
makes the smallest change that satisfies the request, revalidates, and redeploys
to the same location. Minutes rather than an hour, and it can't regress what
already works. Use it for: adding a check, changing the anomaly, adjusting
metadata, rewording a check message, swapping a resource.

**REBUILD** starts from the brief. Use it when the brief itself changed
materially — different asset graph, different stack, different thesis.

Manual reset if you'd rather drive it:

```bash
./scripts/reset_demo.sh <slug>                    # full reset
./scripts/reset_demo.sh <slug> --keep-location    # leave Dagster+ alone
./scripts/reset_demo.sh <slug> --keep-project     # leave the code, requeue only
```

It removes the project directory (`build_demo.sh` refuses to clobber an existing
one, so a stale directory silently blocks rebuilds), deletes the Dagster+
location, and flips the ledger entry to `brief-ready`. **It never touches the
brief** — edit that by hand when the build directives should change.

Commit and push the ledger change, or the cloud run won't see it.

---

## The change map

The single most confusing thing about this system: **some of it lives in the
repo, and some lives in web-UI form fields.** Editing the repo copy of a routine
prompt does nothing. The routine runs whatever text is saved in the form.

### Things that live in the repo (edit, commit, push)

| I want to change... | Edit |
|---|---|
| Standing rules for every build | `CLAUDE.md` |
| Accumulated facts, known dead ends | `LEARNINGS.md` |
| What a brief looks like | `briefs/_TEMPLATE.md` |
| What gets built for one prospect | `briefs/<date>-<slug>.md` |
| The demo_mode subclassing pattern | `templates/demo_mode_pattern.py` |
| What counts as "validated" | `scripts/validate_demo.sh` |
| Pre-deploy packaging checks | `scripts/preflight_deploy.sh` |
| How projects get scaffolded | `scripts/build_demo.sh` |
| How deploys work | `scripts/deploy_demo.sh` |
| How resets work | `scripts/reset_demo.sh` |
| What's been built | `state/ledger.json` |
| Dagster house style, API guidance | `.claude/skills/dagster-expert/` |
| Python style | `.claude/skills/dignified-python/` |

### Things that live in web-UI fields (edit in the browser)

| I want to change... | Where |
|---|---|
| How Recon picks prospects, researches, writes briefs | Recon routine → **Instructions** |
| How Factory builds, validates, deploys, notifies | Factory routine → **Instructions** |
| When they run | Routine → **Triggers** |
| Which connectors they can touch | Routine → **Connectors** |
| Which model | Routine → **Model** |
| Dagster+ org / deployment / token | Environment → **Environment variables** |
| Which domains are reachable | Environment → **Network access** |
| Pre-installed tools | Environment → **Setup script** |

Getting to the environment dialog: `claude.ai/code` → cloud icon **above the
message box** → hover the environment → **gear icon**. There's no settings page
and no direct URL.

The copies of `00-SETUP.md`, `01-routine-prospect-recon.md`, and
`02-routine-demo-factory.md` under `docs/` are **reference only**. Nothing reads
them. Keep them in sync with the live fields by hand so future-you can see what
the prompts say without opening the browser — but changing them changes nothing.

---

## Common failures

**"Resource not accessible by integration" on push.** The Claude GitHub App
isn't installed on the repo, or its scope doesn't include it. Check
`github.com/settings/installations` — if Claude only appears under *Authorized*
and not *Installed*, install it at `github.com/apps/claude` and select this
repo. Reconnecting GitHub in Claude settings will **not** fix this.

**403 with `x-deny-reason: host_not_allowed`.** A domain missing from the
environment's network allowlist. Add the host, save, re-run.

**Deploy fails repeatedly on packaging.** Run
`./scripts/preflight_deploy.sh <slug>` — it checks all seven known causes in
about 20 seconds. Gitignored build artifacts silently missing from the wheel is
the most common and the hardest to spot.

**Factory says nothing is queued.** No ledger entry has `status: "brief-ready"`.
Either Recon hasn't run, or the entry is already `deployed` — reset it.

**Generic Dagster code, no house style.** The skills aren't loading. Confirm
`.claude/skills/dagster-expert/` and `.claude/skills/dignified-python/` are
committed. Cloud sessions cannot read your local `~/.claude/skills/`.

**Email never arrives.** By design. The Gmail connector exposes draft creation
only, and Workspace admin policy disables auto-allow on Send, Reply, and
Forward. Read the draft; the push notification is the real alert. Connect Slack
if you want something that genuinely sends.

---

## Maintenance

**Prune `LEARNINGS.md`** when it passes ~120 lines. It's read at the start of
every run, so a bloated file taxes every build.

**Reap old code locations.** Location names are `demo-<slug>`. Without cleanup
you'll accumulate one per prospect. Either add the weekly cleanup routine from
`docs/00-SETUP.md`, or periodically:

```bash
dagster-cloud deployment delete-location --location-name demo-<slug> \
  --organization "$DAGSTER_CLOUD_ORGANIZATION" \
  --deployment "$DAGSTER_CLOUD_DEPLOYMENT" \
  --api-token "$DAGSTER_CLOUD_API_TOKEN"
```

**Rotate the Dagster+ token quarterly.** Environment variables have no secrets
store — anyone with access to the environment can read them.

**Merge demo PRs** once you've given the demo. Branches accumulate otherwise,
and a stale `claude/demo-<slug>-*` branch can confuse ENHANCE mode about which
one is current.

---

## Testing changes safely

When you change a prompt or a script, don't validate it on a real prospect.

1. Put a fake demo on your calendar 3+ days out with an external attendee — the
   **email domain** is what identifies the company, not the title.
2. Drop a rough discovery doc in Drive named `<Company> — Discovery`.
3. Recon → **Run now**. Read the transcript.
4. Factory → **Run now**. Read the transcript.

Use a *different* fake prospect with a *different* stack each time you test a
significant change. Rebuilding the same one only proves the path you've already
walked works; a fintech on Databricks and Airbyte exercises entirely different
components.

A green run status means the session exited without an infrastructure error. It
does not mean the work was good. Read the transcript.
