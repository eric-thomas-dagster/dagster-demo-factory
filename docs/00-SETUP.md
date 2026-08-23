# Overnight Dagster Demo Factory — Setup Runbook

Two **Claude Code Routines** (cloud, run while your laptop is closed) plus one
**factory repo**. You wake up to a validated, deployed demo and an email.

---

## Why two routines instead of one

| | Routine 1 — Prospect Recon | Routine 2 — Demo Factory |
|---|---|---|
| Runs | Weeknights ~6:30pm | Nightly ~2:00am |
| Cost | Cheap (research only) | Expensive (full build) |
| Output | A committed brief + an email to you | A PR + a Dagster+ deployment + an email |
| Connectors | Calendar, Drive, Gmail | GitHub only |

The split buys you a **veto window**. Recon emails you at 6:30pm saying "tonight
I'm building for Acme Health." If it picked wrong, or the AE's notes are thin,
you edit the brief on your phone before 2am and the build follows your edit.
It also means a failed research run doesn't burn a build run against your daily
routine cap.

If you'd rather have one routine, merge the two prompts and schedule it at 2am —
you lose the veto window but nothing else breaks.

---

## Step 0 — Rotate the token you pasted

The Dagster+ token `user:26f8d14b...` went into a chat message. Revoke it:
**Dagster+ → user menu → Organization Settings → Tokens**. Create a fresh one.

Also note how routine environment variables work — from the Claude Code docs,
env vars are *"visible to anyone who uses the environment"*. So:

- Create a **dedicated cloud environment** for this (call it `dagster-demo-factory`),
  don't put the token in `Default` which your other routines share.
- Use a token scoped as narrowly as your plan allows.
- Put a calendar reminder to rotate it quarterly.

---

## Step 1 — Create the factory repo

`github.com/eric-thomas-dagster/dagster-demo-factory`, private.

```
dagster-demo-factory/
├── CLAUDE.md                    # house rules — both routines read this
├── .claude/
│   └── skills/
│       ├── dagster-expert/      # ← COPY YOUR SKILL HERE
│       └── dignified-python/    # ← COPY YOUR SKILL HERE
├── briefs/
│   ├── _TEMPLATE.md
│   └── 2026-08-24-acme-health.md
├── demos/
│   └── acme_health/             # generated projects land here
├── scripts/
│   ├── build_demo.sh
│   └── deploy_demo.sh
├── templates/
│   └── demo_mode_pattern.py
└── state/
    └── ledger.json              # what's been built, prevents rebuilds
```

**The `.claude/skills/` bit is load-bearing.** Routines run as cloud sessions and
can only use skills *committed to the cloned repository*. Your local
`~/.claude/skills/` is invisible to them. Copy `dagster-expert` and
`dignified-python` into the repo and commit them.

Seed `state/ledger.json` with `{"built": []}`.

---

## Step 2 — Create the cloud environment

At `claude.ai/code/routines` → New routine → environment selector → create
`dagster-demo-factory`.

**Environment variables:**

| Name | Value |
|---|---|
| `DAGSTER_CLOUD_ORGANIZATION` | `ericthomas-dagster` |
| `DAGSTER_CLOUD_API_TOKEN` | *(your fresh token)* |
| `DAGSTER_CLOUD_DEPLOYMENT` | `demos` — see the warning below |
| `GH_TOKEN` | *(optional — only if you want standalone repos per prospect)* |

**Network access:** set to **Custom**, check *"Also include default list of
common package managers"*, and add:

```
dagster.cloud
*.dagster.cloud
ericthomas-dagster.dagster.cloud
raw.githubusercontent.com
dagster-component-ui.vercel.app
docs.dagster.io
```

PyPI, npm, and GitHub are already in the default allowlist. Connector traffic
(Drive, Gmail, Calendar) routes through Anthropic's servers and doesn't need
allowlist entries.

**Setup script:**

```bash
curl -LsSf https://astral.sh/uv/install.sh | sh
export PATH="$HOME/.local/bin:$PATH"
uv tool install dagster-cloud
uv tool install "dagster-dg-cli"
```

The result is cached, so this doesn't re-run every session.

---

### ⚠️ On deploying to `prod`

You asked for deployment to `prod`. I'd push back on that one thing.

An unattended agent deploying a fresh, never-human-reviewed code location into
`prod` nightly means: prod accumulates a code location per prospect, a demo that
fails to load shows a red banner in the deployment you screen-share from, and
there's no rollback path at 6am before a 9am call.

Recommendation: create a separate Dagster+ **full deployment named `demos`** and
point `DAGSTER_CLOUD_DEPLOYMENT` there. Identical experience for the prospect,
zero blast radius on prod. Location names are `demo-<prospect>` either way, so
you can find and delete them.

If you still want prod, just set `DAGSTER_CLOUD_DEPLOYMENT=prod` — the routine
prompt reads the env var and doesn't care. But add the cleanup routine in Step 5.

---

## Step 3 — Create Routine 1 (Prospect Recon)

At `claude.ai/code/routines` → **New routine**.

- **Name:** `Prospect Recon`
- **Prompt:** paste `01-routine-prospect-recon.md`
- **Model:** Opus (this is judgment work — reading vague AE notes and deciding
  what's worth demoing)
- **Repository:** `eric-thomas-dagster/dagster-demo-factory`
- **Environment:** `dagster-demo-factory`
- **Trigger:** Schedule → Weekdays → 6:30pm your time
- **Connectors:** keep **Google Calendar, Google Drive, Gmail**. Remove
  everything else — Claude can call any tool from an included connector without
  asking, so a connector you don't need is just risk surface.

---

## Step 4 — Create Routine 2 (Demo Factory)

- **Name:** `Demo Factory`
- **Prompt:** paste `02-routine-demo-factory.md`
- **Model:** Opus
- **Repository:** `eric-thomas-dagster/dagster-demo-factory`
- **Environment:** `dagster-demo-factory`
- **Trigger:** Schedule → Daily → 2:00am your time
- **Connectors:** **Gmail only.** It doesn't need Drive or Calendar — Recon
  already wrote everything into the brief. Keeping the surface small matters
  more here because this routine runs `git push` and `dagster-cloud deploy`.

Add an **API trigger** too (Edit routine → Add another trigger → API →
Generate token). That gives you a `curl` to fire a build on demand when an
opportunity lands mid-day:

```bash
curl -X POST https://api.anthropic.com/v1/claude_code/routines/<trigger_id>/fire \
  -H "Authorization: Bearer <token>" \
  -H "anthropic-beta: experimental-cc-routine-2026-04-01" \
  -H "anthropic-version: 2023-06-01" \
  -H "Content-Type: application/json" \
  -d '{"text": "Build for Acme Health now, demo moved up to tomorrow 10am"}'
```

Routine 2's prompt explicitly opts into reading that payload. It has to —
fire text arrives wrapped as untrusted data and is ignored unless the saved
prompt references it.

---

## Step 5 — Optional cleanup routine

One-line routine, weekly, Sundays:

> Read `state/ledger.json` in dagster-demo-factory. For every entry with a
> `demo_date` more than 21 days in the past and `status: "deployed"`, run
> `dagster-cloud deployment delete-location --location-name <location>` against
> `$DAGSTER_CLOUD_ORGANIZATION` / `$DAGSTER_CLOUD_DEPLOYMENT`, then set that
> entry's status to `"reaped"` and commit. Email me the list of what you removed.

Without this, you'll have 40 code locations by Q4.

---

## Step 6 — Dry run before trusting it

1. Put a fake demo on your calendar for 3 days out titled
   `Demo — Northwind Logistics`.
2. Drop a short doc in Drive named `Northwind Logistics — Discovery`.
3. Routine 1 → **Run now**. Read the transcript. Check the brief it committed.
4. Routine 2 → **Run now**. Read the transcript.

A green run status only means the session exited without an infrastructure
error — it does **not** mean the build worked. Open the run and read what
actually happened, especially blocked network requests and `dg check defs`
output.

---

## Known sharp edges

**`dagster-component init` does not create a project.** Despite what you'd
expect from the name, `init` only writes `CLAUDE.md` / `.cursorrules` /
`copilot-instructions.md` and injects the `dagster_dg_cli.registry_modules`
entry point + editable install into a project that already exists. The
scaffolding step is `create-dagster project`. The routine prompt has the
order right; just don't "fix" it later.

**The CLI README is stale.** It documents `add`/`search`/`info`/`list`/
`remove`/`update` but the installed package (0.8.15) also has `init`,
`sync-deps`, and `analyze-schedules`.

**Docker isn't available in the sandbox.** `dagster-cloud serverless deploy`
builds a Docker image and will fail. `deploy-python-executable
--build-method local` builds PEX files with no Docker. The catch: PEX-local
can only build wheels, so a dependency that's sdist-only will fail the build.
The prompt handles this by falling back to pinning a wheel-available version
or dropping the dep.

**Push permissions.** Claude pushes to `claude/`-prefixed branches freely.
Pushes to other branches get rejected if the branch is protected, has someone
else's open PR, or carries commits by another author. The prompt uses
`claude/demo-<prospect>-<date>` and opens a PR, which stays inside the safe path.

**Registry size.** 975 components across 18 categories. The prompt tells Claude
to `search` the registry rather than recall component IDs from memory — there
are too many for guessing to be reliable.
