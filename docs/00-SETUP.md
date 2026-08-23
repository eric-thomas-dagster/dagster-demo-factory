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

There is **no settings page and no direct URL** for this. The only way in is
the environment selector:

1. Go to `claude.ai/code`.
2. In the row **above the message box**, click the **cloud icon** showing the
   current environment name (probably says "Default").
3. Hover the environment you created → click the **gear icon** on the right.
   (Or **Add cloud environment** if you haven't made it yet.)
4. The dialog has four fields: Name, Network access, Environment variables,
   Setup script.

You can also reach the same dialog from inside the routine editor — the cloud
icon sits just below the Instructions box.

**Environment variables** is a single text box in `.env` format, one
`KEY=value` per line. No quotes needed. Paste this:

```
DAGSTER_CLOUD_ORGANIZATION=ericthomas-dagster
DAGSTER_CLOUD_DEPLOYMENT=prod
DAGSTER_CLOUD_API_TOKEN=user:your-fresh-token-here
```

Add `GH_TOKEN=ghp_...` **only** if you want the optional standalone-repo-per-
prospect behavior — see the GH_TOKEN note below, it's not a free win.

Three things about this box:

- **Don't quote values, and avoid `#`.** In an unquoted value a `#` starts a
  comment and the rest of the line is silently dropped. Dagster+ tokens don't
  contain `#`, but a password might.
- **Values are copied once at session start.** Editing them affects the *next*
  session, not one already running. Edit before 2am, not at 2:05.
- **There's no secrets store, and the dialog says so.** Anyone with access to
  the environment can read these. It's your personal environment on your own
  account, so the practical exposure is you — but it's why the token needs to
  be one you're willing to rotate casually.

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
curl -LsSf https://astral.sh/uv/install.sh | sh || true
export PATH="$HOME/.local/bin:$PATH"
uv tool install dagster-cloud || true
uv tool install dagster-dg-cli || true
# pex is required by `deploy-python-executable --build-method local`.
# Its absence cost a full deploy cycle on 2026-08-24.
uv tool install pex || true
pip install --break-system-packages pex build || true
exit 0
```

The result is cached, so this doesn't re-run every session.

Three constraints on setup scripts: it must **exit zero** (a non-zero exit means
the session fails to start — append `|| true` to anything non-critical), finish
in **under ~5 minutes**, and it **cannot see your environment variables**.

That last one is a real trap. Values in the Environment variables box are
injected into the *session shell*, not into the setup script — they read as
empty there, with no error. The script above deliberately needs no secrets. If
you ever extend it to do something authenticated, that's the thing that will
bite you.

### The `GH_TOKEN` decision

Leave it unset and GitHub still works: cloud sessions authenticate through a
GitHub proxy that keeps your real credentials outside the VM, and `gh` works
without `gh auth login`. But `GH_TOKEN` then reads as the literal placeholder
string `proxy-injected`, and the proxy only reaches **repositories attached to
the session**.

So with the proxy, `gh repo create` for a brand-new prospect repo will 403.
Two options:

- **Skip it** (recommended to start). The monorepo PR into
  `dagster-demo-factory` is the real deliverable; standalone repos are a nice-
  to-have. Routine 2 detects the placeholder and skips silently.
- **Set a real `GH_TOKEN`** (classic PAT with `repo` scope). Then it passes
  through unchanged and repo creation works — at the cost of a long-lived
  credential sitting in a field with no secrets store.

---

### On `prod`

`ericthomas-dagster` is a playground org and `prod` is just its default
deployment, so demos land there. Location names are `demo-<prospect>`, which
keeps them findable — add the cleanup routine in Step 5 or you'll have 40 of
them by Q4.

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

**Prefer PEX over Docker for deploys.** `dagster-cloud serverless deploy` builds
a Docker image; `deploy-python-executable --build-method local` builds PEX files
and is much faster, which matters in a time-boxed unattended run. Docker *is*
pre-installed in the sandbox, so the Docker path is a genuine fallback rather
than a dead end — worth reaching for if PEX fails. The PEX catch: `--build-method
local` can only bundle packages that publish wheels, so a source-only dependency
fails the build. The prompt handles that by pinning to a wheel-publishing
version, dropping the dep, or falling back to `serverless deploy`.

**Push permissions.** Claude pushes to `claude/`-prefixed branches freely.
Pushes to other branches get rejected if the branch is protected, has someone
else's open PR, or carries commits by another author. The prompt uses
`claude/demo-<prospect>-<date>` and opens a PR, which stays inside the safe path.

**Registry size.** 975 components across 18 categories. The prompt tells Claude
to `search` the registry rather than recall component IDs from memory — there
are too many for guessing to be reliable.
