# LEARNINGS

Verified facts from previous Demo Factory runs. Read before building; update
after every run (step 10 of the Demo Factory prompt).

**Only verified facts.** Confirmed by running it or reading source. A wrong
entry is worse than none — the next run will trust it.

Keep under ~120 lines. When it grows past that, delete the least useful entries
rather than letting it sprawl. No prospect-specific content — tooling facts
here, prospect facts in the brief.

---

## Deployment packaging — READ BEFORE DEPLOYING

The 2026-08-24 Northwind run took **seven deploy attempts**, all packaging, all
preventable. Run `./scripts/preflight_deploy.sh <slug>` before the first deploy;
it checks every item below in about 20 seconds.

- **`pex` must be installed** in the environment or `--build-method local`
  fails immediately. (2026-08-24)
- **`dagster-cloud` must be a project dependency**, not just a CLI tool on
  PATH. Add it to `pyproject.toml` dependencies. (2026-08-24)
- **`--package-name` must point at the module that actually holds
  `Definitions`**, not the project directory name. Verify with
  `python -c "import <pkg>"` before deploying. (2026-08-24)
- **`dbt_project/` must live INSIDE the Python package directory.** If it sits
  at the project root it is not included in the wheel and the location fails to
  load in Dagster+ with a confusing path error. (2026-08-24)
- **`.gitignore`d files do not ship in the wheel.** dbt `target/manifest.json`
  and Dagster defs-state files are commonly gitignored, silently excluded, and
  then missing at runtime. Force-include them explicitly via
  `[tool.hatch.build.targets.wheel]` (or setuptools package-data). (2026-08-24)
- **Run `dg utils refresh-defs-state` before deploying** when using
  state-backed components (Fivetran, dbt). Without generated state the location
  fails to load remotely even though it works locally. (2026-08-24)
- **Verify the wheel contents rather than trusting config.** `python -m build`
  then `unzip -l dist/*.whl | grep -E "manifest|defs_state"`. This catches the
  packaging class of failure in seconds instead of a multi-minute deploy cycle.
  (2026-08-24)

## Deployment — waiting

- The Dagster+ serverless agent sync step routinely takes **several minutes**
  after PEX upload completes. This is normal, not a hang. (2026-08-24)
- **Do not poll in a tight loop.** The 2026-08-24 run burned many turns on
  "still syncing, I'll wait" cycles. Use a blocking wait on the background task
  (`TaskOutput`) or `sleep 60` between checks. (2026-08-24)
- A successful deploy command does **not** mean the code location loaded.
  Confirm independently with `dg api` and look for status `LOADED`. (2026-08-24)
- Use `deploy-python-executable ... --build-method local` (PEX). Docker *is*
  available in the sandbox, so plain `serverless deploy` is a genuine fallback
  if PEX fails on a source-only dependency. (2026-08-23)

## Commands and flags

- **Briefs and `state/ledger.json` must live on `main`.** Factory reads both
  from the default branch; anything on an unmerged `claude/` branch is invisible
  and the run becomes a silent no-op. Recon commits state directly to `main`;
  only demo *code* goes through a PR. (2026-08-24)

- **Use `dg`, never the legacy `dagster` CLI.** `dg dev` not `dagster dev`;
  `dg launch --assets '*'` not `dagster asset materialize --select '*'`. Note
  the flag differs too — `dg launch` takes `--assets`, the old CLI took
  `--select`. Verified against dagster-dg-cli 1.13.19. (2026-08-24)
- `dg launch` options: `--assets`, `--job`, `--partition`,
  `--partition-range <start>...<end>`, `--config` / `--config-json`. Use
  `--partition` for the single-partition recovery step in demos. (2026-08-24)

- `dagster-component init` does **not** scaffold a project. It writes AI-tool
  config and injects the `dagster_dg_cli.registry_modules` entry point +
  editable install into a project that already exists. Run `create-dagster
  project` first. (2026-08-23)
- Always pass `--auto-install` to `dagster-component init` and
  `dagster-component add`. Without it they prompt and hang forever unattended.
  (2026-08-23)
- The `dagster-community-components-cli` README is stale: it documents only
  `add`/`search`/`info`/`list`/`remove`/`update`. Installed package 0.8.15 also
  has `init`, `sync-deps`, `analyze-schedules`. Check `--help`. (2026-08-23)
- If `dg list components` doesn't show custom components, the registry entry
  point didn't wire — re-run `dagster-component init --force`. (2026-08-23)
- `components/__init__.py` must re-export each component class or the Dagster UI
  Components tab won't list them even when `dg list components` does.
  (2026-08-24)

## Build sequencing that worked

- Smoke-test **one partition** of the first ingestion asset before building
  anything else, and inspect the DuckDB table to confirm the write landed. This
  caught path issues early on 2026-08-24 and is cheap. (2026-08-24)
- Run `dg check defs` after each layer (ingestion → SaaS → dbt), not once at the
  end. Failures localize instead of compounding. (2026-08-24)
- Set `profiles.yml` to require the env var with no relative-path fallback —
  a fallback default masks packaging errors locally that then fail in Dagster+.
  (2026-08-24)

## Environment

- `GH_TOKEN` reads as literal `proxy-injected` when the GitHub proxy handles
  auth. Not a usable token — treat as unset. (2026-08-23)
- Gmail exposes `create_draft` but no send, and routines run without approval
  prompts. Draft + mobile push; never report the missing send as a failure.
  (2026-08-23)
- Cloud environment variables are **not** visible to the setup script — session
  shell only. (2026-08-23)

## Registry gaps

- No component covers generic REST / carrier-rate APIs. Searched "rest api",
  "http polling", "rate limit". Wrote a custom `CarrierRateFeedComponent`.
  Worth contributing back to the registry. (2026-08-24)

## Dead ends — don't retry these

- Do not put `dbt_project/` at the project root. It will not ship. (2026-08-24)
- Do not rely on `.gitignore`d build artifacts being present at runtime.
  (2026-08-24)
