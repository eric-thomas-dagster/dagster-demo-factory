# requests/

**Drop a file here to tell the routines what to do.** Presence of a file is the
instruction; the contents refine it.

This is the highest-priority signal in the system — both routines check this
folder before anything else, ahead of the calendar, ahead of the ledger.

## Creating one

```bash
./scripts/request.sh <slug> <action> ["optional notes"]
```

Or just create the file by hand — it works from the GitHub web UI on a phone,
which is the point.

## Filename

`requests/<slug>.md` — the slug matches the ledger entry.

## Contents

```markdown
---
action: rebuild-brief      # rebuild-brief | rebuild-demo | enhance | build-poc
---

Optional free text. Anything here is an instruction to the routine and takes
precedence over what the brief currently says.

e.g. "Focus on SSIS coexistence this time. The last build ignored the legacy
side entirely. Demo shape is migration-both-states."
```

## Actions

| action | Routine | Effect |
|---|---|---|
| `rebuild-brief` | Recon | Rewrite the brief from scratch, then set the ledger to `brief-ready` |
| `rebuild-demo` | Factory | Delete the project, rebuild from the current brief |
| `enhance` | Factory | Modify the existing project in place; notes describe the change |
| `build-poc` | POC Builder | Build a POC; notes name the scenario document |

## What the routine does with it

1. Reads it before any other selection logic
2. Does what it says
3. **Moves it to `requests/done/<slug>-<YYYY-MM-DD>.md`** and commits, so it
   isn't picked up twice
4. Mentions in the notification that it was honouring a request

If several files are present, the routine handles the one whose slug has the
nearest demo date and leaves the rest for the next run.

## Why this exists

The earlier mechanism was deleting the brief file and letting the routine infer
intent from its absence. That's invisible, carries no instructions, and a
2026-08-26 run correctly found the deletion but filed it as a "secondary note"
and went looking at a different prospect instead. A file that says what you want
is unambiguous.
