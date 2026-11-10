---
name: verify-hyperliquid-tools
description: Drive the real Hyperliquid Tools Streamlit site (home.py, mainly the Arb Opportunities screener) in system Chrome through Playwright, with a throwaway sqlite db, to prove a UI or recorder change works. Use after changing pages/, src/arb_*.py, src/funding_*.py or the recorder, beyond what pytest's AppTest shows.
---

# Verify Hyperliquid Tools

The site only reads data. It never places orders. The Arb page pulls live public venue data through trading-tools, so a drive makes real read-only network calls. Tests must never do that (AGENTS.md rule 6), so this skill is for proof runs only. Run `python -m pytest -q` first for the offline checks.

All commands run from the repo root in Git Bash. Scripts are in `.claude/skills/verify-hyperliquid-tools/scripts/`.

## Launch
```
bash .claude/skills/verify-hyperliquid-tools/scripts/launch.sh
```
Starts `streamlit run home.py` on port 8599 (override with `VERIFY_PORT`), headless, with `DATABASE_URL=sqlite:///<scratch>/verify.db`, which starts empty. Prints `READY` when `/_stcore/health` answers. Ready means the shell is up. The Arb table needs the first venue build, usually 1 to 3 minutes, so drive scripts wait for rows. The recorder thread first samples 60 s after boot, then hourly.

Prerequisites: `venv/Scripts/pip install -e ../trading-tools` and `venv/Scripts/pip install playwright` (it uses the installed Chrome, no browser download). Without trading-tools the Arb page shows "unavailable" and stops.

Scratch and evidence live in `$VERIFY_OUT` (default `$TMPDIR/hl-tools-verify`). Only one instance can run: launch.sh refuses if the port is taken.

## Doctor
`bash .claude/skills/verify-hyperliquid-tools/scripts/doctor.sh` is read-only. It checks that our pid is alive, that health answers, that streamlit is 1.48.0, that trading-tools imports and that playwright is present. Run it first whenever anything looks off.

## Drive
```
venv/Scripts/python .claude/skills/verify-hyperliquid-tools/scripts/drive_arb.py <port> <evidence_dir> <verify.db path>
```
Opens `/arb_opportunities`, waits for `Details ›` buttons, screenshots the table, presses the first Details, screenshots the drawer (`div[data-testid="stDialog"] div[role="dialog"]`), asserts its labels, then waits for `funding_samples` rows in the scratch db. Pass Windows-style paths (`$(cygpath -m /tmp)/...`), because this python is native Windows. For another feature, copy that script and keep to stable handles: button names, `st-key-*` container classes (`.st-key-arb_rows`), `data-testid`. Per-feature recipes are in `features/`.

## Evidence
A proof is: the screenshots in `<evidence_dir>`, the script's `OK` lines, the `funding recorder: stored N samples` line in `server.log`, and the row count in the scratch db. Drive the page through the real UI. Do not set session state or call `arb_runtime` internals to get there. For a change in grading or numbers, also compare the visible drawer or table figures to the change, not only that the page loads. A "No history yet" panel on a fresh db is correct, not a failure.

## Cleanup
`bash .claude/skills/verify-hyperliquid-tools/scripts/cleanup.sh` stops only the server launch.sh started (by its saved pid, mapped to the Windows pid) and deletes the scratch db. It keeps `evidence/`. Run it after every failed attempt too. Never kill by process name.

## Gotchas
- A local `../trading-tools` checkout can differ from the commit pinned in `requirements-trading-tools.txt`. Say which one you proved against.
- A database failure turns recording off but the site keeps working (the log says `funding history is off`). An unwritable `DATABASE_URL` looks exactly like that, so check the log, not just the page.
- AppTest cannot rerun the page's segmented control, which is why `tests/test_arb_page.py` rebuilds the session. A real browser has no such limit.
- The skill is dev tooling and changes no site behavior, so it does not touch README.md.
