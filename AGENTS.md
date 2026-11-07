# Rules for coding agents in this repo

This is a Streamlit site on Railway. It shows Hyperliquid and other perp venue data and the Arb Opportunities screener.
It never places orders. Claude Code reads this file through CLAUDE.md. Cursor and other agents read it directly.

## Hard rules

1. Never put a key, token or address list in code, tests, logs or commit messages. Keys live in the environment.
2. Funding and fee facts come from trading-tools (`bot/venues.toml` and `funding_scan.py`). Do not copy a venue fact into this repo.
3. `requirements.txt` pins streamlit to 1.48.0. Do not bump it without running the page tests.
4. `requirements-trading-tools.txt` pins trading-tools to one commit. Bump the sha only on purpose, in its own change.
5. A database error must never stop the site. Recording and sustain fallbacks turn off quietly.
6. Tests never touch the network. New behaviour gets a test that asserts a literal expected value.
7. Every change also updates README.md, so it describes everything the site does.

## Where things live

- `home.py` starts the app. `pages/arb_opportunities.py` is the screener. `src/arb_view.py` draws its detail panel.
- `src/funding_arbs.py` wraps trading-tools' scanner and grades rows. `apply_sustain` caps each verdict by sustain.
- `src/funding_history.py` records hourly funding samples (Postgres through `DATABASE_URL`, sqlite locally) and fills
  sustain and flip counts for pairs whose venues have no history.
- `src/arb_runtime.py` holds the shared snapshot cache and the recorder thread.
- `tests/` is the offline suite. The page tests use Streamlit's AppTest with a fake snapshot.

## Before you open a pull request

Run `python -m pytest -q` and `mypy . --ignore-missing-imports --exclude venv`. CI runs mypy only, so the tests are on you.
Install trading-tools first (`pip install -e ../trading-tools`), or the page tests fail. Review with `/pstack:poteto-mode`.
Railway needs the `GH_TOKEN` service variable to install trading-tools. Check README.md for the other variables.
