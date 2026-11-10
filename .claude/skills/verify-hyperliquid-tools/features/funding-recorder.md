# Funding recorder and history (`src/arb_runtime.py`, `src/funding_history.py`)

## Sub-features
Hourly `funding_samples` rows (venue, ticker, hour), 35-day retention, the sustain fallback for venues with no history (Variational, TxFlow), and quiet shutdown on a database error.

## How to get to it (user POV)
Invisible. It starts when `home.py` boots. Users see its effect as the averages and sustain filling in.

## Driving it with Playwright
Not through the browser. Launch, wait about 90 s, then check `server.log` for `funding recorder: stored N samples` and run `select count(*) from funding_samples` on the scratch db. `drive_arb.py` does this when given the db path.

For the failure path, launch with an unwritable `DATABASE_URL`: the log shows `funding history is off`, and the Arb page must still show rows.

## Gotchas
- The sustain fallback needs 24 shared recorded hours, so a fresh run can't show it. Cover it with `tests/test_funding_history.py`.
