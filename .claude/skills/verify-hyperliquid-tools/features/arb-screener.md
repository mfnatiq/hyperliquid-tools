# Arb screener (`pages/arb_opportunities.py`)

## Sub-features
- Loading status while the first build runs, then one line per pair: pair and route, verdict pill, net APR, expected APR, break-even, sustain, risk, OI and volume.
- Filters (hide losing pairs, hold days, notional), sort (Verdict, Edge, Carry, Break-even), and the phone layout (under 640 px it shows pair, verdict, Details and net APR).

## How to get to it (user POV)
Open the site and pick "💱 Arb Opportunities" in the sidebar (route `/arb_opportunities`). The "loading venues" caption polls every 3 s until rows appear.

## Driving it with Playwright
`drive_arb.py` does the desktop path. For the phone layout, open a second page with `viewport={"width": 390, "height": 800}` and assert that `.st-key-arb_rows [data-testid="stColumn"]:nth-child(2)` is hidden. Rows are `Details ›` buttons, with keys like `d|ZRO|PHX|VAR`.

## Gotchas
- The first build is slow, so wait up to 4 minutes. Rows are live: assert their shape (labels, verdict ordering), never specific tickers or numbers.
- Expected APR is net APR times sustain, and sustain caps the verdict (under 80% is at most OK, under 50% is MARGINAL, no figure is at most OK). The literal values are asserted in `tests/test_arb_page.py`, so use the live run as the visual proof only.
