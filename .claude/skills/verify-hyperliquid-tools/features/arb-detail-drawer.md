# Arb detail drawer (`src/arb_view.py`)

## Sub-features
Net APR, carry now, break-even, round trip cost, Trade links (referral), carry averages (24h, 7d, 30d), the 7-day flip count, cost per leg, mid basis, and volume and OI of both legs.

## How to get to it (user POV)
Press `Details ›` on any row. A drawer pinned to the right opens (full screen on phones).

## Driving it with Playwright
`pg.get_by_role("button", name="Details ›").first.click()`, then read `div[data-testid="stDialog"] div[role="dialog"]`. `drive_arb.py` asserts the labels Net APR, Break-even, Round trip, Carry averages and Cost per leg, and screenshots the drawer.

## Gotchas
- On a fresh scratch db the averages read "No history yet, one sample is added an hour". The 24h, 7d and 30d figures appear only once the recorder has stored hours, so verify them against a db that already has samples.
