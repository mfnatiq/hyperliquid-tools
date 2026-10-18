## Hyperliquid Tools

### Running

`streamlit run home.py`

Tested with python 3.13

### Live liquidity (work in progress)

`src/live_books/` is the base for a live cross-venue liquidity page. It is not wired into any page yet, so the existing
`pages/liquidity_analysis.py` is unchanged.

- `book.py` builds the cumulative quantity and notional of each book side once per update. A clip size is then one binary
  search, so adding clip sizes costs almost nothing. The result matched the old `calculate_slippage` to 0.005 bps on 2,400
  random clip prices, and the old function takes about 4 times longer to build and 30 times longer per set of clip prices
  on a 500-level book (measured on one machine, treat as a rough guide).
- `history.py` keeps price history in fixed-size ring buffers, so the oldest value is overwritten and memory never grows.
  The fine ring is 1s steps for 1h and the coarse ring is 10s steps for 3d, stored as float32. For 36 series (6 venues times
  6 tokens) with mid plus buy and sell VWAP at one clip, that is about 11 MB for the 3-day ring.
- `trading-tools` is installed from `requirements.txt` at a pinned commit and supplies the venue adapters and websocket
  feeds (Hyperliquid, Extended, RISEx, Phoenix). The Arb Opportunities page uses it today. Planned next: a background store
  with REST fallback per market, and the live liquidity page. Variational and TxFlow would be REST only.

Tests run with `pytest tests` and need `numpy` and `pytest`. They do not touch the network.

### Arb Opportunities (funding screener)

`pages/arb_opportunities.py` ranks cross-venue funding carry, using the `trading-tools` scanner (`src/funding_arbs.py`).
Venues: Phoenix, Variational, RISEx, Hyperliquid, Extended, Pacifica, Nado, QFEX, Ondo Perps and Arcus.

- **Table.** One compact line per pair: net APR, sustainability, spread risk, break-even, open interest and 24h volume.
  Net APR is the carry after the round trip cost and adverse basis for a 3 day hold, as a yearly rate. Volume and open
  interest show only where a venue reports them.
- **Detail.** Tick the box at the left of a row to see the 24h, 7d and 30d carry averages, the fee and slippage of each leg, the mid basis and
  the volume and open interest of both legs. `src/arb_view.py` draws it.
- **No loading screen.** `src/snapshot_cache.py` builds the venue snapshot in a background thread. The page shows a note
  and polls every 3 seconds until the first build is ready, then refreshes the order books every 2 minutes. An old
  snapshot is served while a new one builds.
- **History.** `src/funding_history.py` stores one funding sample per venue, ticker and hour in the table
  `funding_samples` (Postgres through `DATABASE_URL` on Railway, a local sqlite file otherwise) and keeps 35 days.
  `src/arb_runtime.py` starts a recorder thread from `home.py` when the server boots. It samples once an hour and reuses
  the page's snapshot when it is warm. The 24h, 7d and 30d averages and the 30 day sustain figure come from these
  samples, so they fill in over the first month. The 7 day sustain column comes from the venues' own history. A
  database error turns recording off and the page still works.
- **Fees.** Taker fees come from trading-tools' `bot/venues.toml`. Change the pinned trading-tools commit in
  `requirements.txt` to pick up new venues or fees.

### Referrals and donations

Every page shows a sticky header and a fixed footer. `home.py` renders both from `src/utils/render_utils.py`.

- The header button opens the referral links and their codes.
- The footer shows the site author and the donation address, with a copy button.
- All links, codes and perks come from `REFERRALS`. Change them there only.
- `referral("Extended")` returns a venue's entry. The Liquidity Analysis tables use it for the Trade button, with the perk on hover.
- `venue_referral("Hyperliquid")` shows a sign-up line with the perk on the Hyperliquid pages.
- `src/auth/db_utils.py` checks trial payments against `donation_address`. Change the address only on purpose.

### Deployment

Currently deployed with Railway (referral code: https://railway.com?referralCode=_uracj)

`trading-tools` is a private repo, so the build needs a token. Create a fine-grained GitHub token with read-only
"Contents" access to `mfnatiq/trading-tools` only, and add it as a Railway service variable named `GH_TOKEN`. Railway makes
service variables available during the build, and pip fills in `${GH_TOKEN}` in `requirements.txt`. No other repo link
is needed. To pick up a new trading-tools version, change the commit sha at the end of that line.
Locally, export `GH_TOKEN` before `pip install -r requirements.txt`.

Last Updated: 2026-04-19
