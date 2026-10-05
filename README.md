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
- Planned next: install `trading-tools` as a dependency for the venue adapters and websocket feeds (Hyperliquid, Extended,
  RISEx, Phoenix), a background store with REST fallback per market, and the page itself. Variational and TxFlow would be
  REST only.

Tests run with `pytest tests` and need `numpy` and `pytest`. They do not touch the network.

### Deployment

Currently deployed with Railway (referral code: https://railway.com?referralCode=_uracj)

Last Updated: 2026-04-19
