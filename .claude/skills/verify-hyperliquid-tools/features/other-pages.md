# Other pages (smoke only)

## Sub-features
Unit Dashboard (default), Kinetiq Dashboard, HIP-4 Markets, Trial Details, Liquidity Analysis.

## How to get to it (user POV)
Sidebar links. The default page loads at `/`.

## Driving it with Playwright
Open each route and assert the page shows no Streamlit exception (`[data-testid="stException"]` count is 0) and that the "Hyperliquid Tools" header is present. These pages are not mapped in depth. Add a file here when a change touches one.

## Gotchas
- Trial Details and the auth flow need the `[auth]` secrets in `.streamlit/secrets.toml`. Never print or commit them, and do not drive a login.
