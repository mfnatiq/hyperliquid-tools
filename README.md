## Hyperliquid Tools

### Running

`streamlit run home.py`

Tested with python 3.13

### Referrals and donations

Every page shares a sticky header with a "Referrals" button and a fixed footer with "made by" and the donation address
(with a copy button). Both are rendered in `home.py` from `src/utils/render_utils.py`. The links live in one list,
`REFERRALS`, so a link or code changes in one place. `referral_url("Extended")` returns a venue's link: the Liquidity
Analysis tables show a Trade button next to each exchange name through it, and `venue_referral("Hyperliquid")` shows a sign-up line on the
Hyperliquid pages once Hyperliquid has an entry. `donation_address` is unchanged and `src/auth/db_utils.py` still checks
trial payments against it.

### Deployment

Currently deployed with Railway (referral code: https://railway.com?referralCode=_uracj)

Last Updated: 2026-04-19
