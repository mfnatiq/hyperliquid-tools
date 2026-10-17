## Hyperliquid Tools

### Running

`streamlit run home.py`

Tested with python 3.13

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

Last Updated: 2026-04-19
