## Hyperliquid Tools

### Running

`streamlit run home.py`

Tested with python 3.13

### Referrals and donations

The header has one "Referrals & support" button on every page. It opens the referral links with their codes and the
donation address (with a copy button). The links live in one list, `REFERRALS` in `src/utils/render_utils.py`, so a
link or code changes in one place. `referral_url("Extended")` returns a venue's link, so a table or button that names a
venue can link through it. The old fixed footer and its copy script are gone. `donation_address` is unchanged and
`src/auth/db_utils.py` still checks trial payments against it.

### Deployment

Currently deployed with Railway (referral code: https://railway.com?referralCode=_uracj)

Last Updated: 2026-04-19
