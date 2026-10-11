"""referral links and the donation address, shown from one popover in the page header."""
import streamlit as st

# db_utils checks trial payments against this address, so change it only on purpose.
donation_address = "0xB17648Ed98C9766B880b5A24eEcAebA19866d1d7"

# every place that shows a referral reads this list, so a link or code changes in one spot.
REFERRALS = [
    dict(venue="Variational", url="https://omni.variational.io/?ref=OMNIMAMBO", code="OMNIMAMBO"),
    dict(venue="TxFlow", url="https://app.txflow.com/r/EJFMW1T960", code="EJFMW1T960"),
    dict(venue="TxFlow (alt)", url="https://app.txflow.com/r/I6B5D3PAAL", code="I6B5D3PAAL"),
    dict(venue="Phoenix", url="https://phoenix.trade/?code=O88RSK0C", code="O88RSK0C"),
    dict(venue="RISEx", url="https://www.rise.trade/invite/mambo", code="mambo"),
    dict(venue="Extended", url="https://app.extended.exchange/join/MAMBO", code="MAMBO"),
    dict(venue="Nado", url="https://app.nado.xyz/?join=q75b4tC", code="q75b4tC"),
    dict(venue="Pacifica", url="https://app.pacifica.fi/?referral=pacifica", code="pacifica"),
    dict(venue="Bullpen", url="https://bullpen.fi/@bitcoin", code="@bitcoin"),
]


def referral_url(venue):
    """the referral link for a venue name, or None. a table or button that names a venue can link through it."""
    return next((r["url"] for r in REFERRALS if r["venue"].lower() == venue.lower()), None)


def support_popover():
    """one button for the page header. opens the referral links (with the code to copy) and the donation address."""
    with st.popover("🎁 Referrals & support"):
        st.caption("Signing up through these links supports the site. The code is there if you sign up by hand.")
        st.markdown("\n".join(f"- [{r['venue']}]({r['url']}) &nbsp; `{r['code']}`" for r in REFERRALS))
        st.divider()
        st.caption("Donations")
        st.code(donation_address, language=None, wrap_lines=True)           # st.code has a copy button, so no custom JavaScript is needed
        st.caption("made by [@fnatiqmambo](https://x.com/fnatiqmambo)")
