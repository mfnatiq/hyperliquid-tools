"""site chrome shared by every page: a sticky header with the referral links and a fixed footer with the donation address."""
import streamlit as st

# db_utils checks trial payments against this address, so change it only on purpose.
donation_address = "0xB17648Ed98C9766B880b5A24eEcAebA19866d1d7"

# every place that shows a referral reads this list, so a link or code changes in one spot.
REFERRALS = [
    dict(venue="Variational", url="https://omni.variational.io/?ref=OMNIMAMBO", code="OMNIMAMBO"),
    dict(venue="TxFlow", url="https://app.txflow.com/r/EJFMW1T960", code="EJFMW1T960"),
    dict(venue="Phoenix", url="https://phoenix.trade/?code=O88RSK0C", code="O88RSK0C"),
    dict(venue="RISEx", url="https://www.rise.trade/invite/mambo", code="mambo"),
    dict(venue="Extended", url="https://app.extended.exchange/join/MAMBO", code="MAMBO"),
    dict(venue="Nado", url="https://app.nado.xyz/?join=q75b4tC", code="q75b4tC"),
    dict(venue="Pacifica", url="https://app.pacifica.fi/?referral=pacifica", code="pacifica"),
]


def referral_url(venue):
    """the referral link for a venue name, or None. a table or button that names a venue can link through it.
    a trailing " (RFQ)" tag, as the liquidity tables add, is ignored."""
    venue = venue.removesuffix(" (RFQ)").lower()
    return next((r["url"] for r in REFERRALS if r["venue"].lower() == venue), None)


def site_css():
    """keeps the header (st.container key="site_header") stuck below Streamlit's toolbar and the footer
    (key="site_footer") fixed at the bottom. both need an opaque background, so it follows the viewer's theme."""
    bg = "#ffffff" if st.context.theme.type == "light" else "#0e1117"
    st.markdown(f"""<style>
    /* sticky goes on the wrapper Streamlit puts around the container; the container itself can't move inside it */
    [data-testid="stLayoutWrapper"]:has(> .st-key-site_header) {{ position: sticky; top: 3.75rem; z-index: 990; background: {bg}; }}
    .st-key-site_footer {{ position: fixed; left: 0; right: 0; bottom: 0; z-index: 990; background: {bg};
        padding: 0.5rem 1rem; border-top: 1px solid rgba(128, 128, 128, 0.2); }}
    .st-key-site_footer p, .st-key-site_footer code {{ font-size: 0.95rem; }}
    .block-container {{ padding-bottom: 6rem !important; }}  /* so the footer never covers the end of a page */
    [data-testid="stPopoverBody"] p, [data-testid="stPopoverBody"] li {{ font-size: 1.05rem; }}
    </style>""", unsafe_allow_html=True)


def referrals_popover():
    """one button for the page header. opens the referral links, each with its code."""
    with st.popover("🎁 Referrals"):
        st.markdown("Signing up through these links supports the site")
        st.markdown("\n".join(f"- [{r['venue']}]({r['url']}) &nbsp; `{r['code']}`" for r in REFERRALS))


def site_footer():
    """the fixed footer: who made the site and the donation address."""
    with st.container(key="site_footer", horizontal=True, horizontal_alignment="center", vertical_alignment="center"):
        st.markdown("made by [@fnatiqmambo](https://x.com/fnatiqmambo) &nbsp;•&nbsp; donations:", width="content")
        st.code(donation_address, language=None, width="content")    # st.code has a copy button, so no custom JavaScript is needed


def venue_referral(venue):
    """a one-line sign-up link for a page about one venue, or nothing if that venue has no referral."""
    url = referral_url(venue)
    if url:
        st.caption(f"New to {venue}? [Sign up here]({url}) to support the site")
