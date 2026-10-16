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
        padding: 0.75rem 1rem; gap: 0.5rem; border-top: 1px solid rgba(128, 128, 128, 0.2); }}
    .st-key-site_footer p {{ font-size: 0.9rem; margin: 0; }}
    /* Streamlit gives markdown a -1rem bottom margin, which pushes the text below the address pill's centre */
    .st-key-site_footer [data-testid="stMarkdownContainer"] {{ margin-bottom: 0; }}
    .st-key-site_footer .sep {{ margin: 0 15px; }}
    /* a small pill like the old footer's address, with room on the right for st.code's copy button */
    .st-key-site_footer pre {{ padding: 0.45rem 2.25rem 0.45rem 0.6rem !important; margin: 0; border-radius: 5px; }}
    .st-key-site_footer pre code {{ font-size: 0.85rem; }}
    .block-container {{ padding-bottom: 5rem !important; }}  /* so the footer never covers the end of a page */
    [data-testid="stPopoverBody"] p, [data-testid="stPopoverBody"] li {{ font-size: 1.05rem; }}
    /* the header's referrals button is a green pill so it reads as an offer, not a menu */
    .st-key-site_header [data-testid="stPopover"] button {{ background: rgba(110, 231, 120, 0.08);
        border: 1px solid rgba(110, 231, 120, 0.45); border-radius: 0.75rem; padding: 0.4rem 0.9rem; }}
    .st-key-site_header [data-testid="stPopover"] button:hover {{ border-color: rgb(110, 231, 120); }}
    .st-key-site_header [data-testid="stPopover"] button p {{ font-size: 1rem; }}
    </style>""", unsafe_allow_html=True)


def referrals_popover():
    """one button for the page header. opens the referral links, each with its code."""
    with st.popover("🎁 Sign up with our :green[**referral codes**]"):
        st.markdown("Signing up through these links supports the site")
        st.markdown("\n".join(f"- [{r['venue']}]({r['url']}) &nbsp; `{r['code']}`" for r in REFERRALS))


def site_footer():
    """the fixed footer: who made the site and the donation address."""
    with st.container(key="site_footer", horizontal=True, horizontal_alignment="center", vertical_alignment="center"):
        st.markdown('made by [@fnatiqmambo](https://x.com/fnatiqmambo)<span class="sep">•</span>donations:',
                    width="content", unsafe_allow_html=True)
        st.code(donation_address, language=None, width="content")    # st.code has a copy button, so no custom JavaScript is needed


def venue_referral(venue):
    """a one-line sign-up link for a page about one venue, or nothing if that venue has no referral."""
    url = referral_url(venue)
    if url:
        st.caption(f"New to {venue}? [Sign up here]({url}) to support the site")
