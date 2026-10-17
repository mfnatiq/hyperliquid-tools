"""the header, the footer and the referral helpers that every page uses. the footer holds the donation address."""
import html

import streamlit as st

# db_utils checks trial payments against this address, so change it only on purpose.
donation_address = "0xB17648Ed98C9766B880b5A24eEcAebA19866d1d7"

# every referral on the site comes from this list, so change a link, code or perk here only.
# perk is what a new user gets for signing up with the code. leave it out when unknown.
REFERRALS = [
    dict(venue="Hyperliquid", url="https://app.hyperliquid.xyz/join/MAMBO", code="MAMBO", perk="4% off fees"),
    dict(venue="Variational", url="https://omni.variational.io/?ref=OMNIMAMBO", code="OMNIMAMBO",
         perk="12% points boost and Silver tier for 90 days"),
    dict(venue="TxFlow", url="https://app.txflow.com/r/EJFMW1T960", code="EJFMW1T960",
         perk="5% off spot and perp fees on the first $25M volume"),
    dict(venue="Phoenix", url="https://phoenix.trade/?code=O88RSK0C", code="O88RSK0C", perk="10% off fees on the first $100M volume"),
    dict(venue="RISEx", url="https://www.rise.trade/invite/mambo", code="mambo"),
    dict(venue="Extended", url="https://app.extended.exchange/join/MAMBO", code="MAMBO", perk="10% off fees"),
    dict(venue="Nado", url="https://app.nado.xyz/?join=q75b4tC", code="q75b4tC"),
    dict(venue="Lighter", url="https://app.lighter.xyz/?referral=MAMBO&source=none", code="MAMBO",
         perk="first week of Premium free"),
    dict(venue="Pacifica", url="https://app.pacifica.fi/?referral=pacifica", code="pacifica"),
    dict(venue="QFEX", url="https://qfex.com/@rcb0b6d9", code="rcb0b6d9"),
]


def _md(text):
    """escape "$" so Streamlit markdown shows it instead of starting a maths block."""
    return text.replace("$", "\\$")


def referral(venue):
    """return the REFERRALS entry for a venue name, or None. ignore case and a trailing " (RFQ)" tag."""
    venue = venue.removesuffix(" (RFQ)").lower()
    return next((r for r in REFERRALS if r["venue"].lower() == venue), None)


def trade_button(venue):
    """return a " Trade ↗" link for a venue with a referral, or "" for one without. the perk shows on hover."""
    r = referral(venue)
    if not r:
        return ""
    tip = f' title="{html.escape(r["perk"])}"' if r.get("perk") else ""
    return f' <a class="trade" href="{html.escape(r["url"])}"{tip} target="_blank" rel="noopener">Trade ↗</a>'


def page_css(name, style):
    """render a <style> block. site_css() takes its container out of the page flow, so the block adds no gap."""
    with st.container(key=f"css_{name}"):
        st.markdown(style, unsafe_allow_html=True)


def site_css():
    """keep the header (key="site_header") below Streamlit's toolbar and the footer (key="site_footer") at the bottom.
    both need an opaque background, so the colour follows the viewer's theme."""
    bg = "#ffffff" if st.context.theme.type == "light" else "#0e1117"
    page_css("site", f"""<style>
    /* Streamlit wraps the container in a box of the same height. sticky must go on that wrapper. */
    [data-testid="stLayoutWrapper"]:has(> .st-key-site_header) {{ position: sticky; top: 3.75rem; z-index: 990; background: {bg}; }}
    .st-key-site_footer {{ position: fixed; left: 0; right: 0; bottom: 0; z-index: 990; background: {bg};
        padding: 0.75rem 1rem; gap: 0.35rem; border-top: 1px solid rgba(128, 128, 128, 0.2); }}
    .st-key-site_footer p {{ font-size: 0.9rem; margin: 0; }}
    /* Streamlit gives markdown a -1rem bottom margin. reset it so the text centres on the address box. */
    .st-key-site_footer [data-testid="stMarkdownContainer"] {{ margin-bottom: 0; }}
    .st-key-site_footer .sep {{ margin: 0 15px; }}
    /* a small box with the address in its centre. the top padding is 1px more because capitals and digits sit
    above the middle of the line. */
    .st-key-site_footer pre {{ padding: calc(0.3rem + 1px) 0.6rem 0.3rem !important; margin: 0; border-radius: 5px;
        overflow: visible; white-space: nowrap; }}
    .st-key-site_footer pre code {{ font-size: 0.8rem; line-height: 1.4; display: block; }}
    /* show the copy button of st.code just right of the box, so the box needs no room for it */
    .st-key-site_footer [data-testid="stCode"] > div:has(> [data-testid="stCodeCopyButton"]) {{ left: 100%; right: auto;
        width: auto; padding: 0; align-items: center; }}
    /* the row must not squeeze the address box, or a wider font on Windows spills out of it */
    .st-key-site_footer [data-testid="stElementContainer"] {{ flex-shrink: 0; width: auto !important; }}
    /* page_css() blocks and the script-only umami iframe still take a box and a 1rem gap, so a page with one sat
    lower. take their containers out of the page flow. */
    [data-testid="stLayoutWrapper"]:has(> [class*="st-key-css_"]),
    [data-testid="stLayoutWrapper"]:has(> .st-key-umami) {{ position: absolute; height: 0; overflow: hidden; }}
    /* less space above the title (Streamlit's toolbar is 3.75rem tall) and around the divider */
    /* the bottom padding stops the footer from covering the end of a page */
    .block-container {{ padding-top: 4.5rem !important; padding-bottom: 5rem !important; }}
    .st-key-site_header h1 {{ padding-top: 0.5rem; padding-bottom: 0.5rem; }}
    .st-key-site_header hr {{ margin: 0.5rem 0 0.75rem; }}
    [data-testid="stPopoverBody"] p, [data-testid="stPopoverBody"] li {{ font-size: 1.05rem; }}
    /* a green pill makes the referrals button look like an offer */
    .st-key-site_header [data-testid="stPopover"] button {{ background: rgba(110, 231, 120, 0.08);
        border: 1px solid rgba(110, 231, 120, 0.45); border-radius: 0.75rem; padding: 0.4rem 0.9rem; }}
    .st-key-site_header [data-testid="stPopover"] button:hover {{ border-color: rgb(110, 231, 120); }}
    .st-key-site_header [data-testid="stPopover"] button p {{ font-size: 1rem; }}
    </style>""")


def referrals_popover():
    """show one header button that opens the referral links and their codes."""
    with st.popover("🎁 Sign up with my :green[**referral codes**]"):
        st.markdown("\n".join(
            f"- [{r['venue']}]({r['url']}) &nbsp; `{r['code']}`" + (f" &nbsp; {_md(r['perk'])}" if r.get("perk") else "")
            for r in REFERRALS
        ))


def site_footer():
    """show the fixed footer with the site author and the donation address."""
    with st.container(key="site_footer", horizontal=True, horizontal_alignment="center", vertical_alignment="center"):
        st.markdown('made by [@fnatiqmambo](https://x.com/fnatiqmambo)<span class="sep">•</span>donations:',
                    width="content", unsafe_allow_html=True)
        st.code(donation_address, language=None, width="content")    # st.code has a copy button, so no JavaScript is needed


def venue_referral(venue):
    """show a one-line sign-up link for a venue. show nothing if the venue has no referral."""
    r = referral(venue)
    if r:
        perk = f" for {_md(r['perk'])}" if r.get("perk") else ""
        st.caption(f"New to {venue}? [Sign up here]({r['url']}){perk}")
