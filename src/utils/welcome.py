"""the welcome banner's "Don't show again" choice. no login is needed, so a browser cookie keeps it."""
import streamlit as st

from src.utils.render_utils import TWITTER_URL

WELCOME_COOKIE = "hide_welcome"
ONE_YEAR_S = 365 * 24 * 3600

FOLLOW_TEXT = f"If you like what you see, please [follow me on Twitter]({TWITTER_URL}). And if you use my referral links, thank you!"

# runs in a zero-height component frame and writes the cookie on the parent page, like PAGEVIEW_LOADER
REMEMBER_JS = f"""
<script>
window.parent.document.cookie = "{WELCOME_COOKIE}=1; max-age={ONE_YEAR_S}; path=/; SameSite=Lax";
</script>
"""


def welcome_hidden() -> bool:
    """return True when this browser asked to hide the welcome banner. the cookie is read once per new session."""
    return st.context.cookies.get(WELCOME_COOKIE) == "1"
