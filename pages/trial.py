from main import display_upgrade_section
from src.consts import acceptedPayments
import streamlit as st

from src.auth.db_utils import PremiumType, get_user, get_user_premium_type
from pages.main import show_login_info

st.set_page_config(
    'Trial Details',
    "⏳",
    layout="wide",
)

st.title("Trial Details")

# setup and configure logging
import logging
logging.basicConfig(
    format="%(asctime)s [%(levelname)s] %(message)s",
    level=logging.INFO,
)
logger = logging.getLogger(__name__)

if "user_email" in st.session_state:
    # display dynamic user status (premium, trial, expired)
    user = get_user(st.session_state['user_email'], logger)
    status_message = ""

    st.markdown(
        f"Logged in as **{st.session_state['user_email']}** {status_message}",
        width="content",
        unsafe_allow_html=True
    )

    stablesAmountToPay = acceptedPayments['USD₮0']['minAmount']

    if user:
        user_premium_type = get_user_premium_type(st.session_state['user_email'], logger)

        if user_premium_type == PremiumType.FULL:
            st.text("Thanks for subscribing!")
        elif user_premium_type == PremiumType.TRIAL:
            st.text(f'Your trial ends at {user.trial_expires_at.strftime('%Y-%m-%d')}')
            st.text(f"""
                After the trial ends, you can maintain premium access forever with a :green[one-time payment] of

                :green[{stablesAmountToPay} USD₮0] (or approximate equivalent in HYPE)
            """)
        else:
            display_upgrade_section('trial_page')

else:
    st.text("Please log in to view this page")
    show_login_info(show_button_only=True)