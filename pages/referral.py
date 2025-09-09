import uuid
import streamlit as st

from src.auth.db_utils import PremiumType, get_user, get_user_premium_type
from pages.main import show_login_info

st.set_page_config(
    'Referral Details',
    "📣",
    layout="wide",
)

st.title("Referral Details")

# setup and configure logging
import logging
logging.basicConfig(
    format="%(asctime)s [%(levelname)s] %(message)s",
    level=logging.INFO,
)
logger = logging.getLogger(__name__)

ref_discount_percentage = 10

@st.dialog("Refer a friend!")
def show_ref_code(user):
    st.write(f"Enjoying the dashboard? Share your referral code to get {ref_discount_percentage}% off!")
    st.code("")

# same code as in main page
if "user_email" in st.session_state:
    # display dynamic user status (premium, trial, expired)
    user = get_user(st.session_state['user_email'], logger)
    status_message = ""
    if user:
        user_premium_type = get_user_premium_type(st.session_state['user_email'], logger)

        if user_premium_type == PremiumType.FULL:
            status_message = "<span style='color: #28a745;'>(Premium)</span>" # green
        elif user_premium_type == PremiumType.TRIAL:
            expires_str = user.trial_expires_at.strftime('%Y-%m-%d')
            status_message = f"<span style='color: #ffc107;'>(Trial ends {expires_str})</span>" # yellow
        else:
            status_message = "<span style='color: #dc3545;'>(Trial Expired)</span>" # red

    st.markdown(
        f"Logged in as **{st.session_state['user_email']}** {status_message}",
        width="content",
        unsafe_allow_html=True
    )
    if user and user_premium_type == PremiumType.FULL:
        if st.button("Refer a friend"):
            show_ref_code(user)
    else:
        st.text("Sign up for premium to get a referral code and enjoy discounts when referring others!")

else:
    st.text("This page is only relevant for logged-in users")
    show_login_info(show_button_only=True)