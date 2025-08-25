import streamlit as st
from st_paywall import add_auth

st.title("My Subscription App")

# Handle Streamlit's native authentication
if not st.user.is_logged_in:
    st.write("Please log in to access this app")
    st.button("Log in", on_click=st.login)
else:
    # Add subscription check for logged-in users
    add_auth(
        required=True,  # Stop the app if user is not subscribed
        show_redirect_button=True,  # Show the subscription button
        subscription_button_text="Subscribe Now!",  # Custom button text
        button_color="#FF4B4B",  # Button color (CSS color value)
        use_sidebar=False,
    )

    # Your app code here - only runs for subscribed users
    st.write("Welcome, subscriber!")
    st.write(f"Your email is: {st.user.email}")

# TODO need to configure authentication in your Streamlit app's settings in secrets.