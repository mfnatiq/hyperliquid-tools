import streamlit as st

st.title("My Subscription App")

# Handle Streamlit's native authentication
if not st.user.is_logged_in:
    st.write("Please log in to access this app")
    st.button("Log in", on_click=st.login)
else:
    # Your app code here - only runs for subscribed users
    st.write("Welcome, subscriber!")
    st.write(f"Your email is: {st.user.email}")

# TODO need to configure authentication in your Streamlit app's settings in secrets.