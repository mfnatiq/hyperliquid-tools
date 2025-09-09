import streamlit as st

pg = st.navigation(
    pages={
        'Navigation': [
            st.Page('pages/main.py', title="🔧 Unit Dashboard", default=True),
            st.Page('pages/referral.py', title='📣 Referral Details'),
        ]
    }
)

pg.run()