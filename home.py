import streamlit as st

pg = st.navigation(
    pages={
        'Navigation': [
            st.Page('pages/dashboard.py', title="🔧 Unit Dashboard", default=True),
            st.Page('pages/trial.py', title='⏳ Trial Details'),
            st.Page('pages/referral.py', title='📣 Referral Details'),
        ]
    }
)

pg.run()