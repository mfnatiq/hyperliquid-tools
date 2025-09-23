import streamlit as st

pg = st.navigation(
    pages={
        'Navigation': [
            st.Page('pages/dashboard.py', title="🔧 Unit Dashboard", default=True),
            st.Page('pages/trial.py', title='⏳ Trial Details'),
            st.Page('pages/funding_rates.py', title='⚖️ Funding Rates'),
        ]
    }
)

pg.run()