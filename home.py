import streamlit as st
from src.utils.render_utils import support_popover

pg = st.navigation(
    pages={
        'Navigation': [
            st.Page('pages/dashboard.py', title="🔧 Unit Dashboard", default=True),
            st.Page('pages/kinetiq_dashboard.py', title="🔧 Kinetiq Dashboard"),
            st.Page('pages/hip4_dashboard.py', title="🎯 HIP-4 Markets"),
            st.Page('pages/trial.py', title='⏳ Trial Details'),
            st.Page('pages/liquidity_analysis.py', title='📐 Liquidity Analysis'),
        ]
    }
)

col1, col2 = st.columns([1, 1], vertical_alignment='center')
with col1:
    with st.container(vertical_alignment='center', horizontal=True, horizontal_alignment="left"):
        st.title("Hyperliquid Tools")
with col2:
    with st.container(vertical_alignment='center', horizontal=True, horizontal_alignment="right"):
        support_popover()

st.divider()

pg.run()