import streamlit as st
from src.utils.render_utils import footer_html, copy_script
import streamlit.components.v1 as components

st.markdown(
    """
    <style>
    /* add bottom padding to main content so fixed footer doesn't overlap */
    .block-container {
        padding-bottom: 160px !important;  /* tweak to match footer height */
    }
    </style>
    """,
    unsafe_allow_html=True,
)

pg = st.navigation(
    pages={
        'Navigation': [
            st.Page('pages/dashboard.py', title="🔧 Unit Dashboard", default=True),
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
        # TODO consider putting at top?
        # render sticky footer
        st.markdown(footer_html, unsafe_allow_html=True)
        components.html(copy_script, height=0)  # render copy script in a separate component to avoid CSP issues

st.divider()

pg.run()