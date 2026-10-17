import streamlit as st
from src.utils.render_utils import referrals_popover, site_css, site_footer
from src.utils.analytics import PAGEVIEW_LOADER
import streamlit.components.v1 as components

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

site_css()
# umami page views for every page; site_css() takes the key="umami" container out of the layout
with st.container(key="umami"):
    components.html(PAGEVIEW_LOADER, height=0)
# render these before pg.run(), so a page that calls st.stop() still shows them
with st.container(key="site_header"):
    col1, col2 = st.columns([1, 1], vertical_alignment='center')
    with col1:
        with st.container(vertical_alignment='center', horizontal=True, horizontal_alignment="left"):
            st.title("Hyperliquid Tools")
    with col2:
        with st.container(vertical_alignment='center', horizontal=True, horizontal_alignment="right"):
            referrals_popover()
    st.divider()
site_footer()

pg.run()