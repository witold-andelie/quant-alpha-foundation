"""Quant Alpha Foundation — entry point with explicit st.navigation registration."""
from __future__ import annotations

import streamlit as st

st.set_page_config(
    page_title="Quant Alpha Foundation",
    page_icon="📐",
    layout="wide",
    initial_sidebar_state="expanded",
)

home = st.Page("home.py", title="Home", icon="🏠", default=True)
performance = st.Page("pages/1_Performance.py", title="Performance", icon="📈")
factor_research = st.Page("pages/2_Factor_Research.py", title="Factor Research", icon="🔬")
alpha_decay = st.Page("pages/3_Alpha_Decay.py", title="Alpha Decay", icon="📉")
market_data = st.Page("pages/4_Market_Data.py", title="Market Data", icon="⚡")
live_streaming = st.Page("pages/5_Live_Streaming.py", title="Live Streaming", icon="🔴")
data_pipeline = st.Page("pages/6_Data_Pipeline.py", title="Data Pipeline", icon="🔧")
overview = st.Page("pages/7_Overview.py", title="Cross-Track Overview", icon="📊")

pg = st.navigation({
    " ": [home],
    "Research": [performance, factor_research, alpha_decay, market_data],
    "Operations": [live_streaming, data_pipeline, overview],
})
pg.run()
