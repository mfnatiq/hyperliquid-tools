import pandas as pd
import streamlit as st

from src import funding_arbs

st.set_page_config("Arb Opportunities", "💱", layout="wide")

st.header("Funding Arb Opportunities")

if not funding_arbs.AVAILABLE:
    st.info(
        "Install trading-tools to see cross-venue funding carry with entry and exit costs: "
        "`pip install ../trading-tools` (private repo, needs read access)."
    )
    st.stop()

st.markdown("""
- Short the venue with the higher funding rate, long the lower one, equal USD on each leg
- Round-trip cost = 2 x (slippage on both legs + taker fees on both legs), i.e. entry plus exit
- Break-even days = round-trip cost / daily carry. Edge = expected carry over the hold, less round-trip cost and adverse mid basis
- Venue fees, rate limits and symbol aliases come from trading-tools (`bot/venues.toml`)
""")

defaults = funding_arbs.default_taker_bps()
c1, c2, c3, c4, c5 = st.columns(5)
with c1:
    notional = st.selectbox("Notional per leg (USD)", [1_000, 10_000, 50_000, 100_000, 500_000], index=1)
with c2:
    metric = st.selectbox("Funding metric", ["24h", "7d", "last"], index=0)
with c3:
    hold_days = st.number_input("Hold (days)", min_value=0.5, max_value=30.0, value=3.0, step=0.5)
with c4:
    hl_fee = st.number_input("Hyperliquid taker (bps)", min_value=0.0, value=defaults["Hyperliquid"], step=0.1)
with c5:
    ext_fee = st.number_input("Extended taker (bps)", min_value=0.0, value=defaults["Extended"], step=0.1)


@st.cache_data(ttl=300, show_spinner=False)
def scan_cached(notional: float, metric: str, hold_days: float, hl_fee: float, ext_fee: float):
    return funding_arbs.scan(
        notional, metric=metric, hold_days=hold_days, taker_bps={"Hyperliquid": hl_fee, "Extended": ext_fee}
    )


with st.spinner("Scanning funding across venues (this can take a minute)..."):
    rows = scan_cached(float(notional), metric, float(hold_days), float(hl_fee), float(ext_fee))

if not rows:
    st.write("No pairs passed the filters, or the venue APIs could not be reached.")
    st.stop()

pct = lambda x: None if x is None else x * 100
df = pd.DataFrame(
    [
        {
            "Verdict": r["verdict"],
            "Token": r["ticker"],
            "Short": r["short"],
            "Long": r["long"],
            "Carry (APR %)": pct(r["carry_apr"]),
            "Slip Short (bps)": r["slip_short_bps"],
            "Slip Long (bps)": r["slip_long_bps"],
            "Taker Fees (bps)": r["fee_bps"],
            "Mid Basis (bps)": r["mid_basis_bps"],
            "Round-trip Cost (bps)": r["rt_cost_bps"],
            "Break-even (days)": r["breakeven_d"],
            "Break-even incl. basis (days)": r["be_all_d"],
            "Sustain (%)": pct(r["sustain"]),
            "Spread Risk": r["risk"],
            "Edge (bps)": r["edge_bps"],
            "Note": r["note"],
        }
        for r in rows
    ]
)
st.dataframe(df, hide_index=True, use_container_width=True)
st.caption(
    "Mid basis > 0 is favourable. Sustain = share of the last 7 days' shared hours with carry in our favour. "
    "Spread risk is Med above 5 bps and High above 12 bps of slippage plus adverse basis, or when a leg is thinner than the notional."
)
