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

# taker fee presets per venue (bps). the first entry is the trading-tools default
FEE_PRESETS = {
    "HL": ("Hyperliquid", [(4.5, "<5M 14D volume, 0 HYPE staked"), (4.0, ">5M 14D volume"), (3.5, ">25M 14D volume"),
                           (3.0, ">100M 14D volume"), (2.8, ">500M 14D volume"), (2.6, ">2B 14D volume"),
                           (2.4, ">7B 14D volume")]),
    "EXT": ("Extended", [(2.5, "base"), (2.0, "API tier listed by some sources")]),
    "RISE": ("RISEx", [(3.0, "tier 1"), (2.0, "low end of the docs range")]),
    "VAR": ("Variational", [(0.0, "no fee, spread is in the quotes")]),
}
st.caption("Taker fees. Phoenix is not listed because its fee comes from each market's own config.")
fee_cols = st.columns(len(FEE_PRESETS))
taker_bps = {}
for col, (key, (name, presets)) in zip(fee_cols, FEE_PRESETS.items()):
    with col:
        i = st.selectbox(
            f"{name} taker",
            range(len(presets)),
            format_func=lambda i, presets=presets: f"{presets[i][0]:.2f} bps ({presets[i][1]})",
            key=f"fee_{key}",
        )
        taker_bps[key] = presets[i][0]

c1, c2, c3, c4 = st.columns(4)
with c1:
    notional = st.selectbox("Notional per leg (USD)", [1_000, 10_000, 50_000, 100_000, 500_000], index=1)
with c2:
    metric = st.selectbox("Funding metric", ["24h", "7d", "last"], index=0)
with c3:
    hold_days = st.number_input("Hold (days)", min_value=0.5, max_value=30.0, value=3.0, step=0.5)
with c4:
    min_carry = st.number_input(
        "Min carry (APR %)", min_value=0.0, value=10.0, step=5.0,
        help="Pairs below this carry are never fetched. Lower it to see more pairs; it costs more API calls.",
    )


@st.cache_resource(ttl=600, show_spinner=False)
def get_snapshot(metric: str, min_carry: float):
    """funding history and order books, fetched once per 10 minutes for everyone. a failure is not cached."""
    return funding_arbs.Snapshot(metric=metric, min_carry_pct=min_carry)


try:
    with st.spinner("Fetching funding and order books from each venue (up to a minute)..."):
        snapshot = get_snapshot(metric, float(min_carry))
except Exception as e:
    st.error(f"Could not fetch venue data: {e}. Wait a minute and reload; venue APIs rate limit repeated calls.")
    st.stop()

# priced from the cached snapshot, so changing notional, hold or fees makes no API calls
rows = snapshot.rows(float(notional), float(hold_days), taker_bps)
st.caption(f"Venue data fetched {snapshot.fetched_at:%Y-%m-%d %H:%M:%S} UTC, refreshed every 10 minutes.")

if not rows:
    st.write("No pairs passed the filters. Try a lower min carry or a smaller notional.")
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
