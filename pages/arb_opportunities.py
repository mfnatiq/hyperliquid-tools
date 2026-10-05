import pandas as pd
import streamlit as st

from src import funding_arbs

st.set_page_config("Arb Opportunities", "💱", layout="wide")

st.header("Funding Arb Opportunities")

if not funding_arbs.AVAILABLE:
    st.info(
        "Install trading-tools to see cross-venue funding carry with entry and exit costs: "
        "`pip install ../trading-tools` (private repo, needs read access)"
    )
    st.stop()

VENUE_NAMES = {"PHX": "Phoenix", "VAR": "Variational", "RISE": "RISEx", "HL": "Hyperliquid", "EXT": "Extended"}

st.markdown("""
- Short the venue with the higher funding rate, long the one with the lower rate, same USD size on each leg
- Round-trip cost = 2 x (slippage on both legs + taker fees on both legs), i.e. entry plus exit
- Break-even days = round-trip cost / daily carry
- Edge = expected carry over the hold, less round-trip cost and adverse mid basis
""")

# region taker fees
# presets per venue (bps), the first entry is the trading-tools default
FEE_PRESETS = {
    "HL": [(4.5, "<5M 14D volume, 0 HYPE staked"), (4.0, ">5M 14D volume"), (3.5, ">25M 14D volume"),
           (3.0, ">100M 14D volume"), (2.8, ">500M 14D volume"), (2.6, ">2B 14D volume"), (2.4, ">7B 14D volume")],
    "EXT": [(2.5, "base"), (2.0, "API tier listed by some sources")],
    "RISE": [(3.0, "tier 1"), (2.0, "low end of the docs range")],
    "VAR": [(0.0, "no fee, spread is in the quotes")],
}

st.subheader("Taker Fees")
st.caption("Phoenix charges the fee set on each market, so it has no selector")
taker_bps = {}
fee_cols = st.columns(len(FEE_PRESETS))
for col, (key, presets) in zip(fee_cols, FEE_PRESETS.items()):
    with col:
        i = st.selectbox(
            VENUE_NAMES[key],
            range(len(presets)),
            format_func=lambda i, presets=presets: f"{presets[i][0]:.2f} bps ({presets[i][1]})",
            key=f"fee_{key}",
        )
        taker_bps[key] = presets[i][0]
# endregion

st.subheader("Settings")
c1, c2, c3 = st.columns(3)
with c1:
    notional = st.selectbox("Notional per leg (USD)", [1_000, 10_000, 50_000, 100_000, 500_000], index=1)
with c2:
    metric = st.selectbox("Funding metric", ["24h", "7d", "last"], index=0)
with c3:
    hold_days = st.number_input("Hold (days)", min_value=0.5, max_value=30.0, value=3.0, step=0.5)
min_carry = st.slider(
    "Min carry (APR %)", min_value=10, max_value=200, value=10, step=5,
    help="Pairs below this carry are never fetched, so a higher value means fewer API calls",
)


@st.cache_resource(ttl=600, show_spinner=False)
def get_snapshot(metric: str, min_carry: float):
    """funding history and order books, fetched once per 10 minutes for everyone, a failure is not cached"""
    return funding_arbs.Snapshot(metric=metric, min_carry_pct=min_carry)


@st.fragment(run_every=60)
def results():
    """re-runs every minute on its own: refetches order books (not funding history), then reprices"""
    # asked for again on every run, so the snapshot is rebuilt once its 10 minute cache expires
    try:
        with st.spinner("Fetching funding and order books from each venue (up to a minute)"):
            snapshot = get_snapshot(metric, float(min_carry))
    except Exception as e:
        st.error(f"Could not fetch venue data: {e}. Wait a minute and reload, venue APIs rate limit repeated calls")
        return
    snapshot.refresh_books()
    # priced from cached data, so changing notional, hold or fees makes no API calls
    rows = snapshot.rows(float(notional), float(hold_days), taker_bps)

    used = ", ".join(VENUE_NAMES.get(k, k) for k in snapshot.vs)
    st.caption(f"Venues used: {used}")
    if snapshot.missing:
        st.warning("No data from " + ", ".join(VENUE_NAMES.get(k, k) for k in snapshot.missing) + ", so pairs with them are missing")
    st.caption(
        f"Funding history fetched {snapshot.fetched_at:%H:%M:%S} UTC (refreshed every 10 minutes), "
        f"order books fetched {snapshot.books_at:%H:%M:%S} UTC (refreshed every minute)"
    )

    if not rows:
        st.write("No pairs passed the filters, try a lower min carry or a smaller notional")
        return

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
                "Fee Short (bps)": r["short_fee_bps"],
                "Fee Long (bps)": r["long_fee_bps"],
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

    # region colour coding, green is good and red is bad
    GREEN, AMBER, RED = "rgba(46,160,67,0.35)", "rgba(210,153,34,0.35)", "rgba(248,81,73,0.35)"


    def banded(good: float, ok: float):
        """lower is better: green up to `good`, amber up to `ok`, red above"""
        def style(v):
            if v is None or pd.isna(v):
                return ""
            return f"background-color: {GREEN if v <= good else AMBER if v <= ok else RED}"
        return style


    VERDICT_COLOURS = {"GOOD": GREEN, "OK": AMBER, "MARGINAL": RED}
    RISK_COLOURS = {"Low": GREEN, "Med": AMBER, "High": RED}
    colour_map = lambda m: (lambda v: f"background-color: {m[v]}" if v in m else "")

    styled = (
        df.style
        .map(colour_map(VERDICT_COLOURS), subset=["Verdict"])
        .map(colour_map(RISK_COLOURS), subset=["Spread Risk"])
        .map(banded(20, 40), subset=["Round-trip Cost (bps)"])
        .map(banded(3, 7), subset=["Break-even (days)", "Break-even incl. basis (days)"])
        .format("{:.1f}", subset=df.select_dtypes("number").columns, na_rep="-")
    )
    st.dataframe(styled, hide_index=True, use_container_width=True)
    st.caption(
        "Sorted GOOD, then OK, then MARGINAL, and by break-even within each. "
        "Green is cheap or quick to pay back: round-trip cost up to 20 bps, break-even up to 3 days. "
        "Amber is up to 40 bps and 7 days, red is above that. "
        "Mid basis above 0 is favourable. "
        "Sustain is the share of the last 7 days' shared hours with the carry in our favour. "
        "Spread risk is Med above 5 bps and High above 12 bps of slippage plus adverse basis, or when a leg is thinner than the notional"
    )
    # endregion


results()
