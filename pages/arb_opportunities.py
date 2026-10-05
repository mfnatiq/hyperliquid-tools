import html
from datetime import datetime, timezone

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
RISK_RANK = {"Low": 0, "Med": 1, "High": 2}
SORTS = {
    "Verdict": None,  # the snapshot's own order: GOOD, OK, MARGINAL, then break-even
    "Edge": lambda r: -(r["edge_bps"] if r["edge_bps"] is not None else -1e9),
    "Carry": lambda r: -r["carry_apr"],
    "Break-even": lambda r: r["be_all_d"] if r["be_all_d"] is not None else 1e9,
}

# taker fee presets per venue (bps), the first entry is the trading-tools default
FEE_PRESETS = {
    "HL": [(4.5, "<5M 14D volume, 0 HYPE staked"), (4.0, ">5M 14D volume"), (3.5, ">25M 14D volume"),
           (3.0, ">100M 14D volume"), (2.8, ">500M 14D volume"), (2.6, ">2B 14D volume"), (2.4, ">7B 14D volume")],
    "EXT": [(2.5, "base"), (2.0, "API tier listed by some sources")],
    "RISE": [(3.0, "tier 1"), (2.0, "low end of the docs range")],
    "VAR": [(0.0, "no fee, spread is in the quotes")],
}

GREEN, AMBER, RED = "rgba(46,160,67,0.35)", "rgba(210,153,34,0.35)", "rgba(248,81,73,0.35)"

st.markdown(
    """
    <style>
    .chip {display: inline-block; padding: 2px 10px; margin: 0 6px 6px 0; border-radius: 6px; font-size: 0.8rem;
           border: 1px solid rgba(124,108,255,0.6); background: rgba(124,108,255,0.12);}
    .live-dot {display: inline-block; width: 9px; height: 9px; border-radius: 50%; margin-right: 8px; background: #2ea043;}
    .live-dot.stale {background: #d29922;}
    .live-text {font-family: monospace; font-size: 0.85rem; letter-spacing: 0.04em;}
    </style>
    """,
    unsafe_allow_html=True,
)


@st.cache_resource(ttl=600, show_spinner=False)
def get_snapshot(metric: str, min_carry: float):
    """funding history and order books, fetched once per 10 minutes for everyone, a failure is not cached"""
    return funding_arbs.Snapshot(metric=metric, min_carry_pct=min_carry)


def banded(good: float, ok: float):
    """lower is better: green up to `good`, amber up to `ok`, red above"""
    def style(v):
        if v is None or pd.isna(v):
            return ""
        return f"background-color: {GREEN if v <= good else AMBER if v <= ok else RED}"
    return style


colour_map = lambda m: (lambda v: f"background-color: {m[v]}" if v in m else "")


@st.fragment(run_every=60)
def page():
    """re-runs on its own every minute (order books only) and on any widget change, without rerunning the app"""
    # region controls
    bar = st.columns([2.2, 1.6, 1.1, 1.1, 2.4], vertical_alignment="center")
    status_slot = bar[0].empty()
    sort_by = bar[1].segmented_control("Sort", list(SORTS), default="Verdict", label_visibility="collapsed")
    sort_by = sort_by or "Verdict"

    with bar[2].popover("Filters", use_container_width=True):
        st.markdown("**Venues**")
        names_sel = st.pills(
            "Venues", list(VENUE_NAMES.values()), default=list(VENUE_NAMES.values()), selection_mode="multi",
            label_visibility="collapsed", key="f_venues",
        ) or []
        venues_sel = [k for k, name in VENUE_NAMES.items() if name in names_sel]
        st.caption("A pair needs both of its venues selected")
        sustain_min = st.slider(
            "Sustainability, min (%)", 0, 100, 0, step=5, key="f_sustain",
            help="Share of the last 7 days' shared hours with the carry in our favour",
        )
        be_max = st.slider(
            "Break-even, max (days)", 1.0, 14.0, 14.0, step=0.5, key="f_be",
            help="How long funding needs to run to repay the round trip, including adverse basis",
        )
        min_carry = st.slider(
            "Carry, min (APR %)", 10, 200, 10, step=5, key="f_carry",
            help="Pairs below this are never fetched, so a higher value means fewer API calls",
        )
        risk_max = st.select_slider("Spread risk, max", ["Low", "Med", "High"], value="High", key="f_risk")
        counts_slot = st.empty()

    with bar[3].popover("Fees", use_container_width=True):
        st.markdown("**Taker fees**")
        st.caption("Phoenix charges the fee set on each market, so it has no selector")
        taker_bps = {}
        for key, presets in FEE_PRESETS.items():
            i = st.selectbox(
                VENUE_NAMES[key], range(len(presets)), key=f"fee_{key}",
                format_func=lambda i, presets=presets: f"{presets[i][0]:.2f} bps ({presets[i][1]})",
            )
            taker_bps[key] = presets[i][0]
        st.markdown("**Position**")
        notional = st.selectbox("Notional per leg (USD)", [1_000, 10_000, 50_000, 100_000, 500_000], index=1, key="s_notional")
        hold_days = st.number_input("Hold (days)", min_value=0.5, max_value=30.0, value=3.0, step=0.5, key="s_hold")
        metric = st.selectbox("Funding metric", ["24h", "7d", "last"], index=0, key="s_metric")

    search = bar[4].text_input("Search", placeholder="Search pairs", label_visibility="collapsed", key="f_search")
    # endregion

    try:
        with st.spinner("Fetching funding and order books from each venue (up to a minute)"):
            snapshot = get_snapshot(metric, float(min_carry))  # asked for on every run, so it rebuilds when its cache expires
    except Exception as e:
        st.error(f"Could not fetch venue data: {e}. Wait a minute and reload, venue APIs rate limit repeated calls")
        return
    snapshot.refresh_books()

    age = (datetime.now(timezone.utc) - snapshot.books_at).total_seconds()
    status_slot.markdown(
        f'<span class="live-dot{" stale" if age > 150 else ""}"></span>'
        f'<span class="live-text">LIVE {age:.0f}s ago</span>',
        unsafe_allow_html=True,
    )

    # region active filter chips
    chips = []
    if len(venues_sel) != len(VENUE_NAMES):
        chips.append(f"Venues {len(venues_sel)}/{len(VENUE_NAMES)}")
    if sustain_min > 0:
        chips.append(f"Sustain ≥ {sustain_min}%")
    if be_max < 14:
        chips.append(f"Break-even ≤ {be_max:g}d")
    if min_carry > 10:
        chips.append(f"Carry ≥ {min_carry}%")
    if risk_max != "High":
        chips.append(f"Risk ≤ {risk_max}")
    if search.strip():
        chips.append(f"Search “{search.strip()}”")
    if chips:
        st.markdown("".join(f'<span class="chip">{html.escape(c)}</span>' for c in chips), unsafe_allow_html=True)
    # endregion

    # priced from cached data, so changing notional, hold or fees makes no API calls
    all_rows = snapshot.rows(float(notional), float(hold_days), taker_bps)

    def keep(r):
        return (
            r["short"] in venues_sel and r["long"] in venues_sel
            and (sustain_min == 0 or (r["sustain"] is not None and r["sustain"] * 100 >= sustain_min))
            and r["be_all_d"] is not None and r["be_all_d"] <= be_max
            and RISK_RANK.get(r["risk"], 0) <= RISK_RANK[risk_max]
            and search.strip().upper() in r["ticker"].upper()
        )

    rows = [r for r in all_rows if keep(r)]
    if SORTS[sort_by]:
        rows = sorted(rows, key=SORTS[sort_by])
    counts_slot.caption(f"{len(rows)} of {len(all_rows)} pairs")

    used = ", ".join(VENUE_NAMES.get(k, k) for k in snapshot.vs)
    st.caption(f"Venues used: {used}")
    if snapshot.missing:
        st.warning("No data from " + ", ".join(VENUE_NAMES.get(k, k) for k in snapshot.missing) + ", so pairs with them are missing")

    if not rows:
        st.write("No pairs match, loosen a filter")
        return

    pct = lambda x: None if x is None else x * 100
    df = pd.DataFrame(
        [
            {
                "Pair": r["ticker"],
                "Route (short → long)": f"{VENUE_NAMES.get(r['short'], r['short'])} → {VENUE_NAMES.get(r['long'], r['long'])}",
                "Verdict": r["verdict"],
                "Carry (APR %)": pct(r["carry_apr"]),
                "Sustain (%)": pct(r["sustain"]),
                "Spread Risk": r["risk"],
                "Carry Percentile": r["carry_pct"],
                "Break-even (days)": r["breakeven_d"],
                "Break-even incl. basis (days)": r["be_all_d"],
                "Round-trip Cost (bps)": r["rt_cost_bps"],
                "Slip Short (bps)": r["slip_short_bps"],
                "Slip Long (bps)": r["slip_long_bps"],
                "Fee Short (bps)": r["short_fee_bps"],
                "Fee Long (bps)": r["long_fee_bps"],
                "Mid Basis (bps)": r["mid_basis_bps"],
                "Edge (bps)": r["edge_bps"],
                "Note": r["note"],
            }
            for r in rows
        ]
    )
    numeric = [c for c in df.select_dtypes("number").columns if c != "Sustain (%)"]
    styled = (
        df.style
        .map(colour_map({"GOOD": GREEN, "OK": AMBER, "MARGINAL": RED}), subset=["Verdict"])
        .map(colour_map({"Low": GREEN, "Med": AMBER, "High": RED}), subset=["Spread Risk"])
        .map(banded(20, 40), subset=["Round-trip Cost (bps)"])
        .map(banded(3, 7), subset=["Break-even (days)", "Break-even incl. basis (days)"])
        .format("{:.1f}", subset=numeric, na_rep="-")
    )
    st.dataframe(
        styled,
        hide_index=True,
        use_container_width=True,
        column_config={
            "Sustain (%)": st.column_config.ProgressColumn("Sustain (%)", min_value=0, max_value=100, format="%.0f%%"),
        },
    )
    st.caption(
        "Short the venue with the higher funding rate, long the other, same USD size on each leg. "
        "Round-trip cost = 2 x (slippage + taker fees on both legs), i.e. entry plus exit. "
        "Break-even = round-trip cost / daily carry. "
        "Edge = expected carry over the hold, less round-trip cost and adverse basis. "
        "Green is cheap or quick to repay: round-trip cost up to 20 bps, break-even up to 3 days. "
        "Amber is up to 40 bps and 7 days, red is above. "
        "Mid basis above 0 is favourable. "
        "Spread risk is Med above 5 bps and High above 12 bps of slippage plus adverse basis, or when a leg is thinner than the notional. "
        f"Funding history fetched {snapshot.fetched_at:%H:%M:%S} UTC (every 10 minutes), "
        f"order books {snapshot.books_at:%H:%M:%S} UTC (every minute)"
    )


page()
