import html
from datetime import datetime, timezone

import pandas as pd
import streamlit as st

from src import funding_arbs

st.set_page_config("Arb Opportunities", "💱", layout="wide")

st.header("Funding Arb Opportunities")

if not funding_arbs.AVAILABLE:
    st.info(
        "trading-tools is not installed, so cross-venue funding carry is unavailable. "
        "It is a private repo: set GH_TOKEN (read access) and run `pip install -r requirements.txt`."
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


HOLD_DAYS = 3.0  # the hold used for the Edge column
FUNDING_TTL_S, BOOKS_EVERY_S = 1800, 120


@st.cache_resource(ttl=FUNDING_TTL_S, show_spinner=False)
def get_snapshot(metric: str, min_carry: float):
    """funding history and order books, fetched once per 30 minutes for everyone, a failure is not cached"""
    return funding_arbs.Snapshot(metric=metric, min_carry_pct=min_carry)


def banded(good: float, ok: float):
    """lower is better: green up to `good`, amber up to `ok`, red above"""
    def style(v):
        if v is None or pd.isna(v):
            return ""
        return f"background-color: {GREEN if v <= good else AMBER if v <= ok else RED}"
    return style


colour_map = lambda m: (lambda v: f"background-color: {m[v]}" if v in m else "")


def controls():
    """widgets live outside the fragment, so only a change here reruns the page"""
    bar = st.columns([1.6, 1.1, 2.4], vertical_alignment="center")
    sort_by = bar[0].segmented_control("Sort", list(SORTS), default="Verdict", label_visibility="collapsed") or "Verdict"
    with bar[1].popover("Filters", use_container_width=True):
        st.markdown("**Venues**")
        names_sel = st.pills(
            "Venues", list(VENUE_NAMES.values()), default=list(VENUE_NAMES.values()), selection_mode="multi",
            label_visibility="collapsed", key="f_venues",
        ) or []
        st.caption("A pair needs both of its venues selected")
        notional = st.selectbox("Position per leg (USD)", [1_000, 10_000, 50_000, 100_000, 500_000], index=1, key="s_notional")
        metric = st.selectbox(
            "Funding metric", ["24h", "7d", "last"], index=0, key="s_metric",
            help="Which funding rate the carry uses: the average of the last 24 hours, of the last 7 days, or the latest "
                 "reading. Every rate is converted to an annual percentage first, so venues that pay hourly or every "
                 "8 hours compare directly",
        )
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
    search = bar[2].text_input(
        "Search", placeholder="Filter by token, e.g. BTC", label_visibility="collapsed", key="f_search",
        help="Narrows the table below to tickers containing this text. It only sees the pairs already listed, "
             "it does not look up other markets",
    )
    return sort_by, [k for k, name in VENUE_NAMES.items() if name in names_sel], float(notional), metric, \
        sustain_min, be_max, min_carry, risk_max, search.strip(), counts_slot


sort_by, venues_sel, notional, metric, sustain_min, be_max, min_carry, risk_max, search, counts_slot = controls()

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
if search:
    chips.append(f"Search “{search}”")
if chips:
    st.markdown("".join(f'<span class="chip">{html.escape(c)}</span>' for c in chips), unsafe_allow_html=True)

status_slot = st.empty()
table_slot = st.container()


@st.fragment(run_every=BOOKS_EVERY_S)
def table(venues_sel, notional, metric, sustain_min, be_max, min_carry, risk_max, search, sort_by):
    """re-runs on its own and refreshes the order books and this table only, not the controls above it"""
    try:
        with st.spinner("Fetching funding and order books from each venue (up to a minute)"):
            snapshot = get_snapshot(metric, float(min_carry))  # asked for on every run, so it rebuilds when its cache expires
    except Exception as e:
        st.error(f"Could not fetch venue data: {e}. Wait a minute and reload, venue APIs rate limit repeated calls")
        return
    snapshot.refresh_books(min_age_s=BOOKS_EVERY_S - 20)

    age = (datetime.now(timezone.utc) - snapshot.books_at).total_seconds()
    status_slot.markdown(
        f'<span class="live-dot{" stale" if age > 2 * BOOKS_EVERY_S + 30 else ""}"></span>'
        f'<span class="live-text">LIVE {age:.0f}s ago</span>',
        unsafe_allow_html=True,
    )

    # priced from cached data, so changing the position makes no API calls
    all_rows = snapshot.rows(notional, HOLD_DAYS)

    def keep(r):
        return (
            r["short"] in venues_sel and r["long"] in venues_sel
            and (sustain_min == 0 or (r["sustain"] is not None and r["sustain"] * 100 >= sustain_min))
            and r["be_all_d"] is not None and r["be_all_d"] <= be_max
            and RISK_RANK.get(r["risk"], 0) <= RISK_RANK[risk_max]
            and search.upper() in r["ticker"].upper()
        )

    rows = [r for r in all_rows if keep(r)]
    if SORTS[sort_by]:
        rows = sorted(rows, key=SORTS[sort_by])
    counts_slot.caption(f"{len(rows)} of {len(all_rows)} pairs")

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
        height=min(38 + 35 * len(df), 900),
        column_config={
            "Verdict": st.column_config.TextColumn(
                "Verdict",
                help="GOOD: break-even incl. basis within 3 days and no warning. OK: within 7 days and at most one warning. "
                     "Otherwise MARGINAL. Warnings: basis against us, unstable carry, a leg thinner than the position "
                     "within 10 bps, a stale Variational quote. A short break-even with a warning grades lower, so the "
                     "shortest break-even is not always the best verdict",
            ),
            "Round-trip Cost (bps)": st.column_config.NumberColumn(
                "Round-trip Cost (bps)",
                help="2 x (slippage on both legs + taker fees on both legs). Slippage is the VWAP of walking the book "
                     "for the full position (bids on the sell leg, asks on the buy leg) against that venue's own mid. "
                     "Exit is assumed to cost the same as entry",
            ),
            "Sustain (%)": st.column_config.ProgressColumn("Sustain (%)", min_value=0, max_value=100, format="%.0f%%"),
        },
    )
    fees = ", ".join(f"{VENUE_NAMES.get(k, k)} {v:g}" for k, v in funding_arbs.taker_bps().items() if k in VENUE_NAMES)
    with st.expander("Info and assumptions"):
        st.markdown(
            "\n".join(f"- {line}" for line in [
                "Short the venue with the higher funding rate, long the other, same USD size on each leg.",
                "Round-trip cost = 2 x (slippage + taker fees on both legs), i.e. entry plus exit.",
                f"Taker fees in bps, from trading-tools' venues.toml: {fees}. Phoenix uses the fee set on each market.",
                "Break-even = round-trip cost / daily carry.",
                f"Edge = expected carry over a {HOLD_DAYS:g} day hold, less round-trip cost and adverse basis.",
                "Green is cheap or quick to repay: round-trip cost up to 20 bps, break-even up to 3 days.",
                "Amber is up to 40 bps and 7 days, red is above.",
                "Mid basis above 0 is favourable.",
                "Spread risk is Med above 5 bps and High above 12 bps of slippage plus adverse basis, or when a leg is thinner than the position.",
                f"Funding history fetched {snapshot.fetched_at:%H:%M:%S} UTC (every {FUNDING_TTL_S // 60} minutes).",
                f"Order books {snapshot.books_at:%H:%M:%S} UTC (every {BOOKS_EVERY_S // 60} minutes).",
            ])
        )


with table_slot:
    table(venues_sel, notional, metric, sustain_min, be_max, min_carry, risk_max, search, sort_by)
