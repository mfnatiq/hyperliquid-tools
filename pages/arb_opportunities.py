import html
from datetime import datetime, timezone

import pandas as pd
import streamlit as st

from src import arb_runtime, arb_view, funding_arbs, funding_history
from src.funding_arbs import days_short, usd_short

st.set_page_config("Arb Opportunities", "💱", layout="wide")

st.header("Funding Arb Opportunities")

if not funding_arbs.AVAILABLE:
    st.info("Info on cross-venue funding carry is unavailable")
    st.stop()

VENUE_NAMES = {"PHX": "Phoenix", "VAR": "Variational", "RISE": "RISEx", "HL": "Hyperliquid", "EXT": "Extended", "PAC": "Pacifica", "NADO": "Nado", "QFEX": "QFEX", "ONDO": "Ondo Perps", "ARCUS": "Arcus"}
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


HOLD_DAYS = 3.0  # hold used for Net APR and the Edge column
BOOKS_EVERY_S = 120
LOADING_EVERY_S = 3  # while the first load runs in the background, the page checks this often

runtime = arb_runtime.start()


def snapshot_for(metric: str, min_carry: float):
    """(snapshot, built_at), or (None, None) while the first build runs in the background. never waits on venue APIs"""
    return runtime.cache.get((metric, float(min_carry)), lambda: arb_runtime.build_snapshot(metric, float(min_carry)))


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


def pct(x):
    return None if x is None else x * 100


def detail(r, hist):
    arb_view.detail(r, hist, VENUE_NAMES, HOLD_DAYS)


def screener_table(rows):
    """compact table, one line per pair. returns the selected row or None"""
    df = pd.DataFrame(
        [
            {
                "Pair": r["ticker"],
                "Route": f"{VENUE_NAMES.get(r['short'], r['short'])} → {VENUE_NAMES.get(r['long'], r['long'])}",
                "Verdict": r["verdict"],
                "Net APR (%)": pct(r.get("net_apr")),
                "Sustain (%)": pct(r["sustain"]),
                "Risk": r["risk"],
                "Break-even": days_short(r["be_all_d"]),
                "OI (short / long)": f"{usd_short(r['oi_short'])} / {usd_short(r['oi_long'])}",
                "Volume 24h (short / long)": f"{usd_short(r['vol_short'])} / {usd_short(r['vol_long'])}",
            }
            for r in rows
        ]
    )
    styled = (
        df.style
        .map(colour_map({"GOOD": GREEN, "OK": AMBER, "MARGINAL": RED}), subset=["Verdict"])
        .map(colour_map({"Low": GREEN, "Med": AMBER, "High": RED}), subset=["Risk"])
        .format("{:.1f}", subset=["Net APR (%)"], na_rep="-")
    )
    event = st.dataframe(
        styled, hide_index=True, use_container_width=True, height=min(38 + 35 * len(df), 700),
        on_select="rerun", selection_mode="single-row", key="arb_table",
        column_config={
            "Net APR (%)": st.column_config.NumberColumn(
                "Net APR (%)", help=f"Carry after the round trip cost and adverse basis, for a {HOLD_DAYS:g} day hold. Holding longer raises it"),
            "Sustain (%)": st.column_config.ProgressColumn(
                "Sustain (%)", min_value=0, max_value=100, format="%.0f%%",
                help="Share of the last 7 days' shared hours with the carry in our favour, from the venues' funding history"),
            "Risk": st.column_config.TextColumn("Risk", help="Spread risk: slippage plus adverse basis, or a leg thinner than the position"),
            "Verdict": st.column_config.TextColumn(
                "Verdict",
                help="GOOD: break-even incl. basis within 3 days and no warning. OK: within 7 days and at most one warning. "
                     "Otherwise MARGINAL. Warnings: basis against us, unstable carry, a leg thinner than the position "
                     "within 10 bps, a stale Variational quote",
            ),
        },
    )
    picked = event.selection.rows
    return rows[picked[0]] if picked and picked[0] < len(rows) else None


def table_run_every():
    """short polling while the first load runs in the background, then the normal refresh. a full rerun switches between them"""
    return LOADING_EVERY_S if snapshot_for(metric, min_carry)[0] is None else BOOKS_EVERY_S


@st.fragment(run_every=table_run_every())
def table(venues_sel, notional, metric, sustain_min, be_max, min_carry, risk_max, search, sort_by):
    """re-runs on its own and refreshes the order books and this table only, not the controls above it"""
    snapshot, _ = snapshot_for(metric, min_carry)
    if snapshot is None:
        err = runtime.cache.error.get((metric, float(min_carry)))
        if err:
            st.error(f"Could not fetch venue data: {err}. It retries every minute, venue APIs rate limit repeated calls")
        else:
            st.info("Loading funding rates and order books from each venue in the background. This page fills in by itself")
        return
    if "arb_ready" not in st.session_state or st.session_state["arb_ready"] != (metric, min_carry):
        st.session_state["arb_ready"] = (metric, min_carry)
        st.rerun()  # full rerun once, so the fragment switches from loading polls to the normal refresh
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

    st.caption("Tick the box at the left of a row to open its details above the table. A dash means the venue does not report it")
    detail_slot = st.container()  # filled after the table, but drawn above it so the details are in view
    picked = screener_table(rows)
    if picked is not None:
        try:
            hist = funding_history.pair_history(picked["short"], picked["long"], picked["ticker"]) if runtime.recording else None
        except Exception:
            hist = None
        with detail_slot:
            detail(picked, hist)

    fees = ", ".join(f"{VENUE_NAMES.get(k, k)} {v:g}" for k, v in funding_arbs.taker_bps().items() if k in VENUE_NAMES)
    with st.expander("Info and assumptions"):
        st.markdown(
            "\n".join(f"- {line}" for line in [
                "Short the venue with the higher funding rate, long the other, same USD size on each leg",
                "Round-trip cost = 2 x (slippage + taker fees on both legs), i.e. entry plus exit",
                f"Net APR = carry after the round trip cost and adverse basis, for a {HOLD_DAYS:g} day hold, as a yearly rate",
                f"Taker fees in bps, from trading-tools' venues.toml: {fees}. Phoenix uses the fee set on each market. QFEX shows its single-stock rate, indices and commodities pay less",
                "Break-even = round-trip cost / daily carry, including adverse basis",
                f"Edge = expected carry over a {HOLD_DAYS:g} day hold, less round-trip cost and adverse basis",
                "Spread risk is Med above 5 bps and High above 12 bps of slippage plus adverse basis, or when a leg is thinner than the position",
                "24h, 7d and 30d carry averages come from hourly samples this app records itself, so they fill in over time. Volume and open interest are shown only where a venue reports them",
                f"Funding fetched {snapshot.fetched_at:%H:%M:%S} UTC (every {arb_runtime.FUNDING_TTL_S // 60} minutes), order books {snapshot.books_at:%H:%M:%S} UTC (every {BOOKS_EVERY_S // 60} minutes)",
            ])
        )


with table_slot:
    table(venues_sel, notional, metric, sustain_min, be_max, min_carry, risk_max, search, sort_by)
