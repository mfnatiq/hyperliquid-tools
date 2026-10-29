import html
from datetime import datetime, timezone

import streamlit as st

from src import arb_runtime, arb_view, funding_arbs, funding_history
from src.arb_view import badge, net_apr_text, sustain_text
from src.funding_arbs import days_short, usd_short

st.set_page_config("Arb Opportunities", "💱", layout="wide")

st.header("Funding Arb Opportunities")

if not funding_arbs.AVAILABLE:
    st.caption("Funding carry data is unavailable, trading-tools is not installed")
    st.stop()

VENUE_NAMES = {"PHX": "Phoenix", "VAR": "Variational", "RISE": "RISEx", "HL": "Hyperliquid", "EXT": "Extended", "PAC": "Pacifica", "NADO": "Nado", "QFEX": "QFEX", "ONDO": "Ondo Perps", "ARCUS": "Arcus"}
RISK_RANK = {"Low": 0, "Med": 1, "High": 2}
SORTS = {
    "Verdict": None,  # the snapshot's own order: GOOD, OK, MARGINAL, then break-even
    "Edge": lambda r: -(r["edge_bps"] if r["edge_bps"] is not None else -1e9),
    "Carry": lambda r: -r["carry_apr"],
    "Break-even": lambda r: r["be_all_d"] if r["be_all_d"] is not None else 1e9,
}

st.markdown(
    """
    <style>
    .chip {display: inline-block; padding: 2px 10px; margin: 0 6px 6px 0; border-radius: 6px; font-size: 0.8rem;
           border: 1px solid rgba(124,108,255,0.6); background: rgba(124,108,255,0.12);}
    .live-dot {display: inline-block; width: 9px; height: 9px; border-radius: 50%; margin-right: 8px; background: #2ea043;}
    .live-dot.stale {background: #d29922;}
    .live-text {font-family: monospace; font-size: 0.85rem; letter-spacing: 0.04em;}
    /* the detail dialog is a drawer pinned to the right on wide screens and covers the screen on phones */
    div[data-testid="stDialog"] div[role="dialog"] {
        position: fixed; top: 0; right: 0; bottom: 0; margin: 0; width: min(560px, 100vw);
        max-width: 100vw; height: 100vh; max-height: 100vh; border-radius: 0; overflow-y: auto;
    }
    @media (max-width: 640px) {
        div[data-testid="stDialog"] div[role="dialog"] {width: 100vw;}
    }
    /* the screener grid: one line per pair that never stacks, everything centred vertically. on phones only pair
       (with its route underneath), verdict, Details and net APR stay, the rest is in the drawer */
    .st-key-arb_rows {gap: 0 !important;}
    .st-key-arb_rows [data-testid="stHorizontalBlock"] {flex-wrap: nowrap !important; gap: 1rem; align-items: center;
        padding: 0.85rem 0.25rem; border-bottom: 1px solid rgba(128,128,128,0.2);}
    .st-key-arb_rows [data-testid="stElementContainer"] {margin: 0 !important; width: auto;}
    .st-key-arb_rows [data-testid="stColumn"] {min-width: 0 !important; width: auto !important;}
    .st-key-arb_rows [data-testid="stColumn"] > [data-testid="stVerticalBlock"] {justify-content: center; gap: 0;}
    .st-key-arb_rows [data-testid="stMarkdownContainer"], .st-key-arb_rows [data-testid="stCaptionContainer"] {margin-bottom: 0 !important;}
    .st-key-arb_rows [data-testid="stMarkdownContainer"] p {margin: 0; font-size: 1.05rem; line-height: 1.35;}
    .st-key-arb_rows [data-testid="stCaptionContainer"] p {font-size: 1rem; font-weight: 600; margin: 0; white-space: nowrap;}
    .st-key-arb_rows [data-testid="stMarkdown"] > div:has(> label) {display: flex; align-items: center; gap: 0.35rem;}
    .st-key-arb_rows [data-testid="stMarkdown"] > div:has(> label) label {margin: 0; display: flex; align-items: center;}
    .pill {display: inline-block; padding: 0.2rem 0.8rem; border-radius: 6px; font-weight: 700; font-size: 0.95rem; line-height: 1.4;}
    .pill.good {background: #19d98b; color: #04210f;}
    .pill.mid {background: #ffcc33; color: #2a2000;}
    .pill.bad {background: #ff5c6a; color: #2b0509;}
    .tone {font-weight: 600;}
    .tone.good {color: #19d98b;}
    .tone.mid {color: #ffcc33;}
    .tone.bad {color: #ff5c6a;}
    .route-m {display: none; font-size: 0.85rem; opacity: 0.75; font-weight: 400;}
    .st-key-arb_rows button {padding: 0.25rem 0.75rem; min-height: 0; white-space: nowrap;}
    .st-key-arb_rows button p {font-size: 1rem;}
    @media (max-width: 640px) {
        .st-key-arb_rows [data-testid="stColumn"]:nth-child(2), .st-key-arb_rows [data-testid="stColumn"]:nth-child(n+6) {display: none;}
        .st-key-arb_rows [data-testid="stColumn"]:nth-child(1) {flex: 2.2 1 0 !important;}
        .st-key-arb_rows [data-testid="stHorizontalBlock"] {gap: 0.6rem;}
        .pill {white-space: nowrap; padding: 0.2rem 0.6rem;}
        .route-m {display: block;}
        .st-key-arb_rows [data-testid="stMarkdownContainer"] p {font-size: 1rem;}
    }
    </style>
    """,
    unsafe_allow_html=True,
)


HOLD_CHOICES = [1, 3, 7, 14, 30]  # days a position is held, for Net APR and the Edge column
DEFAULT_HOLD = 3
BOOKS_EVERY_S = 120
LOADING_EVERY_S = 3  # while the first load runs in the background, the page checks this often

runtime = arb_runtime.start()


def snapshot_for(metric: str, min_carry: float):
    """(snapshot, built_at), or (None, None) while the first build runs in the background. never waits on venue APIs"""
    return runtime.cache.get((metric, float(min_carry)), lambda: arb_runtime.build_snapshot(metric, float(min_carry)))


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
        st.caption("A pair needs both its venues selected")
        notional = st.selectbox("Position per leg (USD)", [1_000, 10_000, 50_000, 100_000, 500_000], index=1, key="s_notional")
        metric = st.selectbox(
            "Funding metric", ["24h", "7d", "last"], index=0, key="s_metric",
            help="Rate the carry uses, the 24h average, the 7d average or the latest reading. Every rate is converted "
                 "to yearly first, so hourly and 8-hourly venues compare directly",
        )
        sustain_min = st.slider(
            "Sustainability, min (%)", 0, 100, 0, step=5, key="f_sustain",
            help="Share of the last 7 days the carry was positive, counting hours both venues reported",
        )
        be_max = st.slider(
            "Break-even, max (days)", 1.0, 14.0, 14.0, step=0.5, key="f_be",
            help="Days of funding to repay the round trip, adverse basis included",
        )
        min_carry = st.slider(
            "Carry, min (APR %)", 10, 200, 10, step=5, key="f_carry",
            help="Pairs below this carry are not fetched, so higher means fewer API calls",
        )
        risk_max = st.select_slider("Spread risk, max", ["Low", "Med", "High"], value="High", key="f_risk")
        hold = st.select_slider(
            "Hold (days)", HOLD_CHOICES, value=DEFAULT_HOLD, key="f_hold",
            help="How long you plan to hold, entry and exit cost is paid once so a longer hold raises net APR",
        )
        hide_neg = st.toggle(
            "Hide pairs that lose money over the hold", value=True, key="f_neg",
            help="Net APR is negative when entry and exit cost is more than the carry earned over the hold",
        )
        counts_slot = st.empty()
    search = bar[2].text_input(
        "Search", placeholder="Filter by token, e.g. BTC", label_visibility="collapsed", key="f_search",
        help="Shows only tickers containing this text, from the pairs already listed",
    )
    return sort_by, [k for k, name in VENUE_NAMES.items() if name in names_sel], float(notional), metric, \
        sustain_min, be_max, min_carry, risk_max, float(hold), hide_neg, search.strip(), counts_slot


sort_by, venues_sel, notional, metric, sustain_min, be_max, min_carry, risk_max, hold, hide_neg, search, counts_slot = controls()

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
if hold != DEFAULT_HOLD:
    chips.append(f"Hold {hold:g}d")
if not hide_neg:
    chips.append("Losing pairs shown")
if search:
    chips.append(f"Search “{search}”")
if chips:
    st.markdown("".join(f'<span class="chip">{html.escape(c)}</span>' for c in chips), unsafe_allow_html=True)

status_slot = st.empty()
table_slot = st.container()


def pct(x):
    return None if x is None else x * 100


def pair_key(r):
    return f"{r['ticker']}|{r['short']}|{r['long']}"


def clear_pick():
    """the dialog's close button was used: reset the picker, so picking the same pair again reopens it"""
    st.session_state["arb_pick"] = None


def open_detail(r, hist, hold):
    """show the details in a dialog styled as a right hand drawer (see the css above). it has its own close button"""
    @st.dialog(arb_view.detail_title(r, VENUE_NAMES), width="large", on_dismiss=clear_pick)
    def drawer():
        arb_view.detail(r, hist, VENUE_NAMES, hold)

    drawer()


def open_pick(key):
    st.session_state["arb_pick"] = key


# pair, route, verdict, details, net APR, break-even, sustain, risk, open interest, volume
GRID = [1.1, 2.2, 1.3, 1.3, 1.2, 1.2, 1.0, 1.1, 1.7, 1.7]


def md_usd(x):
    """usd_short for a markdown cell: the dollar sign is escaped, or '$139k / $2.0M' renders as a formula"""
    return "n/a" if x is None else usd_short(x).replace("$", "\\$")


HEADERS = [
    ("Pair", None),
    ("Short → Long", "Short the venue with the higher funding rate, long the other"),
    ("Verdict", "GOOD: break-even within 3 days (adverse basis included) and no warning. OK: within 7 days and at most one "
                "warning. Otherwise MARGINAL. Warnings are adverse basis, unstable carry, a leg thinner than the position "
                "within 10 bps, a stale Variational quote"),
    ("More info", None),
    ("Net APR", None),
    ("Break-even", "Days of funding to repay the round trip, adverse basis included"),
    ("Sustain", "Share of the last 7 days the carry was positive, from venue funding history. 80% or more is green, 50% or more is amber"),
    ("Risk", "Slippage plus adverse basis, or a leg thinner than the position"),
    ("OI short / long", "Open interest in USD, short / long leg, n/a if the venue does not report it"),
    ("Volume 24h", "24h volume in USD, short / long leg, n/a if the venue does not report it"),
]


@st.fragment
def details_button(r, hold):
    """one fragment per row, so a click re-renders this button and its drawer only and not the whole table"""
    key = pair_key(r)
    st.button("Details ›", key=f"d|{key}", on_click=open_pick, args=(key,))
    if st.session_state.get("arb_pick") == key:
        try:
            hist = funding_history.pair_history(r["short"], r["long"], r["ticker"]) if runtime.recording else None
        except Exception:
            hist = None
        open_detail(r, hist, hold)


def screener_rows(rows, hold):
    """compact grid, one line per pair, each with a Details button. no horizontal scroll on a phone: see the css"""
    with st.container(key="arb_rows"):
        head = st.columns(GRID, vertical_alignment="center")
        for col, (label, tip) in zip(head, [(n, t if n != "Net APR" else f"Carry after round trip cost and adverse basis, yearly, for a {hold:g} day hold, longer holds raise it") for n, t in HEADERS]):
            col.caption(label, help=tip)
        for r in rows:
            c = st.columns(GRID, vertical_alignment="center")
            short, long_ = VENUE_NAMES.get(r["short"], r["short"]), VENUE_NAMES.get(r["long"], r["long"])
            c[0].markdown(f'**{r["ticker"]}**<div class="route-m">{short} → {long_}</div>', unsafe_allow_html=True)
            c[1].markdown(f"{short} → {long_}")
            c[2].markdown(badge(r["verdict"]), unsafe_allow_html=True)
            with c[3]:
                details_button(r, hold)
            c[4].markdown(net_apr_text(r.get("net_apr")), unsafe_allow_html=True)
            c[5].markdown(days_short(r["be_all_d"]))
            c[6].markdown(sustain_text(r["sustain"]), unsafe_allow_html=True)
            c[7].markdown(badge(r["risk"]), unsafe_allow_html=True)
            c[8].markdown(f"{md_usd(r['oi_short'])} / {md_usd(r['oi_long'])}")
            c[9].markdown(f"{md_usd(r['vol_short'])} / {md_usd(r['vol_long'])}")


def status(text, stale=False):
    status_slot.markdown(
        f'<span class="live-dot{" stale" if stale else ""}"></span><span class="live-text">{html.escape(text)}</span>',
        unsafe_allow_html=True,
    )


def table_run_every():
    """short polling while the first load runs in the background, then the normal refresh. a full rerun switches between them"""
    return LOADING_EVERY_S if snapshot_for(metric, min_carry)[0] is None else BOOKS_EVERY_S


@st.fragment(run_every=table_run_every())
def table(venues_sel, notional, metric, sustain_min, be_max, min_carry, risk_max, hold, hide_neg, search, sort_by):
    """re-runs on its own and refreshes the order books and this table only, not the controls above it"""
    snapshot, _ = snapshot_for(metric, min_carry)
    if snapshot is None:
        err = runtime.cache.error.get((metric, float(min_carry)))
        status("Venue data failed, retrying" if err else "Loading venues", stale=True)
        if err:
            st.caption(str(err).lower())
        return
    if "arb_ready" not in st.session_state or st.session_state["arb_ready"] != (metric, min_carry):
        st.session_state["arb_ready"] = (metric, min_carry)
        st.rerun()  # full rerun once, so the fragment switches from loading polls to the normal refresh
    snapshot.refresh_books(min_age_s=BOOKS_EVERY_S - 20)

    age = (datetime.now(timezone.utc) - snapshot.books_at).total_seconds()
    status(f"LIVE {age:.0f}s ago", stale=age > 2 * BOOKS_EVERY_S + 30)

    # priced from cached data, so changing the position makes no API calls
    all_rows = snapshot.rows(notional, hold)

    def keep(r):
        return (
            r["short"] in venues_sel and r["long"] in venues_sel
            and (sustain_min == 0 or (r["sustain"] is not None and r["sustain"] * 100 >= sustain_min))
            and r["be_all_d"] is not None and r["be_all_d"] <= be_max
            and RISK_RANK.get(r["risk"], 0) <= RISK_RANK[risk_max]
            and search.upper() in r["ticker"].upper()
            and not (hide_neg and r["net_apr"] is not None and r["net_apr"] < 0)
        )

    rows = [r for r in all_rows if keep(r)]
    if SORTS[sort_by]:
        rows = sorted(rows, key=SORTS[sort_by])
    counts_slot.caption(f"{len(rows)} of {len(all_rows)} pairs")

    if snapshot.missing:
        st.caption("No data from " + ", ".join(VENUE_NAMES.get(k, k) for k in snapshot.missing) + ", their pairs are missing")

    if not rows:
        st.caption("No pairs match, loosen a filter")
        return

    screener_rows(rows, hold)
    fees = ", ".join(f"{VENUE_NAMES.get(k, k)} {v:g}" for k, v in funding_arbs.taker_bps().items() if k in VENUE_NAMES)
    with st.expander("Assumptions"):
        st.markdown(
            "\n".join(f"- {line}" for line in [
                "Short the venue with the higher funding rate, long the other, same USD size on each leg",
                "Round trip cost = 2 x (slippage + taker fees on both legs), entry plus exit",
                f"Net APR = carry after round trip cost and adverse basis, yearly, for a {hold:g} day hold",
                f"Taker fees in bps, from trading-tools' venues.toml: {fees}; Phoenix uses each market's own fee, QFEX shows its single-stock rate and indices and commodities pay less",
                "Break-even = round trip cost / daily carry, adverse basis included",
                f"Edge = expected carry over a {hold:g} day hold, less round trip cost and adverse basis",
                "Spread risk is Med above 5 bps and High above 12 bps of slippage plus adverse basis, or when a leg is thinner than the position",
                "24h, 7d and 30d carry averages come from hourly samples this app records, so they fill in over time; volume and open interest show only where a venue reports them",
                f"Funding fetched {snapshot.fetched_at:%H:%M:%S} UTC (every {arb_runtime.FUNDING_TTL_S // 60} minutes), order books {snapshot.books_at:%H:%M:%S} UTC (every {BOOKS_EVERY_S // 60} minutes)",
            ])
        )


with table_slot:
    table(venues_sel, notional, metric, sustain_min, be_max, min_carry, risk_max, hold, hide_neg, search, sort_by)
