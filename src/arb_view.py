"""the detail panel of the Arb Opportunities screener"""
import pandas as pd
import streamlit as st

from src.funding_arbs import days_short, usd_short
from src.utils.render_utils import referral


def pct(x):
    return None if x is None else x * 100


def fmt_pct(x):
    return "-" if x is None else f"{x * 100:.1f}%"


def detail_title(r, venue_names):
    return f"{r['ticker']}: short {venue_names.get(r['short'], r['short'])}, long {venue_names.get(r['long'], r['long'])}"


def trade_links(short, long_):
    """one button per leg, each opening that venue's sign-up link in a new tab. two buttons, never one click for both.
    a venue without a referral entry gets a note instead of a button."""
    st.markdown("**Trade**")
    cols = st.columns(2)
    for col, side, venue in zip(cols, ("Short", "Long"), (short, long_)):
        ref = referral(venue)
        if ref:
            col.link_button(f"{side} on {venue} ↗", ref["url"], help=ref.get("perk"), use_container_width=True)
        else:
            col.caption(f"{side} on {venue}: no referral link yet")


def detail(r, hist, venue_names, hold_days):
    """everything about one pair, drawn inside the page's drawer. hist is funding_history.pair_history() or None"""
    short, long_ = venue_names.get(r["short"], r["short"]), venue_names.get(r["long"], r["long"])
    with st.container():
        top, bottom = st.columns(2), st.columns(2)  # two by two, so a value never truncates in the narrow drawer
        top[0].metric("Net APR", fmt_pct(r.get("net_apr")), help=f"Carry after the round trip cost and adverse basis, for a {hold_days:g} day hold")
        top[1].metric("Carry now", fmt_pct(r["carry_apr"]))
        bottom[0].metric("Break-even", days_short(r["be_all_d"]), help="Days of funding needed to repay the round trip, including adverse basis")
        bottom[1].metric("Round trip", "-" if r["rt_cost_bps"] is None else f"{r['rt_cost_bps']:.1f} bps")

        trade_links(short, long_)

        st.markdown("**Carry against its averages** (annual)")
        if hist and hist["hours"]:
            hours = hist["hours"]
            windows = [("24 hours", 24, hist["avg_24h"]), ("7 days", 7 * 24, hist["avg_7d"]), ("30 days", 30 * 24, hist["avg_30d"])]
            # a window is shown once at least half of its hours are recorded, so one sample never passes for a 30 day average
            avgs = [("Now", r["carry_apr"])] + [(n if hours >= w / 2 else f"{n} (building)", v if hours >= w / 2 else None) for n, w, v in windows]
            st.dataframe(
                pd.DataFrame({"Window": [n for n, _ in avgs], "Carry (APR %)": [pct(v) for _, v in avgs]}),
                hide_index=True, use_container_width=True,
                column_config={"Carry (APR %)": st.column_config.ProgressColumn(
                    "Carry (APR %)", format="%.1f%%", min_value=0, max_value=max(1.0, max((pct(v) or 0) for _, v in avgs)))},
            )
            note = f"{hours} hour{'s' if hours != 1 else ''} of history recorded."
            if hours < 30 * 24:
                note += " Longer averages appear as the recorder keeps running."
            if hist["sustain_30d"] is not None and hours >= 24:
                note += f" Carry was positive in {hist['sustain_30d'] * 100:.0f}% of the recorded hours."
            st.caption(note)
        else:
            st.caption("Carry history for this pair has not been recorded yet. It builds up one sample per hour while the app runs.")
        if r["sustain"] is not None:
            st.caption(f"Sustainability from the venues' own funding history, last 7 days: {r['sustain'] * 100:.0f}% of hours positive.")

        st.markdown("**Cost, by leg**")
        leg = lambda side, name: {
            "Leg": f"{side} {name}", "Taker fee (bps)": r[f"{side.lower()}_fee_bps"], "Slippage (bps)": r[f"slip_{side.lower()}_bps"],
            "Volume 24h": usd_short(r[f"vol_{side.lower()}"]), "Open interest": usd_short(r[f"oi_{side.lower()}"]),
        }
        legs = pd.DataFrame([leg("Short", short), leg("Long", long_)])
        st.dataframe(legs.style.format({"Taker fee (bps)": "{:.2f}", "Slippage (bps)": "{:.2f}"}, na_rep="-"), hide_index=True, use_container_width=True)
        basis, edge = r["mid_basis_bps"], r["edge_bps"]
        st.caption(
            f"Mid basis {'-' if basis is None else f'{basis:+.1f} bps'} (above 0 is in our favour). "
            f"Spread risk {r['risk']}. Edge over {hold_days:g} days {'-' if edge is None else f'{edge:.1f} bps'}."
            + (f" {r['note']}" if r["note"] else "")
        )
