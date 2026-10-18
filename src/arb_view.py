"""the detail panel of the Arb Opportunities screener"""
import pandas as pd
import streamlit as st

from src.funding_arbs import days_short, usd_short


def pct(x):
    return None if x is None else x * 100


def fmt_pct(x):
    return "-" if x is None else f"{x * 100:.1f}%"


def detail(r, hist, venue_names, hold_days):
    """everything about one pair, shown under the table when its row is tapped. hist is funding_history.pair_history() or None"""
    short, long_ = venue_names.get(r["short"], r["short"]), venue_names.get(r["long"], r["long"])
    with st.container(border=True):
        st.markdown(f"#### {r['ticker']}: short {short}, long {long_}")
        cols = st.columns(4)
        cols[0].metric("Net APR", fmt_pct(r.get("net_apr")), help=f"Carry after the round trip cost and adverse basis, for a {hold_days:g} day hold")
        cols[1].metric("Carry now", fmt_pct(r["carry_apr"]))
        cols[2].metric("Break-even", days_short(r["be_all_d"]), help="Days of funding needed to repay the round trip, including adverse basis")
        cols[3].metric("Round trip", "-" if r["rt_cost_bps"] is None else f"{r['rt_cost_bps']:.1f} bps")

        st.markdown("**Carry against its averages** (annual)")
        if hist and hist["hours"]:
            avgs = [("Now", r["carry_apr"]), ("24 hours", hist["avg_24h"]), ("7 days", hist["avg_7d"]), ("30 days", hist["avg_30d"])]
            st.dataframe(
                pd.DataFrame({"Window": [n for n, _ in avgs], "Carry (APR %)": [pct(v) for _, v in avgs]}),
                hide_index=True, use_container_width=True,
                column_config={"Carry (APR %)": st.column_config.ProgressColumn(
                    "Carry (APR %)", format="%.1f%%", min_value=0, max_value=max(1.0, max((pct(v) or 0) for _, v in avgs)))},
            )
            days = hist["hours"] / 24
            note = f"{hist['hours']} hours recorded ({days:.1f} days)."
            if hist["hours"] < 30 * 24:
                note += " The 30 day average fills in as the recorder runs, so treat it as partial until then."
            if hist["sustain_30d"] is not None:
                note += f" Carry was positive in {hist['sustain_30d'] * 100:.0f}% of recorded hours."
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
