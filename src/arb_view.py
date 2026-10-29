"""the detail panel of the Arb Opportunities screener"""
import re

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
    a venue without a referral entry gets no button and no note."""
    links = [(side, venue, referral(venue)) for side, venue in (("Short", short), ("Long", long_))]
    links = [(side, venue, ref) for side, venue, ref in links if ref]
    if not links:
        return
    st.markdown("**Trade**")
    for col, (side, venue, ref) in zip(st.columns(len(links)), links):
        col.link_button(f"{side} on {venue} ↗", ref["url"], help=ref.get("perk"), use_container_width=True)


def plain_note(note, venue_names):
    """one scanner note in plain words. the scanner writes short tags such as 'ARCUS funding = current rate only'.
    an unknown tag is shown as it is."""
    def v(key):
        return venue_names.get(key, key)

    rules = [
        (r"basis against (\d+)bp", lambda m: f"The basis is against us by {m[1]} bps."),
        (r"carry unstable \(min (-?[\d.]+)%\)", lambda m: f"The carry is unstable. Its lowest recent reading was {m[1]}%."),
        (r"(\w+) <\$([\d,]+) within 10bp", lambda m: f"{v(m[1])} has less than ${m[2]} of depth within 10 bps, so a position this size moves the price."),
        (r"stale VAR quote", lambda m: "The Variational quote is more than a minute old."),
        (r"(\w+) funding = current rate only", lambda m: f"{v(m[1])} reports only its current funding rate, so the carry cannot be checked for stability."),
        (r"(\w+) (?:market closed|underlying closed)(?: \(off-hours\))?", lambda m: f"The {v(m[1])} market is closed, so its book is indicative."),
        (r"EXT RFQ: indicative book", lambda m: "Extended prices on request, so its book is indicative."),
        (r"(\w+) book too thin", lambda m: f"The {v(m[1])} book is too thin for this position size."),
        (r"(\w+) no data", lambda m: f"No data from {v(m[1])}."),
    ]
    for pattern, text in rules:
        m = re.fullmatch(pattern, note.strip())
        if m:
            return text(m)
    return note.strip()


def notes_markdown(r, hold_days, venue_names):
    """the lines under the cost table, one per fact, as a markdown list. the dollar sign is escaped for markdown"""
    basis, edge = r["mid_basis_bps"], r["edge_bps"]
    lines = [
        "Mid basis: " + ("n/a" if basis is None else f"{basis:+.1f} bps (above 0 is in our favour)"),
        f"Spread risk: {r['risk'] or 'n/a'}",
        f"Edge over {hold_days:g} days: " + ("n/a" if edge is None else f"{edge:.1f} bps"),
    ]
    lines += [plain_note(n, venue_names) for n in (r["note"] or "").split("; ") if n.strip()]
    return "\n".join(f"- {line}" for line in lines).replace("$", "\\$")


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
            note = f"{hours} hour{'s' if hours != 1 else ''} recorded."
            if hours < 30 * 24:
                note += " Longer averages appear as more hours are recorded."
            if hist["sustain_30d"] is not None and hours >= 24:
                note += f" The carry was positive in {hist['sustain_30d'] * 100:.0f}% of them."
            st.caption(note)
        else:
            st.caption("No history is recorded for this pair yet. The app adds one sample an hour.")
        if r["sustain"] is not None:
            st.caption(f"In the last 7 days the carry was positive in {r['sustain'] * 100:.0f}% of hours, from the venues' own funding history.")

        st.markdown("**Cost, by leg**")
        leg = lambda side, name: {
            "Leg": f"{side} {name}", "Taker fee (bps)": r[f"{side.lower()}_fee_bps"], "Slippage (bps)": r[f"slip_{side.lower()}_bps"],
            "Volume 24h": usd_short(r[f"vol_{side.lower()}"]), "Open interest": usd_short(r[f"oi_{side.lower()}"]),
        }
        legs = pd.DataFrame([leg("Short", short), leg("Long", long_)])
        st.dataframe(legs.style.format({"Taker fee (bps)": "{:.2f}", "Slippage (bps)": "{:.2f}"}, na_rep="-"), hide_index=True, use_container_width=True)
        st.markdown(notes_markdown(r, hold_days, venue_names))
