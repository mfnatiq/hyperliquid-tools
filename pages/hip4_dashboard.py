import os
import sys
import json
import logging
import requests
from datetime import datetime, timezone
import pandas as pd
from dotenv import load_dotenv
import streamlit as st

logging.basicConfig(
    format="%(asctime)s [%(levelname)s] %(message)s",
    level=logging.INFO,
    stream=sys.stdout,
)
logger = logging.getLogger(__name__)

load_dotenv()

st.set_page_config("HIP-4 Markets", "🎯", layout="wide")
st.header("HIP-4 Prediction Markets")

HYDROMANCER_URL = "https://api.hydromancer.xyz/info"
HYDROMANCER_API_KEY = os.getenv("HYDROMANCER_API_KEY")


def _post(payload: dict) -> dict:
    resp = requests.post(
        HYDROMANCER_URL,
        data=json.dumps(payload),
        headers={
            "Content-Type": "application/json",
            "Authorization": f"Bearer {HYDROMANCER_API_KEY}",
        },
        timeout=15,
    )
    resp.raise_for_status()
    return resp.json()


def _parse_expiry(expiry: str | None) -> str:
    if not expiry:
        return "—"
    try:
        # format: "20260508-1245"
        return datetime.strptime(expiry, "%Y%m%d-%H%M").strftime("%Y-%m-%d %H:%M UTC")
    except ValueError:
        return expiry


def _total_volume(yes_stats: dict, no_stats: dict) -> float:
    return float(yes_stats.get("volumeNotional", 0)) + float(no_stats.get("volumeNotional", 0))


def _unique_traders(yes_stats: dict, no_stats: dict) -> int:
    # note: may double-count traders active on both sides
    return int(yes_stats.get("uniqueTraders", 0)) + int(no_stats.get("uniqueTraders", 0))


@st.cache_data(ttl=60, show_spinner=False)
def _fetch_active_markets() -> list[dict]:
    try:
        data = _post({"type": "registeredOutcomes", "limit": 100, "kind": "all"})
        return data.get("outcomes", [])
    except Exception as e:
        logger.error(f"failed to fetch active markets: {e}")
        return []


@st.cache_data(ttl=300, show_spinner=False)
def _fetch_settled_markets() -> list[dict]:
    try:
        data = _post({"type": "settledOutcomes", "limit": 100, "kind": "all"})
        return data.get("outcomes", [])
    except Exception as e:
        logger.error(f"failed to fetch settled markets: {e}")
        return []


def _build_active_df(outcomes: list[dict]) -> pd.DataFrame:
    rows = []
    for o in outcomes:
        yes = o.get("yesStats", {})
        no = o.get("noStats", {})
        rows.append({
            "Market": o.get("name", ""),
            "Underlying": o.get("underlying") or "—",
            "Class": o.get("class") or "—",
            "Period": o.get("period") or "—",
            "Expiry": _parse_expiry(o.get("expiry")),
            "Target Price": o.get("targetPrice") or "—",
            "YES Price": float(yes.get("lastPrice", 0)),
            "NO Price": float(no.get("lastPrice", 0)),
            "Volume (USD)": _total_volume(yes, no),
            "Traders": _unique_traders(yes, no),
        })
    df = pd.DataFrame(rows)
    return df.sort_values("Volume (USD)", ascending=False)


def _build_settled_df(outcomes: list[dict]) -> pd.DataFrame:
    rows = []
    for o in outcomes:
        yes = o.get("yesStats", {})
        no = o.get("noStats", {})
        won = "YES" if o.get("settleFraction") == "1" else "NO"
        rows.append({
            "Market": o.get("name", ""),
            "Underlying": o.get("underlying") or "—",
            "Class": o.get("class") or "—",
            "Period": o.get("period") or "—",
            "Expiry": _parse_expiry(o.get("expiry")),
            "Target Price": o.get("targetPrice") or "—",
            "Outcome": won,
            "Settle Price": o.get("settleDetails") or "—",
            "Volume (USD)": _total_volume(yes, no),
            "Traders": _unique_traders(yes, no),
        })
    df = pd.DataFrame(rows)
    return df.sort_values("Volume (USD)", ascending=False)


tab_active, tab_settled = st.tabs(["Active Markets", "Recently Settled"])

with tab_active:
    with st.spinner("Loading active markets..."):
        active_outcomes = _fetch_active_markets()

    if not active_outcomes:
        st.warning("No active markets found or API unavailable.")
    else:
        df_active = _build_active_df(active_outcomes)

        # filter by underlying
        underlyings = sorted(set(df_active["Underlying"].unique()) - {"—"})
        selected = st.selectbox("Filter by underlying", ["All"] + underlyings, key="active_underlying")
        if selected != "All":
            df_active = df_active[df_active["Underlying"] == selected]

        st.caption(f"{len(df_active)} active markets · refreshes every 60s")
        st.dataframe(
            df_active,
            use_container_width=True,
            hide_index=True,
            column_config={
                "YES Price": st.column_config.NumberColumn(format="%.3f"),
                "NO Price": st.column_config.NumberColumn(format="%.3f"),
                "Volume (USD)": st.column_config.NumberColumn(format="$%.2f"),
                "Traders": st.column_config.NumberColumn(format="%d"),
            },
        )

with tab_settled:
    with st.spinner("Loading settled markets..."):
        settled_outcomes = _fetch_settled_markets()

    if not settled_outcomes:
        st.warning("No settled markets found or API unavailable.")
    else:
        df_settled = _build_settled_df(settled_outcomes)

        underlyings = sorted(set(df_settled["Underlying"].unique()) - {"—"})
        selected = st.selectbox("Filter by underlying", ["All"] + underlyings, key="settled_underlying")
        if selected != "All":
            df_settled = df_settled[df_settled["Underlying"] == selected]

        st.caption(f"{len(df_settled)} recently settled markets · refreshes every 5 min")
        st.dataframe(
            df_settled,
            use_container_width=True,
            hide_index=True,
            column_config={
                "Outcome": st.column_config.TextColumn(),
                "Volume (USD)": st.column_config.NumberColumn(format="$%.2f"),
                "Traders": st.column_config.NumberColumn(format="%d"),
            },
        )
