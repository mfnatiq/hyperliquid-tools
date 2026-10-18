"""the Arb Opportunities page's shared state: the background snapshot cache and the hourly funding recorder.

start() is called from home.py, so recording begins when the app boots and a first visitor finds the data warm.
one set of these objects exists per server process (st.cache_resource), not per browser session.
"""
from dataclasses import dataclass

import streamlit as st

from src import funding_arbs, funding_history
from src.snapshot_cache import BackgroundCache

FUNDING_TTL_S = 300
RECORD_EVERY_S = 3600
SNAPSHOT_TOP = 25
DEFAULT_KEY = ("24h", 10.0)  # the page's default metric and minimum carry


def build_snapshot(metric: str, min_carry: float):
    return funding_arbs.Snapshot(metric=metric, min_carry_pct=min_carry, top=SNAPSHOT_TOP)


@dataclass
class Runtime:
    cache: BackgroundCache
    recording: bool


@st.cache_resource(show_spinner=False)
def start() -> Runtime:
    """create the cache and start the recorder. a database error turns recording off, it never stops the site."""
    cache = BackgroundCache(ttl_s=FUNDING_TTL_S)
    if not funding_arbs.AVAILABLE:
        return Runtime(cache, False)
    try:
        funding_history.init()
    except Exception as e:
        print(f"funding history is off: {e}", flush=True)
        return Runtime(cache, False)

    def sample():
        # reuse the page's snapshot when it is warm, so the hourly run adds no venue calls
        snap, _ = cache.get(DEFAULT_KEY, lambda: build_snapshot(*DEFAULT_KEY))
        return (snap or build_snapshot(*DEFAULT_KEY)).samples()

    funding_history.start_recorder(sample, every_s=RECORD_EVERY_S)
    return Runtime(cache, True)
