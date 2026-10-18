import threading
import time
from datetime import datetime, timezone

import pytest
import streamlit as st
from streamlit.testing.v1 import AppTest

from src import arb_runtime, funding_history

ROW = dict(
    ticker="ZRO", short="PHX", long="VAR", verdict="GOOD", carry_apr=0.631, net_apr=0.5, sustain=1.0, risk="Low",
    be_all_d=1.3333, breakeven_d=1.2, rt_cost_bps=9.0, slip_short_bps=1.0, slip_long_bps=2.0, mid_basis_bps=-1.5,
    edge_bps=30.0, note="", short_fee_bps=1.5, long_fee_bps=0.0, vol_short=56_000.0, vol_long=1_600_000.0,
    oi_short=139_000.0, oi_long=2_000_000.0, carry_pct=50,
)


class FakeSnapshot:
    missing: list = []
    fetched_at = books_at = datetime.now(timezone.utc)

    def refresh_books(self, min_age_s=45.0):
        return False

    def rows(self, notional, hold_days=3.0):
        return [dict(ROW)]


@pytest.fixture(autouse=True)
def fresh_runtime(monkeypatch, tmp_path):
    monkeypatch.setenv("DATABASE_URL", f"sqlite:///{tmp_path / 'p.db'}")
    monkeypatch.setattr(funding_history, "_engine", None)
    monkeypatch.setattr(funding_history, "start_recorder", lambda *a, **k: None)
    st.cache_resource.clear()
    yield
    st.cache_resource.clear()


def run_page():
    return AppTest.from_file("pages/arb_opportunities.py", default_timeout=30).run()


def test_the_page_shows_a_loading_note_at_once_while_the_first_build_runs(monkeypatch):
    gate = threading.Event()
    monkeypatch.setattr(arb_runtime, "build_snapshot", lambda m, c: (gate.wait(5), FakeSnapshot())[1])
    at = run_page()
    assert not at.exception
    assert any("Loading funding rates" in i.value for i in at.info)
    assert len(at.dataframe) == 0
    gate.set()


def test_the_table_appears_once_the_background_build_finishes(monkeypatch):
    monkeypatch.setattr(arb_runtime, "build_snapshot", lambda m, c: FakeSnapshot())
    at = run_page()
    for _ in range(50):
        if len(at.dataframe):
            break
        time.sleep(0.1)
        at = run_page()  # a new session each time: AppTest cannot rerun a page with a segmented control
    assert not at.exception
    df = at.dataframe[0].value
    assert df.loc[0, "Pair"] == "ZRO"
    assert df.loc[0, "Route"] == "Phoenix → Variational"
    assert df.loc[0, "Break-even"] == "1d 8h"
    assert list(df.columns)[:3] == ["Pair", "Route", "Verdict"]
    assert df.loc[0, "OI (short / long)"] == "$139k / $2.0M"
    assert df.loc[0, "Volume 24h (short / long)"] == "$56k / $1.6M"


def test_a_failed_first_build_shows_the_error(monkeypatch):
    def boom(m, c):
        raise RuntimeError("only 1 venue(s) responded")
    monkeypatch.setattr(arb_runtime, "build_snapshot", boom)
    at = run_page()
    for _ in range(30):
        if at.error:
            break
        time.sleep(0.1)
        at = run_page()
    assert any("only 1 venue(s) responded" in e.value for e in at.error)


def test_the_detail_panel_shows_averages_and_the_fee_breakdown():
    def app():
        from src import arb_view
        row = {"ticker": "ZRO", "short": "PHX", "long": "VAR", "carry_apr": 0.631, "net_apr": 0.5, "be_all_d": 1.3333,
               "rt_cost_bps": 9.0, "sustain": 1.0, "risk": "Low", "mid_basis_bps": -1.5, "edge_bps": 30.0, "note": "",
               "short_fee_bps": 1.5, "long_fee_bps": 0.0, "slip_short_bps": 1.0, "slip_long_bps": 2.0,
               "vol_short": 56_000.0, "vol_long": None, "oi_short": 139_000.0, "oi_long": 2_000_000.0}
        hist = {"avg_24h": 0.492, "avg_7d": 0.54, "avg_30d": 0.534, "sustain_30d": 0.9, "hours": 48}
        arb_view.detail(row, hist, {"PHX": "Phoenix", "VAR": "Variational"}, 3.0)
    at = AppTest.from_function(app).run()
    assert not at.exception
    assert [m.value for m in at.metric] == ["50.0%", "63.1%", "1d 8h", "9.0 bps"]
    avg = at.dataframe[0].value
    assert list(avg["Window"]) == ["Now", "24 hours", "7 days", "30 days"]
    assert list(avg["Carry (APR %)"]) == pytest.approx([63.1, 49.2, 54.0, 53.4])
    buttons = {b.proto.label: b.proto.url for b in at.get("link_button")}
    assert buttons == {"Short on Phoenix ↗": "https://phoenix.trade/?code=O88RSK0C",
                       "Long on Variational ↗": "https://omni.variational.io/?ref=OMNIMAMBO"}
    legs = at.dataframe[1].value
    assert list(legs["Leg"]) == ["Short Phoenix", "Long Variational"]
    assert list(legs["Volume 24h"]) == ["$56k", "-"]
    assert list(legs["Open interest"]) == ["$139k", "$2.0M"]
    assert any("48 hours recorded (2.0 days)" in c.value and "partial" in c.value for c in at.caption)


def test_a_venue_without_a_referral_link_gets_a_note_not_a_button():
    def app():
        from src import arb_view
        arb_view.trade_links("Phoenix", "Arcus")
    at = AppTest.from_function(app).run()
    assert not at.exception
    assert [b.proto.label for b in at.get("link_button")] == ["Short on Phoenix ↗"]
    assert [c.value for c in at.caption] == ["Long on Arcus: no referral link yet"]


def test_the_detail_panel_says_so_when_no_history_exists_yet():
    def app():
        from src import arb_view
        row = {"ticker": "ZRO", "short": "PHX", "long": "VAR", "carry_apr": 0.6, "net_apr": None, "be_all_d": None,
               "rt_cost_bps": None, "sustain": None, "risk": None, "mid_basis_bps": None, "edge_bps": None, "note": "",
               "short_fee_bps": 1.5, "long_fee_bps": 0.0, "slip_short_bps": None, "slip_long_bps": None,
               "vol_short": None, "vol_long": None, "oi_short": None, "oi_long": None}
        arb_view.detail(row, None, {}, 3.0)
    at = AppTest.from_function(app).run()
    assert not at.exception
    assert any("has not been recorded yet" in c.value for c in at.caption)
