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
        return [dict(ROW), dict(ROW, ticker="BLUR", net_apr=-0.539, be_all_d=5.1),
                dict(ROW, ticker="ETH", sustain=None, risk=None)]


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


def test_the_page_shows_a_quiet_loading_status_at_once_while_the_first_build_runs(monkeypatch):
    gate = threading.Event()
    monkeypatch.setattr(arb_runtime, "build_snapshot", lambda m, c: (gate.wait(5), FakeSnapshot())[1])
    at = run_page()
    assert not at.exception
    assert any("Loading venues" in m.value for m in at.markdown)
    assert len(at.info) == 0
    assert len(at.dataframe) == 0
    gate.set()


def wait_for_rows(**state):
    """a new AppTest session until the background build is done: AppTest cannot rerun a page with a segmented control"""
    for _ in range(50):
        at = AppTest.from_file("pages/arb_opportunities.py", default_timeout=30)
        for k, v in state.items():
            at.session_state[k] = v
        at.run()
        if any(b.label == "Details ›" for b in at.button):
            return at
        time.sleep(0.1)
    return at


def test_each_pair_is_one_line_with_a_details_button_after_the_verdict(monkeypatch):
    monkeypatch.setattr(arb_runtime, "build_snapshot", lambda m, c: FakeSnapshot())
    run_page()  # starts the background build
    at = wait_for_rows()
    assert not at.exception
    assert [b.key for b in at.button if b.label == "Details ›"] == ["d|ZRO|PHX|VAR", "d|ETH|PHX|VAR"]  # BLUR is hidden: negative net APR over 3 days
    text = [m.value for m in at.markdown]
    assert "**ZRO**  \nPhoenix → Variational" in text
    assert ":green-background[GOOD]" in text
    assert ":green[+50.0%]" in text
    assert text.count("n/a") == 2  # ETH has no sustain and no risk: shown as text, since a lone dash would render as a bullet
    assert "1d 8h" in text
    assert "\\$139k / \\$2.0M" in text  # dollar signs are escaped so markdown does not read them as a formula
    assert "\\$56k / \\$1.6M" in text


def test_a_failed_first_build_shows_the_error(monkeypatch):
    def boom(m, c):
        raise RuntimeError("only 1 venue(s) responded")
    monkeypatch.setattr(arb_runtime, "build_snapshot", boom)
    at = run_page()
    for _ in range(30):
        if any("Venue data failed" in m.value for m in at.markdown):
            break
        time.sleep(0.1)
        at = run_page()
    assert any("Venue data failed, retrying" in m.value for m in at.markdown)
    assert any(c.value == "only 1 venue(s) responded" for c in at.caption)


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
    # 48 hours recorded: the 24 hour window is more than half covered, the 7 and 30 day windows are not
    assert list(avg["Window"]) == ["Now", "24 hours", "7 days (building)", "30 days (building)"]
    assert list(avg["Carry (APR %)"][:2]) == pytest.approx([63.1, 49.2])
    assert avg["Carry (APR %)"][2:].isna().all()
    buttons = {b.proto.label: b.proto.url for b in at.get("link_button")}
    assert buttons == {"Short on Phoenix ↗": "https://phoenix.trade/?code=O88RSK0C",
                       "Long on Variational ↗": "https://omni.variational.io/?ref=OMNIMAMBO"}
    legs = at.dataframe[1].value
    assert list(legs["Leg"]) == ["Short Phoenix", "Long Variational"]
    assert list(legs["Volume 24h"]) == ["$56k", "-"]
    assert list(legs["Open interest"]) == ["$139k", "$2.0M"]
    assert any("48 hours recorded" in c.value and "Longer averages fill in over time" in c.value for c in at.caption)


def test_a_venue_without_a_referral_link_gets_no_button_and_no_note():
    def app():
        from src import arb_view
        arb_view.trade_links("Phoenix", "Arcus")
    at = AppTest.from_function(app).run()
    assert not at.exception
    assert [b.proto.label for b in at.get("link_button")] == ["Short on Phoenix ↗"]
    assert len(at.caption) == 0


def test_no_trade_heading_when_neither_venue_has_a_referral_link():
    def app():
        from src import arb_view
        arb_view.trade_links("Arcus", "Arcus")
    at = AppTest.from_function(app).run()
    assert not at.exception
    assert len(at.get("link_button")) == 0
    assert len(at.markdown) == 0


def test_scanner_notes_are_written_in_plain_words():
    from src.arb_view import plain_note
    names = {"ARCUS": "Arcus", "ONDO": "Ondo Perps", "VAR": "Variational"}
    assert plain_note("ARCUS funding = current rate only", names) == \
        "Arcus gives only its current funding rate, so stability is unchecked"
    assert plain_note("ONDO <$10,000 within 10bp", names) == \
        "Ondo Perps has under $10,000 of depth within 10 bps, so this size moves the price"
    assert plain_note("basis against 12bp", names) == "Basis is against us by 12 bps"
    assert plain_note("carry unstable (min 3.5%)", names) == "Carry is unstable, lowest recent reading 3.5%"
    assert plain_note("ARCUS underlying closed (off-hours)", names) == "Arcus market is closed, book is indicative"
    assert plain_note("something new", names) == "something new"


def test_the_notes_under_the_cost_table_are_one_line_each():
    from src.arb_view import notes_markdown
    r = dict(mid_basis_bps=4.6, risk="Med", edge_bps=8.0, note="ONDO <$10,000 within 10bp; ARCUS funding = current rate only")
    assert notes_markdown(r, 3.0, {"ARCUS": "Arcus", "ONDO": "Ondo Perps"}) == "\n".join([
        "- Mid basis +4.6 bps, above 0 favours us",
        "- Spread risk Med",
        "- Edge over 3 days 8.0 bps",
        "- Ondo Perps has under \\$10,000 of depth within 10 bps, so this size moves the price",
        "- Arcus gives only its current funding rate, so stability is unchecked",
    ])


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
    assert any("No history yet" in c.value for c in at.caption)


def test_the_losing_pair_shows_when_the_toggle_is_off(monkeypatch):
    monkeypatch.setattr(arb_runtime, "build_snapshot", lambda m, c: FakeSnapshot())
    run_page()
    at = wait_for_rows(f_neg=False)
    assert [b.key for b in at.button if b.label == "Details ›"] == ["d|ZRO|PHX|VAR", "d|BLUR|PHX|VAR", "d|ETH|PHX|VAR"]
    assert ":red[-53.9%]" in [m.value for m in at.markdown]
    assert any("Losing pairs shown" in m.value for m in at.markdown)


def test_a_picked_pair_opens_its_details(monkeypatch):
    monkeypatch.setattr(arb_runtime, "build_snapshot", lambda m, c: FakeSnapshot())
    run_page()
    at = wait_for_rows(arb_pick="ZRO|PHX|VAR")
    assert not at.exception
    labels = [b.proto.label for b in at.get("link_button")]  # the drawer's Trade buttons
    assert labels == ["Short on Phoenix ↗", "Long on Variational ↗"]
