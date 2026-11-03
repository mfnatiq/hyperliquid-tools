"""independent network calls run together, and the leaderboard date reads one column. a threading.Barrier only opens when
every call is in flight at once, so a one-after-another version fails with BrokenBarrierError. no network."""
import threading
from collections import Counter
from datetime import datetime
from types import SimpleNamespace

import pytest
from sqlalchemy import create_engine, insert

from src import funding_arbs
from src.trade import trade_data, trade_leaderboard as tl


def test_candlestick_data_fetches_every_token_at_once_and_keeps_token_order():
    bar = threading.Barrier(3, timeout=3)

    class Info:
        def candles_snapshot(self, name, interval, startTime, endTime):
            bar.wait()
            px = {"A": 2.0, "B": 10.0, "C": 1.0}[name]
            return [{"t": 86_400_000, "c": str(px), "v": "3"}, {"t": 2 * 86_400_000, "c": str(px), "v": "1"}]
    df = trade_data.get_candlestick_data(Info(), ["A", "B", "C"], ["tA", "tB", "tC"])
    assert list(df["token_name"]) == ["tA", "tA", "tB", "tB", "tC", "tC"]
    assert list(df["volume_usd"]) == [6.0, 2.0, 30.0, 10.0, 3.0, 1.0]
    assert list(df["cumulative_volume_usd"]) == [6.0, 8.0, 30.0, 40.0, 3.0, 4.0]


@pytest.mark.parametrize("fn,table", [("get_leaderboard_last_updated", "metadata_table"),
                                      ("get_xyz_leaderboard_last_updated", "xyz_leaderboard_metadata_table")])
def test_last_updated_reads_the_date_column_and_gives_none_for_an_empty_table(monkeypatch, tmp_path, fn, table):
    eng = create_engine(f"sqlite:///{tmp_path / 'l.db'}", future=True)
    monkeypatch.setattr(tl, "engine", eng)
    tl.metadata.create_all(eng)
    assert getattr(tl, fn)() is None
    when = datetime(2026, 11, 2, 3, 4, 5)
    with eng.begin() as c:
        c.execute(insert(getattr(tl, table)).values(id=1, last_updated_at=when))
    assert getattr(tl, fn)() == when


class _Venue:
    has_books = False

    def __init__(self, key, bar, names):
        self.key, self.bar, self.names, self.books = key, bar, names, {}
        self.enriched = None

    def load(self):
        return set(self.names)

    def enrich(self, tickers, pool):
        self.bar.wait()
        self.enriched = list(tickers)

    def refresh(self):
        self.bar.wait()

    def volume_24h(self, t):
        self.bar.wait()
        return (100.0, 5)

    def open_interest(self, t):
        return 7.0


def test_snapshot_enriches_refreshes_and_reads_leg_metadata_at_once(monkeypatch):
    legs = 4                                                      # 2 venues x 2 tickers
    bar = threading.Barrier(legs, timeout=3)                       # the widest step, leg metadata, needs all 4 in flight
    two = threading.Barrier(2, timeout=3)
    vs = {"A": _Venue("A", two, {"BTC", "ETH"}), "B": _Venue("B", two, {"BTC", "ETH"})}
    cands = [{"ticker": t, "short": "A", "long": "B", "carry": c} for t, c in (("BTC", 0.5), ("ETH", 0.4))]

    def vol(self, t):                                              # leg metadata runs on the pool: all four legs wait together
        bar.wait()
        return (100.0, 5)
    for v in vs.values():
        v.volume_24h = vol.__get__(v)
    ns = SimpleNamespace(venues="a,b", workers=8, top=25)
    fake_fs = SimpleNamespace(
        make_parser=lambda: SimpleNamespace(parse_args=lambda argv: ns), FUNDING_VENUES="a,b",
        build_venues=lambda *a: vs, safe=lambda fn, *a, tag=None: fn(*a), Counter=Counter,
        build_candidates=lambda a, vs_, listed, overlap: (cands, []))
    monkeypatch.setattr(funding_arbs, "fs", fake_fs)
    snap = funding_arbs.Snapshot()
    assert [v.enriched for v in vs.values()] == [["BTC", "ETH"], ["BTC", "ETH"]]
    assert snap.meta == {(k, t): {"vol": 100.0, "oi": 7.0} for k in ("A", "B") for t in ("BTC", "ETH")}
    assert list(snap.meta) == [("A", "BTC"), ("A", "ETH"), ("B", "BTC"), ("B", "ETH")]
    assert snap.missing == []


def test_fan_out_keeps_call_order_and_raises_the_first_error_after_all_finish():
    done = []
    assert funding_arbs.fan_out([lambda: 1, lambda: 2]) == [1, 2]
    assert funding_arbs.fan_out([]) == []

    def bad():
        raise ValueError("boom")

    def ok():
        done.append(1)
    with pytest.raises(ValueError, match="boom"):
        funding_arbs.fan_out([bad, ok, ok])
    assert len(done) == 2
