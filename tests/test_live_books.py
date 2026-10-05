import math

import pytest

from src.live_books.book import Ladders, PriceLadder, norm_book, slippage_bps
from src.live_books.history import Ring, Series, decimate

ASKS = [(100.0, 5.0), (101.0, 10.0), (103.0, 20.0)]            # 500, 1010, 2060 usd per level
BIDS = [(99.0, 5.0), (98.0, 10.0), (96.0, 20.0)]


def test_a_clip_inside_the_first_level_pays_that_level():
    assert PriceLadder(ASKS).vwap(400.0) == (100.0, 1.0)


def test_a_clip_across_levels_is_volume_weighted():
    # 500 usd at 100 buys 5, 500 usd at 101 buys 4.950495..., so 1000 usd buys 9.950495 and pays 100.4975...
    avg, filled = PriceLadder(ASKS).vwap(1000.0)
    assert avg == pytest.approx(1000.0 / (5.0 + 500.0 / 101.0)) and filled == 1.0
    assert round(avg, 4) == 100.4975


def test_a_clip_that_exactly_empties_a_level_does_not_touch_the_next():
    assert PriceLadder(ASKS).vwap(500.0) == (100.0, 1.0)


def test_a_clip_deeper_than_the_book_gets_the_average_of_what_exists_and_the_filled_share():
    avg, filled = PriceLadder(ASKS).vwap(7000.0)               # the book holds 3570 usd
    assert filled == pytest.approx(3570.0 / 7000.0)
    assert avg == pytest.approx(3570.0 / 35.0)


def test_an_empty_side_gives_no_price():
    assert PriceLadder([]).vwap(1000.0) == (None, 0.0)


def test_slippage_sign_is_positive_when_worse_than_mid_for_both_sides():
    assert slippage_bps(100.5, 100.0, buy=True) == pytest.approx(50.0)
    assert slippage_bps(99.5, 100.0, buy=False) == pytest.approx(50.0)
    assert slippage_bps(99.5, 100.0, buy=True) == pytest.approx(-50.0)


def test_ladders_price_both_sides_against_the_mid():
    lad = Ladders(norm_book(BIDS, ASKS))                        # mid 99.5
    bps, filled = lad.clip(400.0, buy=True)
    assert filled == 1.0 and bps == pytest.approx((100.0 - 99.5) / 99.5 * 1e4)
    bps, _ = lad.clip(400.0, buy=False)
    assert bps == pytest.approx((99.5 - 99.0) / 99.5 * 1e4)


def test_norm_book_sorts_drops_empty_levels_and_rejects_crossed_or_empty_books():
    b = norm_book([("98", "1"), ("99", "2"), ("97", "0")], [("101", "1"), ("100", "2")])
    assert b["bids"] == [(99.0, 2.0), (98.0, 1.0)] and b["asks"] == [(100.0, 2.0), (101.0, 1.0)] and b["mid"] == 99.5
    assert norm_book([("100", "1")], [("100", "1")]) is None
    assert norm_book([], [("100", "1")]) is None


def test_ring_overwrites_the_oldest_value_when_full():
    r = Ring(size=3, step=1)
    for t, v in enumerate([1, 2, 3, 4, 5]):
        r.put(t, v)
    t, v = r.read()
    assert list(t) == [2, 3, 4] and list(v) == [3.0, 4.0, 5.0]


def test_ring_leaves_gaps_as_missing_not_as_stale_values():
    r = Ring(size=10, step=1)
    r.put(0, 1.0)
    r.put(4, 5.0)                                                # seconds 1 to 3 had no data
    t, v = r.read()
    assert list(t) == [0, 4] and list(v) == [1.0, 5.0]


def test_ring_keeps_the_last_value_in_a_step_and_ignores_late_data():
    r = Ring(size=10, step=10)
    r.put(1, 1.0)
    r.put(9, 2.0)
    r.put(15, 3.0)
    r.put(2, 99.0)                                               # older than the newest slot
    t, v = r.read()
    assert list(t) == [0, 10] and list(v) == [2.0, 3.0]


def test_a_long_gap_clears_the_whole_ring():
    r = Ring(size=5, step=1)
    for t in range(5):
        r.put(t, float(t + 1))
    r.put(100, 7.0)
    t, v = r.read()
    assert list(t) == [100] and list(v) == [7.0]


def test_series_reads_fine_data_for_short_windows_and_coarse_for_long_ones():
    s = Series(fine_step=1, fine_span=60, coarse_step=10, coarse_span=600)
    for t in range(0, 300):
        s.put(t, float(t))
    t, v = s.read(30)                                            # fine ring
    assert list(t[:2]) == [269, 270] and t[-1] == 299
    t, v = s.read(250)                                           # past the fine span, so the coarse ring
    assert t[1] - t[0] == 10 and v[-1] == 299.0


def test_float32_keeps_a_100000_price_to_a_fraction_of_a_bp():
    r = Ring(size=2, step=1)
    r.put(0, 100000.01)
    assert abs(float(r.read()[1][0]) - 100000.01) / 100000.01 * 1e4 < 0.001


def test_decimate_averages_equal_chunks_and_keeps_short_series():
    import numpy as np
    t, v = np.arange(10.0), np.arange(10.0)
    assert decimate(t, v, 100)[0] is t
    dt, dv = decimate(t, v, 5)
    assert list(dt) == [0.5, 2.5, 4.5, 6.5, 8.5] and list(dv) == [0.5, 2.5, 4.5, 6.5, 8.5]
    assert not any(math.isnan(x) for x in dv)
