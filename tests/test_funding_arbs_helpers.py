import pytest

from src.funding_arbs import days_short, net_apr, usd_or_none, usd_short


def test_net_apr_takes_the_round_trip_cost_off_a_three_day_hold():
    # 60% carry earns 49.315 bps in 3 days, less 8 bps cost = 41.315 bps, annualised over 3 days = 50.27%
    assert net_apr(0.6, 8.0, 0.0, 3.0) == pytest.approx(0.5026667, abs=1e-6)


def test_net_apr_charges_an_adverse_basis_and_ignores_a_favourable_one():
    assert net_apr(0.6, 8.0, -2.0, 3.0) == pytest.approx(0.4783333, abs=1e-6)
    assert net_apr(0.6, 8.0, 5.0, 3.0) == pytest.approx(0.5026667, abs=1e-6)


def test_net_apr_is_negative_when_the_cost_is_more_than_the_carry_earned():
    assert net_apr(0.1, 20.0, 0.0, 1.0) == pytest.approx(-0.63, abs=1e-6)  # (2.74 bps earned - 20 bps cost) x 365 / 1 day


def test_usd_short():
    assert [usd_short(x) for x in (None, 56, 139_000, 1_600_000, 308_400, 2_000_000_000, 999)] == \
        ["-", "$56", "$139k", "$1.6M", "$308k", "$2.0B", "$999"]


def test_days_short():
    assert [days_short(x) for x in (None, 0.2, 1.3333, 2.0, 2.5)] == ["-", "5h", "1d 8h", "2d", "2d 12h"]


def test_usd_or_none_takes_the_volume_from_a_volume_and_trade_count_pair():
    assert usd_or_none((56000.0, None)) == 56000.0
    assert usd_or_none((None, 12)) is None
    assert usd_or_none(None) is None
    assert usd_or_none(1.5) == 1.5


class FakeVenue:
    def __init__(self, key, rates, vol=None, oi=None):
        self.key, self.rates, self._vol, self._oi = key, rates, vol, oi

    def apr(self, t, metric):
        assert metric == "last"
        return self.rates[t]

    def volume_24h(self, t):
        return self._vol


def test_samples_lists_the_latest_rate_of_every_shared_ticker_on_every_venue_that_lists_it():
    pytest.importorskip("funding_scan")
    from src.funding_arbs import Snapshot
    s = Snapshot.__new__(Snapshot)
    s.vs = {"HL": FakeVenue("HL", {"ZRO": 0.5, "BTC": 0.1}), "PHX": FakeVenue("PHX", {"ZRO": 0.1, "BTC": None})}
    s.listed = {"HL": {"ZRO", "BTC"}, "PHX": {"ZRO", "BTC"}}
    s.overlap = ["BTC", "ZRO"]
    assert s.samples() == [("HL", "BTC", 0.1), ("HL", "ZRO", 0.5), ("PHX", "ZRO", 0.1)]


def test_leg_meta_reads_volume_and_open_interest_where_a_venue_has_them():
    pytest.importorskip("funding_scan")
    from src.funding_arbs import Snapshot
    with_both = FakeVenue("HL", {}, vol=(56000.0, None))
    with_both.open_interest = lambda t: 139000
    assert Snapshot._leg_meta(with_both, "ZRO") == {"vol": 56000.0, "oi": 139000.0}
    assert Snapshot._leg_meta(FakeVenue("PHX", {}, vol=None), "ZRO") == {"vol": None, "oi": None}
