import pytest
from sqlalchemy import create_engine, text

from src import funding_history as fh

H = 1000  # an arbitrary unix hour


@pytest.fixture
def engine(tmp_path):
    e = create_engine(f"sqlite:///{tmp_path / 'h.db'}", future=True)
    fh.init(e)
    return e


def at(hour):
    return hour * 3600 + 10


def test_pair_history_averages_the_short_minus_long_carry_per_window(engine):
    fh.record([("HL", "ZRO", 0.5), ("PHX", "ZRO", 0.1)], now=at(H), engine=engine)
    fh.record([("HL", "ZRO", 0.3), ("PHX", "ZRO", 0.4)], now=at(H - 1), engine=engine)
    fh.record([("HL", "ZRO", 0.2), ("PHX", "ZRO", 0.1)], now=at(H - 30), engine=engine)
    h = fh.pair_history("HL", "PHX", "ZRO", now=at(H), engine=engine)
    assert h["hours"] == 3
    assert h["avg_24h"] == pytest.approx(0.15)  # (0.4 + -0.1) / 2
    assert h["avg_7d"] == pytest.approx(0.4 / 3)  # (0.4 - 0.1 + 0.1) / 3
    assert h["avg_30d"] == pytest.approx(0.4 / 3)
    assert h["sustain_30d"] == pytest.approx(2 / 3)


def test_pair_history_with_no_samples_gives_none_and_zero_hours(engine):
    h = fh.pair_history("HL", "PHX", "ZRO", now=at(H), engine=engine)
    assert h == {"avg_24h": None, "avg_7d": None, "avg_30d": None, "sustain_30d": None, "hours": 0}


def test_hours_without_both_legs_are_left_out(engine):
    fh.record([("HL", "ZRO", 0.5)], now=at(H), engine=engine)
    fh.record([("HL", "ZRO", 0.5), ("PHX", "ZRO", 0.1)], now=at(H - 1), engine=engine)
    assert fh.pair_history("HL", "PHX", "ZRO", now=at(H), engine=engine)["hours"] == 1


def test_a_second_run_in_the_same_hour_overwrites_the_first(engine):
    fh.record([("HL", "ZRO", 0.5)], now=at(H), engine=engine)
    assert fh.record([("HL", "ZRO", 0.7)], now=at(H) + 600, engine=engine) == 1
    with engine.connect() as c:
        assert c.execute(text("SELECT COUNT(*), MAX(apr) FROM funding_samples")).one() == (1, 0.7)


def test_rows_older_than_35_days_are_dropped_and_missing_rates_are_skipped(engine):
    fh.record([("HL", "ZRO", 0.5)], now=at(H - 36 * 24), engine=engine)
    assert fh.record([("HL", "ZRO", None), ("PHX", "ZRO", 0.1)], now=at(H), engine=engine) == 1
    with engine.connect() as c:
        assert c.execute(text("SELECT COUNT(*) FROM funding_samples")).scalar() == 1


def test_samples_older_than_30_days_are_not_in_the_window(engine):
    fh.record([("HL", "ZRO", 0.5), ("PHX", "ZRO", 0.1)], now=at(H - 31 * 24), engine=engine)
    assert fh.pair_history("HL", "PHX", "ZRO", now=at(H), engine=engine)["hours"] == 0
