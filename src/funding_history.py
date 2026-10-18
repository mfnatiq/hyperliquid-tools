"""hourly funding rate samples kept in the app database, and the averages the Arb Opportunities screener shows.

one row per (hour, venue, ticker) with the annualised funding rate as a fraction (0.5 is 50% a year, as funding_scan gives it). a pair's carry for an hour is the short
leg's rate minus the long leg's rate from the same hour. venue APIs cap how much history one call returns (and
Variational has none), so the 30 day averages come from these samples. they fill up from the day the recorder starts.
the table lives in `DATABASE_URL` (Postgres on Railway, a local sqlite file otherwise).
"""
import os
import threading
import time
from datetime import datetime, timezone
from typing import Callable, Optional

from sqlalchemy import create_engine, text
from sqlalchemy.engine import Engine

TABLE = "funding_samples"
KEEP_DAYS = 35
HOUR_S = 3600

_engine: Optional[Engine] = None
_lock = threading.Lock()


def get_engine() -> Engine:
    """one shared engine, created on first use so importing this module never opens a connection"""
    global _engine
    with _lock:
        if _engine is None:
            _engine = create_engine(os.getenv("DATABASE_URL", "sqlite:///users.db"), echo=False, future=True)
        return _engine


def init(engine: Optional[Engine] = None) -> None:
    engine = engine or get_engine()
    with engine.begin() as c:
        c.execute(text(
            f"CREATE TABLE IF NOT EXISTS {TABLE} ("
            "ts_hour BIGINT NOT NULL, venue VARCHAR(16) NOT NULL, ticker VARCHAR(32) NOT NULL, apr DOUBLE PRECISION NOT NULL, "
            "PRIMARY KEY (ts_hour, venue, ticker))"
        ))


def hour_of(ts: float) -> int:
    """unix hour number that `ts` (seconds) falls in"""
    return int(ts // HOUR_S)


def record(samples: list[tuple[str, str, float]], now: Optional[float] = None, engine: Optional[Engine] = None) -> int:
    """store (venue, ticker, annualised fraction) samples under the current hour. a second run in the same hour
    overwrites the first, so the number of rows grows by hours, not by runs. old rows are dropped. returns rows written."""
    engine = engine or get_engine()
    now = time.time() if now is None else now
    h = hour_of(now)
    rows = [{"h": h, "v": v, "t": t, "a": float(a)} for v, t, a in samples if a is not None]
    with engine.begin() as c:
        if rows:
            c.execute(text(
                f"INSERT INTO {TABLE} (ts_hour, venue, ticker, apr) VALUES (:h, :v, :t, :a) "
                "ON CONFLICT (ts_hour, venue, ticker) DO UPDATE SET apr = excluded.apr"
            ), rows)
        c.execute(text(f"DELETE FROM {TABLE} WHERE ts_hour < :cut"), {"cut": h - KEEP_DAYS * 24})
    return len(rows)


def pair_history(short: str, long: str, ticker: str, now: Optional[float] = None, engine: Optional[Engine] = None) -> dict:
    """carry history of one pair, as annualised fractions: mean over the last 24 hours, 7 days and 30 days, the share of
    the 30 day hours with positive carry, and how many hours of data exist. a window with no samples gives None."""
    engine = engine or get_engine()
    h = hour_of(time.time() if now is None else now)
    with engine.connect() as c:
        got = c.execute(text(
            f"SELECT s.ts_hour, s.apr - l.apr FROM {TABLE} s JOIN {TABLE} l "
            "ON l.ts_hour = s.ts_hour AND l.ticker = s.ticker "
            "WHERE s.venue = :s AND l.venue = :l AND s.ticker = :t AND s.ts_hour > :cut ORDER BY s.ts_hour"
        ), {"s": short, "l": long, "t": ticker, "cut": h - 30 * 24}).all()
    carry = [(int(r[0]), float(r[1])) for r in got]

    def avg(hours: int) -> Optional[float]:
        xs = [v for t, v in carry if t > h - hours]
        return sum(xs) / len(xs) if xs else None

    return {
        "avg_24h": avg(24), "avg_7d": avg(7 * 24), "avg_30d": avg(30 * 24),
        "sustain_30d": sum(1 for _, v in carry if v > 0) / len(carry) if carry else None,
        "hours": len(carry),
    }


def start_recorder(sample: Callable[[], list[tuple[str, str, float]]], every_s: float = HOUR_S, first_delay_s: float = 60.0,
                   engine: Optional[Engine] = None) -> threading.Thread:
    """run `sample` now and then every `every_s` seconds on a daemon thread, storing what it returns. a failed run is
    logged and tried again next time. the page never waits on this thread."""
    def loop():
        time.sleep(first_delay_s)
        while True:
            try:
                n = record(sample(), engine=engine)
                print(f"funding recorder: stored {n} samples", flush=True)
            except Exception as e:
                print(f"funding recorder: {e}", flush=True)
            time.sleep(every_s)

    t = threading.Thread(target=loop, name="funding-recorder", daemon=True)
    t.start()
    return t


def utc(ts_hour: int) -> datetime:
    return datetime.fromtimestamp(ts_hour * HOUR_S, tz=timezone.utc)
