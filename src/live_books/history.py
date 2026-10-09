"""fixed-size ring buffers for price history. the oldest value is overwritten, so memory never grows and there is no clearing step.

Series keeps two resolutions of one value: a fine ring (default 1s for 1h) and a coarse ring (default 10s for 3d).
the coarse ring takes the last value seen in each coarse step. values are float32, which is 0.0002 bps off at a price of
100000, and NaN marks a step with no data."""
import numpy as np

NAN = float("nan")


class Ring:
    def __init__(self, size, step):
        self.step, self.size = step, size
        self.buf = np.full(size, np.nan, np.float32)
        self.last_slot = None                                        # absolute slot number of the newest write

    def put(self, ts, value):
        slot = int(ts // self.step)
        if self.last_slot is not None and slot > self.last_slot:
            gap = min(slot - self.last_slot - 1, self.size)           # steps with no data in between stay NaN
            for k in range(gap):
                self.buf[(self.last_slot + 1 + k) % self.size] = np.nan
        if self.last_slot is None or slot >= self.last_slot:
            self.buf[slot % self.size] = value
            self.last_slot = slot

    def read(self, ts_now=None):
        """(times, values) oldest first, only the slots that hold data."""
        if self.last_slot is None:
            return np.empty(0), np.empty(0, np.float32)
        n = min(self.last_slot + 1, self.size)
        slots = np.arange(self.last_slot - n + 1, self.last_slot + 1)
        vals = self.buf[slots % self.size]
        keep = ~np.isnan(vals)
        return slots[keep] * self.step, vals[keep]


class Series:
    def __init__(self, fine_step=1, fine_span=3600, coarse_step=10, coarse_span=3 * 86400):
        self.fine = Ring(fine_span // fine_step, fine_step)
        self.coarse = Ring(coarse_span // coarse_step, coarse_step)

    def put(self, ts, value):
        self.fine.put(ts, value)
        self.coarse.put(ts, value)

    def read(self, window_s):
        """history over the last window_s seconds, from the fine ring when it covers the window and the coarse ring otherwise."""
        ring = self.fine if window_s <= self.fine.size * self.fine.step else self.coarse
        t, v = ring.read()
        if len(t) == 0:
            return t, v
        keep = t >= t[-1] - window_s
        return t[keep], v[keep]


def decimate(t, v, max_points=1000):
    """at most max_points by taking the mean of equal chunks, so a long window plots fast."""
    n = len(t)
    if n <= max_points:
        return t, v
    k = -(-n // max_points)
    m = n - n % k
    return t[:m].reshape(-1, k).mean(axis=1), v[:m].reshape(-1, k).mean(axis=1)
