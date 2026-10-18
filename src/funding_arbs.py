"""cross-venue funding carry rows (entry and exit fees, slippage, break-even) from the trading-tools package.

this is the same scan as `python funding_scan.py`, split in two so the page does not hit the venue APIs on every
widget change:
  Snapshot(...)   fetches funding history and order books once. cache it for a few minutes.
  snapshot.rows() prices the pairs for a notional and hold from that cached data. no network.
venue fees, rate limits and symbol aliases come only from trading-tools' own bot/venues.toml.
"""
import threading
from collections import Counter
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timezone

try:
    import funding_scan as fs
except ImportError:  # trading-tools is a private repo and an optional install
    fs = None

AVAILABLE = fs is not None


def net_apr(carry: float, rt_cost_bps: float, mid_basis_bps: float, hold_days: float) -> float:
    """annual return as a fraction after the round trip cost and adverse basis, if the pair is held `hold_days` and the
    entry and exit cost is paid once. a longer hold spreads the same cost over more days, so net APR rises with the hold.
    carry is the annualised funding spread as a fraction (0.5 is 50%). a positive mid basis is in our favour."""
    gain_bps = carry * 1e4 / 365 * hold_days
    basis_cost_bps = max(0.0, -mid_basis_bps)
    return (gain_bps - rt_cost_bps - basis_cost_bps) / 1e4 * 365 / hold_days


def usd_short(x) -> str:
    """$139k, $1.6M, $2.0B, or a dash when the venue does not report it"""
    if x is None:
        return "-"
    for unit, size in (("B", 1e9), ("M", 1e6), ("k", 1e3)):
        if x >= size:
            v = x / size
            return f"${v:.1f}{unit}" if v < 10 else f"${v:.0f}{unit}"
    return f"${x:.0f}"


def days_short(d) -> str:
    """break-even in days as '1d 8h', '5h' or a dash"""
    if d is None:
        return "-"
    hours = round(d * 24)
    if hours < 24:
        return f"{hours}h"
    return f"{hours // 24}d {hours % 24}h" if hours % 24 else f"{hours // 24}d"


def usd_or_none(x):
    """first number of a venue's volume_24h() result, which is (usd volume, trade count) or None"""
    if isinstance(x, (tuple, list)):
        x = x[0] if x else None
    return None if x is None else float(x)


def taker_bps() -> dict[str, float]:
    """taker fee per venue key from venues.toml with the active tier applied (Phoenix is per market, so it is left out).
    QFEX shows its single-stock rate, indices and commodities pay less."""
    a = fs.make_parser().parse_args([])
    opts = {"RISE": "rise_taker_bps", "VAR": "var_fee_bps", "HL": "hl_taker_bps", "EXT": "ext_taker_bps",
            "PAC": "pac_taker_bps", "NADO": "nado_taker_bps", "QFEX": "qfex_taker_bps", "ONDO": "ondo_taker_bps",
            "ARCUS": "arcus_taker_bps"}
    return {k: getattr(a, opt) for k, opt in opts.items()}


class Snapshot:
    """funding history, candidates and order books fetched once. raises RuntimeError when fewer than two venues
    respond (an unreachable or rate-limited API), so a cache does not keep the failure."""

    def __init__(self, metric: str = "24h", min_carry_pct: float = 10.0, top: int = 25):
        # a higher min carry means fewer per-ticker history and book calls, which is what trips venue rate limits
        self.a = fs.make_parser().parse_args(["--venues", getattr(fs, "ALL_FUNDING_VENUES", fs.FUNDING_VENUES), "--metric", metric,
                                              "--min-carry", str(min_carry_pct), "--top", str(top), "--sort", "verdict"])
        a = self.a
        vs = fs.build_venues(a.venues, a)
        pool = ThreadPoolExecutor(a.workers)
        try:
            listed = dict(zip(vs, pool.map(lambda k: fs.safe(vs[k].load, tag=f"{k} load") or set(), vs)))
            vs = {k: v for k, v in vs.items() if listed[k]}
            if len(vs) < 2:
                raise RuntimeError(f"only {len(vs)} venue(s) responded; the venue APIs may be unreachable or rate limited")
            cnt = Counter(t for k in vs for t in listed[k])
            overlap = sorted(t for t, c in cnt.items() if c >= 2)
            for k, v in vs.items():
                v.enrich([t for t in overlap if t in listed[k]], pool)
            self.listed, self.overlap = listed, overlap
            cands, _ = fs.build_candidates(a, vs, listed, overlap)
            self.cands = sorted(cands, key=lambda c: -c["carry"])[: a.top]
            need = {k: sorted({c["ticker"] for c in self.cands if k in (c["short"], c["long"])}) for k in vs}
            for v in vs.values():
                fs.safe(v.refresh, tag=f"{v.key} refresh")
            jobs = [(v, t) for k, v in vs.items() if v.has_books for t in need[k]]
            books = pool.map(lambda j: fs.safe(j[0].fetch_book, j[1], tag=f"{j[0].key} book {j[1]}"), jobs)
            for (v, t), b in zip(jobs, books):
                v.books[t] = b
            self.meta = {(k, t): self._leg_meta(v, t) for k, v in vs.items() for t in need[k]}
        finally:
            pool.shutdown(wait=False)
        self.vs = vs
        self.missing = [k.upper() for k in a.venues.split(",") if k.upper() not in vs]  # venues that did not respond
        self.fetched_at = datetime.now(timezone.utc)
        self.books_at = self.fetched_at
        self._need = need
        self._pool_size = a.workers
        self._lock = threading.Lock()  # rows() edits the shared args namespace (notional, hold)

    @staticmethod
    def _leg_meta(v, t) -> dict:
        """24h volume and open interest in USD for one market, None where the venue does not report it"""
        vol = fs.safe(v.volume_24h, t, tag=f"{v.key} volume {t}") if hasattr(v, "volume_24h") else None
        oi = fs.safe(v.open_interest, t, tag=f"{v.key} open interest {t}") if hasattr(v, "open_interest") else None
        return {"vol": usd_or_none(vol), "oi": None if oi is None else float(oi)}

    def samples(self) -> list[tuple[str, str, float]]:
        """latest annualised funding rate (a fraction) per venue and ticker for every ticker listed on two or more
        venues, in the form `funding_history.record` takes"""
        out = []
        for t in self.overlap:
            for k, v in self.vs.items():
                if t in self.listed[k] and k != "TXF":
                    x = fs.safe(v.apr, t, "last", tag=f"{k} last {t}")
                    if x is not None:
                        out.append((k, t, x))
        return out

    def refresh_books(self, min_age_s: float = 45.0) -> bool:
        """refetch the order books of the candidate pairs and nothing else (funding history stays as fetched).
        skipped when the books are younger than min_age_s, so many open pages share one refresh. a failed
        fetch keeps the previous book. returns True when it refetched."""
        with self._lock:
            if (datetime.now(timezone.utc) - self.books_at).total_seconds() < min_age_s:
                return False
            jobs = [(v, t) for k, v in self.vs.items() if v.has_books for t in self._need[k]]
            with ThreadPoolExecutor(self._pool_size) as pool:
                for v in self.vs.values():
                    fs.safe(v.refresh, tag=f"{v.key} refresh")
                books = pool.map(lambda j: fs.safe(j[0].fetch_book, j[1], tag=f"{j[0].key} book {j[1]}"), jobs)
                for (v, t), b in zip(jobs, books):
                    if b is not None:
                        v.books[t] = b
            self.books_at = datetime.now(timezone.utc)
        return True

    def rows(self, notional: float, hold_days: float = 3.0) -> list[dict]:
        """graded pairs, best edge first. pairs that were skipped (no fill, break-even too long) are left out."""
        with self._lock:
            a = self.a
            a.notional, a.hold_days = notional, hold_days
            rows = [fs.grade(fs.analyse(c, self.vs, a), c, a) for c in self.cands]
            for r in rows:  # each leg's own taker fee (analyse only keeps the sum)
                r["short_fee_bps"] = self.vs[r["short"]].fee_bps(r["ticker"])
                r["long_fee_bps"] = self.vs[r["long"]].fee_bps(r["ticker"])
                sm, lm = self.meta.get((r["short"], r["ticker"]), {}), self.meta.get((r["long"], r["ticker"]), {})
                r.update(vol_short=sm.get("vol"), oi_short=sm.get("oi"), vol_long=lm.get("vol"), oi_long=lm.get("oi"))
                r["net_apr"] = None if r["rt_cost_bps"] is None else net_apr(
                    r["carry_apr"], r["rt_cost_bps"], r["mid_basis_bps"] or 0.0, hold_days)
        return [r for r in fs.sort_rows(rows, "verdict") if r["verdict"] != "SKIP"]  # GOOD, OK, MARGINAL, then break-even
