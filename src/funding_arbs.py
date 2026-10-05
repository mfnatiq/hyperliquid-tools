"""cross-venue funding carry rows (entry and exit fees, slippage, break-even) from the trading-tools package.

this is the same scan as `python funding_scan.py`, split in two so the page does not hit the venue APIs on every
widget change:
  Snapshot(...)   fetches funding history and order books once. cache it for a few minutes.
  snapshot.rows() prices the pairs for a notional, hold and set of taker fees from that cached data. no network.
venue fees, rate limits and symbol aliases come from trading-tools' own bot/venues.toml.
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

# venue key -> funding_scan option holding its taker fee. PHX is absent: Phoenix reports a fee per market.
FEE_OPTIONS = {"HL": "hl_taker_bps", "EXT": "ext_taker_bps", "RISE": "rise_taker_bps", "VAR": "var_fee_bps"}


def default_taker_bps() -> dict[str, float]:
    """registry taker fees for the venues in FEE_OPTIONS."""
    a = fs.make_parser().parse_args([])
    return {k: getattr(a, opt) for k, opt in FEE_OPTIONS.items()}


class Snapshot:
    """funding history, candidates and order books fetched once. raises RuntimeError when fewer than two venues
    respond (an unreachable or rate-limited API), so a cache does not keep the failure."""

    def __init__(self, metric: str = "24h", min_carry_pct: float = 10.0, top: int = 25):
        # a higher min carry means fewer per-ticker history and book calls, which is what trips venue rate limits
        self.a = fs.make_parser().parse_args(["--venues", fs.FUNDING_VENUES, "--metric", metric,
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
            cands, _ = fs.build_candidates(a, vs, listed, overlap)
            self.cands = sorted(cands, key=lambda c: -c["carry"])[: a.top]
            need = {k: sorted({c["ticker"] for c in self.cands if k in (c["short"], c["long"])}) for k in vs}
            for v in vs.values():
                fs.safe(v.refresh, tag=f"{v.key} refresh")
            jobs = [(v, t) for k, v in vs.items() if v.has_books for t in need[k]]
            books = pool.map(lambda j: fs.safe(j[0].fetch_book, j[1], tag=f"{j[0].key} book {j[1]}"), jobs)
            for (v, t), b in zip(jobs, books):
                v.books[t] = b
        finally:
            pool.shutdown(wait=False)
        self.vs = vs
        self.missing = [k.upper() for k in a.venues.split(",") if k.upper() not in vs]  # venues that did not respond
        self.fetched_at = datetime.now(timezone.utc)
        self._lock = threading.Lock()  # rows() edits the shared args namespace the venues read their fees from

    def rows(self, notional: float, hold_days: float = 3.0, taker_bps: dict[str, float] | None = None) -> list[dict]:
        """graded pairs, best edge first. pairs that were skipped (no fill, break-even too long) are left out."""
        with self._lock:
            a = self.a
            a.notional, a.hold_days = notional, hold_days
            for k, bps in (taker_bps or {}).items():
                if k in FEE_OPTIONS:
                    setattr(a, FEE_OPTIONS[k], bps)
            rows = [fs.grade(fs.analyse(c, self.vs, a), c, a) for c in self.cands]
            for r in rows:  # each leg's own taker fee (analyse only keeps the sum)
                r["short_fee_bps"] = self.vs[r["short"]].fee_bps(r["ticker"])
                r["long_fee_bps"] = self.vs[r["long"]].fee_bps(r["ticker"])
        return [r for r in fs.sort_rows(rows, "verdict") if r["verdict"] != "SKIP"]  # GOOD, OK, MARGINAL, then break-even
