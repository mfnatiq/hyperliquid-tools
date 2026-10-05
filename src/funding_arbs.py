"""cross-venue funding carry rows (entry and exit fees, slippage, break-even) from the trading-tools package.

this is the same scan as `python funding_scan.py`, returned as rows instead of printed. venue fees, rate limits and
symbol aliases come from trading-tools' own bot/venues.toml.
"""
from collections import Counter
from concurrent.futures import ThreadPoolExecutor

try:
    import funding_scan as fs
except ImportError:  # trading-tools is a private repo and an optional install
    fs = None

AVAILABLE = fs is not None

# page exchange name -> funding_scan option that sets its taker fee
FEE_OPTIONS = {"Hyperliquid": "--hl-taker-bps", "Extended": "--ext-taker-bps"}


def scan(notional: float, *, metric: str = "24h", top: int = 25, hold_days: float = 3.0,
         taker_bps: dict[str, float] | None = None) -> list[dict]:
    """graded funding pairs, best first. skipped pairs (no fill, break-even too long) are left out.
    taker_bps overrides the registry taker fee for the exchanges in FEE_OPTIONS."""
    argv = ["--venues", fs.FUNDING_VENUES, "-n", str(notional), "--metric", metric, "--top", str(top),
            "--hold-days", str(hold_days), "--sort", "smart"]
    for exch, bps in (taker_bps or {}).items():
        if exch in FEE_OPTIONS:
            argv += [FEE_OPTIONS[exch], str(bps)]
    a = fs.make_parser().parse_args(argv)
    vs = fs.build_venues(a.venues, a)
    pool = ThreadPoolExecutor(a.workers)
    try:
        listed = dict(zip(vs, pool.map(lambda k: fs.safe(vs[k].load, tag=f"{k} load") or set(), vs)))
        vs = {k: v for k, v in vs.items() if listed[k]}
        if len(vs) < 2:
            return []
        cnt = Counter(t for k in vs for t in listed[k])
        overlap = sorted(t for t, c in cnt.items() if c >= 2)
        for k, v in vs.items():
            v.enrich([t for t in overlap if t in listed[k]], pool)
        cands, _ = fs.build_candidates(a, vs, listed, overlap)
        cands = sorted(cands, key=lambda c: -c["carry"])[: a.top]
        if not cands:
            return []
        need = {k: sorted({c["ticker"] for c in cands if k in (c["short"], c["long"])}) for k in vs}
        for v in vs.values():
            fs.safe(v.refresh, tag=f"{v.key} refresh")
        jobs = [(v, t) for k, v in vs.items() if v.has_books for t in need[k]]
        books = pool.map(lambda j: fs.safe(j[0].fetch_book, j[1], tag=f"{j[0].key} book {j[1]}"), jobs)
        for (v, t), b in zip(jobs, books):
            v.books[t] = b
        rows = [fs.grade(fs.analyse(c, vs, a), c, a) for c in cands]
        return [r for r in fs.sort_rows(rows, a.sort) if r["verdict"] != "SKIP"]
    finally:
        pool.shutdown(wait=False)
