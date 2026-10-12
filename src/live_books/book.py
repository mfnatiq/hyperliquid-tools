"""order book maths. a book is dict(bids=[(px, qty)...] best first, asks=[...] best first, mid=float).

PriceLadder holds the cumulative quantity and notional of one side, built once per book update. every clip size is then
one bisect instead of a walk through the levels, so asking for more clip sizes costs almost nothing."""
from bisect import bisect_left
from itertools import accumulate


def norm_book(bids, asks):
    """sort best first, drop empty levels. None when a side is empty or the book is crossed."""
    bids = sorted(((float(p), float(q)) for p, q in bids if float(p) > 0 and float(q) > 0), key=lambda x: -x[0])
    asks = sorted(((float(p), float(q)) for p, q in asks if float(p) > 0 and float(q) > 0), key=lambda x: x[0])
    if not bids or not asks or bids[0][0] >= asks[0][0]:
        return None
    return dict(bids=bids, asks=asks, mid=(bids[0][0] + asks[0][0]) / 2)


class PriceLadder:
    def __init__(self, levels):
        self.px = [p for p, _ in levels]
        self.cum_qty = list(accumulate(q for _, q in levels))
        self.cum_usd = list(accumulate(p * q for p, q in levels))

    @property
    def depth_usd(self):
        return self.cum_usd[-1] if self.cum_usd else 0.0

    def vwap(self, usd):
        """(average price paid for usd of notional, share of usd the book can fill from 0 to 1). (None, 0) for an empty side."""
        if not self.px:
            return None, 0.0
        i = bisect_left(self.cum_usd, usd)
        if i == len(self.cum_usd):                                   # too thin: the average of everything that is there
            return self.cum_usd[-1] / self.cum_qty[-1], self.cum_usd[-1] / usd
        before_usd, before_qty = (self.cum_usd[i - 1], self.cum_qty[i - 1]) if i else (0.0, 0.0)
        return usd / (before_qty + (usd - before_usd) / self.px[i]), 1.0


def slippage_bps(avg, mid, buy):
    """cost against mid in bps, positive means worse than mid. a buy pays above mid, a sell receives below it."""
    return ((avg - mid) / mid if buy else (mid - avg) / mid) * 1e4


class Ladders:
    """both sides of one book, prepared once."""

    def __init__(self, book):
        self.mid = book["mid"]
        self.ask, self.bid = PriceLadder(book["asks"]), PriceLadder(book["bids"])

    def clip(self, usd, buy):
        """(slippage_bps vs mid, fill share). slippage is None when the side is empty."""
        avg, filled = (self.ask if buy else self.bid).vwap(usd)
        return (None if avg is None else slippage_bps(avg, self.mid, buy)), filled
