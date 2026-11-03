"""fills and subaccount lookups run for several accounts at once and give the same answer as the one-at-a-time loop they
replaced. a threading.Barrier only opens when every call is in flight together. no network."""
import threading

from src.fills_fetch import expand_subaccounts, fetch_account_pages, fetch_all_accounts

DAY = 86_400_000
SPAN = 30 * DAY


def reference(account, fetch, start, now, logged_in, limit_total):
    """the loop as it was in pages/dashboard.py before the change, minus processing"""
    pages, total, hit, s = [], 0, None, start
    while s < now:
        e = min(now, s + SPAN)
        fills = fetch(account, s, e)
        total += len(fills)
        if not logged_in and total > limit_total:
            hit = total
            break
        pages.append((s, e, fills))
        s = max(f["time"] for f in fills) + 1 if len(fills) == 2000 else e + 1
    return pages, hit


def fake_fetch(account, s, e):
    if account == "busy" and s == 0:                      # the first window is full: the next one starts after the newest fill
        return [{"time": 10 * DAY + i} for i in range(2000)]
    return [{"time": s + 1}] if account != "empty" else []


def test_pages_match_the_old_loop_including_the_full_window_restart():
    now = 100 * DAY
    for acct in ("busy", "quiet", "empty"):
        got = fetch_account_pages(acct, fake_fetch, 0, now, SPAN, True, 10)
        assert got == reference(acct, fake_fetch, 0, now, True, 10)
    pages, hit = fetch_account_pages("busy", fake_fetch, 0, now, SPAN, True, 10)
    assert hit is None and pages[1][0] == 10 * DAY + 2000      # 10d + newest offset 1999 + 1


def test_a_visitor_who_is_not_logged_in_stops_at_the_limit_and_keeps_the_earlier_windows():
    now = 100 * DAY
    pages, hit = fetch_account_pages("quiet", fake_fetch, 0, now, SPAN, False, 2)      # one fill per window: third window passes 2
    assert hit == 3 and len(pages) == 2
    assert (pages, hit) == reference("quiet", fake_fetch, 0, now, False, 2)


def test_every_account_is_fetched_at_once_and_results_stay_in_account_order():
    bar = threading.Barrier(3, timeout=3)

    def fetch(account, s, e):
        bar.wait()
        return [{"time": s + 1}]
    got = fetch_all_accounts(["a", "b", "c"], fetch, 0, 10, SPAN, True, 100)
    assert [p[0][2] for p, _ in got] == [[{"time": 1}]] * 3
    assert fetch_all_accounts([], fetch, 0, 10, SPAN, True, 100) == []


def test_subaccounts_are_listed_in_the_order_a_growing_loop_gives_and_looked_up_together():
    tree = {"a": ["a1", "a2"], "b": ["b1"], "a1": ["a1x"], "a2": [], "b1": [], "a1x": []}
    bar = threading.Barrier(2, timeout=3)
    seen = []

    def subs(acct):
        if acct in ("a", "b"):
            bar.wait()                                      # the first level is looked up together
        seen.append(acct)
        return [{"subAccountUser": s} for s in tree[acct]]
    out = expand_subaccounts(["a", "b"], subs)
    legacy, queue = ["a", "b"], ["a", "b"]                   # the old loop: append while iterating
    for acct in legacy:
        legacy += tree[acct]
    assert out == legacy == ["a", "b", "a1", "a2", "b1", "a1x"]
    assert sorted(seen) == sorted(legacy)
