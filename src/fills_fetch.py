"""network side of the fills and subaccount lookups, split from the pages so it can run on worker threads and be tested.

the pages used to fetch one account after another. accounts are independent, so they are fetched together here, and
the pages still process the results in account order on their own thread. one account's windows stay in order, because
a window that returns the 2000 fill cap decides where the next window starts."""
import threading
from concurrent.futures import ThreadPoolExecutor
from typing import Callable

FILLS_PAGE_CAP = 2000       # the fills endpoint returns at most this many per call
MAX_WORKERS = 4             # a few calls at once, to stay friendly to the data provider's rate limit


def bind_script_ctx(fn: Callable) -> Callable:
    """make fn safe to run on a worker thread when it calls st.cache_data functions: attach the caller's Streamlit script
    context to the worker. outside Streamlit (tests), fn is returned unchanged."""
    try:
        from streamlit.runtime.scriptrunner import add_script_run_ctx, get_script_run_ctx
        ctx = get_script_run_ctx()
    except Exception:
        return fn
    if ctx is None:
        return fn

    def run(*a, **kw):
        add_script_run_ctx(threading.current_thread(), ctx)
        return fn(*a, **kw)
    return run


def expand_subaccounts(accounts: list[str], get_subs: Callable[[str], list], workers: int = MAX_WORKERS) -> list[str]:
    """accounts plus their subaccounts, in the order a one-at-a-time loop appends them (each account's subaccounts after
    everything already queued). one level is looked up together, the next level after it."""
    out, level = list(accounts), list(accounts)
    while level:
        with ThreadPoolExecutor(min(workers, len(level))) as pool:
            subs = list(pool.map(get_subs, level))
        level = [s["subAccountUser"] for ss in subs for s in ss]
        out += level
    return out


def fetch_account_pages(account: str, fetch: Callable[[str, int, int], list], start: int, now: int, span_ms: int,
                        logged_in: bool, limit_total: int) -> tuple[list, int | None]:
    """walk one account's fills from `start` to `now` in windows of span_ms. returns ([(start, end, fills)], hit) where hit is
    the running fill count at the moment a visitor who is not logged in passed limit_total (that window is left out), else None."""
    pages, total, hit = [], 0, None
    s = start
    while s < now:
        e = min(now, s + span_ms)
        fills = fetch(account, s, e)
        total += len(fills)
        if not logged_in and total > limit_total:
            hit = total
            break
        pages.append((s, e, fills))
        # at the cap the next window starts after the newest fill, otherwise it slides to the end of this one
        s = max(f["time"] for f in fills) + 1 if len(fills) == FILLS_PAGE_CAP else e + 1
    return pages, hit


def fetch_all_accounts(accounts: list[str], fetch: Callable[[str, int, int], list], start: int, now: int, span_ms: int,
                       logged_in: bool, limit_total: int, workers: int = MAX_WORKERS) -> list[tuple[list, int | None]]:
    """fetch_account_pages for every account at once, results in account order"""
    if not accounts:
        return []
    with ThreadPoolExecutor(min(workers, len(accounts))) as pool:
        return list(pool.map(lambda a: fetch_account_pages(a, fetch, start, now, span_ms, logged_in, limit_total), accounts))
