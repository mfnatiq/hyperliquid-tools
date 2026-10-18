import threading
import time

from src import snapshot_cache as sc


def wait_for(cond, timeout=2.0):
    end = time.time() + timeout
    while time.time() < end and not cond():
        time.sleep(0.01)


def test_first_get_returns_none_and_the_value_arrives_after_the_build():
    c = sc.BackgroundCache(ttl_s=60)
    gate = threading.Event()
    assert c.get("k", lambda: (gate.wait(), 42)[1]) == (None, None)
    gate.set()
    wait_for(lambda: c.get("k", lambda: 0)[0] is not None)
    value, built_at = c.get("k", lambda: 0)
    assert value == 42 and built_at is not None


def test_an_old_value_is_served_while_the_rebuild_runs():
    c = sc.BackgroundCache(ttl_s=0)
    c.get("k", lambda: 1)
    wait_for(lambda: "k" in c._value)
    time.sleep(0.01)
    gate = threading.Event()
    assert c.get("k", lambda: (gate.wait(), 2)[1])[0] == 1
    gate.set()
    wait_for(lambda: c._value["k"][0] == 2)
    assert c._value["k"][0] == 2


def test_a_failed_build_stores_the_error_and_waits_before_retrying(monkeypatch):
    c = sc.BackgroundCache(ttl_s=60)
    calls = []

    def boom():
        calls.append(1)
        raise RuntimeError("only 1 venue(s) responded")

    c.get("k", boom)
    wait_for(lambda: "k" in c.error)
    assert c.error["k"] == "only 1 venue(s) responded"
    c.get("k", boom)
    time.sleep(0.05)
    assert len(calls) == 1  # inside the retry wait
    monkeypatch.setattr(sc, "RETRY_AFTER_S", 0.0)
    c.get("k", boom)
    wait_for(lambda: len(calls) == 2)
    assert len(calls) == 2
