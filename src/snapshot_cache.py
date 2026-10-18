"""build a slow value in a background thread and hand out the last good one, so a page never waits on the build.

get() returns right away. the first call for a key starts the build and returns (None, None). once the value is older
than ttl_s the next call starts a rebuild and keeps returning the old value until the new one is ready. a failed build
keeps the old value, stores the error text and waits RETRY_AFTER_S before the next try.
"""
import threading
import time
from typing import Any, Callable, Optional


RETRY_AFTER_S = 60.0  # after a failed build, wait this long before trying again, so a rate limited API is not hit every rerun


class BackgroundCache:
    def __init__(self, ttl_s: float):
        self.ttl_s = ttl_s
        self._lock = threading.Lock()
        self._value: dict[Any, tuple[Any, float]] = {}
        self._building: set[Any] = set()
        self.error: dict[Any, str] = {}
        self._failed_at: dict[Any, float] = {}

    def get(self, key: Any, build: Callable[[], Any]) -> tuple[Optional[Any], Optional[float]]:
        """(value, built_at unix seconds), or (None, None) while the first build runs"""
        with self._lock:
            have = self._value.get(key)
            due = have is None or time.time() - have[1] > self.ttl_s
            cooling = time.time() - self._failed_at.get(key, 0.0) < RETRY_AFTER_S
            if due and not cooling and key not in self._building:
                self._building.add(key)
                threading.Thread(target=self._run, args=(key, build), name="snapshot-build", daemon=True).start()
        return have if have else (None, None)

    def _run(self, key: Any, build: Callable[[], Any]) -> None:
        try:
            value = build()
            with self._lock:
                self._value[key] = (value, time.time())
                self.error.pop(key, None)
        except Exception as e:
            with self._lock:
                self.error[key] = str(e)
                self._failed_at[key] = time.time()
        finally:
            with self._lock:
                self._building.discard(key)
