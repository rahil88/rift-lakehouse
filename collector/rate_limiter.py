"""Header-driven token-bucket rate limiter for the Riot API.

Riot enforces limits per routing host (na1, americas, ...) at two levels:

* application limits, e.g. ``X-App-Rate-Limit: 100:120,20:1`` (max calls : window seconds)
* method limits, per endpoint, e.g. ``X-Method-Rate-Limit: 2000:10``

and reports Riot's own usage count in ``X-App-Rate-Limit-Count`` / ``X-Method-Rate-Limit-Count``.

Every (limit, window) pair becomes one token bucket. A naive bucket with capacity L and refill
L/W can let ~2L calls through one window (spend the full burst, then a full window of refill).
Here capacity B and refill (L - B) / W are chosen so that B + (L - B) = L: no interval of W
seconds can ever see more than L calls from this process, however Riot aligns its windows.

Headers keep the buckets honest:

* limits are read from the headers, never hard-coded; if Riot changes them, buckets rebuild;
* the first response for a bucket "charges" every call Riot already counted in the current
  window, so a restarted crawler (or one sharing the key) slows down instead of hitting 429;
* 429s block the affected key for Retry-After seconds (see ``block``).
"""

from __future__ import annotations

import math
import time
from collections.abc import Callable, Mapping
from dataclasses import dataclass

Clock = Callable[[], float]
Sleep = Callable[[float], None]
BucketKey = tuple[str, ...]  # ("app", host) or ("method", host, method_name)

DEFAULT_BURST_FRACTION = 0.1
# Float tolerance. Without it, 0.9999999999999998 tokens asks for a ~1e-16 s wait, which can be
# smaller than the clock's resolution at large timestamps, so time never advances and the
# acquire loop spins forever.
EPSILON = 1e-9


@dataclass(frozen=True)
class Limit:
    calls: int
    window_s: float


def parse_limits(header: str | None) -> list[Limit]:
    """``"100:120,20:1"`` -> ``[Limit(100, 120.0), Limit(20, 1.0)]``."""
    if not header:
        return []
    limits = []
    for part in header.split(","):
        calls, window = part.strip().split(":")
        limits.append(Limit(int(calls), float(window)))
    return limits


def parse_counts(header: str | None) -> dict[float, int]:
    """``"3:120,1:1"`` -> ``{120.0: 3, 1.0: 1}`` (window seconds -> calls Riot has counted)."""
    if not header:
        return {}
    counts: dict[float, int] = {}
    for part in header.split(","):
        calls, window = part.strip().split(":")
        counts[float(window)] = int(calls)
    return counts


class TokenBucket:
    """Token bucket sized so that it never admits more than ``limit.calls`` per window."""

    def __init__(
        self, limit: Limit, now: float, burst_fraction: float = DEFAULT_BURST_FRACTION
    ) -> None:
        self.limit = limit
        self.capacity = max(1, math.floor(limit.calls * burst_fraction))
        if limit.calls > self.capacity:
            self.rate = (limit.calls - self.capacity) / limit.window_s  # tokens per second
        else:  # 1-call limits (Riot doesn't use them): one call per window
            self.rate = limit.calls / limit.window_s
        self.tokens = float(self.capacity)
        self._updated = now

    def _refill(self, now: float) -> None:
        elapsed = max(0.0, now - self._updated)
        self.tokens = min(float(self.capacity), self.tokens + elapsed * self.rate)
        self._updated = now

    def wait_time(self, now: float) -> float:
        """Seconds until one token is available (0 if available now)."""
        self._refill(now)
        if self.tokens >= 1.0 - EPSILON:
            return 0.0
        return (1.0 - self.tokens) / self.rate

    def consume(self, now: float) -> None:
        self._refill(now)
        self.tokens -= 1.0

    def charge(self, calls: int, now: float) -> None:
        """Take ``calls`` tokens, going into debt if needed (calls this process didn't make)."""
        self._refill(now)
        self.tokens -= calls

    def clamp(self, remaining: int, now: float) -> None:
        """Never hold more tokens than Riot says are left in its current window."""
        self._refill(now)
        self.tokens = min(self.tokens, float(remaining))


class RateLimiter:
    """Per-host application buckets + per-(host, method) buckets, synced from response headers."""

    def __init__(
        self,
        default_app_limits: list[Limit],
        *,
        clock: Clock = time.monotonic,
        sleep: Sleep = time.sleep,
        burst_fraction: float = DEFAULT_BURST_FRACTION,
    ) -> None:
        self._default_app_limits = default_app_limits
        self._clock = clock
        self._sleep = sleep
        self._burst_fraction = burst_fraction
        self._buckets: dict[BucketKey, list[TokenBucket]] = {}
        self._synced: set[BucketKey] = set()
        self._blocked_until: dict[BucketKey, float] = {}

    @staticmethod
    def app_key(host: str) -> BucketKey:
        return ("app", host)

    @staticmethod
    def method_key(host: str, method: str) -> BucketKey:
        return ("method", host, method)

    def _new_buckets(self, limits: list[Limit], now: float) -> list[TokenBucket]:
        return [TokenBucket(limit, now, self._burst_fraction) for limit in limits]

    def acquire(self, host: str, method: str) -> float:
        """Block until a call to ``method`` on ``host`` fits every known limit. Returns wait (s)."""
        app_key, method_key = self.app_key(host), self.method_key(host, method)
        if app_key not in self._buckets:
            # Before the first response we only know the configured defaults.
            self._buckets[app_key] = self._new_buckets(self._default_app_limits, self._clock())
        waited = 0.0
        while True:
            now = self._clock()
            buckets = self._buckets[app_key] + self._buckets.get(method_key, [])
            delays = [b.wait_time(now) for b in buckets]
            delays += [self._blocked_until.get(k, 0.0) - now for k in (app_key, method_key)]
            delay = max(delays)
            if delay <= EPSILON:
                for bucket in buckets:
                    bucket.consume(now)
                return waited
            self._sleep(delay)
            waited += delay

    def update(self, host: str, method: str, headers: Mapping[str, str]) -> None:
        """Sync buckets with Riot's view after every response (including 429s)."""
        self._sync(
            self.app_key(host),
            headers.get("X-App-Rate-Limit"),
            headers.get("X-App-Rate-Limit-Count"),
        )
        self._sync(
            self.method_key(host, method),
            headers.get("X-Method-Rate-Limit"),
            headers.get("X-Method-Rate-Limit-Count"),
        )

    def _sync(self, key: BucketKey, limits_header: str | None, counts_header: str | None) -> None:
        limits = parse_limits(limits_header)
        if not limits:
            return
        counts = parse_counts(counts_header)
        now = self._clock()
        current = self._buckets.get(key)
        if key not in self._synced or current is None or [b.limit for b in current] != limits:
            # First sight of Riot's numbers (or they changed): rebuild, then charge every call
            # Riot has already counted in this window, as if they had all just happened.
            buckets = self._new_buckets(limits, now)
            for bucket in buckets:
                bucket.charge(counts.get(bucket.limit.window_s, 0), now)
            self._buckets[key] = buckets
            self._synced.add(key)
            return
        for bucket in current:
            used = counts.get(bucket.limit.window_s, 0)
            bucket.clamp(bucket.limit.calls - used, now)

    def block(self, key: BucketKey, seconds: float) -> None:
        """Stop all calls under ``key`` for ``seconds`` (used on 429)."""
        until = self._clock() + seconds
        self._blocked_until[key] = max(self._blocked_until.get(key, 0.0), until)
