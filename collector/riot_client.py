"""Riot API client: rate limiting, retries, and the handful of endpoints the crawl needs.

Routing: league-v4 lives on a *platform* host (``na1.api.riotgames.com``); match-v5 and
account-v1 live on a *regional* host (``americas.api.riotgames.com``). Riot counts rate limits
per host, so each host gets its own buckets (verified: an na1 call doesn't move the americas
``X-App-Rate-Limit-Count``).

Retry policy:

* 200 -> JSON;  404 -> ``None`` (e.g. a match with no timeline)
* 401/403 -> ``RiotAuthError`` immediately (development keys expire every 24h; retrying won't help)
* 429 -> block the limited key for ``Retry-After`` seconds. ``X-Rate-Limit-Type`` says which key:
  ``application`` blocks the whole host, ``method`` blocks the endpoint. A 429 *without* that
  header is a shared *service* limit: back off exponentially when Retry-After is missing too.
* 5xx / network errors -> exponential backoff with jitter
* anything else -> ``RiotApiError``
"""

from __future__ import annotations

import logging
import random
import time
from collections import Counter
from collections.abc import Callable
from typing import Any
from urllib.parse import quote

import httpx

from collector.rate_limiter import Clock, Limit, RateLimiter, Sleep

log = logging.getLogger(__name__)

# Personal/development key limits. Only used until the first response carries the real ones.
DEFAULT_APP_LIMITS = [Limit(20, 1.0), Limit(100, 120.0)]
TIERS = ("challenger", "grandmaster", "master")
RANKED_SOLO_QUEUE = "RANKED_SOLO_5x5"
RANKED_SOLO_QUEUE_ID = 420

JSONDict = dict[str, Any]


class RiotApiError(RuntimeError):
    """Non-retryable or unexpected Riot API response."""


class RiotAuthError(RiotApiError):
    """401/403: key missing, invalid, or expired."""


class RiotRetryExhausted(RiotApiError):
    """Gave up after ``max_retries`` retries."""


class RiotClient:
    def __init__(
        self,
        api_key: str,
        *,
        platform: str = "na1",
        region: str = "americas",
        limiter: RateLimiter | None = None,
        transport: httpx.BaseTransport | None = None,
        clock: Clock = time.monotonic,
        sleep: Sleep = time.sleep,
        rng: Callable[[], float] = random.random,
        max_retries: int = 5,
        backoff_base_s: float = 1.0,
        backoff_cap_s: float = 60.0,
        timeout_s: float = 15.0,
    ) -> None:
        self.platform_host = f"{platform}.api.riotgames.com"
        self.region_host = f"{region}.api.riotgames.com"
        self.limiter = limiter or RateLimiter(DEFAULT_APP_LIMITS, clock=clock, sleep=sleep)
        self._sleep = sleep
        self._rng = rng
        self.max_retries = max_retries
        self.backoff_base_s = backoff_base_s
        self.backoff_cap_s = backoff_cap_s
        self._http = httpx.Client(
            headers={"X-Riot-Token": api_key}, transport=transport, timeout=timeout_s
        )
        self.stats: Counter[str] = Counter()

    def close(self) -> None:
        self._http.close()

    def __enter__(self) -> RiotClient:
        return self

    def __exit__(self, *exc_info: object) -> None:
        self.close()

    # ------------------------------------------------------------------ core request loop
    def _backoff_s(self, attempt: int) -> float:
        """Exponential backoff with "equal jitter": half fixed, half random."""
        ceiling = min(self.backoff_cap_s, self.backoff_base_s * 2**attempt)
        return ceiling / 2 + self._rng() * ceiling / 2

    def _get(self, host: str, method: str, path: str, params: JSONDict | None = None) -> Any:  # noqa: ANN401 - JSON
        url = f"https://{host}{path}"
        for attempt in range(self.max_retries + 1):
            self.limiter.acquire(host, method)
            self.stats["requests"] += 1
            try:
                resp = self._http.get(url, params=params)
            except httpx.TransportError as exc:
                self.stats["network_errors"] += 1
                delay = self._backoff_s(attempt)
                log.warning("%s network error (%s); retry in %.1fs", method, exc, delay)
                self._sleep(delay)
                continue

            self.limiter.update(host, method, resp.headers)
            status = resp.status_code
            if status == 200:
                return resp.json()
            if status == 404:
                self.stats["not_found"] += 1
                return None
            if status in (401, 403):
                raise RiotAuthError(
                    f"{status} from {method}: RIOT_API_KEY is invalid or expired. Development "
                    "keys deactivate every 24h; regenerate at https://developer.riotgames.com "
                    "and update .env."
                )
            if status == 429:
                limit_type = resp.headers.get("X-Rate-Limit-Type")  # application|method|None
                self.stats[f"429_{limit_type or 'service'}"] += 1
                retry_after = _parse_retry_after(resp.headers.get("Retry-After"))
                delay = retry_after if retry_after is not None else self._backoff_s(attempt)
                key = (
                    RateLimiter.app_key(host)
                    if limit_type == "application"
                    else RateLimiter.method_key(host, method)
                )
                self.limiter.block(key, delay)  # the next acquire() waits it out
                log.warning("429 (%s) on %s; waiting %.1fs", limit_type or "service", method, delay)
                continue
            if status >= 500:
                self.stats["server_errors"] += 1
                delay = self._backoff_s(attempt)
                log.warning("%s on %s; retry in %.1fs", status, method, delay)
                self._sleep(delay)
                continue
            raise RiotApiError(f"{status} from {method}: {resp.text[:200]}")
        raise RiotRetryExhausted(f"{method}: gave up after {self.max_retries} retries")

    # ------------------------------------------------------------------ endpoints
    def league(self, tier: str, queue: str = RANKED_SOLO_QUEUE) -> JSONDict:
        """league-v4: the whole Challenger / Grandmaster / Master ladder for a queue."""
        if tier not in TIERS:
            raise ValueError(f"tier must be one of {TIERS}, got {tier!r}")
        path = f"/lol/league/v4/{tier}leagues/by-queue/{queue}"
        result = self._get(self.platform_host, f"league-v4.{tier}", path)
        return result or {"tier": tier.upper(), "queue": queue, "entries": []}

    def match_ids(
        self,
        puuid: str,
        *,
        start_time: int,
        end_time: int,
        queue: int = RANKED_SOLO_QUEUE_ID,
        page_size: int = 100,
    ) -> list[str]:
        """match-v5: every match ID for a player in [start_time, end_time) epoch seconds."""
        ids: list[str] = []
        start = 0
        while True:
            params = {
                "startTime": start_time,
                "endTime": end_time,
                "queue": queue,
                "type": "ranked",
                "start": start,
                "count": page_size,
            }
            path = f"/lol/match/v5/matches/by-puuid/{quote(puuid, safe='')}/ids"
            page = self._get(self.region_host, "match-v5.ids_by_puuid", path, params) or []
            ids.extend(page)
            if len(page) < page_size:
                return ids
            start += page_size

    def match(self, match_id: str) -> JSONDict | None:
        return self._get(self.region_host, "match-v5.match", f"/lol/match/v5/matches/{match_id}")

    def timeline(self, match_id: str) -> JSONDict | None:
        path = f"/lol/match/v5/matches/{match_id}/timeline"
        return self._get(self.region_host, "match-v5.timeline", path)

    def account_by_riot_id(self, game_name: str, tag_line: str) -> JSONDict | None:
        """account-v1: Riot ID (``name#tag``) -> account with ``puuid``."""
        path = (
            f"/riot/account/v1/accounts/by-riot-id/{quote(game_name, safe='')}/"
            f"{quote(tag_line, safe='')}"
        )
        return self._get(self.region_host, "account-v1.by_riot_id", path)


def _parse_retry_after(value: str | None) -> float | None:
    if value is None:
        return None
    try:
        return max(0.0, float(value))
    except ValueError:
        return None
