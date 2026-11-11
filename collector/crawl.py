"""Resumable crawl: ladders -> seed players -> match IDs -> match + timeline.

1. **Ladders** (league-v4, 1 call per tier): every entry becomes a ``player_rank`` event, and
   the top ``max_players`` by tier then LP become seeds.
2. **Discovery** (match-v5, 1+ call per seed): list ranked solo match IDs played in
   [start, end) and add them to the SQLite queue. Complete windows are remembered, so reruns
   skip them.
3. **Fetch** (match-v5, 2 calls per match): drain up to ``max_matches`` unfetched IDs. Each match
   is emitted, the sink is flushed, and only *then* is the ID marked fetched: delivery is
   at-least-once, and downstream dedups on ``event_id`` / natural keys.

Budget with a personal/dev key (100 calls / 2 min per host): ~0.75 call/s sustained, so a match
(2 calls) costs ~2.7 s.
"""

from __future__ import annotations

import logging
import time
from dataclasses import asdict, dataclass, field
from datetime import UTC, datetime

from collector.events import match_event, rank_events, timeline_events
from collector.rate_limiter import Clock
from collector.riot_client import RiotClient
from collector.sinks import Sink
from collector.state import CrawlState

log = logging.getLogger(__name__)

TIER_ORDER = {"CHALLENGER": 0, "GRANDMASTER": 1, "MASTER": 2}


@dataclass(frozen=True)
class CrawlConfig:
    start: datetime  # inclusive, UTC
    end: datetime  # exclusive, UTC
    tiers: tuple[str, ...] = ("challenger", "grandmaster")
    max_players: int = 50
    max_matches: int = 200
    snapshot_ranks: bool = True
    extra_riot_ids: tuple[str, ...] = ()  # "gameName#tagLine", resolved to PUUIDs via account-v1

    def __post_init__(self) -> None:
        if self.start.tzinfo is None or self.end.tzinfo is None:
            raise ValueError("start/end must be timezone-aware (UTC)")
        if self.end <= self.start:
            raise ValueError(f"end ({self.end}) must be after start ({self.start})")


@dataclass
class CrawlReport:
    start: str = ""
    end: str = ""
    rank_events: int = 0
    players_seeded: int = 0
    windows_listed: int = 0
    windows_skipped: int = 0
    match_ids_new: int = 0
    matches_fetched: int = 0
    matches_not_found: int = 0
    timelines_missing: int = 0
    timeline_events: int = 0
    api_requests: int = 0
    rate_limited_429: int = 0
    wall_seconds: float = 0.0
    state_after: dict[str, int] = field(default_factory=dict)

    def as_dict(self) -> dict[str, object]:
        return asdict(self)


def run_crawl(
    client: RiotClient,
    state: CrawlState,
    sink: Sink,
    cfg: CrawlConfig,
    *,
    platform: str,
    now: datetime | None = None,
    clock: Clock = time.monotonic,
) -> CrawlReport:
    t0 = clock()
    stats_before = dict(client.stats)
    snapshot_at = now or datetime.now(UTC)
    report = CrawlReport(start=cfg.start.isoformat(), end=cfg.end.isoformat())

    # 1. ladders -> rank snapshot + seeds
    ladder: list[dict] = []
    for tier in cfg.tiers:
        league = client.league(tier)
        for event in rank_events(league, platform=platform, snapshot_at=snapshot_at):
            ladder.append(event.payload)
            if cfg.snapshot_ranks:
                sink.emit(event)
                report.rank_events += 1
    if cfg.snapshot_ranks:
        sink.flush()
    seeds = sorted(
        (e for e in ladder if e.get("puuid")),
        key=lambda e: (TIER_ORDER.get(e.get("tier", ""), 9), -e.get("leaguePoints", 0)),
    )[: cfg.max_players]
    for riot_id in cfg.extra_riot_ids:  # account-v1: Riot ID -> PUUID
        game_name, _, tag_line = riot_id.partition("#")
        account = client.account_by_riot_id(game_name, tag_line)
        if account is None:
            log.warning("riot id %r not found; skipping", riot_id)
            continue
        seeds.append({"puuid": account["puuid"]})
    report.players_seeded = len(seeds)
    log.info("ladder: %d entries, %d seeds", len(ladder), len(seeds))

    # 2. discovery
    start_s, end_s = int(cfg.start.timestamp()), int(cfg.end.timestamp())
    window_complete = cfg.end <= snapshot_at
    for entry in seeds:
        puuid = entry["puuid"]
        if state.window_listed(puuid, start_s, end_s):
            report.windows_skipped += 1
            continue
        ids = client.match_ids(puuid, start_time=start_s, end_time=end_s)
        report.match_ids_new += state.enqueue(ids)
        report.windows_listed += 1
        if window_complete:
            state.mark_window_listed(puuid, start_s, end_s)
    log.info("discovery: %d new match IDs", report.match_ids_new)

    # 3. fetch
    pending = state.pending(cfg.max_matches)
    for i, match_id in enumerate(pending, start=1):
        match = client.match(match_id)
        if match is None:
            state.mark_fetched(match_id, "not_found")
            report.matches_not_found += 1
            continue
        sink.emit(match_event(match))
        timeline = client.timeline(match_id)
        if timeline is None:
            report.timelines_missing += 1
        else:
            for event in timeline_events(timeline):
                sink.emit(event)
                report.timeline_events += 1
        sink.flush()  # durable first ...
        state.mark_fetched(match_id, "ok")  # ... then recorded: at-least-once
        report.matches_fetched += 1
        if i % 10 == 0 or i == len(pending):
            log.info("fetched %d/%d matches (%.0fs)", i, len(pending), clock() - t0)

    def delta(key: str) -> int:
        return client.stats.get(key, 0) - stats_before.get(key, 0)

    report.api_requests = delta("requests")
    report.rate_limited_429 = sum(delta(k) for k in client.stats if k.startswith("429_"))
    report.wall_seconds = round(clock() - t0, 1)
    report.state_after = state.counts()
    return report
