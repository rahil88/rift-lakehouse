from collections import Counter
from datetime import UTC, datetime

import httpx

from collector.crawl import CrawlConfig, run_crawl
from collector.riot_client import RiotClient
from collector.sinks import MemorySink
from collector.state import CrawlState
from tests.factories import fake_puuid, make_league, make_match, make_timeline

HEADERS = {"X-App-Rate-Limit": "20:1,100:120", "X-App-Rate-Limit-Count": "1:1,1:120"}
NOW = datetime(2026, 11, 11, 12, tzinfo=UTC)
CFG = CrawlConfig(
    start=datetime(2026, 11, 10, tzinfo=UTC),
    end=datetime(2026, 11, 11, tzinfo=UTC),
    tiers=("challenger",),
    max_players=2,
    max_matches=10,
)


def fake_riot(calls: Counter):
    """Two seeded players who share one match (NA1_2): dedup must fetch it once."""
    ids_by_player = {0: ["NA1_1", "NA1_2"], 1: ["NA1_2", "NA1_3"], 2: ["NA1_4"]}

    def handler(request: httpx.Request) -> httpx.Response:
        path = request.url.path
        if "/riot/account/v1/" in path:
            calls["account"] += 1
            return httpx.Response(200, headers=HEADERS, json={"puuid": fake_puuid(2)})
        if "/league/v4/" in path:
            calls["league"] += 1
            return httpx.Response(200, headers=HEADERS, json=make_league("challenger", 3))
        if path.endswith("/ids"):
            calls["ids"] += 1
            player = int(path.split("/by-puuid/")[1].split("-")[1])
            return httpx.Response(200, headers=HEADERS, json=ids_by_player[player])
        match_id = path.split("/matches/")[1].split("/")[0]
        if path.endswith("/timeline"):
            calls["timeline"] += 1
            return httpx.Response(200, headers=HEADERS, json=make_timeline(match_id))
        calls["match"] += 1
        if match_id == "NA1_3":
            return httpx.Response(404, headers=HEADERS, json={})
        return httpx.Response(200, headers=HEADERS, json=make_match(match_id))

    return httpx.MockTransport(handler)


def run(state, clock, calls, cfg=CFG):
    client = RiotClient("k", transport=fake_riot(calls), clock=clock, sleep=clock.sleep)
    sink = MemorySink()
    report = run_crawl(client, state, sink, cfg, platform="na1", now=NOW, clock=clock)
    return report, sink


def test_crawl_emits_ranks_matches_and_exploded_timelines(clock):
    calls = Counter()
    report, sink = run(CrawlState(":memory:"), clock, calls)
    types = Counter(e.event_type for e in sink.events)
    assert types == {"player_rank": 3, "match": 2, "timeline_event": 2 * 6}
    assert report.matches_fetched == 2 and report.matches_not_found == 1
    assert calls == {"league": 1, "ids": 2, "match": 3, "timeline": 2}  # NA1_2 fetched once
    assert report.api_requests == 8


def test_rerun_is_resumable_and_does_not_refetch(clock):
    state = CrawlState(":memory:")
    run(state, clock, Counter())
    calls = Counter()
    report, sink = run(state, clock, calls)
    # complete window already listed, every match already fetched: only the ladder is re-read
    assert calls == {"league": 1}
    assert report.windows_skipped == 2 and report.matches_fetched == 0
    assert {e.event_type for e in sink.events} == {"player_rank"}


def test_crash_mid_fetch_resumes_remaining_matches(clock):
    state = CrawlState(":memory:")
    state.enqueue(["NA1_1", "NA1_2"])
    state.mark_fetched("NA1_1", "ok")  # pretend we crashed after the first match
    assert state.pending(10) == ["NA1_2"]


def test_event_ids_are_deterministic():
    from collector.events import match_event, timeline_events

    assert match_event(make_match("NA1_9")).event_id == "match:NA1_9"
    ids = [e.event_id for e in timeline_events(make_timeline("NA1_9", frames=1))]
    assert ids == ["timeline_event:NA1_9:0:0", "timeline_event:NA1_9:0:1"]


def test_extra_riot_id_is_resolved_via_account_v1_and_seeded(clock):
    from dataclasses import replace

    calls = Counter()
    cfg = replace(CFG, extra_riot_ids=("Me#NA1",))
    report, sink = run(CrawlState(":memory:"), clock, calls, cfg)
    assert calls["account"] == 1 and calls["ids"] == 3
    assert report.players_seeded == 3
    assert "match:NA1_4" in {e.event_id for e in sink.events}


def test_rank_snapshot_labeled_by_logical_date_is_idempotent(clock):
    """Airflow passes the run's logical date: rerunning the day yields the same rank keys."""
    from dataclasses import replace

    cfg = replace(CFG, snapshot_at=datetime(2026, 11, 11, tzinfo=UTC))
    ids = []
    for _ in range(2):
        _, sink = run(CrawlState(":memory:"), clock, Counter(), cfg)
        ids.append(sorted(e.event_id for e in sink.events if e.event_type == "player_rank"))
    assert ids[0] == ids[1]
    assert ids[0][0].endswith(":2026-11-11T00:00:00+00:00")
