"""Turn Riot API responses into pipeline events.

Three event types, matching the three data contracts (Phase 2):

* ``match``          - one per match, the raw match-v5 payload, keyed by match ID
* ``timeline_event`` - one per timeline event (a timeline has ~750 of them). A whole timeline
  is ~0.5 MB and can exceed Redpanda's 1 MiB default batch size, but each event is small.
  ``participantFrames`` (per-minute gold/xp) are not emitted yet: a documented next step.
* ``player_rank``    - one per ladder entry, plus the tier/queue/platform the entry came from
  and the snapshot time (league-v4 only returns *current* rank, so history is built by
  snapshotting over time: the SCD2 table in Phase 4).

Every event carries a deterministic ``event_id``, so duplicates can be dropped downstream.
"""

from __future__ import annotations

from collections.abc import Iterator
from dataclasses import dataclass
from datetime import datetime
from typing import Any

JSONDict = dict[str, Any]

MATCH = "match"
TIMELINE_EVENT = "timeline_event"
PLAYER_RANK = "player_rank"
EVENT_TYPES = (MATCH, TIMELINE_EVENT, PLAYER_RANK)


@dataclass(frozen=True)
class Event:
    event_type: str
    key: str  # Kafka partition key: match ID or PUUID
    event_id: str  # deterministic, for downstream dedup
    payload: JSONDict


def match_event(match: JSONDict) -> Event:
    match_id = match.get("metadata", {}).get("matchId", "")
    return Event(MATCH, match_id, f"{MATCH}:{match_id}", match)


def timeline_events(timeline: JSONDict) -> Iterator[Event]:
    match_id = timeline.get("metadata", {}).get("matchId", "")
    for frame_index, frame in enumerate(timeline.get("info", {}).get("frames", [])):
        for event_index, event in enumerate(frame.get("events", [])):
            yield Event(
                TIMELINE_EVENT,
                match_id,
                f"{TIMELINE_EVENT}:{match_id}:{frame_index}:{event_index}",
                {
                    "matchId": match_id,
                    "frameIndex": frame_index,
                    "eventIndex": event_index,
                    "frameTimestamp": frame.get("timestamp"),
                    "event": event,
                },
            )


def rank_events(league: JSONDict, *, platform: str, snapshot_at: datetime) -> Iterator[Event]:
    snapshot = snapshot_at.isoformat(timespec="seconds")
    for entry in league.get("entries", []):
        puuid = entry.get("puuid", "")
        yield Event(
            PLAYER_RANK,
            puuid,
            f"{PLAYER_RANK}:{puuid}:{snapshot}",
            {
                **entry,
                "tier": league.get("tier"),
                "queue": league.get("queue"),
                "platformId": platform.upper(),
                "snapshotAt": snapshot,
            },
        )
