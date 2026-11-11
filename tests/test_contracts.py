import copy
import json
from datetime import UTC, datetime
from pathlib import Path

import pytest

from collector.contracts import ContractRegistry
from collector.events import match_event, rank_events, timeline_events
from tests.factories import make_league, make_match, make_timeline

REGISTRY = ContractRegistry()
SNAP = datetime(2026, 11, 11, tzinfo=UTC)


def valid_payloads():
    return {
        "match": make_match("NA1_1"),
        "timeline_event": next(timeline_events(make_timeline("NA1_1"))).payload,
        "player_rank": next(
            rank_events(make_league("challenger", 1), platform="na1", snapshot_at=SNAP)
        ).payload,
    }


@pytest.mark.parametrize("event_type", ["match", "timeline_event", "player_rank"])
def test_valid_payloads_pass(event_type):
    assert REGISTRY.validate(event_type, valid_payloads()[event_type]) == []


def _set(path, value):
    def mutate(payload):
        target = payload
        for key in path[:-1]:
            target = target[key]
        if value is _DELETE:
            del target[path[-1]]
        else:
            target[path[-1]] = value

    return mutate


_DELETE = object()

MALFORMED = [
    ("match", _set(["metadata", "matchId"], _DELETE), "$.metadata", "required"),
    ("match", _set(["metadata", "matchId"], "not a match id"), "$.metadata.matchId", "pattern"),
    ("match", _set(["info", "gameDuration"], "1800"), "$.info.gameDuration", "type"),
    ("match", _set(["info", "queueId"], 450), "$.info.queueId", "enum"),  # ARAM is out of scope
    (
        "match",
        lambda p: p["info"]["participants"].pop(),
        "$.info.participants",
        "minItems",
    ),
    (
        "match",
        _set(["info", "participants", 3, "teamPosition"], "SUPPORT"),
        "$.info.participants[3].teamPosition",
        "enum",
    ),
    (
        "match",
        _set(["info", "participants", 0, "kills"], -1),
        "$.info.participants[0].kills",
        "minimum",
    ),
    ("timeline_event", _set(["event", "type"], _DELETE), "$.event", "required"),
    ("timeline_event", _set(["event", "killerId"], _DELETE), "$.event", "required"),  # if/then
    ("timeline_event", _set(["frameIndex"], -1), "$.frameIndex", "minimum"),
    ("player_rank", _set(["tier"], "LEGEND"), "$.tier", "enum"),
    ("player_rank", _set(["leaguePoints"], -5), "$.leaguePoints", "minimum"),
    ("player_rank", _set(["snapshotAt"], "yesterday"), "$.snapshotAt", "pattern"),
]


@pytest.mark.parametrize(("event_type", "mutate", "path", "rule"), MALFORMED)
def test_malformed_payloads_are_rejected_with_path_and_rule(event_type, mutate, path, rule):
    payload = copy.deepcopy(valid_payloads()[event_type])
    mutate(payload)
    violations = REGISTRY.validate(event_type, payload)
    assert (path, rule) in {(v.path, v.rule) for v in violations}, violations


def _timeline_event(event):
    position = {"matchId": "NA1_1", "frameIndex": 0, "eventIndex": 0, "frameTimestamp": 0}
    return {**position, "event": event}


def test_team_zero_allowed_in_general_but_not_on_building_kill():
    """Found in real data: DRAGON_SOUL_GIVEN carries teamId 0."""
    soul = {"type": "DRAGON_SOUL_GIVEN", "timestamp": 1, "teamId": 0, "name": "Ocean"}
    assert REGISTRY.validate("timeline_event", _timeline_event(soul)) == []
    tower = {"type": "BUILDING_KILL", "timestamp": 1, "teamId": 0, "buildingType": "TOWER_BUILDING"}
    rules = {(v.path, v.rule) for v in REGISTRY.validate("timeline_event", _timeline_event(tower))}
    assert ("$.event.teamId", "enum") in rules


def test_unknown_event_type_is_rejected():
    [violation] = REGISTRY.validate("champion_select", {})
    assert violation.rule == "contract"


def test_violation_messages_are_truncated():
    payload = valid_payloads()["match"]
    payload["info"]["participants"] = payload["info"]["participants"][:9]
    assert all(len(v.message) <= 300 for v in REGISTRY.validate("match", payload))


SAMPLES = Path("data/samples")


@pytest.mark.skipif(not SAMPLES.exists(), reason="real Riot samples are local-only (gitignored)")
def test_real_riot_samples_satisfy_the_contracts():
    """Contract vs reality: real payloads from `make` sample fetch must produce zero violations."""
    match = json.loads((SAMPLES / "match.json").read_text())
    assert REGISTRY.validate("match", match_event(match).payload) == []
    timeline = json.loads((SAMPLES / "timeline.json").read_text())
    bad = [
        (e.event_id, v)
        for e in timeline_events(timeline)
        for v in REGISTRY.validate("timeline_event", e.payload)
    ]
    assert bad == []
    league = json.loads((SAMPLES / "challenger.json").read_text())
    ranks = list(rank_events(league, platform="na1", snapshot_at=SNAP))
    assert ranks and all(REGISTRY.validate("player_rank", e.payload) == [] for e in ranks)
