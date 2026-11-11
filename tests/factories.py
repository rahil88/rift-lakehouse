"""Test helpers: a fake clock and synthetic (not real-player) Riot payloads."""

from __future__ import annotations


class FakeClock:
    """Monotonic clock whose sleep() just advances time, so rate-limit tests run instantly."""

    def __init__(self, start: float = 1000.0) -> None:
        self.now = start
        self.sleeps: list[float] = []

    def __call__(self) -> float:
        return self.now

    def sleep(self, seconds: float) -> None:
        self.sleeps.append(seconds)
        self.now += seconds


def fake_puuid(i: int) -> str:
    return f"puuid-{i:04d}-" + "x" * 66  # real PUUIDs are 78 chars


def make_participant(i: int, *, puuid: str, team_id: int, win: bool) -> dict:
    positions = ["TOP", "JUNGLE", "MIDDLE", "BOTTOM", "UTILITY"]
    return {
        "participantId": i,
        "puuid": puuid,
        "riotIdGameName": f"Player{i}",
        "riotIdTagline": "TEST",
        "championId": 100 + i,
        "championName": f"Champ{i}",
        "teamId": team_id,
        "teamPosition": positions[(i - 1) % 5],
        "individualPosition": positions[(i - 1) % 5],
        "win": win,
        "kills": i,
        "deaths": 2,
        "assists": 3,
        "champLevel": 15,
        "goldEarned": 10000 + i,
        "totalMinionsKilled": 150,
        "neutralMinionsKilled": 10,
        "totalDamageDealtToChampions": 20000,
        "visionScore": 25,
        "gameEndedInEarlySurrender": False,
    }


def make_match(match_id: str, *, game_creation_ms: int = 1_762_800_000_000) -> dict:
    puuids = [fake_puuid(i) for i in range(1, 11)]
    participants = [
        make_participant(i, puuid=puuids[i - 1], team_id=100 if i <= 5 else 200, win=i <= 5)
        for i in range(1, 11)
    ]
    return {
        "metadata": {"dataVersion": "2", "matchId": match_id, "participants": puuids},
        "info": {
            "endOfGameResult": "GameComplete",
            "gameCreation": game_creation_ms,
            "gameStartTimestamp": game_creation_ms + 30_000,
            "gameEndTimestamp": game_creation_ms + 30_000 + 1_800_000,
            "gameDuration": 1800,
            "gameId": int(match_id.split("_")[1]),
            "gameMode": "CLASSIC",
            "gameType": "MATCHED_GAME",
            "gameVersion": "16.22.834.8910",
            "mapId": 11,
            "platformId": "NA1",
            "queueId": 420,
            "participants": participants,
            "teams": [
                {"teamId": 100, "win": True, "bans": [], "objectives": {}},
                {"teamId": 200, "win": False, "bans": [], "objectives": {}},
            ],
        },
    }


def make_timeline(match_id: str, *, frames: int = 3, events_per_frame: int = 2) -> dict:
    return {
        "metadata": {"dataVersion": "2", "matchId": match_id, "participants": []},
        "info": {
            "frameInterval": 60000,
            "frames": [
                {
                    "timestamp": f * 60000,
                    "participantFrames": {},
                    "events": [
                        {
                            "type": "CHAMPION_KILL",
                            "timestamp": f * 60000 + e,
                            "killerId": 1,
                            "victimId": 6,
                            "position": {"x": 100, "y": 200},
                        }
                        for e in range(events_per_frame)
                    ],
                }
                for f in range(frames)
            ],
        },
    }


def make_league(tier: str, n: int, *, offset: int = 0) -> dict:
    return {
        "tier": tier.upper(),
        "leagueId": None,
        "queue": "RANKED_SOLO_5x5",
        "name": None,
        "entries": [
            {
                "puuid": fake_puuid(offset + i),
                "leaguePoints": 1000 - i,
                "rank": "I",
                "wins": 100,
                "losses": 80,
                "veteran": False,
                "inactive": False,
                "freshBlood": False,
                "hotStreak": False,
            }
            for i in range(n)
        ],
    }
