"""Crawl state in SQLite (stdlib): what was discovered, what was fetched.

Discovery (listing match IDs per player) and fetching (match + timeline) are separate steps with
a queue between them, so a crash at any point resumes without repeating API calls:

* ``listed_windows``  - (puuid, window) pairs already listed; only *complete* windows are recorded
  (window end <= time of listing), because a window that includes "now" can still gain matches.
* ``match_queue``     - every discovered match ID.
* ``fetched_matches`` - match IDs already fetched (``ok``) or confirmed missing (``not_found``).
"""

from __future__ import annotations

import sqlite3
from collections.abc import Iterable
from datetime import UTC, datetime
from pathlib import Path

SCHEMA = """
CREATE TABLE IF NOT EXISTS listed_windows (
    puuid        TEXT NOT NULL,
    window_start INTEGER NOT NULL,
    window_end   INTEGER NOT NULL,
    listed_at    TEXT NOT NULL,
    PRIMARY KEY (puuid, window_start, window_end)
);
CREATE TABLE IF NOT EXISTS match_queue (
    match_id      TEXT PRIMARY KEY,
    discovered_at TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS fetched_matches (
    match_id   TEXT PRIMARY KEY,
    status     TEXT NOT NULL CHECK (status IN ('ok', 'not_found')),
    fetched_at TEXT NOT NULL
);
"""


def _now() -> str:
    return datetime.now(UTC).isoformat(timespec="seconds")


class CrawlState:
    def __init__(self, path: Path | str) -> None:
        if str(path) != ":memory:":
            Path(path).parent.mkdir(parents=True, exist_ok=True)
        self._db = sqlite3.connect(str(path))
        self._db.executescript(SCHEMA)

    def close(self) -> None:
        self._db.close()

    # discovery -----------------------------------------------------------------
    def window_listed(self, puuid: str, start: int, end: int) -> bool:
        row = self._db.execute(
            "SELECT 1 FROM listed_windows WHERE puuid=? AND window_start=? AND window_end=?",
            (puuid, start, end),
        ).fetchone()
        return row is not None

    def mark_window_listed(self, puuid: str, start: int, end: int) -> None:
        with self._db:
            self._db.execute(
                "INSERT OR IGNORE INTO listed_windows VALUES (?, ?, ?, ?)",
                (puuid, start, end, _now()),
            )

    def enqueue(self, match_ids: Iterable[str]) -> int:
        """Add match IDs to the queue; returns how many were new."""
        with self._db:
            before = self._db.total_changes
            self._db.executemany(
                "INSERT OR IGNORE INTO match_queue VALUES (?, ?)",
                [(mid, _now()) for mid in match_ids],
            )
            return self._db.total_changes - before

    # fetching ------------------------------------------------------------------
    def pending(self, limit: int) -> list[str]:
        """Discovered but not yet fetched, oldest first."""
        rows = self._db.execute(
            """
            SELECT q.match_id FROM match_queue q
            LEFT JOIN fetched_matches f USING (match_id)
            WHERE f.match_id IS NULL
            ORDER BY q.discovered_at, q.match_id
            LIMIT ?
            """,
            (limit,),
        ).fetchall()
        return [r[0] for r in rows]

    def mark_fetched(self, match_id: str, status: str) -> None:
        with self._db:
            self._db.execute(
                "INSERT OR REPLACE INTO fetched_matches VALUES (?, ?, ?)",
                (match_id, status, _now()),
            )

    def counts(self) -> dict[str, int]:
        queued = self._db.execute("SELECT COUNT(*) FROM match_queue").fetchone()[0]
        by_status = dict(
            self._db.execute("SELECT status, COUNT(*) FROM fetched_matches GROUP BY status")
        )
        return {
            "queued": queued,
            "fetched_ok": by_status.get("ok", 0),
            "not_found": by_status.get("not_found", 0),
            "pending": queued - by_status.get("ok", 0) - by_status.get("not_found", 0),
        }
