"""Where events go. The crawl only knows the ``Sink`` protocol.

* ``JsonlSink``  - local files, for Phase 1 and debugging
* ``MemorySink`` - tests
* Kafka sink with contract validation - Phase 2 (``collector.producer``)
"""

from __future__ import annotations

import json
from pathlib import Path
from typing import IO, Protocol

from collector.events import Event


class Sink(Protocol):
    def emit(self, event: Event) -> None: ...

    def flush(self) -> None:
        """Block until everything emitted so far is durably delivered."""
        ...


class MemorySink:
    def __init__(self) -> None:
        self.events: list[Event] = []
        self.flushes = 0

    def emit(self, event: Event) -> None:
        self.events.append(event)

    def flush(self) -> None:
        self.flushes += 1


class JsonlSink:
    """One JSONL file per event type under ``root`` (appends; downstream dedups by event_id)."""

    def __init__(self, root: Path) -> None:
        self.root = root
        self._files: dict[str, IO[str]] = {}

    def emit(self, event: Event) -> None:
        handle = self._files.get(event.event_type)
        if handle is None:
            self.root.mkdir(parents=True, exist_ok=True)
            handle = (self.root / f"{event.event_type}.jsonl").open("a", encoding="utf-8")
            self._files[event.event_type] = handle
        line = {"event_id": event.event_id, "key": event.key, "payload": event.payload}
        handle.write(json.dumps(line, separators=(",", ":")) + "\n")

    def flush(self) -> None:
        for handle in self._files.values():
            handle.flush()

    def close(self) -> None:
        for handle in self._files.values():
            handle.close()
        self._files.clear()
