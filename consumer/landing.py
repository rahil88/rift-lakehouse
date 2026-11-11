"""Kafka -> local JSONL landing files, in micro-batches.

Each cycle polls up to ``max_batch`` messages (or until the topics are idle), writes **every**
buffered message to disk, and only then commits the consumer offsets. A crash at any point
re-delivers uncommitted messages; nothing is lost (at-least-once).

File names are deterministic, built from topic, partition and the first/last offset in the file:

    landing/<table>/ingest_date=<date of first message>/<topic>-p<P>-<first>-<last>.jsonl

so re-consuming the same offsets rewrites the same file, which COPY INTO then skips as already
loaded. If a crash shifts batch boundaries, bronze can hold the same event twice; silver
dedups on ``event_id`` / natural keys, so the pipeline stays idempotent end to end.

Each line keeps the event's payload as a JSON *string*: bronze stores exactly what the producer
sent, and parsing happens in silver.
"""

from __future__ import annotations

import json
import logging
import os
import time
from collections import defaultdict
from collections.abc import Callable
from dataclasses import dataclass, field
from datetime import UTC, datetime
from pathlib import Path
from typing import Any, Protocol

log = logging.getLogger(__name__)

RAW_TOPIC = "lol.match.raw"
DLQ_TOPIC = "lol.match.dlq"
DLQ_TABLE = "dlq"


class MessageLike(Protocol):
    def error(self) -> object: ...
    def topic(self) -> str: ...
    def partition(self) -> int: ...
    def offset(self) -> int: ...
    def value(self) -> bytes: ...
    def timestamp(self) -> tuple[int, int]: ...


class ConsumerLike(Protocol):
    def consume(self, num_messages: int, timeout: float) -> list[Any]: ...
    def commit(self, *, asynchronous: bool) -> object: ...


@dataclass
class LandingReport:
    messages: int = 0
    files_written: int = 0
    cycles: int = 0
    by_table: dict[str, int] = field(default_factory=lambda: defaultdict(int))
    wall_seconds: float = 0.0

    def as_dict(self) -> dict[str, Any]:
        return {**self.__dict__, "by_table": dict(self.by_table)}


def landing_record(msg: MessageLike) -> tuple[str, dict[str, Any]]:
    """Kafka message -> (landing table, JSONL record)."""
    envelope = json.loads(msg.value())
    _, ts_ms = msg.timestamp()
    record: dict[str, Any] = {
        "event_id": envelope.get("event_id"),
        "event_type": envelope.get("event_type"),
        "schema_version": envelope.get("schema_version"),
        "producer": envelope.get("producer"),
        "payload_json": json.dumps(envelope.get("payload"), separators=(",", ":")),
        "kafka_topic": msg.topic(),
        "kafka_partition": msg.partition(),
        "kafka_offset": msg.offset(),
        "kafka_timestamp_ms": ts_ms,
    }
    if msg.topic() == DLQ_TOPIC:
        record["rejected_at"] = envelope.get("rejected_at")
        record["errors_json"] = json.dumps(envelope.get("errors"), separators=(",", ":"))
        return DLQ_TABLE, record
    record["emitted_at"] = envelope.get("emitted_at")
    return str(envelope.get("event_type")), record


def _write_atomic(path: Path, lines: list[str]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_suffix(".jsonl.tmp")
    with tmp.open("w", encoding="utf-8") as fh:
        fh.writelines(lines)
        fh.flush()
        os.fsync(fh.fileno())
    os.replace(tmp, path)  # never leaves a half-written file for the uploader


class LandingConsumer:
    def __init__(
        self,
        consumer: ConsumerLike,
        landing_dir: Path,
        *,
        max_batch: int = 20_000,
        idle_timeout_s: float = 10.0,
        clock: Callable[[], float] = time.monotonic,
    ) -> None:
        self._consumer = consumer
        self._landing_dir = landing_dir
        self._max_batch = max_batch
        self._idle_timeout_s = idle_timeout_s
        self._clock = clock

    def run(self) -> LandingReport:
        """Drain the topics: run micro-batch cycles until no message arrives for idle_timeout."""
        report = LandingReport()
        t0 = self._clock()
        while True:
            written = self._cycle(report)
            if written == 0:
                break
        report.wall_seconds = round(self._clock() - t0, 1)
        return report

    def _cycle(self, report: LandingReport) -> int:
        buffers: dict[tuple[str, str, int], list[tuple[int, int, str]]] = defaultdict(list)
        count = 0
        idle_since = self._clock()
        while count < self._max_batch:
            batch = self._consumer.consume(num_messages=1000, timeout=1.0)
            if not batch:
                if self._clock() - idle_since >= self._idle_timeout_s:
                    break
                continue
            idle_since = self._clock()
            for msg in batch:
                if msg.error():
                    raise RuntimeError(f"kafka error: {msg.error()}")
                table, record = landing_record(msg)
                line = json.dumps(record, separators=(",", ":")) + "\n"
                buffers[(table, msg.topic(), msg.partition())].append(
                    (msg.offset(), record["kafka_timestamp_ms"], line)
                )
                report.by_table[table] += 1
                count += 1
        if count == 0:
            return 0
        for (table, topic, partition), rows in buffers.items():
            first, last = rows[0][0], rows[-1][0]
            day = datetime.fromtimestamp(rows[0][1] / 1000, UTC).date().isoformat()
            name = f"{topic}-p{partition}-{first:012d}-{last:012d}.jsonl"
            _write_atomic(
                self._landing_dir / table / f"ingest_date={day}" / name, [r[2] for r in rows]
            )
            report.files_written += 1
        self._consumer.commit(asynchronous=False)  # only after every buffer is on disk
        report.messages += count
        report.cycles += 1
        log.info("landed %d messages in %d files", count, len(buffers))
        return count


def kafka_consumer(bootstrap_servers: str, group_id: str = "rift-landing"):  # noqa: ANN201
    from confluent_kafka import Consumer

    consumer = Consumer(
        {
            "bootstrap.servers": bootstrap_servers,
            "group.id": group_id,
            "auto.offset.reset": "earliest",
            "enable.auto.commit": False,
        }
    )
    consumer.subscribe([RAW_TOPIC, DLQ_TOPIC])
    return consumer
