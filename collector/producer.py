"""Contract-enforcing Kafka sink: validate *before* the event enters the pipeline.

* valid   -> ``lol.match.raw``  as an envelope ``{event_id, event_type, schema_version,
  emitted_at, producer, payload}``
* invalid -> ``lol.match.dlq``  with the full payload plus ``errors`` (path, message, rule),
  ``schema_version`` and ``rejected_at``, so a contract fix can replay it without re-calling Riot

The partition key is the match ID (or PUUID for ranks), so one match and all of its timeline
events share a partition and stay in order. The producer is idempotent with acks=all and zstd
compression. ``flush()`` raises if any delivery failed, so the crawl never marks a match fetched
unless Kafka acknowledged it.
"""

from __future__ import annotations

import json
import logging
from collections import Counter
from collections.abc import Callable
from dataclasses import asdict, dataclass, field
from datetime import UTC, datetime
from pathlib import Path
from typing import Any, Protocol

from collector.contracts import ContractRegistry
from collector.events import Event

log = logging.getLogger(__name__)

RAW_TOPIC = "lol.match.raw"
DLQ_TOPIC = "lol.match.dlq"
DAY_MS = 24 * 3600 * 1000
TOPICS: dict[str, tuple[int, dict[str, str]]] = {
    RAW_TOPIC: (3, {"retention.ms": str(7 * DAY_MS), "cleanup.policy": "delete"}),
    DLQ_TOPIC: (1, {"retention.ms": str(30 * DAY_MS), "cleanup.policy": "delete"}),
}


def producer_config(bootstrap_servers: str) -> dict[str, Any]:
    return {
        "bootstrap.servers": bootstrap_servers,
        "client.id": "rift-collector",
        "enable.idempotence": True,  # broker dedups producer retries
        "acks": "all",
        "compression.type": "zstd",
        "linger.ms": 50,
    }


def ensure_topics(bootstrap_servers: str) -> list[str]:
    """Create the raw and DLQ topics if missing (idempotent). Returns the topics created."""
    from confluent_kafka.admin import AdminClient, NewTopic

    admin = AdminClient({"bootstrap.servers": bootstrap_servers})
    existing = set(admin.list_topics(timeout=10).topics)
    missing = [
        NewTopic(name, num_partitions=parts, replication_factor=1, config=cfg)
        for name, (parts, cfg) in TOPICS.items()
        if name not in existing
    ]
    if not missing:
        return []
    for name, future in admin.create_topics(missing).items():
        future.result()  # raises on failure
        log.info("created topic %s", name)
    return [t.topic for t in missing]


class ProducerLike(Protocol):
    def produce(self, topic: str, **kwargs: Any) -> None: ...  # noqa: ANN401
    def poll(self, timeout: float) -> int: ...
    def flush(self, timeout: float) -> int: ...


class DeliveryError(RuntimeError):
    pass


@dataclass
class ValidationMetrics:
    valid: Counter[str] = field(default_factory=Counter)
    rejected: Counter[str] = field(default_factory=Counter)

    def record(self, event_type: str, *, ok: bool) -> None:
        (self.valid if ok else self.rejected)[event_type] += 1

    def rejection_rate(self, event_type: str | None = None) -> float:
        if event_type is None:
            valid, rejected = sum(self.valid.values()), sum(self.rejected.values())
        else:
            valid, rejected = self.valid[event_type], self.rejected[event_type]
        total = valid + rejected
        return rejected / total if total else 0.0

    def summary(self) -> dict[str, Any]:
        types = sorted(set(self.valid) | set(self.rejected))
        return {
            "valid": sum(self.valid.values()),
            "rejected": sum(self.rejected.values()),
            "rejection_rate": round(self.rejection_rate(), 6),
            "by_type": {
                t: {
                    "valid": self.valid[t],
                    "rejected": self.rejected[t],
                    "rejection_rate": round(self.rejection_rate(t), 6),
                }
                for t in types
            },
        }


def _utc_now() -> str:
    return datetime.now(UTC).isoformat(timespec="milliseconds")


class KafkaSink:
    def __init__(
        self,
        producer: ProducerLike,
        registry: ContractRegistry,
        *,
        producer_name: str = "rift-collector",
        now: Callable[[], str] = _utc_now,
        flush_timeout_s: float = 30.0,
    ) -> None:
        self._producer = producer
        self._registry = registry
        self._producer_name = producer_name
        self._now = now
        self._flush_timeout_s = flush_timeout_s
        self._delivery_errors: list[str] = []
        self.metrics = ValidationMetrics()

    def _on_delivery(self, err: object, msg: object) -> None:
        if err is not None:
            self._delivery_errors.append(str(err))

    def emit(self, event: Event) -> None:
        violations = self._registry.validate(event.event_type, event.payload)
        envelope: dict[str, Any] = {
            "event_id": event.event_id,
            "event_type": event.event_type,
            "schema_version": self._registry.version,
            "producer": self._producer_name,
        }
        if violations:
            topic = DLQ_TOPIC
            envelope["rejected_at"] = self._now()
            envelope["errors"] = [asdict(v) for v in violations]
        else:
            topic = RAW_TOPIC
            envelope["emitted_at"] = self._now()
        envelope["payload"] = event.payload
        self._produce(
            topic,
            key=event.key.encode(),
            value=json.dumps(envelope, separators=(",", ":")).encode(),
            headers={
                "event_type": event.event_type.encode(),
                "schema_version": self._registry.version.encode(),
            },
        )
        self.metrics.record(event.event_type, ok=not violations)
        if violations:
            log.warning("DLQ %s: %s %s", event.event_id, violations[0].path, violations[0].rule)

    def _produce(self, topic: str, **kwargs: Any) -> None:  # noqa: ANN401
        while True:
            try:
                self._producer.produce(topic, on_delivery=self._on_delivery, **kwargs)
                break
            except BufferError:  # local queue full: let it drain, then retry
                self._producer.poll(0.5)
        self._producer.poll(0)  # serve delivery callbacks

    def flush(self) -> None:
        remaining = self._producer.flush(self._flush_timeout_s)
        if remaining or self._delivery_errors:
            errors, self._delivery_errors = self._delivery_errors, []
            raise DeliveryError(f"{remaining} undelivered, errors: {errors[:3]}")


def write_metrics(
    landing_dir: Path,
    run_id: str,
    started_at: str,
    sink: KafkaSink,
    version: str,
    window: tuple[str, str] = ("", ""),
) -> Path:
    """Write this run's rejection-rate metric under the landing dir (uploaded to bronze).

    ``window`` is the crawl's [start, end) data window, so the DLQ check can ask for the
    rejection rate of the run that collected a given day.
    """
    finished_at = _utc_now()
    out = landing_dir / "producer_metrics" / f"run_date={started_at[:10]}" / f"{run_id}.jsonl"
    out.parent.mkdir(parents=True, exist_ok=True)
    summary = sink.metrics.summary()
    with out.open("w", encoding="utf-8") as fh:
        for event_type, counts in summary["by_type"].items():
            row = {
                "run_id": run_id,
                "run_started_at": started_at,
                "run_finished_at": finished_at,
                "window_start": window[0],
                "window_end": window[1],
                "event_type": event_type,
                "schema_version": version,
                **counts,
            }
            fh.write(json.dumps(row) + "\n")
    return out
