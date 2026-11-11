import json

import pytest

from collector.contracts import ContractRegistry
from collector.events import Event, match_event
from collector.producer import DLQ_TOPIC, RAW_TOPIC, DeliveryError, KafkaSink, write_metrics
from tests.factories import make_match


class FakeProducer:
    def __init__(self, *, fail_delivery=False, buffer_errors=0):
        self.messages = []
        self.fail_delivery = fail_delivery
        self.buffer_errors = buffer_errors
        self._callbacks = []

    def produce(self, topic, *, key, value, headers, on_delivery):
        if self.buffer_errors:
            self.buffer_errors -= 1
            raise BufferError("queue full")
        self.messages.append(
            {"topic": topic, "key": key, "value": json.loads(value), "headers": headers}
        )
        self._callbacks.append(on_delivery)

    def poll(self, timeout):
        return 0

    def flush(self, timeout):
        for cb in self._callbacks:
            cb("broker down" if self.fail_delivery else None, None)
        self._callbacks.clear()
        return 0


def make_sink(producer):
    return KafkaSink(producer, ContractRegistry(), now=lambda: "2026-11-11T00:00:00.000+00:00")


def test_valid_event_goes_to_raw_with_envelope():
    producer = FakeProducer()
    sink = make_sink(producer)
    sink.emit(match_event(make_match("NA1_7")))
    [msg] = producer.messages
    assert msg["topic"] == RAW_TOPIC
    assert msg["key"] == b"NA1_7"
    assert msg["headers"] == {"event_type": b"match", "schema_version": b"v1"}
    envelope = msg["value"]
    assert envelope["event_id"] == "match:NA1_7"
    assert envelope["schema_version"] == "v1"
    assert envelope["emitted_at"] == "2026-11-11T00:00:00.000+00:00"
    assert envelope["payload"]["metadata"]["matchId"] == "NA1_7"
    assert "errors" not in envelope


def test_invalid_event_goes_to_dlq_with_reason_version_and_timestamp():
    producer = FakeProducer()
    sink = make_sink(producer)
    bad = make_match("NA1_8")
    bad["info"]["gameDuration"] = "long"
    sink.emit(match_event(bad))
    [msg] = producer.messages
    assert msg["topic"] == DLQ_TOPIC
    envelope = msg["value"]
    assert envelope["schema_version"] == "v1"
    assert envelope["rejected_at"] == "2026-11-11T00:00:00.000+00:00"
    assert envelope["errors"][0] == {
        "path": "$.info.gameDuration",
        "message": "'long' is not of type 'integer'",
        "rule": "type",
    }
    assert envelope["payload"] == bad  # full payload kept for replay


def test_rejection_rate_metric():
    sink = make_sink(FakeProducer())
    for i in range(3):
        sink.emit(match_event(make_match(f"NA1_{i + 1}")))
    sink.emit(Event("player_rank", "p", "r", {"tier": "LEGEND"}))
    summary = sink.metrics.summary()
    assert summary["valid"] == 3 and summary["rejected"] == 1
    assert summary["rejection_rate"] == 0.25
    assert summary["by_type"]["player_rank"]["rejection_rate"] == 1.0


def test_flush_raises_on_delivery_failure():
    sink = make_sink(FakeProducer(fail_delivery=True))
    sink.emit(match_event(make_match("NA1_9")))
    with pytest.raises(DeliveryError):
        sink.flush()


def test_full_local_queue_is_retried():
    producer = FakeProducer(buffer_errors=2)
    make_sink(producer).emit(match_event(make_match("NA1_10")))
    assert len(producer.messages) == 1


def test_write_metrics_lands_one_row_per_event_type(tmp_path):
    sink = make_sink(FakeProducer())
    sink.emit(match_event(make_match("NA1_11")))
    sink.emit(Event("player_rank", "p", "r", {}))
    window = ("2026-11-10", "2026-11-11")
    out = write_metrics(tmp_path, "crawl-x", "2026-11-11T00:00:00+00:00", sink, "v1", window)
    assert out == tmp_path / "producer_metrics" / "run_date=2026-11-11" / "crawl-x.jsonl"
    rows = [json.loads(line) for line in out.read_text().splitlines()]
    assert {(r["window_start"], r["window_end"]) for r in rows} == {window}
    rates = {r["event_type"]: r["rejection_rate"] for r in rows}
    assert rates == {"match": 0.0, "player_rank": 1.0}
