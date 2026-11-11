import json

import pytest

from consumer.landing import DLQ_TOPIC, RAW_TOPIC, LandingConsumer, landing_record

TS_MS = 1_762_819_200_000  # 2025-11-11T00:00:00Z


class FakeMessage:
    def __init__(self, topic, partition, offset, envelope, ts_ms=TS_MS):
        self._t, self._p, self._o, self._v, self._ts = topic, partition, offset, envelope, ts_ms

    def error(self):
        return None

    def topic(self):
        return self._t

    def partition(self):
        return self._p

    def offset(self):
        return self._o

    def value(self):
        return json.dumps(self._v).encode()

    def timestamp(self):
        return (1, self._ts)


class FakeConsumer:
    """Hands out messages in batches; records commits."""

    def __init__(self, messages, batch=3):
        self.pending = list(messages)
        self.batch = batch
        self.commits = 0

    def consume(self, num_messages, timeout):
        out, self.pending = self.pending[: self.batch], self.pending[self.batch :]
        return out

    def commit(self, *, asynchronous):
        assert asynchronous is False
        self.commits += 1


class FakeClock:
    def __init__(self):
        self.t = 0.0

    def __call__(self):
        self.t += 1.0  # every check moves time forward, so idle detection terminates
        return self.t


def raw(event_type, event_id, payload=None):
    return {
        "event_id": event_id,
        "event_type": event_type,
        "schema_version": "v1",
        "emitted_at": "2026-11-11T00:00:00.000+00:00",
        "producer": "rift-collector",
        "payload": payload or {"k": event_id},
    }


def messages():
    return [
        FakeMessage(RAW_TOPIC, 0, 10, raw("match", "match:NA1_1")),
        FakeMessage(RAW_TOPIC, 0, 11, raw("timeline_event", "t:1")),
        FakeMessage(RAW_TOPIC, 1, 5, raw("timeline_event", "t:2")),
        FakeMessage(RAW_TOPIC, 2, 7, raw("player_rank", "r:1")),
        FakeMessage(
            DLQ_TOPIC,
            0,
            0,
            {
                **raw("match", "bad"),
                "rejected_at": "2026-11-11T00:00:01.000+00:00",
                "errors": [{"path": "$.info", "message": "x", "rule": "required"}],
            },
        ),
    ]


def land(tmp_path, msgs, **kw):
    consumer = FakeConsumer(msgs)
    report = LandingConsumer(consumer, tmp_path, idle_timeout_s=3, clock=FakeClock(), **kw).run()
    return consumer, report


def test_routes_by_event_type_and_names_files_by_offsets(tmp_path):
    consumer, report = land(tmp_path, messages())
    files = sorted(p.relative_to(tmp_path).as_posix() for p in tmp_path.rglob("*.jsonl"))
    assert files == [
        "dlq/ingest_date=2025-11-11/lol.match.dlq-p0-000000000000-000000000000.jsonl",
        "match/ingest_date=2025-11-11/lol.match.raw-p0-000000000010-000000000010.jsonl",
        "player_rank/ingest_date=2025-11-11/lol.match.raw-p2-000000000007-000000000007.jsonl",
        "timeline_event/ingest_date=2025-11-11/lol.match.raw-p0-000000000011-000000000011.jsonl",
        "timeline_event/ingest_date=2025-11-11/lol.match.raw-p1-000000000005-000000000005.jsonl",
    ]
    assert report.messages == 5 and consumer.commits == 1
    assert dict(report.by_table) == {"match": 1, "timeline_event": 2, "player_rank": 1, "dlq": 1}


def test_payload_is_kept_as_raw_json_text(tmp_path):
    payload = {"metadata": {"matchId": "NA1_1"}, "nested": [1, 2, {"a": None}]}
    _, record = landing_record(FakeMessage(RAW_TOPIC, 0, 1, raw("match", "m", payload)))
    assert isinstance(record["payload_json"], str)
    assert json.loads(record["payload_json"]) == payload
    assert record["emitted_at"] == "2026-11-11T00:00:00.000+00:00"
    assert (record["kafka_partition"], record["kafka_offset"]) == (0, 1)


def test_dlq_record_keeps_errors_and_rejected_at():
    msg = messages()[-1]
    table, record = landing_record(msg)
    assert table == "dlq"
    assert json.loads(record["errors_json"])[0]["rule"] == "required"
    assert record["rejected_at"] == "2026-11-11T00:00:01.000+00:00"


def test_reconsuming_same_offsets_rewrites_identical_files(tmp_path):
    """Crash before commit -> Kafka redelivers -> same file names and bytes (idempotent)."""
    land(tmp_path / "a", messages())
    land(tmp_path / "b", messages())
    a = {p.relative_to(tmp_path / "a"): p.read_bytes() for p in (tmp_path / "a").rglob("*.jsonl")}
    b = {p.relative_to(tmp_path / "b"): p.read_bytes() for p in (tmp_path / "b").rglob("*.jsonl")}
    assert a == b


def test_commit_happens_once_per_micro_batch(tmp_path):
    consumer, report = land(tmp_path, messages(), max_batch=3)
    assert report.cycles == 2 and consumer.commits == 2  # 3 + 2 messages
    assert not list(tmp_path.rglob("*.tmp"))  # atomic writes leave no temp files


def test_kafka_error_stops_without_commit(tmp_path):
    class Broken(FakeMessage):
        def error(self):
            return "broker gone"

    consumer = FakeConsumer([Broken(RAW_TOPIC, 0, 1, raw("match", "m"))])
    with pytest.raises(RuntimeError, match="broker gone"):
        LandingConsumer(consumer, tmp_path, clock=FakeClock()).run()
    assert consumer.commits == 0
