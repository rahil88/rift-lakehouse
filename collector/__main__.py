"""CLI.

``python -m collector crawl --start 2026-11-10 --end 2026-11-11 [--sink kafka|jsonl]``
    Dates are UTC days; ``--start`` is inclusive and ``--end`` exclusive (default: yesterday ->
    today). Each run writes a JSON report to ``data/reports/`` so README numbers come from real
    runs.

``python -m collector dlq-demo``
    Sends 3 deliberately broken events through the real contract check, to show the DLQ path
    end to end. They are tagged ``producer=dlq-demo`` so they can be excluded from real metrics.
"""

from __future__ import annotations

import argparse
import json
import logging
import sys
from datetime import UTC, date, datetime, time, timedelta

from collector.config import ConfigError, Settings
from collector.contracts import ContractRegistry
from collector.crawl import CrawlConfig, run_crawl
from collector.events import Event
from collector.riot_client import TIERS, RiotAuthError, RiotClient
from collector.sinks import JsonlSink, Sink
from collector.state import CrawlState


def _utc_day(value: str) -> datetime:
    return datetime.combine(date.fromisoformat(value), time.min, tzinfo=UTC)


def _parser() -> argparse.ArgumentParser:
    today = datetime.now(UTC).date()
    parser = argparse.ArgumentParser(prog="python -m collector")
    sub = parser.add_subparsers(dest="command", required=True)
    crawl = sub.add_parser("crawl", help="ladders -> match IDs -> matches + timelines")
    crawl.add_argument("--start", type=_utc_day, default=_utc_day(str(today - timedelta(1))))
    crawl.add_argument("--end", type=_utc_day, default=_utc_day(str(today)))
    crawl.add_argument("--tiers", default="challenger,grandmaster", help=f"subset of {TIERS}")
    crawl.add_argument("--max-players", type=int, default=50)
    crawl.add_argument("--max-matches", type=int, default=200)
    crawl.add_argument("--no-ranks", action="store_true", help="skip the player_rank snapshot")
    crawl.add_argument(
        "--riot-id",
        action="append",
        default=[],
        metavar="NAME#TAG",
        help="extra seed player, resolved to a PUUID via account-v1 (repeatable)",
    )
    crawl.add_argument("--sink", choices=["kafka", "jsonl"], default="kafka")
    sub.add_parser("dlq-demo", help="send 3 malformed events to show the DLQ path")
    return parser


def _kafka_sink(settings: Settings, producer_name: str = "rift-collector"):  # noqa: ANN202
    from confluent_kafka import Producer

    from collector.producer import KafkaSink, ensure_topics, producer_config

    created = ensure_topics(settings.kafka_bootstrap_servers)
    if created:
        logging.getLogger(__name__).info("created topics: %s", created)
    producer = Producer(producer_config(settings.kafka_bootstrap_servers))
    return KafkaSink(producer, ContractRegistry(), producer_name=producer_name)


def _write_report(settings: Settings, name: str, report: dict) -> None:
    reports = settings.data_dir / "reports"
    reports.mkdir(parents=True, exist_ok=True)
    out = reports / f"{name}.json"
    out.write_text(json.dumps(report, indent=2))
    print(json.dumps(report, indent=2))
    print(f"report: {out}", file=sys.stderr)


def cmd_crawl(args: argparse.Namespace, settings: Settings) -> int:
    from collector.producer import KafkaSink, write_metrics

    run_id = "crawl-" + datetime.now(UTC).strftime("%Y%m%dT%H%M%SZ")
    started_at = datetime.now(UTC).isoformat(timespec="seconds")
    cfg = CrawlConfig(
        start=args.start,
        end=args.end,
        tiers=tuple(t.strip().lower() for t in args.tiers.split(",") if t.strip()),
        max_players=args.max_players,
        max_matches=args.max_matches,
        snapshot_ranks=not args.no_ranks,
        extra_riot_ids=tuple(args.riot_id),
    )
    sink: Sink = (
        _kafka_sink(settings) if args.sink == "kafka" else JsonlSink(settings.data_dir / "raw")
    )
    state = CrawlState(settings.data_dir / "state" / "crawl.sqlite")
    client = RiotClient(
        settings.riot_api_key, platform=settings.riot_platform, region=settings.riot_region
    )
    try:
        report = run_crawl(client, state, sink, cfg, platform=settings.riot_platform).as_dict()
    except RiotAuthError as exc:
        print(f"error: {exc}", file=sys.stderr)
        return 3
    finally:
        sink.flush()
        if isinstance(sink, JsonlSink):
            sink.close()
        state.close()
        client.close()

    report["run_id"] = run_id
    if isinstance(sink, KafkaSink):
        report["validation"] = sink.metrics.summary()
        landing = settings.data_dir / "landing"
        report["metrics_file"] = str(write_metrics(landing, run_id, started_at, sink, "v1"))
    _write_report(settings, run_id, report)
    return 0


def cmd_dlq_demo(settings: Settings) -> int:
    sink = _kafka_sink(settings, producer_name="dlq-demo")
    stamp = datetime.now(UTC).strftime("%Y%m%dT%H%M%SZ")
    broken = [
        Event("match", "NA1_0", f"dlq-demo:{stamp}:match", {"metadata": {}, "info": {}}),
        Event(
            "player_rank",
            "demo",
            f"dlq-demo:{stamp}:rank",
            {"puuid": "demo", "tier": "LEGEND", "rank": "I", "leaguePoints": -5},
        ),
        Event(
            "timeline_event",
            "NA1_0",
            f"dlq-demo:{stamp}:tl",
            {
                "matchId": "NA1_0",
                "frameIndex": 0,
                "eventIndex": 0,
                "frameTimestamp": 0,
                "event": {"type": "CHAMPION_KILL", "timestamp": 1},
            },
        ),
    ]
    for event in broken:
        sink.emit(event)
    sink.flush()
    print(json.dumps(sink.metrics.summary(), indent=2))
    return 0


def main(argv: list[str] | None = None) -> int:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s %(message)s")
    logging.getLogger("httpx").setLevel(logging.WARNING)  # don't log every request URL
    args = _parser().parse_args(argv)
    try:
        settings = Settings.from_env()
    except ConfigError as exc:
        print(f"error: {exc}", file=sys.stderr)
        return 2
    if args.command == "dlq-demo":
        return cmd_dlq_demo(settings)
    return cmd_crawl(args, settings)


if __name__ == "__main__":
    sys.exit(main())
