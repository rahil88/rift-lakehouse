"""CLI: ``python -m collector crawl --start 2026-11-10 --end 2026-11-11``.

Dates are UTC days; ``--start`` is inclusive and ``--end`` exclusive. The default is yesterday
to today. Each run writes a JSON report to ``data/reports/`` so README numbers come from real
runs.
"""

from __future__ import annotations

import argparse
import json
import logging
import sys
from datetime import UTC, date, datetime, time, timedelta

from collector.config import ConfigError, Settings
from collector.crawl import CrawlConfig, run_crawl
from collector.riot_client import TIERS, RiotAuthError, RiotClient
from collector.sinks import JsonlSink
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
    crawl.add_argument("--sink", choices=["jsonl"], default="jsonl")
    return parser


def main(argv: list[str] | None = None) -> int:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s %(message)s")
    logging.getLogger("httpx").setLevel(logging.WARNING)  # don't log every request URL
    args = _parser().parse_args(argv)
    try:
        settings = Settings.from_env()
    except ConfigError as exc:
        print(f"error: {exc}", file=sys.stderr)
        return 2

    cfg = CrawlConfig(
        start=args.start,
        end=args.end,
        tiers=tuple(t.strip().lower() for t in args.tiers.split(",") if t.strip()),
        max_players=args.max_players,
        max_matches=args.max_matches,
        snapshot_ranks=not args.no_ranks,
        extra_riot_ids=tuple(args.riot_id),
    )
    state = CrawlState(settings.data_dir / "state" / "crawl.sqlite")
    sink = JsonlSink(settings.data_dir / "raw")
    client = RiotClient(
        settings.riot_api_key, platform=settings.riot_platform, region=settings.riot_region
    )
    try:
        report = run_crawl(client, state, sink, cfg, platform=settings.riot_platform)
    except RiotAuthError as exc:
        print(f"error: {exc}", file=sys.stderr)
        return 3
    finally:
        sink.close()
        state.close()
        client.close()

    reports = settings.data_dir / "reports"
    reports.mkdir(parents=True, exist_ok=True)
    stamp = datetime.now(UTC).strftime("%Y%m%dT%H%M%SZ")
    out = reports / f"crawl-{stamp}.json"
    out.write_text(json.dumps(report.as_dict(), indent=2))
    print(json.dumps(report.as_dict(), indent=2))
    print(f"report: {out}", file=sys.stderr)
    return 0


if __name__ == "__main__":
    sys.exit(main())
