"""CLI: ``python -m consumer <setup|consume|upload|load-bronze|run>``.

* setup        - idempotent DDL: schemas, landing Volume, bronze tables
* consume      - Kafka -> local JSONL landing files (needs only Redpanda)
* upload       - landing files -> UC Volume, then archive locally
* load-bronze  - COPY INTO every bronze table
* run          - consume + upload + load-bronze
"""

from __future__ import annotations

import argparse
import json
import logging
import os
import sys
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from consumer.bronze import load_bronze, setup_statements
from consumer.databricks import (
    DatabricksConfigError,
    DatabricksSettings,
    SqlRunner,
    workspace_client,
)
from consumer.landing import LandingConsumer, kafka_consumer
from consumer.upload import upload_landing

DATA_DIR = Path(os.environ.get("RIFT_DATA_DIR", "data"))


def _consume() -> dict[str, Any]:
    bootstrap = os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "localhost:19092")
    consumer = kafka_consumer(bootstrap)
    try:
        return LandingConsumer(consumer, DATA_DIR / "landing").run().as_dict()
    finally:
        consumer.close()


def _databricks() -> tuple[DatabricksSettings, Any]:
    settings = DatabricksSettings.from_env()
    return settings, workspace_client(settings)


def _upload(settings: DatabricksSettings, client: Any) -> dict[str, Any]:  # noqa: ANN401
    landing, landed = DATA_DIR / "landing", DATA_DIR / "landed"
    return upload_landing(client.files, landing, landed, settings.volume_root).as_dict()


def _load(settings: DatabricksSettings, client: Any) -> dict[str, Any]:  # noqa: ANN401
    from databricks.sdk.errors import NotFound

    def has_files(path: str) -> bool:
        try:
            return any(True for _ in client.files.list_directory_contents(path))
        except NotFound:
            return False

    runner = SqlRunner(client, settings.warehouse_id)
    return load_bronze(runner.execute, has_files, settings.catalog, settings.volume_root)


def _setup(settings: DatabricksSettings, client: Any) -> dict[str, Any]:  # noqa: ANN401
    runner = SqlRunner(client, settings.warehouse_id)
    statements = setup_statements(settings.catalog)
    for sql in statements:
        runner.execute(sql)
    return {"statements": len(statements), "catalog": settings.catalog}


def main(argv: list[str] | None = None) -> int:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s %(message)s")
    for noisy in ("databricks.sdk", "urllib3", "py4j"):
        logging.getLogger(noisy).setLevel(logging.WARNING)
    parser = argparse.ArgumentParser(prog="python -m consumer")
    parser.add_argument("command", choices=["setup", "consume", "upload", "load-bronze", "run"])
    args = parser.parse_args(argv)

    report: dict[str, Any] = {"command": args.command}
    try:
        if args.command in ("consume", "run"):
            report["consume"] = _consume()
        if args.command != "consume":
            settings, client = _databricks()
            if args.command == "setup":
                report["setup"] = _setup(settings, client)
            if args.command in ("upload", "run"):
                report["upload"] = _upload(settings, client)
            if args.command in ("load-bronze", "run"):
                report["bronze"] = _load(settings, client)
    except DatabricksConfigError as exc:
        print(f"error: {exc}", file=sys.stderr)
        return 2

    reports = DATA_DIR / "reports"
    reports.mkdir(parents=True, exist_ok=True)
    stamp = datetime.now(UTC).strftime("%Y%m%dT%H%M%SZ")
    out = reports / f"consumer-{args.command}-{stamp}.json"
    out.write_text(json.dumps(report, indent=2, default=str))
    print(json.dumps(report, indent=2, default=str))
    return 0


if __name__ == "__main__":
    sys.exit(main())
