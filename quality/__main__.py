"""CLI: ``python -m quality run --date 2026-11-10``.

Exits 1 if any check is ``fail``: that is the alert (the Airflow task goes red). ``warn`` and
``insufficient_data`` are recorded but don't fail the run.
"""

from __future__ import annotations

import argparse
import json
import logging
import sys
from datetime import UTC, date, datetime

from consumer.databricks import (
    DatabricksConfigError,
    DatabricksSettings,
    SqlRunner,
    workspace_client,
)
from quality.checks import FAIL
from quality.runner import evaluate, merge_statement, setup_statements


def main(argv: list[str] | None = None) -> int:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s %(message)s")
    logging.getLogger("databricks.sdk").setLevel(logging.WARNING)
    parser = argparse.ArgumentParser(prog="python -m quality")
    sub = parser.add_subparsers(dest="command", required=True)
    run = sub.add_parser("run", help="evaluate all checks for a data day and record them")
    run.add_argument("--date", type=date.fromisoformat, required=True, help="data day (UTC)")
    run.add_argument("--freshness-slo-hours", type=float, default=26.0)
    run.add_argument("--dlq-rate-threshold", type=float, default=0.01)
    args = parser.parse_args(argv)

    try:
        settings = DatabricksSettings.from_env()
    except DatabricksConfigError as exc:
        print(f"error: {exc}", file=sys.stderr)
        return 2
    runner = SqlRunner(workspace_client(settings), settings.warehouse_id)
    for sql in setup_statements(settings.catalog):
        runner.execute(sql)

    now = datetime.now(UTC)
    results = evaluate(
        runner.execute,
        settings.catalog,
        args.date,
        now=now,
        freshness_slo_hours=args.freshness_slo_hours,
        dlq_rate_threshold=args.dlq_rate_threshold,
    )
    run_id = "quality-" + now.strftime("%Y%m%dT%H%M%SZ")
    runner.execute(merge_statement(settings.catalog, results, args.date, now, run_id))

    for r in results:
        print(
            json.dumps(
                {
                    "check": r.check_name,
                    "status": r.status,
                    "metric": r.metric_value,
                    "threshold": r.threshold,
                    **r.details,
                },
                default=str,
            )
        )
    failed = [r.check_name for r in results if r.status == FAIL]
    if failed:
        print(f"ALERT: failed checks for {args.date}: {', '.join(failed)}", file=sys.stderr)
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())
