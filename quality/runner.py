"""Fetch the inputs for each check from Databricks, evaluate, and MERGE into quality_results.

``quality_results`` is keyed by (check_name, target_date): rerunning the checks for a date
overwrites that date's rows, so the table is idempotent like everything upstream.
"""

from __future__ import annotations

import json
from collections.abc import Callable
from datetime import UTC, date, datetime, timedelta
from typing import Any

from consumer.databricks import sql_string
from quality.checks import CheckResult, dlq_rate, duration_drift, freshness, volume_zscore

QUALITY_SCHEMA = "rift_quality"
BUCKETS = 10  # 5-minute game-duration buckets: [0,5), [5,10), ..., [45, inf)

RunSql = Callable[[str], list[dict[str, Any]]]


def setup_statements(catalog: str) -> list[str]:
    return [
        f"CREATE SCHEMA IF NOT EXISTS {catalog}.{QUALITY_SCHEMA}",
        f"""CREATE TABLE IF NOT EXISTS {catalog}.{QUALITY_SCHEMA}.quality_results (
            check_name    STRING,
            target_date   DATE      COMMENT 'Data day the check judged',
            status        STRING    COMMENT 'pass | warn | fail | insufficient_data',
            metric_value  DOUBLE,
            threshold     DOUBLE,
            details       STRING    COMMENT 'JSON with the inputs behind the verdict',
            checked_at    TIMESTAMP,
            run_id        STRING
        ) USING DELTA COMMENT 'One row per check per data day, MERGE on (check_name, target_date)'
        """,
    ]


def _sql_num(value: float | None) -> str:
    return "NULL" if value is None else repr(float(value))


def merge_statement(
    catalog: str, results: list[CheckResult], target: date, checked_at: datetime, run_id: str
) -> str:
    rows = ",\n  ".join(
        "("
        + ", ".join(
            [
                sql_string(r.check_name),
                f"DATE'{target.isoformat()}'",
                sql_string(r.status),
                _sql_num(r.metric_value),
                _sql_num(r.threshold),
                sql_string(json.dumps(r.details, default=str, sort_keys=True)),
                f"TIMESTAMP'{checked_at.strftime('%Y-%m-%d %H:%M:%S')}'",
                sql_string(run_id),
            ]
        )
        + ")"
        for r in results
    )
    return f"""
MERGE INTO {catalog}.{QUALITY_SCHEMA}.quality_results AS t
USING (
  SELECT * FROM VALUES
  {rows}
  AS s(check_name, target_date, status, metric_value, threshold, details, checked_at, run_id)
) AS s
ON t.check_name = s.check_name AND t.target_date = s.target_date
WHEN MATCHED THEN UPDATE SET *
WHEN NOT MATCHED THEN INSERT *
""".strip()


def _parse_ts(value: object) -> datetime | None:
    if value in (None, ""):
        return None
    parsed = datetime.fromisoformat(str(value).replace("Z", "+00:00"))
    return parsed if parsed.tzinfo else parsed.replace(tzinfo=UTC)


def evaluate(
    run_sql: RunSql,
    catalog: str,
    target: date,
    *,
    now: datetime,
    freshness_slo_hours: float,
    dlq_rate_threshold: float,
) -> list[CheckResult]:
    silver, gold = f"{catalog}.rift_silver", f"{catalog}.rift_gold"
    bronze = f"{catalog}.rift_bronze"
    start = (target - timedelta(days=14)).isoformat()
    day = target.isoformat()

    # 1. freshness: Delta table history says when gold was last written
    history = run_sql(f"DESCRIBE HISTORY {gold}.fct_participant_match LIMIT 1")
    last_write = _parse_ts(history[0]["timestamp"]) if history else None
    results = [freshness(last_write, now, freshness_slo_hours)]

    # 2. volume: matches per game day, target vs trailing 14 days
    rows = run_sql(
        f"SELECT to_date(game_created_at) AS d, count(*) AS n FROM {silver}.stg_matches "
        f"WHERE to_date(game_created_at) BETWEEN DATE'{start}' AND DATE'{day}' GROUP BY 1"
    )
    first = run_sql(f"SELECT min(to_date(game_created_at)) AS d FROM {silver}.stg_matches")
    counts = {date.fromisoformat(str(r["d"])): int(r["n"]) for r in rows}
    if first and first[0]["d"] and date.fromisoformat(str(first[0]["d"])) not in counts:
        counts.setdefault(date.fromisoformat(str(first[0]["d"])), 0)
    results.append(volume_zscore(counts, target))

    # 3. drift: game-duration distribution, target day vs trailing 14 days
    rows = run_sql(
        f"SELECT CASE WHEN to_date(game_created_at) = DATE'{day}' THEN 'target' ELSE 'baseline' END"
        f" AS grp, least(floor(game_duration_s / 300), {BUCKETS - 1}) AS bucket, count(*) AS n"
        f" FROM {silver}.stg_matches"
        f" WHERE to_date(game_created_at) BETWEEN DATE'{start}' AND DATE'{day}' GROUP BY 1, 2"
    )
    buckets = {"baseline": [0] * BUCKETS, "target": [0] * BUCKETS}
    for r in rows:
        buckets[r["grp"]][int(float(r["bucket"]))] += int(r["n"])
    results.append(duration_drift(buckets["baseline"], buckets["target"]))

    # 4. DLQ rate: producer runs that collected the target day
    rows = run_sql(
        f"SELECT sum(valid) AS valid, sum(rejected) AS rejected FROM {bronze}.producer_metrics "
        f"WHERE window_start <= DATE'{day}' AND window_end > DATE'{day}'"
    )
    valid = rows[0]["valid"] if rows else None
    rejected = rows[0]["rejected"] if rows else None
    results.append(
        dlq_rate(
            None if valid is None else int(valid),
            None if rejected is None else int(rejected),
            dlq_rate_threshold,
        )
    )
    return results
