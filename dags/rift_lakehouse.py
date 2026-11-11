"""
### rift_lakehouse

Daily run for one UTC day `[data_interval_start, data_interval_end)`:

```
collect  ─┐
ddragon  ─┴─> consume_and_upload -> copy_into_bronze -> dbt_build -> quality_checks
```

* **collect**: Riot API -> contract validation -> Kafka (raw / DLQ) for the day's matches
* **ddragon**: champion metadata for dim_champion
* **consume_and_upload**: Kafka -> JSONL -> Unity Catalog Volume
* **copy_into_bronze**: idempotent DDL + COPY INTO (skips already-loaded files)
* **dbt_build**: silver MERGE, gold tables, SCD2 snapshot, every test
* **quality_checks**: freshness SLO, volume z-score, drift, DLQ rate -> quality_results

**Idempotent by construction.** Rerunning any task, or backfilling a range, converges to the
same tables: crawl state skips fetched matches, consumer offsets skip landed messages, COPY INTO
skips loaded files, silver MERGEs on natural keys, gold is rebuilt, and quality results MERGE on
(check, date).

**Backfill:** `make backfill START=2026-11-04 END=2026-11-10` (scheduler-managed backfill,
one run at a time). Backfill runs skip the rank snapshot, because league-v4 only knows the
current ladder.

Config: Connections `riot_api` and `databricks_default`, plus `rift_*` Variables. Load them
with `make airflow-config`. No secret appears in this file.
"""

from __future__ import annotations

from datetime import UTC, datetime, timedelta

from airflow.providers.standard.operators.bash import BashOperator
from airflow.sdk import DAG
from airflow.timetables.interval import CronDataIntervalTimetable

PY = "/opt/rift-venv/bin/python"
DBT = "/opt/rift-venv/bin/dbt"
RIFT = "/opt/rift"
DATABRICKS_POOL = "databricks"  # size 1: Free Edition has one 2X-Small SQL warehouse

# Templated at runtime from Connections; Airflow masks the passwords in task logs.
RIOT_ENV = {
    "RIOT_API_KEY": "{{ conn.riot_api.password }}",
    "RIOT_PLATFORM": "{{ conn.riot_api.extra_dejson.platform }}",
    "RIOT_REGION": "{{ conn.riot_api.extra_dejson.region }}",
}
DATABRICKS_ENV = {
    "DATABRICKS_HOST": "{{ conn.databricks_default.host }}",
    "DATABRICKS_TOKEN": "{{ conn.databricks_default.password }}",
    "DATABRICKS_HTTP_PATH": "{{ conn.databricks_default.extra_dejson.http_path }}",
    "DATABRICKS_CATALOG": "{{ conn.databricks_default.extra_dejson.catalog }}",
}

with DAG(
    dag_id="rift_lakehouse",
    description="Riot API -> contracts -> Kafka -> UC Volume -> bronze -> dbt -> quality",
    # Airflow 3's plain "@daily" fires at a point in time with no data interval
    # (create_cron_data_intervals=False). Daily windows need the interval timetable explicitly.
    schedule=CronDataIntervalTimetable("@daily", timezone="UTC"),
    start_date=datetime(2026, 11, 1, tzinfo=UTC),
    catchup=False,
    max_active_runs=1,  # one run at a time: shared Riot rate limit and a single warehouse
    default_args={
        "retries": 2,
        "retry_delay": timedelta(minutes=5),
        "retry_exponential_backoff": True,
        "cwd": RIFT,
        "append_env": True,
    },
    tags=["rift", "lakehouse"],
    doc_md=__doc__,
) as dag:
    collect = BashOperator(
        task_id="collect",
        bash_command=(
            f"{PY} -m collector crawl"
            " --start {{ data_interval_start | ds }} --end {{ data_interval_end | ds }}"
            " --tiers {{ var.value.get('rift_tiers', 'challenger,grandmaster') }}"
            " --max-players {{ var.value.get('rift_max_players', 50) }}"
            " --max-matches {{ var.value.get('rift_max_matches', 200) }}"
            " --snapshot-at {{ data_interval_end | ts }} --max-rank-lag-hours 36"
        ),
        env=RIOT_ENV,
        execution_timeout=timedelta(hours=2),
    )

    ddragon = BashOperator(task_id="ddragon", bash_command=f"{PY} -m collector ddragon")

    consume_and_upload = BashOperator(
        task_id="consume_and_upload",
        bash_command=f"{PY} -m consumer consume && {PY} -m consumer upload",
        env=DATABRICKS_ENV,
    )

    copy_into_bronze = BashOperator(
        task_id="copy_into_bronze",
        bash_command=f"{PY} -m consumer setup && {PY} -m consumer load-bronze",
        env=DATABRICKS_ENV,
        pool=DATABRICKS_POOL,
    )

    dbt_build = BashOperator(
        task_id="dbt_build",
        bash_command=f"{DBT} build --project-dir {RIFT}/dbt --profiles-dir {RIFT}/dbt",
        env=DATABRICKS_ENV,
        pool=DATABRICKS_POOL,
    )

    quality_checks = BashOperator(
        task_id="quality_checks",
        bash_command=(
            f"{PY} -m quality run --date {{{{ data_interval_start | ds }}}}"
            " --freshness-slo-hours {{ var.value.get('rift_freshness_slo_hours', 26) }}"
            " --dlq-rate-threshold {{ var.value.get('rift_dlq_rate_threshold', 0.01) }}"
        ),
        env=DATABRICKS_ENV,
        pool=DATABRICKS_POOL,
    )

    [collect, ddragon] >> consume_and_upload >> copy_into_bronze >> dbt_build >> quality_checks
