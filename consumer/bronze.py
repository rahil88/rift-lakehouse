"""Bronze Delta tables, loaded with COPY INTO from the landing Volume.

COPY INTO is idempotent by design: the table remembers which files it has loaded and skips them
on every rerun, so re-running a load (or a whole backfill) never duplicates rows. Bronze keeps
the raw payload as a JSON string plus ingestion metadata:

* ``_ingested_at``    - when COPY INTO loaded the row
* ``_source_file``    - the Volume file it came from (``_metadata.file_path``)
* ``_schema_version`` - the contract version the producer validated it against
"""

from __future__ import annotations

import logging
from collections.abc import Callable
from dataclasses import dataclass

log = logging.getLogger(__name__)

LANDING_SCHEMA = "rift_landing"
BRONZE_SCHEMA = "rift_bronze"

_EVENT_COLUMNS = """
    event_id          STRING    COMMENT 'Deterministic ID set by the producer, used for dedup',
    event_type        STRING,
    payload           STRING    COMMENT 'Raw event payload, JSON text exactly as produced',
    _schema_version   STRING    COMMENT 'Contract version the payload was validated against',
    _emitted_at       TIMESTAMP COMMENT 'When the producer validated and sent the event',
    _producer         STRING,
    _kafka_topic      STRING,
    _kafka_partition  INT,
    _kafka_offset     BIGINT,
    _kafka_timestamp  TIMESTAMP,
    _source_file      STRING    COMMENT 'Volume file the row was loaded from',
    _ingested_at      TIMESTAMP COMMENT 'When COPY INTO loaded the row'
"""

_EVENT_SELECT = """
    CAST(event_id AS STRING)                              AS event_id,
    CAST(event_type AS STRING)                            AS event_type,
    CAST(payload_json AS STRING)                          AS payload,
    CAST(schema_version AS STRING)                        AS _schema_version,
    CAST(emitted_at AS TIMESTAMP)                         AS _emitted_at,
    CAST(producer AS STRING)                              AS _producer,
    CAST(kafka_topic AS STRING)                           AS _kafka_topic,
    CAST(kafka_partition AS INT)                          AS _kafka_partition,
    CAST(kafka_offset AS BIGINT)                          AS _kafka_offset,
    timestamp_millis(CAST(kafka_timestamp_ms AS BIGINT))  AS _kafka_timestamp,
    _metadata.file_path                                   AS _source_file,
    current_timestamp()                                   AS _ingested_at
"""


@dataclass(frozen=True)
class BronzeTable:
    name: str
    landing_subdir: str
    pattern: str
    columns: str
    select: str
    comment: str


BRONZE_TABLES = (
    BronzeTable(
        "matches",
        "match",
        "ingest_date=*/*.jsonl",
        _EVENT_COLUMNS,
        _EVENT_SELECT,
        "match-v5 matches that passed contract v1 (one row per Kafka message)",
    ),
    BronzeTable(
        "timeline_events",
        "timeline_event",
        "ingest_date=*/*.jsonl",
        _EVENT_COLUMNS,
        _EVENT_SELECT,
        "match-v5 timeline events that passed contract v1",
    ),
    BronzeTable(
        "player_ranks",
        "player_rank",
        "ingest_date=*/*.jsonl",
        _EVENT_COLUMNS,
        _EVENT_SELECT,
        "league-v4 ladder snapshots that passed contract v1",
    ),
    BronzeTable(
        "dlq_events",
        "dlq",
        "ingest_date=*/*.jsonl",
        """
    event_id          STRING,
    event_type        STRING,
    payload           STRING    COMMENT 'Rejected payload, JSON text (kept for replay)',
    errors            STRING    COMMENT 'Contract violations: JSON array of {path, message, rule}',
    _schema_version   STRING,
    _rejected_at      TIMESTAMP,
    _producer         STRING    COMMENT 'dlq-demo rows are injected test events',
    _kafka_topic      STRING,
    _kafka_partition  INT,
    _kafka_offset     BIGINT,
    _kafka_timestamp  TIMESTAMP,
    _source_file      STRING,
    _ingested_at      TIMESTAMP
""",
        """
    CAST(event_id AS STRING)                              AS event_id,
    CAST(event_type AS STRING)                            AS event_type,
    CAST(payload_json AS STRING)                          AS payload,
    CAST(errors_json AS STRING)                           AS errors,
    CAST(schema_version AS STRING)                        AS _schema_version,
    CAST(rejected_at AS TIMESTAMP)                        AS _rejected_at,
    CAST(producer AS STRING)                              AS _producer,
    CAST(kafka_topic AS STRING)                           AS _kafka_topic,
    CAST(kafka_partition AS INT)                          AS _kafka_partition,
    CAST(kafka_offset AS BIGINT)                          AS _kafka_offset,
    timestamp_millis(CAST(kafka_timestamp_ms AS BIGINT))  AS _kafka_timestamp,
    _metadata.file_path                                   AS _source_file,
    current_timestamp()                                   AS _ingested_at
""",
        "Events rejected by the producer's contract check (dead-letter queue)",
    ),
    BronzeTable(
        "producer_metrics",
        "producer_metrics",
        "run_date=*/*.jsonl",
        """
    run_id            STRING,
    run_started_at    TIMESTAMP,
    run_finished_at   TIMESTAMP,
    window_start      DATE      COMMENT 'Crawl window start: the data day this run collected',
    window_end        DATE,
    event_type        STRING,
    schema_version    STRING,
    valid             BIGINT,
    rejected          BIGINT,
    rejection_rate    DOUBLE,
    _source_file      STRING,
    _ingested_at      TIMESTAMP
""",
        """
    CAST(run_id AS STRING)              AS run_id,
    CAST(run_started_at AS TIMESTAMP)   AS run_started_at,
    CAST(run_finished_at AS TIMESTAMP)  AS run_finished_at,
    CAST(window_start AS DATE)          AS window_start,
    CAST(window_end AS DATE)            AS window_end,
    CAST(event_type AS STRING)          AS event_type,
    CAST(schema_version AS STRING)      AS schema_version,
    CAST(valid AS BIGINT)               AS valid,
    CAST(rejected AS BIGINT)            AS rejected,
    CAST(rejection_rate AS DOUBLE)      AS rejection_rate,
    _metadata.file_path                 AS _source_file,
    current_timestamp()                 AS _ingested_at
""",
        "Per-run producer validation counts (the rejection-rate metric)",
    ),
    BronzeTable(
        "ddragon_champions",
        "ddragon_champions",
        "version=*/*.jsonl",
        """
    version           STRING,
    champion_key      INT       COMMENT 'Numeric ID; joins to participant championId',
    champion_id       STRING    COMMENT 'Data Dragon string ID, e.g. MonkeyKing',
    name              STRING,
    title             STRING,
    tags              ARRAY<STRING>,
    _source_file      STRING,
    _ingested_at      TIMESTAMP
""",
        """
    CAST(version AS STRING)        AS version,
    CAST(champion_key AS INT)      AS champion_key,
    CAST(champion_id AS STRING)    AS champion_id,
    CAST(name AS STRING)           AS name,
    CAST(title AS STRING)          AS title,
    CAST(tags AS ARRAY<STRING>)    AS tags,
    _metadata.file_path            AS _source_file,
    current_timestamp()            AS _ingested_at
""",
        "Data Dragon champion metadata per game version",
    ),
)


def setup_statements(catalog: str) -> list[str]:
    """Idempotent DDL: schemas, the landing Volume, and every bronze table."""
    statements = [
        f"CREATE SCHEMA IF NOT EXISTS {catalog}.{LANDING_SCHEMA}",
        f"CREATE VOLUME IF NOT EXISTS {catalog}.{LANDING_SCHEMA}.files "
        "COMMENT 'JSONL landing files written by the consumer'",
        f"CREATE SCHEMA IF NOT EXISTS {catalog}.{BRONZE_SCHEMA}",
    ]
    for table in BRONZE_TABLES:
        statements.append(
            f"CREATE TABLE IF NOT EXISTS {catalog}.{BRONZE_SCHEMA}.{table.name} "
            f"({table.columns}) USING DELTA COMMENT '{table.comment}'"
        )
    return statements


def copy_into_statement(table: BronzeTable, catalog: str, volume_root: str) -> str:
    return f"""
COPY INTO {catalog}.{BRONZE_SCHEMA}.{table.name}
FROM (
  SELECT {table.select}
  FROM '{volume_root}/{table.landing_subdir}'
)
FILEFORMAT = JSON
PATTERN = '{table.pattern}'
COPY_OPTIONS ('mergeSchema' = 'false')
""".strip()


def load_bronze(
    run_sql: Callable[[str], list[dict]],
    has_files: Callable[[str], bool],
    catalog: str,
    volume_root: str,
) -> dict[str, dict]:
    """COPY INTO every bronze table that has landed files. Returns rows inserted per table."""
    results: dict[str, dict] = {}
    for table in BRONZE_TABLES:
        source = f"{volume_root}/{table.landing_subdir}"
        if not has_files(source):  # COPY INTO can't infer a schema from an empty directory
            results[table.name] = {"skipped": "no files"}
            continue
        rows = run_sql(copy_into_statement(table, catalog, volume_root))
        results[table.name] = rows[0] if rows else {}
        log.info("COPY INTO %s: %s", table.name, results[table.name])
    return results
