# Rift Lakehouse

An end-to-end League of Legends match data lakehouse built on Riot's own stack: Python, Kafka
(Redpanda), Databricks (Delta Lake, Unity Catalog, medallion architecture), dbt, Airflow, and data
quality observability.

**The core idea is a data contract enforced at the producer.** Every event is validated against a
versioned JSON Schema *before* it enters the pipeline, and invalid events go to a dead-letter queue
along with the reason they failed. Riot described a problem in their re:Invent 2022 talk where
validation happened too late; this design moves it to the producer.

> Status: **Phase 0 (scaffold)**. Sections marked _TBD_ get filled in as each phase lands.

## Architecture

_TBD (Phase 7): Mermaid diagram._ Planned flow:

```
Riot API -> collector (rate-limited) -> JSON Schema validation -> Redpanda
   lol.match.raw (valid) / lol.match.dlq (invalid + reason)
-> consumer (JSONL batches) -> Unity Catalog Volume -> COPY INTO bronze (Delta)
-> dbt silver -> dbt gold -> quality checks
   (orchestrated end to end by Airflow, with backfill)
```

## Stack

| Layer | Tool | Version |
|---|---|---|
| Language | Python | 3.12 |
| Packaging / lint / test | uv, ruff, pytest | see `pyproject.toml` |
| Streaming | Redpanda (Kafka API) | v26.2.6 |
| Orchestration | Apache Airflow (LocalExecutor) | 3.3.2 |
| Warehouse | Databricks Free Edition (Delta Lake, Unity Catalog) | serverless |
| Transformations | dbt-databricks | _TBD (Phase 4)_ |

## Setup

Prereqs: Docker Desktop (≥4 GB RAM for containers), [uv](https://docs.astral.sh/uv/), Python 3.12, a
[Riot developer key](https://developer.riotgames.com), and a
[Databricks Free Edition](https://docs.databricks.com/aws/en/getting-started/free-edition) workspace.

```bash
cp .env.example .env      # fill in RIOT_API_KEY and generate the two Airflow secrets
make setup                # uv sync (pinned deps)
make lint test            # ruff + pytest
make up                   # Redpanda, Console, Airflow; waits until healthy
make check-riot           # one Riot call: shows HTTP status + live rate-limit headers
```

| Service | URL |
|---|---|
| Airflow UI | http://localhost:18080 (local-only login `airflow` / `airflow`) |
| Redpanda Console | http://localhost:18081 |
| Kafka bootstrap | `localhost:19092` |

Run `make help` to list every target.

## Repo layout

| Path | Purpose |
|---|---|
| `collector/` | Rate-limited Riot API client, resumable crawl, contract validation, Kafka producer |
| `contracts/` | Versioned JSON Schemas (the data contract) |
| `consumer/` | Kafka → JSONL batches → Unity Catalog Volume |
| `dags/` | Airflow DAGs |
| `dbt/` | dbt project: silver + gold models, snapshots, tests, docs |
| `quality/` | Freshness, volume, drift, and DLQ checks |
| `tests/` | pytest suite |
| `docs/` | Design notes |

## Design decisions and tradeoffs

_TBD: added phase by phase._

## Results (measured)

_No numbers yet._ This table is filled only with values measured from real runs, never estimates.

| Metric | Value | How measured |
|---|---|---|
| Matches ingested | | |
| Timeline events | | |
| DLQ rejection % | | |
| Pipeline runtime | | |
| Data freshness | | |

## Limits and next steps

- Riot **development keys expire every 24h**, so scheduled runs need the key refreshed daily.
- Databricks Free Edition: serverless only, **one 2X-Small SQL warehouse**, and an unpublished
  daily compute quota (exceeding it shuts compute off for the rest of the day). Data volume is
  kept modest on purpose.
- _More TBD._
