# References

Official documentation for each tool, and the specific facts this project relied on.

| Tool | Docs | Used for / verified fact |
|---|---|---|
| Riot API | [Developer portal](https://developer.riotgames.com/docs/portal), [API reference](https://developer.riotgames.com/apis), [LoL docs](https://developer.riotgames.com/docs/lol) | Dev keys expire every 24h; personal limit 20/1s + 100/2min; "rate limits are enforced per region" (confirmed from `X-App-Rate-Limit-Count`: na1 and americas count separately); 429 without `X-Rate-Limit-Type` = service limit |
| Data Dragon | [LoL docs → Data Dragon](https://developer.riotgames.com/docs/lol#data-dragon) | Champion `key` (numeric) joins to match `championId` |
| JSON Schema | [Draft 2020-12](https://json-schema.org/draft/2020-12), [python-jsonschema](https://python-jsonschema.readthedocs.io/) | `format: date-time` is only checked if an extra package is installed, so contracts use a regex |
| Redpanda | [Quickstart](https://docs.redpanda.com/current/get-started/quick-start/), [rpk](https://docs.redpanda.com/current/reference/rpk/) | `kafka_batch_max_bytes` = 1 MiB on self-managed (verified with `rpk cluster config get`) |
| confluent-kafka | [Python client](https://docs.confluent.io/kafka-clients/python/current/overview.html) | Idempotent producer, `acks=all`, zstd; manual offset commit |
| Databricks Free Edition | [Overview](https://docs.databricks.com/aws/en/getting-started/free-edition), [Limitations](https://docs.databricks.com/aws/en/getting-started/free-edition-limitations) | Serverless only, one 2X-Small SQL warehouse, daily compute quota, no commercial use |
| Unity Catalog Volumes | [Volumes](https://docs.databricks.com/aws/en/volumes/) | Landing zone for JSONL files |
| COPY INTO | [COPY INTO](https://docs.databricks.com/aws/en/ingestion/cloud-object-storage/copy-into/) | Idempotent file loads (already-loaded files are skipped) |
| Databricks SDK (Python) | [SDK docs](https://databricks-sdk-py.readthedocs.io/) | Files API upload, Statement Execution API |
| Databricks PATs | [Personal access tokens](https://docs.databricks.com/aws/en/dev-tools/auth/pat) | Auth for SDK / dbt / Airflow (documented as legacy but supported) |
| dbt + dbt-databricks | [dbt docs](https://docs.getdbt.com/), [Databricks setup](https://docs.getdbt.com/docs/core/connect-data-platform/databricks-setup), [Snapshots](https://docs.getdbt.com/docs/build/snapshots) | Incremental MERGE, check-strategy SCD2 snapshot with `hard_deletes` |
| Apache Airflow 3 | [Docker Compose (3.3.2)](https://airflow.apache.org/docs/apache-airflow/3.3.2/howto/docker-compose/index.html), [Supported versions](https://airflow.apache.org/docs/apache-airflow/stable/installation/supported-versions.html), [Backfill](https://airflow.apache.org/docs/apache-airflow/stable/core-concepts/backfill.html) | Airflow 2 EOL 2026-04-22. `create_cron_data_intervals=False` in 3.x: plain `@daily` has no data interval, so the DAG uses `CronDataIntervalTimetable` |
| uv | [uv docs](https://docs.astral.sh/uv/) | Lockfile shared by local env and the Airflow image |
| ruff / pytest | [ruff](https://docs.astral.sh/ruff/), [pytest](https://docs.pytest.org/) | Lint + type-hint presence (ANN rules); tests |
| Streamlit | [Streamlit docs](https://docs.streamlit.io/) | Local dashboard |
| Wilson interval | [Wilson score interval](https://en.wikipedia.org/wiki/Binomial_proportion_confidence_interval#Wilson_score_interval) | Win-rate confidence intervals; macro checked against reference values |
| PSI | [Population stability index](https://www.listendata.com/2015/05/population-stability-index.html) | Drift check thresholds (0.1 / 0.25) |
