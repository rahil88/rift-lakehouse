# Rift Lakehouse — task tracker

Rule: finish a phase → summarize, ≤5 design bullets, verification steps → wait for "next".

## Phase 0 — Scaffold
- [x] Own git repo at ~/Documents/Projects/rift-lakehouse (home dir is itself a git repo)
- [x] .gitignore (.env, keys, data/), .env.example, local .env (chmod 600)
- [x] pyproject (uv, exact pins, ruff + pytest config), uv.lock
- [x] docker-compose: Redpanda v26.2.6, Console v3.13.1, Postgres 16.15, Airflow 3.3.2 (LocalExecutor)
- [x] Makefile: setup, up, down, ps, logs, test, lint, fmt, check-riot, collect (stub), backfill (stub)
- [x] Package stubs, smoke test, README skeleton, contracts/README
- [x] Verify: lint + test green, `make up` healthy, Console + Airflow UI reachable, `make check-riot` 200
- [x] Confirm .env ignored, commit step by step, push to GitHub (private)

## Phase 1 — Rate-limited collector
- [ ] Per-host token buckets driven by X-App/X-Method-Rate-Limit(-Count) headers
- [ ] 429: Retry-After, else exponential backoff with jitter; service-limit 429 (no X-Rate-Limit-Type)
- [ ] 401/403 fail fast with "regenerate dev key" message
- [ ] Crawl: league-v4 Challenger/GM seeds → match IDs → match + timeline; fetched-ID state
- [ ] Mocked-HTTP tests incl. 429 path

## Phase 2 — Contracts + Kafka
- [ ] contracts/v1: match, timeline_event, player_rank
- [ ] Producer validation → lol.match.raw / lol.match.dlq (reason, schema version, ts)
- [ ] Timeline exploded into per-event messages (Redpanda 1 MiB default batch limit), zstd
- [ ] Rejection-rate metric; malformed-payload tests

## Phase 3 — Landing + bronze  (needs: Databricks PAT + SQL warehouse HTTP path in .env)
- [ ] Consumer → JSONL → UC Volume (Databricks SDK)
- [ ] COPY INTO bronze with _ingested_at, _source_file, _schema_version

## Phase 4 — dbt silver/gold
## Phase 5 — Airflow DAG + backfill
## Phase 6 — Observability
## Phase 7 — Dashboard + README with measured numbers

## Review

### Phase 0 (2026-11-11)
- `make lint` clean, `make test` 3/3 passed.
- `make up`: all 6 services healthy within ~30s (after the first image pull).
- Host ports moved to 18080 (Airflow) / 18081 (Console). 8080 is held by a local Java process,
  and 8081 by the weather-analytics-lab Airflow container.
- Airflow 3.3.2 `/api/v2/monitor/health`: metadatabase, scheduler, and dag_processor healthy.
  The triggerer is not deployed, by design.
- `make check-riot` → HTTP 200, `x-app-rate-limit: 100:120,20:1` (measured, matches the docs).
