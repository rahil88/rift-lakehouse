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
- [x] Per-host token buckets driven by X-App/X-Method-Rate-Limit(-Count) headers
- [x] 429: Retry-After, else exponential backoff with jitter; service-limit 429 (no X-Rate-Limit-Type)
- [x] 401/403 fail fast with "regenerate dev key" message
- [x] Crawl: league-v4 Challenger/GM seeds → match IDs → match + timeline; fetched-ID state
- [x] account-v1: extra seeds by Riot ID (`--riot-id Name#TAG`)
- [x] Mocked-HTTP tests incl. 429 path (25 tests)

## Phase 2 — Contracts + Kafka
- [x] contracts/v1: match, timeline_event, player_rank (JSON Schema 2020-12, meta-validated on load)
- [x] Producer validation → lol.match.raw / lol.match.dlq (reason, schema version, ts)
- [x] Timeline exploded into per-event messages (Redpanda 1 MiB batch limit verified via rpk), zstd
- [x] Rejection-rate metric (producer_metrics JSONL in landing/); malformed-payload tests (51 tests)

## Phase 3 — Landing + bronze  (needs: Databricks PAT + SQL warehouse HTTP path in .env)
- [x] Consumer → JSONL landing: micro-batches, commit after fsync, deterministic file names
- [x] Idle timer waits for partition assignment (rebalance) — found in review, tested
- [x] Upload → UC Volume (Files API) + local archive; COPY INTO bronze w/ _ingested_at, _source_file, _schema_version
- [x] Data Dragon champions → landing (real: 173 champions, 16.22.1)
- [x] SDK 0.117.0 signatures verified against installed package
- [x] Real run on Databricks: 143 files / 638 MB uploaded; COPY INTO matches 620, timeline 688,812, ranks 2,000, dlq 3, metrics 6, champions 173 (= landed counts). Rerun: 0 files, 0 rows inserted.

## Phase 4 — dbt silver/gold
- [x] Silver: stg_matches / participants / timeline_events / player_ranks / champions (MERGE on natural keys)
- [x] Gold: fct_participant_match, dim_champion, dim_patch, snap_player_rank (SCD2), Wilson win-rate mart
- [x] 50 dbt tests incl. exactly-10-participants, one-winning-team, CI-contains-win-rate; docs
- [x] `dbt parse` clean (0 warnings); Wilson macro evaluated vs reference values
- [x] Real `dbt build`: PASS=60 WARN=0 ERROR=0 in 57.9 s; rerun PASS=60 in 61.8 s with identical row counts and no new snapshot versions

## Phase 5 — Airflow DAG + backfill
- [x] Custom image: isolated /opt/rift-venv from uv.lock (pipeline + dbt; no streamlit/pytest)
- [x] DAG parses (0 import errors), 6 tasks; CronDataIntervalTimetable (Airflow 3 @daily has no interval)
- [x] Connections (riot_api stored Fernet-encrypted, verified in DB), Variables, pool databricks=1
- [x] Rank snapshot labeled by logical date (rerun-idempotent), skipped on backfills
- [ ] Real task run inside Airflow (collect) — next, with Databricks run
- [ ] **Full DAG run + backfill** (blocked on Databricks)

## Phase 6 — Observability
- [x] Freshness SLO (Delta history), volume z-score (14d), duration PSI drift, DLQ rate → quality_results MERGE
- [x] insufficient_data instead of fake pass; exit 1 on fail = Airflow alert; 12 unit tests
- [ ] **Real run** (blocked on Databricks)

## Phase 7 — Dashboard + README with measured numbers
- [x] Streamlit dashboard (Wilson CI chart sorted by lower bound, health tab); chart tests
- [x] docs/references.md
- [ ] README with Mermaid + measured results (Databricks-side numbers pending)

## Review

### Phase 0 (2026-11-11)
- `make lint` clean, `make test` 3/3 passed.
- `make up`: all 6 services healthy within ~30s (after the first image pull).
- Host ports moved to 18080 (Airflow) / 18081 (Console). 8080 is held by a local Java process,
  and 8081 by the weather-analytics-lab Airflow container.
- Airflow 3.3.2 `/api/v2/monitor/health`: metadatabase, scheduler, and dag_processor healthy.
  The triggerer is not deployed, by design.
- `make check-riot` → HTTP 200, `x-app-rate-limit: 100:120,20:1` (measured, matches the docs).

### Phase 1 (2026-11-11)
- Bug found by the property test: a float-precision spin in `acquire()`. A ~1e-16 s wait
  doesn't advance a clock at t≈1000. Fixed with an explicit EPSILON tolerance.
- Real API: 10 players / 20 matches → 52 calls, 0×429, 54.6 s, 21,457 timeline events.
- Real resume: the rerun skipped 10 windows, refetched 0 matches, made 12 calls, 0×429. It took 68 s
  because first-sync "debt" made the new process wait out the ~50 calls Riot had already
  counted. That's the feature working, not slowness.
- Verified from headers: na1 and americas have separate app-limit counters.

### Phase 2 (2026-11-11)
- Contract vs reality: the real-sample test caught `teamId: 0` on DRAGON_SOUL_GIVEN. Contract
  relaxed for that case only; BUILDING_KILL still requires 100/200. A regression test was added.
- Real run into Redpanda: 20 matches → 22,594 events valid, 0 rejected (0.0% organic),
  52 API calls, 0×429, 55.1 s.
- Raw partition high-watermarks sum to 22,594, matching the producer count. Each key maps to one
  partition (checked over 5,000 messages). Retention: raw 7 days, DLQ 30 days.
- `make dlq-demo`: 3 broken events → DLQ with path/rule/schema_version/rejected_at, tagged
  producer=dlq-demo so they can be excluded from real rejection metrics.

### Phases 3–7 local verification (2026-11-11)
- Bulk crawl 2026-11-04 → 11-11: 600 matches, 667,238 timeline events, 1,257 calls, 0×429,
  1,780 s active, 0 rejections. Laptop slept mid-run; 3 DNS errors on wake were retried
  automatically.
- Two producer-metrics files predated the window field; window_start/end were filled from each
  run's own report (data/reports), not guessed.
- `make land`: 691,435 messages → 143 files (609 MB) in 32 s. That equals Kafka exactly
  (22,594 + 668,838 valid + 3 DLQ demo). The rerun landed 0 (offsets committed). 0 duplicate event_ids.
- Waiting on Databricks: upload, COPY INTO, dbt build, quality, full DAG + backfill, dashboard,
  README results.

### Phase 3 on Databricks (2026-11-11)
- PATs work on Free Edition. Warehouse: Serverless Starter Warehouse, 2X-Small, ~7 s cold start.
- First real run found a bug the unit tests missed: an apostrophe in the dlq_events comment broke
  the DDL. Fixed with a shared sql_string() helper and a regression test.
- COPY INTO for all six tables took ~25 s; bronze counts equal the landed counts exactly.
- Idempotency proven: second `make bronze` uploaded 0 files and inserted 0 rows.

### Phase 4 on Databricks (2026-11-11)
- dbt build #1: 4 incremental + 5 table models, 1 snapshot, 50 tests → PASS=60, 57.9 s.
- Rows: stg_matches 620, stg_participants 6,200 (=10×620), stg_timeline_events 688,812,
  stg_player_ranks 2,000, fct_participant_match 6,200, dim_champion 173, dim_patch 2,
  win-rate mart 611, snap_player_rank 1,000.
- dbt build #2: PASS=60, 61.8 s, row counts identical, snapshot still 1,000 versions (1 distinct
  valid_from): MERGE + table rebuild + check-strategy snapshot are idempotent.
