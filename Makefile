SHELL := /bin/bash
.DEFAULT_GOAL := help

.PHONY: help setup up down ps logs test lint fmt check-riot collect dlq-demo topics dlq-peek backfill

help: ## List targets
	@grep -E '^[a-z-]+:.*## ' $(MAKEFILE_LIST) | awk 'BEGIN {FS = ":.*## "} {printf "  %-12s %s\n", $$1, $$2}'

setup: ## Install pinned Python deps into .venv (uv)
	uv sync

up: ## Start Redpanda, Console, Airflow and wait until healthy
	docker compose up -d --wait

down: ## Stop services (volumes kept; `docker compose down -v` wipes them)
	docker compose down

ps: ## Show service status
	docker compose ps

logs: ## Tail service logs
	docker compose logs -f --tail=100

test: ## Run pytest
	uv run pytest

lint: ## Ruff lint + format check
	uv run ruff check .
	uv run ruff format --check .

fmt: ## Auto-fix lint and format
	uv run ruff check --fix .
	uv run ruff format .

check-riot: ## One Riot API call: prints HTTP status + rate-limit headers (never the key)
	@test -f .env || { echo ".env missing: cp .env.example .env"; exit 2; }
	@set -a; source ./.env; set +a; \
	curl -sS -o /dev/null -D - -H "X-Riot-Token: $$RIOT_API_KEY" \
	  "https://$${RIOT_PLATFORM}.api.riotgames.com/lol/status/v4/platform-data" \
	  | grep -iE '^(HTTP/|x-app-rate-limit|x-method-rate-limit|x-rate-limit-type|retry-after)'

MAX_PLAYERS ?= 50
MAX_MATCHES ?= 200
SINK ?= kafka
collect: ## Crawl Riot API -> contracts -> Kafka: [START= END=] [MAX_PLAYERS=50] [MAX_MATCHES=200] [SINK=kafka|jsonl]
	@uv run --env-file .env python -m collector crawl \
	  $(if $(START),--start $(START)) $(if $(END),--end $(END)) \
	  --max-players $(MAX_PLAYERS) --max-matches $(MAX_MATCHES) --sink $(SINK)

dlq-demo: ## Send 3 malformed events through the contract check (shows the DLQ path)
	@uv run --env-file .env python -m collector dlq-demo

topics: ## Describe Kafka topics (message counts per partition)
	docker compose exec -T redpanda rpk topic describe lol.match.raw lol.match.dlq -p

dlq-peek: ## Show the latest DLQ messages (reason + schema version)
	docker compose exec -T redpanda rpk topic consume lol.match.dlq -o -3 -n 3 -f '%v\n' \
	  | jq -c '{event_id, event_type, schema_version, rejected_at, errors}'

backfill: ## Backfill a date range: make backfill START=YYYY-MM-DD END=YYYY-MM-DD (Phase 5)
	@if [ -z "$(START)" ] || [ -z "$(END)" ]; then \
	  echo "usage: make backfill START=YYYY-MM-DD END=YYYY-MM-DD"; exit 2; fi
	@echo "make backfill $(START)..$(END): not implemented yet (Phase 5: Airflow backfill)"; exit 1
