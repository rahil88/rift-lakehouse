"""Turn .env values into Airflow Connections + Variables JSON (printed to stdout).

``make airflow-config`` pipes this into ``airflow connections import`` inside the container, so
secrets never appear on a command line or in the DAG code. Airflow stores Connection passwords
encrypted with the Fernet key and masks them in task logs.
"""

from __future__ import annotations

import json
import os
import sys

kind = sys.argv[1] if len(sys.argv) > 1 else "connections"

if kind == "connections":
    conns: dict[str, dict] = {}
    if os.environ.get("RIOT_API_KEY"):
        conns["riot_api"] = {
            "conn_type": "generic",
            "description": "Riot API key (development keys expire every 24h)",
            "password": os.environ["RIOT_API_KEY"],
            "extra": json.dumps(
                {
                    "platform": os.environ.get("RIOT_PLATFORM", "na1"),
                    "region": os.environ.get("RIOT_REGION", "americas"),
                }
            ),
        }
    if os.environ.get("DATABRICKS_HOST") and os.environ.get("DATABRICKS_TOKEN"):
        conns["databricks_default"] = {
            "conn_type": "generic",
            "description": "Databricks Free Edition workspace (PAT + SQL warehouse)",
            "host": os.environ["DATABRICKS_HOST"],
            "password": os.environ["DATABRICKS_TOKEN"],
            "extra": json.dumps(
                {
                    "http_path": os.environ.get("DATABRICKS_HTTP_PATH", ""),
                    "catalog": os.environ.get("DATABRICKS_CATALOG", "workspace"),
                }
            ),
        }
    else:
        print("warning: DATABRICKS_* not set; skipping databricks_default", file=sys.stderr)
    print(json.dumps(conns))
else:
    print(
        json.dumps(
            {
                "rift_max_players": "50",
                "rift_max_matches": "200",
                "rift_tiers": "challenger,grandmaster",
                "rift_freshness_slo_hours": "26",
                "rift_dlq_rate_threshold": "0.01",
            }
        )
    )
