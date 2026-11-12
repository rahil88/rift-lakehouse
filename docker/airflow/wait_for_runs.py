"""Block until every run of a DAG has finished (used by ``make backfill`` before re-pausing).

Polls ``airflow dags list-runs`` in the scheduler container. It waits until at least one run
exists (the backfill may take a moment to create them) and none is queued or running. It then
prints a one-line summary per run.
"""

from __future__ import annotations

import json
import subprocess
import sys
import time

DAG_ID = sys.argv[1] if len(sys.argv) > 1 else "rift_lakehouse"
POLL_S = 30
CMD = [
    "docker", "compose", "exec", "-T", "airflow-scheduler",
    "airflow", "dags", "list-runs", DAG_ID, "-o", "json",
]  # fmt: skip


def runs() -> list[dict]:
    out = subprocess.run(CMD, capture_output=True, text=True, check=True).stdout
    return json.loads(out or "[]")


def main() -> int:
    while True:
        current = runs()
        active = [r for r in current if r["state"] in ("queued", "running")]
        if current and not active:
            break
        print(f"{len(active)} run(s) queued/running; checking again in {POLL_S}s", flush=True)
        time.sleep(POLL_S)
    for r in sorted(current, key=lambda r: r["logical_date"]):
        print(r["run_id"], r["state"], r.get("start_date"), "->", r.get("end_date"))
    return 0 if all(r["state"] == "success" for r in current) else 1


if __name__ == "__main__":
    sys.exit(main())
