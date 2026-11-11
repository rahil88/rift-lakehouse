"""Databricks access: one SDK client for both the Files API (Volumes) and SQL statements.

Credentials come from DATABRICKS_HOST / DATABRICKS_TOKEN (env or ``.env``), never code. The SQL
warehouse ID is parsed from DATABRICKS_HTTP_PATH, the same value dbt uses.
"""

from __future__ import annotations

import logging
import os
import time
from collections.abc import Mapping
from dataclasses import dataclass
from typing import Any

log = logging.getLogger(__name__)


class DatabricksConfigError(RuntimeError):
    pass


class SqlError(RuntimeError):
    pass


@dataclass(frozen=True)
class DatabricksSettings:
    host: str
    token: str
    http_path: str
    catalog: str

    @property
    def warehouse_id(self) -> str:
        return self.http_path.rstrip("/").rsplit("/", 1)[-1]

    @property
    def volume_root(self) -> str:
        return f"/Volumes/{self.catalog}/rift_landing/files"

    @classmethod
    def from_env(cls, env: Mapping[str, str] = os.environ) -> DatabricksSettings:
        required = ("DATABRICKS_HOST", "DATABRICKS_TOKEN", "DATABRICKS_HTTP_PATH")
        missing = [k for k in required if not env.get(k)]
        if missing:
            raise DatabricksConfigError(f"set {', '.join(missing)} in .env (see .env.example)")
        return cls(
            host=env["DATABRICKS_HOST"].rstrip("/"),
            token=env["DATABRICKS_TOKEN"],
            http_path=env["DATABRICKS_HTTP_PATH"],
            catalog=env.get("DATABRICKS_CATALOG", "workspace"),
        )

    def __repr__(self) -> str:  # never print the token
        return f"DatabricksSettings(host={self.host!r}, catalog={self.catalog!r})"


def workspace_client(settings: DatabricksSettings):  # noqa: ANN201
    from databricks.sdk import WorkspaceClient

    return WorkspaceClient(host=settings.host, token=settings.token)


class SqlRunner:
    """Run SQL on the warehouse via the Statement Execution API and wait for the result."""

    def __init__(self, client: Any, warehouse_id: str, *, poll_s: float = 2.0) -> None:  # noqa: ANN401
        self._client = client
        self._warehouse_id = warehouse_id
        self._poll_s = poll_s

    def execute(self, sql: str) -> list[dict[str, Any]]:
        from databricks.sdk.service.sql import ExecuteStatementRequestOnWaitTimeout, StatementState

        api = self._client.statement_execution
        resp = api.execute_statement(
            statement=sql,
            warehouse_id=self._warehouse_id,
            wait_timeout="50s",
            on_wait_timeout=ExecuteStatementRequestOnWaitTimeout.CONTINUE,
        )
        # a stopped serverless warehouse starts on demand; keep polling until it's done
        while resp.status.state in (StatementState.PENDING, StatementState.RUNNING):
            time.sleep(self._poll_s)
            resp = api.get_statement(resp.statement_id)
        if resp.status.state != StatementState.SUCCEEDED:
            error = resp.status.error.message if resp.status.error else resp.status.state
            raise SqlError(f"{error}\n--- statement ---\n{sql[:500]}")
        if not resp.result or not resp.result.data_array:
            return []
        columns = [c.name for c in resp.manifest.schema.columns]
        return [dict(zip(columns, row, strict=True)) for row in resp.result.data_array]
