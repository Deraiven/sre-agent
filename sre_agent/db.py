"""Small PostgreSQL helpers used by the local SRE agent service."""

from __future__ import annotations

import json
import subprocess
from typing import Any


def sql_literal(value: Any) -> str:
    if value is None:
        return "null"
    if isinstance(value, bool):
        return "true" if value else "false"
    if isinstance(value, (dict, list)):
        return "'" + json.dumps(value, sort_keys=True).replace("'", "''") + "'"
    return "'" + str(value).replace("'", "''") + "'"


def sql_text(value: str) -> str:
    return value.replace("'", "''")


def psql(database_url: str, sql: str) -> str:
    completed = subprocess.run(
        ["psql", database_url, "-qAt", "-v", "ON_ERROR_STOP=1", "-c", sql],
        text=True,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        check=True,
    )
    return completed.stdout.strip()


def psql_json(database_url: str, sql: str) -> Any:
    raw = psql(database_url, sql)
    if not raw:
        return None
    return json.loads(raw)


def psql_exec(database_url: str, sql: str) -> None:
    subprocess.run(
        ["psql", database_url, "-q", "-v", "ON_ERROR_STOP=1"],
        input=sql,
        text=True,
        check=True,
    )

