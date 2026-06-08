"""On-demand New Relic trace evidence for incident inspection."""

from __future__ import annotations

import json
import urllib.request
from datetime import datetime
from typing import Any

from .db import psql_exec, psql_json, sql_literal, sql_text
from .intelligence import format_time


DEFAULT_NEW_RELIC_GRAPHQL_URL = "https://api.newrelic.com/graphql"


def nrql_time(value: datetime) -> str:
    return value.strftime("%Y-%m-%d %H:%M:%S+0000")


def nrql_string(value: str) -> str:
    return value.replace("\\", "\\\\").replace("'", "\\'")


def newrelic_graphql(api_key: str, graphql_url: str, account_id: int, nrql: str) -> list[dict[str, Any]]:
    payload = {
        "query": """
query($accountId: Int!, $nrql: Nrql!) {
  actor {
    account(id: $accountId) {
      nrql(query: $nrql) {
        results
      }
    }
  }
}
""",
        "variables": {"accountId": account_id, "nrql": nrql},
    }
    request = urllib.request.Request(
        graphql_url,
        data=json.dumps(payload).encode("utf-8"),
        headers={
            "API-Key": api_key,
            "Content-Type": "application/json",
            "Accept": "application/json",
        },
        method="POST",
    )
    with urllib.request.urlopen(request, timeout=30) as response:
        body = json.loads(response.read().decode("utf-8"))
    if body.get("errors"):
        raise RuntimeError(json.dumps(body["errors"], sort_keys=True))
    return (
        body.get("data", {})
        .get("actor", {})
        .get("account", {})
        .get("nrql", {})
        .get("results", [])
    )


def service_newrelic_context(database_url: str, service_id: str) -> dict[str, Any] | None:
    sql = f"""
select row_to_json(s)
from (
  select service_id, newrelic_app_name, newrelic_app_id, newrelic_entity_guid
  from services
  where service_id = '{sql_text(service_id)}'
) s;
"""
    return psql_json(database_url, sql)


def query_trace_evidence(
    account_id: int,
    app_name: str,
    start: datetime,
    end: datetime,
    api_key: str,
    graphql_url: str = DEFAULT_NEW_RELIC_GRAPHQL_URL,
) -> dict[str, Any]:
    escaped_app_name = nrql_string(app_name)
    since = nrql_time(start)
    until = nrql_time(end)
    errors: list[dict[str, str]] = []

    queries = {
        "top_slow_transactions": (
            "SELECT count(*) AS 'sample_count', average(duration) * 1000 AS 'avg_duration_ms', "
            "percentile(duration, 95, 99) FROM Transaction "
            f"WHERE appName = '{escaped_app_name}' "
            f"SINCE '{since}' UNTIL '{until}' FACET name LIMIT 10"
        ),
        "span_category_breakdown": (
            "SELECT count(*) AS 'span_count', average(duration.ms) AS 'avg_duration_ms', "
            "percentile(duration.ms, 95, 99) FROM Span "
            f"WHERE appName = '{escaped_app_name}' "
            f"SINCE '{since}' UNTIL '{until}' FACET category LIMIT 10"
        ),
        "external_hotspots": (
            "SELECT count(*) AS 'span_count', average(duration.ms) AS 'avg_duration_ms', "
            "percentile(duration.ms, 95, 99) FROM Span "
            f"WHERE appName = '{escaped_app_name}' AND category = 'http' "
            f"SINCE '{since}' UNTIL '{until}' FACET http.url, peer.hostname LIMIT 10"
        ),
        "datastore_hotspots": (
            "SELECT count(*) AS 'span_count', average(duration.ms) AS 'avg_duration_ms', "
            "percentile(duration.ms, 95, 99) FROM Span "
            f"WHERE appName = '{escaped_app_name}' AND category = 'datastore' "
            f"SINCE '{since}' UNTIL '{until}' FACET db.system, db.statement LIMIT 10"
        ),
        "trace_samples": (
            "SELECT latest(trace.id) AS 'trace_id', latest(name) AS 'span_name', "
            "latest(category) AS 'category', latest(duration.ms) AS 'duration_ms', "
            "latest(transaction.name) AS 'transaction_name', latest(error.class) AS 'error_class', "
            "latest(http.url) AS 'http_url', latest(peer.hostname) AS 'peer_hostname' FROM Span "
            f"WHERE appName = '{escaped_app_name}' "
            f"SINCE '{since}' UNTIL '{until}' FACET trace.id LIMIT 10"
        ),
    }

    evidence: dict[str, Any] = {
        "status": "collected",
        "account_id": account_id,
        "app_name": app_name,
        "window_start": format_time(start),
        "window_end": format_time(end),
        "queries": queries,
        "top_slow_transactions": [],
        "span_category_breakdown": [],
        "external_hotspots": [],
        "datastore_hotspots": [],
        "trace_samples": [],
        "errors": errors,
    }
    for key, nrql in queries.items():
        try:
            evidence[key] = newrelic_graphql(api_key, graphql_url, account_id, nrql)
        except Exception as exc:  # noqa: BLE001 - trace evidence is best-effort.
            errors.append({"source": "newrelic_trace", "query": key, "error": str(exc)})
    if errors and all(not evidence[key] for key in queries):
        evidence["status"] = "error"
    elif errors:
        evidence["status"] = "partial"
    return evidence


def collect_service_trace_evidence(
    database_url: str,
    service_id: str,
    start: datetime,
    end: datetime,
    api_key: str | None,
    graphql_url: str | None = None,
    account_id: int = 464254,
) -> dict[str, Any]:
    context = service_newrelic_context(database_url, service_id)
    app_name = (context or {}).get("newrelic_app_name")
    if not app_name:
        return {
            "status": "skipped",
            "reason": "newrelic_app_name_missing",
            "service_id": service_id,
            "window_start": format_time(start),
            "window_end": format_time(end),
        }
    if not api_key:
        return {
            "status": "skipped",
            "reason": "NEW_RELIC_API_KEY is not set",
            "service_id": service_id,
            "app_name": app_name,
            "window_start": format_time(start),
            "window_end": format_time(end),
        }
    evidence = query_trace_evidence(
        account_id,
        app_name,
        start,
        end,
        api_key,
        graphql_url or DEFAULT_NEW_RELIC_GRAPHQL_URL,
    )
    evidence["service_id"] = service_id
    persist_trace_evidence(database_url, service_id, start, end, evidence)
    return evidence


def persist_trace_evidence(database_url: str, service_id: str, start: datetime, end: datetime, evidence: dict[str, Any]) -> None:
    errors = evidence.get("errors") or []
    sql = """
insert into incident_trace_evidence (
  service_id, window_start, window_end, newrelic_account_id, newrelic_app_name,
  status, trace_summary, errors
) values (
  {service_id}, {window_start}, {window_end}, {account_id}, {app_name},
  {status}, {trace_summary}::jsonb, {errors}::jsonb
);
""".format(
        service_id=sql_literal(service_id),
        window_start=sql_literal(format_time(start)),
        window_end=sql_literal(format_time(end)),
        account_id=sql_literal(evidence.get("account_id")),
        app_name=sql_literal(evidence.get("app_name")),
        status=sql_literal(evidence.get("status", "unknown")),
        trace_summary=sql_literal(evidence),
        errors=sql_literal(errors),
    )
    psql_exec(database_url, sql)

