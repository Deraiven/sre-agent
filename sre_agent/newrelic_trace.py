"""On-demand New Relic trace evidence for incident inspection."""

from __future__ import annotations

import json
import urllib.request
from datetime import datetime, timedelta
from typing import Any

from .db import psql_exec, psql_json, sql_literal, sql_text
from .intelligence import BASELINE_VERSION, format_time, utc_now


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


def service_newrelic_contexts(database_url: str, service_ids: list[str] | None = None) -> list[dict[str, Any]]:
    where = ""
    if service_ids:
        where = "where service_id in (" + ",".join(sql_literal(service_id) for service_id in service_ids) + ")"
    sql = f"""
select coalesce(json_agg(row_to_json(s) order by service_id), '[]'::json)
from (
  select service_id, newrelic_app_name, newrelic_app_id, newrelic_entity_guid
  from services
  {where}
) s;
"""
    return psql_json(database_url, sql) or []


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


def _percentile_ms(item: dict[str, Any], percentile_key: str) -> float | None:
    percentiles = item.get("percentile.duration") or {}
    value = percentiles.get(percentile_key)
    if not isinstance(value, (int, float)):
        return None
    return float(value) * 1000


def query_transaction_baselines(
    account_id: int,
    app_name: str,
    start: datetime,
    end: datetime,
    api_key: str,
    graphql_url: str = DEFAULT_NEW_RELIC_GRAPHQL_URL,
    limit: int = 100,
) -> dict[str, Any]:
    escaped_app_name = nrql_string(app_name)
    since = nrql_time(start)
    until = nrql_time(end)
    nrql = (
        "SELECT count(*) AS 'sample_count', average(duration) * 1000 AS 'avg_duration_ms', "
        "percentile(duration, 50, 75, 90, 95, 99) FROM Transaction "
        f"WHERE appName = '{escaped_app_name}' "
        f"SINCE '{since}' UNTIL '{until}' FACET name LIMIT {max(1, min(limit, 500))}"
    )
    rows = newrelic_graphql(api_key, graphql_url, account_id, nrql)
    baselines = []
    for row in rows:
        transaction_name = row.get("name") or row.get("facet")
        if not transaction_name:
            continue
        baselines.append(
            {
                "transaction_name": transaction_name,
                "sample_count": int(row.get("sample_count") or 0),
                "avg_ms": row.get("avg_duration_ms"),
                "p50_ms": _percentile_ms(row, "50"),
                "p75_ms": _percentile_ms(row, "75"),
                "p90_ms": _percentile_ms(row, "90"),
                "p95_ms": _percentile_ms(row, "95"),
                "p99_ms": _percentile_ms(row, "99"),
            }
        )
    return {
        "status": "collected",
        "account_id": account_id,
        "app_name": app_name,
        "window_start": format_time(start),
        "window_end": format_time(end),
        "query": nrql,
        "transactions": baselines,
    }


def build_transaction_baselines(
    database_url: str,
    service_ids: list[str] | None,
    days: int,
    baseline_version: str,
    api_key: str | None,
    graphql_url: str | None = None,
    account_id: int = 464254,
    limit: int = 100,
) -> dict[str, Any]:
    if not api_key:
        return {"status": "failed", "error": "NEW_RELIC_API_KEY is not set"}
    valid_from = utc_now()
    since = valid_from - timedelta(days=days)
    contexts = service_newrelic_contexts(database_url, service_ids)
    selected_ids = [context["service_id"] for context in contexts]
    statements = [
        "begin;",
        "delete from transaction_baselines where baseline_version = {baseline_version}{service_filter};".format(
            baseline_version=sql_literal(baseline_version),
            service_filter=(
                " and service_id in (" + ",".join(sql_literal(service_id) for service_id in selected_ids) + ")"
                if selected_ids
                else ""
            ),
        ),
    ]
    service_results = []
    baseline_rows = 0
    for context in contexts:
        service_id = context["service_id"]
        app_name = context.get("newrelic_app_name")
        if not app_name:
            service_results.append({"service_id": service_id, "status": "skipped", "reason": "newrelic_app_name_missing"})
            continue
        try:
            result = query_transaction_baselines(
                account_id,
                app_name,
                since,
                valid_from,
                api_key,
                graphql_url or DEFAULT_NEW_RELIC_GRAPHQL_URL,
                limit=limit,
            )
        except Exception as exc:  # noqa: BLE001 - report per-service baseline failures.
            service_results.append({"service_id": service_id, "status": "error", "error": str(exc)})
            continue
        transactions = result.get("transactions", [])
        service_results.append({"service_id": service_id, "status": "collected", "transactions": len(transactions)})
        for item in transactions:
            if item.get("sample_count", 0) <= 0:
                continue
            baseline_rows += 1
            statements.append(
                """
insert into transaction_baselines (
  service_id, baseline_version, transaction_name,
  p50_ms, p75_ms, p90_ms, p95_ms, p99_ms, avg_ms, sample_count, valid_from
) values (
  {service_id}, {baseline_version}, {transaction_name},
  {p50_ms}, {p75_ms}, {p90_ms}, {p95_ms}, {p99_ms}, {avg_ms}, {sample_count}, {valid_from}
);
""".format(
                    service_id=sql_literal(service_id),
                    baseline_version=sql_literal(baseline_version),
                    transaction_name=sql_literal(item.get("transaction_name")),
                    p50_ms=sql_literal(item.get("p50_ms")),
                    p75_ms=sql_literal(item.get("p75_ms")),
                    p90_ms=sql_literal(item.get("p90_ms")),
                    p95_ms=sql_literal(item.get("p95_ms")),
                    p99_ms=sql_literal(item.get("p99_ms")),
                    avg_ms=sql_literal(item.get("avg_ms")),
                    sample_count=sql_literal(item.get("sample_count")),
                    valid_from=sql_literal(format_time(valid_from)),
                )
            )
    statements.append("commit;")
    psql_exec(database_url, "\n".join(statements))
    return {
        "status": "succeeded",
        "baseline_version": baseline_version,
        "history_days": days,
        "services": len(contexts),
        "baseline_rows": baseline_rows,
        "valid_from": format_time(valid_from),
        "results": service_results,
    }


def load_transaction_baselines(
    database_url: str,
    service_id: str,
    baseline_version: str = BASELINE_VERSION,
) -> dict[str, dict[str, Any]]:
    sql = f"""
select coalesce(json_object_agg(transaction_name, row_to_json(b)), '{{}}'::json)
from (
  select transaction_name, p50_ms, p75_ms, p90_ms, p95_ms, p99_ms, avg_ms, sample_count
  from transaction_baselines
  where service_id = '{sql_text(service_id)}'
    and baseline_version = '{sql_text(baseline_version)}'
) b;
"""
    return psql_json(database_url, sql) or {}


def enrich_trace_with_transaction_baselines(
    evidence: dict[str, Any],
    transaction_baselines: dict[str, dict[str, Any]],
) -> dict[str, Any]:
    for item in evidence.get("top_slow_transactions", []) or []:
        transaction_name = item.get("name") or item.get("facet")
        baseline = transaction_baselines.get(transaction_name)
        if not baseline:
            item["baseline_status"] = "missing"
            continue
        p95_ms = _percentile_ms(item, "95")
        p99_ms = _percentile_ms(item, "99")
        item["baseline_status"] = "matched"
        item["baseline"] = baseline
        for metric_name, current_value, baseline_key in [
            ("p95", p95_ms, "p95_ms"),
            ("p99", p99_ms, "p99_ms"),
            ("avg", item.get("avg_duration_ms"), "avg_ms"),
        ]:
            baseline_value = baseline.get(baseline_key)
            if isinstance(current_value, (int, float)) and isinstance(baseline_value, (int, float)) and baseline_value > 0:
                item[f"{metric_name}_vs_baseline_pct"] = (float(current_value) / float(baseline_value) - 1) * 100
                item[f"{metric_name}_vs_baseline_ratio"] = float(current_value) / float(baseline_value)
    return evidence


def collect_service_trace_evidence(
    database_url: str,
    service_id: str,
    start: datetime,
    end: datetime,
    api_key: str | None,
    graphql_url: str | None = None,
    account_id: int = 464254,
    baseline_version: str = BASELINE_VERSION,
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
    transaction_baselines = load_transaction_baselines(database_url, service_id, baseline_version)
    evidence["transaction_baseline_version"] = baseline_version
    evidence["transaction_baseline_count"] = len(transaction_baselines)
    enrich_trace_with_transaction_baselines(evidence, transaction_baselines)
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
