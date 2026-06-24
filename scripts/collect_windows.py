#!/usr/bin/env python3
"""Collect metric windows and write them into service_metric_windows.

MVP scope:
- Reads data/prep/collection_plan.jsonl.
- Collects Prometheus resource metrics with Prometheus HTTP API.
- Writes one row per service/window into PostgreSQL.
- Preserves New Relic, Kubernetes, and GitHub placeholders in the row so the
  table shape is ready for the next collectors.
"""

from __future__ import annotations

import argparse
import json
import os
import subprocess
import sys
import tempfile
import time
import urllib.parse
import urllib.request
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any


DEFAULT_DATABASE_URL = "postgresql://sre_agent:sre_agent@localhost:5432/sre_agent"
DEFAULT_PROMETHEUS_URL = "https://prometheus.pro.mymyhub.com"
DEFAULT_NEW_RELIC_GRAPHQL_URL = "https://api.newrelic.com/graphql"

WINDOW_SECONDS = {
    "5m": 5 * 60,
    "15m": 15 * 60,
    "1h": 60 * 60,
    "1d": 24 * 60 * 60,
}

PROMETHEUS_QUERIES = {
    "cpu_usage": 'sum(rate(container_cpu_usage_seconds_total{{namespace="{namespace}", pod=~"{pod_regex}"}}[5m]))',
    "memory_usage": 'sum(container_memory_working_set_bytes{{namespace="{namespace}", pod=~"{pod_regex}"}})',
    "cpu_throttling": 'sum(rate(container_cpu_cfs_throttled_seconds_total{{namespace="{namespace}", pod=~"{pod_regex}"}}[5m]))',
    "network_receive": 'sum(rate(container_network_receive_bytes_total{{namespace="{namespace}", pod=~"{pod_regex}"}}[5m]))',
    "network_transmit": 'sum(rate(container_network_transmit_bytes_total{{namespace="{namespace}", pod=~"{pod_regex}"}}[5m]))',
}


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--plan", default="data/prep/collection_plan.jsonl")
    parser.add_argument("--database-url", default=os.environ.get("DATABASE_URL", DEFAULT_DATABASE_URL))
    parser.add_argument("--prometheus-url", default=os.environ.get("PROMETHEUS_URL", DEFAULT_PROMETHEUS_URL))
    parser.add_argument("--newrelic-api-key", default=os.environ.get("NEW_RELIC_API_KEY"))
    parser.add_argument("--newrelic-graphql-url", default=os.environ.get("NEW_RELIC_GRAPHQL_URL", DEFAULT_NEW_RELIC_GRAPHQL_URL))
    parser.add_argument("--service", action="append", help="Collect only this service_id. Can be repeated.")
    parser.add_argument("--start", help="UTC start time. Defaults to plan start.")
    parser.add_argument("--end", help="UTC end time. Defaults to plan end.")
    parser.add_argument("--window-size", choices=sorted(WINDOW_SECONDS), help="Override plan window size.")
    parser.add_argument("--max-windows", type=int, help="Stop after N service/window rows.")
    parser.add_argument("--prometheus-step", default="60s")
    parser.add_argument("--batch-size", type=int, default=100, help="Write this many rows per PostgreSQL batch.")
    parser.add_argument("--sleep-seconds-between-batches", type=float, default=0.0)
    parser.add_argument("--dry-run", action="store_true", help="Print rows without writing to PostgreSQL.")
    parser.add_argument("--skip-newrelic", action="store_true")
    parser.add_argument("--skip-prometheus", action="store_true")
    parser.add_argument("--skip-kubernetes", action="store_true")
    parser.add_argument("--skip-kubernetes-events", action="store_true")
    parser.add_argument("--kubectl-context", default=os.environ.get("KUBECTL_CONTEXT"))
    parser.add_argument("--kubectl-aws-profile", default=os.environ.get("KUBECTL_AWS_PROFILE"))
    parser.add_argument("--kubectl-proxy-url", default=os.environ.get("KUBECTL_PROXY_URL"))
    parser.add_argument(
        "--kubectl-timeout-seconds",
        type=int,
        default=int(os.environ.get("KUBECTL_TIMEOUT_SECONDS", "15")),
        help="Maximum seconds to wait for each kubectl command.",
    )
    parser.add_argument("--victorialogs-url", default=os.environ.get("VICTORIALOGS_URL"))
    parser.add_argument("--victorialogs-tenant", default=os.environ.get("VICTORIALOGS_TENANT"))
    parser.add_argument(
        "--kubernetes-events-provider",
        choices=("auto", "kubectl", "victorialogs", "none"),
        default=os.environ.get("SRE_AGENT_KUBERNETES_EVENTS_PROVIDER", "auto").lower(),
        help="Source for Kubernetes events. auto prefers VictoriaLogs when URL and query template are configured.",
    )
    parser.add_argument(
        "--victorialogs-kubernetes-events-query-template",
        default=os.environ.get("VICTORIALOGS_KUBERNETES_EVENTS_QUERY_TEMPLATE"),
        help="LogsQL template for Kubernetes events. Supports {namespace}, {workload_name}, {selector}, {pod_names}, {start}, {end}.",
    )
    parser.add_argument("--skip-github", action="store_true")
    parser.add_argument("--github-commit-detail-limit", type=int, default=5)
    return parser.parse_args()


def parse_time(value: str) -> datetime:
    parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=timezone.utc)
    return parsed.astimezone(timezone.utc)


def format_time(value: datetime) -> str:
    return value.astimezone(timezone.utc).isoformat().replace("+00:00", "Z")


def iter_windows(start: datetime, end: datetime, window_size: str):
    step = timedelta(seconds=WINDOW_SECONDS[window_size])
    cursor = start
    while cursor < end:
        window_end = min(cursor + step, end)
        yield cursor, window_end
        cursor = window_end


def load_plan(path: str, service_filter: set[str] | None) -> list[dict]:
    rows = []
    for line in Path(path).read_text(encoding="utf-8").splitlines():
        if not line.strip():
            continue
        row = json.loads(line)
        if service_filter and row["service_id"] not in service_filter:
            continue
        rows.append(row)
    return rows


def prometheus_query_range(base_url: str, query: str, start: datetime, end: datetime, step: str) -> dict:
    endpoint = base_url.rstrip("/") + "/api/v1/query_range"
    params = urllib.parse.urlencode(
        {
            "query": query,
            "start": start.timestamp(),
            "end": end.timestamp(),
            "step": step,
        }
    )
    req = urllib.request.Request(endpoint + "?" + params, headers={"Accept": "application/json"})
    with urllib.request.urlopen(req, timeout=30) as response:
        payload = json.loads(response.read().decode("utf-8"))
    if payload.get("status") != "success":
        raise RuntimeError(f"Prometheus query failed: {payload}")
    return payload


def values_from_prometheus(payload: dict) -> list[float]:
    result = payload.get("data", {}).get("result", [])
    values: list[float] = []
    for series in result:
        for _, raw_value in series.get("values", []):
            try:
                values.append(float(raw_value))
            except (TypeError, ValueError):
                continue
    return values


def summarize_values(values: list[float]) -> dict:
    if not values:
        return {
            "sample_count": 0,
            "avg": None,
            "min": None,
            "max": None,
            "last": None,
        }
    return {
        "sample_count": len(values),
        "avg": sum(values) / len(values),
        "min": min(values),
        "max": max(values),
        "last": values[-1],
    }


def collect_prometheus(plan: dict, start: datetime, end: datetime, args: argparse.Namespace) -> tuple[dict, list[dict]]:
    source = plan["sources"]["prometheus_resources"]
    namespace = source["namespace"]
    pod_regex = source["pod_regex"]
    collected: dict[str, Any] = {}
    errors: list[dict] = []
    if args.skip_prometheus:
        return {"status": "skipped"}, []
    if source.get("status") == "missing" or not pod_regex:
        return {"status": "missing", "reason": "k8s_mapping_missing", "namespace": namespace, "pod_regex": pod_regex}, []

    for signal in source.get("signals", []):
        template = PROMETHEUS_QUERIES.get(signal)
        if not template:
            errors.append({"source": "prometheus", "signal": signal, "error": "unsupported_signal"})
            continue
        query = template.format(namespace=namespace, pod_regex=pod_regex)
        try:
            payload = prometheus_query_range(args.prometheus_url, query, start, end, args.prometheus_step)
            values = values_from_prometheus(payload)
            collected[signal] = {
                "query": query,
                **summarize_values(values),
            }
        except Exception as exc:  # noqa: BLE001 - persist collection error as data quality.
            collected[signal] = {
                "query": query,
                "sample_count": 0,
                "avg": None,
                "min": None,
                "max": None,
                "last": None,
            }
            errors.append({"source": "prometheus", "signal": signal, "error": str(exc)})
    return collected, errors


def newrelic_time(value: datetime) -> str:
    return value.strftime("%Y-%m-%d %H:%M:%S+0000")


def newrelic_graphql(api_key: str, graphql_url: str, account_id: int, nrql: str) -> dict:
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
        "variables": {
            "accountId": account_id,
            "nrql": nrql,
        },
    }
    req = urllib.request.Request(
        graphql_url,
        data=json.dumps(payload).encode("utf-8"),
        headers={
            "API-Key": api_key,
            "Content-Type": "application/json",
            "Accept": "application/json",
        },
        method="POST",
    )
    with urllib.request.urlopen(req, timeout=30) as response:
        body = json.loads(response.read().decode("utf-8"))
    if body.get("errors"):
        raise RuntimeError(json.dumps(body["errors"], sort_keys=True))
    return body


def first_nrql_result(payload: dict) -> dict:
    return (
        payload.get("data", {})
        .get("actor", {})
        .get("account", {})
        .get("nrql", {})
        .get("results", [{}])[0]
    )


def collect_newrelic(plan: dict, start: datetime, end: datetime, args: argparse.Namespace) -> tuple[dict, list[dict]]:
    source = plan["sources"]["newrelic"]
    base = {
        "account_id": source.get("account_id"),
        "app_id": source.get("app_id"),
        "app_name": source.get("app_name"),
        "entity_guid": source.get("entity_guid"),
        "signals": source.get("signals", []),
    }
    if args.skip_newrelic:
        return {"status": "skipped", **base}, []
    if not args.newrelic_api_key:
        return {"status": "error", **base}, [
            {"source": "newrelic", "signal": "golden_signals", "error": "NEW_RELIC_API_KEY is not set"}
        ]

    app_name = source.get("app_name")
    escaped_app_name = app_name.replace("'", "\\'") if app_name else ""
    account_id = int(source.get("account_id") or 0)
    nrql = (
        "SELECT count(*) AS 'request_count', "
        "rate(count(*), 1 minute) AS 'rpm', "
        "percentage(count(*), WHERE error IS true) AS 'error_rate', "
        "percentile(duration, 95, 99) "
        "FROM Transaction "
        f"WHERE appName = '{escaped_app_name}' "
        f"SINCE '{newrelic_time(start)}' UNTIL '{newrelic_time(end)}'"
    )
    try:
        payload = newrelic_graphql(args.newrelic_api_key, args.newrelic_graphql_url, account_id, nrql)
        result = first_nrql_result(payload)
        percentiles = result.get("percentile.duration") or {}
        p95_seconds = percentiles.get("95")
        p99_seconds = percentiles.get("99")
        return {
            "status": "collected",
            **base,
            "query": nrql,
            "request_count": result.get("request_count"),
            "rpm": result.get("rpm"),
            "error_rate_percent": result.get("error_rate"),
            "latency_p95_ms": p95_seconds * 1000 if isinstance(p95_seconds, (int, float)) else None,
            "latency_p99_ms": p99_seconds * 1000 if isinstance(p99_seconds, (int, float)) else None,
            "raw_result": result,
        }, []
    except Exception as exc:  # noqa: BLE001 - persist collection error as data quality.
        return {"status": "error", **base, "query": nrql}, [
            {"source": "newrelic", "signal": "golden_signals", "error": str(exc)}
        ]


def placeholder_newrelic(plan: dict) -> dict:
    source = plan["sources"]["newrelic"]
    return {
        "status": "not_collected",
        "account_id": source.get("account_id"),
        "app_id": source.get("app_id"),
        "entity_guid": source.get("entity_guid"),
        "signals": source.get("signals", []),
    }


def placeholder_kubernetes(plan: dict) -> dict:
    source = plan["sources"]["kubernetes"]
    return {
        "status": "not_collected",
        "cluster": source.get("cluster"),
        "namespace": source.get("namespace"),
        "workload_kind": source.get("workload_kind"),
        "workload_name": source.get("workload_name"),
        "selector_discovery": source.get("selector_discovery"),
        "signals": source.get("signals", []),
    }


def resolve_kubectl_context(cluster: str | None, explicit_context: str | None) -> str | None:
    if explicit_context:
        return explicit_context
    if not cluster:
        return None
    try:
        completed = subprocess.run(
            ["kubectl", "config", "get-contexts", "-o", "name"],
            check=True,
            text=True,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            timeout=10,
        )
        contexts = [line.strip() for line in completed.stdout.splitlines() if line.strip()]
    except Exception:
        return cluster
    for context in contexts:
        if context == cluster or context.endswith(f"/cluster/{cluster}") or context.endswith(f":cluster/{cluster}"):
            return context
    return cluster


def kubeconfig_with_aws_profile(context: str | None, aws_profile: str | None, timeout_seconds: int) -> str | None:
    if not context or not aws_profile:
        return None
    completed = subprocess.run(
        ["kubectl", "config", "view", "--raw", "-o", "json"],
        check=True,
        text=True,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        timeout=timeout_seconds,
    )
    config = json.loads(completed.stdout)
    context_entry = next((item for item in config.get("contexts", []) if item.get("name") == context), None)
    user_name = context_entry.get("context", {}).get("user") if context_entry else None
    for user in config.get("users", []):
        if user_name and user.get("name") != user_name:
            continue
        exec_config = user.get("user", {}).get("exec")
        if not exec_config:
            continue
        env = exec_config.setdefault("env", [])
        for item in env:
            if item.get("name") == "AWS_PROFILE":
                item["value"] = aws_profile
                break
        else:
            env.append({"name": "AWS_PROFILE", "value": aws_profile})
    handle = tempfile.NamedTemporaryFile("w", prefix="sre-agent-kubeconfig-", suffix=".json", delete=False)
    with handle:
        json.dump(config, handle)
    return handle.name


def kubectl_json(
    args: list[str],
    context: str | None = None,
    kubeconfig: str | None = None,
    proxy_url: str | None = None,
    timeout_seconds: int = 15,
) -> dict:
    command = ["kubectl"]
    if context:
        command.extend(["--context", context])
    command.extend(args)
    env = os.environ.copy()
    if kubeconfig:
        env["KUBECONFIG"] = kubeconfig
    if proxy_url:
        env["HTTPS_PROXY"] = proxy_url
        env["HTTP_PROXY"] = proxy_url
    completed = subprocess.run(
        command,
        text=True,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        env=env,
        timeout=timeout_seconds,
    )
    if completed.returncode != 0:
        raise RuntimeError(
            json.dumps(
                {
                    "command": command,
                    "returncode": completed.returncode,
                    "stderr": completed.stderr.strip(),
                },
                sort_keys=True,
            )
        )
    return json.loads(completed.stdout)


def selector_from_match_labels(match_labels: dict[str, str] | None) -> str:
    if not match_labels:
        return ""
    return ",".join(f"{key}={value}" for key, value in sorted(match_labels.items()))


def parse_kubernetes_time(value: str | None) -> datetime | None:
    if not value:
        return None
    try:
        return parse_time(value)
    except ValueError:
        return None


def event_timestamp(event: dict) -> datetime | None:
    for key in ("eventTime", "lastTimestamp", "firstTimestamp"):
        parsed = parse_kubernetes_time(event.get(key))
        if parsed:
            return parsed
    return None


def first_present(payload: dict, keys: tuple[str, ...]) -> Any:
    for key in keys:
        if key in payload and payload[key] not in (None, ""):
            return payload[key]
    return None


def victorialogs_query(base_url: str, query: str, start: datetime, end: datetime, tenant: str | None, limit: int) -> list[dict]:
    endpoint = base_url.rstrip("/") + "/select/logsql/query"
    params = {
        "query": query,
        "start": format_time(start),
        "end": format_time(end),
        "limit": str(limit),
    }
    if tenant:
        params["tenant"] = tenant
    request = urllib.request.Request(endpoint + "?" + urllib.parse.urlencode(params))
    with urllib.request.urlopen(request, timeout=20) as response:
        body = response.read().decode("utf-8")

    rows: list[dict] = []
    stripped = body.strip()
    if not stripped:
        return rows
    try:
        parsed = json.loads(stripped)
        if isinstance(parsed, list):
            return [item for item in parsed if isinstance(item, dict)]
        if isinstance(parsed, dict):
            if isinstance(parsed.get("data"), list):
                return [item for item in parsed["data"] if isinstance(item, dict)]
            return [parsed]
    except json.JSONDecodeError:
        pass

    for line in stripped.splitlines():
        try:
            item = json.loads(line)
        except json.JSONDecodeError:
            continue
        if isinstance(item, dict):
            rows.append(item)
    return rows


def normalize_victorialogs_kubernetes_event(row: dict) -> dict:
    ts = first_present(row, ("_time", "time", "timestamp", "eventTime", "lastTimestamp", "firstTimestamp"))
    parsed_ts = parse_kubernetes_time(str(ts)) if ts else None
    return {
        "timestamp": format_time(parsed_ts) if parsed_ts else None,
        "reason": first_present(row, ("reason", "event_reason", "kubernetes_reason", "k8s_event_reason")),
        "type": first_present(row, ("type", "level", "event_type", "kubernetes_type", "k8s_event_type")),
        "message": first_present(row, ("message", "_msg", "msg", "log", "event_message")),
        "involved_kind": first_present(row, ("involved_kind", "involvedObject.kind", "object_kind", "kind")),
        "involved_name": first_present(row, ("involved_name", "involvedObject.name", "object_name", "name", "pod")),
        "count": first_present(row, ("count", "event_count")),
        "source": "victorialogs",
    }


def build_victorialogs_event_query(template: str, namespace: str, workload_name: str, selector: str, pod_names: set[str], start: datetime, end: datetime) -> str:
    replacements = {
        "{namespace}": namespace,
        "{workload_name}": workload_name,
        "{selector}": selector,
        "{pod_names}": ",".join(sorted(pod_names)),
        "{start}": format_time(start),
        "{end}": format_time(end),
    }
    query = template
    for token, value in replacements.items():
        query = query.replace(token, value)
    return query


def collect_victorialogs_kubernetes_events(
    namespace: str,
    workload_name: str,
    selector: str,
    pod_names: set[str],
    start: datetime,
    end: datetime,
    args: argparse.Namespace,
) -> list[dict]:
    if not args.victorialogs_url or not args.victorialogs_kubernetes_events_query_template:
        return []
    query = build_victorialogs_event_query(
        args.victorialogs_kubernetes_events_query_template,
        namespace,
        workload_name,
        selector,
        pod_names,
        start,
        end,
    )
    rows = victorialogs_query(args.victorialogs_url, query, start, end, args.victorialogs_tenant, limit=200)
    events = []
    for row in rows:
        event = normalize_victorialogs_kubernetes_event(row)
        involved_name = event.get("involved_name")
        if involved_name and involved_name not in pod_names and involved_name != workload_name:
            continue
        events.append(event)
    return events


def container_state_summary(container_status: dict) -> dict:
    last_state = container_status.get("lastState", {})
    state = container_status.get("state", {})
    terminated = last_state.get("terminated") or state.get("terminated") or {}
    waiting = state.get("waiting") or {}
    return {
        "name": container_status.get("name"),
        "ready": container_status.get("ready"),
        "restart_count": container_status.get("restartCount", 0),
        "state": next(iter(state.keys()), None) if state else None,
        "waiting_reason": waiting.get("reason"),
        "last_terminated_reason": terminated.get("reason"),
        "last_terminated_exit_code": terminated.get("exitCode"),
    }


def collect_kubernetes(plan: dict, start: datetime, end: datetime, args: argparse.Namespace) -> tuple[dict, list[dict]]:
    source = plan["sources"]["kubernetes"]
    base = {
        "cluster": source.get("cluster"),
        "namespace": source.get("namespace"),
        "workload_kind": source.get("workload_kind"),
        "workload_name": source.get("workload_name"),
        "selector_discovery": source.get("selector_discovery"),
        "signals": source.get("signals", []),
    }
    if args.skip_kubernetes:
        return {"status": "skipped", **base}, []
    if source.get("status") == "missing" or not source.get("workload_name"):
        return {"status": "missing", **base, "reason": "k8s_mapping_missing"}, []

    namespace = source.get("namespace")
    workload_kind = source.get("workload_kind", "Deployment")
    workload_name = source.get("workload_name")
    context = resolve_kubectl_context(source.get("cluster"), args.kubectl_context)
    kubeconfig = kubeconfig_with_aws_profile(context, args.kubectl_aws_profile, args.kubectl_timeout_seconds)
    proxy_url = args.kubectl_proxy_url
    errors: list[dict] = []
    try:
        workload = kubectl_json(
            ["get", workload_kind.lower(), workload_name, "-n", namespace, "-o", "json"],
            context,
            kubeconfig,
            proxy_url,
            args.kubectl_timeout_seconds,
        )
        match_labels = workload.get("spec", {}).get("selector", {}).get("matchLabels", {})
        selector = selector_from_match_labels(match_labels)
        if not selector:
            return {"status": "error", **base, "selector": ""}, [
                {"source": "kubernetes", "signal": "selector", "error": "workload selector is empty"}
            ]

        pods_payload = kubectl_json(
            ["get", "pods", "-n", namespace, "-l", selector, "-o", "json"],
            context,
            kubeconfig,
            proxy_url,
            args.kubectl_timeout_seconds,
        )
        pods = pods_payload.get("items", [])
        pod_names = {pod.get("metadata", {}).get("name") for pod in pods if pod.get("metadata", {}).get("name")}
        events = []
        if not args.skip_kubernetes_events:
            provider = args.kubernetes_events_provider
            can_query_victorialogs = bool(args.victorialogs_url and args.victorialogs_kubernetes_events_query_template)
            if provider == "none":
                events = []
            elif provider in {"auto", "victorialogs"} and can_query_victorialogs:
                try:
                    events = collect_victorialogs_kubernetes_events(
                        namespace,
                        workload_name,
                        selector,
                        pod_names,
                        start,
                        end,
                        args,
                    )
                except Exception as exc:  # noqa: BLE001 - event source is best-effort.
                    errors.append({"source": "victorialogs", "signal": "kubernetes_events", "error": str(exc)})
                    if provider == "victorialogs":
                        events = []
                    else:
                        provider = "kubectl"
            if provider == "kubectl" or (provider == "auto" and not can_query_victorialogs):
                events_payload = kubectl_json(
                    ["get", "events", "-n", namespace, "-o", "json"],
                    context,
                    kubeconfig,
                    proxy_url,
                    args.kubectl_timeout_seconds,
                )
                for event in events_payload.get("items", []):
                    involved = event.get("involvedObject", {})
                    involved_name = involved.get("name")
                    involved_kind = involved.get("kind")
                    if involved_name not in pod_names and involved_name != workload_name:
                        continue
                    ts = event_timestamp(event)
                    if ts and not (start <= ts <= end):
                        continue
                    reason = event.get("reason")
                    message = event.get("message")
                    events.append(
                        {
                            "timestamp": format_time(ts) if ts else None,
                            "reason": reason,
                            "type": event.get("type"),
                            "message": message,
                            "involved_kind": involved_kind,
                            "involved_name": involved_name,
                            "count": event.get("count"),
                            "source": "kubectl",
                        }
                    )

        pod_summaries = []
        restart_count = 0
        oom_killed_count = 0
        waiting_reasons: dict[str, int] = {}
        for pod in pods:
            status = pod.get("status", {})
            container_statuses = [container_state_summary(item) for item in status.get("containerStatuses", [])]
            restart_count += sum(item.get("restart_count") or 0 for item in container_statuses)
            oom_killed_count += sum(1 for item in container_statuses if item.get("last_terminated_reason") == "OOMKilled")
            for item in container_statuses:
                reason = item.get("waiting_reason")
                if reason:
                    waiting_reasons[reason] = waiting_reasons.get(reason, 0) + 1
            pod_summaries.append(
                {
                    "name": pod.get("metadata", {}).get("name"),
                    "phase": status.get("phase"),
                    "ready_condition": next(
                        (
                            condition.get("status")
                            for condition in status.get("conditions", [])
                            if condition.get("type") == "Ready"
                        ),
                        None,
                    ),
                    "containers": container_statuses,
                }
            )

        unhealthy_events = [event for event in events if event.get("reason") == "Unhealthy"]
        failed_scheduling_events = [event for event in events if event.get("reason") == "FailedScheduling"]
        killing_events = [event for event in events if event.get("reason") == "Killing"]
        pull_failure_events = [
            event
            for event in events
            if event.get("reason") in {"Failed", "BackOff", "ErrImagePull", "ImagePullBackOff"}
            and "pull" in (event.get("message") or "").lower()
        ]
        status = workload.get("status", {})
        spec = workload.get("spec", {})
        desired_replicas = spec.get("replicas", 0)
        ready_replicas = status.get("readyReplicas", 0)
        updated_replicas = status.get("updatedReplicas", 0)
        available_replicas = status.get("availableReplicas", 0)
        rollout_complete = (
            ready_replicas == desired_replicas
            and updated_replicas == desired_replicas
            and available_replicas == desired_replicas
        )

        return {
            "status": "collected",
            **base,
            "context": context,
            "selector": selector,
            "replicas": {
                "desired": desired_replicas,
                "ready": ready_replicas,
                "updated": updated_replicas,
                "available": available_replicas,
                "rollout_complete": rollout_complete,
            },
            "pod_count": len(pods),
            "restart_count": restart_count,
            "oom_killed_count": oom_killed_count,
            "probe_failure_count": len(unhealthy_events),
            "failed_scheduling_count": len(failed_scheduling_events),
            "killing_event_count": len(killing_events),
            "image_pull_failure_count": len(pull_failure_events),
            "waiting_reasons": waiting_reasons,
            "pods": pod_summaries,
            "events": events,
            "events_collected": not args.skip_kubernetes_events,
            "events_provider": args.kubernetes_events_provider,
        }, errors
    except Exception as exc:  # noqa: BLE001 - persist collection error as data quality.
        return {"status": "error", **base}, [
            {"source": "kubernetes", "signal": "inspect", "error": str(exc)}
        ]
    finally:
        if kubeconfig:
            try:
                Path(kubeconfig).unlink()
            except OSError:
                pass


def classify_changed_files(paths: list[str]) -> dict:
    config_exts = (".yaml", ".yml", ".json", ".toml", ".ini", ".conf", ".properties")
    dependency_files = {
        "package.json",
        "package-lock.json",
        "yarn.lock",
        "pnpm-lock.yaml",
        "go.mod",
        "go.sum",
        "requirements.txt",
        "poetry.lock",
        "Gemfile",
        "Gemfile.lock",
    }
    database_markers = ("migration", "migrations", "schema", "db/")
    lowered = [path.lower() for path in paths]
    return {
        "changed_file_count": len(paths),
        "changed_config_file_count": sum(1 for path in lowered if path.endswith(config_exts)),
        "changed_dependency_file_count": sum(1 for path in lowered if path.rsplit("/", 1)[-1] in dependency_files),
        "changed_database_file_count": sum(1 for path in lowered if any(marker in path for marker in database_markers)),
        "changed_kubernetes_file_count": sum(1 for path in lowered if "k8s" in path or "kubernetes" in path or "helm" in path),
    }


def github_api(path: str) -> Any:
    completed = subprocess.run(
        ["gh", "api", path],
        check=True,
        text=True,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
    )
    return json.loads(completed.stdout)


def collect_github(plan: dict, start: datetime, end: datetime, args: argparse.Namespace) -> tuple[dict, list[dict]]:
    source = plan["sources"]["github"]
    if args.skip_github:
        return {
            "status": "skipped",
            "repo": source.get("repo"),
            "branch": source.get("branch"),
            "signals": source.get("signals", []),
        }, []

    repo = source.get("repo")
    branch = source.get("branch", "master")
    since = urllib.parse.quote(format_time(start), safe="")
    until = urllib.parse.quote(format_time(end), safe="")
    path = f"repos/{repo}/commits?sha={branch}&since={since}&until={until}&per_page=20"
    try:
        commits = github_api(path)
        details = []
        changed_paths: list[str] = []
        for commit in commits[: args.github_commit_detail_limit]:
            sha = commit.get("sha")
            detail = github_api(f"repos/{repo}/commits/{sha}") if sha else {}
            files = [item.get("filename") for item in detail.get("files", []) if item.get("filename")]
            changed_paths.extend(files)
            details.append(
                {
                    "sha": sha,
                    "author": commit.get("commit", {}).get("author", {}).get("email"),
                    "date": commit.get("commit", {}).get("author", {}).get("date"),
                    "message": commit.get("commit", {}).get("message"),
                    "changed_files": files,
                }
            )
        latest = details[0] if details else None
        latest_date = parse_time(latest["date"]) if latest and latest.get("date") else None
        features = classify_changed_files(changed_paths)
        features.update(
            {
                "recent_master_commit_count": len(commits),
                "minutes_since_latest_master_commit": (
                    max(0.0, (end - latest_date).total_seconds() / 60.0) if latest_date else None
                ),
                "inferred_deployment": bool(commits),
                "confidence": "medium" if commits else "low",
            }
        )
        return {
            "status": "collected",
            "repo": repo,
            "branch": branch,
            "signals": source.get("signals", []),
            "features": features,
            "latest_commit": latest,
            "commits": details,
        }, []
    except Exception as exc:  # noqa: BLE001 - persist collection error as data quality.
        return {
            "status": "error",
            "repo": repo,
            "branch": branch,
            "signals": source.get("signals", []),
        }, [{"source": "github", "signal": "latest_commits", "error": str(exc)}]


def placeholder_github(plan: dict) -> dict:
    source = plan["sources"]["github"]
    return {
        "status": "not_collected",
        "repo": source.get("repo"),
        "branch": source.get("branch"),
        "signals": source.get("signals", []),
    }


def sql_literal(value: Any) -> str:
    if value is None:
        return "null"
    if isinstance(value, (dict, list)):
        return "'" + json.dumps(value, sort_keys=True).replace("'", "''") + "'"
    return "'" + str(value).replace("'", "''") + "'"


def upsert_rows(database_url: str, rows: list[dict]) -> None:
    if not rows:
        return
    statements = ["begin;"]
    for row in rows:
        statements.append(
            """
insert into service_metric_windows (
  service_id,
  window_start,
  window_end,
  window_size,
  newrelic,
  prometheus_resources,
  kubernetes,
  change_context,
  data_quality,
  source_snapshot_uri
) values (
  {service_id},
  {window_start},
  {window_end},
  {window_size},
  {newrelic}::jsonb,
  {prometheus_resources}::jsonb,
  {kubernetes}::jsonb,
  {change_context}::jsonb,
  {data_quality}::jsonb,
  {source_snapshot_uri}
)
on conflict (service_id, window_start, window_size) do update set
  window_end = excluded.window_end,
  newrelic = case
    when excluded.newrelic->>'status' = 'skipped' then service_metric_windows.newrelic
    else excluded.newrelic
  end,
  prometheus_resources = case
    when excluded.prometheus_resources->>'status' = 'skipped' then service_metric_windows.prometheus_resources
    else excluded.prometheus_resources
  end,
  kubernetes = case
    when excluded.kubernetes->>'status' = 'skipped' then service_metric_windows.kubernetes
    else excluded.kubernetes
  end,
  change_context = case
    when excluded.change_context->>'status' = 'skipped' then service_metric_windows.change_context
    else excluded.change_context
  end,
  data_quality = excluded.data_quality,
  source_snapshot_uri = excluded.source_snapshot_uri;
""".format(**{key: sql_literal(row.get(key)) for key in [
                "service_id",
                "window_start",
                "window_end",
                "window_size",
                "newrelic",
                "prometheus_resources",
                "kubernetes",
                "change_context",
                "data_quality",
                "source_snapshot_uri",
            ]})
        )
    statements.append("commit;")
    subprocess.run(["psql", database_url, "-q", "-v", "ON_ERROR_STOP=1"], input="\n".join(statements), text=True, check=True)


def build_row(plan: dict, window_start: datetime, window_end: datetime, args: argparse.Namespace) -> dict:
    prometheus_resources, errors = collect_prometheus(plan, window_start, window_end, args)
    newrelic, newrelic_errors = collect_newrelic(plan, window_start, window_end, args)
    kubernetes, kubernetes_errors = collect_kubernetes(plan, window_start, window_end, args)
    change_context, github_errors = collect_github(plan, window_start, window_end, args)
    errors.extend(newrelic_errors)
    errors.extend(kubernetes_errors)
    errors.extend(github_errors)
    return {
        "service_id": plan["service_id"],
        "window_start": format_time(window_start),
        "window_end": format_time(window_end),
        "window_size": args.window_size or plan["window_size"],
        "newrelic": newrelic,
        "prometheus_resources": prometheus_resources,
        "kubernetes": kubernetes,
        "change_context": change_context,
        "data_quality": {
            "collector": "collect_windows.py",
            "collector_version": "mvp-nr-prometheus-k8s-v1",
            "collected_at": format_time(datetime.now(timezone.utc)),
            "errors": errors,
        },
        "source_snapshot_uri": None,
    }


def main() -> None:
    args = parse_args()
    service_filter = set(args.service) if args.service else None
    plans = load_plan(args.plan, service_filter)
    if not plans:
        print("No collection plans matched.", file=sys.stderr)
        sys.exit(1)

    rows: list[dict] = []
    emitted = 0
    written = 0
    started = time.monotonic()
    for plan in plans:
        start = parse_time(args.start or plan["start"])
        end = parse_time(args.end or plan["end"])
        window_size = args.window_size or plan["window_size"]
        for window_start, window_end in iter_windows(start, end, window_size):
            rows.append(build_row(plan, window_start, window_end, args))
            emitted += 1
            if not args.dry_run and len(rows) >= args.batch_size:
                upsert_rows(args.database_url, rows)
                written += len(rows)
                rows.clear()
                print(json.dumps({"written": written, "elapsed_seconds": round(time.monotonic() - started, 3)}), file=sys.stderr)
                if args.sleep_seconds_between_batches > 0:
                    time.sleep(args.sleep_seconds_between_batches)
            if args.max_windows and emitted >= args.max_windows:
                break
        if args.max_windows and emitted >= args.max_windows:
            break

    if args.dry_run:
        for row in rows:
            print(json.dumps(row, sort_keys=True))
    elif rows:
        upsert_rows(args.database_url, rows)
        written += len(rows)

    elapsed = time.monotonic() - started
    print(
        json.dumps(
            {
                "rows": emitted,
                "written": 0 if args.dry_run else written,
                "dry_run": args.dry_run,
                "elapsed_seconds": round(elapsed, 3),
            },
            sort_keys=True,
        )
    )


if __name__ == "__main__":
    main()
