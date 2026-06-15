"""Baseline, anomaly, risk scoring, and incident hypothesis ranking."""

from __future__ import annotations

import math
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from typing import Any

from .db import psql_exec, psql_json, sql_literal, sql_text


BASELINE_VERSION = "baseline-v1"
LABEL_RULE_VERSION = "rule-v1"


METRIC_NAMES = [
    "newrelic.request_count",
    "newrelic.rpm",
    "newrelic.error_rate_percent",
    "newrelic.latency_p95_ms",
    "newrelic.latency_p99_ms",
    "prometheus.cpu_usage.avg",
    "prometheus.memory_usage.avg",
    "prometheus.cpu_throttling.avg",
    "prometheus.network_receive.avg",
    "prometheus.network_transmit.avg",
    "kubernetes.restart_count",
    "kubernetes.oom_killed_count",
    "kubernetes.probe_failure_count",
    "kubernetes.failed_scheduling_count",
    "kubernetes.image_pull_failure_count",
    "kubernetes.ready_ratio",
]


def utc_now() -> datetime:
    return datetime.now(timezone.utc)


def format_time(value: datetime) -> str:
    return value.astimezone(timezone.utc).isoformat().replace("+00:00", "Z")


def parse_time(value: str) -> datetime:
    parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=timezone.utc)
    return parsed.astimezone(timezone.utc)


def percentile(values: list[float], pct: float) -> float | None:
    clean = sorted(value for value in values if value is not None and math.isfinite(value))
    if not clean:
        return None
    if len(clean) == 1:
        return clean[0]
    rank = (len(clean) - 1) * pct
    low = math.floor(rank)
    high = math.ceil(rank)
    if low == high:
        return clean[int(rank)]
    return clean[low] * (high - rank) + clean[high] * (rank - low)


def json_number(payload: dict[str, Any], *path: str) -> float | None:
    cursor: Any = payload
    for key in path:
        if not isinstance(cursor, dict):
            return None
        cursor = cursor.get(key)
    if cursor is None:
        return None
    try:
        value = float(cursor)
    except (TypeError, ValueError):
        return None
    return value if math.isfinite(value) else None


def extract_metrics(row: dict[str, Any]) -> dict[str, float]:
    newrelic = row.get("newrelic") or {}
    prom = row.get("prometheus_resources") or {}
    k8s = row.get("kubernetes") or {}
    replicas = k8s.get("replicas") if isinstance(k8s.get("replicas"), dict) else {}
    desired = replicas.get("desired") or 0
    ready = replicas.get("ready") or 0
    ready_ratio = None
    if isinstance(desired, (int, float)) and desired > 0:
        ready_ratio = float(ready or 0) / float(desired)

    candidates = {
        "newrelic.request_count": json_number(newrelic, "request_count"),
        "newrelic.rpm": json_number(newrelic, "rpm"),
        "newrelic.error_rate_percent": json_number(newrelic, "error_rate_percent"),
        "newrelic.latency_p95_ms": json_number(newrelic, "latency_p95_ms"),
        "newrelic.latency_p99_ms": json_number(newrelic, "latency_p99_ms"),
        "prometheus.cpu_usage.avg": json_number(prom, "cpu_usage", "avg"),
        "prometheus.memory_usage.avg": json_number(prom, "memory_usage", "avg"),
        "prometheus.cpu_throttling.avg": json_number(prom, "cpu_throttling", "avg"),
        "prometheus.network_receive.avg": json_number(prom, "network_receive", "avg"),
        "prometheus.network_transmit.avg": json_number(prom, "network_transmit", "avg"),
        "kubernetes.restart_count": json_number(k8s, "restart_count"),
        "kubernetes.oom_killed_count": json_number(k8s, "oom_killed_count"),
        "kubernetes.probe_failure_count": json_number(k8s, "probe_failure_count"),
        "kubernetes.failed_scheduling_count": json_number(k8s, "failed_scheduling_count"),
        "kubernetes.image_pull_failure_count": json_number(k8s, "image_pull_failure_count"),
        "kubernetes.ready_ratio": ready_ratio,
    }
    return {key: value for key, value in candidates.items() if value is not None}


def load_metric_windows(
    database_url: str,
    service_id: str | None = None,
    since: datetime | None = None,
    until: datetime | None = None,
    limit: int | None = None,
) -> list[dict[str, Any]]:
    where = ["window_size = '15m'"]
    if service_id:
        where.append(f"service_id = '{sql_text(service_id)}'")
    if since:
        where.append(f"window_start >= '{format_time(since)}'")
    if until:
        where.append(f"window_start < '{format_time(until)}'")
    limit_sql = f"limit {limit}" if limit else ""
    sql = f"""
select coalesce(json_agg(row_to_json(w) order by window_start), '[]'::json)
from (
  select service_id, window_start, window_end, window_size, newrelic,
         prometheus_resources, kubernetes, change_context, data_quality
  from service_metric_windows
  where {' and '.join(where)}
  order by window_start
  {limit_sql}
) w;
"""
    return psql_json(database_url, sql) or []


def list_service_ids(database_url: str) -> list[str]:
    sql = """
select coalesce(json_agg(service_id order by service_id), '[]'::json)
from services;
"""
    return psql_json(database_url, sql) or []


@dataclass
class BaselineBucket:
    service_id: str
    metric_name: str
    day_of_week: int | None
    hour_of_day: int | None
    p50: float | None
    p75: float | None
    p90: float | None
    p95: float | None
    p99: float | None
    sample_count: int


def build_baselines(
    database_url: str,
    service_ids: list[str] | None = None,
    days: int = 30,
    baseline_version: str = BASELINE_VERSION,
    min_bucket_samples: int = 12,
) -> dict[str, Any]:
    valid_from = utc_now()
    since = valid_from - timedelta(days=days)
    selected_services = service_ids or list_service_ids(database_url)
    buckets: list[BaselineBucket] = []

    for service_id in selected_services:
        rows = load_metric_windows(database_url, service_id=service_id, since=since, until=valid_from)
        by_metric: dict[tuple[str, int | None, int | None], list[float]] = {}
        for row in rows:
            window_start = parse_time(row["window_start"])
            metrics = extract_metrics(row)
            for metric_name, value in metrics.items():
                by_metric.setdefault((metric_name, None, None), []).append(value)
                by_metric.setdefault((metric_name, window_start.weekday(), window_start.hour), []).append(value)
        for (metric_name, day_of_week, hour_of_day), values in by_metric.items():
            if day_of_week is not None and len(values) < min_bucket_samples:
                continue
            buckets.append(
                BaselineBucket(
                    service_id=service_id,
                    metric_name=metric_name,
                    day_of_week=day_of_week,
                    hour_of_day=hour_of_day,
                    p50=percentile(values, 0.50),
                    p75=percentile(values, 0.75),
                    p90=percentile(values, 0.90),
                    p95=percentile(values, 0.95),
                    p99=percentile(values, 0.99),
                    sample_count=len(values),
                )
            )

    delete_filter = (
        " and service_id in (" + ",".join(sql_literal(service_id) for service_id in selected_services) + ")"
        if selected_services
        else ""
    )
    statements = [
        "begin;",
        f"delete from service_baselines where baseline_version = {sql_literal(baseline_version)}{delete_filter};",
    ]
    for bucket in buckets:
        statements.append(
            """
insert into service_baselines (
  service_id, baseline_version, metric_name, day_of_week, hour_of_day, traffic_bucket,
  p50, p75, p90, p95, p99, sample_count, valid_from
) values (
  {service_id}, {baseline_version}, {metric_name}, {day_of_week}, {hour_of_day}, null,
  {p50}, {p75}, {p90}, {p95}, {p99}, {sample_count}, {valid_from}
);
""".format(
                service_id=sql_literal(bucket.service_id),
                baseline_version=sql_literal(baseline_version),
                metric_name=sql_literal(bucket.metric_name),
                day_of_week=sql_literal(bucket.day_of_week),
                hour_of_day=sql_literal(bucket.hour_of_day),
                p50=sql_literal(bucket.p50),
                p75=sql_literal(bucket.p75),
                p90=sql_literal(bucket.p90),
                p95=sql_literal(bucket.p95),
                p99=sql_literal(bucket.p99),
                sample_count=sql_literal(bucket.sample_count),
                valid_from=sql_literal(format_time(valid_from)),
            )
        )
    statements.append("commit;")
    psql_exec(database_url, "\n".join(statements))
    return {
        "status": "succeeded",
        "baseline_version": baseline_version,
        "services": len(selected_services),
        "baseline_rows": len(buckets),
        "valid_from": format_time(valid_from),
        "history_days": days,
    }


def load_baseline_map(database_url: str, service_id: str, baseline_version: str = BASELINE_VERSION) -> dict[str, dict]:
    sql = f"""
select coalesce(json_agg(row_to_json(b)), '[]'::json)
from (
  select metric_name, day_of_week, hour_of_day, p50, p75, p90, p95, p99, sample_count
  from service_baselines
  where service_id = '{sql_text(service_id)}'
    and baseline_version = '{sql_text(baseline_version)}'
) b;
"""
    rows = psql_json(database_url, sql) or []
    result: dict[str, dict] = {}
    for row in rows:
        key = f"{row['metric_name']}|{row['day_of_week']}|{row['hour_of_day']}"
        result[key] = row
    return result


def pick_baseline(baselines: dict[str, dict], metric_name: str, window_start: datetime) -> dict | None:
    specific_key = f"{metric_name}|{window_start.weekday()}|{window_start.hour}"
    global_key = f"{metric_name}|None|None"
    return baselines.get(specific_key) or baselines.get(global_key)


def evaluate_window(row: dict[str, Any], baselines: dict[str, dict]) -> dict[str, Any]:
    window_start = parse_time(row["window_start"])
    metrics = extract_metrics(row)
    evidence: list[dict[str, Any]] = []
    anomaly_types: set[str] = set()
    score = 0

    for metric_name, value in metrics.items():
        baseline = pick_baseline(baselines, metric_name, window_start)
        if not baseline or not baseline.get("sample_count"):
            continue
        p95 = baseline.get("p95")
        p99 = baseline.get("p99")
        p50 = baseline.get("p50")
        if metric_name == "kubernetes.ready_ratio":
            if value < 1.0:
                severity_points = 35 if value < 0.8 else 20
                score += severity_points
                anomaly_types.add("kubernetes_availability")
                evidence.append({"metric": metric_name, "value": value, "expected": 1.0, "points": severity_points})
            continue
        if p99 is not None and value > p99 and value > 0:
            points = 28
        elif p95 is not None and value > p95 and value > 0:
            points = 16
        else:
            continue
        if metric_name in {"newrelic.request_count", "newrelic.rpm"}:
            anomaly_types.add("traffic_change")
            points = 8
        elif metric_name.startswith("newrelic.error_rate"):
            anomaly_types.add("error_rate")
            points += 12
        elif "latency" in metric_name:
            anomaly_types.add("latency")
        elif metric_name.startswith("prometheus."):
            anomaly_types.add("resource_pressure")
        elif metric_name.startswith("kubernetes."):
            anomaly_types.add("kubernetes_health")
            points += 8
        evidence.append(
            {
                "metric": metric_name,
                "value": value,
                "baseline_p50": p50,
                "baseline_p95": p95,
                "baseline_p99": p99,
                "points": points,
            }
        )
        score += points

    k8s = row.get("kubernetes") or {}
    replicas = k8s.get("replicas") if isinstance(k8s.get("replicas"), dict) else {}
    if replicas and replicas.get("rollout_complete") is False:
        score += 25
        anomaly_types.add("rollout")
        evidence.append({"metric": "kubernetes.rollout_complete", "value": False, "points": 25})
    waiting_reasons = k8s.get("waiting_reasons") if isinstance(k8s.get("waiting_reasons"), dict) else {}
    for reason, count in waiting_reasons.items():
        if count:
            score += 30
            anomaly_types.add("kubernetes_waiting")
            evidence.append({"metric": f"kubernetes.waiting_reasons.{reason}", "value": count, "points": 30})

    data_quality = row.get("data_quality") or {}
    errors = data_quality.get("errors") if isinstance(data_quality.get("errors"), list) else []
    if errors:
        score += min(15, len(errors) * 3)
        anomaly_types.add("data_quality")

    severity = "none"
    if score >= 80:
        severity = "critical"
    elif score >= 50:
        severity = "high"
    elif score >= 25:
        severity = "medium"
    elif score > 0:
        severity = "low"

    return {
        "service_id": row["service_id"],
        "window_start": row["window_start"],
        "window_end": row["window_end"],
        "window_size": row["window_size"],
        "is_anomaly": score > 0,
        "severity": severity,
        "score": min(score, 100),
        "anomaly_types": sorted(anomaly_types),
        "confidence": "high" if evidence else "low",
        "evidence": evidence[:20],
    }


def mark_anomalies(
    database_url: str,
    service_ids: list[str] | None = None,
    since: datetime | None = None,
    until: datetime | None = None,
    baseline_version: str = BASELINE_VERSION,
) -> dict[str, Any]:
    selected_services = service_ids or list_service_ids(database_url)
    since = since or (utc_now() - timedelta(days=30))
    until = until or utc_now()
    labels: list[dict[str, Any]] = []
    for service_id in selected_services:
        baselines = load_baseline_map(database_url, service_id, baseline_version)
        rows = load_metric_windows(database_url, service_id=service_id, since=since, until=until)
        labels.extend(evaluate_window(row, baselines) for row in rows)

    statements = [
        "begin;",
        "delete from anomaly_windows "
        f"where label_rule_version = {sql_literal(LABEL_RULE_VERSION)} "
        f"and window_start >= {sql_literal(format_time(since))} "
        f"and window_start < {sql_literal(format_time(until))} "
        f"and service_id in ({','.join(sql_literal(service_id) for service_id in selected_services)});",
    ]
    for label in labels:
        statements.append(
            """
insert into anomaly_windows (
  service_id, window_start, window_end, window_size, is_anomaly, severity,
  anomaly_types, confidence, label_source, label_rule_version, baseline_version, evidence
) values (
  {service_id}, {window_start}, {window_end}, {window_size}, {is_anomaly}, {severity},
  {anomaly_types}, {confidence}, {label_source}, {label_rule_version}, {baseline_version}, {evidence}::jsonb
);
""".format(
                service_id=sql_literal(label["service_id"]),
                window_start=sql_literal(label["window_start"]),
                window_end=sql_literal(label["window_end"]),
                window_size=sql_literal(label["window_size"]),
                is_anomaly=sql_literal(label["is_anomaly"]),
                severity=sql_literal(label["severity"]),
                anomaly_types=sql_literal("{" + ",".join(label["anomaly_types"]) + "}"),
                confidence=sql_literal(label["confidence"]),
                label_source=sql_literal("{rule_engine}"),
                label_rule_version=sql_literal(LABEL_RULE_VERSION),
                baseline_version=sql_literal(baseline_version),
                evidence=sql_literal(label["evidence"]),
            )
        )
    statements.append("commit;")
    psql_exec(database_url, "\n".join(statements))
    return {
        "status": "succeeded",
        "services": len(selected_services),
        "windows_labeled": len(labels),
        "anomalies": sum(1 for label in labels if label["is_anomaly"]),
        "baseline_version": baseline_version,
        "label_rule_version": LABEL_RULE_VERSION,
        "since": format_time(since),
        "until": format_time(until),
    }


def risk_from_scores(scores: list[int]) -> tuple[int, str]:
    if not scores:
        return 0, "unknown"
    latest = scores[-1]
    peak = max(scores)
    avg_recent = sum(scores[-4:]) / min(len(scores), 4)
    score = round(max(latest, peak * 0.8, avg_recent))
    if score >= 80:
        return min(score, 100), "critical"
    if score >= 55:
        return score, "high"
    if score >= 25:
        return score, "medium"
    return score, "low"


def score_service_risk(
    database_url: str,
    service_id: str,
    lookback_hours: int = 6,
    baseline_version: str = BASELINE_VERSION,
) -> dict[str, Any]:
    until = utc_now()
    since = until - timedelta(hours=lookback_hours)
    baselines = load_baseline_map(database_url, service_id, baseline_version)
    rows = load_metric_windows(database_url, service_id=service_id, since=since, until=until)
    evaluations = [evaluate_window(row, baselines) for row in rows]
    score, level = risk_from_scores([item["score"] for item in evaluations])
    top_evidence: list[dict[str, Any]] = []
    for item in sorted(evaluations, key=lambda row: row["score"], reverse=True):
        for evidence in item["evidence"]:
            evidence = {**evidence, "window_start": item["window_start"]}
            top_evidence.append(evidence)
            if len(top_evidence) >= 10:
                break
        if len(top_evidence) >= 10:
            break
    return {
        "service_id": service_id,
        "risk_score": score,
        "risk_level": level,
        "lookback_hours": lookback_hours,
        "baseline_version": baseline_version,
        "window_count": len(evaluations),
        "latest_window": evaluations[-1] if evaluations else None,
        "top_evidence": top_evidence,
    }


def rank_incident_hypotheses(
    database_url: str,
    service_id: str,
    since: datetime | None = None,
    until: datetime | None = None,
    limit: int = 16,
    baseline_version: str = BASELINE_VERSION,
    trace_evidence: dict[str, Any] | None = None,
) -> dict[str, Any]:
    until = until or utc_now()
    since = since or (until - timedelta(hours=6))
    baselines = load_baseline_map(database_url, service_id, baseline_version)
    rows = load_metric_windows(database_url, service_id=service_id, since=since, until=until, limit=max(limit, 1))
    evaluations = [evaluate_window(row, baselines) for row in rows]
    points = {
        "application_regression_or_downstream_latency": 0,
        "application_error_spike": 0,
        "downstream_dependency_latency": 0,
        "database_latency": 0,
        "resource_pressure": 0,
        "kubernetes_workload_health": 0,
        "recent_change": 0,
        "observability_data_gap": 0,
    }
    evidence: dict[str, list[dict[str, Any]]] = {key: [] for key in points}
    for evaluation in evaluations:
        for item in evaluation["evidence"]:
            metric = item.get("metric", "")
            if "latency" in metric:
                points["application_regression_or_downstream_latency"] += item.get("points", 0)
                evidence["application_regression_or_downstream_latency"].append({**item, "window_start": evaluation["window_start"]})
            elif "error_rate" in metric:
                points["application_error_spike"] += item.get("points", 0)
                evidence["application_error_spike"].append({**item, "window_start": evaluation["window_start"]})
            elif metric.startswith("prometheus."):
                points["resource_pressure"] += item.get("points", 0)
                evidence["resource_pressure"].append({**item, "window_start": evaluation["window_start"]})
            elif metric.startswith("kubernetes."):
                points["kubernetes_workload_health"] += item.get("points", 0)
                evidence["kubernetes_workload_health"].append({**item, "window_start": evaluation["window_start"]})
        row = next((candidate for candidate in rows if candidate["window_start"] == evaluation["window_start"]), {})
        change_context = row.get("change_context") or {}
        features = change_context.get("features") if isinstance(change_context.get("features"), dict) else {}
        if features.get("recent_master_commit_count"):
            points["recent_change"] += 18
            evidence["recent_change"].append({"window_start": evaluation["window_start"], "features": features})
        errors = ((row.get("data_quality") or {}).get("errors") or [])
        if errors:
            points["observability_data_gap"] += min(10, len(errors) * 2)
            evidence["observability_data_gap"].append({"window_start": evaluation["window_start"], "errors": errors[:3]})

    if trace_evidence:
        status = trace_evidence.get("status")
        if status in {"collected", "partial"}:
            for item in trace_evidence.get("top_slow_transactions", [])[:5]:
                transaction_name = item.get("name") or item.get("facet")
                percentiles = item.get("percentile.duration") or {}
                p95_seconds = percentiles.get("95")
                if isinstance(p95_seconds, (int, float)) and p95_seconds > 1:
                    slow_transaction_evidence = {
                        "source": "newrelic_trace",
                        "type": "slow_transaction",
                        "transaction": transaction_name,
                        "p95_ms": p95_seconds * 1000,
                        "sample_count": item.get("sample_count"),
                    }
                    baseline = item.get("baseline")
                    if isinstance(baseline, dict):
                        slow_transaction_evidence["baseline"] = {
                            "p95_ms": baseline.get("p95_ms"),
                            "p99_ms": baseline.get("p99_ms"),
                            "avg_ms": baseline.get("avg_ms"),
                            "sample_count": baseline.get("sample_count"),
                        }
                    for key in ["p95_vs_baseline_pct", "p99_vs_baseline_pct", "avg_vs_baseline_pct"]:
                        if key in item:
                            slow_transaction_evidence[key] = item[key]
                    points["application_regression_or_downstream_latency"] += 25
                    evidence["application_regression_or_downstream_latency"].append(slow_transaction_evidence)
            for item in trace_evidence.get("span_category_breakdown", []):
                category = item.get("category") or item.get("facet")
                percentiles = item.get("percentile.duration.ms") or {}
                p95_ms = percentiles.get("95")
                if not isinstance(p95_ms, (int, float)) or p95_ms <= 500:
                    continue
                if category == "http":
                    points["downstream_dependency_latency"] += 35
                    evidence["downstream_dependency_latency"].append(
                        {"source": "newrelic_trace", "type": "span_category", "category": category, "p95_ms": p95_ms}
                    )
                elif category == "datastore":
                    points["database_latency"] += 35
                    evidence["database_latency"].append(
                        {"source": "newrelic_trace", "type": "span_category", "category": category, "p95_ms": p95_ms}
                    )
                else:
                    points["application_regression_or_downstream_latency"] += 15
                    evidence["application_regression_or_downstream_latency"].append(
                        {"source": "newrelic_trace", "type": "span_category", "category": category, "p95_ms": p95_ms}
                    )
            for item in trace_evidence.get("external_hotspots", [])[:5]:
                facet = item.get("facet") or []
                percentiles = item.get("percentile.duration.ms") or {}
                p95_ms = percentiles.get("95")
                if isinstance(p95_ms, (int, float)) and p95_ms > 500:
                    points["downstream_dependency_latency"] += 20
                    evidence["downstream_dependency_latency"].append(
                        {
                            "source": "newrelic_trace",
                            "type": "external_hotspot",
                            "http_url": item.get("http.url") or (facet[0] if len(facet) > 0 else None),
                            "peer_hostname": item.get("peer.hostname") or (facet[1] if len(facet) > 1 else None),
                            "p95_ms": p95_ms,
                            "span_count": item.get("span_count"),
                        }
                    )
            for item in trace_evidence.get("datastore_hotspots", [])[:5]:
                facet = item.get("facet") or []
                percentiles = item.get("percentile.duration.ms") or {}
                p95_ms = percentiles.get("95")
                if isinstance(p95_ms, (int, float)) and p95_ms > 500:
                    points["database_latency"] += 20
                    evidence["database_latency"].append(
                        {
                            "source": "newrelic_trace",
                            "type": "datastore_hotspot",
                            "db_system": item.get("db.system") or (facet[0] if len(facet) > 0 else None),
                            "db_statement": item.get("db.statement") or (facet[1] if len(facet) > 1 else None),
                            "p95_ms": p95_ms,
                            "span_count": item.get("span_count"),
                        }
                    )
            error_samples = [
                item for item in trace_evidence.get("trace_samples", []) if item.get("error_class")
            ]
            if error_samples:
                points["application_error_spike"] += min(40, len(error_samples) * 8)
                evidence["application_error_spike"].append(
                    {"source": "newrelic_trace", "type": "trace_error_samples", "samples": error_samples[:5]}
                )
        elif status in {"error", "partial"}:
            trace_errors = trace_evidence.get("errors") or []
            if trace_errors:
                points["observability_data_gap"] += min(20, len(trace_errors) * 5)
                evidence["observability_data_gap"].append(
                    {"source": "newrelic_trace", "type": "trace_query_errors", "errors": trace_errors[:3]}
                )

    ranked = []
    recommendations = {
        "application_regression_or_downstream_latency": "Check slow New Relic transactions, external calls, and downstream latency around the first bad window.",
        "application_error_spike": "Inspect top New Relic error groups and recent code/config changes before scaling resources.",
        "downstream_dependency_latency": "Use trace external span hotspots to identify the slow downstream service or host before changing this workload.",
        "database_latency": "Use trace datastore span hotspots to inspect slow queries, indexes, connection pools, or recent DB migrations.",
        "resource_pressure": "Check CPU throttling, memory growth, pod limits, and HPA capacity; scale only if app signals match pressure.",
        "kubernetes_workload_health": "Inspect rollout status, pod readiness, restarts, waiting reasons, and recent events for this workload.",
        "recent_change": "Compare recent master commits with the incident start; consider rollback if symptoms started after change.",
        "observability_data_gap": "Fix missing telemetry before trusting low-risk conclusions for this window.",
    }
    for name, score in sorted(points.items(), key=lambda item: item[1], reverse=True):
        if score <= 0:
            continue
        ranked.append(
            {
                "hypothesis": name,
                "score": min(score, 100),
                "confidence": "high" if score >= 50 else "medium" if score >= 20 else "low",
                "evidence": evidence[name][:8],
                "recommended_next_action": recommendations[name],
            }
        )
    return {
        "service_id": service_id,
        "since": format_time(since),
        "until": format_time(until),
        "baseline_version": baseline_version,
        "risk": score_service_risk(database_url, service_id, lookback_hours=max(1, math.ceil((until - since).total_seconds() / 3600))),
        "trace_evidence": trace_evidence,
        "hypotheses": ranked,
        "windows": evaluations,
    }
