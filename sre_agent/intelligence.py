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

WINDOW_SECONDS = {"5m": 300, "15m": 900, "1h": 3600}
TRAFFIC_CONTEXT_METRICS = {"newrelic.request_count", "newrelic.rpm"}
RESOURCE_RATIO_THRESHOLDS = {
    "prometheus.cpu_usage.avg": (1.20, 1.40),
    "prometheus.memory_usage.avg": (1.20, 1.40),
    "prometheus.cpu_throttling.avg": (1.30, 1.60),
    "prometheus.network_receive.avg": (1.50, 2.00),
    "prometheus.network_transmit.avg": (1.50, 2.00),
}
BASELINE_SCOPE_WEIGHTS = {
    "weekday_hour_slot": 1.00,
    "hour_slot": 0.80,
    "weekday_hour": 0.60,
    "hour": 0.45,
    "global": 0.25,
}


def utc_now() -> datetime:
    return datetime.now(timezone.utc)


def format_time(value: datetime) -> str:
    return value.astimezone(timezone.utc).isoformat().replace("+00:00", "Z")


def parse_time(value: str) -> datetime:
    parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=timezone.utc)
    return parsed.astimezone(timezone.utc)


def align_floor(value: datetime, window_size: str = "15m") -> datetime:
    seconds = WINDOW_SECONDS.get(window_size, 900)
    timestamp = int(value.astimezone(timezone.utc).timestamp())
    return datetime.fromtimestamp(timestamp - (timestamp % seconds), tz=timezone.utc)


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
    minute_slot: int | None
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
    min_precise_bucket_samples: int = 3,
) -> dict[str, Any]:
    valid_from = utc_now()
    since = valid_from - timedelta(days=days)
    selected_services = service_ids or list_service_ids(database_url)
    buckets: list[BaselineBucket] = []

    for service_id in selected_services:
        rows = load_metric_windows(database_url, service_id=service_id, since=since, until=valid_from)
        by_metric: dict[tuple[str, int | None, int | None, int | None], list[float]] = {}
        for row in rows:
            window_start = parse_time(row["window_start"])
            minute_slot = window_start.minute
            metrics = extract_metrics(row)
            for metric_name, value in metrics.items():
                by_metric.setdefault((metric_name, None, None, None), []).append(value)
                by_metric.setdefault((metric_name, None, window_start.hour, None), []).append(value)
                by_metric.setdefault((metric_name, None, window_start.hour, minute_slot), []).append(value)
                by_metric.setdefault((metric_name, window_start.weekday(), window_start.hour, None), []).append(value)
                by_metric.setdefault((metric_name, window_start.weekday(), window_start.hour, minute_slot), []).append(value)
        for (metric_name, day_of_week, hour_of_day, minute_slot), values in by_metric.items():
            precise_bucket = day_of_week is not None and minute_slot is not None
            required_samples = min_precise_bucket_samples if precise_bucket else min_bucket_samples
            if (day_of_week is not None or hour_of_day is not None or minute_slot is not None) and len(values) < required_samples:
                continue
            buckets.append(
                BaselineBucket(
                    service_id=service_id,
                    metric_name=metric_name,
                    day_of_week=day_of_week,
                    hour_of_day=hour_of_day,
                    minute_slot=minute_slot,
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
  service_id, baseline_version, metric_name, day_of_week, hour_of_day, minute_slot, traffic_bucket,
  p50, p75, p90, p95, p99, sample_count, valid_from
) values (
  {service_id}, {baseline_version}, {metric_name}, {day_of_week}, {hour_of_day}, {minute_slot}, null,
  {p50}, {p75}, {p90}, {p95}, {p99}, {sample_count}, {valid_from}
);
""".format(
                service_id=sql_literal(bucket.service_id),
                baseline_version=sql_literal(baseline_version),
                metric_name=sql_literal(bucket.metric_name),
                day_of_week=sql_literal(bucket.day_of_week),
                hour_of_day=sql_literal(bucket.hour_of_day),
                minute_slot=sql_literal(bucket.minute_slot),
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
  select metric_name, day_of_week, hour_of_day, minute_slot, p50, p75, p90, p95, p99, sample_count
  from service_baselines
  where service_id = '{sql_text(service_id)}'
    and baseline_version = '{sql_text(baseline_version)}'
) b;
"""
    rows = psql_json(database_url, sql) or []
    result: dict[str, dict] = {}
    for row in rows:
        key = f"{row['metric_name']}|{row['day_of_week']}|{row['hour_of_day']}|{row.get('minute_slot')}"
        result[key] = row
    return result


def baseline_candidates(baselines: dict[str, dict], metric_name: str, window_start: datetime) -> list[dict[str, Any]]:
    candidates = [
        (f"{metric_name}|{window_start.weekday()}|{window_start.hour}|{window_start.minute}", "weekday_hour_slot"),
        (f"{metric_name}|None|{window_start.hour}|{window_start.minute}", "hour_slot"),
        (f"{metric_name}|{window_start.weekday()}|{window_start.hour}|None", "weekday_hour"),
        (f"{metric_name}|None|{window_start.hour}|None", "hour"),
        (f"{metric_name}|None|None|None", "global"),
    ]
    result = []
    for key, scope in candidates:
        baseline = baselines.get(key)
        if baseline:
            result.append({**baseline, "baseline_scope": scope, "baseline_weight": BASELINE_SCOPE_WEIGHTS[scope]})
    return result


def pick_baseline(baselines: dict[str, dict], metric_name: str, window_start: datetime) -> dict | None:
    candidates = baseline_candidates(baselines, metric_name, window_start)
    return candidates[0] if candidates else None


def metric_deviation_points(metric_name: str, value: float, baseline: dict[str, Any]) -> tuple[int, str | None]:
    p95 = baseline.get("p95")
    p99 = baseline.get("p99")
    if p95 is None and p99 is None:
        return 0, None
    if metric_name in TRAFFIC_CONTEXT_METRICS:
        return 0, "traffic_context"
    if metric_name in RESOURCE_RATIO_THRESHOLDS:
        reference = p99 if isinstance(p99, (int, float)) and p99 > 0 else p95
        if not isinstance(reference, (int, float)) or reference <= 0:
            return 0, None
        warning_ratio, critical_ratio = RESOURCE_RATIO_THRESHOLDS[metric_name]
        ratio = value / reference
        if ratio >= critical_ratio:
            return 28, "resource_critical_ratio"
        if ratio >= warning_ratio:
            return 16, "resource_warning_ratio"
        return 0, None
    if metric_name.startswith("newrelic.error_rate"):
        if p99 is not None and value > p99 and value > 0:
            return 40, "error_rate_p99"
        if p95 is not None and value > p95 and value > 0:
            return 28, "error_rate_p95"
        return 0, None
    if "latency" in metric_name:
        reference = p99 if isinstance(p99, (int, float)) and p99 > 0 else p95
        if isinstance(reference, (int, float)) and reference > 0:
            ratio = value / reference
            if ratio >= 1.5:
                return 28, "latency_critical_ratio"
            if ratio >= 1.2:
                return 16, "latency_warning_ratio"
        return 0, None
    if p99 is not None and value > p99 and value > 0:
        return 20, "p99"
    if p95 is not None and value > p95 and value > 0:
        return 10, "p95"
    return 0, None


def baseline_reference_value(metric_name: str, baseline: dict[str, Any]) -> float | None:
    p95 = baseline.get("p95")
    p99 = baseline.get("p99")
    if metric_name.startswith("newrelic.error_rate"):
        return p99 if isinstance(p99, (int, float)) and p99 > 0 else p95
    if metric_name in RESOURCE_RATIO_THRESHOLDS or "latency" in metric_name:
        return p99 if isinstance(p99, (int, float)) and p99 > 0 else p95
    return p99 if isinstance(p99, (int, float)) and p99 > 0 else p95


def metric_baseline_comparison(metric_name: str, value: float, baseline: dict[str, Any]) -> dict[str, Any]:
    raw_points, deviation_type = metric_deviation_points(metric_name, value, baseline)
    weight = float(baseline.get("baseline_weight") or 1.0)
    weighted_points = round(raw_points * weight)
    reference_value = baseline_reference_value(metric_name, baseline)
    deviation_ratio = (
        value / reference_value
        if isinstance(reference_value, (int, float)) and reference_value > 0
        else None
    )
    return {
        "baseline_scope": baseline.get("baseline_scope"),
        "baseline_weight": weight,
        "baseline_sample_count": baseline.get("sample_count"),
        "baseline_minute_slot": baseline.get("minute_slot"),
        "baseline_p50": baseline.get("p50"),
        "baseline_p95": baseline.get("p95"),
        "baseline_p99": baseline.get("p99"),
        "baseline_reference_value": reference_value,
        "deviation_ratio": deviation_ratio,
        "deviation_type": deviation_type,
        "raw_points": raw_points,
        "weighted_points": weighted_points,
    }


def evaluate_window(row: dict[str, Any], baselines: dict[str, dict]) -> dict[str, Any]:
    window_start = parse_time(row["window_start"])
    metrics = extract_metrics(row)
    evidence: list[dict[str, Any]] = []
    anomaly_types: set[str] = set()
    score = 0

    for metric_name, value in metrics.items():
        candidates = [baseline for baseline in baseline_candidates(baselines, metric_name, window_start) if baseline.get("sample_count")]
        if not candidates:
            continue
        if metric_name == "kubernetes.ready_ratio":
            if value < 1.0:
                severity_points = 35 if value < 0.8 else 20
                score += severity_points
                anomaly_types.add("kubernetes_availability")
                evidence.append({"metric": metric_name, "value": value, "expected": 1.0, "points": severity_points})
            continue
        comparisons = [metric_baseline_comparison(metric_name, value, baseline) for baseline in candidates]
        selected = max(comparisons, key=lambda item: item["weighted_points"])
        points = selected["weighted_points"]
        if points <= 0:
            continue
        if metric_name.startswith("newrelic.error_rate"):
            anomaly_types.add("error_rate")
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
                "baseline_p50": selected["baseline_p50"],
                "baseline_p95": selected["baseline_p95"],
                "baseline_p99": selected["baseline_p99"],
                "baseline_scope": selected["baseline_scope"],
                "baseline_weight": selected["baseline_weight"],
                "baseline_sample_count": selected["baseline_sample_count"],
                "baseline_minute_slot": selected["baseline_minute_slot"],
                "baseline_reference_value": selected["baseline_reference_value"],
                "deviation_ratio": selected["deviation_ratio"],
                "deviation_type": selected["deviation_type"],
                "raw_points": selected["raw_points"],
                "points": points,
                "baseline_comparisons": comparisons,
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


def _risk_level(score: int) -> str:
    if score >= 80:
        return "critical"
    if score >= 55:
        return "high"
    if score >= 25:
        return "medium"
    if score > 0:
        return "low"
    return "unknown"


def load_recent_trace_summaries(
    database_url: str,
    service_id: str,
    since: datetime,
    until: datetime,
    limit: int = 5,
) -> list[dict[str, Any]]:
    sql = f"""
select coalesce(json_agg(row_to_json(t) order by created_at desc), '[]'::json)
from (
  select id, window_start, window_end, status, trace_summary, errors, created_at
  from incident_trace_evidence
  where service_id = {sql_literal(service_id)}
    and window_start < {sql_literal(format_time(until))}
    and window_end > {sql_literal(format_time(since))}
  order by created_at desc
  limit {max(1, min(limit, 20))}
) t;
"""
    return psql_json(database_url, sql) or []


def score_transaction_baseline_risk(trace_summaries: list[dict[str, Any]]) -> dict[str, Any]:
    evidence: list[dict[str, Any]] = []
    points = 0
    matched_transactions = 0
    for trace_row in trace_summaries:
        trace_summary = trace_row.get("trace_summary") or {}
        for item in trace_summary.get("top_slow_transactions", []) or []:
            if item.get("baseline_status") != "matched":
                continue
            matched_transactions += 1
            transaction_name = item.get("name") or item.get("facet")
            p95_ratio = item.get("p95_vs_baseline_ratio")
            p99_ratio = item.get("p99_vs_baseline_ratio")
            avg_ratio = item.get("avg_vs_baseline_ratio")
            ratio_candidates = [
                value for value in [p95_ratio, p99_ratio, avg_ratio] if isinstance(value, (int, float))
            ]
            if not ratio_candidates:
                continue
            ratio = max(ratio_candidates)
            transaction_points = 0
            if ratio >= 3:
                transaction_points = 35
            elif ratio >= 2:
                transaction_points = 25
            elif ratio >= 1.5:
                transaction_points = 15
            elif ratio >= 1.25:
                transaction_points = 8
            if transaction_points <= 0:
                continue
            points += transaction_points
            evidence.append(
                {
                    "source": "incident_trace_evidence",
                    "metric": "newrelic.transaction_latency_vs_baseline",
                    "transaction": transaction_name,
                    "points": transaction_points,
                    "max_baseline_ratio": ratio,
                    "p95_vs_baseline_pct": item.get("p95_vs_baseline_pct"),
                    "p99_vs_baseline_pct": item.get("p99_vs_baseline_pct"),
                    "avg_vs_baseline_pct": item.get("avg_vs_baseline_pct"),
                    "baseline": item.get("baseline"),
                    "trace_window_start": trace_row.get("window_start"),
                    "trace_window_end": trace_row.get("window_end"),
                }
            )
    return {
        "score": min(points, 100),
        "matched_transactions": matched_transactions,
        "evidence": sorted(evidence, key=lambda item: item["points"], reverse=True)[:10],
    }


def score_service_risk(
    database_url: str,
    service_id: str,
    lookback_hours: int = 6,
    baseline_version: str = BASELINE_VERSION,
    since: datetime | None = None,
    until: datetime | None = None,
    risk_version: str = "risk-v2",
) -> dict[str, Any]:
    until = until or utc_now()
    since = since or (until - timedelta(hours=lookback_hours))
    baselines = load_baseline_map(database_url, service_id, baseline_version)
    rows = load_metric_windows(database_url, service_id=service_id, since=since, until=until)
    evaluations = [evaluate_window(row, baselines) for row in rows]
    base_score, _ = risk_from_scores([item["score"] for item in evaluations])
    top_evidence: list[dict[str, Any]] = []
    for item in sorted(evaluations, key=lambda row: row["score"], reverse=True):
        for evidence in item["evidence"]:
            evidence = {**evidence, "window_start": item["window_start"]}
            top_evidence.append(evidence)
            if len(top_evidence) >= 10:
                break
        if len(top_evidence) >= 10:
            break
    transaction_risk = score_transaction_baseline_risk(
        load_recent_trace_summaries(database_url, service_id, since=since, until=until)
    )
    score = min(100, max(base_score, round(base_score * 0.75 + transaction_risk["score"] * 0.5)))
    if transaction_risk["evidence"]:
        score = min(100, max(score, min(100, transaction_risk["score"])))
        top_evidence = (transaction_risk["evidence"] + top_evidence)[:10]
    level = _risk_level(score)
    return {
        "service_id": service_id,
        "risk_score": score,
        "risk_level": level,
        "risk_version": risk_version,
        "lookback_hours": lookback_hours,
        "since": format_time(since),
        "until": format_time(until),
        "baseline_version": baseline_version,
        "window_count": len(evaluations),
        "latest_window": evaluations[-1] if evaluations else None,
        "base_window_risk_score": base_score,
        "transaction_baseline_risk": transaction_risk,
        "top_evidence": top_evidence,
    }


def data_coverage(
    database_url: str,
    since: datetime,
    until: datetime,
    service_id: str | None = None,
    window_size: str = "15m",
) -> dict[str, Any]:
    step_seconds = WINDOW_SECONDS.get(window_size, 900)
    since = align_floor(since, window_size)
    until = align_floor(until, window_size)
    if until <= since:
        until = since + timedelta(seconds=step_seconds)
    service_filter = f"where service_id = {sql_literal(service_id)}" if service_id else ""
    actual_service_filter = f"and service_id = {sql_literal(service_id)}" if service_id else ""
    sql = f"""
with service_scope as (
  select service_id from services {service_filter}
),
service_count as (
  select count(*)::int as expected from service_scope
),
expected_windows as (
  select generate_series(
    {sql_literal(format_time(since))}::timestamptz,
    {sql_literal(format_time(until - timedelta(seconds=step_seconds)))}::timestamptz,
    interval '{step_seconds} seconds'
  ) as window_start
),
actual as (
  select *
  from service_metric_windows
  where window_size = {sql_literal(window_size)}
    and window_start >= {sql_literal(format_time(since))}
    and window_start < {sql_literal(format_time(until))}
    {actual_service_filter}
)
select coalesce(json_agg(row_to_json(x) order by x.window_start), '[]'::json)
from (
  select
    w.window_start,
    w.window_start + interval '{step_seconds} seconds' as window_end,
    sc.expected,
    count(a.*)::int as rows,
    count(a.*) filter (where a.newrelic->>'status' = 'collected')::int as newrelic_collected,
    count(a.*) filter (where coalesce(a.prometheus_resources->>'status', 'collected') in ('collected', 'missing'))::int as prometheus_ok,
    count(a.*) filter (where a.kubernetes->>'status' in ('collected', 'missing'))::int as kubernetes_ok,
    count(a.*) filter (where coalesce(a.data_quality->'errors', '[]'::jsonb) <> '[]'::jsonb)::int as data_quality_errors
  from expected_windows w
  cross join service_count sc
  left join actual a on a.window_start = w.window_start
  group by w.window_start, sc.expected
) x;
"""
    windows = psql_json(database_url, sql) or []
    expected_points = sum(row.get("expected") or 0 for row in windows)
    actual_points = sum(row.get("rows") or 0 for row in windows)
    complete_windows = sum(1 for row in windows if row.get("expected") and row.get("rows") >= row.get("expected"))
    return {
        "since": format_time(since),
        "until": format_time(until),
        "window_size": window_size,
        "service_id": service_id,
        "windows": windows,
        "summary": {
            "window_count": len(windows),
            "complete_windows": complete_windows,
            "expected_points": expected_points,
            "actual_points": actual_points,
            "coverage_pct": (actual_points / expected_points * 100) if expected_points else None,
        },
    }


def data_gaps(
    database_url: str,
    since: datetime,
    until: datetime,
    service_id: str | None = None,
    window_size: str = "15m",
    limit: int = 100,
) -> dict[str, Any]:
    step_seconds = WINDOW_SECONDS.get(window_size, 900)
    since = align_floor(since, window_size)
    until = align_floor(until, window_size)
    if until <= since:
        until = since + timedelta(seconds=step_seconds)
    service_filter = f"where service_id = {sql_literal(service_id)}" if service_id else ""
    actual_service_filter = f"and service_id = {sql_literal(service_id)}" if service_id else ""
    sql = f"""
with service_scope as (
  select service_id from services {service_filter}
),
expected_windows as (
  select generate_series(
    {sql_literal(format_time(since))}::timestamptz,
    {sql_literal(format_time(until - timedelta(seconds=step_seconds)))}::timestamptz,
    interval '{step_seconds} seconds'
  ) as window_start
),
actual as (
  select service_id, window_start, newrelic->>'status' as newrelic_status,
         kubernetes->>'status' as kubernetes_status,
         coalesce(data_quality->'errors', '[]'::jsonb) as data_quality_errors
  from service_metric_windows
  where window_size = {sql_literal(window_size)}
    and window_start >= {sql_literal(format_time(since))}
    and window_start < {sql_literal(format_time(until))}
    {actual_service_filter}
),
coverage as (
  select
    w.window_start,
    s.service_id,
    a.newrelic_status,
    a.kubernetes_status,
    a.data_quality_errors,
    case
      when a.service_id is null then true
      when coalesce(a.newrelic_status, '') <> 'collected' then true
      when coalesce(a.kubernetes_status, '') not in ('collected', 'missing') then true
      when coalesce(a.data_quality_errors, '[]'::jsonb) <> '[]'::jsonb then true
      else false
    end as has_gap
  from expected_windows w
  cross join service_scope s
  left join actual a on a.window_start = w.window_start and a.service_id = s.service_id
)
select coalesce(json_agg(row_to_json(x) order by x.priority, x.window_start desc), '[]'::json)
from (
  select
    window_start,
    window_start + interval '{step_seconds} seconds' as window_end,
    count(*)::int as expected,
    count(*) filter (where not has_gap)::int as healthy,
    array_agg(service_id order by service_id) filter (where has_gap) as service_ids,
    case when count(*) filter (where not has_gap) > 0 then 0 else 1 end as priority
  from coverage
  group by window_start
  having count(*) filter (where has_gap) > 0
  order by priority, window_start desc
  limit {max(1, min(limit, 500))}
) x;
"""
    gaps = psql_json(database_url, sql) or []
    return {
        "since": format_time(since),
        "until": format_time(until),
        "window_size": window_size,
        "service_id": service_id,
        "gap_count": len(gaps),
        "gaps": gaps,
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
        "risk": score_service_risk(
            database_url,
            service_id,
            lookback_hours=max(1, math.ceil((until - since).total_seconds() / 3600)),
            baseline_version=baseline_version,
            since=since,
            until=until,
        ),
        "trace_evidence": trace_evidence,
        "hypotheses": ranked,
        "windows": evaluations,
    }
