"""Baseline, anomaly, risk scoring, and incident hypothesis ranking."""

from __future__ import annotations

import math
import subprocess
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
MODEL_BASELINE_SCOPE_WEIGHTS = {
    "weekday_hour_slot": 1.00,
    "hour_slot": 0.85,
    "weekday_hour": 0.65,
    "hour": 0.50,
    "global": 0.30,
}
KUBERNETES_OK_STATUSES = ("collected", "events_only", "partial", "missing")
RISK_CALIBRATION_VERSION = "feedback-calibration-v1"


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
    window_size: str = "15m",
) -> list[dict[str, Any]]:
    where = [f"window_size = {sql_literal(window_size)}"]
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


def evaluate_window(
    row: dict[str, Any],
    baselines: dict[str, dict],
    calibration_rules: list[dict[str, Any]] | None = None,
) -> dict[str, Any]:
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
                evidence_item = {
                    "source": "rule_baseline",
                    "metric": metric_name,
                    "value": value,
                    "expected": 1.0,
                    "points": severity_points,
                }
                evidence_item = apply_risk_calibration(evidence_item, calibration_rules or [])
                evidence.append(evidence_item)
                score += evidence_item["points"] - severity_points
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
        evidence_item = {
            "source": "rule_baseline",
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
        evidence_item = apply_risk_calibration(evidence_item, calibration_rules or [])
        evidence.append(evidence_item)
        score += evidence_item["points"]

    k8s = row.get("kubernetes") or {}
    replicas = k8s.get("replicas") if isinstance(k8s.get("replicas"), dict) else {}
    if replicas and replicas.get("rollout_complete") is False:
        score += 25
        anomaly_types.add("rollout")
        evidence_item = {
            "source": "rule_baseline",
            "metric": "kubernetes.rollout_complete",
            "value": False,
            "points": 25,
        }
        evidence_item = apply_risk_calibration(evidence_item, calibration_rules or [])
        evidence.append(evidence_item)
        score += evidence_item["points"] - 25
    waiting_reasons = k8s.get("waiting_reasons") if isinstance(k8s.get("waiting_reasons"), dict) else {}
    for reason, count in waiting_reasons.items():
        if count:
            score += 30
            anomaly_types.add("kubernetes_waiting")
            evidence_item = {
                "source": "rule_baseline",
                "metric": f"kubernetes.waiting_reasons.{reason}",
                "value": count,
                "points": 30,
            }
            evidence_item = apply_risk_calibration(evidence_item, calibration_rules or [])
            evidence.append(evidence_item)
            score += evidence_item["points"] - 30

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


def load_dynamic_model_status(database_url: str, service_id: str) -> dict[str, Any]:
    sql = f"""
select row_to_json(x)
from (
  select count(*)::int as active_model_count,
         max(model_version) as model_version,
         max(model_type) as model_type,
         max(created_at) as latest_model_created_at
  from service_metric_models
  where service_id = {sql_literal(service_id)}
    and active
) x;
"""
    row = psql_json(database_url, sql) or {}
    active_count = row.get("active_model_count") or 0
    return {
        "status": "active" if active_count else "not_trained",
        "active_model_count": active_count,
        "model_version": row.get("model_version"),
        "model_type": row.get("model_type"),
        "latest_model_created_at": row.get("latest_model_created_at"),
        "residual_scoring": "enabled" if active_count else "framework_ready",
        "fallback": "risk-v2 rule baseline remains active until a dynamic model is trained and activated",
    }


def load_active_model_buckets(
    database_url: str,
    service_id: str,
    model_version: str | None = None,
    active_only: bool = True,
) -> dict[str, dict[str, dict[str, Any]]]:
    model_version_filter = f"and m.model_version = {sql_literal(model_version)}" if model_version else ""
    active_filter = "and m.active" if active_only else ""
    sql = f"""
select coalesce(json_agg(row_to_json(b) order by b.metric_name, b.baseline_scope), '[]'::json)
from (
  select m.id as model_id,
         m.metric_name,
         m.model_version,
         m.model_type,
         m.training_window_start,
         m.training_window_end,
         m.created_at as model_created_at,
         b.baseline_scope,
         b.day_of_week,
         b.hour_of_day,
         b.minute_slot,
         b.p50,
         b.p75,
         b.p90,
         b.p95,
         b.p99,
         b.median,
         b.mad,
         b.sample_count,
         b.coverage_pct,
         b.confidence
  from service_metric_models m
  join service_metric_model_buckets b on b.model_id = m.id
  where m.service_id = {sql_literal(service_id)}
    {active_filter}
    {model_version_filter}
) b;
"""
    rows = psql_json(database_url, sql) or []
    result: dict[str, dict[str, dict[str, Any]]] = {}
    for row in rows:
        metric_name = row["metric_name"]
        key = f"{row['day_of_week']}|{row['hour_of_day']}|{row.get('minute_slot')}"
        result.setdefault(metric_name, {})[key] = row
    return result


def model_bucket_candidates(
    model_buckets: dict[str, dict[str, dict[str, Any]]],
    metric_name: str,
    window_start: datetime,
) -> list[dict[str, Any]]:
    metric_buckets = model_buckets.get(metric_name) or {}
    candidates = [
        (f"{window_start.weekday()}|{window_start.hour}|{window_start.minute}", "weekday_hour_slot"),
        (f"None|{window_start.hour}|{window_start.minute}", "hour_slot"),
        (f"{window_start.weekday()}|{window_start.hour}|None", "weekday_hour"),
        (f"None|{window_start.hour}|None", "hour"),
        ("None|None|None", "global"),
    ]
    result: list[dict[str, Any]] = []
    for key, scope in candidates:
        bucket = metric_buckets.get(key)
        if bucket:
            result.append(
                {
                    **bucket,
                    "baseline_scope": scope,
                    "baseline_weight": MODEL_BASELINE_SCOPE_WEIGHTS[scope],
                }
            )
    return result


def metric_model_residual_points(metric_name: str, value: float, bucket: dict[str, Any]) -> tuple[int, str | None]:
    if metric_name in TRAFFIC_CONTEXT_METRICS:
        return 0, "traffic_context"
    median = bucket.get("median")
    mad = bucket.get("mad")
    p95 = bucket.get("p95")
    p99 = bucket.get("p99")
    robust_mad_score = None
    if isinstance(median, (int, float)) and isinstance(mad, (int, float)) and mad > 0:
        robust_mad_score = (value - median) / (mad * 1.4826)
    p95_ratio = value / p95 if isinstance(p95, (int, float)) and p95 > 0 else None
    p99_ratio = value / p99 if isinstance(p99, (int, float)) and p99 > 0 else None

    if metric_name == "kubernetes.ready_ratio":
        if value < 1.0:
            return (38 if value < 0.8 else 22), "kubernetes_ready_ratio"
        return 0, None

    points = 0
    deviation_type = None
    if isinstance(p99, (int, float)) and value > p99 and value > 0:
        points = 18 if robust_mad_score is None else 10
        deviation_type = "model_p99_residual"
    elif isinstance(p95, (int, float)) and value > p95 and value > 0:
        points = 8 if robust_mad_score is None else 4
        deviation_type = "model_p95_residual"

    if robust_mad_score is not None:
        if robust_mad_score >= 8:
            points = max(points, 34)
            deviation_type = "model_mad_critical"
        elif robust_mad_score >= 5:
            points = max(points, 24)
            deviation_type = "model_mad_high"
        elif robust_mad_score >= 3:
            points = max(points, 14)
            deviation_type = "model_mad_medium"

    if metric_name.startswith("newrelic.error_rate") and value > 0 and points:
        points += 8
    elif "latency" in metric_name and points:
        if robust_mad_score is not None and robust_mad_score < 3 and (p99_ratio or p95_ratio or 0) < 1.25:
            points = min(points, 6)
        else:
            points += 3
    elif metric_name.startswith("prometheus.") and points:
        if robust_mad_score is not None and robust_mad_score < 3 and (p99_ratio or p95_ratio or 0) < 1.20:
            points = min(points, 6)
    elif metric_name.startswith("kubernetes.") and points:
        points += 8

    return min(points, 50), deviation_type


def metric_model_residual_comparison(metric_name: str, value: float, bucket: dict[str, Any]) -> dict[str, Any]:
    raw_points, deviation_type = metric_model_residual_points(metric_name, value, bucket)
    weight = float(bucket.get("baseline_weight") or 1.0)
    weighted_points = round(raw_points * weight)
    median = bucket.get("median")
    mad = bucket.get("mad")
    residual = value - median if isinstance(median, (int, float)) else None
    residual_ratio = (
        value / median
        if isinstance(median, (int, float)) and median > 0
        else None
    )
    robust_mad_score = (
        residual / (mad * 1.4826)
        if isinstance(residual, (int, float)) and isinstance(mad, (int, float)) and mad > 0
        else None
    )
    p95 = bucket.get("p95")
    p99 = bucket.get("p99")
    return {
        "model_id": bucket.get("model_id"),
        "model_version": bucket.get("model_version"),
        "model_type": bucket.get("model_type"),
        "baseline_scope": bucket.get("baseline_scope"),
        "baseline_weight": weight,
        "sample_count": bucket.get("sample_count"),
        "coverage_pct": bucket.get("coverage_pct"),
        "confidence": bucket.get("confidence"),
        "p50": bucket.get("p50"),
        "p95": p95,
        "p99": p99,
        "median": median,
        "mad": mad,
        "residual": residual,
        "residual_ratio": residual_ratio,
        "p95_deviation": value - p95 if isinstance(p95, (int, float)) else None,
        "p99_deviation": value - p99 if isinstance(p99, (int, float)) else None,
        "robust_mad_score": robust_mad_score,
        "deviation_type": deviation_type,
        "raw_points": raw_points,
        "weighted_points": weighted_points,
    }


def _rule_applies(rule: dict[str, Any], evidence: dict[str, Any]) -> bool:
    metric_name = rule.get("metric_name")
    evidence_source = rule.get("evidence_source")
    if metric_name and metric_name != evidence.get("metric"):
        return False
    if evidence_source and evidence_source != evidence.get("source"):
        return False
    return True


def apply_risk_calibration(evidence: dict[str, Any], rules: list[dict[str, Any]]) -> dict[str, Any]:
    if not rules or not evidence.get("points"):
        return evidence
    original_points = int(evidence["points"])
    calibrated_points = float(original_points)
    applied: list[dict[str, Any]] = []
    for rule in rules:
        if not _rule_applies(rule, evidence):
            continue
        multiplier = float(rule.get("weight_multiplier") or 1.0)
        delta = float(rule.get("points_delta") or 0.0)
        calibrated_points = calibrated_points * multiplier + delta
        applied.append(
            {
                "rule_id": rule.get("id"),
                "metric_name": rule.get("metric_name"),
                "evidence_source": rule.get("evidence_source"),
                "weight_multiplier": multiplier,
                "points_delta": delta,
                "reason": rule.get("reason"),
            }
        )
    if not applied:
        return evidence
    new_points = max(0, min(100, round(calibrated_points)))
    return {
        **evidence,
        "points": new_points,
        "raw_points_before_calibration": original_points,
        "calibration_adjustment": new_points - original_points,
        "calibration_rules": applied,
    }


def load_risk_calibration_rules(
    database_url: str,
    service_id: str,
    risk_version: str = "risk-v2",
    model_version: str | None = None,
) -> list[dict[str, Any]]:
    model_filter = ""
    if model_version:
        model_filter = f"and (model_version is null or model_version = {sql_literal(model_version)})"
    sql = f"""
select coalesce(json_agg(row_to_json(r) order by specificity desc, created_at desc), '[]'::json)
from (
  select id, service_id, metric_name, evidence_source, risk_version, model_version,
         weight_multiplier::float as weight_multiplier,
         points_delta::float as points_delta,
         enabled, generated_by, reason, stats, created_at,
         ((case when metric_name is null then 0 else 1 end) +
          (case when evidence_source is null then 0 else 1 end)) as specificity
  from risk_calibration_rules
  where enabled
    and service_id = {sql_literal(service_id)}
    and risk_version = {sql_literal(risk_version)}
    {model_filter}
) r;
"""
    try:
        return psql_json(database_url, sql) or []
    except subprocess.CalledProcessError:
        return []


def _feedback_evidence_items(payload: dict[str, Any]) -> list[dict[str, Any]]:
    candidates = payload.get("top_evidence") or payload.get("evidence") or []
    if not isinstance(candidates, list):
        return []
    items: list[dict[str, Any]] = []
    for item in candidates:
        if isinstance(item, dict):
            items.append(
                {
                    "metric_name": item.get("metric"),
                    "evidence_source": item.get("source"),
                    "evidence_type": item.get("deviation_type") or item.get("type"),
                }
            )
    return items


def list_risk_feedback_labels(
    database_url: str,
    service_id: str | None = None,
    label_type: str | None = None,
    limit: int = 100,
) -> list[dict[str, Any]]:
    where = ["true"]
    if service_id:
        where.append(f"service_id = {sql_literal(service_id)}")
    if label_type:
        where.append(f"label_type = {sql_literal(label_type)}")
    sql = f"""
select coalesce(json_agg(row_to_json(f) order by created_at desc), '[]'::json)
from (
  select id, service_id, window_start, window_end, risk_version, model_version,
         label_type, actual_severity, false_positive, false_negative, payload,
         created_at
  from risk_feedback_labels
  where {' and '.join(where)}
  order by created_at desc
  limit {max(1, min(limit, 5000))}
) f;
"""
    return psql_json(database_url, sql) or []


def risk_feedback_report(database_url: str, service_id: str | None = None, days: int = 30) -> dict[str, Any]:
    service_filter = f"and service_id = {sql_literal(service_id)}" if service_id else ""
    sql = f"""
select coalesce(json_agg(row_to_json(r) order by r.service_id), '[]'::json)
from (
  select service_id,
         count(*)::int as total_labels,
         count(*) filter (where label_type = 'confirmed_incident')::int as confirmed_incident_count,
         count(*) filter (where label_type = 'false_positive' or false_positive)::int as false_positive_count,
         count(*) filter (where label_type = 'false_negative' or false_negative)::int as false_negative_count,
         min(created_at) as first_label_at,
         max(created_at) as last_label_at
  from risk_feedback_labels
  where created_at >= now() - interval '{max(1, min(days, 365))} days'
    {service_filter}
  group by service_id
) r;
"""
    services = psql_json(database_url, sql) or []
    totals = {
        "total_labels": sum(item["total_labels"] for item in services),
        "confirmed_incident_count": sum(item["confirmed_incident_count"] for item in services),
        "false_positive_count": sum(item["false_positive_count"] for item in services),
        "false_negative_count": sum(item["false_negative_count"] for item in services),
    }
    return {
        "days": days,
        "service_id": service_id,
        **totals,
        "services": services,
    }


def list_risk_calibration_rules(
    database_url: str,
    service_id: str | None = None,
    enabled: bool | None = None,
    limit: int = 100,
) -> list[dict[str, Any]]:
    where = ["true"]
    if service_id:
        where.append(f"service_id = {sql_literal(service_id)}")
    if enabled is not None:
        where.append(f"enabled = {sql_literal(enabled)}")
    sql = f"""
select coalesce(json_agg(row_to_json(r) order by created_at desc), '[]'::json)
from (
  select id, service_id, metric_name, evidence_source, risk_version, model_version,
         weight_multiplier::float as weight_multiplier,
         points_delta::float as points_delta,
         enabled, generated_by, reason, stats, created_at
  from risk_calibration_rules
  where {' and '.join(where)}
  order by created_at desc
  limit {max(1, min(limit, 500))}
) r;
"""
    return psql_json(database_url, sql) or []


def _calibration_values(stats: dict[str, int]) -> tuple[float, float, str]:
    false_positives = stats.get("false_positive_count", 0)
    false_negatives = stats.get("false_negative_count", 0)
    confirmed = stats.get("confirmed_incident_count", 0)
    total = max(1, stats.get("total_labels", 0))
    if false_positives > (false_negatives + confirmed):
        ratio = false_positives / total
        multiplier = max(0.65, 1.0 - min(0.35, ratio * 0.30))
        delta = -3.0 if false_positives >= 2 else -1.0
        return multiplier, delta, "false positives dominate recent feedback; dampen matching risk evidence"
    if false_negatives:
        ratio = false_negatives / total
        multiplier = min(1.50, 1.0 + min(0.40, ratio * 0.45))
        delta = 5.0 if false_negatives >= 2 else 2.0
        return multiplier, delta, "false negatives observed; amplify matching risk evidence"
    if confirmed:
        ratio = confirmed / total
        multiplier = min(1.30, 1.0 + min(0.20, ratio * 0.25))
        return multiplier, 1.0, "confirmed incidents validate this risk evidence"
    return 1.0, 0.0, "insufficient directional feedback"


def generate_risk_calibration_rules(
    database_url: str,
    service_id: str | None = None,
    days: int = 30,
    min_labels: int = 2,
    activate: bool = True,
    risk_version: str = "risk-v2",
    model_version: str | None = None,
) -> dict[str, Any]:
    labels = list_risk_feedback_labels(database_url, service_id=service_id, limit=5000)
    cutoff = utc_now() - timedelta(days=max(1, min(days, 365)))
    groups: dict[tuple[str, str | None, str | None], dict[str, Any]] = {}
    for label in labels:
        created_at = parse_time(label["created_at"])
        if created_at < cutoff:
            continue
        label_service_id = label["service_id"]
        payload = label.get("payload") or {}
        evidence_items = _feedback_evidence_items(payload)
        if not evidence_items:
            evidence_items = [{"metric_name": None, "evidence_source": None}]
        for item in evidence_items:
            key = (label_service_id, item.get("metric_name"), item.get("evidence_source"))
            stats = groups.setdefault(
                key,
                {
                    "service_id": label_service_id,
                    "metric_name": item.get("metric_name"),
                    "evidence_source": item.get("evidence_source"),
                    "total_labels": 0,
                    "confirmed_incident_count": 0,
                    "false_positive_count": 0,
                    "false_negative_count": 0,
                    "label_ids": [],
                },
            )
            stats["total_labels"] += 1
            stats["label_ids"].append(label["id"])
            if label.get("label_type") == "confirmed_incident":
                stats["confirmed_incident_count"] += 1
            if label.get("label_type") == "false_positive" or label.get("false_positive"):
                stats["false_positive_count"] += 1
            if label.get("label_type") == "false_negative" or label.get("false_negative"):
                stats["false_negative_count"] += 1

    selected = [stats for stats in groups.values() if stats["total_labels"] >= max(1, min_labels)]
    statements = ["begin;"]
    filter_sql = f"service_id = {sql_literal(service_id)}" if service_id else "true"
    statements.append(
        f"""
update risk_calibration_rules
set enabled = false
where generated_by = {sql_literal(RISK_CALIBRATION_VERSION)}
  and risk_version = {sql_literal(risk_version)}
  and {filter_sql};
"""
    )
    created_rules: list[dict[str, Any]] = []
    for stats in selected:
        multiplier, delta, reason = _calibration_values(stats)
        if multiplier == 1.0 and delta == 0.0:
            continue
        created_rules.append(
            {
                "service_id": stats["service_id"],
                "metric_name": stats["metric_name"],
                "evidence_source": stats["evidence_source"],
                "risk_version": risk_version,
                "model_version": model_version,
                "weight_multiplier": multiplier,
                "points_delta": delta,
                "enabled": activate,
                "generated_by": RISK_CALIBRATION_VERSION,
                "reason": reason,
                "stats": stats,
            }
        )
        statements.append(
            """
insert into risk_calibration_rules (
  service_id, metric_name, evidence_source, risk_version, model_version,
  weight_multiplier, points_delta, enabled, generated_by, reason, stats
) values (
  {service_id}, {metric_name}, {evidence_source}, {risk_version}, {model_version},
  {weight_multiplier}, {points_delta}, {enabled}, {generated_by}, {reason}, {stats}::jsonb
);
""".format(
                service_id=sql_literal(stats["service_id"]),
                metric_name=sql_literal(stats["metric_name"]),
                evidence_source=sql_literal(stats["evidence_source"]),
                risk_version=sql_literal(risk_version),
                model_version=sql_literal(model_version),
                weight_multiplier=sql_literal(multiplier),
                points_delta=sql_literal(delta),
                enabled=sql_literal(activate),
                generated_by=sql_literal(RISK_CALIBRATION_VERSION),
                reason=sql_literal(reason),
                stats=sql_literal(stats),
            )
        )
    statements.append("commit;")
    psql_exec(database_url, "\n".join(statements))
    return {
        "status": "succeeded",
        "days": days,
        "min_labels": min_labels,
        "activate": activate,
        "risk_version": risk_version,
        "model_version": model_version,
        "eligible_groups": len(selected),
        "created_rule_count": len(created_rules),
        "created_rules": created_rules,
    }


def evaluate_window_dynamic_model(
    row: dict[str, Any],
    model_buckets: dict[str, dict[str, dict[str, Any]]],
    calibration_rules: list[dict[str, Any]] | None = None,
) -> dict[str, Any]:
    window_start = parse_time(row["window_start"])
    metrics = extract_metrics(row)
    evidence: list[dict[str, Any]] = []
    evaluated_metric_count = 0
    score = 0

    for metric_name, value in metrics.items():
        candidates = model_bucket_candidates(model_buckets, metric_name, window_start)
        if not candidates:
            continue
        evaluated_metric_count += 1
        comparisons = [metric_model_residual_comparison(metric_name, value, bucket) for bucket in candidates]
        selected = max(comparisons, key=lambda item: item["weighted_points"])
        points = selected["weighted_points"]
        if points <= 0:
            continue
        evidence_item = {
            "source": "ml_dynamic_baseline",
            "metric": metric_name,
            "value": value,
            "points": points,
            "model_version": selected["model_version"],
            "model_type": selected["model_type"],
            "baseline_scope": selected["baseline_scope"],
            "baseline_weight": selected["baseline_weight"],
            "baseline_sample_count": selected["sample_count"],
            "coverage_pct": selected["coverage_pct"],
            "confidence": selected["confidence"],
            "model_p50": selected["p50"],
            "model_p95": selected["p95"],
            "model_p99": selected["p99"],
            "median": selected["median"],
            "mad": selected["mad"],
            "residual": selected["residual"],
            "residual_ratio": selected["residual_ratio"],
            "p95_deviation": selected["p95_deviation"],
            "p99_deviation": selected["p99_deviation"],
            "robust_mad_score": selected["robust_mad_score"],
            "deviation_type": selected["deviation_type"],
            "raw_points": selected["raw_points"],
            "model_comparisons": comparisons,
        }
        evidence_item = apply_risk_calibration(evidence_item, calibration_rules or [])
        evidence.append(evidence_item)
        score += evidence_item["points"]

    top_points = [item["points"] for item in sorted(evidence, key=lambda item: item["points"], reverse=True)[:3]]
    return {
        "service_id": row["service_id"],
        "window_start": row["window_start"],
        "window_end": row["window_end"],
        "window_size": row["window_size"],
        "score": min(sum(top_points), 70),
        "evaluated_metric_count": evaluated_metric_count,
        "evidence": sorted(evidence, key=lambda item: item["points"], reverse=True)[:20],
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
    model_buckets = load_active_model_buckets(database_url, service_id)
    dynamic_model_status = load_dynamic_model_status(database_url, service_id)
    active_model_version = dynamic_model_status.get("model_version")
    calibration_rules = load_risk_calibration_rules(
        database_url,
        service_id,
        risk_version=risk_version,
        model_version=active_model_version,
    )
    rows = load_metric_windows(database_url, service_id=service_id, since=since, until=until)
    evaluations = [evaluate_window(row, baselines, calibration_rules=calibration_rules) for row in rows]
    base_score, _ = risk_from_scores([item["score"] for item in evaluations])
    model_evaluations = (
        [evaluate_window_dynamic_model(row, model_buckets, calibration_rules=calibration_rules) for row in rows]
        if model_buckets
        else []
    )
    model_score, _ = risk_from_scores([item["score"] for item in model_evaluations])
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
    dynamic_model_evidence: list[dict[str, Any]] = []
    for item in sorted(model_evaluations, key=lambda row: row["score"], reverse=True):
        for evidence in item["evidence"]:
            evidence = {**evidence, "window_start": item["window_start"]}
            dynamic_model_evidence.append(evidence)
            if len(dynamic_model_evidence) >= 10:
                break
        if len(dynamic_model_evidence) >= 10:
            break
    dynamic_baseline_risk = {
        "score": model_score,
        "status": "active" if model_buckets else "not_trained",
        "active_model_count": dynamic_model_status.get("active_model_count") or 0,
        "evaluated_window_count": len(model_evaluations),
        "evaluated_metric_count": sum(item["evaluated_metric_count"] for item in model_evaluations),
        "calibration_rule_count": len(calibration_rules),
        "evidence": dynamic_model_evidence,
    }
    if model_score:
        score = min(100, max(score, model_score, round(base_score * 0.6 + model_score * 0.7)))
        top_evidence = (dynamic_model_evidence + top_evidence)[:10]
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
        "dynamic_baseline_model": dynamic_model_status,
        "dynamic_baseline_risk": dynamic_baseline_risk,
        "risk_calibration": {
            "status": "active" if calibration_rules else "no_rules",
            "rule_count": len(calibration_rules),
            "rules": calibration_rules,
        },
        "top_evidence": top_evidence,
    }


def risk_feedback_candidates(
    database_url: str,
    service_ids: list[str] | None = None,
    lookback_hours: int = 6,
    min_score: int = 50,
    limit: int = 20,
    baseline_version: str = BASELINE_VERSION,
) -> dict[str, Any]:
    selected_services = service_ids or list_service_ids(database_url)
    candidates: list[dict[str, Any]] = []
    errors: list[dict[str, str]] = []
    for service_id in selected_services:
        try:
            risk = score_service_risk(
                database_url,
                service_id,
                lookback_hours=lookback_hours,
                baseline_version=baseline_version,
            )
        except Exception as exc:  # noqa: BLE001 - candidate generation should keep scanning services.
            errors.append({"service_id": service_id, "error": str(exc)})
            continue
        if risk["risk_score"] < min_score:
            continue
        evidence = [
            {
                "source": item.get("source"),
                "metric": item.get("metric"),
                "points": item.get("points"),
                "deviation_type": item.get("deviation_type"),
                "window_start": item.get("window_start"),
            }
            for item in risk.get("top_evidence", [])[:5]
        ]
        candidates.append(
            {
                "service_id": service_id,
                "risk_score": risk["risk_score"],
                "risk_level": risk["risk_level"],
                "risk_version": risk["risk_version"],
                "model_version": (risk.get("dynamic_baseline_model") or {}).get("model_version"),
                "since": risk["since"],
                "until": risk["until"],
                "top_evidence": evidence,
                "feedback_payload_template": {
                    "service_id": service_id,
                    "window_start": risk["since"],
                    "window_end": risk["until"],
                    "risk_version": risk["risk_version"],
                    "model_version": (risk.get("dynamic_baseline_model") or {}).get("model_version"),
                    "label_type": "false_positive | false_negative | confirmed_incident",
                    "actual_severity": "normal | low | medium | high | critical",
                    "payload": {
                        "review_status": "needs_human_review",
                        "risk_score": risk["risk_score"],
                        "risk_level": risk["risk_level"],
                        "top_evidence": evidence,
                    },
                },
            }
        )
    candidates = sorted(candidates, key=lambda item: item["risk_score"], reverse=True)
    return {
        "lookback_hours": lookback_hours,
        "min_score": min_score,
        "candidate_count": len(candidates),
        "returned_count": min(len(candidates), max(1, limit)),
        "candidates": candidates[: max(1, min(limit, 200))],
        "errors": errors,
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
    count(a.*) filter (where a.kubernetes->>'status' in {KUBERNETES_OK_STATUSES})::int as kubernetes_ok,
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
      when coalesce(a.kubernetes_status, '') not in {KUBERNETES_OK_STATUSES} then true
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
