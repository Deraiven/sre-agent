"""SLO recommendation generation and query helpers."""

from __future__ import annotations

import math
from datetime import datetime, timezone
from typing import Any

from .db import psql_exec, psql_json, sql_literal, sql_text


DEFAULT_SLO_RECOMMENDATION_VERSION = "slo-rec-v1"
WINDOW_INTERVALS = {"5m": "5 minutes", "15m": "15 minutes", "1h": "1 hour"}
PERIODIC_ERROR_PATTERN_SLO_SERVICES = {
    "auth-api",
    "backoffice-v2-bff",
    "beep-v1-web",
    "otp-api",
}
LOCALIZED_SPIKE_REVIEW_SERVICES = {
    "e-invoice-adapter-svc",
}
EDGE_JOB_SERVICES = {
    "backoffice-migrate-jobs",
    "core-event-consumer-zendesk",
}
LOW_TRAFFIC_PROVISIONAL_SLO_SERVICES = {
    "3p-webhook-adapter-infra-svc",
    "core-event-consumer-payment",
}
HISTORICAL_AVAILABILITY_ROADMAP_REASON = "historical_availability_below_99_5_reliability_roadmap_recommended"


def parse_window_interval(window_size: str) -> str:
    if window_size not in WINDOW_INTERVALS:
        raise ValueError(f"unsupported window_size: {window_size}")
    return WINDOW_INTERVALS[window_size]


def nice_ceiling(value: float | None, minimum: int = 100) -> int | None:
    if value is None or not math.isfinite(value) or value <= 0:
        return None
    if value <= 1000:
        step = 50
    elif value <= 3000:
        step = 100
    elif value <= 10000:
        step = 500
    else:
        step = 1000
    return max(minimum, int(math.ceil(value / step) * step))


def service_type(tags: list[str], service_id: str) -> str:
    tag_set = set(tags or [])
    if tag_set & {"sse", "streaming", "realtime"} or "sse" in service_id or "realtime-event" in service_id:
        return "streaming"
    if tag_set & {"job", "scheduled"} or "job" in service_id:
        return "job"
    if "consumer" in tag_set or "consumer" in service_id:
        return "consumer"
    if "bff" in tag_set or service_id.endswith("-bff"):
        return "bff"
    if "web" in tag_set or service_id.endswith("-web"):
        return "web"
    if "api" in tag_set or service_id.endswith("-api"):
        return "api"
    if tag_set & {"adapter", "infrastructure"}:
        return "adapter"
    return "service"


def availability_target(availability: float | None, total_requests: float, review_required: list[str]) -> tuple[float, str]:
    if not availability or total_requests <= 0:
        review_required.append("insufficient_request_volume_for_availability_slo")
        return 99.0, "needs_data"
    if total_requests < 1000:
        review_required.append("low_traffic_service")
        return 99.0, "low_traffic_candidate"
    if availability >= 99.95:
        return 99.9, "slo_candidate"
    if availability >= 99.8:
        return 99.5, "slo_candidate"
    if availability >= 99.5:
        review_required.append("historical_availability_below_99_8")
        return 99.5, "slo_candidate"
    review_required.append(HISTORICAL_AVAILABILITY_ROADMAP_REASON)
    return 99.0, "reliability_roadmap"


def confidence(row: dict[str, Any], review_required: list[str], kind: str) -> str:
    coverage = float(row.get("coverage_pct") or 0)
    newrelic_coverage = float(row.get("newrelic_coverage_pct") or 0)
    latency_coverage = float(row.get("latency_coverage_pct") or 0)
    rows = int(row.get("rows") or 0)
    if kind in {"job", "consumer", "streaming"}:
        return "medium" if coverage >= 90 and newrelic_coverage >= 90 else "low"
    if coverage >= 95 and newrelic_coverage >= 95 and latency_coverage >= 80 and rows >= 14 * 96 and not review_required:
        return "high"
    if coverage >= 90 and newrelic_coverage >= 90 and rows >= 7 * 96:
        return "medium"
    return "low"


def load_slo_candidates(
    database_url: str,
    days: int = 30,
    window_size: str = "15m",
    service_ids: list[str] | None = None,
    baseline_version: str = "baseline-v1",
) -> list[dict[str, Any]]:
    interval = parse_window_interval(window_size)
    service_filter = ""
    if service_ids:
        service_filter = "where s.service_id = any(array[{items}])".format(
            items=", ".join(sql_literal(service_id) for service_id in service_ids)
        )
    sql = f"""
with params as (
  select
    date_trunc('minute', now()) - interval '{int(days)} days' as since,
    date_trunc('minute', now()) as until
),
service_scope as (
  select s.service_id, s.description, s.owner, s.tags
  from services s
  {service_filter}
),
expected as (
  select count(*)::int as expected_windows
  from params, generate_series(params.since, params.until - interval '{interval}', interval '{interval}') gs
),
window_data as (
  select
    w.service_id,
    w.window_start,
    nullif(w.newrelic->>'request_count', '')::double precision as request_count,
    nullif(w.newrelic->>'rpm', '')::double precision as rpm,
    nullif(w.newrelic->>'latency_p95_ms', '')::double precision as latency_p95_ms,
    nullif(w.newrelic->>'latency_p99_ms', '')::double precision as latency_p99_ms,
    coalesce(nullif(w.newrelic->>'status', ''), 'missing') as newrelic_status,
    case
      when nullif(w.newrelic->>'error_rate_percent', '') is null
       and nullif(w.newrelic->>'request_count', '')::double precision = 0 then 0
      else nullif(w.newrelic->>'error_rate_percent', '')::double precision
    end as error_rate_percent,
    coalesce(w.data_quality->'errors', '[]'::jsonb) as data_quality_errors
  from service_metric_windows w, params
  where w.window_size = {sql_literal(window_size)}
    and w.window_start >= params.since
    and w.window_start < params.until
),
aggregated as (
  select
    service_id,
    count(*)::int as rows,
    count(*) filter (where data_quality_errors <> '[]'::jsonb)::int as data_quality_error_rows,
    count(*) filter (where newrelic_status = 'collected')::int as newrelic_collected_rows,
    count(*) filter (where newrelic_status <> 'collected')::int as newrelic_error_rows,
    count(*) filter (where latency_p95_ms is not null)::int as latency_sample_count,
    count(*) filter (where request_count is not null)::int as request_sample_count,
    coalesce(sum(request_count), 0)::double precision as total_requests,
    avg(rpm)::double precision as avg_rpm,
    percentile_cont(0.95) within group (order by rpm)::double precision as peak_rpm_p95,
    percentile_cont(0.50) within group (order by latency_p95_ms)::double precision as latency_p95_p50,
    percentile_cont(0.90) within group (order by latency_p95_ms)::double precision as latency_p95_p90,
    percentile_cont(0.95) within group (order by latency_p95_ms)::double precision as latency_p95_p95,
    percentile_cont(0.99) within group (order by latency_p95_ms)::double precision as latency_p95_p99,
    percentile_cont(0.50) within group (order by latency_p99_ms)::double precision as latency_p99_p50,
    percentile_cont(0.90) within group (order by latency_p99_ms)::double precision as latency_p99_p90,
    percentile_cont(0.95) within group (order by latency_p99_ms)::double precision as latency_p99_p95,
    percentile_cont(0.99) within group (order by latency_p99_ms)::double precision as latency_p99_p99,
    percentile_cont(0.50) within group (order by error_rate_percent)::double precision as error_rate_p50,
    percentile_cont(0.90) within group (order by error_rate_percent)::double precision as error_rate_p90,
    percentile_cont(0.95) within group (order by error_rate_percent)::double precision as error_rate_p95,
    percentile_cont(0.99) within group (order by error_rate_percent)::double precision as error_rate_p99,
    case
      when coalesce(sum(request_count), 0) > 0 then
        100 - coalesce(sum(request_count * coalesce(error_rate_percent, 0) / 100), 0) / nullif(sum(request_count), 0) * 100
      else null
    end::double precision as availability_percent
  from window_data
  group by service_id
),
baseline_summary as (
  select
    service_id,
    count(*)::int as baseline_rows,
    count(*) filter (where metric_name = 'newrelic.latency_p95_ms')::int as latency_p95_baseline_rows,
    count(*) filter (where metric_name = 'newrelic.latency_p99_ms')::int as latency_p99_baseline_rows
  from service_baselines
  where baseline_version = {sql_literal(baseline_version)}
  group by service_id
)
select coalesce(json_agg(row_to_json(x) order by x.service_id), '[]'::json)
from (
  select
    s.service_id,
    s.description,
    s.owner,
    s.tags,
    coalesce(a.rows, 0) as rows,
    e.expected_windows,
    round(coalesce(a.rows, 0)::numeric / nullif(e.expected_windows, 0) * 100, 2)::double precision as coverage_pct,
    coalesce(a.data_quality_error_rows, 0) as data_quality_error_rows,
    coalesce(a.newrelic_collected_rows, 0) as newrelic_collected_rows,
    coalesce(a.newrelic_error_rows, 0) as newrelic_error_rows,
    round(coalesce(a.newrelic_collected_rows, 0)::numeric / nullif(e.expected_windows, 0) * 100, 2)::double precision as newrelic_coverage_pct,
    coalesce(a.latency_sample_count, 0) as latency_sample_count,
    round(coalesce(a.latency_sample_count, 0)::numeric / nullif(e.expected_windows, 0) * 100, 2)::double precision as latency_coverage_pct,
    coalesce(a.request_sample_count, 0) as request_sample_count,
    round(coalesce(a.request_sample_count, 0)::numeric / nullif(e.expected_windows, 0) * 100, 2)::double precision as request_coverage_pct,
    coalesce(a.total_requests, 0) as total_requests,
    a.avg_rpm,
    a.peak_rpm_p95,
    a.latency_p95_p50,
    a.latency_p95_p90,
    a.latency_p95_p95,
    a.latency_p95_p99,
    a.latency_p99_p50,
    a.latency_p99_p90,
    a.latency_p99_p95,
    a.latency_p99_p99,
    a.error_rate_p50,
    a.error_rate_p90,
    a.error_rate_p95,
    a.error_rate_p99,
    a.availability_percent,
    coalesce(b.baseline_rows, 0) as service_baseline_rows,
    coalesce(b.latency_p95_baseline_rows, 0) as latency_p95_baseline_rows,
    coalesce(b.latency_p99_baseline_rows, 0) as latency_p99_baseline_rows
  from service_scope s
  cross join expected e
  left join aggregated a on a.service_id = s.service_id
  left join baseline_summary b on b.service_id = s.service_id
) x;
"""
    return psql_json(database_url, sql) or []


def build_slo_recommendation(row: dict[str, Any], days: int, window_size: str, baseline_version: str) -> dict[str, Any]:
    service_id = row["service_id"]
    kind = service_type(row.get("tags") or [], row["service_id"])
    review_required: list[str] = []
    policy_notes: list[str] = []
    if float(row.get("coverage_pct") or 0) < 95:
        review_required.append("coverage_below_95_percent")
    if float(row.get("newrelic_coverage_pct") or 0) < 95:
        review_required.append("newrelic_coverage_below_95_percent")
    if float(row.get("request_coverage_pct") or 0) < 95:
        review_required.append("request_count_coverage_below_95_percent")
    if int(row.get("service_baseline_rows") or 0) == 0:
        review_required.append("service_baseline_missing")
    if kind in {"job", "consumer"}:
        review_required.append(f"{kind}_slo_requires_domain_specific_success_or_freshness_review")
    if kind == "streaming":
        review_required.append("streaming_slo_requires_connection_or_delivery_sli_review")

    latency_p95_target = nice_ceiling((row.get("latency_p95_p95") or 0) * 1.10)
    latency_p99_target = nice_ceiling((row.get("latency_p99_p95") or 0) * 1.15, minimum=200)
    availability = row.get("availability_percent")
    availability_slo, target_type = availability_target(availability, float(row.get("total_requests") or 0), review_required)
    if service_id in LOW_TRAFFIC_PROVISIONAL_SLO_SERVICES and target_type == "low_traffic_candidate":
        target_type = "provisional_slo_candidate"
        if "low_traffic_service" in review_required:
            review_required.remove("low_traffic_service")
        policy_notes.append("low_traffic_service_accepted_as_provisional_slo_until_custom_metrics_exist")
    if service_id in EDGE_JOB_SERVICES and target_type == "needs_data":
        target_type = "edge_service"
        policy_notes.append("edge_job_service_requires_domain_specific_sli_not_http_slo")
    if service_id in PERIODIC_ERROR_PATTERN_SLO_SERVICES and target_type == "reliability_roadmap":
        target_type = "slo_candidate"
        if HISTORICAL_AVAILABILITY_ROADMAP_REASON in review_required:
            review_required.remove(HISTORICAL_AVAILABILITY_ROADMAP_REASON)
        policy_notes.append("periodic_or_business_error_pattern_accepted_as_initial_slo_candidate")
    if service_id in LOCALIZED_SPIKE_REVIEW_SERVICES:
        if "localized_error_spike_requires_review" not in review_required:
            review_required.append("localized_error_spike_requires_review")
        policy_notes.append("localized_error_spike_excluded_from_auto_slo_acceptance")
    error_rate_target = round(max(0.1, 100 - availability_slo), 3)

    if latency_p95_target is None:
        review_required.append("insufficient_latency_samples")
    if latency_p99_target is None:
        review_required.append("insufficient_p99_latency_samples")

    user_facing_latency = kind not in {"job", "consumer", "streaming"} and latency_p95_target is not None and latency_p99_target is not None
    historical_baseline = {
        "window_days": days,
        "window_size": window_size,
        "baseline_version": baseline_version,
        "coverage_pct": row.get("coverage_pct"),
        "rows": row.get("rows"),
        "expected_windows": row.get("expected_windows"),
        "data_quality_error_rows": row.get("data_quality_error_rows"),
        "newrelic_collected_rows": row.get("newrelic_collected_rows"),
        "newrelic_error_rows": row.get("newrelic_error_rows"),
        "newrelic_coverage_pct": row.get("newrelic_coverage_pct"),
        "service_baselines": {
            "rows": row.get("service_baseline_rows"),
            "latency_p95_rows": row.get("latency_p95_baseline_rows"),
            "latency_p99_rows": row.get("latency_p99_baseline_rows"),
        },
        "traffic_profile": {
            "total_requests": row.get("total_requests"),
            "avg_rpm": row.get("avg_rpm"),
            "peak_rpm_p95": row.get("peak_rpm_p95"),
        },
        "availability_percent": availability,
        "latency_p95_ms": {
            "p50": row.get("latency_p95_p50"),
            "p90": row.get("latency_p95_p90"),
            "p95": row.get("latency_p95_p95"),
            "p99": row.get("latency_p95_p99"),
            "sample_count": row.get("latency_sample_count"),
            "sample_coverage_pct": row.get("latency_coverage_pct"),
        },
        "latency_p99_ms": {
            "p50": row.get("latency_p99_p50"),
            "p90": row.get("latency_p99_p90"),
            "p95": row.get("latency_p99_p95"),
            "p99": row.get("latency_p99_p99"),
            "sample_count": row.get("latency_sample_count"),
            "sample_coverage_pct": row.get("latency_coverage_pct"),
        },
        "error_rate_percent": {
            "p50": row.get("error_rate_p50"),
            "p90": row.get("error_rate_p90"),
            "p95": row.get("error_rate_p95"),
            "p99": row.get("error_rate_p99"),
        },
    }
    recommended_slo = {
        "service_type": kind,
        "availability_target": availability_slo,
        "error_rate_percent": error_rate_target,
        "target_type": target_type,
        "slo_candidate": target_type == "slo_candidate",
        "source": "historical_recommendation",
        "reviewed": False,
    }
    if policy_notes:
        recommended_slo["policy_notes"] = policy_notes
    if user_facing_latency:
        recommended_slo["latency_p95_ms"] = latency_p95_target
        recommended_slo["latency_p99_ms"] = latency_p99_target
    else:
        recommended_slo["latency_slo_status"] = (
            "domain_specific_sli_required" if kind in {"job", "consumer", "streaming"} else "insufficient_latency_samples"
        )
    evidence = [
        {
            "type": "coverage",
            "message": f"{days}d coverage is {row.get('coverage_pct')}% ({row.get('rows')}/{row.get('expected_windows')} windows)",
        },
        {
            "type": "newrelic_coverage",
            "message": f"New Relic coverage is {row.get('newrelic_coverage_pct')}% ({row.get('newrelic_collected_rows')}/{row.get('expected_windows')} windows)",
        },
        {
            "type": "availability",
            "message": f"request-weighted availability is {round(availability, 4) if isinstance(availability, (int, float)) else None}%",
        },
        {
            "type": "service_baseline",
            "message": f"{baseline_version} has {row.get('service_baseline_rows')} baseline rows for this service",
        },
    ]
    if user_facing_latency:
        evidence.append(
            {
                "type": "latency",
                "message": f"recommended p95/p99 latency targets are {latency_p95_target}ms/{latency_p99_target}ms from historical p95-of-window latency with headroom",
            }
        )
    else:
        evidence.append(
            {
                "type": "latency",
                "message": "latency SLO not recommended for this service without sufficient user-facing latency samples or a domain-specific SLI review",
            }
        )
    if review_required:
        evidence.append({"type": "review_required", "items": sorted(set(review_required))})
    if policy_notes:
        evidence.append({"type": "policy_note", "items": policy_notes})

    return {
        "historical_baseline": historical_baseline,
        "recommended_slo": recommended_slo,
        "confidence": confidence(row, review_required, kind),
        "evidence": evidence,
    }


def generate_slo_recommendations(
    database_url: str,
    *,
    days: int = 30,
    window_size: str = "15m",
    recommendation_version: str = DEFAULT_SLO_RECOMMENDATION_VERSION,
    baseline_version: str = "baseline-v1",
    service_ids: list[str] | None = None,
    replace: bool = False,
    dry_run: bool = False,
) -> dict[str, Any]:
    rows = load_slo_candidates(database_url, days, window_size, service_ids, baseline_version)
    recommendations = []
    confidence_counts: dict[str, int] = {}
    for row in rows:
        recommendation = build_slo_recommendation(row, days, window_size, baseline_version)
        confidence_counts[recommendation["confidence"]] = confidence_counts.get(recommendation["confidence"], 0) + 1
        recommendations.append({"service_id": row["service_id"], **recommendation})

    summary = {
        "status": "dry_run" if dry_run else "succeeded",
        "generated_at": datetime.now(timezone.utc).isoformat().replace("+00:00", "Z"),
        "recommendation_version": recommendation_version,
        "recommendation_window": f"{days}d",
        "baseline_version": baseline_version,
        "window_size": window_size,
        "service_count": len(recommendations),
        "confidence_counts": confidence_counts,
        "replace": replace,
        "dry_run": dry_run,
    }
    if dry_run:
        return {**summary, "recommendations": recommendations}

    statements = ["begin;"]
    if replace:
        service_filter = (
            " and service_id in (" + ",".join(sql_literal(service_id) for service_id in service_ids) + ")"
            if service_ids
            else ""
        )
        statements.append(
            "delete from slo_recommendations "
            f"where recommendation_version = {sql_literal(recommendation_version)} and status = 'pending_review'{service_filter};"
        )
    for recommendation in recommendations:
        statements.append(
            """
insert into slo_recommendations (
  service_id, recommendation_version, recommendation_window,
  historical_baseline, recommended_slo, confidence, evidence, status
) values (
  {service_id}, {version}, {window},
  {historical_baseline}::jsonb, {recommended_slo}::jsonb,
  {confidence}, {evidence}::jsonb, 'pending_review'
);
""".format(
                service_id=sql_literal(recommendation["service_id"]),
                version=sql_literal(recommendation_version),
                window=sql_literal(f"{days}d"),
                historical_baseline=sql_literal(recommendation["historical_baseline"]),
                recommended_slo=sql_literal(recommendation["recommended_slo"]),
                confidence=sql_literal(recommendation["confidence"]),
                evidence=sql_literal(recommendation["evidence"]),
            )
        )
    statements.append("commit;")
    psql_exec(database_url, "\n".join(statements))
    return summary


def list_slo_recommendations(
    database_url: str,
    *,
    service_id: str | None = None,
    recommendation_version: str | None = None,
    status: str | None = None,
    limit: int = 100,
) -> list[dict[str, Any]]:
    where = []
    if service_id:
        where.append(f"service_id = '{sql_text(service_id)}'")
    if recommendation_version:
        where.append(f"recommendation_version = '{sql_text(recommendation_version)}'")
    if status:
        where.append(f"status = '{sql_text(status)}'")
    where_sql = "where " + " and ".join(where) if where else ""
    sql = f"""
select coalesce(json_agg(row_to_json(r) order by created_at desc, service_id), '[]'::json)
from (
  select id, service_id, recommendation_version, recommendation_window,
         historical_baseline, recommended_slo, confidence, evidence, status,
         reviewer, review_note, created_at, reviewed_at
  from slo_recommendations
  {where_sql}
  order by created_at desc, service_id
  limit {max(1, min(limit, 500))}
) r;
"""
    return psql_json(database_url, sql) or []
