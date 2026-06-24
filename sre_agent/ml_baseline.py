"""Unsupervised dynamic-baseline model registry and quality scaffolding."""

from __future__ import annotations

import math
from datetime import datetime, timedelta
from typing import Any

from .db import psql_exec, psql_json, sql_literal, sql_text
from .intelligence import (
    METRIC_NAMES,
    WINDOW_SECONDS,
    extract_metrics,
    format_time,
    load_metric_windows,
    parse_time,
    percentile,
    utc_now,
)


DEFAULT_MODEL_TYPE = "seasonal_quantile_v1"
DEFAULT_MODEL_VERSION = "seasonal-quantile-v1"
FEATURE_SPEC = {
    "time_features": ["weekday", "hour", "minute_slot", "is_weekend"],
    "context_features": ["newrelic.request_count", "newrelic.rpm"],
    "baseline_scopes": ["weekday_hour_slot", "hour_slot", "weekday_hour", "hour", "global"],
    "scoring": ["residual", "residual_ratio", "robust_mad_score"],
}


def _sql_text_array(values: list[str] | None) -> str:
    if not values:
        return "null"
    return "array[" + ", ".join(sql_literal(item) for item in values) + "]::text[]"


def _service_filter(service_ids: list[str] | None, table_alias: str = "s") -> str:
    if not service_ids:
        return ""
    values = ", ".join(sql_literal(service_id) for service_id in service_ids)
    return f"and {table_alias}.service_id in ({values})"


def _clean_values(values: list[float]) -> list[float]:
    return [value for value in values if value is not None and math.isfinite(value)]


def _mad(values: list[float], median: float | None) -> float | None:
    if median is None:
        return None
    deviations = [abs(value - median) for value in _clean_values(values)]
    return percentile(deviations, 0.50)


def _bucket_confidence(sample_count: int, coverage_pct: float | None) -> str:
    if sample_count >= 12 and (coverage_pct or 0) >= 90:
        return "high"
    if sample_count >= 6 and (coverage_pct or 0) >= 70:
        return "medium"
    if sample_count >= 3:
        return "low"
    return "insufficient"


def _scope_key(
    metric_name: str,
    day_of_week: int | None,
    hour_of_day: int | None,
    minute_slot: int | None,
) -> tuple[str, int | None, int | None, int | None, str]:
    if day_of_week is not None and hour_of_day is not None and minute_slot is not None:
        scope = "weekday_hour_slot"
    elif hour_of_day is not None and minute_slot is not None:
        scope = "hour_slot"
    elif day_of_week is not None and hour_of_day is not None:
        scope = "weekday_hour"
    elif hour_of_day is not None:
        scope = "hour"
    else:
        scope = "global"
    return metric_name, day_of_week, hour_of_day, minute_slot, scope


def model_quality_report(
    database_url: str,
    service_ids: list[str] | None = None,
    days: int = 30,
    window_size: str = "15m",
    model_version: str = DEFAULT_MODEL_VERSION,
) -> dict[str, Any]:
    until = utc_now()
    since = until - timedelta(days=days)
    step_seconds = WINDOW_SECONDS.get(window_size, 900)
    service_filter = _service_filter(service_ids)
    sql = f"""
with service_scope as (
  select s.service_id
  from services s
  where true {service_filter}
),
expected as (
  select greatest(1, count(*)::int) as windows
  from generate_series(
    {sql_literal(format_time(since))}::timestamptz,
    {sql_literal(format_time(until))}::timestamptz - interval '{step_seconds} seconds',
    interval '{step_seconds} seconds'
  )
),
actual as (
  select w.service_id,
         count(*)::int as rows,
         min(w.window_start) as earliest,
         max(w.window_start) as latest,
         count(*) filter (where w.newrelic->>'status' = 'collected')::int as newrelic_rows,
         count(*) filter (where coalesce(w.kubernetes->>'status', '') = 'collected')::int as kubernetes_rows,
         count(*) filter (where coalesce(w.data_quality->'errors', '[]'::jsonb) <> '[]'::jsonb)::int as data_quality_error_rows
  from service_metric_windows w
  where w.window_size = {sql_literal(window_size)}
    and w.window_start >= {sql_literal(format_time(since))}
    and w.window_start < {sql_literal(format_time(until))}
    and w.service_id in (select service_id from service_scope)
  group by w.service_id
),
models as (
  select service_id,
         count(*)::int as model_count,
         count(*) filter (where active)::int as active_model_count,
         max(created_at) as latest_model_created_at
  from service_metric_models
  where model_version = {sql_literal(model_version)}
  group by service_id
)
select coalesce(json_agg(row_to_json(q) order by q.service_id), '[]'::json)
from (
  select
    s.service_id,
    coalesce(a.rows, 0) as rows,
    e.windows as expected_windows,
    round(coalesce(a.rows, 0)::numeric / nullif(e.windows, 0) * 100, 2) as coverage_pct,
    a.earliest,
    a.latest,
    coalesce(a.newrelic_rows, 0) as newrelic_rows,
    coalesce(a.kubernetes_rows, 0) as kubernetes_rows,
    coalesce(a.data_quality_error_rows, 0) as data_quality_error_rows,
    case
      when coalesce(a.rows, 0)::numeric / nullif(e.windows, 0) >= 0.95 then 'high'
      when coalesce(a.rows, 0)::numeric / nullif(e.windows, 0) >= 0.90 then 'medium'
      when coalesce(a.rows, 0)::numeric / nullif(e.windows, 0) >= 0.70 then 'low'
      else 'insufficient'
    end as training_readiness,
    coalesce(m.model_count, 0) as model_count,
    coalesce(m.active_model_count, 0) as active_model_count,
    m.latest_model_created_at
  from service_scope s
  cross join expected e
  left join actual a on a.service_id = s.service_id
  left join models m on m.service_id = s.service_id
) q;
"""
    services = psql_json(database_url, sql) or []
    ready = sum(1 for row in services if row.get("training_readiness") in {"high", "medium"})
    return {
        "model_version": model_version,
        "model_type": DEFAULT_MODEL_TYPE,
        "window_size": window_size,
        "since": format_time(since),
        "until": format_time(until),
        "service_count": len(services),
        "ready_service_count": ready,
        "readiness_policy": {
            "high": "coverage >= 95%",
            "medium": "coverage >= 90%",
            "low": "coverage >= 70%",
            "insufficient": "coverage < 70%",
        },
        "feature_spec": FEATURE_SPEC,
        "services": services,
    }


def create_training_run(
    database_url: str,
    service_ids: list[str] | None = None,
    metric_names: list[str] | None = None,
    days: int = 30,
    model_version: str = DEFAULT_MODEL_VERSION,
    model_type: str = DEFAULT_MODEL_TYPE,
    window_size: str = "15m",
    dry_run: bool = True,
    activate: bool = False,
    min_coverage_pct: float = 70.0,
    min_bucket_samples: int = 12,
    min_precise_bucket_samples: int = 3,
) -> dict[str, Any]:
    until = utc_now()
    since = until - timedelta(days=days)
    metric_names = metric_names or METRIC_NAMES
    quality = model_quality_report(
        database_url,
        service_ids=service_ids,
        days=days,
        window_size=window_size,
        model_version=model_version,
    )
    status = "dry_run" if dry_run else "training"
    sql = """
insert into service_metric_training_runs (
  model_version, model_type, status, training_window_start, training_window_end,
  window_size, service_ids, metric_names, dry_run, quality_summary, started_at, finished_at
) values (
  {model_version}, {model_type}, {status}, {training_window_start}, {training_window_end},
  {window_size}, {service_ids}, {metric_names}, {dry_run}, {quality_summary}::jsonb, now(), now()
) returning row_to_json(service_metric_training_runs);
""".format(
        model_version=sql_literal(model_version),
        model_type=sql_literal(model_type),
        status=sql_literal(status),
        training_window_start=sql_literal(format_time(since)),
        training_window_end=sql_literal(format_time(until)),
        window_size=sql_literal(window_size),
        service_ids=_sql_text_array(service_ids),
        metric_names=_sql_text_array(metric_names),
        dry_run=sql_literal(dry_run),
        quality_summary=sql_literal(
            {
                "service_count": quality["service_count"],
                "ready_service_count": quality["ready_service_count"],
                "readiness_policy": quality["readiness_policy"],
            }
        ),
    )
    run = psql_json(database_url, sql)
    if not dry_run:
        try:
            training = train_seasonal_quantile_model(
                database_url,
                run_id=int(run["id"]),
                quality_report=quality,
                metric_names=metric_names,
                model_version=model_version,
                model_type=model_type,
                since=since,
                until=until,
                window_size=window_size,
                activate=activate,
                min_coverage_pct=min_coverage_pct,
                min_bucket_samples=min_bucket_samples,
                min_precise_bucket_samples=min_precise_bucket_samples,
            )
            return {
                "status": training["status"],
                "training_run": training["training_run"],
                "quality_report": quality,
                "training_summary": training["training_summary"],
            }
        except Exception as exc:
            psql_exec(
                database_url,
                """
update service_metric_training_runs
set status = 'failed', error = {error}, finished_at = now()
where id = {run_id};
""".format(error=sql_literal(str(exc)), run_id=int(run["id"])),
            )
            raise
    return {
        "status": status,
        "training_run": run,
        "quality_report": quality,
        "next_step": "set dry_run=false to train seasonal_quantile_v1 buckets",
    }


def train_seasonal_quantile_model(
    database_url: str,
    run_id: int,
    quality_report: dict[str, Any],
    metric_names: list[str],
    model_version: str,
    model_type: str,
    since: datetime,
    until: datetime,
    window_size: str,
    activate: bool = False,
    min_coverage_pct: float = 70.0,
    min_bucket_samples: int = 12,
    min_precise_bucket_samples: int = 3,
) -> dict[str, Any]:
    eligible_services = [
        row
        for row in quality_report.get("services", [])
        if float(row.get("coverage_pct") or 0) >= min_coverage_pct
    ]
    selected_service_ids = [row["service_id"] for row in eligible_services]
    if not selected_service_ids:
        psql_exec(
            database_url,
            """
update service_metric_training_runs
set status = 'rejected',
    error = {error},
    finished_at = now()
where id = {run_id};
""".format(
                error=sql_literal(f"no services met min_coverage_pct={min_coverage_pct}"),
                run_id=run_id,
            ),
        )
        return {
            "status": "rejected",
            "training_run": get_training_run(database_url, run_id),
            "training_summary": {"trained_service_count": 0, "trained_model_count": 0, "bucket_count": 0},
        }

    psql_exec(
        database_url,
        """
delete from service_metric_models
where model_version = {model_version}
  and service_id in ({service_ids});
""".format(
            model_version=sql_literal(model_version),
            service_ids=", ".join(sql_literal(service_id) for service_id in selected_service_ids),
        ),
    )

    trained_model_count = 0
    bucket_count = 0
    evaluation_count = 0
    trained_services: list[dict[str, Any]] = []

    for service_row in eligible_services:
        service_id = service_row["service_id"]
        coverage_pct = float(service_row.get("coverage_pct") or 0)
        rows = load_metric_windows(
            database_url,
            service_id=service_id,
            since=since,
            until=until,
            window_size=window_size,
        )
        by_metric: dict[str, dict[tuple[str, int | None, int | None, int | None], list[float]]] = {}
        for row in rows:
            window_start = parse_time(row["window_start"])
            metrics = extract_metrics(row)
            for metric_name, value in metrics.items():
                if metric_name not in metric_names:
                    continue
                metric_buckets = by_metric.setdefault(metric_name, {})
                keys = [
                    _scope_key(metric_name, None, None, None),
                    _scope_key(metric_name, None, window_start.hour, None),
                    _scope_key(metric_name, None, window_start.hour, window_start.minute),
                    _scope_key(metric_name, window_start.weekday(), window_start.hour, None),
                    _scope_key(metric_name, window_start.weekday(), window_start.hour, window_start.minute),
                ]
                for key in keys:
                    metric_buckets.setdefault(key, []).append(value)

        service_model_count = 0
        service_bucket_count = 0
        for metric_name, metric_buckets in by_metric.items():
            bucket_rows = []
            for (_, day_of_week, hour_of_day, minute_slot, baseline_scope), values in metric_buckets.items():
                clean_values = _clean_values(values)
                precise_bucket = day_of_week is not None and minute_slot is not None
                required_samples = min_precise_bucket_samples if precise_bucket else min_bucket_samples
                if len(clean_values) < required_samples:
                    continue
                median = percentile(clean_values, 0.50)
                bucket_rows.append(
                    {
                        "baseline_scope": baseline_scope,
                        "day_of_week": day_of_week,
                        "hour_of_day": hour_of_day,
                        "minute_slot": minute_slot,
                        "p50": median,
                        "p75": percentile(clean_values, 0.75),
                        "p90": percentile(clean_values, 0.90),
                        "p95": percentile(clean_values, 0.95),
                        "p99": percentile(clean_values, 0.99),
                        "median": median,
                        "mad": _mad(clean_values, median),
                        "sample_count": len(clean_values),
                        "coverage_pct": coverage_pct,
                        "confidence": _bucket_confidence(len(clean_values), coverage_pct),
                    }
                )
            if not bucket_rows:
                continue
            model = insert_metric_model(
                database_url,
                service_id=service_id,
                metric_name=metric_name,
                model_version=model_version,
                model_type=model_type,
                training_run_id=run_id,
                since=since,
                until=until,
                window_size=window_size,
                active=activate,
                quality_summary={
                    "coverage_pct": coverage_pct,
                    "rows": service_row.get("rows"),
                    "bucket_count": len(bucket_rows),
                    "min_bucket_samples": min_bucket_samples,
                    "min_precise_bucket_samples": min_precise_bucket_samples,
                },
            )
            insert_model_buckets(database_url, int(model["id"]), bucket_rows)
            insert_model_evaluation(
                database_url,
                model_id=int(model["id"]),
                training_run_id=run_id,
                service_id=service_id,
                metric_name=metric_name,
                model_version=model_version,
                since=since,
                until=until,
                metrics={
                    "coverage_pct": coverage_pct,
                    "bucket_count": len(bucket_rows),
                    "status": "trained_not_activated" if not activate else "trained_active",
                },
            )
            trained_model_count += 1
            bucket_count += len(bucket_rows)
            evaluation_count += 1
            service_model_count += 1
            service_bucket_count += len(bucket_rows)
        trained_services.append(
            {
                "service_id": service_id,
                "coverage_pct": coverage_pct,
                "model_count": service_model_count,
                "bucket_count": service_bucket_count,
            }
        )

    training_summary = {
        "trained_service_count": len([row for row in trained_services if row["model_count"] > 0]),
        "eligible_service_count": len(eligible_services),
        "trained_model_count": trained_model_count,
        "bucket_count": bucket_count,
        "evaluation_count": evaluation_count,
        "activated": activate,
        "min_coverage_pct": min_coverage_pct,
        "services": trained_services,
    }
    final_status = "active" if activate else "evaluated"
    psql_exec(
        database_url,
        """
update service_metric_training_runs
set status = {status},
    quality_summary = {quality_summary}::jsonb,
    finished_at = now()
where id = {run_id};
""".format(
            status=sql_literal(final_status),
            quality_summary=sql_literal(training_summary),
            run_id=run_id,
        ),
    )
    return {
        "status": final_status,
        "training_run": get_training_run(database_url, run_id),
        "training_summary": training_summary,
    }


def insert_metric_model(
    database_url: str,
    service_id: str,
    metric_name: str,
    model_version: str,
    model_type: str,
    training_run_id: int,
    since: datetime,
    until: datetime,
    window_size: str,
    active: bool,
    quality_summary: dict[str, Any],
) -> dict[str, Any]:
    sql = """
insert into service_metric_models (
  service_id, metric_name, model_version, model_type, status, active,
  training_run_id, training_window_start, training_window_end, window_size,
  feature_spec, model_params, quality_summary, activated_at
) values (
  {service_id}, {metric_name}, {model_version}, {model_type}, {status}, {active},
  {training_run_id}, {since}, {until}, {window_size},
  {feature_spec}::jsonb, {model_params}::jsonb, {quality_summary}::jsonb, {activated_at}
) returning row_to_json(service_metric_models);
""".format(
        service_id=sql_literal(service_id),
        metric_name=sql_literal(metric_name),
        model_version=sql_literal(model_version),
        model_type=sql_literal(model_type),
        status=sql_literal("active" if active else "evaluated"),
        active=sql_literal(active),
        training_run_id=training_run_id,
        since=sql_literal(format_time(since)),
        until=sql_literal(format_time(until)),
        window_size=sql_literal(window_size),
        feature_spec=sql_literal(FEATURE_SPEC),
        model_params=sql_literal({"algorithm": "seasonal_quantile", "residual_score": "mad"}),
        quality_summary=sql_literal(quality_summary),
        activated_at="now()" if active else "null",
    )
    return psql_json(database_url, sql)


def insert_model_buckets(database_url: str, model_id: int, bucket_rows: list[dict[str, Any]]) -> None:
    statements = ["begin;"]
    for row in bucket_rows:
        statements.append(
            """
insert into service_metric_model_buckets (
  model_id, baseline_scope, day_of_week, hour_of_day, minute_slot,
  p50, p75, p90, p95, p99, median, mad, sample_count, coverage_pct, confidence
) values (
  {model_id}, {baseline_scope}, {day_of_week}, {hour_of_day}, {minute_slot},
  {p50}, {p75}, {p90}, {p95}, {p99}, {median}, {mad}, {sample_count}, {coverage_pct}, {confidence}
);
""".format(
                model_id=model_id,
                baseline_scope=sql_literal(row["baseline_scope"]),
                day_of_week=sql_literal(row["day_of_week"]),
                hour_of_day=sql_literal(row["hour_of_day"]),
                minute_slot=sql_literal(row["minute_slot"]),
                p50=sql_literal(row["p50"]),
                p75=sql_literal(row["p75"]),
                p90=sql_literal(row["p90"]),
                p95=sql_literal(row["p95"]),
                p99=sql_literal(row["p99"]),
                median=sql_literal(row["median"]),
                mad=sql_literal(row["mad"]),
                sample_count=sql_literal(row["sample_count"]),
                coverage_pct=sql_literal(row["coverage_pct"]),
                confidence=sql_literal(row["confidence"]),
            )
        )
    statements.append("commit;")
    psql_exec(database_url, "\n".join(statements))


def insert_model_evaluation(
    database_url: str,
    model_id: int,
    training_run_id: int,
    service_id: str,
    metric_name: str,
    model_version: str,
    since: datetime,
    until: datetime,
    metrics: dict[str, Any],
) -> None:
    sql = """
insert into service_metric_model_evaluations (
  model_id, training_run_id, service_id, metric_name, model_version,
  evaluation_window_start, evaluation_window_end, status, metrics
) values (
  {model_id}, {training_run_id}, {service_id}, {metric_name}, {model_version},
  {since}, {until}, 'created', {metrics}::jsonb
);
""".format(
        model_id=model_id,
        training_run_id=training_run_id,
        service_id=sql_literal(service_id),
        metric_name=sql_literal(metric_name),
        model_version=sql_literal(model_version),
        since=sql_literal(format_time(since)),
        until=sql_literal(format_time(until)),
        metrics=sql_literal(metrics),
    )
    psql_exec(database_url, sql)


def get_training_run(database_url: str, run_id: int) -> dict[str, Any]:
    sql = f"""
select row_to_json(r)
from (
  select id, model_version, model_type, status, training_window_start,
         training_window_end, window_size, service_ids, metric_names, dry_run,
         quality_summary, error, started_at, finished_at, created_at
  from service_metric_training_runs
  where id = {run_id}
) r;
"""
    return psql_json(database_url, sql)


def list_models(
    database_url: str,
    service_id: str | None = None,
    model_version: str | None = None,
    active_only: bool = False,
    limit: int = 100,
) -> list[dict[str, Any]]:
    where = ["true"]
    if service_id:
        where.append(f"service_id = {sql_literal(service_id)}")
    if model_version:
        where.append(f"model_version = {sql_literal(model_version)}")
    if active_only:
        where.append("active")
    sql = f"""
select coalesce(json_agg(row_to_json(m) order by created_at desc), '[]'::json)
from (
  select id, service_id, metric_name, model_version, model_type, status, active,
         training_run_id, training_window_start, training_window_end, window_size,
         quality_summary, created_at, activated_at
  from service_metric_models
  where {' and '.join(where)}
  order by created_at desc
  limit {max(1, min(limit, 500))}
) m;
"""
    return psql_json(database_url, sql) or []


def list_training_runs(database_url: str, limit: int = 50) -> list[dict[str, Any]]:
    sql = f"""
select coalesce(json_agg(row_to_json(r) order by created_at desc), '[]'::json)
from (
  select id, model_version, model_type, status, training_window_start,
         training_window_end, window_size, service_ids, metric_names, dry_run,
         quality_summary, error, started_at, finished_at, created_at
  from service_metric_training_runs
  order by created_at desc
  limit {max(1, min(limit, 200))}
) r;
"""
    return psql_json(database_url, sql) or []


def add_risk_feedback_label(database_url: str, payload: dict[str, Any]) -> dict[str, Any]:
    service_id = payload.get("service_id")
    label_type = payload.get("label_type")
    if not service_id:
        raise ValueError("service_id is required")
    if not label_type:
        raise ValueError("label_type is required")
    sql = """
insert into risk_feedback_labels (
  service_id, window_start, window_end, risk_version, model_version, label_type,
  actual_severity, false_positive, false_negative, payload
) values (
  {service_id}, {window_start}, {window_end}, {risk_version}, {model_version}, {label_type},
  {actual_severity}, {false_positive}, {false_negative}, {payload}::jsonb
) returning row_to_json(risk_feedback_labels);
""".format(
        service_id=sql_literal(service_id),
        window_start=sql_literal(payload.get("window_start")),
        window_end=sql_literal(payload.get("window_end")),
        risk_version=sql_literal(payload.get("risk_version")),
        model_version=sql_literal(payload.get("model_version")),
        label_type=sql_literal(label_type),
        actual_severity=sql_literal(payload.get("actual_severity")),
        false_positive=sql_literal(payload.get("false_positive")),
        false_negative=sql_literal(payload.get("false_negative")),
        payload=sql_literal(payload),
    )
    return psql_json(database_url, sql)


def apply_schema(database_url: str, schema_path: str = "db/schema.sql") -> None:
    with open(schema_path, encoding="utf-8") as handle:
        psql_exec(database_url, handle.read())
