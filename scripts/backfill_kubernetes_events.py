#!/usr/bin/env python3
"""Backfill Kubernetes event counts from VictoriaLogs into existing metric windows.

The bulk 15m backfill intentionally skips Kubernetes because live pod inspect is
too expensive for historical recovery. This script fills that gap by querying
VictoriaLogs for workload events, bucketing them into existing 15m windows, and
rewriting Kubernetes placeholders such as mapped/error into events_only rows.
"""

from __future__ import annotations

import argparse
import json
import os
import sys
import time
import urllib.error
import urllib.parse
import urllib.request
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))
if str(ROOT / "scripts") not in sys.path:
    sys.path.insert(0, str(ROOT / "scripts"))

from sre_agent.db import psql_exec, psql_json, sql_literal  # noqa: E402

from collect_windows import (  # noqa: E402
    DEFAULT_DATABASE_URL,
    build_victorialogs_event_query,
    format_time,
    kubernetes_event_counts,
    load_plan,
    normalize_victorialogs_kubernetes_event,
    parse_time,
    victorialogs_query,
)


WINDOW_SIZE = "15m"
WINDOW_SECONDS = 15 * 60
DEFAULT_QUERY_TEMPLATE = 'log_type:k8s_events namespace:{namespace} name:~"{workload_name}-.+"'
DEFAULT_VICTORIALOGS_URL = (
    "http://k8s-default-victoria-d4f3374ede-74ab6236b3b513dc.elb.ap-southeast-1.amazonaws.com:8427"
)


def utc_now() -> datetime:
    return datetime.now(timezone.utc)


def align_down(value: datetime) -> datetime:
    timestamp = int(value.timestamp())
    return datetime.fromtimestamp(timestamp - (timestamp % WINDOW_SECONDS), tz=timezone.utc)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--plan", default="data/prep/collection_plan.jsonl")
    parser.add_argument("--database-url", default=os.environ.get("DATABASE_URL", DEFAULT_DATABASE_URL))
    parser.add_argument("--service", action="append", help="Backfill only this service_id. Can be repeated.")
    parser.add_argument("--start", help="UTC start time. Defaults to now - --days.")
    parser.add_argument("--end", help="UTC end time. Defaults to the latest aligned 15m window.")
    parser.add_argument("--days", type=int, default=30)
    parser.add_argument("--window-size", choices=[WINDOW_SIZE], default=WINDOW_SIZE)
    parser.add_argument(
        "--statuses",
        default="mapped,error",
        help="Comma-separated existing Kubernetes statuses to replace. Use empty string for all statuses.",
    )
    parser.add_argument("--victorialogs-url", default=os.environ.get("VICTORIALOGS_URL", DEFAULT_VICTORIALOGS_URL))
    parser.add_argument("--victorialogs-tenant", default=os.environ.get("VICTORIALOGS_TENANT"))
    parser.add_argument(
        "--victorialogs-kubernetes-events-query-template",
        default=os.environ.get("VICTORIALOGS_KUBERNETES_EVENTS_QUERY_TEMPLATE", DEFAULT_QUERY_TEMPLATE),
    )
    parser.add_argument("--victorialogs-limit", type=int, default=50000)
    parser.add_argument("--query-chunk-hours", type=int, default=6)
    parser.add_argument("--victorialogs-timeout", type=int, default=60)
    parser.add_argument("--victorialogs-retries", type=int, default=3)
    parser.add_argument("--max-events-per-window", type=int, default=20)
    parser.add_argument(
        "--aggregate-events",
        action="store_true",
        help="Use VictoriaLogs hits aggregation. Faster, but cannot deduplicate repeated Kubernetes Event snapshots.",
    )
    parser.add_argument("--batch-size", type=int, default=500)
    parser.add_argument("--max-services", type=int)
    parser.add_argument("--dry-run", action="store_true")
    return parser.parse_args()


def target_range(args: argparse.Namespace) -> tuple[datetime, datetime]:
    end = align_down(parse_time(args.end) if args.end else utc_now())
    start = align_down(parse_time(args.start) if args.start else end - timedelta(days=args.days))
    if end <= start:
        raise ValueError("empty backfill range")
    return start, end


def fetch_target_windows(
    database_url: str,
    service_id: str,
    start: datetime,
    end: datetime,
    statuses: list[str] | None,
) -> list[dict[str, Any]]:
    status_filter = ""
    if statuses is not None:
        status_filter = "and coalesce(kubernetes->>'status', '') in ({statuses})".format(
            statuses=", ".join(sql_literal(status) for status in statuses)
        )
    sql = f"""
select coalesce(json_agg(row_to_json(q) order by q.window_start), '[]'::json)
from (
  select window_start, window_end, kubernetes
  from service_metric_windows
  where service_id = {sql_literal(service_id)}
    and window_size = {sql_literal(WINDOW_SIZE)}
    and window_start >= {sql_literal(format_time(start))}
    and window_start < {sql_literal(format_time(end))}
    {status_filter}
) q;
"""
    return psql_json(database_url, sql) or []


def event_timestamp(event: dict) -> datetime | None:
    raw = event.get("timestamp")
    if not raw:
        return None
    try:
        return parse_time(str(raw))
    except Exception:  # noqa: BLE001 - malformed log timestamp is ignored.
        return None


def bucket_key(timestamp: datetime) -> str:
    return format_time(align_down(timestamp))


def victorialogs_hits(
    base_url: str,
    query: str,
    start: datetime,
    end: datetime,
    tenant: str | None,
    timeout: int,
    retries: int,
    step: str = "15m",
    field: str | None = None,
    fields_limit: int = 50,
) -> list[dict[str, Any]]:
    endpoint = base_url.rstrip("/") + "/select/logsql/hits"
    params = {
        "query": query,
        "start": format_time(start),
        "end": format_time(end),
        "step": step,
        "fields_limit": str(fields_limit),
    }
    if field:
        params["field"] = field
    if tenant:
        params["tenant"] = tenant
    request = urllib.request.Request(endpoint + "?" + urllib.parse.urlencode(params))
    opener = urllib.request.build_opener(urllib.request.ProxyHandler({}))
    for attempt in range(max(1, retries)):
        try:
            with opener.open(request, timeout=timeout) as response:
                payload = json.loads(response.read().decode("utf-8"))
                return payload.get("hits") or []
        except urllib.error.HTTPError as exc:
            if exc.code == 404:
                return []
            if attempt + 1 >= max(1, retries):
                raise
        except TimeoutError:
            if attempt + 1 >= max(1, retries):
                raise
        time.sleep(min(10, 2**attempt))
    return []


def collect_event_counts_for_plan(
    plan: dict,
    start: datetime,
    end: datetime,
    args: argparse.Namespace,
) -> dict[str, dict[str, int]]:
    source = plan["sources"]["kubernetes"]
    namespace = source.get("namespace")
    workload_name = source.get("workload_name")
    if source.get("status") == "missing" or not namespace or not workload_name:
        return {}
    query = build_victorialogs_event_query(
        args.victorialogs_kubernetes_events_query_template,
        namespace,
        workload_name,
        "",
        set(),
        start,
        end,
    )
    by_window: dict[str, dict[str, int]] = {}
    chunk_start = start
    chunk_size = timedelta(hours=max(1, args.query_chunk_hours))
    while chunk_start < end:
        chunk_end = min(chunk_start + chunk_size, end)
        hits = victorialogs_hits(
            args.victorialogs_url,
            query,
            chunk_start,
            chunk_end,
            args.victorialogs_tenant,
            args.victorialogs_timeout,
            args.victorialogs_retries,
            step=WINDOW_SIZE,
            field="reason",
        )
        for item in hits:
            reason = (item.get("fields") or {}).get("reason") or ""
            timestamps = item.get("timestamps") or []
            values = item.get("values") or []
            for raw_ts, raw_value in zip(timestamps, values, strict=False):
                try:
                    value = int(raw_value or 0)
                except (TypeError, ValueError):
                    continue
                key = bucket_key(parse_time(str(raw_ts)))
                counts = by_window.setdefault(
                    key,
                    {
                        "event_rows": 0,
                        "probe_failure_count": 0,
                        "failed_scheduling_count": 0,
                        "killing_event_count": 0,
                        "image_pull_failure_count": 0,
                        "oom_killed_count": 0,
                    },
                )
                counts["event_rows"] += value
                if reason == "Unhealthy":
                    counts["probe_failure_count"] += value
                if reason == "FailedScheduling":
                    counts["failed_scheduling_count"] += value
                if reason == "Killing":
                    counts["killing_event_count"] += value
                if reason in {"Failed", "BackOff", "ErrImagePull", "ImagePullBackOff"}:
                    counts["image_pull_failure_count"] += value
        chunk_start = chunk_end
    return by_window


def collect_events_for_plan(plan: dict, start: datetime, end: datetime, args: argparse.Namespace) -> list[dict[str, Any]]:
    source = plan["sources"]["kubernetes"]
    namespace = source.get("namespace")
    workload_name = source.get("workload_name")
    if source.get("status") == "missing" or not namespace or not workload_name:
        return []
    events = []
    chunk_start = start
    chunk_size = timedelta(hours=max(1, args.query_chunk_hours))
    while chunk_start < end:
        chunk_end = min(chunk_start + chunk_size, end)
        query = build_victorialogs_event_query(
            args.victorialogs_kubernetes_events_query_template,
            namespace,
            workload_name,
            "",
            set(),
            chunk_start,
            chunk_end,
        )
        rows = victorialogs_query(
            args.victorialogs_url,
            query,
            chunk_start,
            chunk_end,
            args.victorialogs_tenant,
            limit=args.victorialogs_limit,
        )
        for row in rows:
            event = normalize_victorialogs_kubernetes_event(row)
            ts = event_timestamp(event)
            if ts is None or not (start <= ts < end):
                continue
            events.append(event)
        chunk_start = chunk_end
    return events


def kubernetes_payload(
    plan: dict,
    events: list[dict[str, Any]],
    collected_at: datetime,
    max_events_per_window: int,
    counts: dict[str, int] | None = None,
) -> dict[str, Any]:
    source = plan["sources"]["kubernetes"]
    event_sample = events[: max(0, max_events_per_window)]
    event_counts = counts or kubernetes_event_counts(events)
    event_rows = int(event_counts.get("event_rows", len(events)))
    unique_event_count = int(event_counts.get("unique_event_count", len(events)))
    return {
        **source,
        "status": "events_only",
        "probe_failure_count": event_counts.get("probe_failure_count", 0),
        "failed_scheduling_count": event_counts.get("failed_scheduling_count", 0),
        "killing_event_count": event_counts.get("killing_event_count", 0),
        "image_pull_failure_count": event_counts.get("image_pull_failure_count", 0),
        "oom_killed_count": event_counts.get("oom_killed_count", 0),
        "events": event_sample,
        "event_sample_count": len(event_sample),
        "event_rows": event_rows,
        "unique_event_count": unique_event_count,
        "events_truncated": len(event_sample) < unique_event_count,
        "events_collected": True,
        "events_provider": "victorialogs",
        "backfill_collector": "backfill_kubernetes_events.py",
        "backfilled_at": format_time(collected_at),
    }


def update_rows(database_url: str, updates: list[dict[str, Any]]) -> None:
    statements = ["begin;"]
    for update in updates:
        backfill_meta = {
            "collector": "backfill_kubernetes_events.py",
            "status": "events_only",
            "events_provider": "victorialogs",
            "backfilled_at": update["backfilled_at"],
        }
        statements.append(
            """
update service_metric_windows
set kubernetes = {kubernetes}::jsonb,
    data_quality = jsonb_set(
      jsonb_set(
        coalesce(data_quality, '{{}}'::jsonb),
        '{{missing_sources}}',
        coalesce(
          (
            select jsonb_agg(to_jsonb(value))
            from jsonb_array_elements_text(coalesce(data_quality->'missing_sources', '[]'::jsonb)) as value
            where value <> 'kubernetes_events'
          ),
          '[]'::jsonb
        ),
        true
      ),
      '{{kubernetes_events_backfill}}',
      {backfill_meta}::jsonb,
      true
    )
where service_id = {service_id}
  and window_start = {window_start}::timestamptz
  and window_size = {window_size};
""".format(
                kubernetes=sql_literal(update["kubernetes"]),
                backfill_meta=sql_literal(backfill_meta),
                service_id=sql_literal(update["service_id"]),
                window_start=sql_literal(update["window_start"]),
                window_size=sql_literal(WINDOW_SIZE),
            )
        )
    statements.append("commit;")
    psql_exec(database_url, "\n".join(statements))


def main() -> None:
    args = parse_args()
    start, end = target_range(args)
    statuses = None if args.statuses == "" else [item.strip() for item in args.statuses.split(",") if item.strip()]
    service_filter = set(args.service) if args.service else None
    plans = load_plan(args.plan, service_filter)
    if args.max_services:
        plans = plans[: args.max_services]
    if not plans:
        print(json.dumps({"status": "noop", "reason": "no_matching_services"}, sort_keys=True))
        return

    pending: list[dict[str, Any]] = []
    summaries = []
    total_updated = 0
    total_events = 0
    collected_at = utc_now()

    for plan in plans:
        service_id = plan["service_id"]
        windows = fetch_target_windows(args.database_url, service_id, start, end, statuses)
        if not windows:
            summaries.append({"service_id": service_id, "target_windows": 0, "events": 0, "updated": 0})
            continue
        event_counts_by_window = collect_event_counts_for_plan(plan, start, end, args) if args.aggregate_events else {}
        events = collect_events_for_plan(plan, start, end, args) if not args.aggregate_events else []
        events_by_window: dict[str, list[dict[str, Any]]] = {}
        for event in events:
            ts = event_timestamp(event)
            if ts is None:
                continue
            events_by_window.setdefault(bucket_key(ts), []).append(event)
        updated = 0
        for window in windows:
            window_start = parse_time(str(window["window_start"]))
            window_end = parse_time(str(window["window_end"]))
            key = format_time(window_start)
            window_events = [
                event
                for event in events_by_window.get(key, [])
                if (event_timestamp(event) is not None and window_start <= event_timestamp(event) < window_end)
            ]
            pending.append(
                {
                    "service_id": service_id,
                    "window_start": key,
                    "window_size": WINDOW_SIZE,
                    "kubernetes": kubernetes_payload(
                        plan,
                        window_events,
                        collected_at,
                        args.max_events_per_window,
                        event_counts_by_window.get(key),
                    ),
                    "backfilled_at": format_time(collected_at),
                }
            )
            updated += 1
            if not args.dry_run and len(pending) >= args.batch_size:
                update_rows(args.database_url, pending)
                total_updated += len(pending)
                pending.clear()
        service_event_rows = sum(item.get("event_rows", 0) for item in event_counts_by_window.values()) if args.aggregate_events else len(events)
        total_events += service_event_rows
        summaries.append(
            {
                "service_id": service_id,
                "target_windows": len(windows),
                "events": service_event_rows,
                "updated": updated,
            }
        )
        print(json.dumps(summaries[-1], sort_keys=True), file=sys.stderr)

    if pending and not args.dry_run:
        update_rows(args.database_url, pending)
        total_updated += len(pending)
        pending.clear()

    if args.dry_run:
        total_updated = sum(summary["updated"] for summary in summaries)

    print(
        json.dumps(
            {
                "status": "planned" if args.dry_run else "succeeded",
                "dry_run": args.dry_run,
                "start": format_time(start),
                "end": format_time(end),
                "services": len(plans),
                "target_statuses": statuses if statuses is not None else "all",
                "event_rows": total_events,
                "updated_windows": total_updated,
                "sample": summaries[:10],
            },
            sort_keys=True,
        )
    )


if __name__ == "__main__":
    main()
