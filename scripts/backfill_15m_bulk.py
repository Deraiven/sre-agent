#!/usr/bin/env python3
"""Bulk backfill 15m New Relic and Prometheus windows.

This collector is optimized for historical backfills. It queries each service
over the full plan range and writes one row per 15m window. Kubernetes and
GitHub are intentionally skipped here; use collect_windows.py for live inspect.
"""

from __future__ import annotations

import argparse
import json
import os
import sys
import urllib.parse
from datetime import datetime, timedelta, timezone
from typing import Any

from collect_windows import (  # noqa: E402 - script-local import.
    DEFAULT_DATABASE_URL,
    DEFAULT_NEW_RELIC_GRAPHQL_URL,
    DEFAULT_PROMETHEUS_URL,
    PROMETHEUS_QUERIES,
    format_time,
    iter_windows,
    load_plan,
    newrelic_graphql,
    newrelic_time,
    parse_time,
    prometheus_query_range,
    summarize_values,
    upsert_rows,
)


WINDOW_SIZE = "15m"
WINDOW_SECONDS = 15 * 60
NEW_RELIC_CHUNK_DAYS = 3


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--plan", default="data/prep/collection_plan.jsonl")
    parser.add_argument("--database-url", default=os.environ.get("DATABASE_URL", DEFAULT_DATABASE_URL))
    parser.add_argument("--prometheus-url", default=os.environ.get("PROMETHEUS_URL", DEFAULT_PROMETHEUS_URL))
    parser.add_argument("--newrelic-api-key", default=os.environ.get("NEW_RELIC_API_KEY"))
    parser.add_argument("--newrelic-graphql-url", default=os.environ.get("NEW_RELIC_GRAPHQL_URL", DEFAULT_NEW_RELIC_GRAPHQL_URL))
    parser.add_argument("--service", action="append", help="Backfill only this service_id. Can be repeated.")
    parser.add_argument("--start", help="UTC start time. Defaults to plan start.")
    parser.add_argument("--end", help="UTC end time. Defaults to plan end.")
    parser.add_argument("--batch-size", type=int, default=500)
    parser.add_argument("--max-services", type=int)
    parser.add_argument("--dry-run", action="store_true")
    parser.add_argument("--skip-newrelic", action="store_true")
    parser.add_argument("--skip-prometheus", action="store_true")
    return parser.parse_args()


def nrql_timeseries(plan: dict, start, end) -> str:
    source = plan["sources"]["newrelic"]
    app_name = source.get("app_name") or ""
    escaped_app_name = app_name.replace("'", "\\'")
    return (
        "SELECT count(*) AS 'request_count', "
        "rate(count(*), 1 minute) AS 'rpm', "
        "percentage(count(*), WHERE error IS true) AS 'error_rate', "
        "percentile(duration, 95, 99) "
        "FROM Transaction "
        f"WHERE appName = '{escaped_app_name}' "
        f"SINCE '{newrelic_time(start)}' UNTIL '{newrelic_time(end)}' "
        "TIMESERIES 15 minutes"
    )


def collect_newrelic_timeseries(plan: dict, start, end, args: argparse.Namespace) -> tuple[dict[str, dict], list[dict]]:
    source = plan["sources"]["newrelic"]
    base = {
        "account_id": source.get("account_id"),
        "app_id": source.get("app_id"),
        "app_name": source.get("app_name"),
        "entity_guid": source.get("entity_guid"),
        "signals": source.get("signals", []),
    }
    if args.skip_newrelic:
        return {}, []
    if not args.newrelic_api_key:
        return {}, [{"source": "newrelic", "signal": "golden_signals", "error": "NEW_RELIC_API_KEY is not set"}]

    account_id = int(source.get("account_id") or 0)
    chunk_start = start
    by_window: dict[str, dict] = {}
    errors: list[dict] = []
    try:
        while chunk_start < end:
            chunk_end = min(chunk_start + timedelta(days=NEW_RELIC_CHUNK_DAYS), end)
            nrql = nrql_timeseries(plan, chunk_start, chunk_end)
            payload = newrelic_graphql(args.newrelic_api_key, args.newrelic_graphql_url, account_id, nrql)
            results = (
                payload.get("data", {})
                .get("actor", {})
                .get("account", {})
                .get("nrql", {})
                .get("results", [])
            )
            for result in results:
                begin = result.get("beginTimeSeconds")
                if begin is None:
                    continue
                percentiles = result.get("percentile.duration") or {}
                p95_seconds = percentiles.get("95")
                p99_seconds = percentiles.get("99")
                window_start = format_time(datetime.fromtimestamp(begin, tz=timezone.utc))
                by_window[window_start] = {
                    "status": "collected",
                    **base,
                    "query": nrql,
                    "request_count": result.get("request_count"),
                    "rpm": result.get("rpm"),
                    "error_rate_percent": result.get("error_rate"),
                    "latency_p95_ms": p95_seconds * 1000 if isinstance(p95_seconds, (int, float)) else None,
                    "latency_p99_ms": p99_seconds * 1000 if isinstance(p99_seconds, (int, float)) else None,
                    "raw_result": result,
                }
            chunk_start = chunk_end
        return by_window, errors
    except Exception as exc:  # noqa: BLE001 - persist collection error as data quality.
        errors.append({"source": "newrelic", "signal": "golden_signals", "error": str(exc)})
        return by_window, errors


def collect_prometheus_timeseries(plan: dict, start, end, args: argparse.Namespace) -> tuple[dict[str, dict], list[dict]]:
    if args.skip_prometheus:
        return {}, []
    source = plan["sources"]["prometheus_resources"]
    namespace = source["namespace"]
    pod_regex = source["pod_regex"]
    if source.get("status") == "missing" or not pod_regex:
        return {}, []
    by_window: dict[str, dict] = {}
    errors: list[dict] = []
    query_start = start + timedelta(seconds=WINDOW_SECONDS)

    for signal in source.get("signals", []):
        template = PROMETHEUS_QUERIES.get(signal)
        if not template:
            errors.append({"source": "prometheus", "signal": signal, "error": "unsupported_signal"})
            continue
        query = template.format(namespace=namespace, pod_regex=pod_regex)
        try:
            payload = prometheus_query_range(args.prometheus_url, query, query_start, end, "15m")
            for series in payload.get("data", {}).get("result", []):
                for timestamp, raw_value in series.get("values", []):
                    try:
                        value = float(raw_value)
                    except (TypeError, ValueError):
                        continue
                    window_start = format_time(
                        datetime.fromtimestamp(float(timestamp) - WINDOW_SECONDS, tz=timezone.utc)
                    )
                    by_window.setdefault(window_start, {})[signal] = {
                        "query": query,
                        "aggregation": "window_end_instant",
                        **summarize_values([value]),
                    }
        except Exception as exc:  # noqa: BLE001 - persist collection error as data quality.
            errors.append({"source": "prometheus", "signal": signal, "error": str(exc)})
    return by_window, errors


def placeholder_newrelic(plan: dict, skipped: bool = False) -> dict:
    source = plan["sources"]["newrelic"]
    return {
        "status": "skipped" if skipped else "not_collected",
        "account_id": source.get("account_id"),
        "app_id": source.get("app_id"),
        "app_name": source.get("app_name"),
        "entity_guid": source.get("entity_guid"),
        "signals": source.get("signals", []),
    }


def build_rows_for_service(plan: dict, args: argparse.Namespace) -> list[dict]:
    start = parse_time(args.start or plan["start"])
    end = parse_time(args.end or plan["end"])
    nr_by_window, nr_errors = collect_newrelic_timeseries(plan, start, end, args)
    prom_by_window, prom_errors = collect_prometheus_timeseries(plan, start, end, args)
    errors = nr_errors + prom_errors
    rows = []
    for window_start, window_end in iter_windows(start, end, WINDOW_SIZE):
        key = format_time(window_start)
        rows.append(
            {
                "service_id": plan["service_id"],
                "window_start": key,
                "window_end": format_time(window_end),
                "window_size": WINDOW_SIZE,
                "newrelic": nr_by_window.get(key, placeholder_newrelic(plan, args.skip_newrelic)),
                "prometheus_resources": prom_by_window.get(key, {"status": "skipped" if args.skip_prometheus else "not_collected"}),
                "kubernetes": {"status": "skipped", **plan["sources"]["kubernetes"]},
                "change_context": {"status": "skipped", **plan["sources"]["github"]},
                "data_quality": {
                    "collector": "backfill_15m_bulk.py",
                    "collector_version": "mvp-bulk-15m-v1",
                    "collected_at": format_time(datetime.now(timezone.utc)),
                    "missing_sources": ["kubernetes_events", "github"],
                    "errors": errors,
                },
                "source_snapshot_uri": None,
            }
        )
    return rows


def main() -> None:
    args = parse_args()
    service_filter = set(args.service) if args.service else None
    plans = load_plan(args.plan, service_filter)
    if args.max_services:
        plans = plans[: args.max_services]
    if not plans:
        print("No collection plans matched.", file=sys.stderr)
        sys.exit(1)

    pending: list[dict[str, Any]] = []
    emitted = 0
    for plan in plans:
        rows = build_rows_for_service(plan, args)
        for row in rows:
            pending.append(row)
            emitted += 1
            if args.dry_run:
                print(json.dumps(row, sort_keys=True))
            elif len(pending) >= args.batch_size:
                upsert_rows(args.database_url, pending)
                pending.clear()
        if not args.dry_run:
            print(json.dumps({"service_id": plan["service_id"], "emitted": emitted}), file=sys.stderr)

    if pending and not args.dry_run:
        upsert_rows(args.database_url, pending)

    print(json.dumps({"rows": emitted, "dry_run": args.dry_run}, sort_keys=True))


if __name__ == "__main__":
    main()
