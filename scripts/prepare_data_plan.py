#!/usr/bin/env python3
"""Generate a 30-day data preparation plan from the service mapping.

This script does not query production systems. It creates the local control
files needed by the collector: service seed rows, collection ranges, and a
small sample of concrete window tasks for verification.
"""

from __future__ import annotations

import argparse
import base64
import json
from datetime import datetime, timedelta, timezone
from pathlib import Path


WINDOW_SECONDS = {
    "5m": 5 * 60,
    "15m": 15 * 60,
    "1h": 60 * 60,
    "1d": 24 * 60 * 60,
}


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--catalog-json", default="config/service-mapping.raw.json")
    parser.add_argument("--out-dir", default="data/prep")
    parser.add_argument("--days", type=int, default=30)
    parser.add_argument("--window-size", choices=sorted(WINDOW_SECONDS), default="15m")
    parser.add_argument("--end", help="UTC end time, for example 2026-06-04T00:00:00Z")
    parser.add_argument("--newrelic-account-id", default="464254")
    parser.add_argument("--k8s-cluster", default="storehub-pro")
    parser.add_argument("--sample-tasks", type=int, default=50)
    return parser.parse_args()


def parse_end_time(value: str | None) -> datetime:
    if not value:
        now = datetime.now(timezone.utc)
        return now.replace(second=0, microsecond=0)
    normalized = value.replace("Z", "+00:00")
    parsed = datetime.fromisoformat(normalized)
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=timezone.utc)
    return parsed.astimezone(timezone.utc)


def align_time_to_window(value: datetime, window_size: str) -> datetime:
    seconds = WINDOW_SECONDS[window_size]
    epoch_seconds = int(value.timestamp())
    aligned_seconds = epoch_seconds - (epoch_seconds % seconds)
    return datetime.fromtimestamp(aligned_seconds, tz=timezone.utc)


def iter_windows(start: datetime, end: datetime, window_size: str):
    step = timedelta(seconds=WINDOW_SECONDS[window_size])
    cursor = start
    while cursor < end:
        window_end = min(cursor + step, end)
        yield cursor, window_end
        cursor = window_end


def newrelic_entity_guid(account_id: str, app_id: str | None) -> str:
    if not app_id:
        return ""
    raw = f"{account_id}|APM|APPLICATION|{app_id}".encode("utf-8")
    return base64.b64encode(raw).decode("ascii")


def service_row(service: dict, account_id: str, k8s_cluster: str) -> dict:
    deployment = service.get("eks_deployment")
    mapping_status = service.get("k8s_mapping_status", "mapped" if deployment else "missing")
    return {
        "service_id": service["service_id"],
        "description": service.get("description"),
        "owner": "REPLACE_WITH_OWNER",
        "github_repo": service["github_repo"],
        "production_branch": "master",
        "newrelic_app_name": service.get("newrelic_app_name"),
        "newrelic_app_id": service.get("newrelic_app_id"),
        "newrelic_entity_guid": newrelic_entity_guid(account_id, service.get("newrelic_app_id")),
        "k8s_cluster": k8s_cluster,
        "k8s_namespace": service.get("eks_namespace", "pro"),
        "k8s_workload_kind": "Deployment" if mapping_status != "missing" else None,
        "k8s_workload_name": deployment if mapping_status != "missing" else None,
        "k8s_label_selector": "",
        "tags": service.get("tags", []),
    }


def collection_plan(
    service: dict,
    start: datetime,
    end: datetime,
    window_size: str,
    account_id: str,
    k8s_cluster: str,
) -> dict:
    deployment = service.get("eks_deployment")
    namespace = service.get("eks_namespace", "pro")
    mapping_status = service.get("k8s_mapping_status", "mapped" if deployment else "missing")
    pod_regex = f"{deployment}-.*" if mapping_status != "missing" else ""
    return {
        "service_id": service["service_id"],
        "window_size": window_size,
        "start": start.isoformat().replace("+00:00", "Z"),
        "end": end.isoformat().replace("+00:00", "Z"),
        "sources": {
            "newrelic": {
                "app_name": service.get("newrelic_app_name"),
                "app_id": service.get("newrelic_app_id"),
                "account_id": account_id,
                "entity_guid": newrelic_entity_guid(account_id, service.get("newrelic_app_id")),
                "signals": ["throughput", "error_rate", "latency_p95", "latency_p99"],
            },
            "prometheus_resources": {
                "status": mapping_status,
                "namespace": namespace,
                "pod_regex": pod_regex,
                "signals": [] if mapping_status == "missing" else [
                    "cpu_usage",
                    "memory_usage",
                    "cpu_throttling",
                    "network_receive",
                    "network_transmit",
                ],
            },
            "kubernetes": {
                "status": mapping_status,
                "cluster": k8s_cluster,
                "namespace": namespace,
                "workload_kind": "Deployment" if mapping_status != "missing" else None,
                "workload_name": deployment,
                "selector_discovery": "from_workload" if mapping_status != "missing" else "missing",
                "signals": [] if mapping_status == "missing" else [
                    "pod_phase",
                    "restart_count",
                    "oom_killed_count",
                    "probe_failure_count",
                    "rollout_status",
                    "events",
                ],
            },
            "github": {
                "repo": service["github_repo"],
                "branch": "master",
                "signals": ["latest_commits", "changed_files"],
            },
        },
    }


def window_task(service: dict, window_start: datetime, window_end: datetime, window_size: str, account_id: str) -> dict:
    deployment = service.get("eks_deployment")
    return {
        "service_id": service["service_id"],
        "window_start": window_start.isoformat().replace("+00:00", "Z"),
        "window_end": window_end.isoformat().replace("+00:00", "Z"),
        "window_size": window_size,
        "newrelic_app_id": service.get("newrelic_app_id"),
        "newrelic_entity_guid": newrelic_entity_guid(account_id, service.get("newrelic_app_id")),
        "github_repo": service.get("github_repo"),
        "k8s_namespace": service.get("eks_namespace", "pro"),
        "k8s_workload_name": deployment,
    }


def write_jsonl(path: Path, rows: list[dict]) -> None:
    with path.open("w", encoding="utf-8") as handle:
        for row in rows:
            handle.write(json.dumps(row, sort_keys=True) + "\n")


def main() -> None:
    args = parse_args()
    catalog = json.loads(Path(args.catalog_json).read_text(encoding="utf-8"))
    services = catalog["services"]
    end = align_time_to_window(parse_end_time(args.end), args.window_size)
    start = end - timedelta(days=args.days)
    window_count_per_service = sum(1 for _ in iter_windows(start, end, args.window_size))

    out_dir = Path(args.out_dir)
    out_dir.mkdir(parents=True, exist_ok=True)

    service_rows = [service_row(service, args.newrelic_account_id, args.k8s_cluster) for service in services]
    plans = [
        collection_plan(service, start, end, args.window_size, args.newrelic_account_id, args.k8s_cluster)
        for service in services
    ]

    samples: list[dict] = []
    for service in services:
        for window_start, window_end in iter_windows(start, end, args.window_size):
            if len(samples) >= args.sample_tasks:
                break
            samples.append(window_task(service, window_start, window_end, args.window_size, args.newrelic_account_id))
        if len(samples) >= args.sample_tasks:
            break

    summary = {
        "generated_at": datetime.now(timezone.utc).isoformat().replace("+00:00", "Z"),
        "catalog_json": args.catalog_json,
        "service_count": len(services),
        "start": start.isoformat().replace("+00:00", "Z"),
        "end": end.isoformat().replace("+00:00", "Z"),
        "days": args.days,
        "window_size": args.window_size,
        "window_count_per_service": window_count_per_service,
        "newrelic_account_id": args.newrelic_account_id,
        "k8s_cluster": args.k8s_cluster,
        "expected_metric_windows": window_count_per_service * len(services),
        "outputs": {
            "services_seed": "services_seed.jsonl",
            "collection_plan": "collection_plan.jsonl",
            "sample_window_tasks": "window_tasks.sample.jsonl",
        },
    }

    (out_dir / "summary.json").write_text(json.dumps(summary, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    write_jsonl(out_dir / "services_seed.jsonl", service_rows)
    write_jsonl(out_dir / "collection_plan.jsonl", plans)
    write_jsonl(out_dir / "window_tasks.sample.jsonl", samples)

    print(json.dumps(summary, indent=2, sort_keys=True))


if __name__ == "__main__":
    main()
