#!/usr/bin/env python3
"""Backfill historical metric gaps outside the realtime runner window.

This script is intended for cron or Kubernetes CronJob usage. It checks a
bounded historical range, runs the bulk 15m collector only when gaps exist, and
then exits. It deliberately does not enqueue runner collection jobs, so large
backfills cannot starve realtime collection workers.
"""

from __future__ import annotations

import argparse
import json
import os
import subprocess
import sys
from dataclasses import asdict, dataclass
from datetime import datetime, timedelta, timezone
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from sre_agent.config import load_config  # noqa: E402
from sre_agent.runner import (  # noqa: E402
    WINDOW_SECONDS,
    find_recoverable_windows,
    format_time,
    window_step,
)


@dataclass
class BackfillRange:
    start: datetime
    end: datetime
    candidate_windows: int
    missing_rows: int


def parse_args() -> argparse.Namespace:
    config = load_config()
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--history-days", type=int, default=config.historical_backfill_days)
    parser.add_argument(
        "--exclude-recent-hours",
        type=int,
        default=config.historical_backfill_exclude_recent_hours,
        help="Do not touch this recent range; the service runner owns it.",
    )
    parser.add_argument(
        "--max-range-hours",
        type=int,
        default=config.historical_backfill_max_range_hours,
        help="Split long missing spans into smaller bulk collector calls.",
    )
    parser.add_argument("--max-ranges", type=int, default=1)
    parser.add_argument("--database-url", default=config.database_url)
    parser.add_argument("--plan", default=config.plan_path)
    parser.add_argument("--prometheus-url", default=config.prometheus_url)
    parser.add_argument("--newrelic-graphql-url", default=config.newrelic_graphql_url)
    parser.add_argument("--batch-size", type=int, default=1000)
    parser.add_argument("--dry-run", action="store_true")
    return parser.parse_args()


def utc_now() -> datetime:
    return datetime.now(timezone.utc)


def align_down(value: datetime, window_size: str) -> datetime:
    seconds = WINDOW_SECONDS[window_size]
    timestamp = int(value.timestamp())
    return datetime.fromtimestamp(timestamp - (timestamp % seconds), tz=timezone.utc)


def chunk_ranges(candidates, window_size: str, max_range_hours: int) -> list[BackfillRange]:
    if not candidates:
        return []
    ordered = sorted(candidates, key=lambda item: item.window_start)
    step = window_step(window_size)
    max_duration = timedelta(hours=max_range_hours)
    ranges: list[BackfillRange] = []
    current = BackfillRange(
        start=ordered[0].window_start,
        end=ordered[0].window_end,
        candidate_windows=1,
        missing_rows=len(ordered[0].missing_service_ids),
    )
    for candidate in ordered[1:]:
        contiguous = candidate.window_start <= current.end + step
        within_limit = candidate.window_end - current.start <= max_duration
        if contiguous and within_limit:
            current.end = max(current.end, candidate.window_end)
            current.candidate_windows += 1
            current.missing_rows += len(candidate.missing_service_ids)
            continue
        ranges.append(current)
        current = BackfillRange(
            start=candidate.window_start,
            end=candidate.window_end,
            candidate_windows=1,
            missing_rows=len(candidate.missing_service_ids),
        )
    ranges.append(current)
    return ranges


def run_bulk_backfill(args: argparse.Namespace, selected: BackfillRange) -> dict:
    command = [
        sys.executable,
        "scripts/backfill_15m_bulk.py",
        "--plan",
        args.plan,
        "--database-url",
        args.database_url,
        "--start",
        format_time(selected.start),
        "--end",
        format_time(selected.end),
        "--batch-size",
        str(args.batch_size),
    ]
    if args.prometheus_url:
        command.extend(["--prometheus-url", args.prometheus_url])
    if args.newrelic_graphql_url:
        command.extend(["--newrelic-graphql-url", args.newrelic_graphql_url])
    if args.dry_run:
        command.append("--dry-run")

    completed = subprocess.run(
        command,
        cwd=ROOT,
        text=True,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        check=False,
    )
    return {
        "range": {
            "start": format_time(selected.start),
            "end": format_time(selected.end),
            "candidate_windows": selected.candidate_windows,
            "missing_rows": selected.missing_rows,
        },
        "command": command,
        "returncode": completed.returncode,
        "stdout": completed.stdout.strip()[-20000:],
        "stderr": completed.stderr.strip()[-20000:],
    }


def main() -> None:
    args = parse_args()
    config = load_config()
    config = config.__class__(
        **{
            **asdict(config),
            "database_url": args.database_url,
            "plan_path": args.plan,
            "prometheus_url": args.prometheus_url,
            "newrelic_graphql_url": args.newrelic_graphql_url,
            "backfill_skip_kubernetes": True,
        }
    )
    scan_end = align_down(utc_now() - timedelta(hours=args.exclude_recent_hours), config.window_size)
    scan_start = align_down(scan_end - timedelta(days=args.history_days), config.window_size)
    if scan_end <= scan_start:
        print(json.dumps({"status": "skipped", "reason": "empty_scan_range"}, sort_keys=True))
        return

    candidates = find_recoverable_windows(config, scan_start, scan_end, oldest_first=True)
    ranges = chunk_ranges(candidates, config.window_size, args.max_range_hours)
    selected_ranges = ranges[: max(0, args.max_ranges)]
    if not selected_ranges:
        print(
            json.dumps(
                {
                    "status": "noop",
                    "scan_start": format_time(scan_start),
                    "scan_end": format_time(scan_end),
                    "candidate_windows": 0,
                    "ranges": [],
                },
                sort_keys=True,
            )
        )
        return

    if args.dry_run:
        print(
            json.dumps(
                {
                    "status": "planned",
                    "dry_run": True,
                    "scan_start": format_time(scan_start),
                    "scan_end": format_time(scan_end),
                    "candidate_windows": len(candidates),
                    "planned_ranges": len(ranges),
                    "selected_ranges": [
                        {
                            "start": format_time(selected.start),
                            "end": format_time(selected.end),
                            "candidate_windows": selected.candidate_windows,
                            "missing_rows": selected.missing_rows,
                        }
                        for selected in selected_ranges
                    ],
                },
                sort_keys=True,
            )
        )
        return

    results = [run_bulk_backfill(args, selected) for selected in selected_ranges]
    status = "succeeded" if all(result["returncode"] == 0 for result in results) else "failed"
    print(
        json.dumps(
            {
                "status": status,
                "dry_run": args.dry_run,
                "scan_start": format_time(scan_start),
                "scan_end": format_time(scan_end),
                "candidate_windows": len(candidates),
                "planned_ranges": len(ranges),
                "executed_ranges": len(results),
                "results": results,
            },
            sort_keys=True,
        )
    )
    if status != "succeeded":
        sys.exit(1)


if __name__ == "__main__":
    main()
