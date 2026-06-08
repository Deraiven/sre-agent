"""Scheduled collection runner for the SRE agent service."""

from __future__ import annotations

import json
import os
import subprocess
import sys
import threading
import time
from dataclasses import asdict, dataclass
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any

from .config import AgentConfig
from .db import psql_json
from .intelligence import mark_anomalies


WINDOW_SECONDS = {
    "5m": 5 * 60,
    "15m": 15 * 60,
    "1h": 60 * 60,
}


@dataclass
class RunResult:
    status: str
    started_at: str
    finished_at: str | None
    window_start: str
    window_end: str
    service_ids: list[str] | None
    returncode: int | None
    stdout: str
    stderr: str
    error: str | None = None


@dataclass
class GapRecoveryResult:
    status: str
    started_at: str
    finished_at: str | None
    scanned_start: str
    scanned_end: str
    candidate_windows: int
    recovered_ranges: list[dict[str, Any]]
    error: str | None = None


def utc_now() -> datetime:
    return datetime.now(timezone.utc)


def format_time(value: datetime) -> str:
    return value.astimezone(timezone.utc).isoformat().replace("+00:00", "Z")


def parse_time(value: str) -> datetime:
    parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=timezone.utc)
    return parsed.astimezone(timezone.utc)


def aligned_window_range(now: datetime, window_size: str, lookback_minutes: int) -> tuple[datetime, datetime]:
    window_seconds = WINDOW_SECONDS[window_size]
    timestamp = int(now.timestamp())
    end_ts = timestamp - (timestamp % window_seconds)
    end = datetime.fromtimestamp(end_ts, tz=timezone.utc)
    start = end - timedelta(minutes=lookback_minutes)
    return start, end


def window_step(window_size: str) -> timedelta:
    return timedelta(seconds=WINDOW_SECONDS[window_size])


def coalesce_windows(windows: list[datetime], window_size: str) -> list[tuple[datetime, datetime]]:
    if not windows:
        return []
    step = window_step(window_size)
    ordered = sorted(windows)
    ranges: list[tuple[datetime, datetime]] = []
    start = ordered[0]
    previous = ordered[0]
    for current in ordered[1:]:
        if current == previous + step:
            previous = current
            continue
        ranges.append((start, previous + step))
        start = current
        previous = current
    ranges.append((start, previous + step))
    return ranges


def find_recoverable_windows(config: AgentConfig, scan_start: datetime, scan_end: datetime) -> list[datetime]:
    sql = f"""
with service_count as (
  select count(*)::int as expected from services
),
expected_windows as (
  select generate_series(
    '{format_time(scan_start)}'::timestamptz,
    '{format_time(scan_end - window_step(config.window_size))}'::timestamptz,
    interval '{WINDOW_SECONDS[config.window_size]} seconds'
  ) as window_start
),
actual as (
  select
    window_start,
    count(*)::int as rows,
    count(*) filter (where newrelic->>'status' = 'collected')::int as newrelic_collected,
    count(*) filter (
      where kubernetes->>'status' = 'collected'
         or kubernetes->>'status' = 'missing'
         or kubernetes->>'status' = 'skipped'
    )::int as kubernetes_ok,
    count(*) filter (where kubernetes->>'status' = 'error')::int as kubernetes_error
  from service_metric_windows
  where window_size = '{config.window_size}'
    and window_start >= '{format_time(scan_start)}'
    and window_start < '{format_time(scan_end)}'
  group by window_start
)
select coalesce(json_agg(to_char(w.window_start, 'YYYY-MM-DD"T"HH24:MI:SS"Z"') order by w.window_start), '[]'::json)
from expected_windows w
cross join service_count sc
left join actual a on a.window_start = w.window_start
where coalesce(a.rows, 0) < sc.expected
   or coalesce(a.newrelic_collected, 0) < sc.expected
   or coalesce(a.kubernetes_ok, 0) < sc.expected
   or coalesce(a.kubernetes_error, 0) > 0;
"""
    values = psql_json(config.database_url, sql) or []
    return [parse_time(value) for value in values]


def run_collection(
    config: AgentConfig,
    start: datetime,
    end: datetime,
    service_ids: list[str] | None = None,
    dry_run: bool = False,
) -> RunResult:
    started_at = format_time(utc_now())
    command = [
        sys.executable,
        "scripts/collect_windows.py",
        "--plan",
        config.plan_path,
        "--database-url",
        config.database_url,
        "--start",
        format_time(start),
        "--end",
        format_time(end),
        "--window-size",
        config.window_size,
        "--batch-size",
        str(config.collect_batch_size),
    ]
    if config.prometheus_url:
        command.extend(["--prometheus-url", config.prometheus_url])
    if config.newrelic_graphql_url:
        command.extend(["--newrelic-graphql-url", config.newrelic_graphql_url])
    if config.kubectl_context:
        command.extend(["--kubectl-context", config.kubectl_context])
    if config.kubectl_aws_profile:
        command.extend(["--kubectl-aws-profile", config.kubectl_aws_profile])
    if config.kubectl_proxy_url:
        command.extend(["--kubectl-proxy-url", config.kubectl_proxy_url])
    if config.skip_github:
        command.append("--skip-github")
    if config.skip_kubernetes_events:
        command.append("--skip-kubernetes-events")
    if dry_run:
        command.append("--dry-run")
    for service_id in service_ids or []:
        command.extend(["--service", service_id])

    env = os.environ.copy()
    if config.newrelic_api_key:
        env["NEW_RELIC_API_KEY"] = config.newrelic_api_key

    try:
        completed = subprocess.run(
            command,
            cwd=Path(__file__).resolve().parents[1],
            env=env,
            text=True,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            timeout=max(300, config.runner_interval_seconds * 4),
        )
        status = "succeeded" if completed.returncode == 0 else "failed"
        return RunResult(
            status=status,
            started_at=started_at,
            finished_at=format_time(utc_now()),
            window_start=format_time(start),
            window_end=format_time(end),
            service_ids=service_ids,
            returncode=completed.returncode,
            stdout=completed.stdout.strip(),
            stderr=completed.stderr.strip(),
        )
    except Exception as exc:  # noqa: BLE001 - expose runner failures via status API.
        return RunResult(
            status="error",
            started_at=started_at,
            finished_at=format_time(utc_now()),
            window_start=format_time(start),
            window_end=format_time(end),
            service_ids=service_ids,
            returncode=None,
            stdout="",
            stderr="",
            error=str(exc),
        )


def run_gap_recovery(config: AgentConfig, realtime_start: datetime, realtime_end: datetime) -> GapRecoveryResult:
    started_at = format_time(utc_now())
    scan_end = realtime_start
    scan_start = scan_end - timedelta(hours=config.gap_lookback_hours)
    if scan_end <= scan_start:
        return GapRecoveryResult(
            status="skipped",
            started_at=started_at,
            finished_at=format_time(utc_now()),
            scanned_start=format_time(scan_start),
            scanned_end=format_time(scan_end),
            candidate_windows=0,
            recovered_ranges=[],
            error="empty_scan_range",
        )
    try:
        candidates = find_recoverable_windows(config, scan_start, scan_end)
        selected = candidates[-max(0, config.gap_max_windows_per_run) :]
        recovered = []
        for start, end in coalesce_windows(selected, config.window_size):
            result = run_collection(config, start, end)
            if result.status == "succeeded" and config.mark_anomalies_after_collection:
                try:
                    mark_anomalies(config.database_url, since=start, until=end)
                except Exception as exc:  # noqa: BLE001 - report but continue status.
                    result.error = f"post_collection_anomaly_marking_failed: {exc}"
            recovered.append(asdict(result))
        return GapRecoveryResult(
            status="succeeded",
            started_at=started_at,
            finished_at=format_time(utc_now()),
            scanned_start=format_time(scan_start),
            scanned_end=format_time(scan_end),
            candidate_windows=len(candidates),
            recovered_ranges=recovered,
        )
    except Exception as exc:  # noqa: BLE001 - expose recovery failures via status API.
        return GapRecoveryResult(
            status="error",
            started_at=started_at,
            finished_at=format_time(utc_now()),
            scanned_start=format_time(scan_start),
            scanned_end=format_time(scan_end),
            candidate_windows=0,
            recovered_ranges=[],
            error=str(exc),
        )


class ScheduledRunner:
    def __init__(self, config: AgentConfig) -> None:
        self.config = config
        self._thread: threading.Thread | None = None
        self._stop = threading.Event()
        self._lock = threading.Lock()
        self._running = False
        self.last_result: RunResult | None = None
        self.last_gap_recovery: GapRecoveryResult | None = None
        self.next_run_at: str | None = None

    def start(self) -> None:
        if self._thread and self._thread.is_alive():
            return
        self._stop.clear()
        self._thread = threading.Thread(target=self._loop, name="sre-agent-runner", daemon=True)
        self._thread.start()

    def stop(self) -> None:
        self._stop.set()

    def run_once(
        self,
        start: datetime | None = None,
        end: datetime | None = None,
        service_ids: list[str] | None = None,
        dry_run: bool = False,
    ) -> RunResult:
        if start is None or end is None:
            start, end = aligned_window_range(utc_now(), self.config.window_size, self.config.runner_lookback_minutes)
        with self._lock:
            if self._running:
                return RunResult(
                    status="busy",
                    started_at=format_time(utc_now()),
                    finished_at=format_time(utc_now()),
                    window_start=format_time(start),
                    window_end=format_time(end),
                    service_ids=service_ids,
                    returncode=None,
                    stdout="",
                    stderr="",
                    error="collection already running",
                )
            self._running = True
        try:
            result = run_collection(self.config, start, end, service_ids=service_ids, dry_run=dry_run)
            if (
                result.status == "succeeded"
                and not dry_run
                and self.config.mark_anomalies_after_collection
            ):
                try:
                    mark_anomalies(
                        self.config.database_url,
                        service_ids=service_ids,
                        since=start,
                        until=end,
                    )
                except Exception as exc:  # noqa: BLE001 - keep collection success visible.
                    result.error = f"post_collection_anomaly_marking_failed: {exc}"
            if (
                result.status == "succeeded"
                and not dry_run
                and service_ids is None
                and self.config.gap_recovery_enabled
                and self.config.gap_max_windows_per_run > 0
            ):
                self.last_gap_recovery = run_gap_recovery(self.config, start, end)
            self.last_result = result
            return result
        finally:
            with self._lock:
                self._running = False

    def status(self) -> dict[str, Any]:
        return {
            "enabled": self.config.runner_enabled,
            "running": self._running,
            "interval_seconds": self.config.runner_interval_seconds,
            "lookback_minutes": self.config.runner_lookback_minutes,
            "window_size": self.config.window_size,
            "gap_recovery_enabled": self.config.gap_recovery_enabled,
            "gap_lookback_hours": self.config.gap_lookback_hours,
            "gap_max_windows_per_run": self.config.gap_max_windows_per_run,
            "next_run_at": self.next_run_at,
            "last_result": asdict(self.last_result) if self.last_result else None,
            "last_gap_recovery": asdict(self.last_gap_recovery) if self.last_gap_recovery else None,
        }

    def _loop(self) -> None:
        while not self._stop.is_set():
            start, end = aligned_window_range(utc_now(), self.config.window_size, self.config.runner_lookback_minutes)
            self.run_once(start=start, end=end)
            next_run = utc_now() + timedelta(seconds=self.config.runner_interval_seconds)
            self.next_run_at = format_time(next_run)
            self._stop.wait(self.config.runner_interval_seconds)


def result_to_dict(result: RunResult) -> dict[str, Any]:
    return asdict(result)


def result_to_json(result: RunResult) -> str:
    return json.dumps(result_to_dict(result), sort_keys=True)
