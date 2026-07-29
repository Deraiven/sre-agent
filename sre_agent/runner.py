"""Scheduled collection runner and async collection worker."""

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
from zoneinfo import ZoneInfo

from .config import AgentConfig
from .db import psql, psql_exec, psql_json, sql_literal
from .intelligence import mark_anomalies
from .ml_baseline import (
    activate_model_version,
    create_model_training_scheduler_run,
    create_training_run,
    finish_model_training_scheduler_run,
    list_model_training_scheduler_runs,
    training_data_quality_gate,
)


WINDOW_SECONDS = {
    "5m": 5 * 60,
    "15m": 15 * 60,
    "1h": 60 * 60,
}
KUBERNETES_OK_STATUSES = ("collected", "events_only", "partial", "missing")


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
    job_ids: list[int] | None = None


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


@dataclass
class RecoverableWindow:
    window_start: datetime
    window_end: datetime
    rows: int
    expected: int
    missing_service_ids: list[str]


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


def iter_windows_for_range(start: datetime, end: datetime, window_size: str):
    step = window_step(window_size)
    cursor = start
    while cursor < end:
        window_end = min(cursor + step, end)
        yield cursor, window_end
        cursor = window_end


def chunked(values: list[str], size: int) -> list[list[str]]:
    return [values[index : index + size] for index in range(0, len(values), size)]


def list_service_ids(database_url: str) -> list[str]:
    sql = "select coalesce(json_agg(service_id order by service_id), '[]'::json) from services;"
    return psql_json(database_url, sql) or []


def find_recoverable_windows(
    config: AgentConfig,
    scan_start: datetime,
    scan_end: datetime,
    oldest_first: bool = False,
) -> list[RecoverableWindow]:
    window_order = "asc" if oldest_first else "desc"
    kubernetes_gap_check = "false" if config.backfill_skip_kubernetes else "true"
    sql = f"""
with services_ordered as (
  select service_id from services
),
service_count as (
  select count(*)::int as expected from services
),
expected_windows as (
  select generate_series(
    {sql_literal(format_time(scan_start))}::timestamptz,
    {sql_literal(format_time(scan_end - window_step(config.window_size)))}::timestamptz,
    interval '{WINDOW_SECONDS[config.window_size]} seconds'
  ) as window_start
),
actual as (
  select
    window_start,
    service_id,
    newrelic->>'status' as newrelic_status,
    prometheus_resources->>'status' as prometheus_status,
    kubernetes->>'status' as kubernetes_status
  from service_metric_windows
  where window_size = {sql_literal(config.window_size)}
    and window_start >= {sql_literal(format_time(scan_start))}
    and window_start < {sql_literal(format_time(scan_end))}
),
coverage as (
  select
    w.window_start,
    s.service_id,
    a.newrelic_status,
    a.prometheus_status,
    a.kubernetes_status,
    case
      when a.service_id is null then true
      when coalesce(a.newrelic_status, '') <> 'collected' then true
      when coalesce(a.prometheus_status, 'collected') not in ('collected', 'missing') then true
      when {kubernetes_gap_check} and coalesce(a.kubernetes_status, '') not in {KUBERNETES_OK_STATUSES} then true
      else false
    end as needs_recovery
  from expected_windows w
  cross join services_ordered s
  left join actual a on a.window_start = w.window_start and a.service_id = s.service_id
),
summary as (
  select
    window_start,
    count(*) filter (where newrelic_status is not null)::int as rows,
    array_agg(service_id order by service_id) filter (where needs_recovery) as missing_service_ids
  from coverage
  group by window_start
)
select coalesce(json_agg(row_to_json(x) order by x.missing_count desc, x.window_start {window_order}), '[]'::json)
from (
  select
    s.window_start,
    s.window_start + interval '{WINDOW_SECONDS[config.window_size]} seconds' as window_end,
    s.rows,
    sc.expected,
    coalesce(s.missing_service_ids, '{{}}'::text[]) as missing_service_ids,
    coalesce(array_length(s.missing_service_ids, 1), 0)::int as missing_count
  from summary s
  cross join service_count sc
  where coalesce(array_length(s.missing_service_ids, 1), 0) > 0
) x;
"""
    values = psql_json(config.database_url, sql) or []
    return [
        RecoverableWindow(
            window_start=parse_time(row["window_start"]),
            window_end=parse_time(row["window_end"]),
            rows=int(row["rows"] or 0),
            expected=int(row["expected"] or 0),
            missing_service_ids=row.get("missing_service_ids") or [],
        )
        for row in values
    ]


def create_runner_run(
    database_url: str,
    run_type: str,
    status: str,
    window_start: datetime | None = None,
    window_end: datetime | None = None,
    scan_start: datetime | None = None,
    scan_end: datetime | None = None,
    metadata: dict[str, Any] | None = None,
) -> int:
    sql = """
insert into runner_runs (
  run_type, status, window_start, window_end, scan_start, scan_end, metadata
) values (
  {run_type}, {status}, {window_start}, {window_end}, {scan_start}, {scan_end}, {metadata}::jsonb
) returning id;
""".format(
        run_type=sql_literal(run_type),
        status=sql_literal(status),
        window_start=sql_literal(format_time(window_start)) if window_start else "null",
        window_end=sql_literal(format_time(window_end)) if window_end else "null",
        scan_start=sql_literal(format_time(scan_start)) if scan_start else "null",
        scan_end=sql_literal(format_time(scan_end)) if scan_end else "null",
        metadata=sql_literal(metadata or {}),
    )
    return int(psql(database_url, sql))


def finish_runner_run(database_url: str, runner_run_id: int, status: str, error: str | None = None) -> None:
    sql = """
update runner_runs
set status = {status},
    error = {error},
    finished_at = case when {status} in ('queued', 'running') then null else now() end,
    jobs_enqueued = (
      select count(*)::int from collection_jobs where runner_run_id = {runner_run_id}
    ),
    jobs_succeeded = (
      select count(*)::int from collection_jobs where runner_run_id = {runner_run_id} and status = 'succeeded'
    ),
    jobs_failed = (
      select count(*)::int from collection_jobs where runner_run_id = {runner_run_id} and status in ('failed', 'error')
    )
where id = {runner_run_id};
""".format(
        runner_run_id=runner_run_id,
        status=sql_literal(status),
        error=sql_literal(error),
    )
    psql_exec(database_url, sql)


def refresh_runner_run_counts(database_url: str, runner_run_id: int | None) -> None:
    if runner_run_id is None:
        return
    sql = """
with counts as (
  select
    count(*)::int as total,
    count(*) filter (where status in ('queued', 'running'))::int as pending,
    count(*) filter (where status = 'succeeded')::int as succeeded,
    count(*) filter (where status in ('failed', 'error'))::int as failed
  from collection_jobs
  where runner_run_id = {runner_run_id}
)
update runner_runs r
set jobs_enqueued = counts.total,
    jobs_succeeded = counts.succeeded,
    jobs_failed = counts.failed,
    status = case
      when counts.pending > 0 then 'running'
      when counts.failed > 0 then 'failed'
      else 'succeeded'
    end,
    finished_at = case when counts.pending = 0 then now() else null end
from counts
where r.id = {runner_run_id};
""".format(runner_run_id=runner_run_id)
    psql_exec(database_url, sql)


def create_collection_job(
    config: AgentConfig,
    job_type: str,
    start: datetime,
    end: datetime,
    service_ids: list[str] | None = None,
    runner_run_id: int | None = None,
    priority: int = 0,
    dry_run: bool = False,
) -> int:
    service_array = "null"
    if service_ids is not None:
        service_array = "array[" + ",".join(sql_literal(service_id) for service_id in service_ids) + "]::text[]"
    existing_sql = """
select id
from collection_jobs
where status in ('queued', 'running')
  and job_type = {job_type}
  and window_start = {window_start}
  and window_end = {window_end}
  and window_size = {window_size}
  and service_ids is not distinct from {service_ids}
  and dry_run = {dry_run}
order by created_at
limit 1;
""".format(
        job_type=sql_literal(job_type),
        window_start=sql_literal(format_time(start)),
        window_end=sql_literal(format_time(end)),
        window_size=sql_literal(config.window_size),
        service_ids=service_array,
        dry_run=sql_literal(dry_run),
    )
    existing_id = psql(config.database_url, existing_sql)
    if existing_id:
        return int(existing_id)
    sql = """
insert into collection_jobs (
  runner_run_id, job_type, status, priority, window_start, window_end,
  window_size, service_ids, dry_run
) values (
  {runner_run_id}, {job_type}, 'queued', {priority}, {window_start}, {window_end},
  {window_size}, {service_ids}, {dry_run}
) returning id;
""".format(
        runner_run_id=sql_literal(runner_run_id),
        job_type=sql_literal(job_type),
        priority=priority,
        window_start=sql_literal(format_time(start)),
        window_end=sql_literal(format_time(end)),
        window_size=sql_literal(config.window_size),
        service_ids=service_array,
        dry_run=sql_literal(dry_run),
    )
    return int(psql(config.database_url, sql))


def enqueue_collection_jobs(
    config: AgentConfig,
    job_type: str,
    start: datetime,
    end: datetime,
    service_ids: list[str] | None = None,
    runner_run_id: int | None = None,
    priority: int = 0,
    dry_run: bool = False,
    service_chunk_size: int = 10,
) -> list[int]:
    selected_services = service_ids or list_service_ids(config.database_url)
    job_ids: list[int] = []
    for window_start, window_end in iter_windows_for_range(start, end, config.window_size):
        for service_chunk in chunked(selected_services, service_chunk_size):
            job_ids.append(
                create_collection_job(
                    config,
                    job_type,
                    window_start,
                    window_end,
                    service_ids=service_chunk,
                    runner_run_id=runner_run_id,
                    priority=priority,
                    dry_run=dry_run,
                )
            )
    return job_ids


def claim_next_collection_job(config: AgentConfig) -> dict[str, Any] | None:
    realtime_start, _ = aligned_window_range(utc_now(), config.window_size, config.runner_lookback_minutes)
    sql = """
with picked as (
  select id
  from collection_jobs
  where status = 'queued'
    order by
    case
      when job_type = 'manual' then 400
      when job_type = 'realtime' and window_end > {realtime_start} then 300
      when job_type = 'gap_recovery' then 200
      when job_type = 'realtime' then 100
      else 0
    end desc,
    priority desc,
    case when job_type = 'gap_recovery' then window_start end asc,
    case when job_type <> 'gap_recovery' then window_start end desc,
    created_at
  limit 1
  for update skip locked
)
update collection_jobs j
set status = 'running',
    started_at = now(),
    attempts = attempts + 1
from picked
where j.id = picked.id
returning row_to_json(j);
""".format(realtime_start=sql_literal(format_time(realtime_start)))
    return psql_json(config.database_url, sql)


def reset_interrupted_collection_jobs(database_url: str) -> None:
    sql = """
update collection_jobs
set status = 'queued',
    started_at = null,
    error = coalesce(error || '; ', '') || 'worker_restarted_before_completion'
where status = 'running';
"""
    psql_exec(database_url, sql)


def reset_stale_collection_jobs(database_url: str, stale_seconds: int) -> list[int]:
    sql = """
with stale as (
  select id
  from collection_jobs
  where status = 'running'
    and started_at is not null
    and started_at < now() - ({stale_seconds} || ' seconds')::interval
),
updated as (
  update collection_jobs j
  set status = 'queued',
      started_at = null,
      error = coalesce(error || '; ', '') || 'watchdog_requeued_stale_running_job'
  from stale
  where j.id = stale.id
  returning j.id
)
select coalesce(json_agg(id order by id), '[]'::json) from updated;
""".format(stale_seconds=max(1, int(stale_seconds)))
    return psql_json(database_url, sql) or []


def latest_metric_window(database_url: str, window_size: str) -> str | None:
    sql = """
select to_json(max(window_end))
from service_metric_windows
where window_size = {window_size};
""".format(window_size=sql_literal(window_size))
    return psql_json(database_url, sql)


def data_lag_minutes(database_url: str, window_size: str) -> float | None:
    latest = latest_metric_window(database_url, window_size)
    if not latest:
        return None
    latest_dt = parse_time(latest)
    _, aligned_end = aligned_window_range(utc_now(), window_size, 0)
    return max(0.0, (aligned_end - latest_dt).total_seconds() / 60.0)


def create_runner_watchdog_event(
    database_url: str,
    event_type: str,
    severity: str = "warning",
    action: str | None = None,
    details: dict[str, Any] | None = None,
) -> dict[str, Any]:
    sql = """
insert into runner_watchdog_events (event_type, severity, action, details)
values ({event_type}, {severity}, {action}, {details}::jsonb)
returning row_to_json(runner_watchdog_events);
""".format(
        event_type=sql_literal(event_type),
        severity=sql_literal(severity),
        action=sql_literal(action),
        details=sql_literal(details or {}),
    )
    return psql_json(database_url, sql) or {}


def list_runner_watchdog_events(database_url: str, limit: int = 20) -> list[dict[str, Any]]:
    sql = """
select coalesce(json_agg(row_to_json(e) order by created_at desc), '[]'::json)
from (
  select id, event_type, severity, action, details, created_at
  from runner_watchdog_events
  order by created_at desc
  limit {limit}
) e;
""".format(limit=max(1, min(limit, 100)))
    try:
        return psql_json(database_url, sql) or []
    except subprocess.CalledProcessError:
        return []


def parse_collection_stdout(stdout: str) -> dict[str, Any]:
    result: dict[str, Any] = {}
    for line in stdout.splitlines():
        line = line.strip()
        if not line:
            continue
        try:
            parsed = json.loads(line)
        except json.JSONDecodeError:
            continue
        result.update(parsed)
    return result


def update_collection_job_result(database_url: str, job_id: int, result: RunResult) -> None:
    parsed_stdout = parse_collection_stdout(result.stdout)
    sql = """
update collection_jobs
set status = {status},
    finished_at = now(),
    returncode = {returncode},
    stdout = {stdout},
    stderr = {stderr},
    error = {error},
    rows_emitted = {rows_emitted},
    rows_written = {rows_written},
    elapsed_seconds = {elapsed_seconds}
where id = {job_id};
""".format(
        job_id=job_id,
        status=sql_literal(result.status),
        returncode=sql_literal(result.returncode),
        stdout=sql_literal(result.stdout[-20000:]),
        stderr=sql_literal(result.stderr[-20000:]),
        error=sql_literal(result.error),
        rows_emitted=sql_literal(parsed_stdout.get("rows")),
        rows_written=sql_literal(parsed_stdout.get("written")),
        elapsed_seconds=sql_literal(parsed_stdout.get("elapsed_seconds")),
    )
    psql_exec(database_url, sql)


def run_collection(
    config: AgentConfig,
    start: datetime,
    end: datetime,
    service_ids: list[str] | None = None,
    dry_run: bool = False,
    skip_kubernetes: bool | None = None,
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
    if config.victorialogs_url:
        command.extend(["--victorialogs-url", config.victorialogs_url])
    if config.victorialogs_tenant:
        command.extend(["--victorialogs-tenant", config.victorialogs_tenant])
    if config.kubernetes_events_provider:
        command.extend(["--kubernetes-events-provider", config.kubernetes_events_provider])
    if config.victorialogs_kubernetes_events_query_template:
        command.extend(
            [
                "--victorialogs-kubernetes-events-query-template",
                config.victorialogs_kubernetes_events_query_template,
            ]
        )
    if config.skip_github:
        command.append("--skip-github")
    effective_skip_kubernetes = config.skip_kubernetes if skip_kubernetes is None else skip_kubernetes
    if effective_skip_kubernetes:
        command.append("--skip-kubernetes")
    if config.skip_kubernetes_events:
        command.append("--skip-kubernetes-events")
    command.extend(["--kubectl-timeout-seconds", str(config.kubectl_timeout_seconds)])
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
            timeout=config.collection_timeout_seconds,
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


def should_skip_kubernetes_for_job(config: AgentConfig, job_type: str, end: datetime) -> bool:
    if config.skip_kubernetes:
        return True
    if job_type != "realtime":
        return job_type == "gap_recovery" and config.backfill_skip_kubernetes

    realtime_start, _ = aligned_window_range(utc_now(), config.window_size, config.runner_lookback_minutes)
    is_fresh_realtime = end > realtime_start
    if is_fresh_realtime:
        return False
    return config.backfill_skip_kubernetes


def run_gap_recovery(config: AgentConfig, realtime_start: datetime, realtime_end: datetime, runner_run_id: int | None = None) -> GapRecoveryResult:
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
        oldest_first = config.gap_recovery_order != "newest"
        candidates = find_recoverable_windows(config, scan_start, scan_end, oldest_first=oldest_first)
        selected = candidates[: max(0, config.gap_max_windows_per_run)]
        recovered = []
        for candidate in selected:
            job_ids = enqueue_collection_jobs(
                config,
                "gap_recovery",
                candidate.window_start,
                candidate.window_end,
                service_ids=candidate.missing_service_ids,
                runner_run_id=runner_run_id,
                priority=50,
                service_chunk_size=config.gap_service_chunk_size,
            )
            recovered.append(
                {
                    "status": "queued",
                    "window_start": format_time(candidate.window_start),
                    "window_end": format_time(candidate.window_end),
                    "rows": candidate.rows,
                    "expected": candidate.expected,
                    "service_count": len(candidate.missing_service_ids),
                    "job_ids": job_ids,
                }
            )
        return GapRecoveryResult(
            status="queued",
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
        self._watchdog_thread: threading.Thread | None = None
        self._worker_threads: list[threading.Thread] = []
        self._stop = threading.Event()
        self._lock = threading.Lock()
        self._schedule_lock = threading.Lock()
        self._model_training_run_lock = threading.Lock()
        self.last_result: RunResult | None = None
        self.last_gap_recovery: GapRecoveryResult | None = None
        self.next_run_at: str | None = None
        self.current_jobs: dict[str, dict[str, Any]] = {}
        self.last_watchdog_event: dict[str, Any] | None = None
        self.last_watchdog_check_at: str | None = None
        self._last_watchdog_kick_window_end: str | None = None
        self._model_training_thread: threading.Thread | None = None
        self.last_model_training_result: dict[str, Any] | None = None
        self.last_model_training_check_at: str | None = None
        self.next_model_training_at: str | None = None

    def start(self) -> None:
        self._stop.clear()
        self.start_worker()
        if self._thread and self._thread.is_alive():
            self.start_watchdog()
            self.start_model_training_scheduler()
            return
        self._thread = threading.Thread(target=self._loop, name="sre-agent-scheduler", daemon=True)
        self._thread.start()
        self.start_watchdog()
        self.start_model_training_scheduler()

    def start_watchdog(self) -> None:
        if not self.config.runner_watchdog_enabled:
            return
        if self._watchdog_thread and self._watchdog_thread.is_alive():
            return
        self._watchdog_thread = threading.Thread(target=self._watchdog_loop, name="sre-agent-runner-watchdog", daemon=True)
        self._watchdog_thread.start()

    def start_model_training_scheduler(self) -> None:
        if not self.config.model_training_scheduler_enabled:
            return
        if self._model_training_thread and self._model_training_thread.is_alive():
            return
        self._model_training_thread = threading.Thread(
            target=self._model_training_scheduler_loop,
            name="sre-agent-model-training-scheduler",
            daemon=True,
        )
        self._model_training_thread.start()

    def start_worker(self) -> None:
        live_workers = [thread for thread in self._worker_threads if thread.is_alive()]
        if len(live_workers) >= self.config.worker_concurrency:
            self._worker_threads = live_workers
            return
        if not live_workers:
            reset_interrupted_collection_jobs(self.config.database_url)
        self._worker_threads = live_workers
        for idx in range(len(self._worker_threads), self.config.worker_concurrency):
            worker_name = f"sre-agent-worker-{idx + 1}"
            thread = threading.Thread(target=self._worker_loop, name=worker_name, args=(worker_name,), daemon=True)
            self._worker_threads.append(thread)
            thread.start()

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
        run_id = create_runner_run(
            self.config.database_url,
            "manual" if service_ids else "realtime",
            "queued",
            window_start=start,
            window_end=end,
            metadata={"dry_run": dry_run, "service_ids": service_ids},
        )
        job_ids = enqueue_collection_jobs(
            self.config,
            "manual" if service_ids else "realtime",
            start,
            end,
            service_ids=service_ids,
            runner_run_id=run_id,
            priority=100,
            dry_run=dry_run,
        )
        finish_runner_run(self.config.database_url, run_id, "queued")
        result = RunResult(
            status="queued",
            started_at=format_time(utc_now()),
            finished_at=format_time(utc_now()),
            window_start=format_time(start),
            window_end=format_time(end),
            service_ids=service_ids,
            returncode=None,
            stdout=json.dumps({"runner_run_id": run_id, "job_ids": job_ids}),
            stderr="",
            job_ids=job_ids,
        )
        self.last_result = result
        self.start_worker()
        return result

    def status(self) -> dict[str, Any]:
        counts = psql_json(
            self.config.database_url,
            """
select row_to_json(x)
from (
  select
    count(*) filter (where status = 'queued')::int as queued,
    count(*) filter (where status = 'running')::int as running,
    count(*) filter (where status = 'succeeded')::int as succeeded,
    count(*) filter (where status in ('failed', 'error'))::int as failed
  from collection_jobs
  where created_at >= now() - interval '24 hours'
) x;
""",
        ) or {}
        return {
            "enabled": self.config.runner_enabled,
            "scheduler_thread_running": bool(self._thread and self._thread.is_alive()),
            "running": bool(self.current_jobs),
            "worker_running": any(thread.is_alive() for thread in self._worker_threads),
            "worker_concurrency": self.config.worker_concurrency,
            "collection_timeout_seconds": self.config.collection_timeout_seconds,
            "worker_count": sum(1 for thread in self._worker_threads if thread.is_alive()),
            "interval_seconds": self.config.runner_interval_seconds,
            "lookback_minutes": self.config.runner_lookback_minutes,
            "window_size": self.config.window_size,
            "gap_recovery_enabled": self.config.gap_recovery_enabled,
            "gap_lookback_hours": self.config.gap_lookback_hours,
            "gap_max_windows_per_run": self.config.gap_max_windows_per_run,
            "gap_service_chunk_size": self.config.gap_service_chunk_size,
            "gap_recovery_order": self.config.gap_recovery_order,
            "skip_kubernetes": self.config.skip_kubernetes,
            "backfill_skip_kubernetes": self.config.backfill_skip_kubernetes,
            "watchdog_enabled": self.config.runner_watchdog_enabled,
            "watchdog_running": bool(self._watchdog_thread and self._watchdog_thread.is_alive()),
            "watchdog_interval_seconds": self.config.runner_watchdog_interval_seconds,
            "watchdog_schedule_grace_seconds": self.config.runner_watchdog_schedule_grace_seconds,
            "watchdog_data_lag_minutes": self.config.runner_watchdog_data_lag_minutes,
            "watchdog_stale_job_seconds": self.config.runner_watchdog_stale_job_seconds,
            "last_watchdog_check_at": self.last_watchdog_check_at,
            "last_watchdog_event": self.last_watchdog_event,
            "recent_watchdog_events": list_runner_watchdog_events(self.config.database_url, limit=10),
            "model_training_scheduler_enabled": self.config.model_training_scheduler_enabled,
            "model_training_scheduler_running": bool(
                self._model_training_thread and self._model_training_thread.is_alive()
            ),
            "model_training_daily_at": self.config.model_training_daily_at,
            "model_training_timezone": self.config.model_training_timezone,
            "model_training_days": self.config.model_training_days,
            "model_training_min_coverage_pct": self.config.model_training_min_coverage_pct,
            "next_model_training_at": self.next_model_training_at,
            "last_model_training_check_at": self.last_model_training_check_at,
            "last_model_training_result": self.last_model_training_result,
            "recent_model_training_scheduler_runs": list_model_training_scheduler_runs(self.config.database_url, limit=5),
            "next_run_at": self.next_run_at,
            "current_job": next(iter(self.current_jobs.values()), None),
            "current_jobs": list(self.current_jobs.values()),
            "job_counts_24h": counts,
            "last_result": asdict(self.last_result) if self.last_result else None,
            "last_gap_recovery": asdict(self.last_gap_recovery) if self.last_gap_recovery else None,
        }

    def _loop(self) -> None:
        while not self._stop.is_set():
            try:
                with self._schedule_lock:
                    start, end = aligned_window_range(utc_now(), self.config.window_size, self.config.runner_lookback_minutes)
                    run_id = create_runner_run(
                        self.config.database_url,
                        "scheduled",
                        "queued",
                        window_start=start,
                        window_end=end,
                    )
                    job_ids = enqueue_collection_jobs(
                        self.config,
                        "realtime",
                        start,
                        end,
                        runner_run_id=run_id,
                        priority=100,
                    )
                    self.last_result = RunResult(
                        status="queued",
                        started_at=format_time(utc_now()),
                        finished_at=format_time(utc_now()),
                        window_start=format_time(start),
                        window_end=format_time(end),
                        service_ids=None,
                        returncode=None,
                        stdout=json.dumps({"runner_run_id": run_id, "job_ids": job_ids}),
                        stderr="",
                        job_ids=job_ids,
                    )
                    if self.config.gap_recovery_enabled and self.config.gap_max_windows_per_run > 0:
                        self.last_gap_recovery = run_gap_recovery(self.config, start, end, runner_run_id=run_id)
                    finish_runner_run(self.config.database_url, run_id, "queued")
                    next_run = utc_now() + timedelta(seconds=self.config.runner_interval_seconds)
                    self.next_run_at = format_time(next_run)
                self._stop.wait(self.config.runner_interval_seconds)
            except Exception as exc:  # noqa: BLE001 - watchdog should recover scheduler loop failures.
                self._record_watchdog_event(
                    "scheduler_loop_error",
                    severity="critical",
                    action="scheduler_loop_retry",
                    details={"error": str(exc)},
                )
                self._stop.wait(min(60, self.config.runner_watchdog_interval_seconds))

    def _record_watchdog_event(
        self,
        event_type: str,
        severity: str = "warning",
        action: str | None = None,
        details: dict[str, Any] | None = None,
    ) -> None:
        try:
            event = create_runner_watchdog_event(
                self.config.database_url,
                event_type,
                severity=severity,
                action=action,
                details=details or {},
            )
        except Exception as exc:  # noqa: BLE001 - status should still expose in-memory watchdog failures.
            event = {
                "event_type": event_type,
                "severity": severity,
                "action": action,
                "details": {**(details or {}), "record_error": str(exc)},
                "created_at": format_time(utc_now()),
            }
        self.last_watchdog_event = event

    def _job_counts(self) -> dict[str, int]:
        return psql_json(
            self.config.database_url,
            """
select row_to_json(x)
from (
  select
    count(*) filter (where status = 'queued')::int as queued,
    count(*) filter (where status = 'running')::int as running
  from collection_jobs
  where created_at >= now() - interval '24 hours'
) x;
""",
        ) or {"queued": 0, "running": 0}

    def _kick_realtime_collection(self, reason: str, details: dict[str, Any]) -> None:
        start, end = aligned_window_range(utc_now(), self.config.window_size, self.config.runner_lookback_minutes)
        window_end = format_time(end)
        if self._last_watchdog_kick_window_end == window_end:
            return
        with self._schedule_lock:
            self._last_watchdog_kick_window_end = window_end
            result = self.run_once(start=start, end=end)
        self._record_watchdog_event(
            reason,
            severity="critical",
            action="queued_realtime_collection",
            details={
                **details,
                "window_start": result.window_start,
                "window_end": result.window_end,
                "job_ids": result.job_ids or [],
            },
        )

    def _watchdog_loop(self) -> None:
        while not self._stop.is_set():
            self.last_watchdog_check_at = format_time(utc_now())
            try:
                if not self.config.runner_enabled:
                    self._stop.wait(self.config.runner_watchdog_interval_seconds)
                    continue

                if not (self._thread and self._thread.is_alive()):
                    self._thread = threading.Thread(target=self._loop, name="sre-agent-scheduler", daemon=True)
                    self._thread.start()
                    self._record_watchdog_event(
                        "scheduler_thread_not_running",
                        severity="critical",
                        action="scheduler_thread_restarted",
                    )

                live_worker_count = sum(1 for thread in self._worker_threads if thread.is_alive())
                if live_worker_count < self.config.worker_concurrency:
                    self.start_worker()
                    self._record_watchdog_event(
                        "worker_count_below_concurrency",
                        severity="warning",
                        action="workers_restarted",
                        details={"live_worker_count": live_worker_count, "target": self.config.worker_concurrency},
                    )

                stale_job_ids = reset_stale_collection_jobs(
                    self.config.database_url,
                    self.config.runner_watchdog_stale_job_seconds,
                )
                if stale_job_ids:
                    self.start_worker()
                    self._record_watchdog_event(
                        "stale_running_jobs",
                        severity="critical",
                        action="requeued_stale_jobs",
                        details={"job_ids": stale_job_ids, "stale_seconds": self.config.runner_watchdog_stale_job_seconds},
                    )

                counts = self._job_counts()
                queued = int(counts.get("queued") or 0)
                running = int(counts.get("running") or 0)
                if self.next_run_at:
                    next_run_at = parse_time(self.next_run_at)
                    overdue_seconds = (utc_now() - next_run_at).total_seconds()
                    if overdue_seconds > self.config.runner_watchdog_schedule_grace_seconds and queued == 0 and running == 0:
                        self._kick_realtime_collection(
                            "scheduler_next_run_overdue",
                            {
                                "next_run_at": self.next_run_at,
                                "overdue_seconds": overdue_seconds,
                                "queued": queued,
                                "running": running,
                            },
                        )

                lag_minutes = data_lag_minutes(self.config.database_url, self.config.window_size)
                if (
                    lag_minutes is not None
                    and lag_minutes > self.config.runner_watchdog_data_lag_minutes
                    and queued == 0
                    and running == 0
                ):
                    self._kick_realtime_collection(
                        "metric_window_lag_exceeded",
                        {
                            "lag_minutes": lag_minutes,
                            "threshold_minutes": self.config.runner_watchdog_data_lag_minutes,
                            "latest_window": latest_metric_window(self.config.database_url, self.config.window_size),
                        },
                    )
            except Exception as exc:  # noqa: BLE001 - watchdog should stay alive.
                self._record_watchdog_event(
                    "watchdog_error",
                    severity="critical",
                    action="watchdog_continue",
                    details={"error": str(exc)},
                )
            self._stop.wait(self.config.runner_watchdog_interval_seconds)

    def _next_model_training_time(self, after: datetime | None = None) -> datetime:
        after = after or utc_now()
        try:
            hour_text, minute_text = self.config.model_training_daily_at.split(":", 1)
            hour = int(hour_text)
            minute = int(minute_text)
            if not (0 <= hour <= 23 and 0 <= minute <= 59):
                raise ValueError
        except ValueError:
            hour = 4
            minute = 0
        try:
            local_tz = ZoneInfo(self.config.model_training_timezone)
        except Exception:  # noqa: BLE001 - bad timezone should fall back predictably.
            local_tz = timezone.utc
        local_after = after.astimezone(local_tz)
        candidate = local_after.replace(hour=hour, minute=minute, second=0, microsecond=0)
        if candidate <= local_after:
            candidate += timedelta(days=1)
        return candidate.astimezone(timezone.utc)

    def _model_training_model_version(self, scheduled_for: datetime) -> str:
        return f"seasonal-quantile-auto-{scheduled_for.strftime('%Y%m%d%H%M')}"

    def run_model_training_once(
        self,
        model_version: str | None = None,
        scheduled_for: datetime | None = None,
        trigger_source: str = "service_scheduler",
    ) -> dict[str, Any]:
        scheduled_for = scheduled_for or utc_now()
        model_version = model_version or self._model_training_model_version(scheduled_for)
        scheduler_run = create_model_training_scheduler_run(
            self.config.database_url,
            model_version=model_version,
            scheduled_for=scheduled_for,
            trigger_source=trigger_source,
        )
        scheduler_run_id = int(scheduler_run["id"])
        if not self._model_training_run_lock.acquire(blocking=False):
            result = finish_model_training_scheduler_run(
                self.config.database_url,
                scheduler_run_id,
                "blocked_concurrent_run",
                error="another model training scheduler run is already active",
            )
            self.last_model_training_result = result
            return {"status": "blocked_concurrent_run", "scheduler_run": result}
        try:
            precheck = training_data_quality_gate(
                self.config.database_url,
                days=self.config.model_training_days,
                window_size=self.config.window_size,
                min_coverage_pct=self.config.model_training_min_coverage_pct,
                max_latest_lag_minutes=self.config.runner_watchdog_data_lag_minutes,
                min_source_success_pct=95.0,
            )
            if not precheck.get("eligible"):
                result = finish_model_training_scheduler_run(
                    self.config.database_url,
                    scheduler_run_id,
                    "blocked_precheck",
                    precheck=precheck,
                    error="training data quality gate blocked automatic training",
                )
                self.last_model_training_result = result
                return {"status": "blocked_precheck", "scheduler_run": result, "precheck": precheck}

            training = create_training_run(
                self.config.database_url,
                days=self.config.model_training_days,
                model_version=model_version,
                model_type="seasonal_quantile_v1",
                window_size=self.config.window_size,
                dry_run=False,
                activate=False,
                min_coverage_pct=self.config.model_training_min_coverage_pct,
                min_bucket_samples=self.config.model_training_min_bucket_samples,
                min_precise_bucket_samples=self.config.model_training_min_precise_bucket_samples,
            )
            training_run_id = int(training["training_run"]["id"])
            activation = activate_model_version(
                self.config.database_url,
                model_version=model_version,
                policy=self.config.model_training_activation_policy,
                force=False,
            )
            activation_event_id = None
            if activation.get("event") and activation["event"].get("id"):
                activation_event_id = int(activation["event"]["id"])
            final_status = "activated" if activation.get("status") == "activated" else "blocked_activation"
            result = finish_model_training_scheduler_run(
                self.config.database_url,
                scheduler_run_id,
                final_status,
                precheck=precheck,
                training_run_id=training_run_id,
                activation_result=activation,
                activation_event_id=activation_event_id,
                error=None if final_status == "activated" else "activation gate blocked trained candidate model",
            )
            self.last_model_training_result = result
            return {
                "status": final_status,
                "scheduler_run": result,
                "precheck": precheck,
                "training": training,
                "activation": activation,
            }
        except Exception as exc:  # noqa: BLE001 - scheduler should audit failures and continue.
            result = finish_model_training_scheduler_run(
                self.config.database_url,
                scheduler_run_id,
                "error",
                error=str(exc),
            )
            self.last_model_training_result = result
            return {"status": "error", "scheduler_run": result, "error": str(exc)}
        finally:
            self._model_training_run_lock.release()

    def _model_training_scheduler_loop(self) -> None:
        if self.config.model_training_startup_delay_seconds:
            self._stop.wait(self.config.model_training_startup_delay_seconds)
        while not self._stop.is_set():
            try:
                next_run = self._next_model_training_time()
                self.next_model_training_at = format_time(next_run)
                while not self._stop.is_set():
                    wait_seconds = (next_run - utc_now()).total_seconds()
                    if wait_seconds <= 0:
                        break
                    # Poll wall-clock time instead of one long Event.wait().
                    # Long waits can remain asleep across local laptop sleep/wake,
                    # leaving next_model_training_at in the past while the thread
                    # still appears healthy.
                    if self._stop.wait(min(wait_seconds, 60.0)):
                        return
                self.last_model_training_check_at = format_time(utc_now())
                self.run_model_training_once(scheduled_for=next_run)
            except Exception as exc:  # noqa: BLE001 - keep scheduler alive.
                self.last_model_training_result = {
                    "status": "error",
                    "error": str(exc),
                    "created_at": format_time(utc_now()),
                }
                self._stop.wait(60)

    def _worker_loop(self, worker_name: str) -> None:
        while not self._stop.is_set():
            job = claim_next_collection_job(self.config)
            if not job:
                self._stop.wait(2)
                continue
            with self._lock:
                tracked_job = dict(job)
                tracked_job["worker_name"] = worker_name
                self.current_jobs[worker_name] = tracked_job
            try:
                start = parse_time(job["window_start"])
                end = parse_time(job["window_end"])
                service_ids = job.get("service_ids")
                skip_kubernetes = should_skip_kubernetes_for_job(self.config, str(job.get("job_type")), end)
                with self._lock:
                    if worker_name in self.current_jobs:
                        self.current_jobs[worker_name]["skip_kubernetes"] = skip_kubernetes
                result = run_collection(
                    self.config,
                    start,
                    end,
                    service_ids=service_ids,
                    dry_run=bool(job.get("dry_run")),
                    skip_kubernetes=skip_kubernetes,
                )
                update_collection_job_result(self.config.database_url, int(job["id"]), result)
                refresh_runner_run_counts(self.config.database_url, job.get("runner_run_id"))
                if result.status == "succeeded" and not job.get("dry_run") and self.config.mark_anomalies_after_collection:
                    try:
                        mark_anomalies(self.config.database_url, service_ids=service_ids, since=start, until=end)
                    except Exception as exc:  # noqa: BLE001 - keep collection success visible.
                        psql_exec(
                            self.config.database_url,
                            """
update collection_jobs
set error = coalesce(error || '; ', '') || {error}
where id = {job_id};
""".format(
                                job_id=int(job["id"]),
                                error=sql_literal(f"post_collection_anomaly_marking_failed: {exc}"),
                            ),
                        )
                        refresh_runner_run_counts(self.config.database_url, job.get("runner_run_id"))
            finally:
                with self._lock:
                    self.current_jobs.pop(worker_name, None)


def result_to_dict(result: RunResult) -> dict[str, Any]:
    return asdict(result)


def result_to_json(result: RunResult) -> str:
    return json.dumps(result_to_dict(result), sort_keys=True)
