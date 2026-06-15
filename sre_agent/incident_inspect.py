"""Asynchronous incident inspection worker and persistence helpers."""

from __future__ import annotations

import threading
from datetime import datetime, timedelta
from typing import Any

from .config import AgentConfig
from .db import psql_exec, psql_json, sql_literal
from .intelligence import format_time, parse_time, rank_incident_hypotheses, utc_now
from .newrelic_trace import collect_service_trace_evidence


def create_incident_inspection(
    database_url: str,
    service_id: str,
    since: datetime,
    until: datetime,
    baseline_version: str,
    request_payload: dict[str, Any],
    status: str = "queued",
) -> dict[str, Any]:
    sql = """
insert into incident_inspections (
  service_id, status, attempts, since, until, baseline_version, request, started_at
) values (
  {service_id}, {status}, {attempts}, {since}, {until}, {baseline_version}, {request}::jsonb, {started_at}
) returning row_to_json(incident_inspections);
""".format(
        service_id=sql_literal(service_id),
        status=sql_literal(status),
        attempts=1 if status == "running" else 0,
        since=sql_literal(format_time(since)),
        until=sql_literal(format_time(until)),
        baseline_version=sql_literal(baseline_version),
        request=sql_literal(request_payload),
        started_at="now()" if status == "running" else "null",
    )
    return psql_json(database_url, sql)


def get_incident_inspection(database_url: str, inspection_id: int) -> dict[str, Any] | None:
    sql = """
select row_to_json(i)
from (
  select id, service_id, status, attempts, since, until, baseline_version, request,
         summary, timeline, result, error, started_at, finished_at, created_at, updated_at
  from incident_inspections
  where id = {inspection_id}
) i;
""".format(inspection_id=inspection_id)
    return psql_json(database_url, sql)


def claim_next_incident_inspection(database_url: str) -> dict[str, Any] | None:
    sql = """
with picked as (
  select id
  from incident_inspections
  where status = 'queued'
  order by created_at
  limit 1
  for update skip locked
)
update incident_inspections i
set status = 'running',
    attempts = attempts + 1,
    started_at = now(),
    updated_at = now()
from picked
where i.id = picked.id
returning row_to_json(i);
"""
    return psql_json(database_url, sql)


def reset_interrupted_incident_inspections(database_url: str) -> None:
    sql = """
update incident_inspections
set status = 'queued',
    error = coalesce(error || '; ', '') || 'inspect_worker_restarted_before_completion',
    updated_at = now()
where status = 'running';
"""
    psql_exec(database_url, sql)


def build_inspection_summary(result: dict[str, Any]) -> str:
    service_id = result.get("service_id")
    risk = result.get("risk") or {}
    hypotheses = result.get("hypotheses") or []
    top = hypotheses[0] if hypotheses else None
    if top:
        return (
            f"{service_id} risk={risk.get('risk_level')} score={risk.get('risk_score')}; "
            f"top hypothesis={top.get('hypothesis')} confidence={top.get('confidence')}."
        )
    return f"{service_id} risk={risk.get('risk_level')} score={risk.get('risk_score')}; no strong hypothesis found."


def build_inspection_timeline(result: dict[str, Any]) -> list[dict[str, Any]]:
    timeline: list[dict[str, Any]] = []
    for window in result.get("windows", []) or []:
        if not window.get("is_anomaly"):
            continue
        timeline.append(
            {
                "time": window.get("window_start"),
                "type": "anomaly_window",
                "severity": window.get("severity"),
                "score": window.get("score"),
                "anomaly_types": window.get("anomaly_types"),
            }
        )
    for hypothesis in result.get("hypotheses", [])[:3]:
        for item in hypothesis.get("evidence", [])[:3]:
            event_time = item.get("window_start") or item.get("trace_window_start")
            if event_time:
                timeline.append(
                    {
                        "time": event_time,
                        "type": "hypothesis_evidence",
                        "hypothesis": hypothesis.get("hypothesis"),
                        "evidence": item,
                    }
                )
    trace = result.get("trace_evidence") or {}
    if trace.get("status") in {"collected", "partial"}:
        timeline.append(
            {
                "time": trace.get("window_start"),
                "type": "trace_inspect",
                "status": trace.get("status"),
                "top_slow_transactions": [
                    (item.get("name") or item.get("facet"))
                    for item in (trace.get("top_slow_transactions") or [])[:5]
                ],
            }
        )
    return sorted(timeline, key=lambda item: item.get("time") or "")


def finish_incident_inspection(
    database_url: str,
    inspection_id: int,
    status: str,
    result: dict[str, Any] | None = None,
    summary: str | None = None,
    timeline: list[dict[str, Any]] | None = None,
    error: str | None = None,
) -> None:
    sql = """
update incident_inspections
set status = {status},
    result = {result}::jsonb,
    summary = {summary},
    timeline = {timeline}::jsonb,
    error = {error},
    finished_at = now(),
    updated_at = now()
where id = {inspection_id};
""".format(
        inspection_id=inspection_id,
        status=sql_literal(status),
        result=sql_literal(result or {}),
        summary=sql_literal(summary),
        timeline=sql_literal(timeline or []),
        error=sql_literal(error),
    )
    psql_exec(database_url, sql)


def run_incident_inspection(config: AgentConfig, inspection: dict[str, Any]) -> dict[str, Any]:
    request = inspection.get("request") or {}
    service_id = inspection["service_id"]
    since = parse_time(inspection["since"])
    until = parse_time(inspection["until"])
    baseline_version = inspection.get("baseline_version") or request.get("baseline_version", "baseline-v1")
    trace_evidence = None
    if request.get("include_trace", True):
        expand_minutes = int(request.get("trace_expand_minutes", 30))
        trace_evidence = collect_service_trace_evidence(
            config.database_url,
            service_id,
            since - timedelta(minutes=expand_minutes),
            until + timedelta(minutes=expand_minutes),
            config.newrelic_api_key,
            config.newrelic_graphql_url,
            baseline_version=baseline_version,
        )
    result = rank_incident_hypotheses(
        config.database_url,
        service_id,
        since=since,
        until=until,
        limit=int(request.get("limit", 16)),
        baseline_version=baseline_version,
        trace_evidence=trace_evidence,
    )
    result["inspection_id"] = inspection["id"]
    result["summary"] = build_inspection_summary(result)
    result["timeline"] = build_inspection_timeline(result)
    return result


def add_incident_feedback(
    database_url: str,
    inspection_id: int,
    payload: dict[str, Any],
) -> dict[str, Any]:
    inspection = get_incident_inspection(database_url, inspection_id)
    if not inspection:
        raise ValueError("inspection not found")
    sql = """
insert into incident_inspection_feedback (
  inspection_id, service_id, confirmed_root_cause, correct_hypothesis, usefulness, note, payload
) values (
  {inspection_id}, {service_id}, {confirmed_root_cause}, {correct_hypothesis},
  {usefulness}, {note}, {payload}::jsonb
) returning row_to_json(incident_inspection_feedback);
""".format(
        inspection_id=inspection_id,
        service_id=sql_literal(inspection["service_id"]),
        confirmed_root_cause=sql_literal(payload.get("confirmed_root_cause")),
        correct_hypothesis=sql_literal(payload.get("correct_hypothesis")),
        usefulness=sql_literal(payload.get("usefulness")),
        note=sql_literal(payload.get("note")),
        payload=sql_literal(payload),
    )
    return psql_json(database_url, sql)


class IncidentInspectionRunner:
    def __init__(self, config: AgentConfig) -> None:
        self.config = config
        self._thread: threading.Thread | None = None
        self._stop = threading.Event()
        self.current_inspection: dict[str, Any] | None = None

    def start(self) -> None:
        if self._thread and self._thread.is_alive():
            return
        reset_interrupted_incident_inspections(self.config.database_url)
        self._stop.clear()
        self._thread = threading.Thread(target=self._loop, name="sre-agent-inspect-worker", daemon=True)
        self._thread.start()

    def stop(self) -> None:
        self._stop.set()

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
    count(*) filter (where status = 'failed')::int as failed
  from incident_inspections
  where created_at >= now() - interval '24 hours'
) x;
""",
        ) or {}
        return {
            "worker_running": bool(self._thread and self._thread.is_alive()),
            "current_inspection": self.current_inspection,
            "inspection_counts_24h": counts,
        }

    def enqueue(self, payload: dict[str, Any]) -> dict[str, Any]:
        service_id = payload["service_id"]
        until = parse_time(payload["until"]) if payload.get("until") else utc_now()
        since = parse_time(payload["since"]) if payload.get("since") else until - timedelta(hours=6)
        baseline_version = payload.get("baseline_version", "baseline-v1")
        inspection = create_incident_inspection(
            self.config.database_url,
            service_id,
            since,
            until,
            baseline_version,
            payload,
        )
        self.start()
        return inspection

    def _loop(self) -> None:
        while not self._stop.is_set():
            inspection = claim_next_incident_inspection(self.config.database_url)
            if not inspection:
                self._stop.wait(2)
                continue
            self.current_inspection = inspection
            try:
                result = run_incident_inspection(self.config, inspection)
                finish_incident_inspection(
                    self.config.database_url,
                    int(inspection["id"]),
                    "succeeded",
                    result=result,
                    summary=result.get("summary"),
                    timeline=result.get("timeline"),
                )
            except Exception as exc:  # noqa: BLE001 - inspect failures are persisted.
                finish_incident_inspection(
                    self.config.database_url,
                    int(inspection["id"]),
                    "failed",
                    error=str(exc),
                )
            finally:
                self.current_inspection = None
