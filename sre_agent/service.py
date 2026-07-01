"""HTTP service entrypoint for the SRE agent MVP."""

from __future__ import annotations

import argparse
import json
from datetime import timedelta
from dataclasses import asdict
from http import HTTPStatus
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from typing import Any
from urllib.parse import parse_qs, urlparse

from .config import load_config
from .db import psql_json, sql_text
from .incident_inspect import (
    IncidentInspectionRunner,
    add_incident_feedback,
    create_incident_inspection,
    finish_incident_inspection,
    get_incident_inspection,
    run_incident_inspection,
)
from .intelligence import build_baselines, data_coverage, data_gaps, mark_anomalies, score_service_risk, utc_now
from .ml_baseline import (
    add_risk_feedback_label,
    create_training_run,
    list_models,
    list_training_runs,
    model_drift_report,
    model_freshness_report,
    model_quality_report,
)
from .newrelic_trace import build_transaction_baselines
from .runner import ScheduledRunner, parse_time, result_to_dict
from .slo_recommendations import generate_slo_recommendations, list_slo_recommendations


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--host", default="127.0.0.1")
    parser.add_argument("--port", type=int, default=8080)
    return parser.parse_args()


class AgentRequestHandler(BaseHTTPRequestHandler):
    runner: ScheduledRunner
    inspector: IncidentInspectionRunner

    def log_message(self, format: str, *args: object) -> None:
        return

    def _send_json(self, status: HTTPStatus, payload: dict[str, Any]) -> None:
        body = json.dumps(payload, sort_keys=True).encode("utf-8")
        self.send_response(status)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def _read_json(self) -> dict[str, Any]:
        length = int(self.headers.get("Content-Length") or 0)
        if length == 0:
            return {}
        body = self.rfile.read(length).decode("utf-8")
        return json.loads(body) if body.strip() else {}

    def do_GET(self) -> None:
        parsed_url = urlparse(self.path)
        path = parsed_url.path
        query = parse_qs(parsed_url.query)
        try:
            if path == "/health":
                self._send_json(HTTPStatus.OK, {"status": "ok"})
                return
            if path == "/config":
                cfg = asdict(self.runner.config)
                cfg["newrelic_api_key"] = "***" if cfg.get("newrelic_api_key") else None
                self._send_json(HTTPStatus.OK, cfg)
                return
            if path == "/runner/status":
                status = self.runner.status()
                status["inspect_worker"] = self.inspector.status()
                self._send_json(HTTPStatus.OK, status)
                return
            if path == "/runner/runs":
                limit = max(1, min(int((query.get("limit") or ["20"])[0]), 100))
                sql = f"""
select coalesce(json_agg(row_to_json(r) order by started_at desc), '[]'::json)
from (
  select id, run_type, status, window_start, window_end, scan_start, scan_end,
         jobs_enqueued, jobs_succeeded, jobs_failed, metadata, error,
         started_at, finished_at
  from runner_runs
  order by started_at desc
  limit {limit}
) r;
"""
                self._send_json(HTTPStatus.OK, {"runner_runs": psql_json(self.runner.config.database_url, sql)})
                return
            if path == "/collection/jobs":
                limit = max(1, min(int((query.get("limit") or ["50"])[0]), 200))
                status_filter = (query.get("status") or [None])[0]
                where = f"where status = '{sql_text(status_filter)}'" if status_filter else ""
                sql = f"""
select coalesce(json_agg(row_to_json(j) order by created_at desc), '[]'::json)
from (
  select id, runner_run_id, job_type, status, priority, window_start, window_end,
         window_size, service_ids, dry_run, attempts, returncode, error,
         rows_emitted, rows_written, elapsed_seconds, created_at, started_at, finished_at
  from collection_jobs
  {where}
  order by created_at desc
  limit {limit}
) j;
"""
                self._send_json(HTTPStatus.OK, {"collection_jobs": psql_json(self.runner.config.database_url, sql)})
                return
            if path == "/data/coverage":
                until = parse_time((query.get("until") or [None])[0]) if query.get("until") else utc_now()
                since = (
                    parse_time((query.get("since") or [None])[0])
                    if query.get("since")
                    else until - timedelta(hours=int((query.get("lookback_hours") or ["24"])[0]))
                )
                self._send_json(
                    HTTPStatus.OK,
                    data_coverage(
                        self.runner.config.database_url,
                        since=since,
                        until=until,
                        service_id=(query.get("service_id") or [None])[0],
                        window_size=(query.get("window_size") or ["15m"])[0],
                    ),
                )
                return
            if path == "/gaps":
                until = parse_time((query.get("until") or [None])[0]) if query.get("until") else utc_now()
                since = (
                    parse_time((query.get("since") or [None])[0])
                    if query.get("since")
                    else until - timedelta(hours=int((query.get("lookback_hours") or ["24"])[0]))
                )
                self._send_json(
                    HTTPStatus.OK,
                    data_gaps(
                        self.runner.config.database_url,
                        since=since,
                        until=until,
                        service_id=(query.get("service_id") or [None])[0],
                        window_size=(query.get("window_size") or ["15m"])[0],
                        limit=int((query.get("limit") or ["100"])[0]),
                    ),
                )
                return
            if path.startswith("/inspect/incident/"):
                parts = [part for part in path.split("/") if part]
                if len(parts) == 3:
                    inspection = get_incident_inspection(self.runner.config.database_url, int(parts[2]))
                    if not inspection:
                        self._send_json(HTTPStatus.NOT_FOUND, {"error": "inspection not found"})
                        return
                    self._send_json(HTTPStatus.OK, {"inspection": inspection})
                    return
            if path == "/services":
                sql = """
select coalesce(json_agg(row_to_json(s) order by service_id), '[]'::json)
from (
  select service_id, owner, github_repo, newrelic_app_name, k8s_cluster,
         k8s_namespace, k8s_workload_kind, k8s_workload_name
  from services
) s;
"""
                self._send_json(HTTPStatus.OK, {"services": psql_json(self.runner.config.database_url, sql)})
                return
            if path == "/models/quality":
                service_ids = query.get("service_id")
                self._send_json(
                    HTTPStatus.OK,
                    model_quality_report(
                        self.runner.config.database_url,
                        service_ids=service_ids,
                        days=int((query.get("days") or ["30"])[0]),
                        window_size=(query.get("window_size") or ["15m"])[0],
                        model_version=(query.get("model_version") or ["seasonal-quantile-v1"])[0],
                    ),
                )
                return
            if path == "/models/freshness":
                service_ids = query.get("service_id")
                self._send_json(
                    HTTPStatus.OK,
                    model_freshness_report(
                        self.runner.config.database_url,
                        service_ids=service_ids,
                        model_version=(query.get("model_version") or [None])[0],
                        window_size=(query.get("window_size") or ["15m"])[0],
                        warning_hours=int((query.get("warning_hours") or ["24"])[0]),
                        critical_hours=int((query.get("critical_hours") or ["72"])[0]),
                    ),
                )
                return
            if path == "/models/drift":
                service_ids = query.get("service_id")
                self._send_json(
                    HTTPStatus.OK,
                    model_drift_report(
                        self.runner.config.database_url,
                        service_ids=service_ids,
                        lookback_hours=int((query.get("lookback_hours") or ["24"])[0]),
                        window_size=(query.get("window_size") or ["15m"])[0],
                        min_samples=int((query.get("min_samples") or ["12"])[0]),
                    ),
                )
                return
            if path == "/models":
                self._send_json(
                    HTTPStatus.OK,
                    {
                        "models": list_models(
                            self.runner.config.database_url,
                            service_id=(query.get("service_id") or [None])[0],
                            model_version=(query.get("model_version") or [None])[0],
                            active_only=(query.get("active_only") or ["false"])[0].lower() in {"1", "true", "yes"},
                            limit=int((query.get("limit") or ["100"])[0]),
                        )
                    },
                )
                return
            if path == "/models/training_runs":
                self._send_json(
                    HTTPStatus.OK,
                    {"training_runs": list_training_runs(self.runner.config.database_url, int((query.get("limit") or ["50"])[0]))},
                )
                return
            if path == "/slo/recommendations":
                self._send_json(
                    HTTPStatus.OK,
                    {
                        "recommendations": list_slo_recommendations(
                            self.runner.config.database_url,
                            service_id=(query.get("service_id") or [None])[0],
                            recommendation_version=(query.get("recommendation_version") or [None])[0],
                            status=(query.get("status") or [None])[0],
                            limit=int((query.get("limit") or ["100"])[0]),
                        )
                    },
                )
                return
            if path.startswith("/services/") and path.endswith("/risk"):
                parts = [part for part in path.split("/") if part]
                if len(parts) != 3:
                    self._send_json(HTTPStatus.NOT_FOUND, {"error": "not_found"})
                    return
                service_id = parts[1]
                lookback_hours = int((query.get("lookback_hours") or ["6"])[0])
                baseline_version = (query.get("baseline_version") or ["baseline-v1"])[0]
                until = parse_time((query.get("until") or [None])[0]) if query.get("until") else None
                since = parse_time((query.get("since") or [None])[0]) if query.get("since") else None
                self._send_json(
                    HTTPStatus.OK,
                    score_service_risk(
                        self.runner.config.database_url,
                        service_id,
                        lookback_hours=max(1, min(lookback_hours, 168)),
                        baseline_version=baseline_version,
                        since=since,
                        until=until,
                    ),
                )
                return
            self._send_json(HTTPStatus.NOT_FOUND, {"error": "not_found"})
        except Exception as exc:  # noqa: BLE001 - API should report operational errors.
            self._send_json(HTTPStatus.INTERNAL_SERVER_ERROR, {"error": str(exc)})

    def do_POST(self) -> None:
        path = urlparse(self.path).path
        try:
            payload = self._read_json()
            if path == "/collect/run":
                start = parse_time(payload["start"]) if payload.get("start") else None
                end = parse_time(payload["end"]) if payload.get("end") else None
                service_ids = payload.get("service_ids")
                if service_ids is not None and not isinstance(service_ids, list):
                    self._send_json(HTTPStatus.BAD_REQUEST, {"error": "service_ids must be a list"})
                    return
                result = self.runner.run_once(
                    start=start,
                    end=end,
                    service_ids=service_ids,
                    dry_run=bool(payload.get("dry_run", False)),
                )
                status = (
                    HTTPStatus.ACCEPTED
                    if result.status in {"queued", "succeeded", "busy"}
                    else HTTPStatus.INTERNAL_SERVER_ERROR
                )
                self._send_json(status, result_to_dict(result))
                return
            if path == "/runner/start":
                self.runner.start()
                self._send_json(HTTPStatus.OK, self.runner.status())
                return
            if path == "/runner/stop":
                self.runner.stop()
                self._send_json(HTTPStatus.OK, self.runner.status())
                return
            if path == "/baseline/recompute":
                service_ids = payload.get("service_ids")
                if service_ids is not None and not isinstance(service_ids, list):
                    self._send_json(HTTPStatus.BAD_REQUEST, {"error": "service_ids must be a list"})
                    return
                result = build_baselines(
                    self.runner.config.database_url,
                    service_ids=service_ids,
                    days=int(payload.get("days", 30)),
                    baseline_version=payload.get("baseline_version", "baseline-v1"),
                    min_bucket_samples=int(payload.get("min_bucket_samples", 12)),
                    min_precise_bucket_samples=int(payload.get("min_precise_bucket_samples", 3)),
                )
                self._send_json(HTTPStatus.OK, result)
                return
            if path == "/baseline/recompute_transactions":
                service_ids = payload.get("service_ids")
                if service_ids is not None and not isinstance(service_ids, list):
                    self._send_json(HTTPStatus.BAD_REQUEST, {"error": "service_ids must be a list"})
                    return
                result = build_transaction_baselines(
                    self.runner.config.database_url,
                    service_ids=service_ids,
                    days=int(payload.get("days", 30)),
                    baseline_version=payload.get("baseline_version", "baseline-v1"),
                    api_key=self.runner.config.newrelic_api_key,
                    graphql_url=self.runner.config.newrelic_graphql_url,
                    limit=int(payload.get("limit", 100)),
                )
                status = HTTPStatus.OK if result.get("status") == "succeeded" else HTTPStatus.INTERNAL_SERVER_ERROR
                self._send_json(status, result)
                return
            if path == "/slo/recommendations/generate":
                service_ids = payload.get("service_ids")
                if service_ids is not None and not isinstance(service_ids, list):
                    self._send_json(HTTPStatus.BAD_REQUEST, {"error": "service_ids must be a list"})
                    return
                result = generate_slo_recommendations(
                    self.runner.config.database_url,
                    days=int(payload.get("days", 30)),
                    window_size=payload.get("window_size", "15m"),
                    recommendation_version=payload.get("recommendation_version", "slo-rec-v1"),
                    baseline_version=payload.get("baseline_version", "baseline-v1"),
                    service_ids=service_ids,
                    replace=bool(payload.get("replace", False)),
                    dry_run=bool(payload.get("dry_run", False)),
                )
                self._send_json(HTTPStatus.OK, result)
                return
            if path == "/anomalies/mark":
                service_ids = payload.get("service_ids")
                if service_ids is not None and not isinstance(service_ids, list):
                    self._send_json(HTTPStatus.BAD_REQUEST, {"error": "service_ids must be a list"})
                    return
                since = parse_time(payload["since"]) if payload.get("since") else None
                until = parse_time(payload["until"]) if payload.get("until") else None
                result = mark_anomalies(
                    self.runner.config.database_url,
                    service_ids=service_ids,
                    since=since,
                    until=until,
                    baseline_version=payload.get("baseline_version", "baseline-v1"),
                )
                self._send_json(HTTPStatus.OK, result)
                return
            if path == "/models/train":
                service_ids = payload.get("service_ids")
                if service_ids is not None and not isinstance(service_ids, list):
                    self._send_json(HTTPStatus.BAD_REQUEST, {"error": "service_ids must be a list"})
                    return
                metric_names = payload.get("metric_names")
                if metric_names is not None and not isinstance(metric_names, list):
                    self._send_json(HTTPStatus.BAD_REQUEST, {"error": "metric_names must be a list"})
                    return
                result = create_training_run(
                    self.runner.config.database_url,
                    service_ids=service_ids,
                    metric_names=metric_names,
                    days=int(payload.get("days", 30)),
                    model_version=payload.get("model_version", "seasonal-quantile-v1"),
                    model_type=payload.get("model_type", "seasonal_quantile_v1"),
                    window_size=payload.get("window_size", "15m"),
                    dry_run=bool(payload.get("dry_run", True)),
                    activate=bool(payload.get("activate", False)),
                    min_coverage_pct=float(payload.get("min_coverage_pct", 70.0)),
                    min_bucket_samples=int(payload.get("min_bucket_samples", 12)),
                    min_precise_bucket_samples=int(payload.get("min_precise_bucket_samples", 3)),
                )
                self._send_json(HTTPStatus.ACCEPTED, result)
                return
            if path == "/risk/feedback":
                self._send_json(HTTPStatus.OK, {"feedback": add_risk_feedback_label(self.runner.config.database_url, payload)})
                return
            if path == "/risk/score":
                service_id = payload.get("service_id")
                if not service_id:
                    self._send_json(HTTPStatus.BAD_REQUEST, {"error": "service_id is required"})
                    return
                since = parse_time(payload["since"]) if payload.get("since") else None
                until = parse_time(payload["until"]) if payload.get("until") else None
                result = score_service_risk(
                    self.runner.config.database_url,
                    service_id,
                    lookback_hours=int(payload.get("lookback_hours", 6)),
                    baseline_version=payload.get("baseline_version", "baseline-v1"),
                    since=since,
                    until=until,
                )
                self._send_json(HTTPStatus.OK, result)
                return
            if path == "/inspect/incident":
                service_id = payload.get("service_id")
                if not service_id:
                    self._send_json(HTTPStatus.BAD_REQUEST, {"error": "service_id is required"})
                    return
                since = parse_time(payload["since"]) if payload.get("since") else None
                until = parse_time(payload["until"]) if payload.get("until") else None
                if payload.get("async", False):
                    inspection = self.inspector.enqueue(payload)
                    self._send_json(
                        HTTPStatus.ACCEPTED,
                        {
                            "inspection_id": inspection["id"],
                            "status": inspection["status"],
                            "service_id": inspection["service_id"],
                            "since": inspection["since"],
                            "until": inspection["until"],
                            "get_url": f"/inspect/incident/{inspection['id']}",
                        },
                    )
                    return
                if payload.get("rank_hypotheses", True):
                    inspect_until = until or utc_now()
                    inspect_since = since or (inspect_until - timedelta(hours=6))
                    inspection = create_incident_inspection(
                        self.runner.config.database_url,
                        service_id,
                        inspect_since,
                        inspect_until,
                        payload.get("baseline_version", "baseline-v1"),
                        {**payload, "async": False},
                        status="running",
                    )
                    claimed = get_incident_inspection(self.runner.config.database_url, int(inspection["id"]))
                    if not claimed:
                        self._send_json(HTTPStatus.INTERNAL_SERVER_ERROR, {"error": "inspection create failed"})
                        return
                    result = run_incident_inspection(self.runner.config, claimed)
                    finish_incident_inspection(
                        self.runner.config.database_url,
                        int(inspection["id"]),
                        "succeeded",
                        result=result,
                        summary=result.get("summary"),
                        timeline=result.get("timeline"),
                    )
                    self._send_json(HTTPStatus.OK, result)
                    return
                limit = int(payload.get("limit", 8))
                sql = f"""
select coalesce(json_agg(row_to_json(w) order by window_start desc), '[]'::json)
from (
  select service_id, window_start, window_end, newrelic, prometheus_resources,
         kubernetes, change_context, data_quality
  from service_metric_windows
  where service_id = '{sql_text(service_id)}'
  order by window_start desc
  limit {max(1, min(limit, 48))}
) w;
"""
                windows = psql_json(self.runner.config.database_url, sql)
                self._send_json(
                    HTTPStatus.OK,
                    {
                        "service_id": service_id,
                        "status": "evidence_collected",
                        "windows": windows,
                        "next_step": "rank root-cause hypotheses from these windows and related dependency signals",
                    },
                )
                return
            if path.startswith("/inspect/incident/") and path.endswith("/feedback"):
                parts = [part for part in path.split("/") if part]
                if len(parts) != 4:
                    self._send_json(HTTPStatus.NOT_FOUND, {"error": "not_found"})
                    return
                feedback = add_incident_feedback(self.runner.config.database_url, int(parts[2]), payload)
                self._send_json(HTTPStatus.OK, {"feedback": feedback})
                return
            self._send_json(HTTPStatus.NOT_FOUND, {"error": "not_found"})
        except KeyError as exc:
            self._send_json(HTTPStatus.BAD_REQUEST, {"error": f"missing field: {exc}"})
        except Exception as exc:  # noqa: BLE001 - API should report operational errors.
            self._send_json(HTTPStatus.INTERNAL_SERVER_ERROR, {"error": str(exc)})


def main() -> None:
    args = parse_args()
    config = load_config()
    runner = ScheduledRunner(config)
    inspector = IncidentInspectionRunner(config)
    AgentRequestHandler.runner = runner
    AgentRequestHandler.inspector = inspector
    runner.start_worker()
    inspector.start()
    if config.runner_enabled:
        runner.start()
    server = ThreadingHTTPServer((args.host, args.port), AgentRequestHandler)
    print(json.dumps({"status": "listening", "host": args.host, "port": args.port, "runner_enabled": config.runner_enabled}))
    try:
        server.serve_forever()
    except KeyboardInterrupt:
        runner.stop()
        inspector.stop()


if __name__ == "__main__":
    main()
