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
from .intelligence import build_baselines, mark_anomalies, rank_incident_hypotheses, score_service_risk, utc_now
from .newrelic_trace import collect_service_trace_evidence
from .runner import ScheduledRunner, parse_time, result_to_dict


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--host", default="127.0.0.1")
    parser.add_argument("--port", type=int, default=8080)
    return parser.parse_args()


class AgentRequestHandler(BaseHTTPRequestHandler):
    runner: ScheduledRunner

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
                self._send_json(HTTPStatus.OK, self.runner.status())
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
            if path.startswith("/services/") and path.endswith("/risk"):
                parts = [part for part in path.split("/") if part]
                if len(parts) != 3:
                    self._send_json(HTTPStatus.NOT_FOUND, {"error": "not_found"})
                    return
                service_id = parts[1]
                lookback_hours = int((query.get("lookback_hours") or ["6"])[0])
                baseline_version = (query.get("baseline_version") or ["baseline-v1"])[0]
                self._send_json(
                    HTTPStatus.OK,
                    score_service_risk(
                        self.runner.config.database_url,
                        service_id,
                        lookback_hours=max(1, min(lookback_hours, 168)),
                        baseline_version=baseline_version,
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
                status = HTTPStatus.ACCEPTED if result.status in {"succeeded", "busy"} else HTTPStatus.INTERNAL_SERVER_ERROR
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
            if path == "/risk/score":
                service_id = payload.get("service_id")
                if not service_id:
                    self._send_json(HTTPStatus.BAD_REQUEST, {"error": "service_id is required"})
                    return
                result = score_service_risk(
                    self.runner.config.database_url,
                    service_id,
                    lookback_hours=int(payload.get("lookback_hours", 6)),
                    baseline_version=payload.get("baseline_version", "baseline-v1"),
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
                if payload.get("rank_hypotheses", True):
                    trace_evidence = None
                    if payload.get("include_trace", True):
                        trace_start = since or None
                        trace_end = until or None
                        if trace_start is None or trace_end is None:
                            trace_end = trace_end or utc_now()
                            trace_start = trace_start or (trace_end - timedelta(hours=6))
                        expand_minutes = int(payload.get("trace_expand_minutes", 30))
                        trace_evidence = collect_service_trace_evidence(
                            self.runner.config.database_url,
                            service_id,
                            trace_start - timedelta(minutes=expand_minutes),
                            trace_end + timedelta(minutes=expand_minutes),
                            self.runner.config.newrelic_api_key,
                            self.runner.config.newrelic_graphql_url,
                        )
                    self._send_json(
                        HTTPStatus.OK,
                        rank_incident_hypotheses(
                            self.runner.config.database_url,
                            service_id,
                            since=since,
                            until=until,
                            limit=int(payload.get("limit", 16)),
                            baseline_version=payload.get("baseline_version", "baseline-v1"),
                            trace_evidence=trace_evidence,
                        ),
                    )
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
            self._send_json(HTTPStatus.NOT_FOUND, {"error": "not_found"})
        except KeyError as exc:
            self._send_json(HTTPStatus.BAD_REQUEST, {"error": f"missing field: {exc}"})
        except Exception as exc:  # noqa: BLE001 - API should report operational errors.
            self._send_json(HTTPStatus.INTERNAL_SERVER_ERROR, {"error": str(exc)})


def main() -> None:
    args = parse_args()
    config = load_config()
    runner = ScheduledRunner(config)
    AgentRequestHandler.runner = runner
    if config.runner_enabled:
        runner.start()
    server = ThreadingHTTPServer((args.host, args.port), AgentRequestHandler)
    print(json.dumps({"status": "listening", "host": args.host, "port": args.port, "runner_enabled": config.runner_enabled}))
    try:
        server.serve_forever()
    except KeyboardInterrupt:
        runner.stop()


if __name__ == "__main__":
    main()
