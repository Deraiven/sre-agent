#!/usr/bin/env python3
"""Watch and restart the local SRE Agent service.

The watcher is intentionally small and dependency-free so it can be run from a
terminal while developing locally. It restarts the API process when it exits or
when the health endpoint stops responding.
"""

from __future__ import annotations

import argparse
import json
import os
import signal
import subprocess
import sys
import time
import urllib.request
from datetime import datetime, timezone
from pathlib import Path


DEFAULT_DATABASE_URL = "postgresql://sre_agent:sre_agent@localhost:5432/sre_agent"


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--host", default=os.environ.get("SRE_AGENT_HOST", "127.0.0.1"))
    parser.add_argument("--port", type=int, default=int(os.environ.get("SRE_AGENT_PORT", "8080")))
    parser.add_argument("--check-interval-seconds", type=float, default=15.0)
    parser.add_argument("--startup-grace-seconds", type=float, default=5.0)
    parser.add_argument("--health-timeout-seconds", type=float, default=5.0)
    parser.add_argument("--restart-backoff-seconds", type=float, default=5.0)
    parser.add_argument("--max-restart-backoff-seconds", type=float, default=60.0)
    parser.add_argument("--log-dir", default="logs")
    parser.add_argument("--skip-kubernetes", action="store_true", default=os.environ.get("SRE_AGENT_SKIP_KUBERNETES") == "true")
    parser.add_argument(
        "--skip-backfill-kubernetes",
        action=argparse.BooleanOptionalAction,
        default=os.environ.get("SRE_AGENT_BACKFILL_SKIP_KUBERNETES", "true").lower() in {"1", "true", "yes", "on"},
    )
    parser.add_argument("--kubectl-timeout-seconds", type=int, default=int(os.environ.get("SRE_AGENT_KUBECTL_TIMEOUT_SECONDS", "15")))
    return parser.parse_args()


def utc_now() -> str:
    return datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")


def log_event(log_file, event: str, **fields) -> None:
    payload = {"ts": utc_now(), "event": event, **fields}
    print(json.dumps(payload, sort_keys=True), flush=True)
    print(json.dumps(payload, sort_keys=True), file=log_file, flush=True)


def service_env(args: argparse.Namespace) -> dict[str, str]:
    env = os.environ.copy()
    env.setdefault("DATABASE_URL", DEFAULT_DATABASE_URL)
    env.setdefault("SRE_AGENT_SKIP_GITHUB", "true")
    env.setdefault("SRE_AGENT_SKIP_KUBERNETES_EVENTS", "true")
    env["SRE_AGENT_BACKFILL_SKIP_KUBERNETES"] = "true" if args.skip_backfill_kubernetes else "false"
    env["SRE_AGENT_KUBECTL_TIMEOUT_SECONDS"] = str(args.kubectl_timeout_seconds)
    if args.skip_kubernetes:
        env["SRE_AGENT_SKIP_KUBERNETES"] = "true"
    else:
        env["SRE_AGENT_SKIP_KUBERNETES"] = "false"
    return env


def service_command(args: argparse.Namespace) -> list[str]:
    return [
        sys.executable,
        "-m",
        "sre_agent.service",
        "--host",
        args.host,
        "--port",
        str(args.port),
    ]


def health_ok(args: argparse.Namespace) -> tuple[bool, str | None]:
    url = f"http://{args.host}:{args.port}/health"
    try:
        with urllib.request.urlopen(url, timeout=args.health_timeout_seconds) as response:
            if response.status != 200:
                return False, f"unexpected_status_{response.status}"
            payload = json.loads(response.read().decode("utf-8"))
            if payload.get("status") != "ok":
                return False, f"unexpected_payload_{payload}"
            return True, None
    except Exception as exc:  # noqa: BLE001 - watcher should report operational failures.
        return False, str(exc)


def terminate(process: subprocess.Popen, log_file, reason: str) -> None:
    if process.poll() is not None:
        return
    log_event(log_file, "service_terminate", pid=process.pid, reason=reason)
    process.terminate()
    try:
        process.wait(timeout=10)
    except subprocess.TimeoutExpired:
        log_event(log_file, "service_kill", pid=process.pid, reason=reason)
        process.kill()
        process.wait(timeout=10)


def main() -> int:
    args = parse_args()
    repo_root = Path(__file__).resolve().parents[1]
    log_dir = repo_root / args.log_dir
    log_dir.mkdir(parents=True, exist_ok=True)
    watcher_log_path = log_dir / "sre-agent-watchdog.jsonl"
    service_log_path = log_dir / "sre-agent-service.log"
    command = service_command(args)
    env = service_env(args)
    stop = False

    with watcher_log_path.open("a", encoding="utf-8") as watcher_log, service_log_path.open(
        "a", encoding="utf-8"
    ) as service_log:

        def handle_signal(signum, _frame) -> None:
            nonlocal stop
            stop = True
            log_event(watcher_log, "watchdog_signal", signal=signum)

        signal.signal(signal.SIGTERM, handle_signal)
        signal.signal(signal.SIGINT, handle_signal)

        process: subprocess.Popen | None = None
        backoff = args.restart_backoff_seconds
        log_event(
            watcher_log,
            "watchdog_start",
            command=command,
            skip_kubernetes=args.skip_kubernetes,
            skip_backfill_kubernetes=args.skip_backfill_kubernetes,
            kubectl_timeout_seconds=args.kubectl_timeout_seconds,
            health_url=f"http://{args.host}:{args.port}/health",
        )
        while not stop:
            if process is None or process.poll() is not None:
                if process is not None:
                    log_event(watcher_log, "service_exited", pid=process.pid, returncode=process.returncode)
                    time.sleep(backoff)
                    backoff = min(args.max_restart_backoff_seconds, max(args.restart_backoff_seconds, backoff * 2))
                process = subprocess.Popen(
                    command,
                    cwd=repo_root,
                    env=env,
                    stdout=service_log,
                    stderr=subprocess.STDOUT,
                    text=True,
                )
                log_event(watcher_log, "service_started", pid=process.pid, backoff_seconds=backoff)
                time.sleep(args.startup_grace_seconds)

            ok, error = health_ok(args)
            if ok:
                backoff = args.restart_backoff_seconds
            else:
                log_event(watcher_log, "health_failed", pid=process.pid, error=error)
                terminate(process, watcher_log, "health_failed")
                process = None
                continue
            time.sleep(args.check_interval_seconds)

        if process is not None:
            terminate(process, watcher_log, "watchdog_stopping")
        log_event(watcher_log, "watchdog_stopped")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
