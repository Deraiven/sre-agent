"""Configuration helpers for the SRE agent service."""

from __future__ import annotations

import os
from dataclasses import dataclass


@dataclass(frozen=True)
class AgentConfig:
    database_url: str
    plan_path: str
    runner_enabled: bool
    runner_interval_seconds: int
    runner_lookback_minutes: int
    window_size: str
    collect_batch_size: int
    worker_concurrency: int
    collection_timeout_seconds: int
    prometheus_url: str | None
    newrelic_api_key: str | None
    newrelic_graphql_url: str | None
    kubectl_context: str | None
    kubectl_aws_profile: str | None
    kubectl_proxy_url: str | None
    victorialogs_url: str | None
    victorialogs_tenant: str | None
    kubernetes_events_provider: str
    victorialogs_kubernetes_events_query_template: str | None
    skip_github: bool
    skip_kubernetes: bool
    backfill_skip_kubernetes: bool
    skip_kubernetes_events: bool
    kubectl_timeout_seconds: int
    mark_anomalies_after_collection: bool
    gap_recovery_enabled: bool
    gap_lookback_hours: int
    gap_max_windows_per_run: int
    gap_service_chunk_size: int
    gap_recovery_order: str
    historical_backfill_days: int
    historical_backfill_exclude_recent_hours: int
    historical_backfill_max_range_hours: int
    runner_watchdog_enabled: bool
    runner_watchdog_interval_seconds: int
    runner_watchdog_schedule_grace_seconds: int
    runner_watchdog_data_lag_minutes: int
    runner_watchdog_stale_job_seconds: int
    model_training_scheduler_enabled: bool
    model_training_daily_at: str
    model_training_timezone: str
    model_training_days: int
    model_training_min_coverage_pct: float
    model_training_min_bucket_samples: int
    model_training_min_precise_bucket_samples: int
    model_training_activation_policy: dict
    model_training_startup_delay_seconds: int


def env_bool(name: str, default: bool) -> bool:
    raw = os.environ.get(name)
    if raw is None:
        return default
    return raw.strip().lower() in {"1", "true", "yes", "on"}


def env_int(name: str, default: int) -> int:
    raw = os.environ.get(name)
    if raw is None or raw.strip() == "":
        return default
    return int(raw)


def env_float(name: str, default: float) -> float:
    raw = os.environ.get(name)
    if raw is None or raw.strip() == "":
        return default
    return float(raw)


def env_json_dict(name: str, default: dict) -> dict:
    raw = os.environ.get(name)
    if raw is None or raw.strip() == "":
        return default
    import json

    value = json.loads(raw)
    if not isinstance(value, dict):
        raise ValueError(f"{name} must be a JSON object")
    return value


def load_config() -> AgentConfig:
    return AgentConfig(
        database_url=os.environ.get("DATABASE_URL", "postgresql://sre_agent:sre_agent@localhost:5432/sre_agent"),
        plan_path=os.environ.get("SRE_AGENT_PLAN", "data/prep/collection_plan.jsonl"),
        runner_enabled=env_bool("SRE_AGENT_RUNNER_ENABLED", True),
        runner_interval_seconds=env_int("SRE_AGENT_RUNNER_INTERVAL_SECONDS", 15 * 60),
        runner_lookback_minutes=env_int("SRE_AGENT_LOOKBACK_MINUTES", 60),
        window_size=os.environ.get("SRE_AGENT_WINDOW_SIZE", "15m"),
        collect_batch_size=env_int("SRE_AGENT_COLLECT_BATCH_SIZE", 20),
        worker_concurrency=max(1, env_int("SRE_AGENT_WORKER_CONCURRENCY", 3)),
        collection_timeout_seconds=max(60, env_int("SRE_AGENT_COLLECTION_TIMEOUT_SECONDS", 300)),
        prometheus_url=os.environ.get("PROMETHEUS_URL"),
        newrelic_api_key=os.environ.get("NEW_RELIC_API_KEY"),
        newrelic_graphql_url=os.environ.get("NEW_RELIC_GRAPHQL_URL"),
        kubectl_context=os.environ.get("KUBECTL_CONTEXT"),
        kubectl_aws_profile=os.environ.get("KUBECTL_AWS_PROFILE"),
        kubectl_proxy_url=os.environ.get("KUBECTL_PROXY_URL"),
        victorialogs_url=os.environ.get("VICTORIALOGS_URL"),
        victorialogs_tenant=os.environ.get("VICTORIALOGS_TENANT"),
        kubernetes_events_provider=os.environ.get("SRE_AGENT_KUBERNETES_EVENTS_PROVIDER", "auto").strip().lower(),
        victorialogs_kubernetes_events_query_template=os.environ.get("VICTORIALOGS_KUBERNETES_EVENTS_QUERY_TEMPLATE"),
        skip_github=env_bool("SRE_AGENT_SKIP_GITHUB", True),
        skip_kubernetes=env_bool("SRE_AGENT_SKIP_KUBERNETES", False),
        backfill_skip_kubernetes=env_bool("SRE_AGENT_BACKFILL_SKIP_KUBERNETES", True),
        skip_kubernetes_events=env_bool("SRE_AGENT_SKIP_KUBERNETES_EVENTS", False),
        kubectl_timeout_seconds=env_int("SRE_AGENT_KUBECTL_TIMEOUT_SECONDS", 15),
        mark_anomalies_after_collection=env_bool("SRE_AGENT_MARK_ANOMALIES_AFTER_COLLECTION", True),
        gap_recovery_enabled=env_bool("SRE_AGENT_GAP_RECOVERY_ENABLED", True),
        gap_lookback_hours=env_int("SRE_AGENT_GAP_LOOKBACK_HOURS", 24),
        gap_max_windows_per_run=env_int("SRE_AGENT_GAP_MAX_WINDOWS_PER_RUN", 8),
        gap_service_chunk_size=max(1, env_int("SRE_AGENT_GAP_SERVICE_CHUNK_SIZE", 20)),
        gap_recovery_order=os.environ.get("SRE_AGENT_GAP_RECOVERY_ORDER", "newest").strip().lower(),
        historical_backfill_days=env_int("SRE_AGENT_HISTORICAL_BACKFILL_DAYS", 15),
        historical_backfill_exclude_recent_hours=env_int("SRE_AGENT_HISTORICAL_BACKFILL_EXCLUDE_RECENT_HOURS", 24),
        historical_backfill_max_range_hours=env_int("SRE_AGENT_HISTORICAL_BACKFILL_MAX_RANGE_HOURS", 24),
        runner_watchdog_enabled=env_bool("SRE_AGENT_RUNNER_WATCHDOG_ENABLED", True),
        runner_watchdog_interval_seconds=max(10, env_int("SRE_AGENT_RUNNER_WATCHDOG_INTERVAL_SECONDS", 60)),
        runner_watchdog_schedule_grace_seconds=max(60, env_int("SRE_AGENT_RUNNER_WATCHDOG_SCHEDULE_GRACE_SECONDS", 5 * 60)),
        runner_watchdog_data_lag_minutes=max(15, env_int("SRE_AGENT_RUNNER_WATCHDOG_DATA_LAG_MINUTES", 60)),
        runner_watchdog_stale_job_seconds=max(
            120,
            env_int("SRE_AGENT_RUNNER_WATCHDOG_STALE_JOB_SECONDS", env_int("SRE_AGENT_COLLECTION_TIMEOUT_SECONDS", 300) + 120),
        ),
        model_training_scheduler_enabled=env_bool("SRE_AGENT_MODEL_TRAINING_SCHEDULER_ENABLED", False),
        model_training_daily_at=os.environ.get("SRE_AGENT_MODEL_TRAINING_DAILY_AT", "04:00"),
        model_training_timezone=os.environ.get("SRE_AGENT_MODEL_TRAINING_TIMEZONE", "Asia/Shanghai"),
        model_training_days=max(14, env_int("SRE_AGENT_MODEL_TRAINING_DAYS", 30)),
        model_training_min_coverage_pct=max(0.0, env_float("SRE_AGENT_MODEL_TRAINING_MIN_COVERAGE_PCT", 95.0)),
        model_training_min_bucket_samples=max(3, env_int("SRE_AGENT_MODEL_TRAINING_MIN_BUCKET_SAMPLES", 12)),
        model_training_min_precise_bucket_samples=max(3, env_int("SRE_AGENT_MODEL_TRAINING_MIN_PRECISE_BUCKET_SAMPLES", 3)),
        model_training_activation_policy=env_json_dict("SRE_AGENT_MODEL_TRAINING_ACTIVATION_POLICY", {}),
        model_training_startup_delay_seconds=max(0, env_int("SRE_AGENT_MODEL_TRAINING_STARTUP_DELAY_SECONDS", 120)),
    )
