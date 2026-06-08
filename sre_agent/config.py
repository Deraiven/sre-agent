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
    prometheus_url: str | None
    newrelic_api_key: str | None
    newrelic_graphql_url: str | None
    kubectl_context: str | None
    kubectl_aws_profile: str | None
    kubectl_proxy_url: str | None
    skip_github: bool
    skip_kubernetes_events: bool
    mark_anomalies_after_collection: bool
    gap_recovery_enabled: bool
    gap_lookback_hours: int
    gap_max_windows_per_run: int


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


def load_config() -> AgentConfig:
    return AgentConfig(
        database_url=os.environ.get("DATABASE_URL", "postgresql://sre_agent:sre_agent@localhost:5432/sre_agent"),
        plan_path=os.environ.get("SRE_AGENT_PLAN", "data/prep/collection_plan.jsonl"),
        runner_enabled=env_bool("SRE_AGENT_RUNNER_ENABLED", True),
        runner_interval_seconds=env_int("SRE_AGENT_RUNNER_INTERVAL_SECONDS", 15 * 60),
        runner_lookback_minutes=env_int("SRE_AGENT_LOOKBACK_MINUTES", 60),
        window_size=os.environ.get("SRE_AGENT_WINDOW_SIZE", "15m"),
        collect_batch_size=env_int("SRE_AGENT_COLLECT_BATCH_SIZE", 20),
        prometheus_url=os.environ.get("PROMETHEUS_URL"),
        newrelic_api_key=os.environ.get("NEW_RELIC_API_KEY"),
        newrelic_graphql_url=os.environ.get("NEW_RELIC_GRAPHQL_URL"),
        kubectl_context=os.environ.get("KUBECTL_CONTEXT"),
        kubectl_aws_profile=os.environ.get("KUBECTL_AWS_PROFILE"),
        kubectl_proxy_url=os.environ.get("KUBECTL_PROXY_URL"),
        skip_github=env_bool("SRE_AGENT_SKIP_GITHUB", True),
        skip_kubernetes_events=env_bool("SRE_AGENT_SKIP_KUBERNETES_EVENTS", True),
        mark_anomalies_after_collection=env_bool("SRE_AGENT_MARK_ANOMALIES_AFTER_COLLECTION", True),
        gap_recovery_enabled=env_bool("SRE_AGENT_GAP_RECOVERY_ENABLED", True),
        gap_lookback_hours=env_int("SRE_AGENT_GAP_LOOKBACK_HOURS", 24),
        gap_max_windows_per_run=env_int("SRE_AGENT_GAP_MAX_WINDOWS_PER_RUN", 8),
    )
