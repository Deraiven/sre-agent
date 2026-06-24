# Agent Guide

This file is the handoff guide for coding agents working in this repository.
Follow it before making code, config, data, or operational changes.

## Project Goal

SRE Agent is a local-first service for:

- Predicting whether a production service is entering a risky state.
- Collecting 15-minute training and risk windows from New Relic, Prometheus,
  Kubernetes inspect, GitHub, and derived baselines.
- Investigating incidents with persisted evidence, summary, timeline,
  hypothesis ranking, trace context, and feedback.
- Building service-specific dynamic baselines from limited historical data.

Production services run from `master`. There is no dedicated deployment event
source yet, so recent GitHub commits are the first change signal.

## Data Source Rules

- Prometheus is resource-only: CPU, memory, network, pod/container pressure,
  resource saturation, and related infrastructure metrics.
- New Relic is the source for application signals: p95/p99 latency, throughput,
  request count/rpm, errors, transactions, traces, and related APM evidence.
- Kubernetes inspect is important for realtime collection and incident inspect:
  pods, workloads, rollouts, restarts, probes, scheduling, events, limits, and
  config/state.
- Historical gap recovery should skip Kubernetes by default. Use New Relic and
  Prometheus to catch up first, then let realtime windows include Kubernetes.
- Local access to `storehub-pro` Kubernetes may require the jumpserver SOCKS
  tunnel. Do not assume the private EKS API is directly reachable.

## Important Files

| Path | Purpose |
| --- | --- |
| `sre_agent/service.py` | HTTP API entrypoint and route handling |
| `sre_agent/runner.py` | Scheduled runner, workers, collection jobs, gap recovery |
| `sre_agent/intelligence.py` | Baselines, anomaly marking, risk scoring |
| `sre_agent/ml_baseline.py` | Dynamic baseline training and model quality |
| `sre_agent/incident_inspect.py` | Incident inspect workflow and persistence |
| `sre_agent/newrelic_trace.py` | New Relic trace and transaction baseline logic |
| `scripts/collect_windows.py` | Collector subprocess for one window/service chunk |
| `scripts/historical_gap_backfill.py` | Standalone cron/CronJob worker for historical gaps outside the runner window |
| `scripts/watch_service.py` | Local watchdog that restarts the API service |
| `db/schema.sql` | PostgreSQL schema |
| `config/service-catalog.yaml` | Current service mapping source |
| `docs/runtime-runbook.md` | Local runner and debugging runbook |
| `todo.MD` | Known limitations and next work |

## Local Runtime

Start local PostgreSQL:

```bash
docker compose up -d postgres
psql postgresql://sre_agent:sre_agent@localhost:5432/sre_agent -f db/schema.sql
python3 scripts/seed_services.py
```

Start the service directly:

```bash
export DATABASE_URL=postgresql://sre_agent:sre_agent@localhost:5432/sre_agent
export NEW_RELIC_API_KEY=...
export KUBECTL_AWS_PROFILE=pro
export KUBECTL_PROXY_URL=socks5://127.0.0.1:1080
python3 -m sre_agent.service --host 127.0.0.1 --port 8080
```

Prefer the watchdog for long-running local collection:

```bash
SRE_AGENT_BACKFILL_SKIP_KUBERNETES=true \
SRE_AGENT_KUBECTL_TIMEOUT_SECONDS=15 \
python3 scripts/watch_service.py --host 127.0.0.1 --port 8080 --skip-backfill-kubernetes
```

If realtime Kubernetes inspect is slowing collection, it is acceptable as an
operational mitigation to restart with global Kubernetes skipped so New Relic
and Prometheus can catch up first:

```bash
SRE_AGENT_SKIP_KUBERNETES=true \
python3 scripts/watch_service.py --host 127.0.0.1 --port 8080 --skip-kubernetes
```

## Runner Behavior

- The runner runs every 15 minutes by default.
- Realtime collection covers the latest 60 minutes of complete 15-minute
  windows.
- Gap recovery scans the previous 24 hours and queues bounded recovery work.
- Workers claim jobs by effective priority:
  `manual > fresh realtime > gap_recovery > stale realtime`.
- Gap recovery uses larger service chunks and skips Kubernetes by default.
- Running jobs left behind by service restart are reset to `queued`.
- `collection_jobs`, `runner_runs`, `/runner/status`, `/data/coverage`, and
  `/gaps` are the source of truth for collection health.

Key defaults:

| Variable | Default |
| --- | --- |
| `SRE_AGENT_RUNNER_INTERVAL_SECONDS` | `900` |
| `SRE_AGENT_LOOKBACK_MINUTES` | `60` |
| `SRE_AGENT_WORKER_CONCURRENCY` | `3` |
| `SRE_AGENT_COLLECTION_TIMEOUT_SECONDS` | `300` |
| `SRE_AGENT_BACKFILL_SKIP_KUBERNETES` | `true` |
| `SRE_AGENT_GAP_LOOKBACK_HOURS` | `24` |
| `SRE_AGENT_GAP_MAX_WINDOWS_PER_RUN` | `8` |
| `SRE_AGENT_GAP_SERVICE_CHUNK_SIZE` | `20` |

## Health Checks

Useful API checks:

```bash
curl --noproxy '*' http://127.0.0.1:8080/health
curl --noproxy '*' http://127.0.0.1:8080/runner/status
curl --noproxy '*' 'http://127.0.0.1:8080/data/coverage?lookback_hours=24'
curl --noproxy '*' 'http://127.0.0.1:8080/gaps?lookback_hours=24&limit=20'
curl --noproxy '*' 'http://127.0.0.1:8080/collection/jobs?status=running'
```

Useful SQL checks:

```bash
psql postgresql://sre_agent:sre_agent@localhost:5432/sre_agent -c "
select max(window_end) as latest_window_end
from service_metric_windows;"
```

```bash
psql postgresql://sre_agent:sre_agent@localhost:5432/sre_agent -c "
select job_type, status, count(*)
from collection_jobs
group by job_type, status
order by job_type, status;"
```

```bash
psql postgresql://sre_agent:sre_agent@localhost:5432/sre_agent -c "
select id, job_type, window_start, window_end, started_at,
       round(extract(epoch from (now() - started_at)) / 60, 1) as runtime_minutes,
       service_ids
from collection_jobs
where status = 'running'
order by started_at;"
```

Healthy local collection usually has latest `window_end` within one complete
15-minute window of current aligned time. Running jobs over 5 minutes deserve
attention; repeated 10+ minute jobs are suspicious.

## Development Rules

- Read the existing module before editing; this project is intentionally simple
  Python with standard library HTTP and subprocess-based collectors.
- Keep changes scoped. Do not refactor unrelated intelligence, runner, or
  schema code while fixing one behavior.
- Use structured JSON fields and existing database helper functions instead of
  ad hoc string parsing.
- Keep generated runtime artifacts out of commits unless explicitly requested:
  `logs/`, local database dumps, caches, and temporary outputs.
- Do not revert user or previous-agent changes in a dirty worktree. Check
  `git status --short` first and only touch files needed for the request.
- Use `apply_patch` for manual edits.
- Prefer `rg` / `rg --files` for searching.
- When adding environment variables, update `.env.example` and
  `docs/runtime-runbook.md`.
- When changing an API response, update README or docs with a sample endpoint
  and expected fields.

## Verification

Run at least the narrowest relevant checks:

```bash
python3 -m py_compile sre_agent/*.py scripts/*.py
git diff --check
```

For runner or collector changes, also verify:

```bash
curl --noproxy '*' http://127.0.0.1:8080/runner/status
psql postgresql://sre_agent:sre_agent@localhost:5432/sre_agent -c "
select job_type, status, count(*)
from collection_jobs
where created_at >= now() - interval '24 hours'
group by job_type, status
order by job_type, status;"
```

For risk or baseline changes, test at least one real service:

```bash
curl --noproxy '*' 'http://127.0.0.1:8080/services/backoffice-v2-bff/risk?lookback_hours=6'
curl --noproxy '*' 'http://127.0.0.1:8080/models/quality?days=30'
```

For incident inspect changes:

```bash
curl --noproxy '*' -X POST http://127.0.0.1:8080/inspect/incident \
  -H 'Content-Type: application/json' \
  -d '{"service_id":"backoffice-v2-bff","limit":8,"include_trace":true}'
```

## Risk Scoring Context

- Risk uses weighted comparisons across multiple baselines.
- Service-specific seasonal windows should matter more than global baselines.
- Global baseline still participates with smaller weight as long-term context.
- Request count/rpm is traffic context. It should not add risk by itself unless
  explicitly designed into a traffic-aware evaluator.
- Transaction baselines support trace/inspect and may be included in risk when
  the evaluator explicitly reads transaction residuals.
- Current known gaps include dependency propagation risk, burn rate/SLO error
  budget, trend slope, incident feedback calibration, and full ML-driven dynamic
  baseline activation. Check `todo.MD` before extending the model.

## Operational Judgement

When collection lags:

1. Check latest `service_metric_windows.window_end` and aligned current window.
2. Check `collection_jobs` queued/running/succeeded/error counts.
3. Inspect running job durations and job types.
4. If fresh realtime is blocked by long Kubernetes inspect, temporarily skip
   realtime Kubernetes and let New Relic + Prometheus catch up.
5. If jobs are stale after restart, rely on the runner reset path instead of
   manually deleting rows.
6. Avoid destructive DB edits unless the user explicitly asks.

When reporting status to the user, use concrete Beijing times and say whether
manual action is needed.
