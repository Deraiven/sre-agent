# SRE Agent

An SRE agent for early service risk prediction, incident investigation, and practical remediation recommendations.

## What It Should Do

- Predict whether a service is drifting into a dangerous state before a user-visible incident happens.
- Investigate incident causes across metrics, logs, traces, deployments, dependencies, and alerts.
- Recommend concrete remediation actions with evidence, confidence, blast radius, and rollback guidance.
- Learn from postmortems, historical incidents, and runbooks to improve future diagnosis.

## Core Capabilities

| Capability | Purpose | Example Output |
| --- | --- | --- |
| Risk prediction | Detect services likely to breach SLO or alert soon | `payment-api risk=high, p95 latency trend +43%, deploy at 14:02 correlated` |
| Incident triage | Build a timeline and identify likely causes | `First symptom: Kafka lag at 14:07, API latency followed at 14:10` |
| Root cause analysis | Compare system signals and rank hypotheses | `Most likely: downstream inventory timeout after config rollout` |
| Remediation advice | Propose safe, actionable changes | `Rollback release 2026.06.04.2 or reduce batch size to 500` |
| Knowledge loop | Store decisions and outcomes | `Similar incident: INC-2026-021, fixed by scaling consumer group` |

## Suggested MVP

1. Connect telemetry sources: Prometheus, New Relic MCP, and Kubernetes.
2. Use GitHub `master` commits as the first version of deployment/change events.
3. Implement risk scoring based on New Relic application signals, Prometheus resource signals, Kubernetes workload state, dependency health, and recent code changes.
4. Implement incident investigation that creates a timeline and ranks possible causes.
5. Generate recommendations from runbooks, historical incidents, and current evidence.
6. Add human approval for any action that mutates production.

## Current Assumptions

- Production runs from the `master` branch.
- There is no dedicated deployment event source yet.
- The latest GitHub commits on `master` can be used as a practical change signal.
- Prometheus provides resource metrics only, such as CPU, memory, network, disk, pod/container resource pressure, and queue/resource saturation where available.
- New Relic MCP provides application golden signals, including throughput, error rate, p95/p99 latency, transactions, logs, errors, deployments if available, and alert context.
- Kubernetes inspect is a required incident investigation step for workload state, rollout state, pod restarts, events, probes, scheduling, config, and resource limits.

## Design

See [docs/sre-agent-design.md](docs/sre-agent-design.md).

For SLO design from historical data, see [docs/slo-from-history.md](docs/slo-from-history.md).

For the derived training dataset and persistence design, see [docs/data-storage.md](docs/data-storage.md).

For the current data preparation runbook, see [docs/data-preparation-runbook.md](docs/data-preparation-runbook.md).

## Configuration

Start from [config/service-catalog.example.yaml](config/service-catalog.example.yaml) and replace each service's owner, GitHub repository, New Relic entity GUID, Kubernetes workload mapping, SLO, dependency list, New Relic signal queries, and Prometheus resource queries.

The current service mapping has been imported into [config/service-catalog.yaml](config/service-catalog.yaml), with the original source preserved at [config/service-mapping.raw.json](config/service-mapping.raw.json).

## Local PostgreSQL

```bash
docker compose up -d postgres
psql postgresql://sre_agent:sre_agent@localhost:5432/sre_agent -f db/schema.sql
python3 scripts/seed_services.py
```

The local database is available at `postgresql://sre_agent:sre_agent@localhost:5432/sre_agent`.

## MVP Collector

```bash
python3 scripts/collect_windows.py \
  --service auth-api \
  --start 2026-06-04T09:45:00Z \
  --end 2026-06-04T10:00:00Z \
  --max-windows 1 \
  --skip-github
```

## Local Agent Service

The service wraps the existing collector in a long-running process. It starts a
scheduler and collector worker by default, every 15 minutes. Each scheduler run
records a `runner_runs` row, enqueues `collection_jobs` for the last 60 minutes
of complete 15-minute windows, then scans only the recent runner-owned gap
window for missing, partial, or failed windows and enqueues up to 8 prioritized
near-term gap-recovery windows. Large historical gap recovery runs separately,
so it cannot starve realtime collection workers.
The API returns quickly while the worker drains jobs in the background.

For historical gaps outside the runner gap window, run the standalone worker
from cron or a Kubernetes CronJob:

```bash
python3 scripts/historical_gap_backfill.py --history-days 15 --exclude-recent-hours 24
```

It checks for gaps, calls the optimized bulk collector only when needed, and
exits without creating runner `collection_jobs`.

For local `storehub-pro` Kubernetes inspect, start the jumpserver SOCKS tunnel
in another terminal first:

```bash
ssh -i /Users/storehub/.ssh/jumpserver.pem -N -D 127.0.0.1:1080 root@54.169.8.128
```

Then run:

```bash
export DATABASE_URL=postgresql://sre_agent:sre_agent@localhost:5432/sre_agent
export NEW_RELIC_API_KEY=...
export KUBECTL_AWS_PROFILE=pro
export KUBECTL_PROXY_URL=socks5://127.0.0.1:1080

python3 -m sre_agent.service --host 127.0.0.1 --port 8080
```

Useful endpoints:

```bash
curl http://127.0.0.1:8080/health
curl http://127.0.0.1:8080/runner/status
curl 'http://127.0.0.1:8080/runner/runs?limit=10'
curl 'http://127.0.0.1:8080/collection/jobs?limit=20'
curl 'http://127.0.0.1:8080/data/coverage?lookback_hours=24'
curl 'http://127.0.0.1:8080/gaps?lookback_hours=24&limit=20'
curl http://127.0.0.1:8080/services
curl 'http://127.0.0.1:8080/services/auth-api/risk?lookback_hours=6'

curl -X POST http://127.0.0.1:8080/collect/run \
  -H 'Content-Type: application/json' \
  -d '{"service_ids":["auth-api"],"dry_run":true}'

curl -X POST http://127.0.0.1:8080/baseline/recompute \
  -H 'Content-Type: application/json' \
  -d '{"service_ids":["auth-api"],"days":30}'

curl -X POST http://127.0.0.1:8080/baseline/recompute_transactions \
  -H 'Content-Type: application/json' \
  -d '{"service_ids":["backoffice-v2-bff"],"days":30,"limit":100}'

curl -X POST http://127.0.0.1:8080/slo/recommendations/generate \
  -H 'Content-Type: application/json' \
  -d '{"days":30,"replace":true}'

curl 'http://127.0.0.1:8080/slo/recommendations?recommendation_version=slo-rec-v1&limit=20'

curl -X POST http://127.0.0.1:8080/anomalies/mark \
  -H 'Content-Type: application/json' \
  -d '{"service_ids":["auth-api"]}'

curl -X POST http://127.0.0.1:8080/risk/score \
  -H 'Content-Type: application/json' \
  -d '{"service_id":"auth-api","lookback_hours":6}'

curl 'http://127.0.0.1:8080/models/quality?days=30'
curl 'http://127.0.0.1:8080/models/freshness?model_version=seasonal-quantile-v2-20260626'
curl 'http://127.0.0.1:8080/models/drift?lookback_hours=24'

curl -X POST http://127.0.0.1:8080/models/train \
  -H 'Content-Type: application/json' \
  -d '{"dry_run":true,"days":30,"model_version":"seasonal-quantile-v1"}'

curl http://127.0.0.1:8080/models/training_runs

curl -X POST http://127.0.0.1:8080/inspect/incident \
  -H 'Content-Type: application/json' \
  -d '{"service_id":"auth-api","limit":8,"include_trace":true}'

curl -X POST http://127.0.0.1:8080/inspect/incident \
  -H 'Content-Type: application/json' \
  -d '{"service_id":"auth-api","async":true,"limit":8,"include_trace":true}'

curl http://127.0.0.1:8080/inspect/incident/1
```

The first intelligence layer is deliberately rule-based:

- `baseline/recompute` builds percentile baselines from the last 30 days of
  15-minute windows.
- `baseline/recompute_transactions` builds New Relic Transaction latency
  baselines so trace inspect can report slow transaction deviation percentages.
- `slo/recommendations/generate` writes pending-review SLO recommendations from
  `service_metric_windows` plus `service_baselines`; `slo/recommendations`
  lists generated candidates for owner review.
- `runner/runs` and `collection/jobs` expose collection audit state, job
  failures, retries, elapsed seconds, and rows written.
- `data/coverage` and `gaps` expose coverage and missing/incomplete windows
  without writing SQL by hand.
- `anomalies/mark` labels windows by comparing New Relic, Prometheus, and
  Kubernetes signals against the baseline.
- `risk/score` computes risk v2 from recent window scores, active ML seasonal
  baseline residuals, and persisted New Relic transaction baseline deviations
  when trace evidence exists. ML evidence is returned under
  `dynamic_baseline_risk` and merged into `top_evidence`.
  High request volume or rpm is treated as traffic context, not risk by itself;
  resource metrics are compared against multiple weighted baselines. The
  service's weekday/hour/15m slot baseline has the highest weight, while global
  baseline still participates with a small weight as long-term context.
- `inspect/incident` supports synchronous and asynchronous incident inspection.
  It persists inspect requests/results, returns `summary` and `timeline`, and
  accepts feedback for confirmed root cause learning.
- `models/quality`, `models/freshness`, `models/drift`, `models/train`, and
  `models/training_runs` provide the P0 unsupervised dynamic-baseline workflow.
  Freshness reports whether active models are stale relative to the latest
  metric windows; drift reports whether recent values are breaching active
  seasonal buckets by p95/p99 rate or robust MAD score. The first trainable
  version builds `seasonal_quantile_v1` model buckets from historical
  15-minute windows and stores evaluated models without activating them unless
  requested. Active models are consumed by `risk/score` through seasonal bucket
  fallback:
  `weekday + hour + minute_slot`, `hour + minute_slot`, `weekday + hour`,
  `hour`, then `global`.
