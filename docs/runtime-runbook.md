# Runtime Runbook

This runbook covers the local SRE Agent service, scheduled runner, gap recovery,
and common debugging steps.

## Runtime Components

| Component | Purpose |
| --- | --- |
| `sre_agent.service` | HTTP API, scheduler thread, and collector worker thread |
| `scripts/collect_windows.py` | Worker subprocess that collects one 15m service chunk into PostgreSQL |
| PostgreSQL | Stores services, metric windows, baselines, anomalies, runner runs, jobs, and incident evidence |
| Jumpserver SOCKS tunnel | Local access path to the private `storehub-pro` EKS API |

## Start Local Dependencies

```bash
docker compose up -d postgres
psql postgresql://sre_agent:sre_agent@localhost:5432/sre_agent -f db/schema.sql
python3 scripts/seed_services.py
```

For local Kubernetes inspect against `storehub-pro`, keep this tunnel running:

```bash
ssh -i /Users/storehub/.ssh/jumpserver.pem \
  -N -D 127.0.0.1:1080 \
  -o ExitOnForwardFailure=yes \
  root@54.169.8.128
```

## Start The Service

```bash
export DATABASE_URL=postgresql://sre_agent:sre_agent@localhost:5432/sre_agent
export NEW_RELIC_API_KEY=...
export KUBECTL_AWS_PROFILE=pro
export KUBECTL_PROXY_URL=socks5://127.0.0.1:1080

python3 -m sre_agent.service --host 127.0.0.1 --port 8080
```

For local long-running collection, prefer the watchdog so the service is
restarted automatically if the API process exits or `/health` stops responding:

```bash
SRE_AGENT_SKIP_KUBERNETES=true \
SRE_AGENT_KUBECTL_TIMEOUT_SECONDS=15 \
python3 scripts/watch_service.py --host 127.0.0.1 --port 8080 --skip-kubernetes
```

To collect Kubernetes inspect only for realtime windows while keeping historical
catch-up fast, run the watchdog without the global `--skip-kubernetes` flag and
leave backfill skipping enabled:

```bash
SRE_AGENT_BACKFILL_SKIP_KUBERNETES=true \
SRE_AGENT_KUBECTL_TIMEOUT_SECONDS=15 \
python3 scripts/watch_service.py --host 127.0.0.1 --port 8080 --skip-backfill-kubernetes
```

With the default settings, fresh realtime jobs include Kubernetes inspect.
`SRE_AGENT_BACKFILL_SKIP_KUBERNETES=true` only skips Kubernetes for
`gap_recovery` and stale realtime backlog jobs, so historical recovery cannot
drag the current 15m window behind.

The watchdog writes:

| File | Purpose |
| --- | --- |
| `logs/sre-agent-watchdog.jsonl` | Restart, health-check, and signal events |
| `logs/sre-agent-service.log` | stdout/stderr from `python3 -m sre_agent.service` |

When the service exits, the next watchdog loop records the return code and
starts a fresh process. On restart, the service returns interrupted
`collection_jobs` from `running` to `queued`.

## Runner Behavior

The runner starts automatically when `SRE_AGENT_RUNNER_ENABLED=true`.
The runner also starts an internal watchdog by default. It checks that the
scheduler thread is alive, workers match the configured concurrency, queued or
running jobs are draining, and latest metric windows are not falling behind.
Recovery events are written to `runner_watchdog_events` and exposed from
`/runner/status.recent_watchdog_events`.

Each run:

1. Creates a `runner_runs` audit record.
2. Enqueues realtime `collection_jobs` for the latest complete range.
3. Scans only the recent runner-owned gap window for missing, partial, or failed windows.
4. Enqueues bounded `gap_recovery` jobs for near-term self-healing.
5. Lets the worker execute jobs and mark anomalies after successful writes.

Default settings:

| Variable | Default | Meaning |
| --- | --- | --- |
| `SRE_AGENT_RUNNER_INTERVAL_SECONDS` | `900` | Run every 15 minutes |
| `SRE_AGENT_LOOKBACK_MINUTES` | `60` | Realtime collection covers the latest 60 minutes |
| `SRE_AGENT_WINDOW_SIZE` | `15m` | Primary training and risk window |
| `SRE_AGENT_COLLECT_BATCH_SIZE` | `20` | PostgreSQL write batch size |
| `SRE_AGENT_WORKER_CONCURRENCY` | `3` | Number of collection jobs drained in parallel |
| `SRE_AGENT_COLLECTION_TIMEOUT_SECONDS` | `300` | Maximum runtime for one collector job |
| `SRE_AGENT_SKIP_KUBERNETES` | `false` | Global emergency switch to skip all Kubernetes inspect |
| `SRE_AGENT_BACKFILL_SKIP_KUBERNETES` | `true` | Skip Kubernetes inspect for gap recovery and stale realtime backlog jobs |
| `SRE_AGENT_SKIP_KUBERNETES_EVENTS` | `false` | Skip Kubernetes event lookup inside inspect collection |
| `SRE_AGENT_KUBERNETES_EVENTS_PROVIDER` | `auto` | Use `victorialogs` when configured, otherwise `kubectl`; can be `kubectl`, `victorialogs`, or `none` |
| `VICTORIALOGS_URL` | `https://log.pro.mymyhub.com` | VictoriaLogs base URL for Kubernetes event lookup |
| `VICTORIALOGS_TENANT` | unset | Optional VictoriaLogs tenant, formatted as `AccountID:ProjectID` |
| `VICTORIALOGS_KUBERNETES_EVENTS_QUERY_TEMPLATE` | `log_type:k8s_events namespace:{namespace} name:~"{workload_name}-.+"` | LogsQL template used to query Kubernetes events; supports `{namespace}`, `{workload_name}`, `{selector}`, `{pod_names}`, `{start}`, `{end}` |
| `SRE_AGENT_KUBECTL_TIMEOUT_SECONDS` | `15` | Per-command timeout for Kubernetes inspect |
| `SRE_AGENT_MARK_ANOMALIES_AFTER_COLLECTION` | `true` | Label collected windows after successful collection |
| `SRE_AGENT_GAP_RECOVERY_ENABLED` | `true` | Enable automatic gap recovery |
| `SRE_AGENT_GAP_LOOKBACK_HOURS` | `24` | Scan this much recent history for runner-owned gaps |
| `SRE_AGENT_GAP_MAX_WINDOWS_PER_RUN` | `8` | Recover at most this many 15m windows per runner cycle |
| `SRE_AGENT_GAP_SERVICE_CHUNK_SIZE` | `20` | Number of services collected by one gap recovery job |
| `SRE_AGENT_GAP_RECOVERY_ORDER` | `newest` | Recover recent gaps first inside the runner-owned window |
| `SRE_AGENT_RUNNER_WATCHDOG_ENABLED` | `true` | Enable internal runner self-healing |
| `SRE_AGENT_RUNNER_WATCHDOG_INTERVAL_SECONDS` | `60` | Watchdog check interval |
| `SRE_AGENT_RUNNER_WATCHDOG_SCHEDULE_GRACE_SECONDS` | `300` | Allowed delay after `next_run_at` before recovery |
| `SRE_AGENT_RUNNER_WATCHDOG_DATA_LAG_MINUTES` | `60` | Latest metric window lag threshold before recovery |
| `SRE_AGENT_RUNNER_WATCHDOG_STALE_JOB_SECONDS` | `420` | Requeue running jobs older than this threshold |
| `SRE_AGENT_HISTORICAL_BACKFILL_DAYS` | `15` | Historical gap scan window for the standalone backfill worker |
| `SRE_AGENT_HISTORICAL_BACKFILL_EXCLUDE_RECENT_HOURS` | `24` | Recent window reserved for the service runner |
| `SRE_AGENT_HISTORICAL_BACKFILL_MAX_RANGE_HOURS` | `24` | Maximum bulk collector range per historical backfill call |

Gap recovery inside the service is deliberately small. The runner owns realtime
collection plus a short self-healing window, so large historical recovery cannot
starve fresh 15m windows.

Historical recovery is handled by a separate process:

```bash
python3 scripts/historical_gap_backfill.py \
  --history-days 15 \
  --exclude-recent-hours 24 \
  --max-range-hours 24 \
  --max-ranges 1
```

The script checks the historical range outside the runner gap window. If no
missing windows are found it exits with `status=noop`; if gaps exist it calls
`scripts/backfill_15m_bulk.py` for coalesced missing ranges and exits. This is
safe to run from cron or a Kubernetes CronJob because it does not enqueue
`collection_jobs` and does not consume runner workers.

Example cron entry:

```cron
*/30 * * * * cd /Users/storehub/Documents/sre-agent && /usr/bin/env python3 scripts/historical_gap_backfill.py >> logs/historical-gap-backfill.log 2>&1
```

Workers claim jobs by effective priority instead of raw creation time. Manual
jobs run first, fresh realtime windows run before near-term recovery and run
Kubernetes inspect by default, targeted `gap_recovery` jobs run before stale
realtime backlog, and old realtime jobs are kept as the lowest-priority
fallback. This keeps the latest 15m windows current while using spare worker
capacity only for small recent gaps.

The API no longer blocks on long collector work. `/collect/run` returns
`202 Accepted` with a `runner_run_id` and `job_ids`; the worker drains queued
jobs in the background.

On service restart, any `collection_jobs` left in `running` are marked with
`worker_restarted_before_completion` and returned to `queued` so they can be
retried.

## Useful API Checks

```bash
curl http://127.0.0.1:8080/health
curl http://127.0.0.1:8080/config
curl http://127.0.0.1:8080/runner/status
curl 'http://127.0.0.1:8080/runner/runs?limit=10'
curl 'http://127.0.0.1:8080/collection/jobs?limit=20'
curl 'http://127.0.0.1:8080/data/coverage?lookback_hours=24'
curl 'http://127.0.0.1:8080/gaps?lookback_hours=24&limit=20'
```

`/runner/status` reports scheduler settings, `worker_running`,
`worker_concurrency`, `worker_count`, `current_job`, `current_jobs`,
`collection_timeout_seconds`, `skip_kubernetes`, `backfill_skip_kubernetes`,
`gap_service_chunk_size`, `job_counts_24h`, `last_result`,
`last_gap_recovery`, and `next_run_at`. Each `current_jobs` entry includes
`skip_kubernetes` after the worker decides whether that job is realtime or
backfill.

`/runner/runs` shows each scheduler/manual run with job counts and duration
timestamps. `/collection/jobs` shows per-window/service chunk status, attempts,
return code, rows emitted/written, errors, and elapsed seconds. Use
`?status=failed`, `?status=queued`, or `?status=running` to filter jobs.

`/data/coverage` summarizes expected versus collected service/window points.
`/gaps` returns incomplete windows and the service ids that need recovery. Both
accept `since`, `until`, `lookback_hours`, `service_id`, and `window_size`.

## Check Collection Progress

Latest global data window:

```bash
psql "$DATABASE_URL" -At -c "
select max(window_start), max(window_end), count(*)
from service_metric_windows;"
```

Latest service coverage:

```bash
psql "$DATABASE_URL" -At -c "
select
  window_start,
  count(*) as rows,
  count(*) filter (where newrelic->>'status' = 'collected') as newrelic_collected,
  count(*) filter (where kubernetes->>'status' = 'collected') as k8s_collected,
  count(*) filter (where kubernetes->>'status' = 'missing') as k8s_missing,
  count(*) filter (where kubernetes->>'status' = 'error') as k8s_error
from service_metric_windows
where window_start >= (
  select max(window_start) - interval '3 hours'
  from service_metric_windows
)
group by window_start
order by window_start desc;"
```

## Manual Collection

Trigger collection through the service:

```bash
curl -X POST http://127.0.0.1:8080/collect/run \
  -H 'Content-Type: application/json' \
  -d '{
    "start": "2026-06-08T02:00:00Z",
    "end": "2026-06-08T03:00:00Z",
    "service_ids": ["backoffice-v2-bff"]
  }'
```

Or run the collector directly:

```bash
python3 scripts/collect_windows.py \
  --start 2026-06-08T02:00:00Z \
  --end 2026-06-08T03:00:00Z \
  --skip-github \
  --skip-kubernetes-events \
  --kubectl-aws-profile pro \
  --kubectl-proxy-url socks5://127.0.0.1:1080
```

Kubernetes events can come from either the Kubernetes API or VictoriaLogs. Set
`SRE_AGENT_SKIP_KUBERNETES_EVENTS=false`,
`SRE_AGENT_KUBERNETES_EVENTS_PROVIDER=auto`, `VICTORIALOGS_URL`, and
`VICTORIALOGS_KUBERNETES_EVENTS_QUERY_TEMPLATE`. The collector normalizes
VictoriaLogs rows into the existing `kubernetes.events` shape and reuses the
same `probe_failure_count`, `failed_scheduling_count`, `killing_event_count`,
and `image_pull_failure_count` feature fields. If kubectl workload/pod inspect
times out but VictoriaLogs events are available, the row is written with
`kubernetes.status=events_only`. In that mode risk and inspect only use event
signals, not replicas, pod phase, current restart count, or rollout state.

For the current `pro` VictoriaLogs stream, Kubernetes event rows use fields like
`log_type=k8s_events`, `namespace=pro`, `kind=Pod`, `name=<pod-name>`,
`reason`, `level`, `count`, `_time`, and `_msg`. The default template filters by
namespace and pod-name prefix derived from the service workload name.

## Baseline And Anomaly Operations

```bash
curl -X POST http://127.0.0.1:8080/baseline/recompute \
  -H 'Content-Type: application/json' \
  -d '{"days":30}'

curl -X POST http://127.0.0.1:8080/baseline/recompute_transactions \
  -H 'Content-Type: application/json' \
  -d '{"service_ids":["backoffice-v2-bff"],"days":30,"limit":100}'

curl -X POST http://127.0.0.1:8080/slo/recommendations/generate \
  -H 'Content-Type: application/json' \
  -d '{"days":30,"replace":true}'

curl 'http://127.0.0.1:8080/slo/recommendations?recommendation_version=slo-rec-v1&limit=20'

curl -X POST http://127.0.0.1:8080/anomalies/mark \
  -H 'Content-Type: application/json' \
  -d '{"service_ids":["backoffice-v2-bff"]}'

curl 'http://127.0.0.1:8080/services/backoffice-v2-bff/risk?lookback_hours=6'

curl 'http://127.0.0.1:8080/services/backoffice-v2-bff/risk?since=2026-06-15T02:00:00Z&until=2026-06-15T04:00:00Z'

curl 'http://127.0.0.1:8080/models/quality?days=30'
curl 'http://127.0.0.1:8080/models/freshness?model_version=seasonal-quantile-v2-20260626'
curl 'http://127.0.0.1:8080/models/drift?lookback_hours=24'

curl -X POST http://127.0.0.1:8080/models/train \
  -H 'Content-Type: application/json' \
  -d '{"dry_run":true,"days":30,"model_version":"seasonal-quantile-v1"}'

curl -X POST http://127.0.0.1:8080/models/train \
  -H 'Content-Type: application/json' \
  -d '{
    "dry_run": false,
    "activate": false,
    "days": 30,
    "min_coverage_pct": 70,
    "service_ids": ["backoffice-v2-bff"],
    "model_version": "seasonal-quantile-v1"
  }'

curl http://127.0.0.1:8080/models/training_runs
```

`/baseline/recompute_transactions` stores New Relic Transaction p50/p95/p99
baselines in `transaction_baselines` so incident inspect can report transaction
latency deviation percentages.

Risk v2 includes persisted trace transaction deviations when recent
`incident_trace_evidence` overlaps the risk window. It does not query New Relic
live during normal risk reads.

Risk v2 is calibrated for recurring daily peaks and uses weighted baseline
comparisons:

- `newrelic.request_count` and `newrelic.rpm` are traffic context, not risk
  points by themselves.
- Resource metrics are compared against all available baseline scopes:
  `weekday + hour + 15m minute_slot`, `hour + minute_slot`, `weekday + hour`,
  `hour`, and `global`.
- The precise slot baseline has the highest weight. Global baseline still
  participates, but with low weight, so long-term abnormality can be visible
  without turning normal daily peaks into critical alerts.
- CPU, memory, throttling, and network signals must exceed baseline by a
  material ratio before they add risk. Normal peak-period traffic should stay
  low risk unless latency, errors, Kubernetes health, or severe resource
  pressure also degrade.

## Dynamic Baseline Model Framework

P0 model endpoints are available. The current implementation can create
`seasonal_quantile_v1` training runs, persist evaluated per-service/per-metric
models, write seasonal bucket quantiles, and explicitly activate reviewed model
versions.

| Endpoint | Purpose |
| --- | --- |
| `GET /models/quality` | Check coverage/readiness for unsupervised seasonal baseline training |
| `GET /models/freshness` | Compare active model training windows with latest metric windows and flag stale models |
| `GET /models/drift` | Compare recent windows with active seasonal buckets and report p95/p99 breach rates plus robust MAD drift |
| `POST /models/train` | Dry-run readiness or persist evaluated `seasonal_quantile_v1` models |
| `GET /models/training_runs` | List training and dry-run records |
| `GET /models` | List persisted model versions and activation state |
| `GET /models/activation/evaluate` | Evaluate a model version against activation gates without changing active state |
| `POST /models/activate` | Activate a model version only when policy gates pass, unless `force=true` |
| `POST /models/rollback` | Roll back active models to a previous or explicit target model version |
| `GET /models/activation/events` | List activation, blocked activation, and rollback audit events |
| `POST /risk/feedback` | Store false positive, false negative, or confirmed incident labels |
| `GET /risk/feedback` | List recent feedback labels |
| `GET /risk/feedback/candidates` | List high-risk windows that need human feedback review |
| `GET /risk/feedback/report` | Summarize feedback by service and label type |
| `POST /risk/calibration/generate` | Generate enabled calibration rules from feedback labels |
| `GET /risk/calibration/rules` | List active or inactive calibration rules |

The initial model type is `seasonal_quantile_v1`. It is unsupervised and learns
normal service behavior by `weekday + hour + 15m minute_slot`. Training writes
models, buckets, and evaluation rows when `dry_run=false`. Keep
`activate=false` while validating backtest quality; after activation,
`/risk/score` loads active seasonal buckets, computes residuals, p95/p99
deviation, and robust MAD score, then exposes the result as
`dynamic_baseline_risk` and merges ML evidence into `top_evidence`. If no active
model exists for a service, risk v2 continues using the rule baseline fallback.

Activation / rollback policy:

- Default activation requires service coverage >= 95%, model coverage >= 95%,
  average training coverage >= 95%, no missing modeled services, no stale
  services, max training lag <= 24h, and quality score >= 90.
- Drift is evaluated and returned with the decision. The default policy records
  drift as an audit signal but does not block activation because Prometheus
  resource drift can be intentionally sensitive during early tuning. Set
  `policy.drift_gate_enabled=true` plus `max_drift_high_service_pct` and
  `max_drift_warning_service_pct` to make drift a hard gate.
- `POST /models/activate` writes a `model_activation_events` audit row whether
  activation succeeds or is blocked. Use `force=true` only for an intentional
  manual override; the forced decision is still recorded.
- `POST /models/rollback` reactivates an explicit `target_model_version`, or the
  previous model version from the latest successful activation event when no
  target is provided.

Example:

```bash
curl 'http://127.0.0.1:8080/models/activation/evaluate?model_version=seasonal-quantile-v4-20260703'

curl -X POST http://127.0.0.1:8080/models/activate \
  -H 'Content-Type: application/json' \
  -d '{"model_version":"seasonal-quantile-v4-20260703"}'

curl -X POST http://127.0.0.1:8080/models/rollback \
  -H 'Content-Type: application/json' \
  -d '{"target_model_version":"seasonal-quantile-v3-20260701","reason":"post-activation false positives increased"}'
```

Feedback-aware calibration:

1. Pull high-risk windows that need human review:

   ```bash
   curl 'http://127.0.0.1:8080/risk/feedback/candidates?lookback_hours=6&min_score=50&limit=20'
   ```

2. Submit labels after reviewing a candidate window:

   ```bash
   curl -X POST http://127.0.0.1:8080/risk/feedback \
     -H 'Content-Type: application/json' \
     -d '{
       "service_id": "backoffice-v2-bff",
       "window_start": "2026-06-25T08:00:00Z",
       "window_end": "2026-06-25T14:00:00Z",
       "risk_version": "risk-v2",
       "model_version": "seasonal-quantile-v2-20260626",
       "label_type": "false_positive",
       "actual_severity": "normal",
       "false_positive": true,
       "payload": {
         "reason": "normal business peak",
         "top_evidence": [
           {"source": "ml_dynamic_baseline", "metric": "newrelic.latency_p95_ms"}
         ]
       }
     }'
   ```

3. Review label distribution:

   ```bash
   curl 'http://127.0.0.1:8080/risk/feedback/report?days=30'
   ```

4. Generate calibration rules:

   ```bash
   curl -X POST http://127.0.0.1:8080/risk/calibration/generate \
     -H 'Content-Type: application/json' \
     -d '{"days":30,"min_labels":2,"activate":true}'
   ```

5. Verify `/risk/score` returns `risk_calibration.status=active` and evidence
   items include `calibration_rules` when a rule matched.

Freshness statuses:

- `fresh`: active model training window is within the warning threshold of the
  latest metric window.
- `stale_warning`: model lags latest data by at least 24 hours.
- `stale_critical`: model lags latest data by at least 72 hours.
- `no_active_model`: service has no active model.
- `no_recent_data`: service has an active model but no metric windows.

Drift statuses:

- `stable`: recent windows are still represented by the active seasonal buckets.
- `drift_warning`: p95 breach rate is at least 20% or MAD p95 is at least 4.
- `drift_high`: p99 breach rate is at least 10% or MAD p95 is at least 6.
- `insufficient_samples`: not enough recent samples for a reliable drift read.
- `no_active_model`: service has no active model.

## Incident Inspect V2

Synchronous inspect still returns the result directly:

```bash
curl -X POST http://127.0.0.1:8080/inspect/incident \
  -H 'Content-Type: application/json' \
  -d '{
    "service_id": "backoffice-v2-bff",
    "since": "2026-06-15T02:00:00Z",
    "until": "2026-06-15T04:00:00Z",
    "include_trace": true
  }'
```

For trace-heavy investigations, enqueue async inspect:

```bash
curl -X POST http://127.0.0.1:8080/inspect/incident \
  -H 'Content-Type: application/json' \
  -d '{"service_id":"backoffice-v2-bff","async":true,"include_trace":true}'

curl http://127.0.0.1:8080/inspect/incident/1
```

Submit feedback after a human confirms the root cause:

```bash
curl -X POST http://127.0.0.1:8080/inspect/incident/1/feedback \
  -H 'Content-Type: application/json' \
  -d '{
    "confirmed_root_cause": "downstream dependency latency",
    "correct_hypothesis": "downstream_dependency_latency",
    "usefulness": 4,
    "note": "Trace external hotspot pointed to the right host."
  }'
```

## Common Failures

| Symptom | Likely cause | Fix |
| --- | --- | --- |
| `curl: Failed to connect to 127.0.0.1:8080` | Service process is not running | Restart `python3 -m sre_agent.service ...` |
| Service exits repeatedly | Unhandled service failure or local dependency issue | Run `scripts/watch_service.py` and inspect `logs/sre-agent-service.log` plus `logs/sre-agent-watchdog.jsonl` |
| `proxyconnect tcp ... 127.0.0.1:1080` | Jumpserver SOCKS tunnel is down | Restart the SSH tunnel |
| K8s status is `error` for most services | Missing proxy/profile or tunnel failure | Check `/config`, `KUBECTL_AWS_PROFILE`, and `KUBECTL_PROXY_URL` |
| New Relic status is `error` | Missing or invalid `NEW_RELIC_API_KEY` | Export a valid key and restart service |
| Runner `running=false` after `next_run_at` passed | Runner thread or service lifecycle issue | Restart service or run `scripts/watch_service.py` |
| Worker not draining queued jobs | Worker thread stopped or service is stale | Check `/runner/status.worker_running`, then restart the service |
| Gap recovery does not run | Gap recovery disabled or no incomplete windows found | Check `last_gap_recovery`, `gap_recovery_enabled`, and `/collection/jobs?status=failed` |

## Known V1 Limitations

- API, scheduler, and worker still share one local process, but long collector work runs in a worker subprocess instead of blocking API requests.
- Kubernetes inspect runs for current realtime windows; historical gap recovery and stale realtime backlog skip it by default.
- Collector worker concurrency is configurable with `SRE_AGENT_WORKER_CONCURRENCY`; keep it conservative when Kubernetes inspect is enabled or telemetry APIs are rate limited.
- Local access to `storehub-pro` depends on a manually maintained SSH tunnel.
