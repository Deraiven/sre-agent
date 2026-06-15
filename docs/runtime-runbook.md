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

## Runner Behavior

The runner starts automatically when `SRE_AGENT_RUNNER_ENABLED=true`.

Each run:

1. Creates a `runner_runs` audit record.
2. Enqueues realtime `collection_jobs` for the latest complete range.
3. Scans recent history for missing, partial, or failed windows.
4. Enqueues bounded `gap_recovery` jobs, prioritizing partial windows first.
5. Lets the worker execute jobs and mark anomalies after successful writes.

Default settings:

| Variable | Default | Meaning |
| --- | --- | --- |
| `SRE_AGENT_RUNNER_INTERVAL_SECONDS` | `900` | Run every 15 minutes |
| `SRE_AGENT_LOOKBACK_MINUTES` | `60` | Realtime collection covers the latest 60 minutes |
| `SRE_AGENT_WINDOW_SIZE` | `15m` | Primary training and risk window |
| `SRE_AGENT_COLLECT_BATCH_SIZE` | `20` | PostgreSQL write batch size |
| `SRE_AGENT_MARK_ANOMALIES_AFTER_COLLECTION` | `true` | Label collected windows after successful collection |
| `SRE_AGENT_GAP_RECOVERY_ENABLED` | `true` | Enable automatic gap recovery |
| `SRE_AGENT_GAP_LOOKBACK_HOURS` | `24` | Scan this much history for gaps |
| `SRE_AGENT_GAP_MAX_WINDOWS_PER_RUN` | `8` | Recover at most this many 15m windows per runner cycle |

Gap recovery is deliberately budgeted. If the tunnel or a telemetry source is
down for multiple hours, the agent catches up gradually instead of issuing a
large burst of New Relic, Prometheus, and Kubernetes queries.

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
```

`/runner/status` reports scheduler settings, `worker_running`, `current_job`,
`job_counts_24h`, `last_result`, `last_gap_recovery`, and `next_run_at`.

`/runner/runs` shows each scheduler/manual run with job counts and duration
timestamps. `/collection/jobs` shows per-window/service chunk status, attempts,
return code, rows emitted/written, errors, and elapsed seconds. Use
`?status=failed`, `?status=queued`, or `?status=running` to filter jobs.

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

## Baseline And Anomaly Operations

```bash
curl -X POST http://127.0.0.1:8080/baseline/recompute \
  -H 'Content-Type: application/json' \
  -d '{"days":30}'

curl -X POST http://127.0.0.1:8080/baseline/recompute_transactions \
  -H 'Content-Type: application/json' \
  -d '{"service_ids":["backoffice-v2-bff"],"days":30,"limit":100}'

curl -X POST http://127.0.0.1:8080/anomalies/mark \
  -H 'Content-Type: application/json' \
  -d '{"service_ids":["backoffice-v2-bff"]}'

curl 'http://127.0.0.1:8080/services/backoffice-v2-bff/risk?lookback_hours=6'
```

`/baseline/recompute_transactions` stores New Relic Transaction p50/p95/p99
baselines in `transaction_baselines` so incident inspect can report transaction
latency deviation percentages.

## Common Failures

| Symptom | Likely cause | Fix |
| --- | --- | --- |
| `curl: Failed to connect to 127.0.0.1:8080` | Service process is not running | Restart `python3 -m sre_agent.service ...` |
| `proxyconnect tcp ... 127.0.0.1:1080` | Jumpserver SOCKS tunnel is down | Restart the SSH tunnel |
| K8s status is `error` for most services | Missing proxy/profile or tunnel failure | Check `/config`, `KUBECTL_AWS_PROFILE`, and `KUBECTL_PROXY_URL` |
| New Relic status is `error` | Missing or invalid `NEW_RELIC_API_KEY` | Export a valid key and restart service |
| Runner `running=false` after `next_run_at` passed | Runner thread or service lifecycle issue | Restart service; future work should add a watchdog |
| Worker not draining queued jobs | Worker thread stopped or service is stale | Check `/runner/status.worker_running`, then restart the service |
| Gap recovery does not run | Gap recovery disabled or no incomplete windows found | Check `last_gap_recovery`, `gap_recovery_enabled`, and `/collection/jobs?status=failed` |

## Known V1 Limitations

- API, scheduler, and worker still share one local process, but long collector work runs in a worker subprocess instead of blocking API requests.
- Kubernetes inspect runs per service/window and can make full-service collection slow.
- Collector worker concurrency is currently one job at a time.
- Local access to `storehub-pro` depends on a manually maintained SSH tunnel.
