# SRE Agent TODO

Updated: 2026-06-12

This file tracks known limitations, discovered issues, and next implementation
work for the SRE Agent. Keep it current when behavior, config, or docs change.

## Current Progress

| Area | Status | Notes |
| --- | --- | --- |
| 30d metric backfill | Done | New Relic + Prometheus 15m windows are stored in PostgreSQL |
| Realtime runner | Working | Runs every 15m with 60m lookback and 24h gap scan |
| Gap recovery | Working, needs improvement | Bounded to 8 windows per run; can take many cycles to catch up |
| Kubernetes inspect | Working locally through tunnel | Should move into `storehub-pro` with RBAC to remove tunnel/VPN dependency |
| Service catalog | Mostly complete | 52 services mapped; owners/SLOs still need human review |
| Dynamic service baselines | Done | `service_baselines` supports service-specific metric baselines |
| Transaction baselines | Done, manual | `transaction_baselines` built for 52 services, 1403 rows total |
| Risk scoring v1 | Working | Rule-based scoring from recent 15m windows and service baselines |
| Incident inspect v1 | Working | Returns windows, hypotheses, trace evidence, and recommended next actions |
| Launchd local runtime | Added | `ops/launchd` can keep local service and tunnel alive |

## Known Issues Found During Testing

| Priority | Issue | Impact | Current workaround |
| --- | --- | --- | --- |
| P0 | Collector worker is single-threaded | Large backlog still drains slowly, although API no longer blocks | Add concurrency limits and per-source timeout budgets |
| P1 | No `/data/coverage` and `/gaps` APIs yet | Operators still need SQL for coverage summaries | Use `/runner/runs`, `/collection/jobs`, and coverage SQL |
| P1 | Inspect nested `risk` is not aligned to `since/until` | Inspect may show `risk=unknown` while inspected windows have anomalies | Use `windows` and `hypotheses` as source of truth for now |
| P1 | Risk score does not include trace/transaction baseline | Slow transaction deviations are visible in inspect, but not risk | Use inspect for transaction-level analysis |
| P1 | Local Kubernetes access depends on jumpserver tunnel/VPN routing | K8s evidence fails when tunnel or route is down | Run tunnel or deploy agent inside cluster |
| P1 | Direct OpenVPN route can miss EKS private API IPs | `kubectl` timed out when `192.168.4.238/192.168.5.105` routed via `en0` | Add `192.168.0.0/16` route through OpenVPN or use tunnel |
| P1 | Transaction baseline is manual | Runner does not refresh it automatically | Call `/baseline/recompute_transactions` manually |
| P2 | Owner/SLO values are inferred | Catalog is usable but not authoritative | Service owner review required |
| P2 | Some services have zero transaction baseline rows | Three services returned zero New Relic Transaction rows | Treat as no transaction evidence, not API failure |
| P2 | Trace evidence is summary-level | It is not a full waterfall or span graph | Use trace hotspots as direction, verify in New Relic |
| P2 | Logs/error groups are not integrated into inspect response | RCA may miss log-only failures | Add New Relic logs/error groups to inspect v2 |

## Risk Scoring V2 Backlog

### 1. Include Transaction Baseline in Risk

Status: pending

Current limitation:

- Transaction baseline is only used by `inspect/incident` trace evidence.
- `/risk/score` does not know whether a hot transaction is above or below its historical baseline.

Implementation notes:

- Add a lightweight transaction signal collector for top transactions in the latest window.
- Compare transaction p95/p99/avg against `transaction_baselines`.
- Add risk evidence:
  - `newrelic.transaction_latency_p95`
  - `transaction_name`
  - `baseline_p95_ms`
  - `p95_vs_baseline_pct`
- Avoid querying transaction baselines every 15m for all services if it is too expensive; start with high-risk services or inspect-triggered enrichment.

Acceptance criteria:

- `/risk/score` can show top transaction latency deviation when available.
- Risk score increases when critical transactions exceed baseline by a configurable threshold.

### 2. Incident Feedback Training Loop

Status: pending

Current limitation:

- Risk scoring does not use incident feedback or post-incident labels.
- Hypothesis ranking has no quality feedback loop.

Implementation notes:

- Add `incident_inspections` table.
- Add `incident_hypothesis_feedback` table.
- Add API:
  - `POST /inspect/incident/{inspection_id}/feedback`
  - `GET /inspect/incident/{inspection_id}`
- Capture whether each hypothesis was confirmed, rejected, or unknown.
- Use confirmed incidents to tune rule weights before introducing ML.

Acceptance criteria:

- Each inspect run is persisted.
- Users can mark root cause category and whether suggestions were useful.
- Risk/hypothesis weights can be evaluated against confirmed incidents.

### 3. Dependency Propagation Risk

Status: pending

Current limitation:

- Upstream services do not inherit risk from downstream services.
- A downstream issue such as `report-api` or `core-api` latency is visible in trace inspect, but not propagated to callers.

Implementation notes:

- Populate `dependencies.downstream` and `dependencies.upstream` in service catalog.
- Derive runtime dependencies from New Relic spans:
  - HTTP URLs
  - gRPC targets
  - datastore spans
- Add dependency risk features:
  - downstream service risk level
  - downstream p95/p99 deviation
  - dependency error rate
  - caller span volume

Acceptance criteria:

- If a downstream service is high risk, upstream services show dependency risk evidence.
- Inspect can say whether the likely root cause is local or downstream.

### 4. SLO Burn Rate / Error Budget

Status: pending

Current limitation:

- Risk does not calculate burn rate or error budget consumption.
- SLO config exists, but risk is still based on baseline deviations.

Implementation notes:

- Formalize SLI per service:
  - availability/error rate
  - latency p95/p99
  - throughput health
- Compute burn rates for:
  - 5m
  - 1h
  - 6h
  - 3d
- Add risk evidence:
  - `burn_rate_5m`
  - `burn_rate_1h`
  - `error_budget_remaining`

Acceptance criteria:

- `/risk/score` includes SLO burn evidence when SLO is configured.
- Services with fast burn get high/critical risk even if resource metrics look normal.

### 5. Trend Slope Detection

Status: pending

Current limitation:

- Risk only fires when a metric crosses p95/p99 or hard K8s conditions.
- It misses continuous degradation before threshold crossing.

Implementation notes:

- Add rolling trend features for p95, p99, error rate, CPU, memory, and request volume.
- Compare short window slope against historical slope baseline.
- Detect:
  - monotonic latency increase
  - accelerating error rate
  - memory growth without release
  - CPU throttling trend

Acceptance criteria:

- Risk can report "latency is rising toward baseline breach" before p95/p99 is crossed.
- Trend evidence includes slope, start value, end value, and confidence.

## Incident Inspect V2 Backlog

| Priority | Task | Status |
| --- | --- | --- |
| P0 | Persist inspect requests/results in `incident_inspections` | pending |
| P0 | Make inspect asynchronous for trace/log-heavy requests | pending |
| P1 | Add concise `summary` and `timeline` fields to response | pending |
| P1 | Fix nested `risk` to use inspect `since/until` or remove it | pending |
| P1 | Add New Relic logs and error groups | pending |
| P1 | Add dependency classification from trace hotspots | pending |
| P2 | Add data quality section explaining missing/partial signals | pending |
| P2 | Add feedback endpoint for confirmed root cause | pending |

## Runner / Data Quality Backlog

| Priority | Task | Status |
| --- | --- | --- |
| P0 | Add `/data/coverage` and `/gaps` APIs | pending |
| P1 | Support manual targeted retry by existing failed `collection_jobs.id` | pending |
| P1 | Add collector concurrency limits and per-source timeout budgets | pending |
| P2 | Add daily baseline refresh job | pending |
| P2 | Add daily transaction baseline refresh job | pending |

## Productionization Backlog

| Priority | Task | Status |
| --- | --- | --- |
| P0 | Deploy service inside `storehub-pro` | pending |
| P0 | Add Kubernetes ServiceAccount and read-only RBAC | pending |
| P0 | Move secrets to Kubernetes Secret or external secret manager | pending |
| P1 | Replace local launchd/tunnel runtime with cluster Deployment | pending |
| P1 | Add health checks and resource requests/limits | pending |
| P1 | Add self-monitoring metrics for runner and API | pending |
| P2 | Add CI checks for schema/docs/config consistency | pending |

## Config / Documentation Backlog

| Priority | Task | Status |
| --- | --- | --- |
| P1 | Review inferred owners for all 52 services | pending |
| P1 | Review service SLO targets with owners | pending |
| P1 | Fill upstream/downstream dependencies in `service-catalog.yaml` | pending |
| P1 | Add runbook links for critical services | pending |
| P2 | Document launchd local runtime setup | pending |
| P2 | Document OpenVPN route requirement for direct Kubernetes access | pending |

## Recently Completed

- Added `runner_runs` and `collection_jobs`.
- Added async scheduler/worker flow:
  - scheduler enqueues jobs instead of blocking API/runner;
  - worker records status, return code, stdout/stderr, rows, failures, and elapsed seconds;
  - `/collect/run` now returns queued job ids.
- Added `/runner/runs` and `/collection/jobs`.
- Improved gap recovery:
  - prioritizes incomplete windows before empty windows;
  - targets missing or failed service ids instead of blindly recollecting full ranges;
  - treats skipped Kubernetes inspect as incomplete for recovery.
- Added worker restart recovery for interrupted `running` jobs.
- Added `transaction_baselines` table.
- Added `POST /baseline/recompute_transactions`.
- Built full transaction baseline:
  - services queried: 52
  - baseline rows: 1403
  - zero-transaction services:
    - `3p-webhook-adapter-infra-svc`
    - `backoffice-migrate-jobs`
    - `core-event-consumer-zendesk`
- Enriched inspect trace slow transactions with:
  - `baseline_status`
  - `baseline.p95_ms`
  - `baseline.p99_ms`
  - `p95_vs_baseline_pct`
  - `p99_vs_baseline_pct`
  - `avg_vs_baseline_pct`
- Added local launchd configs for:
  - SRE Agent service
  - jumpserver SOCKS tunnel
