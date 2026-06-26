# SRE Agent TODO

Updated: 2026-06-26

This file tracks known limitations, discovered issues, and next implementation
work for the SRE Agent. Keep it current when behavior, config, or docs change.

## Current Progress

| Area | Status | Notes |
| --- | --- | --- |
| 30d metric backfill | Done | New Relic + Prometheus 15m windows are stored in PostgreSQL |
| Realtime runner | Working | Runs every 15m with 60m lookback and 24h gap scan |
| Gap recovery | Working | Runner is bounded to near-term gaps; historical gaps are handled by a standalone backfill worker |
| Kubernetes inspect | Working locally through tunnel | Should move into `storehub-pro` with RBAC to remove tunnel/VPN dependency |
| Service catalog | Mostly complete | 51 services mapped; owners/SLOs still need human review |
| SLO recommendations | Generated | `slo-rec-v1` wrote 51 pending-review recommendations from the last 30d of 15m windows |
| Dynamic service baselines | Working | Rule-based seasonal baselines exist; reviewed ML seasonal models are active for eligible services |
| Transaction baselines | Done, manual refresh | `transaction_baselines` currently covers 48 services, 1393 rows total |
| Risk scoring v2 | Working | Calibrated rule-based scoring from recent 15m windows, service baselines, and persisted trace evidence |
| Incident inspect v2 | Working | Supports sync/async inspect, persisted results, summary, timeline, and feedback |
| Launchd local runtime | Added | `ops/launchd` can keep local service and tunnel alive |

## Core P0 Roadmap

These items are the core of the SRE Agent product, not optional polish.

| Priority | Task | Status | Target outcome |
| --- | --- | --- | --- |
| P0 | Generate SLO recommendations | generated, owner review pending | 51 services have initial availability/error-rate/p95/p99 candidates in `slo_recommendations` |
| P0 | ML-driven dynamic baseline per service/metric | active for eligible services | Learn each service's periodic behavior and automatically produce expected p50/p95/p99/residual ranges |
| P0 | Baseline model quality report | framework added | Show coverage, sample count, slot coverage, confidence, drift, and model freshness for every service/metric |
| P0 | Risk evaluator based on dynamic baseline residuals | working | Risk score is based on deviation from learned seasonal behavior, not fixed thresholds |
| P0 | Automatic retraining schedule | schema/API added, scheduler pending | Refresh baselines daily or weekly with versioned model snapshots and rollback |
| P0 | Feedback-aware risk calibration | feedback label table/API added, calibration pending | Use incident feedback and false positives to tune risk weights before introducing heavier ML classifiers |

### P0: ML-Driven Dynamic Baselines

Goal:

- Automatically learn periodic patterns for every service and every metric.
- Use historical `15m` windows as the primary training dataset.
- Start with interpretable models before introducing complex ML.

Minimum data gate:

| Use case | Minimum | Recommended |
| --- | --- | --- |
| Seasonal baseline | 7 days `15m`, coverage >= 90% | 14-21 days, coverage >= 95% |
| Stable dynamic baseline | 14 days `15m` | 21-30 days |
| Risk prediction model | 30-60 days + labels/feedback | 90 days |
| 5m spike/burn model | Core services 7-14 days `5m` | 30 days `5m` |

First implementation:

- Add `service_metric_models` table.
- Add `service_metric_training_runs`, `service_metric_model_buckets`,
  `service_metric_model_evaluations`, and `risk_feedback_labels` tables.
- Train `seasonal_quantile_v1` per `service_id + metric_name`.
- Features:
  - weekday
  - hour
  - 15m minute slot
  - traffic context
  - optional traffic-normalized resource metrics
- Outputs:
  - expected p50/p75/p90/p95/p99
  - residual
  - residual z-score or robust MAD score
  - baseline confidence
  - model freshness
- Fallback order remains:
  - `weekday + hour + minute_slot`
  - `hour + minute_slot`
  - `weekday + hour`
  - `hour`
  - `global`

Acceptance criteria:

- `/risk/score` can explain which learned baseline model was used.
- Normal traffic peaks remain low risk.
- Deviations during the same seasonal slot are visible with residual evidence.
- Each service/metric exposes model quality and coverage.

Framework completed:

- Added model registry and training-run tables.
- Added model quality/readiness API.
- Added training dry-run and first `seasonal_quantile_v1` training API.
- Added risk feedback label API.
- Added `/risk/score.dynamic_baseline_model` fallback status.
- Added active-model residual scoring in `/risk/score.dynamic_baseline_risk`.

Current activated model state:

- `seasonal-quantile-v2-20260626` is active for eligible SLO-candidate HTTP
  services with sufficient coverage.
- 360 service/metric models are active across 36 services.
- 147 evaluated models remain inactive for services intentionally excluded from
  automatic activation, such as consumer/job/streaming/edge candidates and
  services still in reliability roadmap review.
- `/risk/score` loads active seasonal buckets in this fallback order:
  `weekday_hour_slot`, `hour_slot`, `weekday_hour`, `hour`, `global`.
- Risk evidence now includes residual, p95/p99 deviation, robust MAD score,
  model version, bucket scope, sample count, and confidence.

Remaining P0 work:

- Formalize automatic activation and rollback policy instead of manual SQL.
- Add model freshness and drift checks to the quality report.
- Schedule daily or weekly retraining with a reviewed activation gate.
- Use `risk_feedback_labels` to calibrate residual thresholds and reduce false
  positives during normal peak periods.

## Known Issues Found During Testing

| Priority | Issue | Impact | Current workaround |
| --- | --- | --- | --- |
| P0 | Realtime Kubernetes inspect can make fresh jobs slow | Realtime windows can lag when `kubectl` calls hang or the private API is slow | Keep kubectl timeout low; temporarily set `SRE_AGENT_SKIP_KUBERNETES=true` when catching up |
| P1 | Logs/error groups are not integrated into inspect response | RCA may miss log-only failures | Add New Relic logs/error groups to inspect v3 |
| P1 | Async inspect worker is in-process | Production should separate long trace work from API replicas | Deploy supervised worker or Kubernetes worker replica |
| P1 | Local Kubernetes access depends on jumpserver tunnel/VPN routing | K8s evidence fails when tunnel or route is down | Run tunnel or deploy agent inside cluster |
| P1 | Direct OpenVPN route can miss EKS private API IPs | `kubectl` timed out when `192.168.4.238/192.168.5.105` routed via `en0` | Add `192.168.0.0/16` route through OpenVPN or use tunnel |
| P1 | Transaction baseline is manual | Runner does not refresh it automatically | Call `/baseline/recompute_transactions` manually |
| P1 | Historical backfill has no deployed CronJob yet | The script exists, but scheduling still needs cron or cluster wiring | Run `scripts/historical_gap_backfill.py` from cron or Kubernetes CronJob |
| P2 | Owner/SLO values are inferred | Catalog is usable but not authoritative | Service owner review required |
| P2 | Some services have zero transaction baseline rows | Three services returned zero New Relic Transaction rows | Treat as no transaction evidence, not API failure |
| P2 | Trace evidence is summary-level | It is not a full waterfall or span graph | Use trace hotspots as direction, verify in New Relic |

## Risk Scoring V2 Backlog

### 1. Include Transaction Baseline in Risk

Status: working

Current limitation:

- Risk v2 uses persisted `incident_trace_evidence` transaction deviations when
  trace evidence overlaps the risk window.
- Risk does not query New Relic live during normal reads, so transaction risk is
  only present after inspect/high-risk drill-down has generated trace evidence.

Implementation notes:

- Optional next step: add a lightweight transaction signal collector for top transactions in the latest window.
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
| P1 | Add New Relic logs and error groups | pending |
| P1 | Add dependency classification from trace hotspots | pending |
| P2 | Add data quality section explaining missing/partial signals | done |
| P2 | Use incident feedback to tune hypothesis ranking weights | pending |
| P2 | Add persisted inspect worker concurrency limits | pending |

## Runner / Data Quality Backlog

| Priority | Task | Status |
| --- | --- | --- |
| P1 | Support manual targeted retry by existing failed `collection_jobs.id` | pending |
| P1 | Add per-source timeout budgets inside one collector job | pending |
| P2 | Add daily baseline refresh job | pending |
| P2 | Add daily transaction baseline refresh job | pending |
| P2 | Deploy historical gap backfill as cron/CronJob | pending |

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
| P1 | Review inferred owners for all 51 services | pending |
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
- Added `/data/coverage` and `/gaps`.
- Fixed inspect nested risk to use the exact inspect `since/until` window.
- Added risk v2 transaction-baseline evidence from persisted New Relic trace summaries.
- Calibrated risk v2 for normal peak traffic:
  - request count/rpm no longer add risk by themselves;
  - baseline comparison now uses weighted weekday/hour/slot, hour/slot, hour, and global scopes;
  - global baseline is retained as low-weight long-term context;
  - resource metrics require material ratio deviation before adding risk.
- Added incident inspect v2:
  - persisted `incident_inspections`;
  - async inspect worker;
  - `summary` and `timeline`;
  - `GET /inspect/incident/{id}`;
  - `POST /inspect/incident/{id}/feedback`;
  - persisted `incident_inspection_feedback`.
- Added `transaction_baselines` table.
- Added `POST /baseline/recompute_transactions`.
- Added multi-worker collection with `SRE_AGENT_WORKER_CONCURRENCY`.
- Added `SRE_AGENT_COLLECTION_TIMEOUT_SECONDS` and `SRE_AGENT_KUBECTL_TIMEOUT_SECONDS`.
- Changed runner gap recovery to a short near-term self-healing window.
- Added `scripts/historical_gap_backfill.py` for historical gaps outside the runner window.
- Added `scripts/watch_service.py` watchdog for local service restarts.
- Added `AGENT.md` coding/operations guide.
- Built full transaction baseline before catalog cleanup:
  - services queried: 52
  - baseline rows: 1403
  - current catalog after removing invalid services: 51 services; transaction baseline table covers 48 services and 1393 rows
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
