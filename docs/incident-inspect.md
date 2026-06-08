# Incident Inspect Runbook

Incident inspect is the on-demand investigation path. It combines stored metric
windows, dynamic baseline/anomaly evidence, Kubernetes state, and New Relic
trace evidence to rank likely root-cause hypotheses.

## API

```bash
curl -X POST http://127.0.0.1:8080/inspect/incident \
  -H 'Content-Type: application/json' \
  -d '{
    "service_id": "backoffice-v2-bff",
    "since": "2026-06-08T02:45:00Z",
    "until": "2026-06-08T03:00:00Z",
    "include_trace": true,
    "trace_expand_minutes": 30,
    "limit": 16
  }'
```

| Field | Required | Default | Meaning |
| --- | --- | --- | --- |
| `service_id` | yes | none | Service to inspect |
| `since` | no | last 6 hours | Incident analysis start time |
| `until` | no | now | Incident analysis end time |
| `limit` | no | `16` | Maximum metric windows used for ranking |
| `rank_hypotheses` | no | `true` | Return root-cause hypothesis ranking |
| `include_trace` | no | `true` | Query New Relic trace evidence on demand |
| `trace_expand_minutes` | no | `30` | Expand trace query before/after incident window |
| `baseline_version` | no | `baseline-v1` | Baseline version for anomaly scoring |

Trace inspection requires `NEW_RELIC_API_KEY`. If the key is missing or a trace
query fails, incident inspect still returns metric/Kubernetes hypotheses and
adds the trace failure to observability evidence.

## Current Evidence Sources

| Evidence | Source | Stored in |
| --- | --- | --- |
| Golden signals | `service_metric_windows.newrelic` | PostgreSQL |
| Resource signals | `service_metric_windows.prometheus_resources` | PostgreSQL |
| Kubernetes state | `service_metric_windows.kubernetes` | PostgreSQL |
| Change context | `service_metric_windows.change_context` | PostgreSQL |
| Anomaly labels | `anomaly_windows` | PostgreSQL |
| Trace summary | New Relic NRQL `Transaction` and `Span` | `incident_trace_evidence` |

## New Relic Trace Queries

| Query | Purpose |
| --- | --- |
| `top_slow_transactions` | Find transactions with high p95/p99 duration |
| `span_category_breakdown` | Separate app/generic, http, datastore span latency |
| `external_hotspots` | Identify slow downstream HTTP/gRPC calls |
| `datastore_hotspots` | Identify slow datastore spans |
| `trace_samples` | Capture trace IDs and representative span samples |

Trace evidence is not part of the 15m scheduled runner. It is fetched only for
incident inspect or future high-risk drill-downs, because trace queries can be
more expensive and are only useful when narrowing root cause.

## Hypotheses

| Hypothesis | Strong evidence |
| --- | --- |
| `downstream_dependency_latency` | HTTP/gRPC span p95 is high or external hotspot appears |
| `database_latency` | Datastore span p95 is high or DB hotspot appears |
| `application_regression_or_downstream_latency` | Slow transactions or latency anomalies |
| `application_error_spike` | Error-rate anomalies or trace error samples |
| `resource_pressure` | CPU, memory, throttling, or network above baseline |
| `kubernetes_workload_health` | Readiness, rollout, restart, OOM, waiting reason, probe issues |
| `recent_change` | GitHub master commit/change context near first bad window |
| `observability_data_gap` | Missing telemetry or trace query failures |

## Trace Evidence Persistence

Each trace inspect writes a row to `incident_trace_evidence`:

```sql
select service_id, window_start, window_end, status, newrelic_app_name
from incident_trace_evidence
order by created_at desc
limit 5;
```

The `trace_summary` JSON contains the raw summarized NRQL results and the exact
queries used, which makes RCA reports auditable.

## Known V1 Limitations

- Trace evidence is a summary, not a full trace waterfall.
- Trace query field names differ by instrumentation; missing fields are expected.
- Dependency names come from New Relic span attributes and may be URLs, hostnames,
  or gRPC target strings.
- Logs and error groups are not yet integrated into the same inspect response.

