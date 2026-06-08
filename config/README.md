# Configuration

This directory contains the service mapping and service catalog used by the SRE
Agent v1 collectors and incident inspect flow.

## Files

| File | Purpose |
| --- | --- |
| `service-mapping.raw.json` | Raw imported service mapping used by `scripts/prepare_data_plan.py` |
| `service-catalog.yaml` | Human-readable enriched service catalog for review and future CMDB sync |
| `service-catalog.example.yaml` | Minimal examples for mapped and unmapped services |

## Current Catalog Status

`service-catalog.yaml` has 52 services. Required runtime fields are present for
New Relic, GitHub, Kubernetes, Prometheus, SLO defaults, dependencies, runbooks,
and tags.

Some fields are inferred and require owner review:

| Field | Meaning |
| --- | --- |
| `owner_source: inferred_from_service_name` | Owner was inferred from service/repo/tag naming |
| `owner_review_required: true` | Human owner confirmation is still required |
| `slo.reviewed: false` | SLO target has not been approved by the service owner |
| `slo.latency_p95_source: baseline-v1.global_p95` | Latency target was filled from the current global p95 baseline |
| `slo.latency_p95_source: insufficient_or_zero_latency_baseline` | Baseline was zero or insufficient; latency target remains null |

## Missing Kubernetes Mapping

If a service has no Kubernetes workload mapping, use:

```yaml
kubernetes:
  workload_kind: null
  workload_name: null
  selector_discovery: missing
  mapping_status: missing

prometheus_resources:
  status: missing
  reason: k8s_workload_mapping_missing
  cpu_usage: null
  memory_usage: null
  cpu_throttling: null
  network_receive: null
  network_transmit: null
```

Collectors treat this as an explicit missing mapping instead of querying a fake
pod regex.

## Review Checklist

Before using the catalog as an authoritative production source, review:

- `owner` for every service.
- `slo.availability_target` and `slo.latency_p95_ms`.
- `kubernetes.workload_name` for services with `mapping_status: missing`.
- `dependencies.downstream` and `dependencies.upstream`.
- `runbooks`.

