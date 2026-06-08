create table if not exists services (
  service_id text primary key,
  description text,
  owner text,
  github_repo text not null,
  production_branch text not null default 'master',
  newrelic_app_name text,
  newrelic_app_id text,
  newrelic_entity_guid text,
  k8s_cluster text,
  k8s_namespace text,
  k8s_workload_kind text,
  k8s_workload_name text,
  k8s_label_selector text,
  tags text[] not null default '{}',
  created_at timestamptz not null default now(),
  updated_at timestamptz not null default now()
);

create table if not exists service_metric_windows (
  id bigserial primary key,
  service_id text not null references services(service_id),
  window_start timestamptz not null,
  window_end timestamptz not null,
  window_size text not null,
  newrelic jsonb not null default '{}',
  prometheus_resources jsonb not null default '{}',
  kubernetes jsonb not null default '{}',
  change_context jsonb not null default '{}',
  data_quality jsonb not null default '{}',
  source_snapshot_uri text,
  created_at timestamptz not null default now(),
  unique (service_id, window_start, window_size)
);

create index if not exists service_metric_windows_service_time_idx
  on service_metric_windows (service_id, window_start desc);

create index if not exists service_metric_windows_window_size_idx
  on service_metric_windows (window_size, window_start desc);

create index if not exists service_metric_windows_newrelic_gin_idx
  on service_metric_windows using gin (newrelic);

create index if not exists service_metric_windows_prometheus_gin_idx
  on service_metric_windows using gin (prometheus_resources);

create index if not exists service_metric_windows_kubernetes_gin_idx
  on service_metric_windows using gin (kubernetes);

create table if not exists service_baselines (
  id bigserial primary key,
  service_id text not null references services(service_id),
  baseline_version text not null,
  metric_name text not null,
  day_of_week int,
  hour_of_day int,
  traffic_bucket text,
  p50 double precision,
  p75 double precision,
  p90 double precision,
  p95 double precision,
  p99 double precision,
  sample_count int not null,
  valid_from timestamptz not null,
  valid_to timestamptz,
  created_at timestamptz not null default now()
);

create index if not exists service_baselines_lookup_idx
  on service_baselines (service_id, baseline_version, metric_name, day_of_week, hour_of_day);

create table if not exists anomaly_windows (
  id bigserial primary key,
  service_id text not null references services(service_id),
  window_start timestamptz not null,
  window_end timestamptz not null,
  window_size text not null,
  is_anomaly boolean not null,
  severity text not null,
  anomaly_types text[] not null default '{}',
  confidence text not null,
  label_source text[] not null default '{}',
  label_rule_version text not null,
  baseline_version text,
  evidence jsonb not null default '[]',
  reviewed boolean not null default false,
  reviewer text,
  review_note text,
  created_at timestamptz not null default now()
);

create index if not exists anomaly_windows_service_time_idx
  on anomaly_windows (service_id, window_start desc);

create index if not exists anomaly_windows_types_gin_idx
  on anomaly_windows using gin (anomaly_types);

create table if not exists incident_windows (
  id bigserial primary key,
  incident_id text not null,
  service_id text not null references services(service_id),
  impact_start timestamptz not null,
  impact_end timestamptz,
  severity text,
  root_cause_category text,
  summary text,
  source_url text,
  created_at timestamptz not null default now()
);

create index if not exists incident_windows_service_time_idx
  on incident_windows (service_id, impact_start desc);

create table if not exists incident_trace_evidence (
  id bigserial primary key,
  service_id text not null references services(service_id),
  window_start timestamptz not null,
  window_end timestamptz not null,
  newrelic_account_id text,
  newrelic_app_name text,
  status text not null,
  trace_summary jsonb not null default '{}',
  errors jsonb not null default '[]',
  created_at timestamptz not null default now()
);

create index if not exists incident_trace_evidence_service_time_idx
  on incident_trace_evidence (service_id, window_start desc);

create table if not exists slo_recommendations (
  id bigserial primary key,
  service_id text not null references services(service_id),
  recommendation_version text not null,
  recommendation_window text not null,
  historical_baseline jsonb not null,
  recommended_slo jsonb not null,
  confidence text not null,
  evidence jsonb not null default '[]',
  status text not null default 'pending_review',
  reviewer text,
  review_note text,
  created_at timestamptz not null default now(),
  reviewed_at timestamptz
);

create index if not exists slo_recommendations_service_idx
  on slo_recommendations (service_id, created_at desc);

create table if not exists artifact_refs (
  id bigserial primary key,
  artifact_type text not null,
  service_id text references services(service_id),
  related_id text,
  uri text not null,
  content_type text,
  metadata jsonb not null default '{}',
  created_at timestamptz not null default now()
);

create index if not exists artifact_refs_lookup_idx
  on artifact_refs (artifact_type, service_id, created_at desc);
