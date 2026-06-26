# 数据准备 Runbook

## 当前状态

已完成本地数据准备控制面：

| 项 | 状态 |
| --- | --- |
| 服务目录 | `config/service-catalog.yaml`，51 个服务 |
| New Relic account | `464254` StoreHub |
| New Relic entity GUID | 已根据 app id 自动补齐 |
| Prometheus 连接 | 已验证健康 |
| Prometheus 资源指标 | 已验证 CPU、memory、throttling、network 指标存在 |
| 训练窗口 | 30 天，15 分钟窗口 |
| 预计样本量 | 51 * 2880 = 146,880 个窗口 |

## 生成数据准备计划

```bash
python3 scripts/prepare_data_plan.py \
  --days 30 \
  --window-size 15m \
  --out-dir data/prep \
  --newrelic-account-id 464254 \
  --k8s-cluster storehub-pro
```

输出：

| 文件 | 说明 |
| --- | --- |
| `data/prep/summary.json` | 数据准备摘要 |
| `data/prep/services_seed.jsonl` | 可导入 `services` 表的服务种子数据 |
| `data/prep/collection_plan.jsonl` | 每个服务的采集范围和数据源 |
| `data/prep/window_tasks.sample.jsonl` | 少量窗口任务样本，用于验证 collector |

## 初始化数据库

```bash
docker compose up -d postgres
psql "$DATABASE_URL" -f db/schema.sql
python3 scripts/seed_services.py
```

MVP 必须先建：

| 表 | 目的 |
| --- | --- |
| `services` | 服务上下文 |
| `service_metric_windows` | 聚合训练窗口 |
| `service_baselines` | 动态基线 |
| `anomaly_windows` | 异常标签 |
| `incident_windows` | 人工确认 incident 标签 |
| `incident_trace_evidence` | New Relic trace 深查摘要 |
| `slo_recommendations` | SLO 建议 |
| `artifact_refs` | S3/外部 artifact 引用 |

## 已验证的 Prometheus 查询

以 `auth-api` 为例：

```promql
sum(rate(container_cpu_usage_seconds_total{namespace="pro", pod=~"auth-api-.*"}[5m]))
sum(container_memory_working_set_bytes{namespace="pro", pod=~"auth-api-.*"})
sum(rate(container_cpu_cfs_throttled_seconds_total{namespace="pro", pod=~"auth-api-.*"}[5m]))
```

这些查询已返回数据。

## 已验证的 New Relic Entity

| App | App ID | Entity GUID |
| --- | --- | --- |
| Auth API | `831620574` | `NDY0MjU0fEFQTXxBUFBMSUNBVElPTnw4MzE2MjA1NzQ` |
| Core API | `456826027` | `NDY0MjU0fEFQTXxBUFBMSUNBVElPTnw0NTY4MjYwMjc` |

Entity GUID 规则：

```text
base64("464254|APM|APPLICATION|<newrelic_app_id>")
```

## V1 已实现能力

第一版已经具备完整的数据采集和基础智能分析链路：

| 能力 | 实现 |
| --- | --- |
| 历史回填 | `scripts/backfill_15m_bulk.py` 回填 30 天 New Relic + Prometheus 15m 窗口 |
| 历史缺口修复 | `scripts/historical_gap_backfill.py` 独立修复 runner gap window 之外的历史缺口 |
| 实时采集 | `scripts/collect_windows.py` 采集 New Relic、Prometheus、Kubernetes inspect、GitHub change context |
| HTTP 服务 | `python3 -m sre_agent.service` |
| Schedule runner | 每 15 分钟采集最近 60 分钟完整窗口 |
| Gap recovery | runner 只修复最近窗口内的缺失/失败窗口，默认最新窗口优先，最多回填 8 个 15m 窗口 |
| Baseline | `POST /baseline/recompute` 写入 `service_baselines` |
| Anomaly | `POST /anomalies/mark` 或 runner 采集后自动写入 `anomaly_windows` |
| Risk | `GET /services/{service_id}/risk` 和 `POST /risk/score` |
| Incident inspect | `POST /inspect/incident`，默认包含 New Relic trace 深查 |

运行和排障见 `docs/runtime-runbook.md`。Incident 深查见 `docs/incident-inspect.md`。

## 运行 MVP Collector

先采集一个服务的一个 15 分钟窗口验证链路：

```bash
python3 scripts/collect_windows.py \
  --service auth-api \
  --start 2026-06-04T09:45:00Z \
  --end 2026-06-04T10:00:00Z \
  --max-windows 1
```

当前 MVP collector 会采集 New Relic APM 指标、Prometheus 资源指标和 Kubernetes inspect 信息。GitHub 信息会在安装并认证 `gh` 后采集，也可以用 `--skip-github` 跳过。

## 近 30 天回填策略

不要对 `5m`、`15m`、`1h`、`1d` 全部窗口都直接查询外部系统。第一阶段使用 `15m` 作为主训练粒度，覆盖近 30 天。51 个服务约产生 `51 * 2880 = 146,880` 条窗口，已经足够建立 SLO baseline、异常标签和风险模型。

Risk baseline 必须按周期时间槽使用历史数据。当前主 baseline 使用
`weekday + hour + 15m minute_slot`，例如“周一 10:15”会优先对比历史周一
10:15 的样本；样本不足时再 fallback 到“每天 10:15”、“周一 10 点”、
“每天 10 点”和全局 baseline。这样 C2C/B2C/PaaS 服务的正常高峰不会被
全局 p95/p99 误判为危险。

推荐窗口策略：

| 窗口 | 是否直接采集 | 用途 |
| --- | --- | --- |
| `15m` | 是，主窗口 | SLO、异常检测、周期 baseline、风险预测训练主粒度 |
| `1h` | 否，从 15m 聚合 | 趋势、报表、降噪后的容量观察 |
| `1d` | 否，从 15m 聚合 | 周期性、周报、SLO 推荐证据 |
| `5m` | 选择性采集 | 快速故障定位、突刺分析、burn rate、核心链路早期预警 |

`5m` 不建议第一阶段对所有服务全量回填 30 天，因为查询成本和存储会增加
约 3 倍，而且普通周期 baseline 用 `15m minute_slot` 已经足够。建议策略：

| 范围 | 建议 |
| --- | --- |
| 全服务 | 30 天 `15m` 必须完整，用于周期 baseline |
| C2C/B2C/PaaS 核心服务 | 可补最近 7-14 天 `5m`，用于突刺和 burn-rate |
| Incident 前后 | 对影响服务补 `5m`，窗口覆盖 incident 前后 2-6 小时 |
| 长期报表 | 从 `15m` 聚合到 `1h/1d`，不要重复查询外部系统 |

最有效的建模维度优先级：

| 优先级 | 维度 | 原因 |
| --- | --- | --- |
| 1 | `service_id + 15m window_start` | 每个服务的行为差异最大，15m 对 SLO 和异常标记最稳 |
| 2 | New Relic golden signals | `error_rate_percent`、`latency_p95_ms`、`latency_p99_ms`、`rpm` 最能直接反映用户风险 |
| 3 | 时间周期 | `day_of_week`、`hour_of_day` 能区分业务高峰和低谷 |
| 4 | traffic bucket | 低流量窗口容易误判，按 rpm/request_count 分桶能提高 baseline 质量 |
| 5 | Prometheus resource signals | CPU、memory、throttling 更适合作为原因解释和提前风险信号 |
| 6 | Kubernetes inspect | 适合解释当前状态和故障现场；历史 events 保留有限，应该从现在开始连续采 |
| 7 | GitHub master commits | 作为变更上下文，不作为主要异常标签依据 |

近 30 天回填建议分两步：

1. 回填 New Relic + Prometheus 的 `15m` 历史窗口，先跳过 Kubernetes 和 GitHub。
2. 用 `15m` 历史窗口构建周期 baseline：`weekday + hour + minute_slot`。
3. 从今天开始持续采集 Kubernetes inspect；只有核心服务或 incident 附近再补 `5m` 精细窗口。

示例命令：

```bash
python3 scripts/backfill_15m_bulk.py \
  --batch-size 500
```

`backfill_15m_bulk.py` 会把 New Relic 查询切成 3 天一段，避免 New Relic `TIMESERIES` 366 buckets 限制；Prometheus 会按服务和指标做 30 天 `query_range`，避免逐窗口查询。

日常运行时不要让 runner 承担大量历史缺口回填。推荐拆成两个进程：

- SRE Agent service：每 15 分钟采集实时窗口，并只修复最近 `SRE_AGENT_GAP_LOOKBACK_HOURS` 范围内的小缺口。
- Historical gap backfill：用 cron 或 Kubernetes CronJob 周期运行 `scripts/historical_gap_backfill.py`，扫描最近半个月但排除 runner gap window 的区间；没有缺口直接退出，有缺口再调用 bulk collector。

示例命令：

```bash
python3 scripts/historical_gap_backfill.py \
  --history-days 15 \
  --exclude-recent-hours 24 \
  --max-range-hours 24 \
  --max-ranges 1
```

如果先跑单服务验证：

```bash
python3 scripts/backfill_15m_bulk.py \
  --service auth-api \
  --batch-size 100
```

实时窗口采集使用 `collect_windows.py`，从最近一个完整 15 分钟窗口开始：

```bash
python3 scripts/collect_windows.py \
  --start <window_start_utc> \
  --end <window_end_utc> \
  --skip-github \
  --skip-kubernetes-events \
  --kubectl-aws-profile pro \
  --kubectl-proxy-url socks5://127.0.0.1:1080 \
  --batch-size 10
```

New Relic 采集依赖环境变量：

```bash
export NEW_RELIC_API_KEY=...
```

写入 `service_metric_windows.newrelic` 的核心字段：

| 字段 | 说明 |
| --- | --- |
| `request_count` | 窗口内 Transaction 数 |
| `rpm` | 每分钟请求数 |
| `error_rate_percent` | 错误率百分比 |
| `latency_p95_ms` | p95 duration，毫秒 |
| `latency_p99_ms` | p99 duration，毫秒 |

如果本地没有安装 GitHub CLI，可以先跳过 GitHub 采集：

```bash
python3 scripts/collect_windows.py \
  --service auth-api \
  --start 2026-06-04T09:45:00Z \
  --end 2026-06-04T10:00:00Z \
  --max-windows 1 \
  --skip-github
```

GitHub 采集依赖 `gh api`。未安装或未认证时，collector 会把错误写入 `data_quality.errors`，不会中断 Prometheus 数据写入。

Kubernetes inspect 采集依赖本机 `kubectl` 和只读 kubeconfig 权限。collector 会执行：

```bash
kubectl get deployment <workload> -n <namespace> -o json
kubectl get pods -n <namespace> -l <selector-from-workload> -o json
kubectl get events -n <namespace> -o json
```

当前生产集群使用 `storehub-pro`。该集群 API 是私有地址，本地开发时需要先通过 jumpserver 建立 SSH 动态隧道：

```bash
ssh -i .ssh/jumpserver.pem -N -D 127.0.0.1:1080 root@54.169.8.128
```

保持上面的 SSH 进程运行后，collector 通过 SOCKS proxy 访问 Kubernetes API。这样 kubectl 仍访问 kubeconfig 里的原始 API hostname，不需要改 server 地址，也不会遇到 TLS hostname 不匹配。

如果 kubeconfig context 里的 exec profile 与本机 AWS profile 不一致，可以显式覆盖，collector 会生成临时 kubeconfig，不会修改真实 kubeconfig：

```bash
python3 scripts/collect_windows.py \
  --service auth-api \
  --start 2026-06-04T09:45:00Z \
  --end 2026-06-04T10:00:00Z \
  --max-windows 1 \
  --skip-newrelic \
  --skip-prometheus \
  --skip-github \
  --kubectl-aws-profile pro \
  --kubectl-proxy-url socks5://127.0.0.1:1080
```

部署到 Kubernetes 集群内后，不需要 `--kubectl-proxy-url` 和 jumpserver 隧道；服务应使用挂载的 ServiceAccount token 访问 Kubernetes API，并通过 RBAC 授权只读 inspect 权限。

本地验证时的错误判断：

| 错误 | 含义 | 下一步 |
| --- | --- | --- |
| `TLS handshake timeout` | 本地无法连到私有 Kubernetes API | 先建立 jumpserver SOCKS 隧道，并传 `--kubectl-proxy-url` |
| `the server has asked for the client to provide credentials` | 网络已经打通，但当前 kube credential 未被集群接受 | 检查 EKS IAM/kubeconfig；部署到集群内后改用 ServiceAccount + RBAC |

写入 `service_metric_windows.kubernetes` 的核心字段：

| 字段 | 说明 |
| --- | --- |
| `replicas` | desired/ready/updated/available 和 rollout 完成状态 |
| `pod_count` | selector 匹配到的 Pod 数 |
| `restart_count` | 当前 Pod container restart 总数 |
| `oom_killed_count` | 最近 terminated reason 为 OOMKilled 的容器数 |
| `probe_failure_count` | 窗口内 `Unhealthy` event 数 |
| `failed_scheduling_count` | 窗口内调度失败 event 数 |
| `image_pull_failure_count` | 镜像拉取相关失败 event 数 |
| `waiting_reasons` | 当前 waiting reason 计数 |

如果暂时不能访问 Kubernetes，可以跳过：

```bash
python3 scripts/collect_windows.py \
  --service auth-api \
  --start 2026-06-04T09:45:00Z \
  --end 2026-06-04T10:00:00Z \
  --max-windows 1 \
  --skip-kubernetes \
  --skip-github
```
