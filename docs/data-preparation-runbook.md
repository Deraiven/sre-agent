# 数据准备 Runbook

## 当前状态

已完成本地数据准备控制面：

| 项 | 状态 |
| --- | --- |
| 服务目录 | `config/service-catalog.yaml`，52 个服务 |
| New Relic account | `464254` StoreHub |
| New Relic entity GUID | 已根据 app id 自动补齐 |
| Prometheus 连接 | 已验证健康 |
| Prometheus 资源指标 | 已验证 CPU、memory、throttling、network 指标存在 |
| 训练窗口 | 30 天，15 分钟窗口 |
| 预计样本量 | 52 * 2880 = 149,760 个窗口 |

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
| `anomaly_windows` | 异常标签 |
| `slo_recommendations` | SLO 建议 |

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

## 下一步

1. 写 collector，把 `collection_plan.jsonl` 转成实际 New Relic、Prometheus、Kubernetes、GitHub 查询。
2. 每个 15m 窗口写入 `service_metric_windows`。
3. 基于 SLO 和动态基线写入 `anomaly_windows`。
4. 生成 `slo_recommendations`，交给 owner 审核。

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

不要对 `5m`、`15m`、`1h`、`1d` 全部窗口都直接查询外部系统。第一阶段使用 `15m` 作为主训练粒度，覆盖近 30 天。52 个服务约产生 `52 * 2880 = 149,760` 条窗口，已经足够建立 SLO baseline、异常标签和风险模型。

推荐窗口策略：

| 窗口 | 是否直接采集 | 用途 |
| --- | --- | --- |
| `15m` | 是，主窗口 | SLO、异常检测、风险预测训练主粒度 |
| `1h` | 否，从 15m 聚合 | 趋势、报表、降噪后的容量观察 |
| `1d` | 否，从 15m 聚合 | 周期性、周报、SLO 推荐证据 |
| `5m` | 只在 incident 附近或核心服务采 | 快速故障定位、突刺分析 |

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
2. 从今天开始持续采集 Kubernetes inspect；只有 incident 附近再补 `5m` 精细窗口。

示例命令：

```bash
python3 scripts/backfill_15m_bulk.py \
  --batch-size 500
```

`backfill_15m_bulk.py` 会把 New Relic 查询切成 3 天一段，避免 New Relic `TIMESERIES` 366 buckets 限制；Prometheus 会按服务和指标做 30 天 `query_range`，避免逐窗口查询。

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
