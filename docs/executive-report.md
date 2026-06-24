# SRE Agent 第一版简要报告

日期：2026-06-09

## 一、建设目的

SRE Agent 的目标是把线上稳定性工作从“告警后人工排查”推进到“提前识别风险、快速定位原因、给出可执行整改建议”。

第一版重点解决三个问题：

1. 服务是否正在接近危险状态。
2. incident 发生后，能否快速整理指标、链路、Kubernetes 状态和代码变更证据。
3. 能否基于证据给出可信的根因假设排序和整改建议。

这个 Agent 不替代 on-call 决策，而是作为 SRE/研发的诊断副驾驶，帮助缩短发现、定位和恢复时间。

## 二、当前完成度

第一版核心能力已经完成，当前完成度约为 70%-80%，可以进入内部试用和持续数据积累阶段。

| 模块 | 完成情况 |
| --- | --- |
| 服务目录 | 已完成 51 个生产服务 mapping，包含 New Relic、GitHub、Kubernetes、Prometheus、SLO、tags 等上下文 |
| 历史数据回填 | 已支持近 30 天 New Relic + Prometheus 15 分钟窗口回填 |
| 实时采集 | 已支持服务启动后每 15 分钟自动采集最近完整窗口 |
| Gap recovery | runner 负责最近 24 小时内的小缺口自愈，大量历史缺口由独立 historical backfill 进程处理，避免拖慢实时采集 |
| 动态基线 | 已支持按服务计算 baseline，避免所有服务使用同一套固定阈值 |
| 异常标注 | 已支持基于 SLO、动态基线、资源和 Kubernetes 信号标记异常窗口 |
| Risk scoring | 已支持查询服务当前风险等级和主要风险原因 |
| Incident inspect | 已支持被动接收 incident 查询，并汇总 New Relic、Prometheus、Kubernetes、GitHub 和 trace 证据 |
| 根因假设排序 | 已支持输出 hypothesis ranking，用 evidence 和 confidence 辅助人工判断 |
| 文档和运行手册 | 已补齐数据准备、数据存储、运行、incident inspect、配置说明等文档 |

## 三、已实现价值

当前版本已经具备可落地的稳定性数据闭环：

- 把 New Relic、Prometheus、Kubernetes 和 GitHub 信息统一到服务维度。
- 将短期监控留存转成长期训练数据，避免 30 天后历史数据丢失。
- 通过动态 baseline 识别“对这个服务来说不正常”的状态，而不是使用一刀切阈值。
- incident 排查时自动整合黄金指标、资源指标、Kubernetes 状态、trace 摘要和 master 分支变更上下文。
- 输出风险等级、异常证据和整改方向，减少人工在多个系统之间来回查询的时间。

## 四、当前限制

第一版仍有一些已知限制：

| 限制 | 影响 |
| --- | --- |
| Kubernetes inspect 依赖本地 jumpserver tunnel | 本地运行时隧道中断会影响 K8s 证据采集 |
| 部署事件暂未接入 | 当前用 GitHub master commit 作为变更代理，只能作为 inferred 证据 |
| owner 和部分 SLO 仍需人工确认 | 配置已补齐，但业务 owner 和正式 SLO 需要服务团队审核 |
| Trace 目前是摘要级别 | 可以辅助 RCA，但还不是完整 waterfall 级追踪分析 |
| Runner 与 API 暂在同一进程 | 已通过后台 worker 降低阻塞风险，但生产环境仍建议拆成独立 Deployment/worker |

## 五、下一步建议

建议下一阶段聚焦三件事：

1. 生产化运行：把 Agent 部署到 `storehub-pro` 集群内，使用 RBAC 直接访问 Kubernetes，减少本地隧道依赖。
2. 数据质量提升：补齐 owner、正式 SLO、服务依赖和 runbook，并持续积累 60-90 天训练数据。
3. 智能分析增强：增加 runner 运行记录、incident 人工反馈闭环、真实部署事件接入，并把根因排序与整改建议做成可评估的质量指标。

整体判断：SRE Agent 第一版已经完成从数据采集、动态基线、风险评分到 incident 调研的主链路，可以开始内部试运行；后续重点是生产化部署、数据质量治理和诊断效果评估。
