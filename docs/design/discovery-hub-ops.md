# Discovery Hub 部署与运维手册

设计见 [discovery-hub.md](discovery-hub.md)，实现拆分见
[discovery-hub-impl.md](discovery-hub-impl.md)。本文档面向部署与运维。

## 1. 何时需要 hub

TiProxy 以 per-pod sidecar 方式大规模部署（N 数百上千实例）时，每个实例直连 PD
etcd 轮询拓扑会把 PD 压垮（读放大 O(tiproxy × tidb)，详见设计 §1）。hub 把 PD 的
watch 消费者从 N 收敛到 K（hub 副本数，≈3）。

小规模部署（几个 TiProxy）**不需要** hub，保持默认 `discovery-source = "pd"`。

## 2. 部署拓扑

```
PD etcd ── watch(K 条) ── tiproxy discovery ×K ── gRPC push ── sidecar tiproxy ×N
```

### Hub（`tiproxy discovery`）

同一个 tiproxy 二进制，`discovery` 子命令。每副本独立 watch PD，副本间无协调，
无状态（重启后重新 bootstrap）。

```toml
# hub.toml —— hub 连 PD，在 api.addr 上服务
[proxy]
pd-addrs = "pd-0:2379,pd-1:2379,pd-2:2379"
[api]
addr = "0.0.0.0:3080"
```

```
tiproxy discovery --config hub.toml
```

Docker 镜像内置模板 `/etc/proxy/hub.toml`（源码 `conf/hub.toml`），k8s 里同一镜像
换 args 即可：

```yaml
# hub Deployment（与 sidecar 同一镜像）
command: ["/bin/tiproxy", "discovery", "--config", "/etc/proxy/hub.toml"]
# 用 ConfigMap 挂载覆盖 /etc/proxy/hub.toml，至少要设 pd-addrs
```

构建镜像：`make docker DOCKERPREFIX=<repo>/ IMAGE_TAG=<tag>`（多平台发布用
`make docker-release`）。

- K≥2 副本挂在一个 k8s Service（如 `tidb-discovery.<ns>.svc:3080`）后面。
- readiness 探针：gRPC health（首次拓扑 bootstrap 成功后才 SERVING），或 HTTP
  `GET /debug/health`。PD 不可达时 hub 永不 ready，k8s 自动把 sidecar 挡在空 hub
  外面。
- hub **要求恰好一个 PD 集群**配置（`pd-addrs` 或单个 `backend-clusters` 条目）。
  多 TiDB 集群 = 每集群一组 hub。

### Sidecar（`tiproxy` 代理模式）

```toml
# sidecar.toml —— 零 PD 依赖
[proxy]
pd-addrs = ""            # 显式留空，避免默认值困惑（代码按 source 分支，非判空）
[[proxy.backend-clusters]]
name = "c1"
discovery-source = "hub"
hub-addrs = "tidb-discovery.default.svc:3080"
```

- `discovery-source` 按**集群条目**配置，同一个 proxy 里 pd/hub 集群可混用。
- hub-addrs 用 k8s Service DNS 或静态列表下发，**不要**让 sidecar 到 PD 查 hub
  地址（否则又引回 PD 依赖）。
- 约束：`ha.virtual-ip` 与 hub 集群互斥（VIP 选主依赖 PD etcd，配置校验直接报错；
  mesh 场景本来也不用 VIP）。

## 3. 行为差异（hub 模式 vs pd 模式）

| 项 | pd（默认） | hub |
|---|---|---|
| 拓扑感知 | 每 3s 轮询 PD | hub 推送（秒级），本地缓存 |
| PD 连接 | 每实例一个 etcd client | **零** |
| 自注册 `/topology/tiproxy` | 是 | 否（sidecar 不出现在 PD 拓扑里） |
| 健康检查 | sidecar 本地打 TiDB status port | 同左（不变） |
| metrics 均衡（默认 resource 策略） | owner 选举 + 去重读取 | **仍工作**：无选举，每实例直接从 backend 读（missing-metrics 路径）；代价是 TiDB status port 承受 N 份读取 |
| VIP | 支持 | 配置报错 |

## 4. 灰度（canary）步骤

按集群条目逐步切换，`discovery-source`/`hub-addrs` 可热改（配置 reload 时该集群
会重建）：

1. 部署 K 个 hub 副本，确认全部 ready（health SERVING，`tiproxy_discovery_backends`
   与实际 TiDB 数一致）。
2. 选 1 台 sidecar，把目标集群条目改为 `discovery-source = "hub"`，reload。
3. 对比该实例与 pd 模式实例的后端列表（`/api/backend` 或日志），观察
   `tiproxy_discovery_subscribers` +1。
4. kill 一个 TiDB（或缩容），确认 canary 实例在 lease TTL（45s）+ 推送延迟内摘除
   该后端。
5. 分批扩大；每批观察 hub 侧指标（见 §6）。
6. 全量后确认 PD etcd 侧 watch/range 指标降到 O(K)。

## 5. 回滚

- **单实例回滚**：集群条目改回 `discovery-source = "pd"`（补上 pd-addrs），
  reload 即可，集群会用 PD 路径重建。
- **hub 全挂（不回滚场景）**：sidecar 用最后已知缓存继续服务，路由不中断。缓存
  内的后端仍由本地健康检查治理：宕机被围栏，**重启后自动恢复可路由**（e2e 实测
  确认）；只有缓存从未见过的**真正新增** TiDB 不可见。恢复 hub 或按上面回滚。

## 6. 监控

Hub 侧指标（`/metrics`，前缀 `tiproxy_discovery_`）：

| 指标 | 含义 | 告警建议 |
|---|---|---|
| `subscribers` | 当前订阅者数 | 明显低于 sidecar 数 → sidecar 连不上 |
| `backends` | 快照内存活 TiDB 数 | 与实际 TiDB 数不符 / 各 hub 副本间不一致 |
| `revision` | 快照对应的 etcd revision | 各副本间长期差距大 → 某副本 watch 落后 |
| `rebootstrap_total` | watch 重建次数 | 持续增长 → PD 连接不稳 / compaction 频繁 |
| `sub_dropped_total` | 慢订阅者被断次数 | 持续增长 → sidecar 消费不动或网络差 |
| `broadcast_total{type}` | full/delta 广播次数 | delta 速率异常高 → 拓扑抖动 |

PromQL 示例：

```promql
# 各 hub 副本 revision 偏差
max(tiproxy_discovery_revision) - min(tiproxy_discovery_revision)
# 订阅者掉线率
rate(tiproxy_discovery_sub_dropped_total[5m])
# watch 重建频率
rate(tiproxy_discovery_rebootstrap_total[15m])
```

Grafana 面板：待 grafonnet 工具链接入后补进 `tiproxy_summary.jsonnet`
（Discovery row，上表 6 个指标）。

Sidecar 侧无新指标；hub 断连体现在日志
（`the subscription to the discovery hub is broken, reconnecting`）与后端列表停止
更新。

## 7. 排障

**症状：sidecar 反复打 `the subscription to the discovery hub is broken, reconnecting`，拓扑不更新。**

按序排查：

1. hub 是否 ready：`grpc_health_v1` SERVING / `tiproxy_discovery_backends` 正常。
   hub 连不上 PD 时永不 ready，所有订阅被 readiness 拒绝。
2. 网络可达：sidecar 能否连通 `hub-addrs`。
3. **TLS 配置是否对称**：hub 侧 api TLS 用 `[security.server-http-tls]`，sidecar
   侧 HubClient 用 `[security.cluster-tls]`（与集群内组件互访 api 的既有配对一致）。
   任一侧单边开 TLS，连接会被立刻重置（cmux 不匹配明文 h2c / TLS 握手打进明文
   端口），表现就是这个快速重连循环 —— 这是有意的 fail-fast，不要通过在 TLS 端
   点上放行明文 gRPC 来"修复"。

**症状：sidecar 后端列表长期为空，日志有 `the discovery hub has not pushed a topology snapshot yet`。**

订阅从未成功建立（见上），或 hub 侧拓扑本身为空
（`tiproxy_discovery_backends == 0`，检查 PD 里 `/topology/tidb/` 是否有存活
TiDB）。

## 8. 容量参考

- 单 hub fan-out：单元测试中 200 订阅者 × 全量+增量 <1s（`TestHubManySubscribers`）。
  生产按 N/K 每副本数百订阅者规划，瓶颈在 gRPC 连接数而非 CPU（稳态零广播，见设计
  F5/F16）。
- 全量快照体积 ≈ TiDB 数 × ~200B；1000 sidecar 同时重连（hub 重启）由 full 响应
  缓存（F17）+ 客户端 ±20% jitter 摊平。
- hub 对 PD 的负载：每副本 1 次 bootstrap Txn + 2 条 watch 流 + 每 30s 一次
  Prometheus 信息读取。
