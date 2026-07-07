# TiDB 拓扑发现(Discovery)—— sidecar 模式下的 TiProxy

TiProxy 以 per-pod sidecar 大规模部署(N 数百上千实例)时,不再直连 PD,而是
轮询一个集群侧的拓扑服务。本文档描述该方案的最终设计。

系列文档:[实现导览](discovery-hub-impl.md) · [部署运维](discovery-hub-ops.md) ·
[E2E 测试](discovery-hub-e2e.md)

## 1. 问题

默认模式(`discovery-source = "pd"`)下,每个 TiProxy 实例自带一个 etcd 客户端
直连 PD,每 3s(`HealthCheck.Interval`)全量拉取 TiDB 拓扑。N=1000 时对 PD 内嵌
etcd 的负载:

| 项 | 频率 | 1000 实例 |
|---|---|---|
| 拓扑全量读 | 2× range-Get / 3s | ~667 次/s,**linearizable** |
| 自身拓扑写 info+ttl | 每 30s | ~67 raft 写/s |
| lease/session keepalive | ~15s | 1000 个 lease + keepalive 流 |
| metricsreader 选举 watch | 常驻 | 1000 个 watch 流 |

核心是**读放大**:linearizable range 读走 raft leader read-index,与 TSO/调度抢
leader;egress 随 `tiproxy 数 × tidb 数` 增长。

## 2. 方案总览

```
                 ┌────────── PD (embedded etcd) ──────────┐
                 │   /topology/tidb/*  /keyspaces/tidb/*   │
                 └───────▲────────────────────▲───────────┘
                         │ watch + Txn (K 条)  │
              ┌──────────┴─────┐   ┌──────────┴─────┐
              │ tidb-discovery │...│ tidb-discovery │   K≈3,无状态,副本对等
              │  (pd-server)   │   │  (pd-server)   │   ← pingkai/pd 仓库
              └──────────▲─────┘   └──────────▲─────┘
                         │ HTTP 轮询 + ETag(304)│
        ┌────────────────┼─────────────────────┼──────────┐
        │                │                     │          │
   sidecar-1        sidecar-2      ...    sidecar-N       ← tiproxy 仓库
   (HubClient 本地缓存;router/observer 读缓存)
```

两个组件,两个仓库:

| 组件 | 仓库 | 职责 |
|---|---|---|
| **tidb-discovery 服务** | pingkai/pd,`pkg/mcs/tidbdiscovery`,`pd-server services tidb-discovery` | watch PD 拓扑一次,以 HTTP + ETag 供轮询;集群侧组件,随 PD/TiDB 集群部署与发布 |
| **HubClient(sidecar)** | tiproxy,`pkg/discovery` | 每 3s 轮询,维护本地拓扑缓存,作为 backend cluster 的 topology source |

负载收敛:PD etcd 的消费者从 N 降到 K;稳态流量 = N 次**空 304** / 3s,由 K 副
本分摊;服务内部 1 次序列化缓存,每请求 O(1)。实测(e2e S10,30 实例 fleet,
60s 窗口):PD etcd range 请求 pd 模式 2800 vs hub 模式 160(**17.5×**,hub 模
式的 160 全部为 PD 自身与 TiDB 的本底,fleet 贡献为零)。

## 3. 线协议

```
GET /api/topology
  If-None-Match: <etag>        # 可选;命中回 304 空 body
→ 200 OK, ETag: <fnv64a of body>
  {"revision": 254674,
   "backends": [{"addr":"10.0.0.1:4000","ip":"10.0.0.1","status_port":10080,
                 "labels":{"zone":"z1"},"keyspace":"ks1","version":"v8.5.5"}],
   "prometheus": {"ip":"...","port":9090}}
→ 503                          # 服务尚未完成首次 bootstrap
```

- **JSON 字段名是两仓库间的契约**,由两侧各一份 golden-JSON 测试互锁
  (tiproxy `pkg/discovery/types_test.go`,pd `pkg/mcs/tidbdiscovery/server/topology_test.go`)。
  改字段必须两仓库同步。
- **ETag = 响应内容的 fnv64a hash**,不是本地计数器:client 在副本间 failover
  时,计数器会碰撞产生假 304(实测抓到的 bug);内容 hash 跨副本语义天然正确,
  内容相同回 304 是合法优化。
- `revision` 是拓扑最后一次变化时的 etcd revision,仅供观测;新鲜度由 ETag 协商。
- 升级路径:`GET /api/topology?wait=30s` 即 long polling(rev 变了立即返回,否则
  超时 304),协议前向兼容。
- 可观测性是选 HTTP 的核心动因:`curl <hub>:3080/api/topology | jq .` 随手查
  拓扑,零客户端依赖。

## 4. 服务端(pingkai/pd `pkg/mcs/tidbdiscovery`)

mcs 微服务外壳(子命令/BaseServer/InitClient/StartGRPCAndHTTPServers/注册
service registry 供 pd-ctl 观测),**无 primary 选举** —— 副本无状态且对等,
客户端失败轮换即可;gRPC 端口只承载标准 diagnostics。

拓扑维护(watchLoop,唯一碰 etcd 的 goroutine):

1. **Bootstrap**:单个 etcd `Txn` 同时读 `/topology/tidb/` 与
   `/keyspaces/tidb/` 两前缀 —— 两次独立 Get 会落在不同 revision,watch 起点与
   快照错位漏/重事件;Txn 保证单一 `baseRev`,两条 watch 都从 `baseRev+1` 起。
2. **存活判定**:`info` 与 `ttl` key 配对,`ttl` 缺失(lease 过期)即视为宕机
   丢弃;支持 keyspace 前缀剥离;按 key 排序遍历保证结果确定(ETag 是序列化的
   内容 hash,顺序必须稳定)。
3. **ttl 刷新短路**:TiDB 每 ~30s 重 Put info+ttl,ttl 值是时间戳必然变化,但
   存活判定只看 ttl **存在性** —— 一批事件若全是"已存在 key 的 PUT 且(是 ttl
   或 info 值未变)",跳过重解析与重序列化。稳态开销归零,轮询端持续 304。
4. **watch 失败全量重建**:channel 关闭或 `Canceled`(compaction 等)→ 回到
   bootstrap。注意 channel 关闭读到的是零值(`Canceled=false`),必须检查
   `ok` 标志,否则热循环。
5. 内容变化时重建一次序列化缓存(全请求共享只读字节)+ 重算 ETag;快照
   copy-on-write,绝不原地修改。
6. Prometheus 信息(`/topology/prometheus`)每 30s 刷新,变化时同样重建缓存。

首次 bootstrap 前 `/api/topology` 回 503 —— readiness 探针直接用该端点。

指标(前缀 `tidb_discovery_`):`requests_total{code}`、`backends`、`revision`、
`rebootstrap_total`。

## 5. Sidecar 端(tiproxy `pkg/discovery`)

### 注入点

`backendcluster.Cluster` 通过内部 `topoSource` 接口消费拓扑:

```go
type topoSource interface {
    GetTiDBTopology(context.Context) (map[string]*infosync.TiDBTopologyInfo, error)
    GetPromInfo(context.Context) (*infosync.PrometheusInfo, error)
    Close() error
}
```

`NewCluster` 按集群条目的 `discovery-source` 分支:`"pd"`(默认)装
`InfoSyncer`(现状路径);`"hub"` 装 `HubClient` 且**不建 etcd 客户端**。
必须按 source 显式分支而非判空地址 —— `proxy.pd-addrs` 有默认值
`127.0.0.1:2379`。Manager/nsMgr/observer/router/跨集群合并对此无感知。

### HubClient

- 每 3s(与健康检查同量级)GET 一次,带 `If-None-Match`;304 无事,200 全量
  替换本地缓存(copy-on-write,交出的快照不被后续更新修改)。
- **sticky + 轮换**:粘住当前 hub 地址,失败换下一个;fleet 的轮询时钟因启动
  时间不同天然错开,无惊群。
- 首个快照到达前 `GetTiDBTopology` 返回错误,调用方(PDFetcher)无限重试 ——
  语义与"PD 未就绪"一致。
- observer 的 3s 健康检查循环读本地缓存 —— 拓扑感知端到端延迟与 pd 模式相同
  量级(对照 lease TTL 45s 都是小量)。

### 配置

```toml
[[proxy.backend-clusters]]
name = "default"
discovery-source = "hub"        # pd(默认)| hub
hub-addrs = "tidb-discovery.<ns>.svc:3080"   # 逗号列表或 Service DNS
```

- 按**集群条目**配置,同一 proxy 内 pd/hub 可混用、可逐集群灰度;两字段可热改
  (reload 时该集群重建)。
- 校验:hub 必须给 `hub-addrs`;`ha.virtual-ip` 与 hub 集群互斥(VIP 选主依赖
  PD etcd;mesh 场景无 VIP)。
- TLS:sidecar 用 `[security.cluster-tls]`,服务端用 PD 组件证书,同一集群 CA。

## 6. 失败模式

| 场景 | 行为 |
|---|---|
| 单副本挂 | sidecar 下一轮轮询换地址,秒级 |
| **全部副本挂** | sidecar 用最后已知缓存继续服务;缓存内后端仍由本地健康检查治理 —— 宕机围栏、**重启自动恢复可路由**(e2e 实测);只有缓存从未见过的新 TiDB 不可见 |
| 服务重启 | 无状态,重新 bootstrap;内容未变则 ETag 不变,轮询端无感 |
| PD 挂 | 服务持续 bootstrap 重试(未 ready 阶段 503);已 ready 的副本缓存继续服务 |
| sidecar 冷启动且服务不可达 | 同"PD 挂"现状:无后端,PDFetcher 重试,新连接失败直至恢复 |

## 7. 已知特性:metrics 均衡

`balance.policy` 默认 `resource`(CPU/内存因子)。pd 模式靠 etcd 选举 owner 采
集并去重分发;hub 模式无 etcd,选举不启动,但 metricsreader 的 missing-metrics
路径让**每个实例直接从 backend status port 读取** —— resource 策略照常工作,
代价是 TiDB status port 承受 N 份采集。N×T 采集量成为实际问题时,可让拓扑服务
顺带采集并将负载样本并入响应(协议兼容的扩展)。

## 8. 关键决策记录

| 决策 | 依据 |
|---|---|
| **HTTP 轮询而非 gRPC 推送** | 控制面对延迟不敏感(sidecar 本就 3s 健康检查周期),对可观测性极度敏感(curl 直查/零依赖);轮询消灭推送模型全部自带复杂度(订阅者管理/慢消费者/delta 协议/断线续传/proto 工具链),服务端变无状态。曾以 gRPC 流实现并通过全部测试,替换后净删 ~940 行 |
| **ETag 用内容 hash** | 本地计数器跨副本碰撞产生假 304(failover 单测抓到);内容 hash 跨副本语义天然正确 |
| **组件归 PD 仓库** | 集群侧组件:生命周期/版本/发布随 PD/TiDB 集群走(tiup/operator 一并分发),不随应用侧 tiproxy;etcd 访问面不出集群边界,sidecar→服务是最耐远的 HTTP 链路 |
| **外置进程(mcs 微服务)而非 PD API 进程内 handler** | PD 是集群命脉,进程内代码共享其故障域;外置副本独立扩缩/重启/发版,异常秒级切换 |
| **无 primary 选举** | 副本无状态对等,谁都能答;选举反而制造单点语义 |
| **sidecar 地址不走 PD service registry** | 走 Service DNS/静态配置 —— 否则 sidecar 又引回 PD 依赖(设计红线) |
| **v1 不做** | long polling(协议已兼容,需要时加 `?wait=`)、keyspace 过滤下推、断线 delta 续传 |

## 9. 后续

- k8s 千级压测(kind/真集群专项;30 实例的编排与负载对比见 e2e S9/S10,已实测)。
- 上游化(pingcap/tiproxy + tikv/pd)。
