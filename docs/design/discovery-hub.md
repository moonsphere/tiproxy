# TiProxy Discovery Hub 设计（sidecar mesh）

> **宿主迁移（2026-07-07）**：hub 已从 tiproxy 迁入 PD 仓库,形态为
> `pd-server services tidb-discovery`(mcs 微服务,无 primary 选举,副本对等),
> tiproxy 侧 hub server(`tiproxy discovery` 子命令)已删除,sidecar 侧
> (HubClient/wire 契约/配置)不变。决策与实现见
> [discovery-hub-pd-external.md](discovery-hub-pd-external.md);本文档 §4/§5 的
> hub 宿主描述为 tiproxy 时代的设计记录,机制(watch/ETag/短路)在 PD 侧原样保留。
>
> **传输层修订（2026-07-06）**：v1 传输从 gRPC 服务端流改为 **HTTP 轮询 + ETag**。
> 动因：控制面对延迟不敏感（sidecar 侧本来就有 3s 健康检查周期），但对**可观测性
> 极度敏感** —— HTTP 端点可以 curl 直查、零客户端依赖；且轮询消灭了推送模型的全部
> 自带复杂度（订阅者管理、慢消费者、delta 协议、重连续传语义、proto 工具链），hub
> 变**无状态**。文中 F4/F9/F12/F17 等推送模型的发现随传输层一并成为历史，保留作设
> 计记录。升级路径：GET 加 `?wait=30s` 即 long polling（etcd watch 的祖师爷模型），
> 协议前向兼容。

## 1. 背景与问题

把 TiProxy 作为 per-pod sidecar 部署（service mesh）意味着 N≈1000 个 TiProxy 实
例。今天每个实例都自带一个 etcd 客户端直连 PD、各自轮询 TiDB 拓扑：

- `pkg/balance/observer/backend_fetcher.go` `PDFetcher.GetTiDBTopology`
- `pkg/manager/infosync/info.go:257` `InfoSyncer.GetTiDBTopology` —— 两次
  `etcdCli.Get(prefix)`，**linearizable、返回整个 `/topology/tidb/*`**
- 由 `DefaultBackendObserver.observe` 每 `HealthCheck.Interval`（3s）驱动

对 PD 内嵌 etcd 的负载按 **O(tiproxy × tidb)** 增长：

| 项 | 频率 | 1000 实例 |
|---|---|---|
| 拓扑全量读（`info.go:259,263`） | 2× range-Get / 3s | **~667 次/s，且 linearizable** |
| 自身拓扑写 info+ttl | 每 30s | ~67 raft 写/s |
| lease/session（TTL45）keepalive | ~15s | 1000 个 lease + keepalive 流 |
| metricsreader 选举 watch（`backend_reader.go:125`） | 常驻 | 1000 个 watch 流 |
| AutoSync 成员同步 | 30s | 1000× |

真正的瓶颈是**读放大**：`GetTiDBTopology` 用默认 `Get`（无 `WithSerializable` →
linearizable，走 leader read-index），且 `WithPrefix` 每次拉**全量**：
`egress ≈ tidb数 × info大小 × 667/s`，随 `tiproxy × tidb` 平方增长。而 PD 内嵌
etcd 同时扛 TSO、region 元数据、调度，这些 linearizable 读会跟控制面抢 raft leader。

## 2. 方案：Hub 控制面（xDS / Istio-pilot 模型）

插入一层薄控制面（Discovery Hub）。hub 只 watch PD **一次**（每个 hub 副本一次），
以 HTTP + ETag 供 sidecar 轮询（详见 §6）。sidecar 永不碰 PD。PD 的 watch 消费者从
N（1000）降到 K（hub 副本数，≈3）。

```
        ┌─────────── PD (embedded etcd) ───────────┐
        │  /topology/tidb/*   /keyspaces/tidb/*     │
        └───────▲───────────────────────▲──────────┘
                │ Watch+Get (K 条流)      │
        ┌───────┴───────┐        ┌───────┴───────┐
        │  Hub 副本 1    │  ...   │  Hub 副本 K    │   K≈3, HA
        │  snapshot+rev  │        │  snapshot+rev  │
        └───────▲───────┘        └───────▲───────┘
                │ HTTP 轮询 + ETag(304)    │
   ┌────────────┼─────────────┬───────────┼───────────┐
   │            │             │           │           │
 sidecar-1   sidecar-2  ...  sidecar-i ... sidecar-N   (N≈1000)
 HubClient   HubClient       HubClient     HubClient
 (本地缓存；GetTiDBTopology 读缓存)
```

tidb 拓扑变化 → K 个 etcd watch 事件 → hub 内容/ETag 更新,N 个 sidecar 下一轮轮询
拿到新拓扑,PD 零额外负载。稳态 = N 次空 304 往返 / 3s,由 K 台 hub 分摊。

### 为什么方案 A（tiproxy 运行模式）起步

hub 的宿主有两个候选：**A** = tiproxy 二进制的一个运行模式（本仓库）；**B** = PD
的一个微服务（`pd-server services tidb-discovery`）。

- **A 起步**：拓扑解析代码 + proto + HubClient 全在 tiproxy，团队能不等 PD 发版、
  不跨团队评审直接上，快速拿 PD 负载下降数据。
- **B 是长期正解**：PD 已有微服务框架、`services <mode>` 子命令、MetaStorage etcd
  封装、HA，dashboard 也在读 `/topology/*`。
- **关键**：线协议是稳定契约 —— `GET /api/topology` 的 JSON 字段名（§6）。未来
  A→B 时 server 换宿主，路径与 JSON 不变，sidecar 只把 `hub-addrs` 重新指向。

本文档设计 **A**。

## 3. 注入点：为什么 TiProxy 侧改动极小

> 2026-07 上游引入 `backendcluster`（#1150，多 PD 集群支持）后，注入点从
> `server.go` 移进了 `Cluster` —— 对本方案**更干净**。

新架构：`backendcluster.Manager`（`pkg/manager/backendcluster/`）按配置管理 N 个
`Cluster`；每个 `Cluster` 是一个 PD 集群的容器，**私有** `{etcdCli, infoSyncer,
ClusterReader(metrics), httpCli, DNSDialer}`（`cluster.go:23`）。上层全部通过
Manager 消费：

- `nsMgr.Init(..., srv.clusterManager, ...)`（`server.go:143`）——
  Manager 实现 `observer.TopologyFetcher`（`GetTiDBTopology` 跨集群合并 +
  `HasBackendClusters`，`manager.go:217`）；
- metrics 走 `clusterManager.MetricsQuerier()`；VIP 走
  `PrimaryCluster().EtcdClient()`。

`Cluster` 对拓扑的消费只有两个方法（`cluster.go:40-46`）：

```go
func (c *Cluster) GetTiDBTopology(ctx) (map[string]*infosync.TiDBTopologyInfo, error) {
    return c.infoSyncer.GetTiDBTopology(ctx)
}
func (c *Cluster) GetPromInfo(ctx) (*infosync.PrometheusInfo, error) {
    return c.infoSyncer.GetPromInfo(ctx)
}
```

所以注入点 = **`Cluster` 内部的 topology source**。抽一个小接口：

```go
type topoSource interface {   // *infosync.InfoSyncer 和 *discovery.HubClient 都满足
    GetTiDBTopology(context.Context) (map[string]*infosync.TiDBTopologyInfo, error)
    GetPromInfo(context.Context) (*infosync.PrometheusInfo, error)
    Close() error
}
```

`NewCluster`（`cluster.go:74`）按该集群的 `discovery-source` 配置装 `InfoSyncer`
（现状）或 `HubClient`（新）。**Manager、nsMgr、observer、router、跨集群合并逻辑
全部零改动** —— 它们只见 `Cluster.GetTiDBTopology`。sidecar 保留 3s observer 节奏
用于**健康检查**；但 `GetTiDBTopology` 变成读一个由轮询保持新鲜的本地缓存。

多集群与 hub 的关系天然对齐：**一个 hub 服务一个 PD 集群**；sidecar 的每个
`BackendCluster` 条目独立选 `pd` 或 `hub`。`TiDBTopologyInfo.ClusterName` 是本地
配置概念（Manager 合并时填，`manager.go:268`），**不进线协议**；`Addr` 由解析器
填（`info.go:299`），proto 里的 `addr` 与之对应。

---

## 4. 形态 —— `tiproxy discovery` 子命令

同一个 tiproxy 二进制，加一个运行模式：

```
tiproxy                      # 默认（rootCmd.RunE）—— 代理，行为不变
tiproxy discovery --config…  # 新增 —— 作为 discovery hub 运行
```

部署 K（≈3）个 `tiproxy discovery` 副本在一个 k8s Service 后。每副本独立 watch
PD → PD watch 消费者 = K。sidecar 把 `hub-addrs` 指向 Service DNS，彻底不碰 PD。

### 入口 —— `cmd/tiproxy/main.go`

```go
discoveryCmd := &cobra.Command{
    Use:   "discovery",
    Short: "run as a TiDB topology discovery hub for sidecar TiProxy",
    RunE: func(cmd *cobra.Command, _ []string) error {
        srv, err := server.NewDiscoveryServer(cmd.Context(), sctx)
        if err != nil {
            return errors.Wrapf(err, "fail to create discovery server")
        }
        <-cmd.Context().Done()
        return srv.Close()
    },
}
rootCmd.AddCommand(discoveryCmd)   // 复用同一套 --config/--advertise-addr 全局 flag
```

### 精简 server —— `pkg/server/discovery_server.go`

`NewDiscoveryServer` 只装配 hub 需要的东西。与 `NewServer`（`server.go:62`）对比：

| 组件 | 代理 `NewServer` | discovery `NewDiscoveryServer` |
|---|---|---|
| configManager、loggerManager、metricsManager、memManager | ✅ | ✅（复用） |
| certManager | ✅ | ✅ |
| etcdCli 连 PD | ✅（per-cluster，在 clusterManager 内） | ✅（自建，`etcd.InitEtcdClient`） |
| **Hub**（watch + fan-out） | — | ✅ **新增** |
| HTTP listener（`cfg.API.Addr`） | api.Server（完整） | 精简：/api/topology + diagnostics + /metrics + /debug |
| infoSyncer 写入循环 | ✅ | ❌（hub 不注册自己） |
| metricsReader / namespaceManager / proxy SQLServer / replay / meter / vip | ✅ | ❌ |

```go
func NewDiscoveryServer(ctx context.Context, sctx *sctx.Context) (*Server, error) {
    srv := &Server{
        configManager: mgrcfg.NewConfigManager(),
        metricsManager: metrics.NewMetricsManager(),
        certManager:   cert.NewCertManager(),
    }
    // ...与 NewServer 第 73-119 行相同的初始化：config→logger→metrics→cert→etcd
    // （抽成 initBase(ctx, sctx) 供两个 server 共用，避免漂移）

    srv.hub = discovery.NewHub(lg.Named("hub"), srv.etcdCli, cfg)
    srv.hub.Run(ctx)
    srv.apiServer, err = api.NewDiscoveryServer(cfg.API, lg.Named("api"), srv.hub, ready)
    ...
    return srv, nil
}
```

> **Close 的 nil 安全（已验证）**：`Server.Close()` / `preClose()` 对每个组件都做
> 了 nil 判空，discovery-mode 复用大 `Server` 结构体、其余字段留 nil，关闭干净。
> 只需加一行 `if s.hub != nil { s.hub.Close() }`。

## 5. Hub 内部 —— `pkg/discovery/hub.go`

> 包布局：hub 与 HubClient 同属发现基础设施，统一放 `pkg/discovery/`
>（`hub.go` + `client.go` + `types.go`）。

```go
type Hub struct {
    etcdCli *clientv3.Client
    lg      *zap.Logger
    mu      sync.RWMutex
    raw     map[string][]byte                     // 所有 /topology/{tidb,keyspaces} 的 kv（info+ttl）
    snap    map[string]*infosync.TiDBTopologyInfo // 派生出的存活快照（info+ttl 配对后）
    prom    *infosync.PrometheusInfo
    rev     int64                                  // etcd revision（观测用）
    respJSON []byte                                // 序列化好的 TopologyResponse，全部请求共享
    etag     string                                // respJSON 的 fnv64a hash
}
```

hub **无状态**：没有订阅者表，请求处理是纯读（`HandleTopology`：If-None-Match
命中回 304，否则回缓存的 respJSON + ETag）。内容变化时 `rebuildRespLocked` 重新
序列化一次并重算 hash —— 每请求 O(1)。

### watchLoop（唯一碰 PD 的 goroutine）

```go
func (h *Hub) watchLoop(ctx context.Context) {
  for ctx.Err() == nil {
    // 1. bootstrap 全量 —— 两个 range 用一个 Txn 读到同一 revision（F3）
    resp,_ := h.etcdCli.Txn(ctx).Then(
        clientv3.OpGet("/topology/tidb/",  clientv3.WithPrefix()),
        clientv3.OpGet("/keyspaces/tidb/", clientv3.WithPrefix()),
    ).Commit()
    baseRev := resp.Header.Revision
    h.applyFull(resp.Responses, baseRev)   // raw、snap=ParseTiDBTopology(raw)、rebuildResp

    // 2. 增量 —— 两个 watch 都 pin 到同一 baseRev+1
    wch1 := h.etcdCli.Watch(ctx, "/topology/tidb/",  clientv3.WithPrefix(), clientv3.WithRev(baseRev+1))
    wch2 := h.etcdCli.Watch(ctx, "/keyspaces/tidb/", clientv3.WithPrefix(), clientv3.WithRev(baseRev+1))
    for {
      select {
      case wr, ok := <-wch1:  if !ok || wr.Canceled { goto rebootstrap } // F15
                              h.applyEvents(wr.Events, wr.Header.Revision)
      case wr, ok := <-wch2:  if !ok || wr.Canceled { goto rebootstrap }
                              h.applyEvents(wr.Events, wr.Header.Revision)
      case <-ctx.Done(): return
      }
    }
    rebootstrap:
  }
}
```

> **F3 —— bootstrap 的 revision 必须一致。** 两次独立 `Get` 返回在不同 revision，
> 从其中一个起 watch 会漏/重事件。单个 `Txn` 读两个 prefix 共享
> `resp.Header.Revision`，watch 从 `baseRev+1` 起。
>
> **F15 —— 必须处理 watch channel 关闭。** 关闭的 channel 读到零值
> （`Canceled=false`），只判 `Canceled` 会热循环。`wr, ok := <-wch; !ok →
> rebootstrap`。仓库先例：`election.watchOwner`。
>
> **F14 —— `rev = max(rev, header.Revision)`**：两个 watch channel 交错，直接赋值
> 可能回退。

### 存活判定 —— 复用现有解析，别重写

`ttl` 缺失即宕机的配对逻辑在 `infosync.ParseTiDBTopology`（PR1 抽出的共享
helper）。增量路径保留 **raw** kv map，每批事件后用同一解析器重derive快照：

```
applyEvents:  F16 短路: 一批事件全是"已存在 key 的 PUT 且(是 ttl key 或值相同)"
              → 只更新 raw,跳过全部（ttl 值是时间戳每 30s 变,稳态 100% 走这条）
              否则: raw2 := clone+apply; newSnap := ParseTiDBTopology(raw2)
              diff 非空才 rebuildRespLocked（F5: 内容不变则 ETag 不变,轮询端持续 304）
```

> **F5/F16 —— 稳态零成本。** TiDB 每 ~30s 重 Put info+ttl；短路 + 内容比对让稳态
> 下既不重解析、也不换 ETag —— 轮询端一直 304。
>
> **F8 —— 快照 copy-on-write。** 新 map + 新指针，绝不原地改；respJSON 字节只读
> 共享。
>
> **ETag = 内容 hash（fnv64a），不是本地计数器。** client 在 hub 副本间 failover
> 时计数器会碰撞产生假 304（实现期单测抓到的 bug）；内容 hash 跨副本语义天然正
> 确，副本间内容一致回 304 反而是合法优化。

> 历史注记：gRPC 推送版的 F4（重连全量）/F9（慢消费者）/F12（注册原子性）/
> F17（full 响应缓存）随传输层退役 —— 轮询模型里这些问题不存在。

## 6. 传输协议 —— HTTP + JSON（`GET /api/topology`）

传输刻意选最朴素的形态：

```
GET /api/topology
  If-None-Match: <etag>        # 可选;匹配则 304 空转
→ 200 OK
  ETag: <内容 hash>
  {"revision": 254674,
   "backends": [{"addr":"10.0.0.1:4000","ip":"10.0.0.1","status_port":10080,
                 "labels":{"zone":"z1"},"keyspace":"ks1"}],
   "prometheus": {"ip":"...","port":9090}}
→ 304 Not Modified             # 内容未变,空 body
→ 503                          # 尚未完成首次 bootstrap
```

- **ETag = 响应内容的 fnv64a hash**,不是本地计数器 —— client 在 hub 副本间
  failover 时,计数器会碰撞产生假 304(实测抓到的 bug);内容 hash 跨副本语义天然
  正确,内容相同回 304 反而是合法优化。
- client 每 3s 轮询,steady state = 一次空 304 往返;sticky 在当前 hub,失败轮换下
  一个地址。fleet 的轮询时钟天然错开(启动时间不同),无惊群。
- JSON 字段名是协议契约,保持稳定;server 未来迁 PD 宿主时 client 不变。
- v2 升级:`GET /api/topology?wait=30s` long polling —— rev 变了立即返回,否则超
  时 304,协议前向兼容。
- 可观测性:`curl hub:3080/api/topology` 随手查拓扑;`tiproxy_discovery_requests_total{code}`
  看轮询健康度。

## 7. 路由注册 —— `pkg/server/api`

复用 api server 的 listener/中间件装配（`newBaseServer` + `start`），精简
discovery profile 的兄弟构造函数：

```go
func NewDiscoveryServer(cfg config.API, lg, cfgMgr, certMgr, hub api.TopologyHandler, ready) (*Server, error) {
    // 与 NewServer 相同的 listener + 中间件，但只注册：
    engine.Group("api").GET("/topology", hub.HandleTopology)
    // + 原有 diagnostics gRPC、/metrics、/debug
}
```

Hub 在 `cfg.API.Addr` 上服务 —— 不开新端口。readiness 探针直接用
`GET /api/topology`（bootstrap 前 503/挡在 readyState，之后 200）或
`/debug/health`。

## 8. Sidecar 侧（代理模式）—— `pkg/discovery/client.go`

`HubClient` 是 `infoSyncer` 的直接替换：实现 `GetTiDBTopology`/`GetPromInfo`/
`Close`，下游（Cluster/observer/router）零改动。

```go
type HubClient struct {
    hubAddrs []string          // 逗号列表
    tlsGetter func() *tls.Config
    pollIntvl time.Duration    // 默认 3s
    httpCli  *http.Client      // DisableKeepAlives: 每次新建连接,规避 hub 重部署后的 DNS 滞后
    mu struct { snap map[...]; prom *...; rev int64; etag string }
}

// pollLoop: 每 3s GET http(s)://<sticky addr>/api/topology,带 If-None-Match
//   304 → 无事;200 → 解码 → COW 全量替换缓存 + 存新 ETag
//   失败 → 轮换下一个地址(sticky-on-success),错误日志限频
```

- 首个快照到达前 `GetTiDBTopology` 返回 `ErrHubNotReady` —— PDFetcher 对错误无限
  重试,语义与"PD 未就绪"一致。
- fleet 的轮询时钟因启动时间不同天然错开,hub 重启无惊群(304 本身近零成本)。
- 3s observer/健康检查循环读本地缓存,轮询间隔与之同量级 —— 端到端感知延迟不变。

### 装配分支 —— `backendcluster.NewCluster`

（与 gRPC 版一致,见原文）按 `clusterCfg.DiscoverySource` 分支:`"hub"` →
`NewHubClient` 且**不建 etcd 客户端**（F10:`PDAddrs` 有默认值,必须显式按 source
分支）;默认走现状 `InfoSyncer` 路径。`Cluster.Close` 对 etcdCli 判 nil。
`clusterReusable` 纳入 `DiscoverySource`/`HubAddrs`,热改触发集群重建。

## 9. 配置 —— `lib/config/proxy.go` `BackendCluster`

字段挂在 `BackendCluster`（`proxy.go:90`）上，粒度是**每个后端集群**：

```go
type BackendCluster struct {
    Name            string   `toml:"name,omitempty" ...`
    PDAddrs         string   `toml:"pd-addrs,omitempty" ...`
    NSServers       []string `toml:"ns-servers,omitempty" ...`
    DiscoverySource string   `toml:"discovery-source,omitempty" ...` // 新：pd(默认)|hub
    HubAddrs        string   `toml:"hub-addrs,omitempty" ...`        // 新：逗号分隔，source=hub 必填
}
```

- **Hub 模式**（`tiproxy discovery`）：复用 `pd-addrs`（连 PD）+ `api.addr`（服务）。
  v1 一个 hub 服务一个 PD 集群。
- **Sidecar**（`tiproxy` 代理）：

```toml
[[proxy.backend-clusters]]
name = "c1"
discovery-source = "hub"
hub-addrs = "tidb-discovery-c1.svc:3080"
# 无 pd-addrs → 该集群零 PD 依赖（F10：代码显式分支，不靠判空）
```

  多集群 mesh：每个条目独立选 `pd`/`hub`，可混用、可逐集群灰度。
- 校验（`ProxyServer.Check`，`proxy.go:297`）：`source=hub` 而 `hub-addrs` 空 →
  报错；`source=hub` 的集群 + VIP 启用 → 报错（VIP 依赖
  `PrimaryCluster().EtcdClient()`，hub 集群无 etcd；mesh 场景无 VIP）。

> **被拒绝的备选：`hub-fallback-pd`**（hub 全挂时 sidecar 直连 PD 兜底）。自相矛
> 盾：兜底要求每个 sidecar 常备 pd-addrs + etcd 客户端代码路径，把"sidecar 零 PD
> 依赖"这个方案卖点又引回来了，且 1000 个 sidecar 同时 fallback 正好在 PD 最脆弱时
> 制造惊群。破窗手段用运维路径替代：把 `discovery-source` 改回 `pd` 滚动重启。

## 10. 失败 / HA

- K≥2 个 hub 副本在一个 Service 后；sidecar 轮询失败时轮换到下一个地址。
- 所有 hub 挂 → sidecar 用最后已知缓存服务（路由照常，只是发现不了新 tidb、也清不
  掉已死条目 —— 死后端由 sidecar 本地健康检查兜住，不至于把流量打到宕机 tidb）。破
  窗手段见 §9：切回 `discovery-source=pd` 滚动重启。
- 轮询天然全量:每次 200 都是完整快照,无续传语义,没有跨 hub 的状态要推敲。
- Hub 地址由 k8s Service DNS / 静态配置下发 —— **不**经 PD 服务注册表 —— sidecar
  100% 不碰 PD。
- **鉴权**：sidecar→hub 的 gRPC 复用 cluster mTLS（`certManager.ClusterTLS`），与
  现有 etcd/http 一致。
- **可观测性**：hub 需暴露 subscriber 数、watch lag、当前 rev 的 metrics，否则
  hub 落后 PD 时无感。

## 11. hub 模式下的 metrics 均衡（实现后修订：比原判轻）

> **PR4 实证修订**：原设计判定"hub 模式 metrics 均衡失效"。实现时发现上游
> #1176 已加"Prometheus 查不到时直接从 backend 读 metrics"的 missing-metrics
> 路径，结论变轻，如下。

背景分量：`balance.policy` 默认 `"resource"`（`lib/config/balance.go:96`），
resource 策略靠 CPU/内存等 metrics 因子选后端 —— hub 模式动的是**默认路径**。

每个 `Cluster` 私有一个 `ClusterReader`（`cluster.go:117`），原本通过 **etcd**
选举 owner 去采集 per-backend 负载并在成员间去重分发。hub 集群**没有 etcd 客户
端**，实际行为（PR4 实现）：

- `election.Start` 对 nil etcd 立即返回（有 nil-guard），永远选不出 owner；
  `queryAllOwners` 对 nil etcd 返回空（PR4 加的守卫，原本会 panic）。
- 无 owner + 无 owner 可转发 → 每个成员都命中 **missing-metrics 路径**
  （`backend_reader.go` "read directly from backends"）→ **每个 sidecar 自己直接
  从全部 backend 的 status port 读 metrics**。
- 结果：**resource 均衡策略在 hub 模式仍然工作**。丢掉的只是选举带来的读取去重
  —— 代价从"1 个 owner 读 T 个 backend"变成"N 个 sidecar 各读 T 个 backend"，
  TiDB status port 承受 N 份采集负载。

**后续优化（可选，不再紧迫）**：让 hub 顺带采集并把 per-backend 负载塞进
`DiscoveryResponse`（已捎 `PrometheusInfo`），sidecar 从流里拿负载 —— 把 N×T 的
直接采集收敛回 K×T。当 N×T 的 status port 采集量成为实际问题时再做。

## 12. Rollout

1. **重构**：抽出 `infosync.ParseTiDBTopology`（零行为变更）。
2. **Proto**：本仓库 `pkg/discovery/pb/` 加 `tidb_discovery.proto` + 生成代码。
3. **Hub**：`pkg/discovery/hub.go` + `NewDiscoveryServer` + `discovery`
   子命令。部署 K 副本；核对快照与 PD 一致。
4. **Client**：`HubClient` + `NewCluster` 分支 + 配置。
5. **Canary**：几个 sidecar 用 `discovery-source=hub`；对比拓扑与 PD 路径。
6. **翻量**：全 fleet 切 `hub`；看 PD etcd read/watch 指标从 ~O(N) 掉到 O(K)。

## 13. 之后

- 健康检查仍留在 sidecar 本地（打 tidb status port，不是 PD 负载）。
- `vip` manager 也用 per-instance etcd 选举 —— 对 sidecar 无关（mesh 无 VIP）；hub
  模式下让它保持关闭。
- ~~Server 迁移进 PD（方案 B / B-lite）~~：HTTP 化后曾评审"把拓扑端点直接做进
  PD API"（B-lite，见 [discovery-hub-pd-blite.md](discovery-hub-pd-blite.md)），
  **决策暂缓** —— PD 故障域隔离、运维弹性、PD 分支维护成本三个理由，保持外置
  hub 为长期形态。线协议是 HTTP+JSON，未来若重启该方向 sidecar 侧零改动。
```
