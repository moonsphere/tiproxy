# TiProxy Discovery Hub 设计（sidecar mesh）

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
把拓扑通过流式 gRPC 推给 sidecar。sidecar 永不碰 PD。PD 的 watch 消费者从 N（1000）
降到 K（hub 副本数，≈3）。

```
        ┌─────────── PD (embedded etcd) ───────────┐
        │  /topology/tidb/*   /keyspaces/tidb/*     │
        └───────▲───────────────────────▲──────────┘
                │ Watch+Get (K 条流)      │
        ┌───────┴───────┐        ┌───────┴───────┐
        │  Hub 副本 1    │  ...   │  Hub 副本 K    │   K≈3, HA
        │  snapshot+rev  │        │  snapshot+rev  │
        └───────▲───────┘        └───────▲───────┘
                │ gRPC 流 (push)          │
   ┌────────────┼─────────────┬───────────┼───────────┐
   │            │             │           │           │
 sidecar-1   sidecar-2  ...  sidecar-i ... sidecar-N   (N≈1000)
 HubClient   HubClient       HubClient     HubClient
 (本地缓存；GetTiDBTopology 读缓存)
```

tidb 拓扑变化 → K 个 etcd watch 事件 → 内存 fan-out 给 N 个 sidecar，PD 零额外负载。

### 为什么方案 A（tiproxy 运行模式）起步

hub 的宿主有两个候选：**A** = tiproxy 二进制的一个运行模式（本仓库）；**B** = PD
的一个微服务（`pd-server services tidb-discovery`）。

- **A 起步**：拓扑解析代码 + proto + HubClient 全在 tiproxy，团队能不等 PD 发版、
  不跨团队评审直接上，快速拿 PD 负载下降数据。
- **B 是长期正解**：PD 已有微服务框架、`services <mode>` 子命令、MetaStorage etcd
  封装、HA，dashboard 也在读 `/topology/*`。
- **关键**：proto 是稳定契约。v1 先放本仓库（免跨仓依赖），但 **proto package 名
  `tidb_discoverypb` 定死不改** —— gRPC 方法全名
  `/tidb_discoverypb.TiDBDiscovery/Subscribe` 由 proto package 决定，与 Go import
  路径无关。未来 A→B 时把 `.proto` 上移 kvproto、重新生成，**线协议零变化**，
  sidecar 只把 `hub-addrs` 重新指向。

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
用于**健康检查**；但 `GetTiDBTopology` 变成读一个由 hub 推送保持新鲜的本地缓存。

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
| gRPC/HTTP listener（`cfg.API.Addr`） | api.Server（完整） | 精简：gRPC(TiDBDiscovery+health)+/metrics+/debug |
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
>（`hub.go` + `client.go`），不拆到 `pkg/balance/` 和 `pkg/manager/` 两处。

```go
type Hub struct {
    etcdCli *clientv3.Client
    lg      *zap.Logger
    mu      sync.RWMutex
    raw     map[string][]byte                    // 所有 /topology/{tidb,keyspaces} 的 kv（info+ttl）
    snap    map[string]*infosync.TiDBTopologyInfo // 派生出的存活快照（info+ttl 配对后）
    prom    *infosync.PrometheusInfo
    rev     int64
    subs    map[int64]*subscriber
    nextID  int64
}
type subscriber struct {
    keyspaces map[string]struct{}                // 空 = 全部
    ch        chan *pb.DiscoveryResponse         // 带缓冲；慢消费者 → 关闭并丢弃
}
```

### watchLoop（唯一碰 PD 的 goroutine）

```go
func (h *Hub) watchLoop(ctx context.Context) {
  for ctx.Err() == nil {
    // 1. bootstrap 全量 —— 两个 range 用一个 Txn 读到同一 revision（见 F3）
    resp,_ := h.etcdCli.Txn(ctx).Then(
        clientv3.OpGet("/topology/tidb/",  clientv3.WithPrefix()),
        clientv3.OpGet("/keyspaces/tidb/", clientv3.WithPrefix()),
    ).Commit()
    baseRev := resp.Header.Revision                    // 单一、一致的 revision
    h.applyFull(resp.Responses, baseRev)               // raw=…、snap=ParseTiDBTopology(raw)、rev=baseRev
    h.broadcastFull()

    // 2. 增量 —— 两个 watch 都 pin 到同一 baseRev+1
    wch := h.etcdCli.Watch(ctx, "/topology/tidb/",  clientv3.WithPrefix(), clientv3.WithRev(baseRev+1))
    wch2:= h.etcdCli.Watch(ctx, "/keyspaces/tidb/", clientv3.WithPrefix(), clientv3.WithRev(baseRev+1))
    for {
      select {
      case wr, ok := <-wch:  if !ok || wr.Canceled { goto rebootstrap } // F15：关闭/取消都要重建
                             h.applyEvents(wr.Events, wr.Header.Revision)
      case wr, ok := <-wch2: if !ok || wr.Canceled { goto rebootstrap }
                             h.applyEvents(wr.Events, wr.Header.Revision)
      case <-ctx.Done(): return
      }
    }
    rebootstrap:
  }
}
```

> **F15 —— 必须处理 watch channel 关闭，只查 `Canceled` 会热循环。** clientv3 的
> watch channel 在客户端关闭/ctx 结束等情况下会**直接 close**；从已关闭 channel
> 读到的是零值 `WatchResponse`（`Canceled=false`、无事件）—— 只判 `Canceled` 的
> select 会以零值空转成 CPU 热循环。必须 `wr, ok := <-wch; if !ok { rebootstrap }`。
> 仓库先例：`election.watchOwner`（`election.go:271`）就是这么处理的。

> 两个 watch channel 的事件交错到达，revision 可能非单调（ch1 送来 rev105 后 ch2
> 送来 rev103）。两个 prefix 的 key 不相交，应用顺序无所谓；但
> `h.rev = max(h.rev, header.Revision)`，别直接赋值，避免 rev 回退。

> **F3 —— bootstrap 的 revision 必须一致。** 两次独立 `Get` 返回在两个不同的
> revision 上，从其中一个起 watch 会静默漏掉或重放间隙里的事件。用一个 `Txn` 把两
> 个 prefix 读进来共享 `resp.Header.Revision`，再从 `baseRev+1` 起 watch。（现有
> `GetTiDBTopology` 是两次独立 Get —— 对自愈轮询无害，建 watch 基线不行。）

### 存活判定的坑 —— 复用现有解析，别重写

`InfoSyncer.GetTiDBTopology`（`info.go:257`）把 `…/info` 与 `…/ttl` 配对，`ttl`
缺失的 backend（宕机）被丢弃。增量路径上一个 tidb 宕机表现为它的 `ttl` key 的
**DELETE 事件**（lease 过期，`info` 可能一起被删）。与其在事件路径重写这套逻辑，不
如保留 **raw** kv map，每批事件后用**同一个解析器**重新派生存活快照：

```
applyEvents:  用事件更新 h.raw（PUT 置入 / DELETE 移除）
              newSnap := infosync.ParseTiDBTopology(h.raw)   // 从 GetTiDBTopology 抽出的共享 helper
              delta   := diff(h.snap, newSnap)               // upserted + removed addrs
              h.snap = newSnap; h.rev = header.Revision       // 新 map + 新指针（F8）
              if delta 非空 { h.broadcastDelta(delta, h.rev) }   // 抑制无变化抖动（F5）
```

> **F5 —— 在派生快照上做 diff，绝不转发 raw 事件。** 每个 TiDB 周期性重 `Put`
> `info` 与 `ttl`（tiproxy 对称写入器每 30s 两个都写，`syncTopology`，
> `info.go:210`）。拓扑不变时 watch 也持续吐 PUT。若转发 raw 事件，会每 ~30s 给全
> 部 N 个 sidecar 推一次无变化更新。派生存活快照、只广播非空 `diff` 把抖动收敛到
> 零。这是正确性，不是优化。
>
> **F16 —— 纯 ttl 刷新要短路，别每批事件全量重解析。** `ttl` 的值是
> `time.Now().UnixNano()`（`info.go:240`）—— **每次刷新都变**，watch 稳定期持续吐
> PUT。而存活判定只看 ttl key 的**存在性**，不看值。若每批事件都
> `ParseTiDBTopology(全量 raw)`，T 个 tidb = 每 30s 内 T 次全量 JSON 反序列化循环，
> T=1000 时纯浪费的 CPU。短路规则：一批事件若全是"已存在 key 的 PUT 且（是 ttl
> key 或 value 与 raw 中相同）"→ 只更新 raw，跳过重解析（快照不可能变）。出现
> DELETE / 新 key PUT / info 值变化才走全量重解析 + diff。稳态开销归零，重解析只在
> 拓扑真变时发生（罕见），解析器仍是同一个。
>
> **F8 —— 快照是 copy-on-write。** `applyEvents` 构造**新** map + **新**
> `*TiDBTopologyInfo` 指针，绝不原地改旧值。`GetTiDBTopology` 交出去的是
> `maps.Clone(snap)`（浅拷 —— 共享指针）；COW 让共享指针可并发安全读。
> `HubClient.apply` 同样纪律。

### Subscribe（gRPC handler）

```go
func (h *Hub) Subscribe(req *pb.SubscribeRequest, stream pb.TiDBDiscovery_SubscribeServer) error {
    // register 与 full 快照的捕获必须在同一次持锁内完成（F12）
    sub, fullResp := h.register(req.Keyspaces)
    defer h.deregister(sub)
    // 每次（重）连都发当前快照 full=true（F4）。req.KnownRevision 接受但暂时忽略。
    if err := stream.Send(fullResp); err != nil { return err }
    for {
        select {
        case resp, ok := <-sub.ch:                       // 溢出时被 broadcast 关闭（F9）
            if !ok { return status.Error(codes.ResourceExhausted, "slow consumer") }
            if err := stream.Send(resp); err != nil { return err }
        case <-stream.Context().Done(): return nil
        }
    }
}

// broadcast 绝不阻塞 watchLoop：非阻塞发送，丢弃慢订阅者（F9）
func (h *Hub) broadcast(resp *pb.DiscoveryResponse) {
    for id, sub := range h.subs {
        select {
        case sub.ch <- resp:
        default:                                          // 缓冲满 → 强制重连
            close(sub.ch); delete(h.subs, id)             // sidecar 重新 Subscribe → 拿全量
        }
    }
}
```

> **F4 —— 重连不做 delta 重放。** hub 不保留 per-client 历史，无法从任意
> `known_revision` 重建 delta。每次（重）连都拿 **full** 快照（几 KB～几十 KB，
> 就算 1000 个 sidecar 一起重连也便宜）。`known_revision` 字段保留在 proto 里，留
> 给未来带界限的事件日志优化，初版忽略。这也消掉了跨 hub 副本的 revision 一致性负
> 担。
>
> **F9 —— broadcast 不能阻塞 `watchLoop`。** 向每个订阅者的带缓冲 channel 非阻塞
> 发送；溢出就关闭并丢弃。被丢的 sidecar 的流报错、重连、拿到新的全量快照。
>
> **F12 —— full 快照必须与注册原子捕获。** 若 `register`（入 `h.subs`）和
> `fullResponse`（读 `h.snap`）分两次拿锁，中间广播进 `sub.ch` 的 delta 会**旧于**
> 后拿到的 full —— handler 先发 full 再回放旧 delta，客户端把过期变更盖到新快照上。
> `register` 在同一次持锁内完成"入订阅表 + 生成 full 响应"，之后进入 `sub.ch` 的一
> 定严格新于 full，顺序天然正确。
>
> **F17 —— full 响应按快照版本缓存一份，重连风暴时 O(1)。** v1 无 keyspace 过滤，
> full 响应对所有订阅者相同 —— 快照更新时构建一次 `*pb.DiscoveryResponse` 存在
> `Hub` 上，`register` 直接复用指针（proto 消息并发只读/序列化是安全的，配合 F8 的
> COW 不会被改）。否则 hub 重启后 1000 个 sidecar 同时重连，每个 register 都持锁做
> O(T) 转换 = 10 万次转换 + 锁竞争尖峰。
>
> **keyspace 过滤 v1 不实现。** `broadcast` 对所有订阅者发同一条响应，不做 per-sub
> 过滤；`SubscribeRequest.keyspaces` 字段保留在 proto，hub 先忽略（发全量），需要时
> 再加过滤，客户端无需变更。

Prometheus 信息：一个小 `promLoop`（复用 `GetPromInfo` 或 watch
`/topology/prometheus`）更新 `h.prom`，捎带在下一条响应里。

## 6. 传输协议 —— 放本仓库（`pkg/discovery/pb/tidb_discovery.proto`）

v1 先放 tiproxy 仓库：免跨仓依赖、免 `replace` 指令、免 kvproto 上游评审，独立
迭代。**约束一条：`package tidb_discoverypb;` 定死不改** —— gRPC 方法全名由 proto
package 决定（`/tidb_discoverypb.TiDBDiscovery/Subscribe`），与 Go import 路径无
关。未来迁 PD（方案 B）时把 `.proto` 原样上移 kvproto（它已有
`meta_storagepb/`、`tsopb/` 先例）、各自重新生成，线协议零变化。

```proto
syntax = "proto3";
package tidb_discoverypb;   // 定死：决定 gRPC 方法全名，上移 kvproto 时不变
option go_package = "github.com/pingcap/tiproxy/pkg/discovery/pb";

service TiDBDiscovery {
  rpc Subscribe(SubscribeRequest) returns (stream DiscoveryResponse);
}
message SubscribeRequest {
  repeated string keyspaces = 1;   // 空 = 全部
  int64  known_revision   = 2;     // 续传提示；0 = 需要全量
  string client_id        = 3;
}
message DiscoveryResponse {
  int64  revision            = 1;
  bool   full                = 2;  // true：替换缓存；false：应用 delta
  repeated TiDBInstance upserted = 3;
  repeated string removed_addrs  = 4;
  PrometheusInfo prometheus      = 5;
}
message TiDBInstance {
  string addr = 1; string ip = 2; uint32 status_port = 3;
  map<string,string> labels = 4; string keyspace = 5; string version = 6;
}
message PrometheusInfo { string ip = 1; int32 port = 2; string binary_path = 3; }
```

> **字段审计（已验证）**：下游只消费 `Labels/IP/StatusPort`
>（`PDFetcher.GetBackendList`）+ `IP/StatusPort`（`backend_reader.go:556`）。
> `GitHash/DeployPath/StartTimestamp` 无人使用，可丢弃。`Keyspace/Version` 保留做前
> 向兼容。

生成的 `.pb.go` 提交进仓库（tiproxy 无 proto 工具链，加一个 `make gen-proto`
target，见实现文档 PR2）。

## 7. gRPC 注册接口 —— `pkg/server/api`

API server 已把 gRPC+HTTP 复用在一个 listener 上并注册了 `diagnosticspb`
（`server.go:188`）。加一个用于精简 discovery profile 的兄弟构造函数：

```go
func NewDiscoveryServer(cfg config.API, lg *zap.Logger, hub *discovery.Hub, ready *atomic.Bool) (*Server, error) {
    // 与 NewServer 相同的 listener + h2c mux，但只注册：
    tidb_discoverypb.RegisterTiDBDiscoveryServer(h.grpc, hub)
    grpc_health_v1.RegisterHealthServer(h.grpc, healthSrv)   // 供 k8s readiness
    diagnosticspb.RegisterDiagnosticsServer(h.grpc, ...)
    // gin：只留 /metrics、/debug/pprof
}
```

Hub 在 `cfg.API.Addr` 上服务 —— 不开新端口。

## 8. Sidecar 侧（代理模式）—— `pkg/discovery/client.go`

`HubClient` 是 `infoSyncer` 的直接替换：实现消费方用到的那两个方法 + `Close`。

```go
type HubClient struct {
    hubAddrs []string
    tls      *tls.Config
    lg       *zap.Logger
    mu       sync.RWMutex
    snap     map[string]*infosync.TiDBTopologyInfo
    prom     *infosync.PrometheusInfo
    rev      int64
    readyCh  chan struct{}
}

func (c *HubClient) Start(ctx) { go c.streamLoop(ctx) }

func (c *HubClient) streamLoop(ctx) { // dial→Subscribe→apply→重连(backoff, 下一个 addr)
  for ctx.Err()==nil {
    conn,_ := grpc.DialContext(ctx, pick(c.hubAddrs), creds)
    stream,_ := pb.NewTiDBDiscoveryClient(conn).Subscribe(ctx, &pb.SubscribeRequest{KnownRevision: c.rev})
    for {
      resp, err := stream.Recv(); if err != nil { break } // → backoff、重连
      c.apply(resp)   // full → 替换；delta → upsert/remove；置 rev；首次关闭 readyCh
    }
  }
}

// infoSyncer 的直接替换：
func (c *HubClient) GetTiDBTopology(ctx) (map[string]*infosync.TiDBTopologyInfo, error) {
    c.mu.RLock(); defer c.mu.RUnlock()
    if c.snap == nil { return nil, errHubNotReady }  // PDFetcher 无限重试 —— 与 PD 未就绪同理
    return maps.Clone(c.snap), nil
}
func (c *HubClient) GetPromInfo(ctx) (*infosync.PrometheusInfo, error) { ... }
func (c *HubClient) Close() error { ... }
```

3s observer 循环现在读这个本地缓存 —— 便宜；拓扑新鲜度来自 push 流。

### 装配分支 —— `backendcluster.NewCluster`

分支点在 `NewCluster`（`cluster.go:74`），server.go 不动：

```go
// Cluster 内部：infoSyncer *infosync.InfoSyncer 字段改为 topo topoSource
func NewCluster(ctx, cfg, clusterCfg, clusterTLS, logger, cfgGetter, metricsQuerier) (*Cluster, error) {
    ...
    var topo topoSource
    var etcdCli *clientv3.Client
    switch clusterCfg.DiscoverySource {
    case "hub":
        // F10：不建 etcd 客户端。etcdCli 留 nil。
        hub := discovery.NewHubClient(clusterCfg.HubAddrs, clusterTLS, logger.Named("hubcli"))
        hub.Start(ctx)
        topo = hub
    default: // "" / "pd"，现状路径原样
        etcdCli, err = etcd.InitEtcdClientWithAddrsAndDialer(..., clusterCfg.PDAddrs, ...)
        is := infosync.NewInfoSyncer(...); is.Init(ctx, cfg)
        topo = is
    }
    cluster := &Cluster{cfg: clusterCfg, etcdCli: etcdCli, topo: topo, ...}
    // ClusterReader 照旧创建；etcdCli 为 nil 时其内部选举 no-op（election.go 有
    // nil-guard），见 §11 限制
    ...
}
```

- `Cluster.GetTiDBTopology`/`GetPromInfo` 改调 `c.topo`；`Close` 里 `c.etcdCli`
  加 nil 判。
- **静态后端不需要 source**：不配 `backend-clusters`/`pd-addrs` 时
  `GetBackendClusters()` 返回空、Manager 无集群，`FallbackFetcher`（nsMgr
  `manager.go:59-61`）自动退到 namespace 的静态 instance 列表 —— 旧设计里的
  `discovery-source=static` 档位**删除**，上游已原生覆盖。

> **F10（修订）—— hub 集群条目必须显式绕过 etcd，不能靠判空 addr。** 旧版结论不
> 变、位置变了：legacy 单集群路径 `GetBackendClusters()` 用 `Proxy.PDAddrs` 合成
> 默认集群（`proxy.go:284`），而 `NewConfig()` 给它填默认值 `"127.0.0.1:2379"` ——
> 不显式按 `DiscoverySource` 分支的话，hub 模式会拿默认地址去连不存在的本地 PD。
> `NewCluster` 的 switch 按 source 分支、hub 分支无条件不建 etcd 客户端。

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

- K≥2 个 hub 副本在一个 Service 后；sidecar 故障时重连另一个 hub。
- 所有 hub 挂 → sidecar 用最后已知缓存服务（路由照常，只是发现不了新 tidb、也清不
  掉已死条目 —— 死后端由 sidecar 本地健康检查兜住，不至于把流量打到宕机 tidb）。破
  窗手段见 §9：切回 `discovery-source=pd` 滚动重启。
- 每次（重）连 → hub 发 `full=true`；sidecar 替换缓存（F4）。无 delta 续传，没有跨
  hub 的 revision 偏移要推敲。
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
- Server 可迁移成 PD 微服务（方案 B）：届时把 `.proto` 原样上移 kvproto（proto
  package 名不变 → 线协议不变），PD/tiproxy 各自重新生成。sidecar 二进制不用动，
  只把 `hub-addrs` 重新指向。
```
