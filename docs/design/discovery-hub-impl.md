# Discovery Hub 实现文档

设计见 [discovery-hub.md](discovery-hub.md)。本文档是施工图：按 PR 拆分，每个 PR
可独立合入、独立回滚。实现时按顺序走，每个 PR 内按小节顺序写代码。

> **施工完成记录（2026-07-06）**：PR1-PR10 全部合入 `feat/discovery-hub`。
> PR1-5 按下文计划执行；实际增补：
>
> | PR | 内容 |
> |---|---|
> | PR6 | 全功能 review 修复（discovery 模式拒绝 hub-source 集群、Close 错误不再被吞、空地址守卫、source 归一化） |
> | PR7 | 打包：镜像内置 `conf/hub.toml` 模板；实测 `make docker` + 容器连真 PD |
> | PR8 | E2E P0 六场景（compose 真集群,SQL 指纹裁决） |
> | PR9 | E2E P1（S7 hub 重启 full 替换、S8 pd↔hub 热迁移） |
> | **PR10** | **传输层替换**：gRPC 流 → HTTP 轮询 + ETag（内容 hash）。**下文 PR2
> （proto）与 PR3/PR4 中的 gRPC/订阅者部分已退役**,保留作施工历史;现行实现见设计
> 文档 §5/§6/§8 与 `pkg/discovery/{hub,client,types}.go` |
>
> E2E 环境注记：宿主机磁盘 >90% 会触发 TiKV low-space 保护导致 TiDB bootstrap
> FATAL —— compose 已给 TiKV 声明 `--capacity=10GB` 规避。

约定（全仓库通用）：
- 错误处理用 `lib/util/errors`（`errors.WithStack` / `errors.Wrapf`），重试用
  `lib/util/retry`，goroutine 用 `pkg/util/waitgroup`。
- 测试用 `pkg/testkit` helpers + `require`；涉及 etcd 的测试用
  `etcd.CreateEtcdServer`（embedded etcd，`pkg/util/etcd/etcd.go:77`，infosync/vip/
  elect 的测试都这么干）。
- 每个 PR 收尾跑 `make lint` + 相关包 `go test`。

---

## PR1 —— 重构：抽出 `ParseTiDBTopology`（零行为变更）

**目标**：把 `InfoSyncer.GetTiDBTopology`（`pkg/manager/infosync/info.go:257`）里
"解析 etcd kv → 存活拓扑"的纯逻辑抽成包级函数，供后续 hub 的 watch 路径复用。

### 改动

`pkg/manager/infosync/info.go`：

```go
// ParseTiDBTopology derives the live TiDB topology from raw etcd kvs.
// kvs: full key (含 /topology/tidb/ 或 /keyspaces/tidb/ 前缀) → value。
// 逻辑与原 GetTiDBTopology 循环体逐行一致：keyspace 剥离、info 反序列化、
// ttl 配对、ttl 缺失即丢弃。
func ParseTiDBTopology(lg *zap.Logger, kvs map[string][]byte) map[string]*TiDBTopologyInfo
```

- 把 `GetTiDBTopology` 第 267-310 行的循环体和交叉过滤原样搬进去；输入从
  `[]*mvccpb.KeyValue` 改为 `map[string][]byte`。
- **保留 `topology.Addr = addr` 的填充**（上游 #1150 新增，`info.go:299`）——
  backendcluster 合并层靠它生成 backendID。
- **迭代必须按 key 排序**（`slices.Sorted(maps.Keys(kvs))`）。原代码按 slice 顺序
  遍历（先无 keyspace 后有 keyspace，确定性）；改成 map 后裸 range 顺序随机 —— 同
  一 addr 在两个前缀下都有 info 时（`infos[addr]` 覆盖写）结果会随机翻转，hub 的
  diff 会因此产生虚假抖动广播。排序遍历 = 确定性，也让该边界 case 行为可测。
- `GetTiDBTopology` 改为：两次 `etcdCli.Get` → 把 `resp.Kvs` 拍平成
  `map[string][]byte` → 调 `ParseTiDBTopology`。
- 反序列化失败只 log 不中断（保持现状）。

### 测试

- 现有 `TestFetchTiDBTopology`、`TestGetTopology`（`info_test.go`）必须不改而过 ——
  这是"零行为变更"的验收。
- 新增 `TestParseTiDBTopology`：直接喂 kv map，覆盖:
  1. info+ttl 齐全 → 保留;
  2. 只有 info 无 ttl → 丢弃;
  3. 只有 ttl 无 info → 不出现;
  4. keyspace 路径（`/keyspaces/tidb/ks1/topology/tidb/...`）→ `Keyspace` 字段正确;
  5. 坏 JSON → 跳过该条、其余正常;
  6. keyspace 路径畸形（无第二段斜杠）→ 跳过;
  7. **确定性**：同一 addr 同时出现在两个前缀 → 多次调用结果一致（排序遍历）。

### 验收

`cd pkg/manager/infosync && go test ./...` 全绿；`make lint` 干净。

---

## PR2 —— 本仓库 proto：`pkg/discovery/pb/`

**目标**：把线协议定为稳定契约。v1 放本仓库（免跨仓依赖、免 replace、免上游评
审）；未来迁 PD（方案 B）时把 `.proto` 原样上移 kvproto。

**硬约束**：`package tidb_discoverypb;` **定死不改** —— gRPC 方法全名
`/tidb_discoverypb.TiDBDiscovery/Subscribe` 由 proto package 决定，与 Go import
路径无关。上移 kvproto 时只有 `option go_package` 和各方 import 变，线协议零变化。

### 改动

- 新增 `pkg/discovery/pb/tidb_discovery.proto`，内容照抄设计文档 §6
  （`Subscribe` 流 + `SubscribeRequest`/`DiscoveryResponse`/`TiDBInstance`/
  `PrometheusInfo`），`option go_package = "github.com/pingcap/tiproxy/pkg/discovery/pb"`。
- 生成代码提交进仓库（`tidb_discovery.pb.go` + `tidb_discovery_grpc.pb.go`）。
- `Makefile` 加 target（tiproxy 目前无 proto 工具链）：

```make
gen-proto:
	protoc --go_out=. --go_opt=paths=source_relative \
	       --go-grpc_out=. --go-grpc_opt=paths=source_relative \
	       pkg/discovery/pb/tidb_discovery.proto
```

  文件头注释注明 protoc / protoc-gen-go / protoc-gen-go-grpc 版本，保证再生成可复
  现。工具不进 go.mod（生成结果已提交，CI 不需要 protoc）。

### 验收

`go build ./...` 通过；能 import `pkg/discovery/pb` 并引用
`RegisterTiDBDiscoveryServer`；重跑 `make gen-proto` 无 diff。

---

## PR3 —— Hub server + `tiproxy discovery` 子命令

**目标**：可部署的 hub。依赖 PR1、PR2。

### 3.1 `pkg/discovery/hub.go`

```go
type Hub struct {
    etcdCli *clientv3.Client
    lg      *zap.Logger
    wg      waitgroup.WaitGroup
    cancel  context.CancelFunc

    mu     sync.RWMutex
    raw    map[string][]byte                     // watchLoop 单写者；锁内替换
    snap   map[string]*infosync.TiDBTopologyInfo // COW：每次更新换新 map+新指针
    prom   *infosync.PrometheusInfo
    rev    int64
    subs   map[int64]*subscriber
    nextID int64
}

type subscriber struct {
    id int64
    ch chan *pb.DiscoveryResponse // cap=16；pb = "github.com/pingcap/tiproxy/pkg/discovery/pb"
}

func NewHub(lg *zap.Logger, etcdCli *clientv3.Client) *Hub
func (h *Hub) Run(ctx context.Context)   // wg.RunWithRecover(watchLoop) + wg.Run(promLoop)
func (h *Hub) Close() error              // cancel + wg.Wait
```

**并发模型**：`watchLoop` 是 `raw`/`snap`/`rev` 的唯一写者；`mu` 只用来保护
读者（Subscribe 注册、fullResponse）与写者的交界。`subs` 的增删和 broadcast 都在
`mu` 内。

**watchLoop**（设计 §5，注意 F3/F5/F8/F14）：

```
for ctx 未取消:
  1. Txn(OpGet /topology/tidb/ prefix, OpGet /keyspaces/tidb/ prefix).Commit()
     失败 → retry.Retry（RetryInterval 复用 config.HealthCheck 默认 3s，无限次）
  2. baseRev = resp.Header.Revision
     raw = 拍平两个 RangeResponse
     snap = infosync.ParseTiDBTopology(lg, raw)     // PR1 的共享 helper
     rev = baseRev
     锁内替换 + broadcast full 给所有现存 subs
  3. wch1 = Watch(/topology/tidb/, WithPrefix, WithRev(baseRev+1))
     wch2 = Watch(/keyspaces/tidb/, WithPrefix, WithRev(baseRev+1))
  4. select 两个 channel（必须用 `wr, ok := <-wch` 形式）:
     - !ok 或 wr.Canceled → 跳回 1（全量重建）
       // F15: channel 关闭读到的是零值（Canceled=false），只判 Canceled 会热循环;
       // 先例: election.watchOwner (election.go:271)
     - 事件 → applyEvents:
         // F16 短路: 一批事件若全是"已存在 key 的 PUT 且(是 ttl key 或
         // bytes.Equal(旧值,新值))" → 只更新 raw、rev,跳过下面全部
         //（ttl 值是时间戳每 30s 变一次,稳态事件 100% 走这条,重解析开销归零）
         raw2 := maps.Clone(raw); PUT 置入 / DELETE 删除
         newSnap := infosync.ParseTiDBTopology(lg, raw2)
         delta := diffSnap(snap, newSnap)          // 见下
         锁内: raw=raw2; snap=newSnap; rev=max(rev, wr.Header.Revision)   // F14
               fullResp 重建缓存                                           // F17
         delta 非空才 broadcast（F5）
```

注意 raw 的更新：F16 短路路径只改值不改快照,可以原地写 `raw[k]=v`（watchLoop 单写
者,读者不碰 raw）；走重解析的路径才 `maps.Clone`。

```go
// diffSnap: 比较新旧 snap。
// upserted: 新增的 addr + Equals 不等的 addr（复用 BackendInfo 同款字段比较：
//           IP/StatusPort/Labels/Keyspace/Version）
// removed:  旧有新无的 addr
func diffSnap(old, new map[string]*infosync.TiDBTopologyInfo) (upserted []*pb.TiDBInstance, removed []string)
```

**broadcast**（F9，锁内调用）：

```go
func (h *Hub) broadcast(resp *pb.DiscoveryResponse) {
    for id, sub := range h.subs {
        select {
        case sub.ch <- resp:
        default:
            close(sub.ch)
            delete(h.subs, id)
            metrics.DiscoverySubDropped.Inc()
        }
    }
}
```

**Subscribe**（F4/F12/F17）：

```go
func (h *Hub) Subscribe(req *pb.SubscribeRequest, stream pb.TiDBDiscovery_SubscribeServer) error {
    sub, fullResp := h.register()          // 一次持锁：入 subs + 取缓存的 fullResp（F17，
                                           // 快照更新时预构建，register 不做 O(T) 转换）
    defer h.deregister(sub)                // 幂等：只 delete(h.subs, id)，不 close(ch)
                                           //（close 只属于 broadcast 的慢消费者路径，防双 close panic）
    if err := stream.Send(fullResp); err != nil { return err }
    for {
        select {
        case resp, ok := <-sub.ch:
            if !ok { return status.Error(codes.ResourceExhausted, "slow consumer") }
            if err := stream.Send(resp); err != nil { return err }
        case <-stream.Context().Done():
            return nil
        }
    }
}
```

- `req.Keyspaces`、`req.KnownRevision` 接受但忽略（v1 语义，见设计 F4/keyspace 说明）。
- `req.ClientId` 打进日志。

**promLoop**：每 30s 调一次现有 `GetPromInfo` 逻辑（把 `InfoSyncer.GetPromInfo`
的 etcd 读取部分抽成包级 `FetchPromInfo(ctx, etcdCli, cfg)` 或直接内联同样三行读
取），变化时更新 `h.prom` 并 broadcast 一条只带 `prometheus` 字段的 delta。

**类型转换**：`TiDBTopologyInfo ↔ pb.TiDBInstance` 的两个转换函数放
`pkg/discovery/convert.go`，双向都写、双向都测（addr 是 map key，proto 里显式带）。

### 3.2 metrics —— `pkg/metrics/discovery.go`

| 指标 | 类型 | 含义 |
|---|---|---|
| `tiproxy_discovery_subscribers` | Gauge | 当前订阅者数 |
| `tiproxy_discovery_sub_dropped_total` | Counter | 慢消费者被丢次数 |
| `tiproxy_discovery_revision` | Gauge | 当前 rev（对比多副本/PD 落后） |
| `tiproxy_discovery_backends` | Gauge | snap 内存活 tidb 数 |
| `tiproxy_discovery_rebootstrap_total` | Counter | watch 重建次数 |
| `tiproxy_discovery_broadcast_total` | Counter{type=full\|delta} | 广播次数 |

注册进现有 `metrics.MetricsManager` 的 registry（照 `pkg/metrics/` 其他文件抄）。

### 3.3 `pkg/server/api/discovery.go` —— 精简 API server

在 `api` 包内加兄弟构造函数（复用 `Server` 结构与中间件链）：

```go
func NewDiscoveryServer(cfg config.API, lg *zap.Logger, cfgMgr ConfigManager,
    certMgr *mgrcrt.CertManager, hub pb.TiDBDiscoveryServer,
    ready *atomic.Bool) (*Server, error)
```

与 `NewServer`（`server.go:74`）的差异：
- gRPC 注册：`RegisterTiDBDiscoveryServer(h.grpc, hub)` +
  `grpc_health_v1.RegisterHealthServer` + 原有 diagnostics；
- gin 只挂 `registerMetrics` + `registerDebug`，**不挂** `registerAPI`（无
  namespace/config/backend/traffic 路由）；
- 其余（listener、h2c、TLS、限流、readyState、grpcServer 中间件）逐行同
  `NewServer` —— 把 74-116 行的公共装配抽成私有 `newBaseServer(cfg, lg, certMgr)`,
  两个构造函数共用，避免复制漂移。

**ready 语义**：`readyState` 中间件挡所有请求（含走 gin 的 gRPC）直到
`ready.Toggle()`。hub 模式在**首次 bootstrap 成功后**才 toggle —— k8s readiness 自
然把 sidecar 挡在空 hub 外面；PD 不可达时 hub 永不 ready，sidecar 继续用缓存/换副本。

### 3.4 `pkg/server/discovery_server.go` + `initBase`

- 把 `NewServer` 73-119 行（configManager.Init → logger → printInfo → metrics →
  memManager → certManager.Init → etcd.InitEtcdClient → httpCli）抽成
  `func (srv *Server) initBase(ctx, sctx) (cfg *config.Config, lg *zap.Logger, err error)`，
  `NewServer` 原地改调它（行为不变）。
- 新增：

```go
func NewDiscoveryServer(ctx context.Context, sctx *sctx.Context) (*Server, error) {
    srv := &Server{ configManager: ..., metricsManager: ..., certManager: ... }
    cfg, lg, err := srv.initBase(ctx, sctx)
    // 此模式 PDAddrs 必填：etcdCli == nil 直接报错退出
    srv.hub = discovery.NewHub(lg.Named("hub"), srv.etcdCli)
    srv.hub.Run(ctx)
    ready := atomic.NewBool(false)
    srv.apiServer, err = api.NewDiscoveryServer(cfg.API, lg.Named("api"),
        srv.configManager, srv.certManager, srv.hub, ready)
    // hub 首次 bootstrap 完成后 ready.Toggle()：Hub.Run 接受一个 onReady func()
    return srv, err
}
```

- `Server` 结构体加字段 `hub *discovery.Hub`；`Close()` 里加
  `if s.hub != nil { errs = append(errs, s.hub.Close()) }`（其余 nil-guard 已备）。

### 3.5 `cmd/tiproxy/main.go`

照设计 §4 加 `discovery` 子命令（`rootCmd.AddCommand`），复用全局 flag。

### 测试

`pkg/discovery/hub_test.go`，用 `etcd.CreateEtcdServer` 起 embedded etcd，测试自己
往 `/topology/tidb/...` 写 kv 模拟 tidb：

1. **bootstrap**：预写 2 个 tidb（info+ttl）→ Subscribe → 收到 full=true、2 条。
2. **新增**：再写 1 个 tidb → 收到 delta upserted=1。
3. **宕机**：删某 tidb 的 ttl key → 收到 delta removed=1。
4. **F5/F16 抑制**：重 Put 相同 info + 新时间戳 ttl（模拟 30s 刷新）→ **不**收到任
   何消息（带超时的 recv 断言无消息），且 `broadcast_total` 不增；把重解析计数暴露
   为测试钩子或指标，断言短路路径没触发解析。
5. **F12 顺序**：并发 Subscribe 与写入,断言每个客户端 full 之后的 delta 序列应用后
   与最终 etcd 状态一致。
6. **慢消费者**：起一个不读流的订阅者，狂写 20+ 变更 → 该订阅者被断流
   （收到 ResourceExhausted），其他订阅者不受影响。
7. **rebootstrap**：compact etcd 到最新 rev、制造 watch cancel → hub 重建后新订阅
   者拿到正确 full。
8. **F15 channel 关闭**：直接 Close 底层 etcd client（或断 embedded etcd）→ watch
   channel 关闭 → hub 走 rebootstrap 路径而非热循环（断言 CPU 不飙 / rebootstrap
   计数 +1，且恢复 etcd 后能继续服务）。
9. **F17 full 缓存**：连续 50 个 Subscribe → 收到的 fullResp 指针相同（或断言快照
   未变时无重复转换）；快照变更后新订阅拿到新 full。
10. **convert 双向**：`TiDBTopologyInfo → TiDBInstance → TiDBTopologyInfo` 往返相等。

`pkg/server/` 侧：`TestDiscoveryServerStartClose`（照 `server_test.go` 模式，起一个
embedded etcd + NewDiscoveryServer，确认 Close 干净、无 goroutine 泄漏）。

### 验收

- `tiproxy discovery --config xx.toml` 起得来，`/metrics` 有 discovery 指标，
  `grpc_health` 在 bootstrap 前 NOT_SERVING、后 SERVING。
- 全部新测试 + `pkg/server` 既有测试绿；`make lint` 干净。

---

## PR4 —— Sidecar 侧：`HubClient` + 装配分支 + 配置

**目标**：`discovery-source=hub` 端到端可用。依赖 PR2、PR3（联调）；代码上只依赖 PR2。

### 4.1 配置 —— `lib/config/proxy.go`

- **`BackendCluster`**（`proxy.go:90`）加两个字段（设计 §9）：
  `DiscoverySource string`、`HubAddrs string`。粒度是每个后端集群，多集群 mesh 可
  逐集群灰度。
- `ProxyServer.Check()`（`proxy.go:297`）加校验：
  - 任一集群 `DiscoverySource` ∉ {"", "pd", "hub"} → 报错;
  - `=="hub"` 且 `HubAddrs == ""` → 报错;
  - 存在 `hub` 集群且 VIP 启用 → 报错（VIP 依赖 `PrimaryCluster().EtcdClient()`，
    hub 集群无 etcd）。
- 旧设计的 `static` 档位**不做**：不配集群时 `GetBackendClusters()` 为空、nsMgr 的
  `FallbackFetcher`（`namespace/manager.go:59-61`）自动退到静态 instance 列表，上
  游已原生覆盖。

### 4.2 `pkg/discovery/client.go`

```go
type HubClient struct {
    hubAddrs []string           // strings.Split(cfg.HubAddrs, ",")
    tlsGetter func() *tls.Config // certManager.ClusterTLS，热更新
    lg       *zap.Logger
    wg       waitgroup.WaitGroup
    cancel   context.CancelFunc

    mu   sync.RWMutex
    snap map[string]*infosync.TiDBTopologyInfo
    prom *infosync.PrometheusInfo
    rev  int64
}

func NewHubClient(hubAddrs string, tlsGetter func() *tls.Config, lg *zap.Logger) *HubClient
func (c *HubClient) Start(ctx context.Context)          // wg.RunWithRecover(streamLoop)
func (c *HubClient) GetTiDBTopology(ctx) (map[string]*infosync.TiDBTopologyInfo, error)
func (c *HubClient) GetPromInfo(ctx) (*infosync.PrometheusInfo, error)
func (c *HubClient) Close() error
```

**streamLoop**：
- round-robin 选 addr；`grpc.NewClient` + keepalive（Time 10s/Timeout 3s，参数照
  `etcd.InitEtcdClient` 的 DialOptions 抄）+ TLS creds。
- `Subscribe{ClientId: cfg 的 advertise addr, KnownRevision: c.rev}`。
- `Recv` 循环 → `c.apply(resp)`：
  - `full=true`：整表重建（新 map + 新指针，F8）;
  - `full=false`：`maps.Clone` 旧表 → upsert（`convert.ToTopologyInfo`）/
    remove → 换表;
  - `resp.Prometheus != nil` → 更新 `c.prom`;
  - `c.rev = resp.Revision`。
- Recv 错误 → backoff（1s 起、上限 10s，**带 ±20% 随机 jitter**）→ 换下一个 addr
  重连。jitter 必须有：hub 重启时 1000 个 sidecar 的重连时钟是同步的，无 jitter =
  每个 backoff 周期一波惊群（K 个 hub 每台瞬时 ~N/K 条新流 + N/K 次 full 快照）。
  有 F17 的 full 缓存兜底,但摊开到几秒内更稳。
- **每次 apply full 后调 `observer.Refresh` 不需要** —— observer 3s 内自然拉到，
  不加这个耦合。

**GetTiDBTopology**：`snap == nil`（从未收到 full）→ 返回
`errors.New("discovery hub not ready")`；PDFetcher 对错误无限重试，语义与"PD 未就
绪"一致。非 nil → `maps.Clone(c.snap)`。

**GetPromInfo**：`prom == nil` → 返回 `infosync.ErrNoProm`（复用现有错误,
metricsreader 已处理这个路径）。

### 4.3 装配 —— `pkg/manager/backendcluster/cluster.go`（server.go 零改动）

上游 backendcluster 重构（#1150）后，装配点在 `NewCluster`（`cluster.go:74`），
设计 §8 修订版：

- `Cluster.infoSyncer *infosync.InfoSyncer` 字段改为 `topo topoSource`（小接口:
  `GetTiDBTopology`/`GetPromInfo`/`Close`，`*InfoSyncer` 和 `*HubClient` 都满足）;
  `GetTiDBTopology`/`GetPromInfo`（`cluster.go:40-46`）改调 `c.topo`。
- `NewCluster` 按 `clusterCfg.DiscoverySource` 分支：
  - `"hub"`：`NewHubClient(clusterCfg.HubAddrs, clusterTLS, ...)` + `Start`;
    **不建 etcd 客户端**（F10 修订：legacy 路径 `GetBackendClusters()` 用
    `Proxy.PDAddrs` 合成默认集群，而 `NewConfig()` 给它填 `"127.0.0.1:2379"` 默认
    值 —— 必须按 source 显式分支，不能判空 addr）;
  - 默认（`""`/`"pd"`）：现状路径原样（`InitEtcdClientWithAddrsAndDialer` +
    `InfoSyncer`）。
- `Cluster.Close()`：`c.etcdCli` 加 nil 判（hub 集群无 etcd）。
- **typed-nil 坑**：`topo` 从具体指针变接口，分支里只赋非 nil 具体值；`Close` 用
  `reflect.ValueOf(...).IsNil()` 双保险（仓库先例：vipManager 的 nil-guard）。
- 连带（都已验证不崩）：hub 集群的 `ClusterReader`（`cluster.go:117`）拿到 nil
  etcd → 内部选举 no-op（`election.go:95` nil-guard）→ 该集群无 metrics 均衡（设
  计 §11 已知限制）；VIP 冲突由 4.1 的配置校验挡住。
- `clusterReusable`（`manager.go:162`）要把 `DiscoverySource`/`HubAddrs` 纳入比
  较，否则热改这两个字段不会触发集群重建。
- Manager 合并层（`manager.go:217` 的 `GetTiDBTopology`）零改动 —— `ClusterName`
  由它填,`Addr` 由 PR1 的解析器填,HubClient 返回的 infos 结构与 InfoSyncer 完全一
  致。

### 测试

`pkg/discovery/client_test.go`：
1. **fake hub**：进程内起真 `Hub`（embedded etcd）+ gRPC server，`HubClient` 连上:
   full 应用、delta 应用、`GetTiDBTopology` 结果与 etcd 状态一致。
2. **not ready**：不起 hub → `GetTiDBTopology` 返回错误、不 panic。
3. **重连**：杀掉 hub 的 listener → client 换 addr 重连（起两个 hub）→ 拿到新
   full 覆盖。
4. **COW**：拿到 `GetTiDBTopology` 返回值后触发一次 delta，断言先前返回的 map 不被
   修改。

`lib/config`：`Check()` 校验用例（非法 source、hub 无地址、hub+VIP）。

`pkg/server`：`TestServerWithHubSource` —— 完整 `NewServer`（`discovery-source=hub`
指向进程内 fake hub），确认 nsmgr/observer/router 全链路拿到后端（照
`server_test.go` 模式）。

### 验收

手工链路：embedded/真实 PD + tidb → `tiproxy discovery` ×1 → sidecar `tiproxy`
（`discovery-source=hub`）→ mysql client 连 sidecar 能路由到 tidb；kill 一个
tidb，~lease TTL 后 sidecar 后端列表收敛。

---

## PR5 —— 收尾：文档 + 集成验证（落地版）

1. ✅ `README`：Service Discovery 小节加 hub 简介 + 文档链接。
2. ✅ 部署/运维/Canary/监控：合并成一份
   [discovery-hub-ops.md](discovery-hub-ops.md)（部署拓扑、配置样例、行为差异
   表、灰度与回滚步骤、指标 + PromQL、容量参考）。
3. ⚠️ Grafana 面板：`tiproxy_summary.json` 由 grafonnet 生成，仓库无 jsonnet 工
   具链，手改会导致 json/jsonnet 分叉 —— 面板 PromQL 先落在 ops 文档 §6,
   工具链就绪后再补 Discovery row。
4. ✅ 压测：落成常驻单测 `TestHubManySubscribers`（200 订阅者全量+增量 fan-out
   <1s，`-short` 跳过），比一次性脚本可维护。
5. ✅ 设计文档 §11 按 PR4 实证修订：hub 模式 metrics 均衡经 missing-metrics
   直接读取路径仍然工作,限制降级为"无选举去重"。

---

## 明确不做（v1 边界，动手前再读一遍）

- `known_revision` 续传 / delta 重放（F4：每次连接全量）。
- keyspace 过滤下推（proto 字段保留，hub 全量广播）。
- `hub-fallback-pd`（被拒绝：引回 PD 依赖 + 惊群，见设计 §9）。
- metrics 负载均衡进 hub 流（设计 §11，v1 接受降级为 static 同级行为；作为
  紧接着的独立设计跟进）。
- 方案 B（PD 微服务宿主）：proto 已是契约，届时只动 server 宿主。

## 依赖关系

```
PR1（infosync 重构）──┐
                      ├──> PR3（hub server）──> PR5
PR2（kvproto proto）──┤
                      └──> PR4（sidecar client）──> PR5
```

PR1/PR2 可并行；PR3/PR4 可并行（联调在 PR4 的集成测试里）。
