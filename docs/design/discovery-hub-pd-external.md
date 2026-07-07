# Discovery Hub 迁移 PD：`pd-server services tidb-discovery`

系列文档：[设计](discovery-hub.md) · [实现](discovery-hub-impl.md) ·
[运维](discovery-hub-ops.md) · [E2E](discovery-hub-e2e.md)

> **状态：采纳（2026-07-07）。** 决策链：
> 1. hub 应贴 PD 部署（sidecar→hub 是最耐远的 HTTP 轮询链路,hub→PD 的 etcd
>    watch 才怕远;etcd 访问面不出集群边界）。
> 2. 贴 PD 部署 → 组件归属 PD 仓库：hub 是**集群侧组件**,生命周期/版本/发布该随
>    PD/TiDB 集群走(tiup/operator 一起分发),不随应用侧 tiproxy 走。
> 3. 进程**保持外置**(独立于 pd-server 主进程)：PD leader 负担已重,外置多副本
>    可独立扩缩、异常秒级切换、故障域与 PD 隔离。曾评审"做进 PD API 进程内"
>    (B-lite),据此否决。
> 4. 承载形态用 **PD 微服务框架(mcs)**：`pd-server services tidb-discovery`,与
>    tso/scheduling 同款运维形态;用其外壳(子命令/装配/生命周期),不用 primary
>    选举(hub 无状态多副本对等)。
> 5. **无过渡期**：现 tiproxy 侧 hub server 无存量用户,迁移即删除。

## 1. 目标形态

```
pd-server services tso              # 已有
pd-server services scheduling      # 已有
pd-server services tidb-discovery  # 本计划新增
```

```
sidecar ×N ──HTTP 轮询 GET /api/topology + ETag/304──> tidb-discovery ×K ──watch──> PD etcd
```

- 线协议不变：JSON 字段 = tiproxy `discovery.TopologyResponse`
  （revision/backends/prometheus），路径 `/api/topology`，ETag = 内容 fnv64a。
  **sidecar（HubClient）零改动，连配置键都不变**（`hub-addrs` 指新组件地址）。
- 部署：K≥2 副本随 TiDB 集群侧部署（与 PD 同域），副本间无协调、无状态。

## 2. 仓库与分支

- PD 侧：`pingkai/pd`，分支 `release-7.1.8-5-jett-tiproxy-sidecar`
  （基线对应上游 8.5.5，mcs 框架成熟：tso/scheduling/resource-manager 均在）。
- tiproxy 侧：`feat/discovery-hub`（同步 `pingkai/tiproxy` 的
  `release-8.5.6-disc-hub`）。

## 3. PD 侧设计（侦察结论支撑）

### 3.1 落位

```
pkg/mcs/tidbdiscovery/
  server/
    config.go     # 裁剪自 tso config：backend-endpoints/listen-addr/TLS/log,~80 行
    server.go     # Server 结构 + 生命周期,~200 行
    hub.go        # hub 核心,从 tiproxy pkg/discovery 移植,~350 行
    hub_test.go   # 移植 tiproxy hub 单测(embedded etcd)
cmd/pd-server/main.go   # NewTiDBDiscoveryServiceCommand,~25 行,挂进 services 组
```

### 3.2 Server 骨架（复用 mcs 全家桶）

```go
type Server struct {
    *server.BaseServer                  // pkg/mcs/server,186 行现成,兜掉 20 方法接口的大半
    diagnosticspb.DiagnosticsServer     // sysutil.NewDiagnosticsServer 直接嵌
    cfg *Config
    hub *Hub
    ...
}
```

- 自实现方法仅：`Name/Run/Close/IsClosed/IsSecure/RegisterGRPCService(空)/
  SetUpRestHandler`。
- 生命周期走 mcs 标准链：`CreateServerWrapper`(71 行模板,照 tso 抄) →
  `utils.InitClient`(etcd 装配) → `utils.StartGRPCAndHTTPServers`
  (cmux;grpc 起空 server,diagnostics 白送,与其它 mcs 服务运维一致)。
- `SetUpRestHandler`：**mcs 本来就用 gin**（`utils.PromHandler`/`StatusHandler`
  都是 `gin.HandlerFunc`）→ tiproxy 的 `HandleTopology(c *gin.Context)` 原样移植：

```go
engine.GET("/api/topology", s.hub.HandleTopology)
engine.GET("/status", utils.StatusHandler)
engine.GET("/metrics", utils.PromHandler())
```

- **service registry**：走 `utils.Register` 正常注册（pd-ctl 可观测、与框架一致）；
  但 **sidecar 的 hub 地址仍由 Service DNS/静态配置下发,不依赖 registry** ——
  设计红线：sidecar 不回 PD 查地址。
- **不用 primary 选举**（`expected_primary.go` 不引入）：副本对等,谁都能答。

### 3.3 hub 核心移植清单（tiproxy `pkg/discovery` → `pkg/mcs/tidbdiscovery/server`）

| 移植项 | 来源 | 备注 |
|---|---|---|
| watchLoop（Txn 单 revision bootstrap + 双前缀 watch + F15 关闭检测 + F16 ttl 短路） | hub.go | 原样 |
| `ParseTiDBTopology`（info+ttl 配对/keyspace/排序确定性） | infosync（PR1 抽出的） | PD 侧复制实现——两仓库不共享代码 |
| 1 次序列化缓存 + **ETag=内容 fnv64a** | hub.go | 原样;内容 hash 跨副本语义正确（计数器碰撞 bug 的教训） |
| wire 类型（TopologyResponse/TiDBInstance/PrometheusInfo JSON tag） | types.go | 复制;JSON tag 是两仓库契约,**两侧各留 golden-JSON 测试防漂移** |
| promLoop（/topology/prometheus） | hub.go | 原样 |
| readiness：首次 bootstrap 前 `/api/topology` 回 503 | hub.go | 原样;k8s readiness 探针直接用该端点 |

- etcd 读一律 `WithSerializable()`（连本地 PD member 时不打扰 leader）。
- metrics 改挂 PD 命名空间（`tidb_discovery_requests_total{code}` 等四个,语义与
  tiproxy 版一一对应,Grafana 告警建议沿用 ops 手册 §6）。

### 3.4 PD 侧测试

- `hub_test.go`：移植 tiproxy 的 hub 单测全集（bootstrap/内容变化/ttl 刷新抑制/
  ETag 304/prom 捎带/rebootstrap 不热循环/并发 poller）,embedded etcd。
- golden-JSON 契约测试（固定样例响应字节,防字段漂移）。
- 服务级 smoke：CreateServer → 起真监听 → curl 200/304 → Close 干净。

## 4. tiproxy 侧：删除 hub server（无过渡）

**保留**（sidecar 消费端,继续用）：
- `pkg/discovery/client.go`（HubClient 轮询）+ `types.go`（wire 契约）+ 其单测
- `BackendCluster` 配置（`discovery-source`/`hub-addrs`）与校验、`NewCluster` 分支
- golden-JSON 契约测试（新增,对齐 PD 侧）

**删除**：
- `pkg/discovery/hub.go` + hub 单测、`pkg/server/discovery_server.go`、
  `pkg/server/api/discovery.go`（TopologyHandler 接口/路由）
- `cmd/tiproxy/main.go` 的 `discovery` 子命令
- `conf/hub.toml`、hub 侧 metrics（requests/backends/revision/rebootstrap 移交
  PD 侧）、`server.go` 的 `hub/hubEtcdCli` 字段与 Close 分支
- 文档相应章节改写（设计 §4/§5 hub 内部移至本文档引用,ops 手册部署节改指
  `pd-server services tidb-discovery`）

**e2e 适配**：compose 里 `hub-0/hub-1` 服务改用 `pingkai/pd` 镜像
（`command: ["pd-server", "services", "tidb-discovery", ...]`）。镜像由内部构建
提供（`TIDB_DISCOVERY_IMAGE` 环境变量注入）;8 个场景语义全部不变（S5/S6/S7 的
hub kill/restart 照旧）。官方 PD 镜像无此组件,故 e2e 依赖内部镜像——在 e2e
README/Makefile 注明。

## 5. 负载与初心（不变式确认）

hub 的价值 = 两个收敛,迁移后原样成立,只是宿主换了：
- **N 个昂贵 etcd 消费者 → K 个**：sidecar 仍零 etcd client;watch 流 = K 条。
- **全量读 → 缓存 + 304**：稳态 N 次空 304/3s,由 K 副本分摊;etcd 侧 O(K)。

与 PD 本体的隔离：独立进程,不占 pd-server 的 CPU/内存/端口;PD 升级与 hub
发布解耦（同仓库不同进程）。

## 6. 交付拆分

| # | 仓库 | 内容 | 验证 |
|---|---|---|---|
| PD-1 | pingkai/pd | mcs 组件全量（§3）：config/server/hub 移植/子命令/单测 | `pkg/mcs/tidbdiscovery` 套件全绿;本地 `pd-server services tidb-discovery` 起来 curl 200/304 |
| TP-1 | tiproxy | 删除 hub server（§4 删除清单）+ golden 契约测试 | 全量单测/lint;`go build ./...` |
| TP-2 | tiproxy | e2e compose 改用 PD 镜像跑 hub | 8 场景全绿 |
| 联调 | 两者 | 真集群:pd-server services tidb-discovery ×2 + sidecar | SQL 指纹 + S5 类故障切换手验 |
| 文档 | tiproxy | 设计/ops/impl 文档改写宿主;memory 更新 | — |

顺序：PD-1 → TP-2（e2e 先用新镜像验证等价）→ TP-1（确认等价后再删）→ 文档。
**TP-1 放在 e2e 通过之后** —— 删除是不可逆动作,先证明替代品可用。

## 7. 风险

| # | 项 | 处置 |
|---|---|---|
| 1 | pingkai/pd 推送权限 | 本地分支先做;tiproxy 侧 pingkai 推送已有先例 |
| 2 | 契约漂移（两仓库各持一份 wire 类型） | 两侧 golden-JSON 测试 + 联调逐字节比对 |
| 3 | e2e 依赖内部 PD 镜像,外部贡献者跑不了 | Makefile 变量可切;README 注明;hub 场景可 `-run` 跳过 |
| 4 | mcs 框架升级连带（BaseServer 接口变化） | 与 tso/scheduling 同暴露面,PD 侧统一升级时自然覆盖 |
| 5 | tiproxy 删除后回滚成本 | git 历史在;真需要时 revert TP-1 即恢复 |
