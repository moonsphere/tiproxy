# TiDB 拓扑发现 —— 实现导览

设计见 [discovery-hub.md](discovery-hub.md)。本文档是两仓库实现的代码地图,
面向要读/改这套代码的人。

## 1. 代码分布

功能横跨两个仓库,以 HTTP JSON 契约为界:

```
pingkai/pd                                tiproxy(本仓库)
pkg/mcs/tidbdiscovery/server/             pkg/discovery/
  ├ hub.go        拓扑维护核心              ├ client.go   HubClient(轮询+缓存)
  ├ topology.go   wire 类型+解析            ├ types.go    wire 类型+转换
  ├ server.go     mcs 外壳+HTTP 路由    ⇄   └ *_test.go   golden JSON 互锁
  ├ config.go     启动配置                pkg/manager/backendcluster/
  ├ metrics.go    tidb_discovery_*          └ cluster.go  topoSource 注入点
  └ *_test.go     golden JSON 互锁        lib/config/proxy.go  配置+校验
cmd/pd-server/main.go  子命令注册         e2e/            端到端(两镜像合演)
```

### PD 侧(`pingkai/pd` `pkg/mcs/tidbdiscovery/server`)

| 文件 | 内容 |
|---|---|
| `hub.go` | 唯一碰 etcd 的 goroutine:bootstrap(单 Txn 读双前缀,单一 baseRev)、双 watch(`baseRev+1` 起)、事件应用(ttl 刷新短路;watch channel 关闭需查 `ok` 标志防热循环)、COW 快照、序列化缓存 + fnv64a ETag 重建、promLoop(30s) |
| `topology.go` | wire 类型(`TopologyResponse`/`TiDBInstance`/`PrometheusInfo`)与 `parseTiDBTopology`(info+ttl 配对、keyspace 前缀剥离、按 key 排序保证序列化确定) |
| `server.go` | mcs 微服务外壳:嵌 `BaseServer`,`Run` = InitClient → service registry 注册 → 启 gRPC/HTTP;gin 路由 `GET /api/topology`(ETag/304/503)、`/status`、`/metrics`;无 primary 选举(`GetLeaderListenUrls` 返回自身地址) |
| `config.go` | `--backend-endpoints` / `--listen-addr`(默认 `http://127.0.0.1:3379`)/ `--advertise-listen-addr` / TLS / 日志,从 tso 服务配置裁剪 |
| `metrics.go` | 命名空间 `tidb_discovery`:`requests_total{code}`、`backends`、`revision`、`rebootstrap_total` |

入口:`cmd/pd-server/main.go` 在 `services` 组注册
`NewTiDBDiscoveryServiceCommand`;服务名常量在
`pkg/mcs/utils/constant`(`tidb-discovery`)。

构建(dashboard 资产与本功能无关且需额外工具链):

```bash
CGO_ENABLED=0 go build -tags without_dashboard ./cmd/pd-server
```

### tiproxy 侧(sidecar)

| 文件 | 内容 |
|---|---|
| `pkg/discovery/client.go` | `HubClient`:3s 轮询循环(sticky 地址,失败轮换并限速告警)、`If-None-Match` 协商、200 时 COW 替换本地缓存;`GetTiDBTopology`/`GetPromInfo` 读缓存,首个快照前返回 `ErrHubNotReady` |
| `pkg/discovery/types.go` | wire 类型 + `ToTopologyInfo`/`ToPromInfo`(转 infosync 内部类型) |
| `pkg/manager/backendcluster/cluster.go` | `topoSource` 接口;`NewCluster` 按 `discovery-source` 显式分支:`pd` 装 InfoSyncer,`hub` 装 HubClient(不建 etcd client)。**不要**用"pd-addrs 是否为空"判断 —— 它有默认值 |
| `lib/config/proxy.go` | `BackendCluster.DiscoverySource/HubAddrs` + 校验(hub 必须给地址;hub 与 `ha.virtual-ip` 互斥) |

## 2. 测试地图

| 测试 | 位置 | 盖什么 |
|---|---|---|
| golden JSON(×2) | `pkg/discovery/types_test.go` / pd `topology_test.go` | **契约互锁**:同一 golden 串两侧断言,含 re-marshal 字节级一致 |
| HubClient 单测 | `pkg/discovery/client_test.go` | `topoStub`(httptest,复刻内容 hash ETag 语义):轮询应用/未 ready/Prom/failover 轮换/COW |
| hub 单测 | pd `hub_test.go` | embedded etcd(`etcdutil.NewTestEtcdCluster`):bootstrap/增删/ttl 过期/keyspace/ttl 短路(ETag 不变)/watch 重建/revision 冻结 |
| 集群注入 | `pkg/manager/backendcluster/cluster_hub_test.go`、`pkg/server/server_test.go` | hub-source 集群经 stub 起 server、pd/hub 混用、热切换 |
| e2e(8 场景) | `e2e/`,`make e2e` | 真 PD/TiKV/TiDB + PD 侧 hub 镜像 + sidecar,SQL 指纹裁决,见 [e2e 文档](discovery-hub-e2e.md) |

## 3. 改动须知

- **改 wire JSON 必须两仓库同步**:先在两侧 golden 测试改同一串,再改类型;
  单侧改动会被对侧 golden 测试拦下。
- **ETag 语义**:必须保持"响应内容的确定性 hash"。任何引入非确定性序列化
  (map 遍历序、时间戳入 body)都会破坏 304,退化成每 3s 全量传输。
  服务端排序遍历与 revision 冻结即为此服务。**绝不可换回本地计数器** ——
  跨副本 failover 时计数器碰撞会产生假 304 冻结拓扑。
- **协议演进**:加字段向后兼容(旧 client 忽略);语义扩展走新 query 参数
  (如 long polling `?wait=`),不改既有请求形状。
- 排障手段见 [ops 文档](discovery-hub-ops.md) §7。
