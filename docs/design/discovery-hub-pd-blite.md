# Discovery Hub B-lite：拓扑端点内置到 PD

> **状态：已评审，暂不采纳（2026-07-07）。保持外置 hub（方案 A）为长期形态。**
>
> 决策理由：
> 1. **风险偏好**：PD 是集群命脉、leader 负载已重 —— 即使 handler 是
>    serializable 本地读、逻辑很轻，任何代码跑在 PD 进程内都共享其故障域;
>    外置 hub 的故障与 PD 完全隔离。
> 2. **运维弹性**：外置 hub 可独立多副本部署、独立扩缩/重启/发版，异常时
>    sidecar 秒级轮换到下一副本（e2e S5 已验证）;不占用 PD 的变更窗口。
> 3. **维护成本**：handler 进 PD = `pingkai/pd` 分支永久背一个功能补丁，每次
>    PD 升级都要 rebase;外置 hub 零 PD 侧维护负担。
>
> 本文档保留为设计记录：负载论证（§5、初心对比）与 ETag/revision 冻结等
> 结论对外置 hub 同样适用;若未来 N 达十万级需要重评架构，此文档是起点。

系列文档：[设计](discovery-hub.md) · [实现](discovery-hub-impl.md) ·
[运维](discovery-hub-ops.md) · [E2E](discovery-hub-e2e.md)

## 1. 背景与动机

当前形态（方案 A）：`tiproxy discovery` 独立 hub 进程贴 PD 部署，sidecar 经
`GET /api/topology`（HTTP + ETag）轮询。部署位置推演的自然结论：**hub 归属 PD
的网络/安全域** —— 那不如直接做进 PD。

HTTP 传输（PR10）把这一步的成本降到一个 handler：不需要 PD 微服务框架（那是
serverless 多租户拆分用的，与本需求无关，B-full 已排除），就是给单进程
`pd-server` 的 API 加一个只读路由。

收益：
- **少一个组件**：hub 进程/部署/监控全省，HA 直接复用 PD member（K = PD 副本数）。
- **少一跳网络**：sidecar → PD，不再 sidecar → hub → PD。
- **每个 member 本地服务**：无 leader 依赖，member 挂一个 sidecar 轮换下一个。

hub 组件**不删**：无状态小组件，留作 PD 版本不带端点时的 fallback 形态。

## 2. 目标仓库与分支

- PD 侧：`pingkai/pd`，分支 `release-7.1.8-5-jett-tiproxy-sidecar`
  （基线对应上游 8.5.5，`server/api` mux 框架、redirector/localOnlyPaths 机制齐备）。
- tiproxy 侧：`feat/discovery-hub`（同步 `pingkai/tiproxy` 的
  `release-8.5.6-disc-hub`）。

## 3. PD 侧设计

### 3.1 端点

```
GET /pd/api/v1/tidb-topology
  If-None-Match: <etag>   # 可选
→ 200 OK, ETag: <fnv64a of body>
  {"revision": 254674,
   "backends": [{"addr":"10.0.0.1:4000","ip":"10.0.0.1","status_port":10080,
                 "labels":{"zone":"z1"},"keyspace":"ks1","version":"v8.5.5"}],
   "prometheus": {"ip":"...","port":9090}}
→ 304 Not Modified        # ETag 命中,空 body
```

**JSON 字段与 tiproxy `discovery.TopologyResponse` 逐字段一致** —— 这是两仓库间
的线协议契约，sidecar 的 HubClient 解码零改动。ETag 语义同 hub：内容 hash，跨
member 一致（内容相同 304 合法）。

### 3.2 实现 —— `server/api/tidb_topology.go`（新文件）

```go
type tidbTopologyHandler struct {
    svr *server.Server
    rd  *render.Render
    mu  struct {
        sync.Mutex
        expiresAt time.Time  // 1s TTL
        body      []byte
        etag      string
    }
}
```

- **handler 流程**：缓存未过期 → 直接答（ETag 命中 304，否则 200 + body）；过期
  → 重建：读 etcd 两前缀 → 解析 → 序列化 → fnv64a → 更新缓存。singleflight 不
  需要——重建 ~1ms 级，Mutex 串行即可。
- ⚠️ **ETag 必须只对拓扑内容敏感，revision 要随内容冻结**：etcd
  `Header.Revision` 是全局的，TiDB 每 30s 刷 ttl 就推进它——若每次重建都取新
  revision 进 body，拓扑没变 body 也变，ETag 每 30s 失效一轮，304 机制形同虚
  设（hub 版靠 F16 短路天然规避，B-lite 的 TTL 缓存没有这层）。实现：重建时对
  **backends+prometheus 部分**做内容 hash，与旧值相同 → **整体复用旧缓存**
  （旧 body、旧 revision、旧 ETag）；不同才换新。revision 字段语义由此变为
  "拓扑最后一次变化时的 revision"——正好与 hub 版一致。
- **etcd 读**：`svr.GetClient()` + `clientv3.WithSerializable()`。⚠️ 必须显式
  serializable：PD 的 `etcdutil.EtcdKVGet` 和 clientv3 默认都是 linearizable
  （走 leader read-index），follower 本地服务的意义就没了。两前缀两次 Get 即可
  —— 轮询模型自愈，不需要 hub 那样的单 Txn 一致性（那是给 watch 定基线用的）。
- **解析**：从 tiproxy `infosync.ParseTiDBTopology` 移植（~60 行）：info+ttl 配
  对、ttl 缺失即丢弃、`/keyspaces/tidb/{ks}/topology/tidb/...` 前缀剥离、**按
  key 排序保证确定性**（ETag 是内容 hash，序列化顺序必须稳定）。放
  handler 同文件私有函数；不复用 `pkg/autoscaling.GetTiDBs`（它只看 ttl、不读
  info 详情、不支持 keyspace）。
- **revision**：取 `/topology/tidb/` 那次 Get 的 `resp.Header.Revision`（观测
  用，新鲜度由 ETag 负责）。
- **prometheus**：同一次重建里读 `/topology/prometheus`（WithPrefix 取首条，
  对齐 tiproxy hub 的读法），失败或为空则省略字段，不影响 backends。

### 3.3 路由注册 —— `server/api/router.go`

```go
tidbTopologyHandler := newTiDBTopologyHandler(svr, rd)
registerFunc(apiRouter, "/tidb-topology", tidbTopologyHandler.GetTiDBTopology,
    setMethods(http.MethodGet), setAuditBackend(prometheus))
```

加 `pkg/utils/apiutil/serverapi/middleware.go`：

```go
var localOnlyPaths = []string{
    ...
    "/pd/api/v1/tidb-topology",   // 每个 member 本地服务,不转发 leader
}
```

（HasPrefix 匹配，已核实。）

### 3.4 测试 —— `server/api/tidb_topology_test.go`

`mustNewServer` 套件，用例：

1. 空拓扑：200、`backends: []`、ETag 非空。
2. 完整字段：往测试 server 的 etcd 写 info+ttl（两前缀各一）→ 断言 addr/ip/
   status_port/labels/keyspace 逐字段。
3. ttl 缺失丢弃 / 只有 ttl 不出现 / 坏 JSON 跳过（对齐 tiproxy 解析器用例）。
4. ETag：First GET 拿 etag → If-None-Match 回 304 空 body → 写入新 tidb →
   同 etag 回 200 新内容新 etag。
5. 缓存：TTL 内两次 GET body 指针/内容一致；金丝雀写入在 TTL 内不可见、过期后可见。
6. 确定性：同一 addr 双前缀，多次 GET 的 body 逐字节一致。

follower 本地服务不单测（localOnlyPaths 机制自身有既有测试覆盖），联调时用真
集群验证（见 §6）。

## 4. tiproxy 侧设计（小 PR）

- `BackendCluster` 加 `HubPath string`（toml `hub-path`，默认 `/api/topology`）。
  校验：必须以 `/` 开头。`normalizeCluster` 把空值归一为默认值（防 ""↔默认写法
  切换触发无谓集群重建，同 R4 先例），`clusterReusable` 纳入该字段。
- `HubClient` 轮询 URL 改为 `scheme://addr + hubPath`。
- sidecar 直连 PD 的配置形态（进 ops 手册）：

```toml
[[proxy.backend-clusters]]
name = "default"
discovery-source = "hub"
hub-addrs = "pd-0:2379,pd-1:2379,pd-2:2379"
hub-path  = "/pd/api/v1/tidb-topology"
```

- TLS：PD client-urls 的 TLS 即 cluster TLS，HubClient 现有 `tlsGetter`
  （`[security.cluster-tls]`）配对直接成立，无需新配置。

## 5. 负载模型

N=1000 sidecar、3s 轮询、K=3 PD member：

```
每 member HTTP QPS ≈ 1000/3/3 ≈ 111（绝大多数 304,近零字节）
每 member etcd 读   ≈ 1/s（1s 缓存 TTL,serializable 本地读,不碰 leader）
```

对 PD 本体（TSO/心跳/调度）零干扰。与 A 形态相比少一跳、少一个组件。

## 6. 交付拆分与验证

| # | 仓库 | 内容 | 验证 |
|---|---|---|---|
| PD-1 | pingkai/pd | handler + 解析移植 + 缓存/ETag + localOnlyPaths + 单测 | `server/api` 套件；本地起 pd-server 后 `curl -H 'If-None-Match: …'` 手验 200/304 |
| PR11 | tiproxy | `hub-path` 配置 + client + 校验 + 单测 | 现有 discovery 套件 + hub-path 用例 |
| 联调 | 两者 | 本地 pd-server（pingkai/pd 编译）+ sidecar 直连 | SQL 指纹经 sidecar 路由 ✓；对比 hub 形态输出逐字节一致 |
| 契约防漂移 | tiproxy | golden-JSON 单测：固定一份 PD 端点样例响应,断言 HubClient 解码字段逐一正确 | 两仓库手工同步 JSON 的漂移在 tiproxy 侧被测试拦截 |
| 文档 | tiproxy | 设计 §13 演进记 + ops 手册"直连 PD"形态 | — |

e2e（compose）保持 hub 形态不变 —— 官方 PD 镜像无此端点；pingkai/pd 镜像的
端到端验证走内部 CI/联调。

## 7. 风险与开放点

| # | 项 | 处置 |
|---|---|---|
| 1 | pingkai/pd 推送权限 | 本地分支先做，推送时验证（tiproxy 侧已有先例） |
| 2 | PD API 无认证,端点暴露拓扑信息 | 与 /health、/members 等现有端点同一暴露面,拓扑本非敏感;TLS 由 client-urls 配置管 |
| 3 | 上游 PD 未来自带类似端点 | 路径经 `hub-path` 可配,切换零代码 |
| 4 | 1s 缓存导致拓扑感知最坏 +1s | 对照 3s 轮询 + 3s 健康检查 + 45s lease,可忽略 |
| 5 | PD member 间 ETag 抖动 | ETag 只对拓扑内容敏感（§3.2 revision 冻结）后,内容一致的 member 间 ETag 必然一致,抖动源消除;sidecar sticky 轮询进一步兜底 |
| 6 | hub-addrs 指 PD 但忘配 hub-path → 全部 404 循环 | fail-fast:轮换日志带 http status,排障手册补该症状 |
