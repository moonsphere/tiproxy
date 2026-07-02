# Discovery Hub E2E 测试设计

设计见 [discovery-hub.md](discovery-hub.md)。现有测试的覆盖与空缺：

| 层 | 已有 | 空缺 |
|---|---|---|
| 单元/集成（embedded etcd + 进程内 gRPC） | hub/client/backendcluster/server 87 项 | — |
| 容器烟囱（手工） | 镜像内 discovery 模式连真 PD | 未自动化 |
| **端到端** | — | **真 PD/TiDB/SQL 链路、生命周期事件、hub HA、灰度** |

E2E 补的是单测替身盖不住的东西：**真 TiDB 的注册/注销行为**（lease、优雅退出
时的 key 清理）、**真 SQL 流量经 sidecar 的路由结果**、**多进程真网络下的故障
切换时序**。

## 1. 基建选型：docker-compose

| 候选 | 评价 |
|---|---|
| **docker-compose（选定）** | 可复现、无 host 依赖（只要 docker）、直接复用 `moonsphere/tiproxy:discovery-hub` 镜像、天然支持 kill/stop/scale 编排 |
| tiup playground | 最快，但依赖 host 装 tiup，进程编排（kill 单个 tidb）别扭 |
| kind (k8s) | 最贴近 sidecar mesh 终态，但重、慢，规模压测阶段再上 |

TiDB 要向 PD 注册 `/topology/tidb/*` 必须真集群模式 → 最小拓扑 **PD×1 + TiKV×1 +
TiDB×2**（两台 TiDB 才能断言路由分布与摘除）。

### Compose 拓扑

```
                       ┌────────────────────────────────────┐
                       │            e2e 网络（bridge）        │
  pd (pingcap/pd)      │  tikv (pingcap/tikv)               │
      ▲   ▲            │      tidb-0, tidb-1 (pingcap/tidb) │
      │   │ watch      │            ▲                       │
   hub-0  hub-1        │            │ SQL(4000)             │
      ▲   ▲            │       健康检查(10080)               │
      └─┬─┘ Subscribe  │            │                       │
     sidecar ──────────┴────────────┘                       │
      ▲ 6000/3080                                           │
      │                                                     │
  Go test harness（host，docker CLI + mysql driver + HTTP）  │
```

- hub-0/hub-1、sidecar 都用 `moonsphere/tiproxy:discovery-hub` 镜像，仅 command
  与挂载的 toml 不同。
- sidecar 配置：`discovery-source = "hub"`、`hub-addrs = "hub-0:3080,hub-1:3080"`、
  **无 pd-addrs**（顺带持续验证零 PD 路径）。
- 对照组（场景 S8 用）：`sidecar-pd`，`pd-addrs = "pd:2379"` 传统模式。

## 2. 断言手段

| 手段 | 用途 |
|---|---|
| **SQL 指纹**：host 用 go-sql-driver 连 sidecar:6000，`SELECT @@port`（tidb-0=4000 映射区分）循环 N 次收集集合 | 路由真相 —— 后端可达性/分布/摘除的最终裁决 |
| **hub /metrics**：`tiproxy_discovery_{backends,subscribers,revision,rebootstrap_total}` | hub 侧状态 |
| **sidecar 日志 grep**：`docker logs`（reconnecting / not pushed snapshot） | 切换与降级路径确认 |
| **PD etcd 直查**（etcdctl in pd 容器）：`/topology/tidb/` key 存在性 | 区分"TiDB 没注册"和"链路没传到" |

原则：**每个场景的最终断言必须落在 SQL 指纹上**（用户视角），metrics/日志只做中
间态定位。

## 3. 场景矩阵

统一节奏：`require.Eventually` 轮询断言,超时按事件类型给预算（见每行）。

### P0 —— 核心链路与故障（首批实现）

| # | 场景 | 步骤 | 断言 | 时限预算 |
|---|---|---|---|---|
| S1 | 基础链路 | compose up 全套 → 连 sidecar | SQL 指纹集合 = {tidb-0, tidb-1}；hub `backends=2`，`subscribers≥1` | 启动后 60s |
| S2 | TiDB 扩容 | `docker compose up tidb-2` | 指纹集合出现 tidb-2（sidecar 不重启） | 30s（推送秒级 + 健康检查 3s + 路由收敛） |
| S3 | TiDB 优雅缩容 | `docker stop tidb-2`（SIGTERM，TiDB 主动清 etcd key） | 指纹集合移除 tidb-2；期间 SQL 零错误（存量连接由既有迁移逻辑处理，e2e 只断言新连接） | 30s |
| S4 | TiDB 硬杀 | `docker kill tidb-1` | 指纹先靠 sidecar 本地健康检查摘除（秒级，新连接不再去 tidb-1）；hub 侧 `backends` 在 lease TTL（~45s）后降为 1 | 健康摘除 15s；拓扑收敛 90s |
| S5 | Hub 故障切换 | 找出 sidecar 当前连的 hub（两台 hub 的 `subscribers` 指标），`docker kill` 它 → 再扩容一台 TiDB | sidecar 日志出现 reconnecting；新 TiDB 仍进入指纹集合（经另一台 hub） | 60s |
| S6 | 全 hub 宕机降级 | kill 两台 hub | 存量拓扑 SQL 持续可用；**缓存内后端**宕机被健康检查围栏、重启后恢复可路由（resurrection，实测确认的正确行为）；**真正新增**（缓存从未见过）的 TiDB 不可见；hub 恢复后可见 | 降级验证 15s；恢复收敛 120s |

### P1 —— 生命周期与灰度

| # | 场景 | 步骤 | 断言 | 时限 |
|---|---|---|---|---|
| S7 | Hub 重启 full 重推 | hub 宕机期间缩容 tidb-2（制造 sidecar 缓存 stale）→ 重启 hub | 缓存驱逐的观测 = 该 stale 后端健康检查 tick（debug 日志 `unhealthy backend is not in router`）的**停止**；宕机期间 tick 持续增长为正向对照。⚠️ 两个不可用的观测：per-backend metrics 序列（2h retention GC 才清）、router 的 list-removal 日志（只对 router 仍持有的后端触发，被围栏且无连接的早已不在 router） | 120s |
| S8 | 灰度热切换 | sidecar-pd（pd 模式）经 `PUT /api/admin/config/` 切 hub 模式 → 再切回 | 切换前持有的连接跨两次切换零错误；hub `subscribers` 总数 ±1 证实真切换；指纹集合不变。⚠️ 配置接口是 **merge 语义**（TOML 数组缺席=保留旧值），回切必须显式给 pd-source 的 backend-clusters 条目 | 每次 60s |

### P2 —— 规模与价值验证（后续，可选）

| # | 场景 | 说明 |
|---|---|---|
| S9 | 规模烟囱 | `docker compose up --scale sidecar=30`，单 hub `subscribers=30`，随机抽 sidecar 验指纹 |
| S10 | PD 负载对比（卖点验证） | 30 sidecar 分别跑 pd 模式 / hub 模式 5 分钟，采 PD `etcd_debugging_mvcc_range_total` 或 grpc served QPS，报告对比比值（预期 ~N/K 倍差） |

## 4. 仓库落位与运行方式

```
e2e/
  docker-compose.yaml        # pd/tikv/tidb-{0,1,2}/hub-{0,1}/sidecar/sidecar-pd
  conf/                      # hub.toml sidecar.toml sidecar-pd.toml
  e2e_test.go                # 场景实现,//go:build e2e
  harness.go                 # compose 编排(docker CLI 封装)、SQL 指纹、指标读取
```

- **build tag 隔离**：`//go:build e2e`，`go test ./...` 不受影响。
- sidecar 日志级别为 debug：S7 的缓存驱逐断言依赖 router 的 debug tick。
- 运行：`make e2e` → 构建镜像（复用 `make docker`）→ `go test -tags e2e ./e2e/
  -v -timeout 30m`。测试自己 `compose up/down`（`t.Cleanup` 保证残局清理）。
- 镜像版本：`TIPROXY_IMAGE` 环境变量注入，默认 `moonsphere/tiproxy:discovery-hub`；
  PD/TiKV/TiDB 用固定 tag（如 v8.5.x），避免 latest 漂移。
- 场景间**不共享集群状态**：P0 一套 compose 起一次跑 S1-S6（有序，前一场景的
  终态是后一场景的初态，节省起集群时间 ~1-2min/次）；S7/S8 各自独立 up/down。

## 5. 边界（不在本 e2e 范围）

- 连接迁移语义（session migration）本身 —— 上游既有能力，非 hub 引入。
- keyspace 拓扑 —— 单测已覆盖解析,e2e 需要 keyspace 化的 TiDB 部署，重。
- TLS 对称/非对称矩阵 —— 单测 + 排障文档覆盖；e2e 只跑明文。
- 1000 级规模 —— S9 到 30 即可证明编排正确性,千级留给 k8s 环境专项压测。

## 6. 时间预算

P0 全套（含起集群）目标 < 10min；P1 每场景 < 3min。CI 无（fork 无 pipeline），
本地/手动触发 `make e2e`。
