# TiDB 拓扑发现 —— E2E 测试

设计见 [discovery-hub.md](discovery-hub.md)。测试分层:

| 层 | 位置 | 盖什么 |
|---|---|---|
| 单元/集成 | tiproxy `pkg/discovery`、`pkg/manager/backendcluster`;pd `pkg/mcs/tidbdiscovery`(embedded etcd) | 协议语义、hub 拓扑维护、集群注入;golden JSON 契约互锁 |
| **端到端(本文档)** | `e2e/`,docker-compose,8 场景 | 单测替身盖不住的:**真 TiDB 注册/注销**(lease、优雅退出清 key)、**真 SQL 流量经 sidecar 的路由结果**、**多进程真网络故障切换时序** |

千级规模不在此层:S9/S10 到 30 实例(`E2E_SCALE=1` 门控),更大规模走 k8s 专项。

## 1. 拓扑

docker-compose(可复现、只依赖 docker、天然支持 kill/scale 编排)。TiDB 要向
PD 注册 `/topology/tidb/*` 必须真集群模式,故最小拓扑 PD×1 + TiKV×1 + TiDB×2
(两台才能断言路由分布与摘除)。

```
                       ┌────────────────────────────────────┐
                       │            e2e 网络(bridge)         │
  pd (pingcap/pd)      │  tikv (pingcap/tikv)               │
      ▲   ▲            │      tidb-0, tidb-1 (pingcap/tidb) │
      │   │ watch      │            ▲                       │
   hub-0  hub-1        │            │ SQL(4000)             │
      ▲   ▲            │       健康检查(10080)               │
      └─┬─┘ HTTP 轮询   │            │                       │
     sidecar ──────────┴────────────┘                       │
      ▲ 6000                                                │
      │                                                     │
  Go test harness(host,docker CLI + mysql driver + HTTP)    │
```

- hub-0/hub-1 用 PD 侧组件镜像(`TIDB_DISCOVERY_IMAGE`,构建见 §4),入口
  `pd-server services tidb-discovery --backend-endpoints=http://pd:2379`。
- sidecar:`discovery-source = "hub"`、`hub-addrs = "hub-0:3080,hub-1:3080"`、
  **无 pd-addrs**(顺带持续验证零 PD 依赖路径)。
- 对照组(S8 用):`sidecar-pd`,`pd-addrs = "pd:2379"` 传统模式。
- TiDB 容器 pin `hostname:`(指纹依据);TiKV 声明 `--capacity=10GB`
  (宿主机磁盘 >90% 会触发 low-space 保护,TiDB bootstrap FATAL)。

## 2. 断言手段

| 手段 | 用途 |
|---|---|
| **SQL 指纹**:host 连 sidecar:6000,开一批连接各 `SELECT @@hostname`,收集主机名集合。连接**并发持有**再关 —— 路由按评分,顺序建连会全落同一后端 | 路由真相 —— 可达性/分布/摘除的最终裁决 |
| **hub /metrics**:`tidb_discovery_{backends,revision,rebootstrap_total}` | hub 侧状态 |
| **sidecar 日志 grep**:`rotating to the next hub` / `has not served a topology snapshot yet` / `updated backend cluster` | 切换、降级、reload 路径确认 |
| **PD etcd 直查**(pd 容器内 etcdctl):`/topology/tidb/` key 存在性 | 区分"TiDB 没注册"与"链路没传到" |

原则:**每个场景的最终断言落在 SQL 指纹上**(用户视角),metrics/日志只做中间态
定位。统一 `require.Eventually` 轮询断言。

## 3. 场景矩阵

### P0 —— 核心链路与故障

| # | 场景 | 步骤 | 断言 | 时限预算 |
|---|---|---|---|---|
| S1 | 基础链路 | compose up 全套 → 连 sidecar | 指纹集合 = {tidb-0, tidb-1};hub `backends=2` | 启动后 60s |
| S2 | TiDB 扩容 | `up tidb-2` | 指纹出现 tidb-2(sidecar 不重启) | 30s(hub watch + 轮询 3s + 健康检查 3s) |
| S3 | TiDB 优雅缩容 | `stop tidb-2`(SIGTERM,TiDB 主动清 etcd key) | 指纹移除 tidb-2;期间新连接零错误 | 30s |
| S4 | TiDB 硬杀 | `kill tidb-1` | 新连接先靠 sidecar 本地健康检查摘除(秒级);hub `backends` 在 lease TTL(~45s)后降为 1 | 健康摘除 15s;拓扑收敛 90s |
| S5 | Hub 故障切换 | client 粘住地址表首位(hub-0),`kill hub-0` → 再扩容一台 TiDB | sidecar 日志出现 `rotating to the next hub`;新 TiDB 经 hub-1 进入指纹 | 60s |
| S6 | 全 hub 宕机降级 | kill 两台 hub | 存量拓扑 SQL 持续可用;**缓存内后端**宕机被健康检查围栏、重启后恢复可路由(resurrection);**缓存从未见过**的新 TiDB 不可见;hub 恢复后可见 | 降级验证 15s;恢复收敛 120s |

### P1 —— 生命周期与灰度

| # | 场景 | 步骤 | 断言 | 时限 |
|---|---|---|---|---|
| S7 | Hub 重启后驱逐 stale 缓存 | hub 全宕期间缩容 tidb-2(制造 sidecar 缓存 stale)→ 重启 hub | 驱逐的观测 = 该 stale 后端健康检查 tick(debug 日志 `unhealthy backend is not in router`)**停止**;宕机期间 tick 持续增长为正向对照。⚠️ 两个不可用的观测:per-backend metrics 序列(2h retention 才 GC)、router list-removal 日志(只对 router 仍持有的后端触发) | 120s |
| S8 | 灰度热切换 | sidecar-pd 经 `PUT /api/admin/config/` pd→hub → 再切回 | 切换前持有的连接跨两次切换零错误;每次切换 `updated backend cluster` 日志 +1 证实集群真重建;指纹集合不变。⚠️ 配置接口 **merge 语义**(TOML 数组缺席=保留旧值),回切必须显式给 pd-source 的 backend-clusters 条目 | 每次 60s |

### P2 —— 规模与价值验证(`E2E_SCALE=1` 门控,常规 `make e2e` 跳过)

| # | 场景 | 步骤 | 断言与实测 |
|---|---|---|---|
| S9 | 规模烟囱 | `--profile fleet-hub up --scale sidecar-fleet=30`(无 host 端口) | 抽 3 实例容器内 `wget /metrics`,各见 2 个健康后端(`tiproxy_backend_b_status`);两 hub `requests_total` 增速 = fleet × 1/3s。**实测: 155 次/15s/31 实例 = 每实例精确 3s 一次** |
| S10 | PD 负载对比(卖点验证) | 同一 30 实例 fleet 分别跑 hub/pd 模式,同 60s 窗口采 PD embedded etcd 的 range 计数(`etcd_debugging_mvcc_range_total`,host 经 42379) | **实测: pd 模式 2800 vs hub 模式 160(含集群本底),17.5×**。hub 模式 fleet 对 PD 零贡献(160 全为 PD 自身+TiDB 本底);pd 模式合每实例 ≈4.4 range/3s(拓扑读+自注册+选举 watch) |

## 4. 运行方式

```
e2e/
  docker-compose.yaml        # pd/tikv/tidb-{0,1,2}/hub-{0,1}/sidecar/sidecar-pd
  Dockerfile.pd-discovery    # alpine + 静态 pd-server,入口 services tidb-discovery
  conf/                      # sidecar.toml sidecar-pd.toml
  e2e_test.go harness.go     # //go:build e2e
```

1. hub 镜像(在 pingkai/pd 仓库):

   ```bash
   CGO_ENABLED=0 go build -tags without_dashboard ./cmd/pd-server
   docker build -f <tiproxy>/e2e/Dockerfile.pd-discovery -t tidb-discovery:e2e .
   ```

2. tiproxy 仓库:`make e2e` —— 构建 sidecar 镜像(复用 `make docker`)后
   `go test -tags e2e ./e2e/ -v -timeout 30m`。测试自管 compose up/down
   (`t.Cleanup` 清残局)。

- 镜像注入:`TIPROXY_IMAGE`(默认 `moonsphere/tiproxy:discovery-hub`)、
  `TIDB_DISCOVERY_IMAGE`(默认 `tidb-discovery:e2e`);PD/TiKV/TiDB 固定 tag
  (v8.5.x),避免 latest 漂移。
- build tag 隔离:`go test ./...` 不受影响。
- sidecar 日志级别 debug:S7 的驱逐断言依赖 router 的 debug tick。
- P0 一套 compose 有序跑 S1-S6(前一场景终态 = 后一场景初态,省起集群时间);
  S7/S8 各自独立 up/down。全套 < 10min。
- 规模场景另行触发:`E2E_SCALE=1 go test -tags e2e -run TestScaleFleetAndPDLoad
  ./e2e/ -v`(~3min,30 个额外容器)。
- CI 无(fork 无 pipeline),本地/手动触发。

## 5. 边界(不在本 e2e 范围)

- 连接迁移(session migration)语义 —— 上游既有能力,非本功能引入。
- keyspace 拓扑 —— 单测已盖解析;e2e 需 keyspace 化部署,重。
- TLS 对称/非对称矩阵 —— 单测 + [ops 排障](discovery-hub-ops.md) 覆盖;e2e 走明文。
- 千级规模 —— S9 到 30 证明编排正确性,千级留 k8s 专项。
