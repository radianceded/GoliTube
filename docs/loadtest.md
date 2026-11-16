# WebSocket 容量压测

本文记录 GoliTube 的连接容量、广播容量和持续处理能力测试方法与本机结果。测试工具、指标端点、隔离的中间件栈和运行脚本都在仓库内，可以重复执行。

## 目标与边界

三项能力分开测量：

| 能力 | 观察点 |
| --- | --- |
| 连接容量 | 稳定维持多少条 WebSocket 连接；建连失败、意外断连、心跳是否稳定，内存是否线性增长 |
| 广播容量 | 每秒接收多少条弹幕、完成多少次客户端投递；端到端 P99 与投递缺失率 |
| 持续处理能力 | Kafka 消费是否跟得上，积压是否持续增长；MySQL 事务写入速率与延迟 |

端到端延迟从压测客户端写出前开始，到接收连接读到广播为止，包含 Kafka 接收确认、Redis 发布/订阅、本机回环网络和 Hub 扇出，不包含 MySQL 落库。压测进程与服务端在同一台机器上，共用时钟，不存在跨机器时钟偏差。

本轮为单次、缩短时长的拐点探测，不是正式容量结论。正式结论需要按本文末尾的完整矩阵运行 30 分钟并重复 3 次。

## 环境

| 项目 | 值 |
| --- | --- |
| CPU | 13th Gen Intel Core i9-13980HX，24 物理核 / 32 逻辑核 |
| 内存 | 31.6 GB |
| 操作系统 | Windows 11 家庭版 中文版，10.0.26200 |
| Go | go1.26.5 windows/amd64 |
| 提交 | `3c4cad4`（分支 `loadtest-capacity`，基于 `52cb08d`） |
| Docker | 29.9.2，VM 32 核 / 15.4 GB |
| Redis | redis:7.4-alpine，2 CPU / 1 GB，`maxmemory 512mb`、`noeviction`、关闭 RDB/AOF，端口 16379 |
| Kafka | apache/kafka:4.1.0，4 CPU / 3 GB，堆 1 GB，单节点 KRaft，`golitube-danmaku` 3 分区 1 副本，保留 1 小时，端口 29092 |
| MySQL | mysql:8.4.13，4 CPU / 3 GB，buffer pool 1 GB，`max_connections=200`，测试库 `golitube_loadtest`，端口 23306 |
| 服务端 | 原生 Windows 进程 `golitube.exe`，监听 127.0.0.1:18081，`GOLITUBE_METRICS=1`、`GOLITUBE_QUIET=1` |
| 持久化 | 独立 `golitube.exe -mode persist` 进程，串行消费并逐条提交 MySQL 事务 |

所有进程与容器共用同一台机器。压测客户端本身也要消耗 CPU，因此高投递档位下的服务端 CPU 读数不是“独占机器”的数字；容器限制只约束容器，宿主机上的 Go 进程不受 cgroup 约束。

完整环境快照由 `deploy/loadtest/collect-environment.ps1` 写入每次运行的 `environment.json`（含 CPU、内存、系统版本、Go 版本、Git SHA、镜像 ID 和容器限制）。

## 压测客户端

`cmd/loadtest` 是独立 Go 程序，不依赖仓库内的服务端代码路径，只复用 `internal/metrics` 的分桶实现。

```powershell
go build -o build\loadtest.exe ./cmd/loadtest
```

主要参数：

| 参数 | 含义 |
| --- | --- |
| `-base` / `-ws` | HTTP 地址（用于推导 `ws://host/ws`）或直接指定 WebSocket 地址 |
| `-connections`、`-rooms` | 连接总数与房间数，连接平均分配，余数给前面的房间 |
| `-rate` | 每个房间每秒发送的消息数；0 表示只连接不发送 |
| `-msg-size` | 发送 JSON 文本的目标字节数（64～320，内容上限 255 字符） |
| `-warmup`、`-duration` | 预热和正式测量时长；预热期间同样发送流量，结束后清零统计并设置序号起点 |
| `-connect-rate` | 每秒建立连接数，避免瞬时建连把服务端 accept 队列打满 |
| `-senders` | 每个房间的发送连接数；默认按 `ceil(rate / 1.5)` 计算 |
| `-mysql-dsn`、`-redis-addr` | 注入测试账号和会话所需的测试库与 Redis |
| `-out`、`-summary-interval` | 结果目录与 10 秒汇总间隔 |

行为约定：

- 每条连接有独立读协程持续 `ReadMessage`，并实现 Ping/Pong 处理与读期限重置，空闲连接不会因为客户端自身不读而假性变慢。
- 每条消息内容以固定 18 字节标识开头（`LT` + 16 位十六进制全局序号），其余用 `x` 补齐到目标长度；接收端从 `content` 前缀解析序号。
- 发送序号是全局单调递增的，发送时间保存在 1M 槽位的环形数组里，接收时按序号取回，避免为每条消息保存长生命周期的 map。
- 统计每档的发送数、期望投递数、接收数、重复数、缺失率、P50/P95/P99、断连、限流、写错误、本地发送队列丢弃。
- 每 10 秒输出一行汇总，同时写 `summary.csv` 和 `summary.jsonl`；结束时写 `final.json`。
- 缺失率定义为 `(expected - received) / expected`。`expected` 在每条消息实际写出时按房间当前存活连接数累加；`final.json` 另给出按“发送数 × 房间连接数”独立计算的 `expected_by_formula` 与比值，用于核对。
- 重复检测是每条连接最近 16384 个序号的位图，超出窗口的迟到消息按重复计；当前链路没有重放，正常情况下重复应为 0。

### 账号与限流

发送需要登录态，但注册/登录接口按直连 IP 限制 30 次/分钟，数百个发送账号无法在压测前通过 HTTP 建立。工具用 `-mysql-dsn` 向测试库写入独立用户，并直接在 Redis 写入 `golitube:session:<sha256(token)>` 会话；每条发送连接使用一个独立账号。

- 单连接令牌桶：2 条/秒，突发 5。
- 单用户窗口：每 5 秒 10 条，跨连接、跨实例共享。

因此默认每个发送连接平均不超过 1.5 条/秒，既低于单连接令牌桶，也低于单用户窗口（5 秒 7.5 条 < 10 条）。同一账号开多条连接不能提高上限，压测不会用“一条连接 1 条/秒”来回避这个约束。

`-seed-only` 只注入账号，`-cleanup -out <原运行目录>` 删除对应账号和会话。

## 分档

本轮实际运行的缩短矩阵（每档只跑一次）：

| 组 | 档位 | 连接 | 房间 | 每房间速率 | 加热/测量 |
| --- | --- | --- | --- | --- | --- |
| 空闲连接 | idle-500 | 500 | 20 | 0 | 20s / 60s |
| 空闲连接 | idle-1000 | 1000 | 20 | 0 | 20s / 60s |
| 空闲连接 | idle-2000 | 2000 | 20 | 0 | 20s / 60s |
| 空闲连接 | idle-5000 | 5000 | 20 | 0 | 20s / 60s |
| 多房间广播 | mr-10x1 | 100 | 10 | 1/s | 20s / 90s |
| 多房间广播 | mr-50x5 | 500 | 50 | 5/s | 20s / 90s |
| 多房间广播 | mr-100x10 | 1000 | 100 | 10/s | 20s / 90s |
| 多房间广播 | mr-200x10 | 2000 | 200 | 10/s | 20s / 90s |
| 热点房间 | hs-100x50 | 100 | 1 | 50/s | 20s / 90s |
| 热点房间 | hs-500x100 | 500 | 1 | 100/s | 20s / 90s |
| 热点房间 | hs-1000x200 | 1000 | 1 | 200/s | 20s / 90s |

广播量核对：`mr-100x10` 为 100 房间 × 10 条/秒 = 1000 TPS，每房间 10 条连接，理论投递 10000 次/秒；`hs-1000x200` 为 200 TPS × 1000 连接 = 200000 次/秒。每档的期望值与实测值都在 `final.json` 中。

正式矩阵应把每档改为 1 分钟预热 + 5 分钟测量，在拐点附近缩小增量，并对最终稳定档运行 30 分钟、重复 3 次。脚本中的档位表和时长可以直接修改。

## 运行

```powershell
# 1. 中间件（独立项目名、端口和卷）
docker compose -f deploy/loadtest/compose.yaml up -d --wait

# 2. 测试库建表
$env:GOLITUBE_MYSQL_DSN = 'golitube:golitube-local@tcp(127.0.0.1:23306)/golitube_loadtest'
go run ./cmd/golitube -mode migrate-history

# 3. 服务端；指标端点和安静日志只在压测时开启
$env:GOLITUBE_QUIET = '1'
$env:GOLITUBE_METRICS = '1'
$env:GOLITUBE_PPROF = '1'   # 接近瓶颈时用 /debug/pprof 抓 CPU 和锁竞争
.\build\golitube.exe -addr 127.0.0.1:18081 -redis-addr 127.0.0.1:16379 -kafka-brokers 127.0.0.1:29092

# 4. 另一个终端启动持久化消费者
$env:GOLITUBE_MYSQL_DSN = 'golitube:golitube-local@tcp(127.0.0.1:23306)/golitube_loadtest'
.\build\golitube.exe -mode persist -kafka-brokers 127.0.0.1:29092 -redis-addr 127.0.0.1:16379

# 5. 运行矩阵；ServerPid 必须是 server 进程，不能是 persist
.\deploy\loadtest\run-matrix.ps1 -Tag run1 -ServerPid <server-pid>

# 6. 生成分析表
.\deploy\loadtest\summarize.ps1 -RunDir build\loadtest-results\run1
```

`run-matrix.ps1` 在每档开始前等待 Kafka 积压清零，结束后再等待排空；档位边界写入 `gears.csv`。`collect.ps1` 每 10 秒采样：

- 服务端与压测客户端进程的 CPU、RSS、线程数；
- `/debug/metrics` 的连接数、goroutine、堆、GC、投递数、慢连接淘汰、限流数、发送队列积压；
- Redis 的 CPU、内存、`instantaneous_ops_per_sec`、`publish` 的 `usec_per_call`；
- Kafka 消费组总积压；
- MySQL 的 `Innodb_rows_inserted`、`Com_commit`、连接数，以及 `performance_schema` 事务总耗时/次数。

## 验收标准与判读

| 指标 | 目标 |
| --- | --- |
| 广播端到端 P99 | ≤ 200 ms |
| 缺失率（排除建连与结束阶段） | ≤ 0.1% |
| 正常读消息的客户端 | 没有被持续淘汰 |
| 内存与 Kafka 积压 | 不持续增长 |

判读规则：

1. 首次持续不满足标准的档位是容量拐点。
2. 前一档经过 30 分钟复测仍满足标准，才作为稳定容量。
3. 广播正常但 Kafka 积压持续增长时，只能报告短时接入能力。
4. 广播量按 `各房间发送 TPS × 对应房间连接数` 求和核对；实测投递量与公式预期差距超过 0.1% 时先排查统计口径，再下结论。

## 已知限制

- 压测客户端、服务端、persist 和三个中间件容器同机运行；高投递档位下客户端抢占 CPU，服务端容量会被低估。
- 回环网络没有真实 RTT、Nginx/TLS、跨机房链路，也没有多实例广播。
- 单 Kafka broker、3 分区、单消费者进程；MySQL 单机、每次事务一次提交。
- 本轮每档 60～90 秒、只跑一次，不能证明长时间稳定；30 分钟复测未执行。
- 空闲档只验证连接与心跳，不包含浏览器前端、TLS 握手和公网 NAT 超时。
- `scripts` 为 Windows PowerShell 5.1 编写；Linux 下需要把采集器替换为等价脚本。

## 本轮结果

结果表在矩阵结束后由 `summarize.ps1` 生成，分析表保存在 `build/loadtest-results/<tag>/analysis.csv`。当前轮次的数据与结论见下方“结果摘要”。
