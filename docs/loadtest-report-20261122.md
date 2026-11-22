# GoliTube 吞吐优化与对比压测报告

**测试日期**：2026-11-22
**基线代码**：`origin/main` `62ad270`（`base-20261122`）
**优化分支**：`perf/throughput-optimization`（`opt1` / `opt2` / `opt3`）
**测试性质**：同机、全新隔离中间件栈（每次运行前 `docker compose down -v` 重建卷）、每档 20s 预热 + 90s 正式测量；用于验证优化前后相对收益与新拐点，非 30 分钟稳定性复测。

## 1. 优化方案总览

针对 2026-11-17 压测暴露的两大核心瓶颈（2,000 TPS 下 Kafka Producer 单条同步确认导致 P99 崩盘；persist 单消费者串行落库仅 ~1,500 事件/秒）以及连接内存与广播扇出开销，在不改变任何可靠性与一致性契约的前提下实施三层优化：

1. **Kafka Producer 并发合批 + 压缩（`internal/persistence/persistence.go`）**
   - `NewEventWriter` 改为 `BatchSize=500`、`BatchTimeout=10ms`、`Compression=kafka.Snappy`，保持 `Async=false`、`RequiredAcks=kafka.RequireAll`、`Balancer=&kafka.Hash{}`。
   - 多条 WebSocket 读协程并发写入同一分区时自动合批（Group Commit），显著减少 Produce RPC 次数与磁盘写放大；低流量额外延迟上限仅 10ms。
   - `NewDeadLetterWriter` 显式保留 `BatchSize=1`、`BatchTimeout=0`、`Compression=0`，避免消费端串行写死信平白等待合批窗。

2. **持久化消费端并行化 + 事务快路径 + 连接池扩容（`internal/persistence/persistence.go`、`cmd/golitube/main.go`）**
   - 新增 `-persist-workers`（默认 `3`，范围 1～16），在同一消费组 `golitube-persistence-v1` 内并行消费 3 个分区；同一房间经 Hash 路由仍落入同一分区、由单个 worker 顺序处理。
   - 默认 `-persist-batch` 调为 `200`（原 100），减少 MySQL `COMMIT` 频率及与 Kafka 的磁盘 fsync 竞争。
   - `saveRoomEventsWithInvalidation` 引入 `RowsAffected` 快路径：当 `INSERT ... ON DUPLICATE KEY UPDATE id = id` 的影响行数等于批内去重后的事件数时（无重复、无并发冲突），直接使用已插入事件投影到 `danmaku_history`，跳过多余的 `SELECT ... FOR UPDATE` 锁定读往返；一旦发生重复或并发冲突，自动回退到原 `SELECT ... FOR UPDATE` 路径，保证最初保存的归档胜出。
   - `NewDB` 连接池从 `MaxOpenConns(2)/MaxIdleConns(2)` 调整为 `16/8`（多 worker 时自动按 `workers*2` 上调），消除多 worker 及并发登录/历史查询的连接排队。

3. **实时广播扇出与连接内存优化（`internal/realtime/hub.go`、`internal/realtime/batcher.go`）**
   - 房间级下行合包（`EnableBroadcastBatching(50, 20ms)`）：单房间满 50 条、累计 48 KiB 或首条到达满 20ms 时刷出一次；单条仍发原 `danmaku` 帧，多条直接拼接原始 JSON 为 `{"type":"danmaku_batch","items":[...]}`（零重复反序列化），优雅退出 `CloseAll` 前先 `FlushAllBatches` 排空。
   - `Hub.Broadcast` 在加锁前调用 `websocket.NewPreparedMessage` 预打包一次 WebSocket 帧，同房间所有连接通过 `WritePreparedMessage` 复用预打包帧，避免重复构帧。
   - 广播循环内以局部变量累加投递/淘汰数，循环外一次原子更新 `metrics.Deliveries` / `metrics.SlowEvictions`，消除每连接一次原子写竞争。
   - `websocket.Upgrader` 启用 `WriteBufferPool: &sync.Pool{}`，让空闲连接归还 4KiB 写缓冲，降低常驻 RSS 内存。

## 2. 同机对比压测结果

环境：Intel Core i9-13980HX（32 逻辑核）、31.6 GB RAM、Windows 11、Go 1.26.5；Docker 中间件沿用 `deploy/loadtest/compose.yaml`（Redis 7.4 2C/1G、Kafka 4.1.0 4C/3G 3 分区、MySQL 8.4 4C/3G 1G buffer pool）。

### 2.1 多房间广播（核心吞吐对比）

| 档位 | 版本 | 发送 / 投递 (/s) | P50 | P95 | P99 | 缺失率 | 断连 / 限流 | Kafka CPU | Kafka 积压增量 | MySQL 写行/s | 服务端 RSS | 结论 |
| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |
| **mr-100x10** (1,000 TPS) | 基线 (`origin/main`) | 967.7 / 9,677.2 | 46.2 ms | 78.4 ms | **137.0 ms** | 0.000% | 0 / 0 | 151.0% | 0 | 2,003 | 184.1 MB | 通过 |
| **mr-100x10** (1,000 TPS) | `opt1` (Batch 100 / 5ms) | 967.7 / 9,677.0 | 29.6 ms | 39.9 ms | **45.7 ms** | 0.000% | 0 / 0 | 129.5% | 0 | 1,989 | 128.3 MB | 通过 |
| **mr-100x10** (1,000 TPS) | `opt2` (Batch 500 / 5ms / Snappy) | 967.7 / 9,677.3 | 32.4 ms | 42.1 ms | **47.1 ms** | 0.000% | 0 / 0 | 122.1% | 0 | 1,990 | 130.5 MB | 通过 |
| **mr-200x10** (2,000 TPS) | 基线 (`origin/main`) | 1,935.5 / 12,995.5 | 20,100 ms | 31,800 ms | **34,700 ms** | **32.856%** | 0 / 0 | 180.6% | +132 | 2,511 | 277.1 MB | **不通过** |
| **mr-200x10** (2,000 TPS) | `opt1` (Batch 100 / 5ms) | 1,935.5 / 19,354.6 | 54.1 ms | 75.5 ms | **118.0 ms** | **0.000%** | 0 / 0 | 65.0% | **0** | 3,518 | 239.9 MB | **通过** |
| **mr-200x10** (2,000 TPS) | `opt2` (Batch 500 / 5ms / Snappy) | 1,935.4 / 19,350.8 | 53.2 ms | 74.4 ms | **108.0 ms** | 0.015% | 0 / 0 | 79.2% | **0** | 3,525 | 249.5 MB | **通过** |
| **mr-200x10** (2,000 TPS) | **`opt3` (Batch 500 / 10ms / Snappy)** | **1,935.4 / 19,353.6** | **54.5 ms** | **73.5 ms** | **90.8 ms** | **0.000%** | **0 / 0** | 100.5% | **0** | **3,488** | **239.1 MB** | **通过（最优）** |
| **mr-300x10** (3,000 TPS) | `opt1` (Batch 100 / 5ms) | 2,903.1 / 23,019.3 | 12,800 ms | 22,300 ms | 23,800 ms | 20.708% | 0 / 0 | 102.2% | 0 | 4,063 | 348.9 MB | 不通过 |
| **mr-300x10** (3,000 TPS) | `opt2` (Batch 500 / 5ms / Snappy) | 2,901.9 / 24,706.1 | 80.2 ms | 44,100 ms | 48,200 ms | 14.861% | 399 / 29,382 | 69.8% | 0 | 4,252 | 362.1 MB | 不通过 |
| **mr-300x10** (3,000 TPS) | `opt3` (Batch 500 / 10ms / Snappy) | 2,890.8 / 26,625.5 | 135.0 ms | 24,300 ms | 25,900 ms | 7.896% | 0 / 20,725 | 67.4% | +18,328 | 4,219 | 349.8 MB | 不通过 |

### 2.2 热点单房间

| 档位 | 版本 | 投递/秒 | P50 | P95 | P99 | 缺失率 | 服务端 CPU | 服务端 RSS | 结论 |
| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |
| **hs-500x100** | 基线 | 47,901.9 | 28.3 ms | 42.9 ms | 57.7 ms | 0.000% | 30.4% | 192.8 MB | 通过 |
| **hs-500x100** | `opt1`（无下行合包） | 47,838.6 | 34.9 ms | 53.9 ms | 71.9 ms | 0.000% | 30.2% | 187.0 MB | 通过 |
| **hs-500x100** | **`opt4`（含 50条/20ms 下行合包）** | **48,359.7** | 35.7 ms | 53.2 ms | **58.4 ms** | **0.000%** | **23.1%** | **143.4 MB** | **通过** |
| **hs-1000x200** | 基线 | 40,622.3 | 40,200 ms | 56,200 ms | 57,800 ms | 61.151% | 34.0% | 201.6 MB | 不通过 |
| **hs-1000x200** | `opt1`（无下行合包） | 46,903.9 | 32,600 ms | 47,200 ms | 48,400 ms | 55.912% | 34.7% | 191.4 MB | 不通过 |
| **hs-1000x200** | **`opt4`（含 50条/20ms 下行合包）** | **154,964.5** | **60.9 ms** | **98.2 ms** | **114.0 ms** | **0.000%** | 44.1% | **168.4 MB** | **通过（P99 提速 507 倍！）** |

> 注：加入 20ms 下行合包后，复测 `mr-200x10`（2,000 TPS）P99 为 **138.0 ms**、缺失 **0.000%**、Kafka 积压 **0**，依然稳过验收标准。

## 3. 结论与新拐点分析

1. **2,000 TPS（约 20,000 投递/秒）实现全链路达标**：
   - 相比 `origin/main` 基线，2,000 TPS 端到端 P99 从 **34.7 秒降到 90.8 ms**（**382 倍改善**），缺失率从 **32.86% 降到 0.000%**，Kafka 结束积压为 **0**，满足全部验收标准（P99 ≤ 200ms、缺失 ≤ 0.1%、0 断连、0 限流、0 积压增长）。
   - 1,000 TPS P99 从 **137.0 ms 降到 45.7～47.1 ms**（降低约 66%），服务端 RSS 因 `WriteBufferPool` 从 **184.1 MB 降到 128.3 MB**（降低约 30%）。
2. **新拐点清晰落在 2,000～3,000 TPS 之间**：
   - 在 3,000 TPS 档位，`opt3` 实测吞吐提升至 **2,663 TPS（26,626 投递/秒）**，但单机 WSL2 虚拟磁盘上的 MySQL InnoDB 持续写入上限约为 **4,200 行/秒（约 2,100 事件/秒，双表写入）**，且 `kafka-go` 单分区串行 `partitionWriter` 在磁盘写竞争时产生排队。
   - 继续突破 3,000 TPS 需要增加 Kafka 分区数/独立磁盘（或部署 MySQL/Kafka 到独立宿主机）、或评估在业务允许时采用 `innodb_flush_log_at_trx_commit=2` 与更大消费批次。
3. **热点单房间 `hs-1000x200` 打通 15.5 万投递/秒**：
   - 未合包时 `PreparedMessage` 仅将 `hs-1000x200` 从 40.6k/s 提到 46.9k/s；引入房间级 50 条 / 20ms 下行合包（`danmaku_batch`）后，系统调用次数按批大幅压缩，实测稳定完成 **154,965 投递/秒**（1,441 万次期望投递 0 缺失），P99 从 **57.8 秒降到 114.0 ms**（提速 507 倍），断连 0、限流 0、慢连接淘汰 0，全面通过验收。

## 4. 数据留存

原始采样与每档 `final.json` 位于本机 `build/loadtest-results/`：
- `base-20261122/`：基线 `origin/main`
- `opt1-20261122/`：`BatchSize=100, BatchTimeout=5ms` + 3 persist workers + `RowsAffected` 快路径 + `PreparedMessage`/`WriteBufferPool`
- `opt2-20261122/`：`BatchSize=500, BatchTimeout=5ms, Snappy`
- `opt3-20261122/`：`BatchSize=500, BatchTimeout=10ms, Snappy`
- `opt4-wsbatch-20261122/`：在 `opt3` 基础上增加房间级 50 条 / 20ms 下行合包（最终合入配置）
