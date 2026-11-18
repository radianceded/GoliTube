# 单房间 1,000 连接弹幕 TPS 对照实验

2026-11-18，在 Windows 本地和腾讯云 Ubuntu 测试机分别做同机回环压测。两边均使用 `1c53d47` 的服务端代码。本地和云端的压测客户端使用同一个工作区版本，含未使用的源 IP 绑定选项。

## 负载与验收

- 单房间保持 1,000 条 WebSocket 连接，发送连接使用独立会话，走现有鉴权和限流。上行 JSON 目标大小为 128 字节。
- Kafka 3 分区、1 副本。生产端同步确认，每批上限 1 条。消费者每批写入上限 500 条，收集窗口 50 ms，事务成功后提交消费位点。
- 预热 15 秒，正式档测量 45 秒，停发后等待 6 秒再统计投递。10 TPS 连通性档测量 15 秒。
- 通过标准为实际发送速率达到目标的 95%，P99 ≤ 200 ms，缺失和重复计数均 ≤ 0.1%，无连接或协议错误。Kafka 观测窗口净积压增量 ≤ 200 条，排空后为 0。
- 表中上行和投递速率按最终计数除以正式测量时长。工具终端显示的速率还计入排空时间。广播 P99 从发送前到接收端读到消息，包含 Kafka 生产确认、Redis 广播和 WebSocket 扇出，MySQL 落库另外通过 Kafka 消费位点与表行数核对。
- 每条已广播弹幕预期投递给房间内全部 1,000 条连接，包括发送者自己的回传。缺失表示到排空截止仍未观测到的投递。

| 环境 | Go 服务与发压 | 中间件资源 |
|---|---|---|
| Windows 本地 | i9-13980HX，24 物理核 / 32 逻辑核，约 31.6 GB 内存，Windows 11 26200。Go 进程原生运行，使用默认 `GOMAXPROCS` | Docker Desktop 的 Linux VM 有 32 个逻辑 CPU、约 15.4 GiB 内存。Kafka 和 MySQL 各限 4 核 / 3 GiB，Redis 限 2 核 / 1 GiB |
| 腾讯云 Ubuntu | 4 vCPU，约 3.6 GiB 可用物理内存总量。Go 服务、消费者、发压客户端和测试 Redis 共享 1.5 核 / 288 MiB 配额。每个 Go 进程设置 `GOMAXPROCS=1` | Kafka 限 0.75 核 / 544 MiB，MySQL 限 0.75 核 / 704 MiB，MySQL buffer pool 128 MiB |

两边均使用 Go 1.26.5、Kafka 4.1.0、MySQL 8.4 和 Redis 7.4。硬件、操作系统和资源配额不同，这组结果适用于各自测试环境。发压与服务同机共享资源，公网 WSS 表现和长期稳定容量需要独立验证。

## 实测结果

| 环境 | 目标 TPS | 实际上行 TPS | 投递/秒 | P99 | 缺失率 | 排空后 Kafka 积压 | 结果 |
|---|---:|---:|---:|---:|---:|---:|---|
| Windows | 25 | 25 | 25,000 | 48.2 ms | 0% | 0 | 通过 |
| Windows | 35 | 35 | 35,000 | 49.7 ms | 0% | 0 | 通过 |
| Windows | 40 | 39.91 | 39,911 | 70 ms | 0% | 0 | 通过 |
| Windows | 45 | 44.42 | 44,422 | 185 ms | 0% | 0 | 通过 |
| Windows | 50 | 46.8 | 46,800 | 383 ms | 0% | 0 | 延迟与发压速率未达标 |
| Windows（复测） | 50 | 46.67 | 46,667 | 338 ms | 0% | 0 | 延迟与发压速率未达标 |
| 腾讯云 Ubuntu | 25 | 25 | 25,000 | 26.3 ms | 0% | 0 | 通过 |
| 腾讯云 Ubuntu | 50 | 50 | 50,000 | 45.5 ms | 0% | 0 | 通过 |
| 腾讯云 Ubuntu | 60 | 56.47 | 56,467 | 90.8 ms | 0% | 0 | 发压速率未达标 |
| 腾讯云 Ubuntu | 75 | 67.22 | 66,598 | 6.41 秒 | 0.9283% | 0 | 延迟、缺失率与发压速率未达标 |

- Windows 最高通过目标档为 45 TPS，实际 44.42 条/秒，44,422 次投递/秒。50 TPS 两次均低于发压速率要求，P99 分别为 383 ms 和 338 ms。
- 腾讯云最高通过目标档为 50 TPS，实际 50 条/秒，50,000 次投递/秒，P99 45.5 ms。60 TPS 档实际发出 56.47 条/秒，低于该档验收要求。75 TPS 档的 0.9283% 为排空截止仍未收到的投递。
- 双方各档结束后的 Kafka 积压均为 0。所有试次结束后，本地 Kafka 提交数、归档表和历史表分别为 15,175 条，云端三个计数分别为 12,226 条。本地计数包含首次预热验证和后续各档的预热流量。

目前只定位到热点房间下的实际发送速率和广播延迟开始退化。发压端调度、同机 CPU 竞争、生产确认与服务端 WebSocket 扇出的各自贡献仍待测量。

[逐档结果 CSV](loadtest-single-room-20261118.csv)包含正式测量窗口的发送、预期投递、实际投递计数，以及积压采样和验收结果。

## 复现 Windows 单档

在仓库根目录启动独立 Compose 项目。项目名隔离容器和卷，以下命令使用压测 Compose 的默认回环端口。

```powershell
docker compose -p golitube-single-room -f deploy/loadtest/compose.yaml up -d --wait redis kafka mysql
docker compose -p golitube-single-room -f deploy/loadtest/compose.yaml run --rm kafka-init
go build -o build/golitube.exe ./cmd/golitube
go build -o build/loadtest.exe ./cmd/loadtest
$env:GOLITUBE_MYSQL_DSN = 'golitube:golitube-local@tcp(127.0.0.1:23306)/golitube_loadtest'
.\build\golitube.exe -mode migrate-history
```

在两个终端分别启动 server 和 persist。两个终端均设置上面的 `GOLITUBE_MYSQL_DSN`。

```powershell
# 终端 A
$env:GOLITUBE_METRICS = '1'
$env:GOLITUBE_QUIET = '1'
.\build\golitube.exe -addr 127.0.0.1:18081 -redis-addr 127.0.0.1:16379 -kafka-brokers 127.0.0.1:29092

# 终端 B
.\build\golitube.exe -mode persist -persist-batch 500 -redis-addr 127.0.0.1:16379 -kafka-brokers 127.0.0.1:29092
```

在第三个终端运行指定档位。每档开始前确认消费积压已排空，结束后核对 `final.json`、`summary.csv` 及 Kafka 消费组位点。压测工具生成的会话令牌文件保存在本地结果目录。

```powershell
$env:GOLITUBE_LOADTEST_MYSQL_DSN = 'golitube:golitube-local@tcp(127.0.0.1:23306)/golitube_loadtest'
.\build\loadtest.exe -base http://127.0.0.1:18081 -connections 1000 -rooms 1 -rate 40 -msg-size 128 -connect-rate 250 -warmup 15s -duration 45s -drain 6s -summary-interval 5s -redis-addr 127.0.0.1:16379 -run-id single-40 -out build/loadtest-results/single-40
docker compose -p golitube-single-room -f deploy/loadtest/compose.yaml exec -T kafka /opt/kafka/bin/kafka-consumer-groups.sh --bootstrap-server localhost:9092 --describe --group golitube-persistence-v1
```

测试后停止两个 Go 进程，然后停止独立的 Compose 项目。

```powershell
docker compose -p golitube-single-room -f deploy/loadtest/compose.yaml stop
```
