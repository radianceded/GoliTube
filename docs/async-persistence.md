# Kafka + MySQL 异步持久化

## 消息链路

```text
浏览器 ws.send
  → Go 读取，生成事件 ID
  → Kafka 确认接收
  → Redis Pub/Sub → 各 Go 实例 → 同房间客户端发送队列 → 浏览器

Kafka → 独立 persist 进程 → MySQL 事务提交 → 提交 Kafka 消费位点
```

“异步”的对象是 MySQL 写入：接收弹幕的 Go 流程仍等待 Kafka 接收确认，但不等待数据库保存完成。`kafka.Writer.Async` 故意设为 `false`，否则调用返回不代表 Kafka 已接收。

Redis 消费者只广播，不负责保存。持久化消费者属于同一个消费组，避免每个广播实例都重复负责全部保存任务。

## 启动

要求安装 Go、Docker Engine 与 Docker Compose v2。Redis 继续使用现有的 `127.0.0.1:6379`，Compose 不操作该实例。Compose 中的账号、密码仅用于本地学习，服务端口绑定本机。

在项目目录的 PowerShell 中启动 Kafka 与 MySQL，再创建 topic：

```powershell
docker compose up -d --wait kafka mysql
docker compose run --rm kafka-init
```

Kafka 的宿主机地址是 `127.0.0.1:19092`；MySQL 是 `127.0.0.1:13306`。数据保存在命名卷中，`docker compose stop` 不删除数据。topic `golitube-danmaku` 为 3 分区、1 副本，保留时间为 7 天；这是本地单节点配置，不提供节点故障容灾。

终端 A，启动接收和实时广播服务：

```powershell
go run . -addr localhost:8081 -kafka-brokers 127.0.0.1:19092
```

终端 B，启动独立持久化消费者：

```powershell
$env:GOLITUBE_MYSQL_DSN = 'golitube:golitube-local@tcp(127.0.0.1:13306)/golitube'
go run . -mode persist -kafka-brokers 127.0.0.1:19092
```

消费者启动时要求 MySQL 可连接，自动执行 `schema.sql` 建表，账号需有该库的建表和读写权限。Ctrl+C 停止消费者；再次运行相同命令会使用相同消费组，从已提交位点继续。首次使用该消费组时从 Kafka 尚保留的最早消息开始。

不指定 `-kafka-brokers` 的 server 模式仍是原来的 Redis 广播模式，不保存消息。`/health` 仍只是 HTTP 存活检查，不能证明 Kafka 或 MySQL 已就绪；Kafka 生产者在发送消息时连接，不做启动探测。

## 手操验证

1. 先启动服务端，暂不启动持久化消费者。
2. Postman 连接 `ws://localhost:8081/ws?room=1001`，发送 `hello`。应能收到回声，消息此时进入 Kafka。
3. 启动消费者，执行以下命令检查数据库，应看到对应房间和内容。

```powershell
docker compose exec mysql mysql -ugolitube -p golitube -e "SELECT id, room_id, message_type, CONVERT(data USING utf8mb4) AS content, stored_at FROM danmaku ORDER BY stored_at DESC LIMIT 10;"
```

交互输入本地开发密码 `golitube-local`。`stored_at` 是数据库首次保存时间，不是客户端发送时间。

进一步验证运行中的数据库短暂不可用：

1. 保持消费者运行，执行 `docker compose stop mysql`。
2. Postman 再发送一条不同的消息，实时广播应仍可进行，消费者日志应显示保存失败和重试。
3. 执行 `docker compose start mysql`，等待数据库恢复；消费者应继续写入刚才的任务。
4. 再次查询数据库确认实际保存结果。

如果 MySQL 在消费者启动前就不可用，消费者会启动失败，需要恢复后重新启动。这与运行期间的自动重试不同。

## 为什么先写数据库，再提交消费位点

消费位点表示“这个消费组处理到哪里”。如果先提交位点再写数据库，中间进程崩溃，下次就可能跳过一条未保存的消息。

当前每次只处理一条：读取 → 数据库事务提交 → 提交该条 Kafka 位点 → 下一条。写库失败每秒重试同一个事件，不读取下一条，也不提交位点。提交位点失败则退出，重启消费者后允许再次读取未提交事件。

如果数据库已经提交，进程在提交位点前崩溃，重启后会再次收到同一个事件。`danmaku.id` 是主键，SQL 使用 `ON DUPLICATE KEY UPDATE id = id`，重复 ID 保留原来的记录，不插入第二行。这是针对相同事件 ID 的幂等写入，不是整条链路的 exactly-once 保证。

事件 ID 在 WebSocket 接收处用 `crypto/rand.Text()` 生成一次。相同内容的两次发送拥有不同 ID；浏览器重发也会得到新 ID。目前 ID 只在服务端事件内部流转，浏览器收到的仍是原始弹幕内容。

## 失败边界

- Kafka 入队出错或整个发布流程超过现有的 3 秒期限，发送连接关闭，不继续 Redis 广播。生产者超时可能出现“Kafka 实际收到了，但调用方未收到确认”，不能据此断言消息不存在。
- Kafka 接收后 Redis 发布失败，发送连接仍会关闭，但已进入 Kafka 的事件仍可能保存。两次写入不在一个事务中，不提供跨 Kafka/Redis 的原子成功；消费者不重放 Redis 广播。
- 事务提交或位点提交的响应丢失，可能导致重复处理；相同 ID 的数据库写入仍只保留一行。
- 非法 JSON、缺失 ID 或超限事件会使消费者报错退出并保留位点，避免静默丢弃。该消息会阻塞继续消费，需要人工排查并处理后恢复；尚未实现死信队列。
- 消费者逐条写库，任何一条持续失败都会阻塞这个进程的后续消费。尚未批量写入或按分区并行处理。
- 数据库停机超过 topic 保留时间，或 Kafka 单节点磁盘丢失，都可能导致未入库任务丢失。`RequireAll` 不能给单副本配置增加额外副本。
- 历史记录查询 API、客户端接收确认、客户端重试去重、压测与高可用部署尚未实现。

## 自动测试与本次验证

不依赖外部服务的测试：

```powershell
go test -race -timeout 30s ./...
go vet ./...
```

新增测试覆盖事件 ID 流转、相同内容的不同 ID、Kafka/Redis 两阶段失败行为、写库失败先重试后提交、取消退出、非法事件与提交失败不越过当前消息。这些测试用函数替换外部服务，不能证明真实 Kafka/MySQL 的行为。

真实集成测试需显式启用，建议使用专用测试数据库。测试创建随机 topic 和随机房间，只清理自己创建的 topic 和该房间数据；数据库表会保留。

```powershell
$env:GOLITUBE_TEST_KAFKA_BROKERS = '127.0.0.1:19092'
$env:GOLITUBE_TEST_MYSQL_DSN = 'golitube:golitube-local@tcp(127.0.0.1:13306)/golitube'
go test -race -v -run '^TestAsyncPersistenceIntegration$' -timeout 90s .
```

该测试使用真实 Kafka 与 MySQL，验证先入队后启动消费者、二进制与空消息、写库后模拟位点提交失败并重建 Kafka Reader 时的重放、重复事件只保留一行。这里没有实际杀死 Go 进程。为聚焦持久化，测试将 Redis 转发替换为本地广播；真实 Redis 跨实例行为由已有集成测试覆盖。

2026-11-02 初次实现时：已通过独立于外部服务的测试与 Go 静态检查；当时未安装 Docker，真实 Kafka/MySQL 测试未运行。

同日完成安装和真实集成验证：

- Docker Desktop 程序位于 `F:\Docker\Desktop`，WSL 数据磁盘位于 `F:\Docker\WSL`；Kafka、MySQL 镜像和数据卷保存在该磁盘中。
- Docker Engine 29.9.2、Kafka 4.1.0、MySQL 8.4.13；两个 Compose 服务健康，`golitube-danmaku` topic 的 3 个分区均存在。
- 创建独立的 `golitube_test` 数据库，设置上述测试环境变量（DSN 数据库名改为 `golitube_test`），执行真实集成测试命令，`TestAsyncPersistenceIntegration` 通过，测试函数耗时 23.22 秒。
- 验证消费者启动前 Kafka 已接收而数据库尚无记录、后续保存二进制和空消息，以及模拟位点提交失败后重放同一事件且不增加重复行。测试自己的随机 topic 和房间数据已由清理逻辑移除。
- 测试日志保存在本机 `F:\Docker\GoliTube-tests\persistence-integration.log`。
- 本次没有验证真实 Redis 与 Kafka/MySQL 同时参与的完整链路、实际 Go 进程崩溃恢复或运行期间 MySQL 停服恢复。上面的停库手操步骤仍是待执行实验。

### 2026-11-03 完整链路本机验收

使用项目实际 CLI 启动两个独立 server 进程（随机 HTTP 端口）和一个独立 persist 进程，二进制带 `-race` 编译。使用已有 WSL Redis `127.0.0.1:6379`、Kafka `127.0.0.1:19092` 和 MySQL 专用数据库 `golitube_test`，没有替换外部服务或发布函数。

1. 暂不启动 persist，将两个 WebSocket 客户端分别连接到两个 server 的同一随机房间。
2. 发送两条就绪消息，再发送中文、二进制和空消息，共 5 条事件；按成员加入的时序，检查 9 次 WebSocket 投递，均匹配消息类型和原始内容。
3. 消息已经通过 Kafka 入队和 Redis 广播，此时查询 MySQL，该随机房间为 0 行。
4. 启动真实 `-mode persist` 进程，轮询查询得到 5 行；逐条核对非空且互不相同的事件 ID、消息类型和完整内容，全部通过。
5. 退出本次启动的 Go 进程，清理该随机房间的数据库记录，复查为 0 行。Kafka 公共 topic 中的验收事件按现有保留策略保留，不删除或重建开发用 topic。

本机验收脚本：`F:\Docker\GoliTube-tests\verify-full-chain.go`；结果日志：`F:\Docker\GoliTube-tests\full-chain-20261103.log`；对应 server 和 worker 日志位于同一目录，以随机房间名开头。这是本机验收脚本，未纳入仓库自动测试。

这次证明正常运行时可以跨实例广播并延后入库，没有测量吞吐或延迟，也没有测试网络分区、进程崩溃或端到端恰好一次投递。数据库停服恢复在下述独立实验中验证。

### 2026-11-03 MySQL 停服恢复实验

新增显式启用的 `TestMySQLRestartRecovery`，普通 `go test` 默认跳过。测试只停止、启动和删除自己创建并核对所有权标签的 MySQL 容器，端口选取后显式绑定，数据位于 Docker 的 F 盘数据磁盘中，不操作现有开发用 MySQL。测试结束会删除该专用容器及其匿名数据卷。

```powershell
$env:GOLITUBE_TEST_MYSQL_RESTART = '1'
$env:GOLITUBE_TEST_REDIS_ADDR = '127.0.0.1:6379'
$env:GOLITUBE_TEST_KAFKA_BROKERS = '127.0.0.1:19092'
$env:GOLITUBE_TEST_DOCKER_BIN = 'F:\Docker\Desktop\resources\bin\docker.exe'
go test -race -v -run '^TestMySQLRestartRecovery$' -timeout 180s .
```

实验使用两个独立的广播子进程、真实 Redis/Kafka、测试进程内的真实持久化消费循环，以及独立 MySQL。测试消费组与房间使用随机名称。生产者复用实际 `runServer`、Kafka 写入和 Redis 发布路径，消费者复用实际读取、重试、数据库事务和位点提交逻辑。

实际结果：

1. 两条就绪消息完成跨实例广播、入库和位点提交，确认消费者已工作。
2. 正常停止专用 MySQL，再发送两条弹幕；两个客户端均收到消息，观察到真实写库失败，消费者未提交这两条事件的位点。
3. 启动同一个 MySQL 容器，保持 Go 广播子进程和消费循环不变；两条积压消息自动入库，并成功提交位点。
4. 原 WebSocket 连接继续收发恢复后的新消息；查询随机房间，共 5 行，事件 ID 非空且互不相同，每条内容仅出现一次。
5. 修正测试端口设置后，`go test -race` 实验通过，测试函数耗时 15.41 秒。日志位于 `F:\Docker\GoliTube-tests\mysql-recovery-20261103.log`。

首次实验曾因 Docker 自动分配的宿主机端口在 stop/start 后变化而超时。独立复现观测到端口从 45505 变为 46453；测试改为显式绑定同一空闲端口，并断言重启后地址不变。这里修复的是实验环境，未修改业务的重试策略。

为什么采用这些处理：写库失败时不提交位点，让 Kafka 保留未处理任务；继续重试同一事件，避免把“广播完成”误当作“保存完成”。失败事件会阻塞该消费者的后续处理，这是当前逐条保存策略的代价。使用专用 MySQL 避免影响开发数据，等待实际失败和提交信号比固定睡眠更能判断恢复行为。

实验只覆盖一次正常停服与重启，不证明数据库副本容灾、网络分区恢复、长期积压不丢失或端到端恰好一次投递。

实现依据：[kafka-go 手动提交消费位点](https://github.com/segmentio/kafka-go#explicit-commits)、[MySQL 重复键处理](https://dev.mysql.com/doc/refman/8.4/en/insert-on-duplicate.html)、[Kafka 官方 Docker 示例](https://github.com/apache/kafka/blob/trunk/docker/examples/docker-compose-files/single-node/plaintext/docker-compose.yml)。
