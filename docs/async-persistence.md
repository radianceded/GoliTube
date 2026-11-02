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

该测试使用真实 Kafka 与 MySQL，验证先入队后启动消费者、二进制与空消息、写库后未提交位点时重启重放、重复事件只保留一行。为聚焦持久化，测试将 Redis 转发替换为本地广播；真实 Redis 跨实例行为由已有集成测试覆盖。

2026-11-02：本机未安装 Docker、Kafka 或 MySQL。已通过独立于外部服务的测试与 Go 静态检查；真实 Kafka/MySQL 测试默认跳过，Compose 仅做 YAML 解析检查，尚未启动验证。上述手操步骤是待执行的验收路径，不是已完成的实验结果。

实现依据：[kafka-go 手动提交消费位点](https://github.com/segmentio/kafka-go#explicit-commits)、[MySQL 重复键处理](https://dev.mysql.com/doc/refman/8.4/en/insert-on-duplicate.html)、[Kafka 官方 Docker 示例](https://github.com/apache/kafka/blob/trunk/docker/examples/docker-compose-files/single-node/plaintext/docker-compose.yml)。
