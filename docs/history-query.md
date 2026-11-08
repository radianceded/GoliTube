# 历史弹幕查询

按房间和视频播放位置加载已入库弹幕。当前尚无视频实体，房间名暂作为视频的弹幕分组标识；历史查询公开，未知房间返回空数组。登录后的新弹幕带服务端确定的 `user_id`；旧弹幕没有作者则省略该字段。

## 初始化与升级

先停止旧版 persist 消费者，再用新版执行初始化和回填。该命令新增所需表、列和查询记录，不删除原归档；回填期间 Kafka 仍可保留待处理任务，保留期限见异步持久化文档。

本次升级会检查并补充 `danmaku_history.user_id VARBINARY(128) NULL`，可重复执行。已有旧记录保留 NULL；不会按房间或当前登录用户补造作者。归档中已有作者且投影缺失作者时，回填以原归档为准补充；重复消费不覆盖已有作者。

```powershell
$env:GOLITUBE_MYSQL_DSN = 'golitube:golitube-local@tcp(127.0.0.1:13306)/golitube'
go run . -mode migrate-history
```

命令幂等，每批最多扫描 200 条归档。重复执行会保留已有历史记录；中途失败可重跑，但会从头扫描。不要同时保留仍只写原归档的旧版消费者，否则扫描游标之前的并发写入可能需要再次回填。

在一个终端启动新版消费者：

```powershell
$env:GOLITUBE_MYSQL_DSN = 'golitube:golitube-local@tcp(127.0.0.1:13306)/golitube'
go run . -mode persist -kafka-brokers 127.0.0.1:19092
```

在另一个终端启动 HTTP/WebSocket 服务，两个进程使用相同数据库：

```powershell
$env:GOLITUBE_MYSQL_DSN = 'golitube:golitube-local@tcp(127.0.0.1:13306)/golitube'
go run . -addr localhost:8081 -kafka-brokers 127.0.0.1:19092
```

server 模式不设置 `GOLITUBE_MYSQL_DSN` 时仍可实时广播，历史查询返回 503。设置后创建连接池，但不在启动时等待 MySQL，单次历史查询最多等待 3 秒。缓存未命中且数据库不可用或表尚未初始化时返回 503，仍有效的缓存可以继续返回已有历史。

## 请求和响应

```http
GET /danmaku/history?room=1001&start_ms=0&end_ms=60000&limit=200
```

| 参数 | 规则 |
| --- | --- |
| `room` | 必填，去掉首尾空白后为 1～128 字节的 UTF-8 房间名 |
| `start_ms` | 必填非负 int64 整数，含此播放时间点 |
| `end_ms` | 必填整数，不含此播放时间点；大于开始，单段最多 60000 毫秒 |
| `limit` | 可选，默认 200，范围 1～500 |
| `cursor` | 可选，沿用上页的 `next_cursor`；房间和时间段必须保持一致 |

成功响应结构：

```json
{
  "items": [
    {
      "type":"danmaku",
      "id":"server-generated-id",
      "room_id":"1001",
      "content":"你好",
      "position_ms":12500,
      "color":"#ffffff",
      "created_at_ms":1793719200000
    }
  ],
  "next_cursor":""
}
```

`items` 沿用实时弹幕结构，按 `(position_ms, id)` 升序排列。`next_cursor` 非空表示还有下一页，将它作为 URL 查询参数传回；为空则当前查询已读完。无记录返回 `{"items":[],"next_cursor":""}`。

区间采用 `[start_ms,end_ms)`：加载 `[0,60000)` 后再加载 `[60000,120000)`，边界弹幕不会在两段重复。相同毫秒可能有很多弹幕，游标同时带播放位置和 ID，翻页不会仅因位置相同而跳过已有记录。

仅支持 GET，其他方法返回 405 并带 `Allow: GET`。参数错误返回 400，错误码 `invalid_query`；查询不可用返回 503，错误码 `history_unavailable`。错误对象结构与 WebSocket 错误一致。响应带 `Cache-Control: no-store`，浏览器与代理不保存响应，服务内部可以使用 Redis 缓存。

## Redis 缓存

起点对齐 30000 毫秒、长度为 30000 或 60000 毫秒且没有游标的查询使用 Redis。缓存键包含房间、区间、条数和游标字段的 SHA-256，以及房间版本。非空结果保留 10 秒，空结果保留 2 秒，查询耗时计入有效期，命中不延长 TTL。

未命中时查询 MySQL 并填充缓存。Redis 读取或写入失败时保留数据库查询的结果和错误语义，数据库错误不缓存。游标后续页直接查库，联合游标规则保持一致。

persist 消费者在 MySQL 事务提交成功后更新房间版本，版本有效期 60 秒。旧的并发查询只能回填旧版本页面，页面自身按短 TTL 自动过期。失效失败时写日志并继续处理消息，读者可能在短 TTL 内看到旧结果。已有部署需要更新 persist，并保证它的 `-redis-addr` 与 server 一致。独立回填或外部 SQL 修改后，已有缓存等待 TTL 过期。

历史分页仍然不提供快照隔离，首次请求读到旧结果的并发情况仍可能发生。当前没有合并同一键的并发回源请求，缓存容量和命中率需要后续按真实访问模式验证。

## 保存方式与取舍

- 原始 `danmaku` 表保留收到的事件 JSON；新增 `danmaku_history` 表保存适合查询的内容、播放时间、颜色和接收时间。
- 同一 MySQL 事务写入两张表，全部成功后才提交 Kafka 位点。查询表写入失败会回滚归档写入，留给消费者重试。
- 查询索引为 `(room_id,position_ms,id)`，对应按房间过滤、按时间范围查询和稳定排序，不需要每次扫描并解析全部 BLOB。
- 同 ID 重试以已有归档为准，两表均幂等。原始文本、二进制、缺字段或 ID/房间不匹配的旧记录保留归档，不猜测播放时间；完整的旧正式 JSON 可以回填。
- 代价是多一份存储和额外 SQL 写入。当前没有验证该设计的吞吐上限。

历史查询读取的是已完成入库的数据，收到实时广播后立即查询可能还看不到。分页不是数据库快照：晚入库的弹幕可能落在已经读过的游标之前；客户端需要按需重拉时间段，并用 ID 合并去重。当前不提供前端合并逻辑、客户端送达保证或视频存在性校验。

## Postman 手操

1. WebSocket 连接 `ws://localhost:8081/ws?room=1001`，发送 `{"content":"你好","position_ms":12500}`。
2. 收到正式弹幕 JSON 后，使用普通 HTTP GET 请求 `http://localhost:8081/danmaku/history?room=1001&start_ms=0&end_ms=60000`。
3. 消费者完成保存后，应看到同 ID、同内容和播放位置的弹幕。若刚发送时返回空数组，稍后重查同一区间。
4. 将 `limit` 设为 1，发送多条落在同一播放位置的不同内容，沿 `next_cursor` 翻页，应能逐条取完。

## 自动验证

```powershell
go test -race -timeout 30s ./...
go vet ./...
$env:GOLITUBE_TEST_MYSQL_DSN = 'golitube:golitube-local@tcp(127.0.0.1:13306)/golitube_test'
go test -race -v -run '^TestHistoryIntegration$' -timeout 60s .
```

真实数据库测试要求库名以 `_test` 结尾，使用随机房间，只清理自己插入的记录和临时约束。设置 `GOLITUBE_TEST_REDIS_ADDR=127.0.0.1:6379` 后，还会启动真实服务子进程验证路由和数据库配置。

2026-11-03 已通过真实 HTTP/MySQL 测试：时间范围、相同毫秒跨页、房间隔离、旧记录回填、重复回填、重复 ID 保留原事件，以及查询表写入失败时归档事务回滚。同时通过 MySQL 停服恢复回归，恢复后 5 条补存记录均能查到。结果日志：`F:\Docker\GoliTube-tests\history-recovery-integration-20261103.log`。没有进行容量压测。

2026-11-04 完成最终复验：普通竞态测试和静态检查通过；真实 `TestHistoryIntegration`、`TestAsyncPersistenceIntegration` 通过，复查旧二进制归档、消费重放与新增查询表可以共存。日志：`F:\Docker\GoliTube-tests\history-final-20261104.log`。本地开发库 `golitube` 已执行 `migrate-history` 初始化成功，当时归档为 0 条；HTTP 服务仍需按上文配置启动。
