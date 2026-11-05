# 账号和登录会话

已完成注册、登录、查看当前身份、退出，并将会话接入弹幕发送。**游客可以连接并接收公开房间弹幕，发送需要有效会话；作者 `user_id` 由服务端确定。** 历史查询仍公开。

## 启动与手操

升级前停止旧 persist 消费者，再使用现有初始化命令创建 `users` 表，幂等初始化其他表并回填历史记录。已有 `danmaku_history` 会补充可空的 `user_id` 列；旧弹幕保留未知作者，历史 JSON 省略该字段：

```powershell
$env:GOLITUBE_MYSQL_DSN = 'golitube:golitube-local@tcp(127.0.0.1:13306)/golitube'
go run . -mode migrate-history
go run . -addr localhost:8081 -kafka-brokers 127.0.0.1:19092
```

账号服务使用已有 Redis `127.0.0.1:6379`；服务端需配置上述 MySQL DSN。注册、登录依赖 MySQL 和 Redis，身份查询、退出依赖 Redis。初始化命令不会创建默认账号。

在 Postman 中始终使用同一个主机名，例如 `localhost`；不要在 `localhost` 和 `127.0.0.1` 之间切换。保持 Cookie 管理开启，注册后再单独登录：

```http
POST http://localhost:8081/auth/register
Content-Type: application/json

{"username":"alice","password":"choose-your-own-password"}
```

返回 201，正文只有 `user.id`、`user.username`。用户名为 3～32 位 ASCII 字母、数字或下划线，去掉首尾空白并统一小写，重复返回 409。密码至少 8 个 Unicode 字符且最多 72 个 UTF-8 字节，不去掉首尾空格；请求体上限 4096 字节，不接受额外字段。

```http
POST http://localhost:8081/auth/login
Content-Type: application/json

{"username":"alice","password":"choose-your-own-password"}
```

成功返回 200 和 `user` 对象，通过 `Set-Cookie` 设置登录态；正文不返回密码或令牌。账号不存在与密码不匹配统一返回 401、`login_failed`。参数格式错误返回 400。

```http
GET http://localhost:8081/auth/me
```

Postman 自动携带 Cookie，成功返回当前 `user`；无会话、令牌错误或会话过期返回 401、`unauthenticated`。

```http
POST http://localhost:8081/auth/logout
```

退出删除当前令牌在 Redis 中的会话，并清除 Cookie，返回 204。重复退出仍返回 204。随后重新请求 `/auth/me` 应返回 401。当前只退出这个会话，不退出其他设备。

## 登录后发送弹幕

1. 登录成功后建立 `ws://localhost:8081/ws?room=1001` 连接。浏览器会在握手中自动携带同主机 Cookie；Postman 的 WebSocket 请求需确认握手带有 `golitube_session`，如果没有则在 Headers 中手动填写 `Cookie: golitube_session=<登录响应设置的值>`。不要把令牌放进 URL 或截图。
2. 发送 `{"content":"你好","position_ms":12500,"color":"#ff8800"}`。本房间成员收到相同 JSON，包含你的 `user_id`；客户端不能提交 `user_id` 字段。
3. 无 Cookie、会话过期、退出或令牌已轮换时，发送者收到 `{"type":"error","code":"unauthenticated","message":"请先登录，并重新建立弹幕连接"}`；该消息不发布、不保存，连接保留用于接收公开弹幕。
4. Redis 查询失败返回私有 `auth_unavailable` 错误，不降级为匿名发送。Redis 恢复后若原会话还有效，可继续尝试；会话丢失时需重新登录。
5. **Cookie 固定于 WebSocket 握手。** 游客连接建立后再登录，或重新登录轮换 Cookie，都需要关闭旧连接并重建；HTTP 请求的新 Cookie 不会改变已建立的 WebSocket。

每条发送都查询 Redis，不缓存握手时的登录结果，因此退出完成后开始会话检查的发送会被拒绝。已经通过检查、正在发布的消息可能在退出后完成；没有承诺将会话撤销与 Kafka/Redis 发布组成原子事务。只接收的游客不需要定期检查登录态。

## 选型与行为

- **bcrypt 保存密码哈希**：数据库不保存明文密码；使用 Go bcrypt 默认 cost 10，盐由库生成并编码在哈希中。没有自定义密码加密算法。未知账号也进行一次哈希比较，减少明显的执行路径差异；没有宣称响应时间严格相同。
- **随机令牌 + Redis 会话**：令牌由 `crypto/rand.Text()` 生成，会话固定 24 小时过期，访问不续期。服务端只接受 Cookie，不接受 URL 或请求体中的会话令牌。Redis 的键保存令牌 SHA-256 摘要，值只含用户 ID、用户名。
- **重新登录轮换令牌**：同一浏览器已有令牌时，在 Redis 一次 Lua 执行中创建新会话并删除旧会话；旧 Cookie 重放失效。多个设备可以保留各自的会话。
- **Cookie**：名称 `golitube_session`，设置 HttpOnly、SameSite=Lax、Path=/，不设置 Domain。浏览器脚本无法直接读取该 Cookie；浏览器 WebSocket 握手自动携带同主机的 Cookie，保留 Gorilla 默认同 Host Origin 检查。
- **HTTPS**：直接 TLS 请求自动设置 Secure；本地 HTTP 默认不设置。通过 HTTPS 入口代理时设置 `GOLITUBE_COOKIE_SECURE=1`，并保持正确的 Host；HTTP 本地开发不要启用该选项。
- **同源操作**：POST 请求若携带 Origin，要求其与服务地址匹配；不提供跨源账号接口。无 Origin 的 Postman 等客户端可以使用。同源前端代理可以用于后续前端接入。
- **请求限制**：注册和登录共用每个直连 IP 的 60 秒固定窗口，最多 30 次请求，Redis 原子更新计数；超过返回 429 和 `Retry-After: 60`。不信任客户端提供的 X-Forwarded-For，代理后的真实 IP 识别仍需单独设计。
- **依赖失败**：数据库或 Redis 错误返回 503、`auth_unavailable`，不伪装为密码错误或登录成功；会话记录丢失则需重新登录。响应不缓存，日志不输出请求正文、Cookie 或令牌。

选择 Redis 会话是为了复用已有组件，并能在退出时直接撤销当前令牌；代价是每条弹幕多一次 Redis 查询。当前没有密码重置、角色权限、账号注销或弹幕发送限流，也没有性能容量结论。

## 验证

```powershell
go test -race -timeout 30s ./...
go vet ./...
$env:GOLITUBE_TEST_MYSQL_DSN = 'golitube:golitube-local@tcp(127.0.0.1:13306)/golitube_test'
$env:GOLITUBE_TEST_REDIS_ADDR = '127.0.0.1:6379'
go test -race -v -run '^TestAuthIntegration$' -timeout 60s .
```

集成测试仅接受 `_test` 数据库，使用随机用户名和独立 Redis 命名空间，退出时清理自己的账号和会话。2026-11-04 已通过真实 MySQL/Redis 测试，测试函数耗时 5.96 秒：注册、重复账号、密码哈希、登录失败统一响应、两个 HTTP 服务共享身份、令牌轮换、退出后重放、过期、HTTPS Cookie 和限流。两个 HTTP 服务运行在同一测试进程，此次没有验证两个独立 Go 进程的账号路由。

本机日志：`F:\Docker\GoliTube-tests\auth-integration-20261104.log`。实现依据：[Go bcrypt](https://pkg.go.dev/golang.org/x/crypto/bcrypt)、[OWASP 会话与 Cookie](https://cheatsheetseries.owasp.org/cheatsheets/Session_Management_Cheat_Sheet.html#cookies)。

2026-11-05 接入发送鉴权后，重新通过竞态测试、`go vet`、真实 MySQL/Redis 的账号与历史查询测试、双进程 Redis 广播测试。`TestAuthIntegration` 本次耗时 4.81 秒，覆盖游客只能接收、伪造 `user_id` 被拒、广播与历史作者一致，以及轮换/退出/过期使已有连接下次发送失效；该测试显式调用保存函数，不经过 Kafka。

另启动两个真实 Go server 进程和一个 persist 进程，使用真实 Kafka、Redis、MySQL 走通注册登录 → Cookie 发送 → 跨进程广播 → 异步入库 → 历史查询 → 另一实例退出撤销。广播与历史 JSON（含作者）完全一致，拒绝的消息不出现在历史中。开发库升级已执行；演示账号和房间已清理，验证用 Go 进程已退出。日志：`F:\Docker\GoliTube-tests\auth-danmaku-integration-20261105.log`、`F:\Docker\GoliTube-tests\authflow-result-20261105.log`。逐条检查会话的设计参考 [OWASP WebSocket 消息级授权](https://cheatsheetseries.owasp.org/cheatsheets/WebSocket_Security_Cheat_Sheet.html#message-level-authorization)。
