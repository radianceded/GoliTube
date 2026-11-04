# 账号和登录会话

本次完成 HTTP 账号流程：注册、登录、查看当前身份、退出。**当前 `/ws` 仍允许匿名发送，尚未使用会话检查发送权限，弹幕中也未绑定用户 ID。** 历史查询仍公开。

## 启动与手操

使用现有初始化命令创建 `users` 表，同时幂等初始化其他表和回填历史记录：

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

## 选型与行为

- **bcrypt 保存密码哈希**：数据库不保存明文密码；使用 Go bcrypt 默认 cost 10，盐由库生成并编码在哈希中。没有自定义密码加密算法。未知账号也进行一次哈希比较，减少明显的执行路径差异；没有宣称响应时间严格相同。
- **随机令牌 + Redis 会话**：令牌由 `crypto/rand.Text()` 生成，会话固定 24 小时过期，访问不续期。服务端只接受 Cookie，不接受 URL 或请求体中的会话令牌。Redis 的键保存令牌 SHA-256 摘要，值只含用户 ID、用户名。
- **重新登录轮换令牌**：同一浏览器已有令牌时，在 Redis 一次 Lua 执行中创建新会话并删除旧会话；旧 Cookie 重放失效。多个设备可以保留各自的会话。
- **Cookie**：名称 `golitube_session`，设置 HttpOnly、SameSite=Lax、Path=/，不设置 Domain。浏览器脚本无法直接读取该 Cookie；后续浏览器 WebSocket 握手可以自动携带它。
- **HTTPS**：直接 TLS 请求自动设置 Secure；本地 HTTP 默认不设置。通过 HTTPS 入口代理时设置 `GOLITUBE_COOKIE_SECURE=1`，并保持正确的 Host；HTTP 本地开发不要启用该选项。
- **同源操作**：POST 请求若携带 Origin，要求其与服务地址匹配；不提供跨源账号接口。无 Origin 的 Postman 等客户端可以使用。同源前端代理可以用于后续前端接入。
- **请求限制**：注册和登录共用每个直连 IP 的 60 秒固定窗口，最多 30 次请求，Redis 原子更新计数；超过返回 429 和 `Retry-After: 60`。不信任客户端提供的 X-Forwarded-For，代理后的真实 IP 识别仍需单独设计。
- **依赖失败**：数据库或 Redis 错误返回 503、`auth_unavailable`，不伪装为密码错误或登录成功；会话记录丢失则需重新登录。响应不缓存，日志不输出请求正文、Cookie 或令牌。

选择 Redis 会话是为了复用已有组件，并能在退出时直接撤销当前令牌；代价是身份查询依赖 Redis。当前没有密码重置、角色权限、账号注销或 WebSocket 发送鉴权，也没有性能容量结论。

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
