# Redis 停服恢复实验

验证日期：2026-10-31（Asia/Shanghai）。

## 环境与执行方式

Windows Go 后端，Go 1.26.5、go-redis v9.23.0；Redis 7.0.15 在 WSL Ubuntu 中运行。测试以独立配置、随机端口启动专用 Redis，关闭 RDB/AOF 持久化；两个 Go 子进程拥有独立内存。停服前核对 Redis 的配置路径，只向测试自己的 Redis 发送 SHUTDOWN NOSAVE，结束时回收测试进程。

```powershell
$env:GOLITUBE_TEST_REDIS_RESTART = '1'
go test -race -v -run '^TestRedisRestartRecovery$' -timeout 60s .
```

本次执行将 GOTMPDIR、TEMP、TMP、GOCACHE 临时指向 D 盘，测试配置路径也位于 D 盘，可通过 WSL 的 `/mnt/d/` 访问。若本机临时目录存在 WSL 访问问题，可按以下方式运行；这些环境变量仅影响当前终端：

```powershell
$buildRoot = 'D:\codex-temp\golitube-go'
New-Item -ItemType Directory -Force -Path "$buildRoot\work", "$buildRoot\cache" | Out-Null
$env:GOTMPDIR = "$buildRoot\work"
$env:TEMP = "$buildRoot\work"
$env:TMP = "$buildRoot\work"
$env:GOCACHE = "$buildRoot\cache"
```

## 本次结果

测试通过，启用了竞态检测。本次专用 Redis 端口为 52809，Go 实例端口为 51777 和 51783；端口不作为下次执行的固定配置。

| 阶段 | 操作 | 实际观察 |
| --- | --- | --- |
| 正常 | A、B 分别连接两个实例的同名房间 | 跨实例收发通过 |
| 停服 | 关闭专用 Redis，A 发送 `during-failure` | 发布失败，A 的 WebSocket 被关闭，成员清理；没有直接本地回声 |
| 重启 | 在原端口启动新的 Redis 进程，保持两个 Go 进程运行 | Redis 重新出现两个频道订阅者 |
| 恢复 | A 重新连接；B 保留原连接 | `after-recovery` 和反向消息均被双方收到，未读到停服期间消息 |

关键输出：

```text
正常阶段：两个独立实例跨实例收发通过
停服阶段：发布失败后发送连接关闭
恢复阶段：Go 进程未重启，A 重连、B 保持原连接，双向广播恢复；未收到故障期间消息
--- PASS: TestRedisRestartRecovery (10.81s)
```

## 结论边界

证明了本次环境中单次正常停服、重启后的重新订阅和业务收发恢复；恢复依赖当前客户端库的重连能力。它不证明 Redis 高可用、所有网络分区都可恢复、持久化、离线补发、去重或 exactly-once。没有覆盖黑洞网络、长时间中断、重复抖动、积压压力等情况。

本次只增加实验测试与说明，没有调整业务容错策略。故障期间发布失败与“消息已发布但某订阅者离线”是不同情况，仍需分别评估。
