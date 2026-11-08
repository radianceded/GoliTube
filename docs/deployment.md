# Ubuntu 单机部署与自动发布

## 运行布局

适用于 Ubuntu 24.04、Linux amd64。Go server 和 persist 由 systemd 管理，Nginx 转发 HTTP/WebSocket，MySQL 与 Kafka 使用生产 Compose 文件，Redis 使用 Ubuntu 软件包。

- `/opt/golitube/releases/<commit SHA>/golitube` 保存各版本程序，`current` 指向当前版本。
- `/etc/golitube/app.env` 保存应用配置，`dependencies.env` 保存随机生成的数据库密码，均由 root 管理，权限 600。
- MySQL 和 Kafka 使用 Docker 命名卷，端口绑定 `127.0.0.1`。Redis 也只监听本机，使用 AOF、每秒 fsync 和 `noeviction`，内存上限 256 MB。
- 安装脚本设置 `GOLITUBE_TRUST_LOOPBACK_PROXY=1`。账号限流只在直连来源为回环地址时接受 Nginx 覆盖写入的 `X-Real-IP`，Go 监听地址保持 `127.0.0.1:8081`。直接来自外部的转发头不会被信任。
- Redis 达到内存上限时可能拒绝新增写入，应用会返回不可用。AOF 每秒同步仍存在故障时丢失最近写入的窗口，备份与恢复演练需要另行安排。[Redis 持久化说明](https://redis.io/docs/latest/operate/oss_and_stack/management/persistence/)
- Kafka 每分区日志保留目标设为 256 MiB，时间上限 7 天。清理按日志段进行，实际磁盘占用可能超过该目标。消费者长时间落后可能超过保留窗口。当前单节点配置用于演示，容量未压测。

## 首次安装

以下命令使用 Bash，可在 Linux 或 WSL 中执行。先确认 SSH 可登录目标主机，且主机指纹与已核实的服务器一致。

```bash
HOST=your-server-ip
ssh ubuntu@"$HOST" 'mkdir -p /home/ubuntu/golitube-src'
scp compose.yaml ubuntu@"$HOST":/home/ubuntu/golitube-src/
scp -r deploy ubuntu@"$HOST":/home/ubuntu/golitube-src/
ssh ubuntu@"$HOST" 'sudo bash /home/ubuntu/golitube-src/deploy/install.sh'
```

安装脚本用于这台专用服务器，会配置 Nginx 默认站点、Redis 和 systemd 服务。它复用根目录的 Compose 定义，增加生产密码、资源限制、重启策略和日志轮转。重跑时保留已生成的密码。

中国大陆服务器访问 Docker Hub 失败时，可从可访问的镜像源拉取对应版本并核对摘要，再标记为 `mysql:8.4` 与 `apache/kafka:4.1.0`。安装过程使用本机已有镜像。

## 受限部署密钥

为 Actions 创建独立密钥，部署账号只接受 `upload <40位SHA>`、`upload-gzip <40位SHA>` 和 `deploy <40位SHA>`。该账号通过 root 管理的发布脚本安装程序及重启服务，上传内容最终以 `golitube` 运行账号执行。

```bash
ssh-keygen -t ed25519 -f ~/.ssh/golitube-deploy -N ''
{
  printf 'restrict,command="/usr/local/bin/golitube-release-command" '
  cat ~/.ssh/golitube-deploy.pub
} | ssh ubuntu@"$HOST" 'sudo tee /var/lib/golitube-deploy/.ssh/authorized_keys >/dev/null; sudo chown golitube-deploy:golitube-deploy /var/lib/golitube-deploy/.ssh/authorized_keys; sudo chmod 600 /var/lib/golitube-deploy/.ssh/authorized_keys'
```

上传与发布入口只接受十六进制提交号，部署加文件锁，限制解压后程序大小为 100 MiB。同一提交号的程序内容不可被不同内容覆盖。保留版本目录便于回滚，需要按磁盘情况清理旧版本。Actions 移除调试符号并使用 gzip 传输，以缩短跨境 SSH 链路的上传时间，原始上传入口继续兼容手动发布。

## GitHub Actions

在仓库 Actions Secrets 中配置三个值。

| 名称 | 内容 |
| --- | --- |
| `GOLITUBE_DEPLOY_HOST` | 服务器公网 IP 或主机名 |
| `GOLITUBE_DEPLOY_KEY` | 上述专用部署私钥 |
| `GOLITUBE_SSH_HOST_KEY` | 已核实的 OpenSSH known_hosts 条目 |

例如可通过 GitHub CLI 从文件读取密钥，避免在命令参数中直接展开。Secrets 的配置方式见官方文档。[GitHub Actions Secrets](https://docs.github.com/en/actions/how-tos/write-workflows/choose-what-workflows-do/use-secrets)

```bash
gh secret set GOLITUBE_DEPLOY_HOST --body "$HOST"
gh secret set GOLITUBE_DEPLOY_KEY < ~/.ssh/golitube-deploy
ssh-keygen -F "$HOST" | sed '/^#/d' | gh secret set GOLITUBE_SSH_HOST_KEY
```

`.github/workflows/deploy.yml` 在 PR 中运行真实 Redis、MySQL、Kafka 集成测试、竞态检测、vet、脚本语法检查和 Linux 编译。main 收到 push 或手动触发时，测试通过后通过 SSH 上传可执行文件，再发布同一提交号。Secrets 只在 main 的部署步骤使用，SSH 启用严格主机指纹校验。

本地 `git commit` 后推送分支并合并 PR，才会触发 main 的自动部署。首次加入 workflow 也需要合并到 main 后启用。应用程序与内嵌前端一起编译。Actions 构建与部署由并发组串行管理，服务器再通过文件锁避免发布交错。

基础设施配置变更由管理员审核后重新运行安装脚本，发布密钥的权限范围覆盖程序上传及发布。部署期间先完整写入临时程序，再原子重命名到最终文件，复制中断后可重试。

域名 HTTPS 启用后，设置仓库 Actions Variable `GOLITUBE_PUBLIC_URL`，例如 `https://your-domain.cn`。部署后 runner 会再次检查公网 `/health` 的证书与响应，失败时 workflow 标记失败，便于发现云防火墙或公网入口问题。

## 发布与回滚

发布先暂停旧 persist，原子切换 current，执行已有 `migrate-history` 初始化及回填，再重启 server 与 persist。初始化或检查失败时恢复上一个程序版本。检查覆盖 `/health`、一次直接查库的历史请求、Redis 在线统计和 persist 进程状态。

当前迁移包含历史归档全量扫描，随着数据增长会延长发布窗口，超时上限为 300 秒。结构变化需要保持前后版本兼容。应用回滚无法撤回数据库结构与数据变化。单实例重启会短暂中断 WebSocket，网页自动重连。

首次手动发布同样使用受限入口，以下仍为 Bash 命令。

```bash
CGO_ENABLED=0 GOOS=linux GOARCH=amd64 go build -trimpath -buildvcs=false -ldflags="-s -w" -o build/golitube .
SHA=$(git rev-parse HEAD)
gzip -1 -c build/golitube | ssh -i ~/.ssh/golitube-deploy golitube-deploy@"$HOST" "upload-gzip $SHA"
ssh -i ~/.ssh/golitube-deploy golitube-deploy@"$HOST" "deploy $SHA"
```

常用运维命令在服务器执行。

```bash
systemctl status golitube-server golitube-persist
sudo journalctl -u golitube-server -u golitube-persist -n 100 --no-pager
sudo readlink -f /opt/golitube/current
curl -f http://127.0.0.1:8081/health
sudo docker compose --env-file /etc/golitube/dependencies.env -f /opt/golitube/deploy/compose.yaml ps
```

## 公网入口

初始 Nginx 配置提供 80 端口的 HTTP 验收入口。云防火墙需要允许该端口。SSH 及数据库端口的访问策略应按运维需要配置，数据库与消息队列当前仅绑定本机。

域名解析与备案确认完成后，可上传新版 deploy 目录，在服务器执行以下命令。云防火墙需要允许 80 和 443 端口。

```bash
sudo bash /home/ubuntu/golitube-src/deploy/enable-https.sh your-domain.cn
```

脚本通过 Let's Encrypt 为根域名与 www 签发证书，配置 HTTP 跳转 HTTPS、WebSocket 代理、证书续期定时器和续期后重载 Nginx，同时将 `GOLITUBE_COOKIE_SECURE` 改为 `1` 并重启 server。首次签发采用无通知邮箱的 ACME 账户，后续可由管理员补充账户通知邮箱。重新运行安装脚本保留已有 TLS 站点配置。

HTTPS 配置前，初始 HTTP 入口的浏览器流量为明文，正式账号使用应在 HTTPS 配置完成后进行。续期可通过以下命令演练。

```bash
sudo certbot renew --dry-run
systemctl status certbot.timer
```
