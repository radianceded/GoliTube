#!/bin/bash
set -euo pipefail
test "$(id -u)" = 0
source_dir=$(cd "$(dirname "$0")/.." && pwd)
apt-get update -qq
DEBIAN_FRONTEND=noninteractive apt-get install -y -qq docker.io docker-compose-v2 redis-server nginx
id golitube >/dev/null 2>&1 || useradd --system --home /opt/golitube --shell /usr/sbin/nologin golitube
id golitube-deploy >/dev/null 2>&1 || useradd --create-home --home-dir /var/lib/golitube-deploy --shell /bin/bash golitube-deploy
install -d -m 755 /opt/golitube/releases /opt/golitube/deploy
install -d -m 700 /etc/golitube
install -d -o golitube-deploy -g golitube-deploy -m 700 /var/lib/golitube-deploy/uploads /var/lib/golitube-deploy/.ssh
install -m 644 "$source_dir/compose.yaml" /opt/golitube/compose.yaml
install -m 644 "$source_dir/deploy/compose.yaml" /opt/golitube/deploy/compose.yaml
install -m 644 "$source_dir"/deploy/golitube-*.service /etc/systemd/system/
install -m 755 "$source_dir/deploy/deploy.sh" /usr/local/sbin/golitube-deploy
install -m 755 "$source_dir/deploy/release-command.sh" /usr/local/bin/golitube-release-command
printf '%s\n' 'golitube-deploy ALL=(root) NOPASSWD: /usr/local/sbin/golitube-deploy *' > /etc/sudoers.d/golitube-deploy
chmod 440 /etc/sudoers.d/golitube-deploy
visudo -cf /etc/sudoers.d/golitube-deploy

# 凭据在服务器上生成，重跑不会改变已有数据库密码。
python3 - <<'PY'
from pathlib import Path
import secrets

dependencies = Path('/etc/golitube/dependencies.env')
if not dependencies.exists():
    dependencies.write_text('MYSQL_ROOT_PASSWORD=' + secrets.token_hex(24) + '\nMYSQL_PASSWORD=' + secrets.token_hex(24) + '\n')
dependencies.chmod(0o600)
values = dict(line.split('=', 1) for line in dependencies.read_text().splitlines())
application = Path('/etc/golitube/app.env')
if not application.exists():
    application.write_text('GOLITUBE_MYSQL_DSN=golitube:' + values['MYSQL_PASSWORD'] + '@tcp(127.0.0.1:13306)/golitube\nGOLITUBE_COOKIE_SECURE=0\nGIN_MODE=release\n')
application.chmod(0o600)
if 'GOLITUBE_TRUST_LOOPBACK_PROXY=' not in application.read_text():
    with application.open('a') as output:
        output.write('GOLITUBE_TRUST_LOOPBACK_PROXY=1\n')
PY
if ! grep -q '# GoliTube persistence' /etc/redis/redis.conf; then
    cat >> /etc/redis/redis.conf <<'CONF'

# GoliTube persistence
appendonly yes
appendfsync everysec
maxmemory 256mb
maxmemory-policy noeviction
bind 127.0.0.1 ::1
CONF
fi
systemctl enable --now docker redis-server nginx
systemctl restart redis-server
docker compose --env-file /etc/golitube/dependencies.env -f /opt/golitube/deploy/compose.yaml up -d --wait kafka mysql
docker compose --env-file /etc/golitube/dependencies.env -f /opt/golitube/deploy/compose.yaml run --rm kafka-init
if [[ ! -f /etc/nginx/sites-available/golitube ]]; then
    install -m 644 "$source_dir/deploy/nginx.conf" /etc/nginx/sites-available/golitube
fi
ln -sfn /etc/nginx/sites-available/golitube /etc/nginx/sites-enabled/golitube
if [[ -L /etc/nginx/sites-enabled/default ]]; then
    unlink /etc/nginx/sites-enabled/default
fi
nginx -t
systemctl reload nginx
systemctl daemon-reload
systemctl enable golitube-server golitube-persist
echo 'Runtime installed. Add the restricted deployment public key, then upload and deploy a release.'
