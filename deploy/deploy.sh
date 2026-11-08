#!/bin/bash
set -euo pipefail

[[ $# == 1 && $1 =~ ^[a-f0-9]{40}$ ]] || exit 2
sha=$1
exec 9>/run/lock/golitube-deploy.lock
flock -w 300 9
base=/opt/golitube
release=$base/releases/$sha
upload=/var/lib/golitube-deploy/uploads/$sha
test -f "$upload"
test ! -L "$upload"
previous=$(readlink -f "$base/current" || true)
mkdir -p "$release"
if [[ -f $release/golitube ]]; then
    cmp "$upload" "$release/golitube" # 同一版本的程序不可被不同内容覆盖。
else
    install -o root -g root -m 755 "$upload" "$release/golitube"
fi

rollback() {
    echo "Deployment failed, restoring previous application" >&2
    if [[ -n $previous && -f $previous/golitube ]]; then
        ln -sfn "$previous" "$base/.current-next"
        mv -Tf "$base/.current-next" "$base/current"
        systemctl restart golitube-server golitube-persist
    else
        systemctl stop golitube-server golitube-persist || true
    fi
}
trap rollback ERR
systemctl stop golitube-persist
ln -sfn "$release" "$base/.current-next"
mv -Tf "$base/.current-next" "$base/current"
systemctl reset-failed golitube-migrate || true
systemctl start golitube-migrate
systemctl restart golitube-server golitube-persist
healthy=false
for attempt in {1..30}; do
    if curl -fsS --max-time 3 http://127.0.0.1:8081/health >/dev/null &&
       curl -fsS --max-time 3 'http://127.0.0.1:8081/danmaku/history?room=__deploy_probe__&start_ms=0&end_ms=1' >/dev/null &&
       curl -fsS --max-time 3 'http://127.0.0.1:8081/rooms/online?room=__deploy_probe__' >/dev/null &&
       systemctl is-active --quiet golitube-persist; then
        healthy=true
        break
    fi
    sleep 2
done
test "$healthy" = true
trap - ERR
rm -f "$upload"
echo "Deployed $sha"
