#!/bin/bash
set -euo pipefail
test "$(id -u)" = 0
[[ $# == 1 && $1 =~ ^[a-z0-9][a-z0-9.-]*\.[a-z]{2,}$ ]] || exit 2
domain=$1
source_dir=$(cd "$(dirname "$0")" && pwd)
DEBIAN_FRONTEND=noninteractive apt-get install -y -qq certbot
install -d -m 755 /var/www/golitube-acme

# 首次签发需要 HTTP 验证路径。重跑时保留已经生效的 TLS 配置。
if ! grep -q 'listen 443 ssl' /etc/nginx/sites-available/golitube; then
    install -m 644 "$source_dir/nginx.conf" /etc/nginx/sites-available/golitube
    nginx -t
    systemctl reload nginx
fi
certbot certonly --non-interactive --agree-tos --register-unsafely-without-email \
    --webroot -w /var/www/golitube-acme -d "$domain" -d "www.$domain" --keep-until-expiring
config=/etc/nginx/sites-available/golitube
cp "$config" "$config.before-https"
temporary=$(mktemp /etc/nginx/sites-available/.golitube.XXXXXX)
trap 'rm -f "$temporary"' EXIT
cat > "$temporary" <<CONF
server {
    listen 80 default_server;
    listen [::]:80 default_server;
    server_name $domain www.$domain;
    location ^~ /.well-known/acme-challenge/ {
        root /var/www/golitube-acme;
    }
    location / {
        return 301 https://$domain\$request_uri;
    }
}
server {
    listen 443 ssl default_server;
    listen [::]:443 ssl default_server;
    server_name $domain www.$domain;
    ssl_certificate /etc/letsencrypt/live/$domain/fullchain.pem;
    ssl_certificate_key /etc/letsencrypt/live/$domain/privkey.pem;
    ssl_protocols TLSv1.2 TLSv1.3;
    client_max_body_size 8k;
    location / {
        proxy_pass http://127.0.0.1:8081;
        proxy_http_version 1.1;
        proxy_set_header Host \$http_host;
        proxy_set_header X-Real-IP \$remote_addr;
        proxy_set_header Upgrade \$http_upgrade;
        proxy_set_header Connection "upgrade";
        proxy_read_timeout 90s;
        proxy_send_timeout 90s;
        proxy_buffering off;
    }
}
CONF
chmod 644 "$temporary"
mv -f "$temporary" "$config"
if ! nginx -t; then
    cp "$config.before-https" "$config"
    exit 1
fi
systemctl reload nginx
sed -i 's/^GOLITUBE_COOKIE_SECURE=.*/GOLITUBE_COOKIE_SECURE=1/' /etc/golitube/app.env
systemctl restart golitube-server
install -d /etc/letsencrypt/renewal-hooks/deploy
printf '#!/bin/sh\nset -e\nsystemctl reload nginx\n' > /etc/letsencrypt/renewal-hooks/deploy/golitube-nginx
chmod 755 /etc/letsencrypt/renewal-hooks/deploy/golitube-nginx
systemctl enable --now certbot.timer
echo "HTTPS enabled for $domain and www.$domain"
