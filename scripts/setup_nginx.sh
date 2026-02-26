#!/bin/bash
# === Nginx + SSL Setup for Polymarket Dashboard ===
# Usage: sudo bash setup_nginx.sh YOUR_DOMAIN

set -euo pipefail

DOMAIN="${1:?Usage: setup_nginx.sh YOUR_DOMAIN}"

echo "=== Nginx Setup für ${DOMAIN} ==="

# 1. Install Nginx
echo "[1/4] Nginx installieren..."
apt install -y nginx

# 2. Install Certbot
echo "[2/4] Certbot installieren..."
apt install -y certbot python3-certbot-nginx

# 3. Copy and configure Nginx config
echo "[3/4] Nginx konfigurieren..."
CONF_SRC="$(dirname "$0")/../nginx/polymarket-dashboard.conf"
CONF_DST="/etc/nginx/sites-available/polymarket-dashboard"

cp "$CONF_SRC" "$CONF_DST"
sed -i "s/YOUR_DOMAIN/${DOMAIN}/g" "$CONF_DST"

ln -sf "$CONF_DST" /etc/nginx/sites-enabled/polymarket-dashboard
rm -f /etc/nginx/sites-enabled/default

nginx -t && systemctl reload nginx

# 4. Get SSL certificate
echo "[4/4] SSL-Zertifikat holen..."
certbot --nginx -d "${DOMAIN}" --non-interactive --agree-tos --register-unsafely-without-email

echo "=== Setup abgeschlossen ==="
echo "Dashboard erreichbar unter: https://${DOMAIN}"
