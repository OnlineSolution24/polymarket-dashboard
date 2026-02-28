#!/bin/bash
# ============================================
# Polymarket Dashboard — Deploy Script
# ============================================
# Run on your dashboard server: bash deploy-dashboard.sh

set -e

echo "============================================"
echo "  Polymarket Dashboard — Deploy"
echo "============================================"

REPO="https://github.com/OnlineSolution24/polymarket-dashboard.git"
APP_DIR="/opt/polymarket-dashboard"
DOMAIN=""
BOT_API_URL=""
BOT_API_KEY=""
APP_PASSWORD=""

# --- Ask for config ---
read -p "Dashboard Domain (z.B. dashboard.deinedomain.de): " DOMAIN
read -p "Bot API URL (z.B. https://bot.deinedomain.de): " BOT_API_URL
read -p "Bot API Key (vom Bot-Deployment): " BOT_API_KEY
read -p "Dashboard Passwort: " APP_PASSWORD

if [ -z "$DOMAIN" ] || [ -z "$BOT_API_URL" ] || [ -z "$BOT_API_KEY" ] || [ -z "$APP_PASSWORD" ]; then
    echo "Alle Felder sind erforderlich!"
    exit 1
fi

# --- 1. Clone/Update ---
echo ""
echo "[1/6] Repository klonen..."
if [ -d "$APP_DIR" ]; then
    cd "$APP_DIR" && git pull origin main
else
    git clone "$REPO" "$APP_DIR"
    cd "$APP_DIR"
fi

# --- 2. Create .env ---
echo ""
echo "[2/6] .env erstellen..."
cat > "$APP_DIR/.env" <<EOF
APP_PASSWORD=$APP_PASSWORD
BOT_API_URL=$BOT_API_URL
BOT_API_KEY=$BOT_API_KEY
EOF
echo ".env erstellt"

# --- 3. Nginx + SSL ---
echo ""
echo "[3/6] Nginx + SSL..."
if ! command -v nginx &> /dev/null; then
    apt-get update && apt-get install -y nginx certbot python3-certbot-nginx
fi

if [ ! -d "/etc/letsencrypt/live/$DOMAIN" ]; then
    certbot certonly --standalone -d "$DOMAIN" --non-interactive --agree-tos --email admin@$DOMAIN || {
        echo "SSL fehlgeschlagen. Weiter ohne SSL? (y/n)"
        read -p "> " SKIP_SSL
        if [ "$SKIP_SSL" != "y" ]; then exit 1; fi
    }
fi

# --- 4. Nginx Config ---
echo ""
echo "[4/6] Nginx konfigurieren..."
NGINX_CONF="/etc/nginx/sites-available/polymarket-dashboard"
cp "$APP_DIR/nginx/polymarket-dashboard.conf" "$NGINX_CONF"
sed -i "s|YOUR_DOMAIN|$DOMAIN|g" "$NGINX_CONF"
ln -sf "$NGINX_CONF" /etc/nginx/sites-enabled/polymarket-dashboard
nginx -t && systemctl reload nginx

# --- 5. Start Dashboard ---
echo ""
echo "[5/6] Dashboard starten..."
cd "$APP_DIR"
docker compose -f docker-compose.dashboard.yml up -d --build

# --- 6. Verify ---
echo ""
echo "[6/6] Verifizierung..."
sleep 5
if curl -s http://localhost:8501/_stcore/health > /dev/null 2>&1; then
    echo "✅ Dashboard erreichbar"
else
    echo "⚠️  Dashboard noch nicht bereit. Prüfe Logs:"
    echo "   docker compose -f docker-compose.dashboard.yml logs -f"
fi

echo ""
echo "============================================"
echo "  ✅ DASHBOARD DEPLOYMENT ABGESCHLOSSEN"
echo "============================================"
echo ""
echo "  URL: https://$DOMAIN"
echo "  Passwort: $APP_PASSWORD"
echo ""
echo "  Nützliche Befehle:"
echo "    Logs:     docker compose -f docker-compose.dashboard.yml logs -f"
echo "    Restart:  docker compose -f docker-compose.dashboard.yml restart"
echo "============================================"
