#!/bin/bash
# ============================================
# Polymarket Trading Bot — VPS Deploy Script
# ============================================
# Run on your VPS: bash deploy-bot.sh
#
# Prerequisites:
#   - Ubuntu 22.04+ with Docker installed
#   - Domain pointing to this VPS IP
#   - Git installed

set -e

echo "============================================"
echo "  Polymarket Trading Bot — Deploy"
echo "============================================"

# --- Config ---
REPO="https://github.com/OnlineSolution24/polymarket-dashboard.git"
APP_DIR="/opt/polymarket-bot"
DOMAIN=""  # Will be asked

# --- Ask for domain ---
read -p "Bot-API Domain (z.B. bot.deinedomain.de): " DOMAIN
if [ -z "$DOMAIN" ]; then
    echo "Domain ist erforderlich!"
    exit 1
fi

# --- 1. Clone/Update repo ---
echo ""
echo "[1/7] Repository klonen..."
if [ -d "$APP_DIR" ]; then
    cd "$APP_DIR" && git pull origin main
else
    git clone "$REPO" "$APP_DIR"
    cd "$APP_DIR"
fi

# --- 2. Create .env if not exists ---
echo ""
echo "[2/7] Umgebungsvariablen..."
if [ ! -f "$APP_DIR/.env" ]; then
    cp "$APP_DIR/.env.bot.example" "$APP_DIR/.env"

    # Generate API key
    API_KEY=$(python3 -c "import secrets; print(secrets.token_urlsafe(32))" 2>/dev/null || openssl rand -base64 32)
    sed -i "s|^BOT_API_KEY=.*|BOT_API_KEY=$API_KEY|" "$APP_DIR/.env"

    echo ""
    echo "================================================"
    echo "  WICHTIG: .env Datei wurde erstellt!"
    echo "  Dein BOT_API_KEY ist: $API_KEY"
    echo "  Speichere diesen Key — du brauchst ihn im Dashboard!"
    echo ""
    echo "  Bearbeite jetzt die .env:"
    echo "  nano $APP_DIR/.env"
    echo "================================================"
    echo ""
    read -p "Hast du die .env bearbeitet? (Enter zum Fortfahren) "
else
    echo ".env existiert bereits."
fi

# --- 3. Install Nginx + Certbot ---
echo ""
echo "[3/7] Nginx + SSL Setup..."
if ! command -v nginx &> /dev/null; then
    apt-get update && apt-get install -y nginx certbot python3-certbot-nginx
fi

# --- 4. SSL Certificate ---
echo ""
echo "[4/7] SSL Zertifikat für $DOMAIN..."
if [ ! -d "/etc/letsencrypt/live/$DOMAIN" ]; then
    certbot certonly --standalone -d "$DOMAIN" --non-interactive --agree-tos --email admin@$DOMAIN || {
        echo "SSL fehlgeschlagen. Stelle sicher, dass $DOMAIN auf diese IP zeigt."
        echo "Weiter ohne SSL? (y/n)"
        read -p "> " SKIP_SSL
        if [ "$SKIP_SSL" != "y" ]; then exit 1; fi
    }
fi

# --- 5. Nginx Config ---
echo ""
echo "[5/7] Nginx konfigurieren..."
NGINX_CONF="/etc/nginx/sites-available/bot-api"
cp "$APP_DIR/nginx/bot-api.conf" "$NGINX_CONF"
sed -i "s|YOUR_BOT_DOMAIN|$DOMAIN|g" "$NGINX_CONF"

# Remove rate limit zone if already defined
if ! grep -q "limit_req_zone.*zone=api" /etc/nginx/nginx.conf 2>/dev/null; then
    sed -i '/http {/a \    limit_req_zone $binary_remote_addr zone=api:10m rate=30r/m;' /etc/nginx/nginx.conf 2>/dev/null || true
fi
# Remove inline limit_req_zone from site config (it belongs in nginx.conf http block)
sed -i '/limit_req_zone/d' "$NGINX_CONF"

ln -sf "$NGINX_CONF" /etc/nginx/sites-enabled/bot-api
nginx -t && systemctl reload nginx
echo "Nginx OK"

# --- 6. Start Bot ---
echo ""
echo "[6/7] Bot starten..."
cd "$APP_DIR"
docker compose -f docker-compose.bot.yml up -d --build
echo "Bot Container gestartet"

# --- 7. Verify ---
echo ""
echo "[7/7] Verifizierung..."
sleep 5
if curl -s http://localhost:8000/api/docs > /dev/null 2>&1; then
    echo "✅ Bot API erreichbar auf localhost:8000"
else
    echo "⚠️  Bot API noch nicht erreichbar. Prüfe Logs:"
    echo "   docker compose -f docker-compose.bot.yml logs -f"
fi

API_KEY=$(grep "^BOT_API_KEY=" "$APP_DIR/.env" | cut -d'=' -f2)

echo ""
echo "============================================"
echo "  ✅ DEPLOYMENT ABGESCHLOSSEN"
echo "============================================"
echo ""
echo "  Bot API: https://$DOMAIN/api/status"
echo "  API Key: $API_KEY"
echo ""
echo "  Für das Dashboard brauchst du:"
echo "    BOT_API_URL=https://$DOMAIN"
echo "    BOT_API_KEY=$API_KEY"
echo ""
echo "  Nützliche Befehle:"
echo "    Logs:     docker compose -f docker-compose.bot.yml logs -f"
echo "    Restart:  docker compose -f docker-compose.bot.yml restart"
echo "    Stop:     docker compose -f docker-compose.bot.yml down"
echo "============================================"
