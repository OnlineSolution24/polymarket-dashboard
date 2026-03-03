"""
Security & Setup — Bot connection status and setup info.
"""

import streamlit as st

from services.bot_api_client import get_bot_client


def render():
    st.header("Security & Setup")

    # --- Bot Connection Status ---
    st.subheader("Bot-Verbindung")

    client = get_bot_client()
    reachable = client.is_reachable()

    if reachable:
        st.success("Bot API erreichbar")
        status = client.get_status()
        if status:
            st.markdown(f"- **Trading Mode:** {status.get('trading_mode', '?')}")
            st.markdown(f"- **Aktive Agents:** {status.get('active_agents', 0)}")
            st.markdown(f"- **Bot pausiert:** {'Ja' if status.get('bot_paused') else 'Nein'}")
            st.markdown(f"- **Timestamp:** {status.get('timestamp', '?')}")
    else:
        st.error("Bot API nicht erreichbar!")
        st.markdown("""
        **Mögliche Ursachen:**
        - Bot-Container läuft nicht
        - Falsche BOT_API_URL in der .env
        - Falscher BOT_API_KEY
        - Firewall blockiert die Verbindung
        """)

    st.divider()

    # --- Setup Info ---
    st.subheader("Setup-Anleitung")

    st.markdown("""
    ### Dashboard .env Konfiguration

    Das Dashboard benötigt nur 3 Umgebungsvariablen:

    ```
    APP_PASSWORD=dein_passwort
    BOT_API_URL=https://bot.deinedomain.de
    BOT_API_KEY=dein_api_key_vom_bot
    ```

    ### Bot VPS

    Der Bot läuft unabhängig auf dem VPS und stellt eine REST API bereit.
    Alle Secrets (Polymarket Keys, Telegram API, etc.) liegen nur auf dem Bot.

    ### Nützliche Befehle (Bot VPS)

    ```bash
    # Bot Logs
    docker compose -f docker-compose.bot.yml logs -f

    # Bot Neustart
    docker compose -f docker-compose.bot.yml restart

    # Bot stoppen
    docker compose -f docker-compose.bot.yml down

    # .env bearbeiten
    nano /opt/polymarket-bot/.env

    # Config bearbeiten
    nano /opt/polymarket-bot/platform_config.yaml
    ```

    ### Nützliche Befehle (Dashboard)

    ```bash
    # Dashboard Logs
    docker compose -f docker-compose.dashboard.yml logs -f

    # Dashboard Neustart
    docker compose -f docker-compose.dashboard.yml restart
    ```
    """)

    st.divider()

    # --- VPS Hardening Script ---
    st.subheader("VPS Härtungs-Script")
    st.markdown("Generiert ein Bash-Script zum Härten des VPS.")

    if st.button("Script generieren", type="primary"):
        script = _generate_hardening_script()
        st.code(script, language="bash")
        st.download_button(
            "Script herunterladen",
            data=script,
            file_name="harden_vps.sh",
            mime="text/x-shellscript",
        )


def _generate_hardening_script() -> str:
    return '''#!/bin/bash
# === Polymarket VPS Hardening Script ===
# Run as root: sudo bash harden_vps.sh

set -euo pipefail

echo "=== VPS Hardening ==="

# 1. System Updates
echo "[1/6] System-Updates..."
apt update && apt upgrade -y

# 2. Disable SSH Root Login
echo "[2/6] SSH härten..."
sed -i 's/^#*PermitRootLogin.*/PermitRootLogin no/' /etc/ssh/sshd_config
sed -i 's/^#*PasswordAuthentication.*/PasswordAuthentication no/' /etc/ssh/sshd_config
systemctl restart sshd

# 3. UFW Firewall
echo "[3/6] Firewall konfigurieren..."
apt install -y ufw
ufw default deny incoming
ufw default allow outgoing
ufw allow ssh
ufw allow 80/tcp
ufw allow 443/tcp
echo "y" | ufw enable

# 4. Fail2Ban
echo "[4/6] Fail2Ban installieren..."
apt install -y fail2ban
systemctl enable fail2ban
systemctl start fail2ban

# 5. Unattended Upgrades
echo "[5/6] Auto-Updates aktivieren..."
apt install -y unattended-upgrades
dpkg-reconfigure -plow unattended-upgrades

# 6. Swap (falls nicht vorhanden)
echo "[6/6] Swap prüfen..."
if [ ! -f /swapfile ]; then
    fallocate -l 2G /swapfile
    chmod 600 /swapfile
    mkswap /swapfile
    swapon /swapfile
    echo "/swapfile swap swap defaults 0 0" >> /etc/fstab
fi

echo "=== Hardening abgeschlossen! ==="
echo "Wichtig: Stelle sicher dass du einen SSH-Key hast bevor du dich ausloggst!"
'''
