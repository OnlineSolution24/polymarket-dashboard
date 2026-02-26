"""
Tab 1: Security & Setup
Shows VPS security status and provides hardening scripts.
"""

import streamlit as st
import subprocess
import platform


def render():
    st.header("Security & Setup")

    st.markdown("""
    Überprüfe den Sicherheitsstatus deines VPS und generiere Härtungs-Skripte.
    """)

    # --- Current Status ---
    st.subheader("Aktueller Status")

    is_linux = platform.system() == "Linux"

    if is_linux:
        checks = _run_security_checks()
    else:
        checks = _mock_security_checks()
        st.info("Security-Checks sind nur auf Linux/VPS verfügbar. Zeige Mock-Daten.")

    col1, col2 = st.columns(2)

    with col1:
        for check in checks[:len(checks)//2 + 1]:
            icon = "🟢" if check["ok"] else "🔴"
            st.markdown(f"{icon} **{check['name']}**: {check['status']}")

    with col2:
        for check in checks[len(checks)//2 + 1:]:
            icon = "🟢" if check["ok"] else "🔴"
            st.markdown(f"{icon} **{check['name']}**: {check['status']}")

    st.divider()

    # --- Hardening Script ---
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

    st.divider()

    # --- OpenClaw Status ---
    st.subheader("OpenClaw Verbindung")

    from config import AppConfig
    config = AppConfig.from_env()

    checks_openclaw = [
        ("Telegram API ID", bool(config.telegram_api_id)),
        ("Telegram API Hash", bool(config.telegram_api_hash)),
        ("OpenClaw Chat ID", bool(config.openclaw_chat_id)),
        ("Alert User ID", bool(config.alert_telegram_user_id)),
    ]

    for name, configured in checks_openclaw:
        icon = "🟢" if configured else "🔴"
        status = "Konfiguriert" if configured else "Nicht gesetzt"
        st.markdown(f"{icon} **{name}**: {status}")


def _run_security_checks() -> list[dict]:
    """Run actual security checks on Linux."""
    checks = []

    # SSH root login
    try:
        result = subprocess.run(
            ["grep", "-i", "^PermitRootLogin", "/etc/ssh/sshd_config"],
            capture_output=True, text=True, timeout=5,
        )
        no_root = "no" in result.stdout.lower()
        checks.append({"name": "SSH Root Login", "ok": no_root, "status": "Deaktiviert" if no_root else "Aktiviert!"})
    except Exception:
        checks.append({"name": "SSH Root Login", "ok": False, "status": "Prüfung fehlgeschlagen"})

    # Firewall
    try:
        result = subprocess.run(["ufw", "status"], capture_output=True, text=True, timeout=5)
        active = "active" in result.stdout.lower()
        checks.append({"name": "UFW Firewall", "ok": active, "status": "Aktiv" if active else "Inaktiv!"})
    except Exception:
        checks.append({"name": "UFW Firewall", "ok": False, "status": "Nicht installiert"})

    # Fail2Ban
    try:
        result = subprocess.run(["systemctl", "is-active", "fail2ban"], capture_output=True, text=True, timeout=5)
        active = "active" in result.stdout.strip()
        checks.append({"name": "Fail2Ban", "ok": active, "status": "Aktiv" if active else "Inaktiv!"})
    except Exception:
        checks.append({"name": "Fail2Ban", "ok": False, "status": "Nicht installiert"})

    # Unattended upgrades
    try:
        result = subprocess.run(
            ["dpkg", "-l", "unattended-upgrades"],
            capture_output=True, text=True, timeout=5,
        )
        installed = "ii" in result.stdout
        checks.append({"name": "Auto-Updates", "ok": installed, "status": "Aktiv" if installed else "Nicht installiert"})
    except Exception:
        checks.append({"name": "Auto-Updates", "ok": False, "status": "Prüfung fehlgeschlagen"})

    return checks


def _mock_security_checks() -> list[dict]:
    """Mock security checks for non-Linux systems."""
    return [
        {"name": "SSH Root Login", "ok": False, "status": "Nur auf VPS prüfbar"},
        {"name": "UFW Firewall", "ok": False, "status": "Nur auf VPS prüfbar"},
        {"name": "Fail2Ban", "ok": False, "status": "Nur auf VPS prüfbar"},
        {"name": "Auto-Updates", "ok": False, "status": "Nur auf VPS prüfbar"},
    ]


def _generate_hardening_script() -> str:
    return '''#!/bin/bash
# === Polymarket Dashboard VPS Hardening Script ===
# Run as root: sudo bash harden_vps.sh

set -euo pipefail

echo "=== VPS Hardening ==="

# 1. System Updates
echo "[1/7] System-Updates..."
apt update && apt upgrade -y

# 2. Disable SSH Root Login
echo "[2/7] SSH härten..."
sed -i 's/^#*PermitRootLogin.*/PermitRootLogin no/' /etc/ssh/sshd_config
sed -i 's/^#*PasswordAuthentication.*/PasswordAuthentication no/' /etc/ssh/sshd_config
systemctl restart sshd

# 3. UFW Firewall
echo "[3/7] Firewall konfigurieren..."
apt install -y ufw
ufw default deny incoming
ufw default allow outgoing
ufw allow ssh
ufw allow 80/tcp
ufw allow 443/tcp
echo "y" | ufw enable

# 4. Fail2Ban
echo "[4/7] Fail2Ban installieren..."
apt install -y fail2ban
systemctl enable fail2ban
systemctl start fail2ban

# 5. Unattended Upgrades
echo "[5/7] Auto-Updates aktivieren..."
apt install -y unattended-upgrades
dpkg-reconfigure -plow unattended-upgrades

# 6. Swap (falls nicht vorhanden)
echo "[6/7] Swap prüfen..."
if [ ! -f /swapfile ]; then
    fallocate -l 2G /swapfile
    chmod 600 /swapfile
    mkswap /swapfile
    swapon /swapfile
    echo "/swapfile swap swap defaults 0 0" >> /etc/fstab
fi

# 7. Docker installieren (falls nicht vorhanden)
echo "[7/7] Docker prüfen..."
if ! command -v docker &> /dev/null; then
    curl -fsSL https://get.docker.com | sh
    systemctl enable docker
fi

echo "=== Hardening abgeschlossen! ==="
echo "Wichtig: Stelle sicher dass du einen SSH-Key hast bevor du dich ausloggst!"
'''
