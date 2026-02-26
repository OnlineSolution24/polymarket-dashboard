#!/bin/bash
# === Polymarket Dashboard VPS Hardening Script ===
# Run as root: sudo bash harden_vps.sh

set -euo pipefail

echo "=== VPS Hardening ==="

# 1. System Updates
echo "[1/7] System-Updates..."
apt update && apt upgrade -y

# 2. Disable SSH Root Login + Password Auth
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

# 6. Swap
echo "[6/7] Swap prüfen..."
if [ ! -f /swapfile ]; then
    fallocate -l 2G /swapfile
    chmod 600 /swapfile
    mkswap /swapfile
    swapon /swapfile
    echo "/swapfile swap swap defaults 0 0" >> /etc/fstab
fi

# 7. Docker
echo "[7/7] Docker prüfen..."
if ! command -v docker &> /dev/null; then
    curl -fsSL https://get.docker.com | sh
    systemctl enable docker
fi

echo "=== Hardening abgeschlossen ==="
echo "Wichtig: SSH-Key prüfen bevor du dich ausloggst!"
