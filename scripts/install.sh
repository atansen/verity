#!/usr/bin/env bash
# ---------------------------------------------------------------------------
# Skincare Scanner — Raspberry Pi Installation Script
# Run as root: sudo bash scripts/install.sh
# ---------------------------------------------------------------------------

set -euo pipefail

INSTALL_DIR="/opt/skincare-scanner"
SERVICE_NAME="skincare-scanner"
SERVICE_FILE="systemd/${SERVICE_NAME}.service"
VENV_DIR="${INSTALL_DIR}/venv"
APP_USER="pi"

echo "=== Skincare Scanner Installer ==="
echo

# ---- Check running as root ----
if [[ $EUID -ne 0 ]]; then
  echo "ERROR: This script must be run as root (use sudo)." >&2
  exit 1
fi

# ---- Install system dependencies ----
echo "[1/6] Installing system packages..."
apt-get update -qq
apt-get install -y -qq python3 python3-pip python3-venv git

# ---- Copy app files ----
echo "[2/6] Copying application files to ${INSTALL_DIR}..."
mkdir -p "${INSTALL_DIR}"
# Copy everything except the venv and .git
rsync -av --exclude='.git' --exclude='venv' --exclude='__pycache__' \
  "$(dirname "$(realpath "$0")")/.." "${INSTALL_DIR}/" \
  || cp -r "$(dirname "$(realpath "$0")")/.." "${INSTALL_DIR}/"

chown -R "${APP_USER}:${APP_USER}" "${INSTALL_DIR}"

# ---- Create virtual environment ----
echo "[3/6] Creating Python virtual environment..."
sudo -u "${APP_USER}" python3 -m venv "${VENV_DIR}"

# ---- Install Python dependencies ----
echo "[4/6] Installing Python dependencies..."
sudo -u "${APP_USER}" "${VENV_DIR}/bin/pip" install --upgrade pip -q
sudo -u "${APP_USER}" "${VENV_DIR}/bin/pip" install -r "${INSTALL_DIR}/requirements.txt" -q

# ---- Configure API keys ----
echo "[5/6] Configuration"
CONFIG_FILE="${INSTALL_DIR}/config.json"
echo
echo "  To enable EWG Skin Deep lookups, edit the config file and add your free API key:"
echo "  sudo nano ${CONFIG_FILE}"
echo "  (Get a key at: https://www.ewg.org/skindeep/developers/)"
echo

# ---- Install and enable systemd service ----
echo "[6/6] Installing systemd service..."
cp "${INSTALL_DIR}/${SERVICE_FILE}" "/etc/systemd/system/${SERVICE_NAME}.service"
systemctl daemon-reload
systemctl enable "${SERVICE_NAME}.service"

echo
echo "=== Installation complete! ==="
echo
echo "Start the scanner now:    sudo systemctl start ${SERVICE_NAME}"
echo "Check status:             sudo systemctl status ${SERVICE_NAME}"
echo "View logs:                sudo journalctl -u ${SERVICE_NAME} -f"
echo "Stop the scanner:         sudo systemctl stop ${SERVICE_NAME}"
echo
echo "To run manually (without systemd):"
echo "  cd ${INSTALL_DIR} && ${VENV_DIR}/bin/python main.py"
echo
echo "NOTE: The barcode scanner must be plugged into a USB port."
echo "      The systemd service assumes TTY1. If you're using a desktop"
echo "      environment, remove the StandardInput/TTYPath lines from"
echo "      the service file and run manually from a terminal instead."
