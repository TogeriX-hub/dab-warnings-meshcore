#!/bin/bash
# WarnBridge – Raspberry Pi 4 Setup
# Aufruf: bash install_pi.sh
# Nicht automatisch starten – jeden Schritt selbst prüfen.

set -e
REPO_DIR="$(cd "$(dirname "$0")" && pwd)"

echo "=== WarnBridge Pi Setup ==="
echo "Repo: $REPO_DIR"
echo ""

# ── 1. System-Pakete ──────────────────────────────────────────────────────────
echo "[1/7] System-Pakete installieren..."
sudo apt update
sudo apt install -y \
    git \
    python3-pip \
    python3-venv \
    rtl-sdr \
    librtlsdr-dev \
    cmake \
    build-essential \
    libfaad-dev \
    libfftw3-dev \
    libmpg123-dev \
    libmp3lame-dev \
    pkg-config \
    zlib1g-dev \
    logrotate

# ── 2. RTL-SDR Kernel-Treiber blacklisten ────────────────────────────────────
echo "[2/7] RTL-SDR Kernel-Treiber blacklisten..."
BLACKLIST=/etc/modprobe.d/blacklist-rtl.conf
if [ ! -f "$BLACKLIST" ]; then
    echo 'blacklist dvb_usb_rtl28xxu' | sudo tee "$BLACKLIST"
    sudo rmmod dvb_usb_rtl28xxu 2>/dev/null || true
    echo "    Blacklist angelegt. Dongle beim nächsten Start verfügbar."
else
    echo "    Blacklist bereits vorhanden."
fi

# ── 3. Python-Abhängigkeiten ──────────────────────────────────────────────────
echo "[3/7] Python-Abhängigkeiten installieren..."
pip3 install -r "$REPO_DIR/requirements.txt" --break-system-packages
# sdnotify für systemd Watchdog-Heartbeat
pip3 install sdnotify --break-system-packages

# ── 4. welle-cli kompilieren ──────────────────────────────────────────────────
echo "[4/7] welle-cli kompilieren..."
WELLE_DIR="$REPO_DIR/welle.io"
BUILD_DIR="$WELLE_DIR/build"

if [ ! -d "$WELLE_DIR" ]; then
    echo "    FEHLER: welle.io Verzeichnis nicht gefunden unter $WELLE_DIR"
    echo "    Stelle sicher dass das welle.io Repo im Repo-Verzeichnis liegt."
    exit 1
fi

mkdir -p "$BUILD_DIR"
cd "$BUILD_DIR"

cmake .. \
    -DRTLSDR=1 \
    -DBUILD_WELLE_IO=OFF \
    -DBUILD_WELLE_CLI=ON \
    -DKISS_FFT=ON \
    -DCMAKE_POLICY_VERSION_MINIMUM=3.5

make -j4

if [ -f "$BUILD_DIR/welle-cli" ]; then
    echo "    welle-cli erfolgreich kompiliert: $BUILD_DIR/welle-cli"
else
    echo "    FEHLER: welle-cli Binary nicht gefunden nach Build."
    exit 1
fi

cd "$REPO_DIR"

# ── 5. RTL-SDR testen ────────────────────────────────────────────────────────
echo "[5/7] RTL-SDR Dongle testen..."
echo "    Dongle einstecken falls noch nicht geschehen."
echo "    Teste mit: rtl_test -t"
echo "    (Nicht automatisch – manuell ausführen)"

# ── 6. systemd-Service einrichten ────────────────────────────────────────────
echo "[6/7] systemd-Service einrichten..."

# Pfad im Service auf aktuellen User und Repo anpassen
CURRENT_USER="$(whoami)"
SERVICE_SRC="$REPO_DIR/warnbridge.service"
SERVICE_TMP="/tmp/warnbridge.service"

sed "s|/home/pi|/home/$CURRENT_USER|g" "$SERVICE_SRC" > "$SERVICE_TMP"

sudo cp "$SERVICE_TMP" /etc/systemd/system/warnbridge.service
sudo systemctl daemon-reload
sudo systemctl enable warnbridge.service
echo "    systemd-Service installiert und aktiviert."
echo "    Starten:  sudo systemctl start warnbridge"
echo "    Status:   sudo systemctl status warnbridge"
echo "    Logs:     journalctl -u warnbridge -f"

# ── 7. logrotate einrichten ──────────────────────────────────────────────────
echo "[7/7] logrotate einrichten..."

LOGROTATE_SRC="$REPO_DIR/warnbridge-logrotate"
LOGROTATE_TMP="/tmp/warnbridge-logrotate"

sed "s|/home/pi|/home/$CURRENT_USER|g" "$LOGROTATE_SRC" > "$LOGROTATE_TMP"

sudo cp "$LOGROTATE_TMP" /etc/logrotate.d/warnbridge
echo "    logrotate eingerichtet (täglich, 7 Tage aufbewahren)."

# ── Abschluss ─────────────────────────────────────────────────────────────────
echo ""
echo "=== Setup abgeschlossen ==="
echo ""
echo "config.yaml anpassen:"
echo "  meshcore.simulator: false"
echo "  meshcore.host: 192.168.4.2  (Heltec IP aus: cat /var/lib/misc/dnsmasq.leases)"
echo "  meshcore.port: 5000"
echo "  dab.welle_cli_path: $BUILD_DIR/welle-cli"
echo ""
echo "WarnBridge manuell starten (zum Testen):"
echo "  cd $REPO_DIR && python3 warnbridge.py 2>&1 | tee warnbridge_log.txt"
echo ""
echo "WarnBridge als Service starten (Dauerbetrieb):"
echo "  sudo systemctl start warnbridge"
echo "  journalctl -u warnbridge -f"
