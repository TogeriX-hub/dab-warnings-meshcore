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
echo "[1/6] System-Pakete installieren..."
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
    zlib1g-dev

# ── 2. RTL-SDR Kernel-Treiber blacklisten ────────────────────────────────────
echo "[2/6] RTL-SDR Kernel-Treiber blacklisten..."
BLACKLIST=/etc/modprobe.d/blacklist-rtl.conf
if [ ! -f "$BLACKLIST" ]; then
    echo 'blacklist dvb_usb_rtl28xxu' | sudo tee "$BLACKLIST"
    sudo rmmod dvb_usb_rtl28xxu 2>/dev/null || true
    echo "    Blacklist angelegt. Dongle beim nächsten Start verfügbar."
else
    echo "    Blacklist bereits vorhanden."
fi

# ── 3. Python-Abhängigkeiten ──────────────────────────────────────────────────
echo "[3/6] Python-Abhängigkeiten installieren..."
pip3 install -r "$REPO_DIR/requirements.txt" --break-system-packages

# ── 4. welle-cli kompilieren ──────────────────────────────────────────────────
echo "[4/6] welle-cli kompilieren..."
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
echo "[5/6] RTL-SDR Dongle testen..."
echo "    Dongle einstecken falls noch nicht geschehen."
echo "    Teste mit: rtl_test -t"
echo "    (Nicht automatisch – manuell ausführen)"

# ── 6. config.yaml anpassen ──────────────────────────────────────────────────
echo "[6/6] Hinweise zur config.yaml:"
echo ""
echo "    Folgende Einstellungen für den Pi anpassen:"
echo ""
echo "    meshcore:"
echo "      simulator: false          # Echten Heltec verwenden"
echo "      host: 192.168.4.1         # Heltec im Pi-Hotspot"
echo "      port: 4403"
echo ""
echo "    dab:"
echo "      welle_cli_path: $BUILD_DIR/welle-cli"
echo ""
echo "    (oder welle_cli_path leer lassen – auto-detect findet den Pfad)"
echo ""
echo "=== Setup abgeschlossen ==="
echo ""
echo "WarnBridge starten:"
echo "  cd $REPO_DIR && python3 warnbridge.py"
echo ""
echo "Mit Log-Datei:"
echo "  cd $REPO_DIR && python3 warnbridge.py 2>&1 | tee warnbridge_log.txt"
