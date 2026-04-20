#!/bin/bash
set -e

echo "=== WarnBridge Install ==="

# 1. Homebrew prüfen
if ! command -v brew &>/dev/null; then
    echo "Homebrew nicht gefunden. Bitte zuerst installieren: https://brew.sh"
    exit 1
fi

# 2. Abhängigkeiten
echo "→ Installiere Abhängigkeiten..."
brew install cmake libusb zlib lame mpg123 librtlsdr faad2 git python3

# 3. welle-cli bauen
echo "→ Baue welle-cli..."
SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
cd "$SCRIPT_DIR/welle.io"
mkdir -p build && cd build
rm -rf *

cmake .. \
  -DRTLSDR=1 \
  -DBUILD_WELLE_IO=OFF \
  -DBUILD_WELLE_CLI=ON \
  -DKISS_FFT=ON \
  -DCMAKE_POLICY_VERSION_MINIMUM=3.5 \
  -DLAME_INCLUDE_DIRS=$(brew --prefix lame)/include \
  -DLAME_LIBRARIES=$(brew --prefix lame)/lib/libmp3lame.dylib \
  -DMPG123_INCLUDE_DIR=$(brew --prefix mpg123)/include \
  -DMPG123_LIBRARIES=$(brew --prefix mpg123)/lib/libmpg123.dylib \
  -DCMAKE_CXX_FLAGS="-I$(brew --prefix mpg123)/include -I$(brew --prefix lame)/include -I$(brew --prefix faad2)/include"

make -j4
cd "$SCRIPT_DIR"

# 4. Python
echo "→ Installiere Python-Pakete..."
pip3 install --break-system-packages -r requirements.txt

echo ""
echo "✓ Installation abgeschlossen."
echo ""
echo "Nächste Schritte:"
echo "  1. RTL-SDR Dongle einstecken"
echo "  2. welle-cli starten:"
echo "     cd welle.io/build && ./welle-cli -c 9D -C 1 -w 7979"
echo "  3. WarnBridge starten (neues Terminal-Fenster):"
echo "     python3 warnbridge.py"
