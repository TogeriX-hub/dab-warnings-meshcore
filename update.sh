#!/usr/bin/env bash
# WarnBridge aktualisieren
#
#   ./update.sh            neuen Stand holen, bei Bedarf welle-cli neu bauen, Dienst neu starten
#   ./update.sh --check    nur anzeigen, ob es etwas Neues gibt
#   ./update.sh --no-build welle-cli nicht neu bauen, auch wenn sich welle.io geändert hat
#
# config.yaml, die Datenbank und Logs sind nicht Teil des Repos und bleiben unverändert.

set -euo pipefail

CHECK_ONLY=false
NO_BUILD=false
for arg in "$@"; do
    case "$arg" in
        --check)    CHECK_ONLY=true ;;
        --no-build) NO_BUILD=true ;;
        -h|--help)  sed -n '2,8p' "$0" | sed 's/^# \{0,1\}//'; exit 0 ;;
        *) echo "Unbekannte Option: $arg (siehe ./update.sh --help)"; exit 2 ;;
    esac
done

cd "$(dirname "$0")"
REPO_DIR="$(pwd)"
BRANCH="main"

version() { git describe --tags --always --dirty 2>/dev/null || echo "unbekannt"; }

echo "WarnBridge in $REPO_DIR"
echo "Installiert: $(version)"
echo ""

git fetch --quiet --tags origin "$BRANCH"

LOCAL="$(git rev-parse HEAD)"
REMOTE="$(git rev-parse "origin/$BRANCH")"

if [ "$LOCAL" = "$REMOTE" ]; then
    echo "Schon auf dem neuesten Stand."
    exit 0
fi

if ! git merge-base --is-ancestor "$LOCAL" "$REMOTE"; then
    echo "Dein Stand und GitHub sind auseinandergelaufen (lokale Commits?)."
    echo "Automatisch aktualisieren geht so nicht. Lokale Commits anzeigen:"
    echo "  git log --oneline origin/$BRANCH..HEAD"
    exit 1
fi

echo "Neu seit deinem Stand:"
git log --format='  %h %s' "$LOCAL..$REMOTE"
LATEST_TAG="$(git describe --tags --abbrev=0 "$REMOTE" 2>/dev/null || true)"
[ -n "$LATEST_TAG" ] && echo "" && echo "Neueste Version: $LATEST_TAG (Änderungen: CHANGELOG.md)"
echo ""

if $CHECK_ONLY; then
    echo "Zum Aktualisieren: ./update.sh"
    exit 0
fi

# Lokal geänderte Repo-Dateien würden überschrieben – lieber vorher anhalten.
CHANGED_LOCAL="$(git status --porcelain --untracked-files=no)"
if [ -n "$CHANGED_LOCAL" ]; then
    echo "Diese Dateien wurden auf diesem Gerät geändert:"
    echo "$CHANGED_LOCAL" | sed 's/^/  /'
    echo ""
    echo "Damit nichts verloren geht, bricht das Update hier ab."
    echo "Änderungen verwerfen:   git checkout -- <datei>"
    echo "Oder zur Seite legen:   git stash   (später zurück mit: git stash pop)"
    exit 1
fi

CHANGED_FILES="$(git diff --name-only "$LOCAL" "$REMOTE")"

git merge --quiet --ff-only "$REMOTE"
echo "Aktualisiert auf $(version)."

# Python-Abhängigkeiten
if grep -qx "requirements.txt" <<< "$CHANGED_FILES"; then
    echo ""
    echo "requirements.txt hat sich geändert – Python-Pakete werden installiert..."
    pip3 install -q -r requirements.txt --break-system-packages 2>/dev/null \
        || pip3 install -q -r requirements.txt
fi

# welle-cli neu bauen, wenn sich der DAB+-Decoder geändert hat
if grep -q "^welle.io/" <<< "$CHANGED_FILES"; then
    echo ""
    if $NO_BUILD; then
        echo "welle.io hat sich geändert, Bauen übersprungen (--no-build)."
        echo "Später nachholen: cd welle.io/build && make -j4"
    else
        echo "welle.io hat sich geändert – welle-cli wird neu gebaut (auf dem Pi einige Minuten)..."
        mkdir -p welle.io/build
        (
            cd welle.io/build
            cmake .. -DRTLSDR=1 -DBUILD_WELLE_IO=OFF -DBUILD_WELLE_CLI=ON \
                     -DKISS_FFT=ON -DCMAKE_POLICY_VERSION_MINIMUM=3.5 > /dev/null
            make -j4 > /dev/null
        )
        echo "welle-cli gebaut."
    fi
fi

# Dienst neu starten
echo ""
if command -v systemctl > /dev/null && systemctl is-enabled --quiet warnbridge 2>/dev/null; then
    echo "WarnBridge wird neu gestartet..."
    sudo systemctl restart warnbridge
    sleep 5
    if systemctl is-active --quiet warnbridge; then
        echo "WarnBridge läuft wieder ($(version))."
    else
        echo "WarnBridge ist nicht wieder angelaufen. Letzte Log-Zeilen:"
        journalctl -u warnbridge -n 20 --no-pager
        exit 1
    fi
else
    echo "Kein systemd-Dienst gefunden. WarnBridge neu starten mit:"
    echo "  python3 warnbridge.py"
fi
