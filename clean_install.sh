#!/bin/bash
# WarnBridge Clean Install
# Löscht alle persistenten Daten und setzt config.yaml zurück.
# Danach startet das Dashboard mit dem Onboarding-Screen.

set -e
SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
cd "$SCRIPT_DIR"

echo "╔══════════════════════════════════════╗"
echo "║   WarnBridge – Clean Install         ║"
echo "╚══════════════════════════════════════╝"
echo ""

# Laufenden Prozess beenden falls aktiv
if pgrep -f "python3 warnbridge.py" > /dev/null 2>&1; then
  echo "→ WarnBridge wird gestoppt..."
  pkill -f "python3 warnbridge.py" || true
  sleep 1
fi

# SQLite-Datenbank löschen
if [ -f "warnbridge.db" ]; then
  echo "→ warnbridge.db gelöscht"
  rm -f warnbridge.db
fi

# AGS-Cache löschen (wird neu geladen)
if [ -f "ags_cache.json" ]; then
  echo "→ ags_cache.json gelöscht"
  rm -f ags_cache.json
fi

# Config zurücksetzen (region_state leeren)
if [ -f "config.yaml" ]; then
  echo "→ config.yaml: region_state und broadcast_districts geleert"
  python3 -c "
import yaml
with open('config.yaml') as f:
    cfg = yaml.safe_load(f)
cfg.setdefault('nina', {})['region_state'] = ''
cfg['nina']['broadcast_districts'] = []
with open('config.yaml','w') as f:
    yaml.dump(cfg, f, allow_unicode=True, default_flow_style=False)
print('  config.yaml aktualisiert')
"
fi

echo ""
echo "✓ Clean Install abgeschlossen."
echo ""
echo "Jetzt starten:"
echo "  python3 warnbridge.py"
echo ""
echo "Dashboard öffnen: http://localhost:8080"
echo "→ Onboarding-Screen erscheint automatisch."
