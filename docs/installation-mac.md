# Installation – macOS

WarnBridge läuft zur Entwicklung vollständig auf dem Mac – ohne Raspberry Pi und ohne echte MeshCore-Hardware. Der integrierte Simulator zeigt alle ausgehenden Mesh-Nachrichten im Dashboard.

## Voraussetzungen

- macOS (Intel oder Apple Silicon)
- [Homebrew](https://brew.sh)
- Nooelec NESDR SMArt v5 (oder anderer RTL-SDR-Dongle) für DAB+-Empfang

## Installation

```bash
git clone https://github.com/TogeriX-hub/dab-warnings-meshcore
cd dab-warnings-meshcore
bash install.sh
```

Das Skript installiert alle Abhängigkeiten über Homebrew, kompiliert welle-cli und richtet die Python-Umgebung ein. Der erste Build dauert ca. 5–10 Minuten.

## Starten

```bash
python3 warnbridge.py
```

Dashboard: [http://localhost:8080](http://localhost:8080)

welle-cli wird automatisch von WarnBridge gestartet – manuell über das Dashboard (Konfiguration → DAB+ → ▶ Start) oder direkt:

```bash
cd welle.io/build && ./welle-cli -c 9D -C 1 -w 7979
```

## Simulator-Modus

Solange kein Heltec per TCP erreichbar ist, läuft WarnBridge im Simulator-Modus (`meshcore.simulator: true` in `config.yaml`). Alle Mesh-Nachrichten erscheinen im Dashboard unter **Mesh-Output** statt ins echte Netz zu gehen.

Zum Testen: Dashboard → Monitor → **Testwarnung auslösen**

## Dauerbetrieb (Langzeittest)

Damit macOS den USB-Dongle bei Inaktivität nicht trennt:

```bash
caffeinate -is python3 warnbridge.py 2>&1 | tee warnbridge_log.txt
```

`Ctrl+Shift+Power` dimmt den Bildschirm ohne den Rechner zu schlafen. Den Apple-Menü-Ruhezustand vermeiden – er kann USB-Power trennen.

## DAB+-Empfang testen

```bash
rtl_test -t
```

Erwartete Ausgabe: `Found 1 device(s)` – dann mit `Ctrl+C` abbrechen.

## Bekannte Einschränkungen macOS

- FFTW baut nicht zuverlässig auf Intel-Macs → WarnBridge nutzt stattdessen KISS FFT (automatisch)
- SWR BW N (Kanal 9D) sendet Journaline nur bei aktiver ASA-Warnung – für Decoder-Tests eignet sich DLF 5C
