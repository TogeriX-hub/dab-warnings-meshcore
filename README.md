# WarnBridge

DAB+ Notfallwarnungen offline empfangen und ins MeshCore-LoRa-Netz einspeisen.

WarnBridge ist ein Python-basiertes System, das offizielle Katastrophenwarnungen aus zwei Quellen verarbeitet:

- **DAB+ (ASA/EWF/Journaline)** – vollständig offline, primärer Kanal  
- **NINA API (BBK)** – sekundärer Online-Kanal  

Die Warnungen werden vereinheitlicht, dedupliziert und anschließend als kompakte Nachrichten in ein MeshCore LoRa Mesh-Netzwerk übertragen.

---

## Ziel des Projekts

WarnBridge soll sicherstellen, dass Warnmeldungen auch unter schwierigen Bedingungen (z. B. Stromausfall, Mobilfunküberlastung oder Internetausfall) verfügbar bleiben.

- DAB+ dient als resiliente, unabhängige Quelle  
- LoRa Mesh ermöglicht die lokale Weiterverteilung ohne Infrastruktur  

---

## Funktionsübersicht

- Empfang von DAB+ Multiplexen über RTL-SDR  
- Auswertung von ASA (Automatic Safety Alert) inkl. Geocode-Filter (ETSI TS 104 089)  
- Journaline-Dekodierung für Warntexte (Fraunhofer NML Decoder)  
- NINA API Integration (MoWaS + DWD)  
- CAP-Normalisierung (einheitliche Datenstruktur)  
- Deduplizierung (ID + Inhalts-Hash)  
- Speicherung in SQLite (48h)  
- Versand ins MeshCore-Netz inkl. RegionScope Einstellung  

### Bot-Befehle

- `/details`  
- `/warnings <Ort>`  
- `/status`  
- `/help`

- Web-Dashboard mit Simulator (für Entwicklung & Konfiguration)

---

## Voraussetzungen

### Hardware (empfohlen)

- Raspberry Pi 3B+ (oder vergleichbar)  
- RTL-SDR (z. B. Nooelec NESDR SMArt v5)  
- DAB+ Antenne  
- Heltec WiFi LoRa 32 v3 (für MeshCore)  

### Software

- macOS mit Homebrew **oder** Raspberry Pi OS  
- Python 3.9+  
- welle-cli (angepasste Version, bereits im Repo enthalten)

---

## Installation (macOS)

### 1. Homebrew installieren (falls noch nicht vorhanden)

```bash
/bin/bash -c "$(curl -fsSL https://raw.githubusercontent.com/Homebrew/install/HEAD/install.sh)"
```

### 2. Repository klonen

```bash
git clone https://github.com/TogeriX-hub/dab-warnings-meshcore
cd dab-warnings-meshcore
```

### 3. Installation mit einem Befehl

```bash
chmod +x install.sh
./install.sh
```

Das Skript installiert automatisch alle Abhängigkeiten, kompiliert welle-cli und richtet die Python-Umgebung ein.

---

## Starten

```bash
python3 warnbridge.py
```

welle-cli wird automatisch von WarnBridge gestartet. Der Pfad ist in `config.yaml` vorgegeben:

```yaml
dab:
  welle_cli_path: ./welle.io/build/welle-cli
```

---

## Geocode-Filter (ASA)

WarnBridge filtert ASA-Alerts geografisch nach ETSI TS 104 089 Annex A/F.  
Der Standort-Code wird in `config.yaml` als Präsentationsformat eingegeben:

```yaml
dab:
  asa_geocode: 1257-1533-2371  # Beispiel: Sindelfingen
```

Der Code kann über die offizielle DAB-Lokalisierungsseite ermittelt werden.  
Auf 5C läuft dauerhaft ein Testalert mit dem Paris-Code `1253-3513-3668` – korrekt konfigurierte Geräte ignorieren diesen.

---

## Wichtiger Hinweis zu welle.io

Dieses Projekt enthält eine angepasste Version von welle.io direkt im Repository (`welle.io/`). Der Fork enthält folgende Erweiterungen gegenüber dem Original:

- ASA-Erkennung über FIG 0/15 (ETSI TS 104 089) mit korrektem Location Code Parser  
- ASA Holdover: `active=true` bleibt 10s nach Alert-Ende (für zuverlässiges Polling)  
- Packet-Mode Subchannel Decoder  
- Fraunhofer NML Journaline Decoder (zlib-komprimierte JML-Objekte)  
- `/journaline.json` und `/rxlog.json` HTTP-Endpunkte  

Original: https://github.com/AlbrechtL/welle.io  
Unser Fork: https://github.com/TogeriX-hub/welle.io  

---

## Entwicklung (Mac-first)

- MeshCore Simulator aktiv über `config.yaml` (`meshcore.simulator: true`)
- Dashboard unter: http://localhost:8080  
- Keine Hardware für Entwicklung nötig  

---

## Projektstatus

| Komponente | Status |
|---|---|
| DAB+ Empfang | ✅ OK |
| ASA-Erkennung | ✅ OK |
| ASA Geocode-Filter | ✅ OK (ETSI TS 104 089) |
| Journaline-Decoder | ✅ OK (getestet auf DLF 5C) |
| NINA Integration (MoWaS + DWD) | ✅ OK |
| MeshCore Integration | ✅ OK |
| Web-Dashboard | ✅ OK |
| Raspberry Pi Setup | 🔜 Ausstehend |
| Warntag-Livetest | 🗓 10.09.2026 |

---

## Offene Punkte (Phase 4+)

- Journaline-Zuweisung zu ASA-Events verbessern  
- Umlaut-Fix in welle.io (`webprogrammehandler.cpp`, UTF-8/Latin-1)  
- `journalineSId` deterministisch auf den Service mit validem Subchannel setzen  
- Welle.io Prozesse für Dauerbetrieb stabilisieren (systemd, Watchdog)  
- Raspberry Pi Setup  
