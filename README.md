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
- Auswertung von ASA (Alarm Signalisation)  
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

### welle-cli starten (Terminal 1)

```bash
cd welle.io/build
./welle-cli -c 9D -C 1 -w 7979
```

### WarnBridge starten (Terminal 2)

```bash
cd dab-warnings-meshcore
python3 warnbridge.py
```

---

## Wichtiger Hinweis zu welle.io

Dieses Projekt enthält eine angepasste Version von welle.io direkt im Repository (`welle.io/`). Der Fork enthält folgende Erweiterungen gegenüber dem Original:

- ASA-Erkennung über FIG 0/15 und FIG 0/19  
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

<<<<<<< Updated upstream
- DAB+ Empfang: OK  
- ASA-Erkennung: OK  
- NINA Integration: OK  
- MeshCore Integration: in Entwicklung 
- Web-Dashboard: OK  
- Journaline: in Entwicklung

## To-Do:

- Verbesserung der Zuweisung von Journaline zu ASA-Events
- Duplikaterkennung von ASA Meldungen (Testwarnungen auf 5C)
- Welle.io Prozesse verschlanken (für Dauerbetrieb)
- Duplikaterekennung von ASA + gleichzeitig Nina
- ASA-Warnungen [Test] werden nicht ins Mesh gesendet
=======
| Komponente | Status |
|---|---|
| DAB+ Empfang | ✅ OK |
| ASA-Erkennung | ✅ OK |
| Journaline-Decoder | ✅ OK (getestet auf DLF 5C) |
| NINA Integration (MoWaS + DWD) | ✅ OK |
| MeshCore Integration | ✅ OK |
| Web-Dashboard | ✅ OK |
| Raspberry Pi Setup | 🔜 Ausstehend |
| Warntag-Livetest | 🗓 10.09.2026 |
