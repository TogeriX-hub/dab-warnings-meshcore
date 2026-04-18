# WarnBridge

DAB+ Notfallwarnungen offline empfangen und ins MeshCore-LoRa-Netz einspeisen.

WarnBridge ist ein Python-basiertes System, das offizielle Katastrophenwarnungen aus zwei Quellen verarbeitet:

- **DAB+ (ASA/EWF)** – vollständig offline, primärer Kanal  
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
- (Geplant) Journaline-Dekodierung für Warntexte  
- NINA API Integration (MoWaS)  
- CAP-Normalisierung (einheitliche Datenstruktur)  
- Deduplizierung (ID + Inhalts-Hash)  
- Speicherung in SQLite (48h)  
- Versand ins MeshCore-Netz inkl. RegionScope Einstellung  

### Bot-Befehle

- `/details`  
- `/warnings <Ort>`  
- `/status`  

- Web-Dashboard mit Simulator (für Entwicklung & Konfiguration)

---

## Voraussetzungen

### Hardware (empfohlen)

- Raspberry Pi 3B+ (oder vergleichbar)  
- RTL-SDR (z. B. Nooelec NESDR SMArt v5)  
- DAB+ Antenne  
- Heltec WiFi LoRa 32 v3 (für MeshCore)  

### Software

- Python 3.9+  
- welle-cli (angepasste Version, siehe unten)  

---

## Wichtiger Hinweis zu welle.io

Dieses Projekt nutzt eine angepasste Version von welle.io:

https://github.com/TogeriX-hub/welle.io  

Bitte diesen Fork verwenden und selbst kompilieren.

---

## Installation

### 1. Repository klonen

```bash
git clone https://github.com/TogeriX-hub/dab-warnings-meshcore
cd dab-warnings-meshcore
```

### 2. Python-Abhängigkeiten installieren

```bash
pip install -r requirements.txt
```

### 3. welle-cli installieren

```bash
git clone https://github.com/TogeriX-hub/welle.io
cd welle.io
mkdir build && cd build

cmake .. \
  -DRTLSDR=1 \
  -DBUILD_WELLE_IO=OFF \
  -DBUILD_WELLE_CLI=ON \
  -DKISS_FFT=ON

make -j4
```

### 4. welle-cli starten

```bash
./welle-cli -c 9D -C 1 -w 7979
```

### 5. WarnBridge starten

```bash
python3 warnbridge.py
```

---

## Entwicklung (Mac-first)

- MeshCore Simulator aktiv über `config.yaml`  
- Dashboard unter: http://localhost:8080  
- Keine Hardware für Entwicklung nötig  

---

## Projektstatus

- DAB+ Empfang: OK  
- ASA-Erkennung: OK  
- NINA Integration: OK  
- MeshCore Integration: OK  
- Web-Dashboard: OK  
- Journaline: in Entwicklung  
