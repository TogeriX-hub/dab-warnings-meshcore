# Installation – Raspberry Pi

## Benötigte Hardware

| Komponente | Modell |
|---|---|
| Raspberry Pi 4 | 2 GB RAM |
| RTL-SDR | Nooelec NESDR SMArt v5 |
| DAB+-Antenne | Im Bundle |
| MeshCore-Node | Heltec WiFi LoRa 32 v3 |
| LAN-Kabel | Pi → Router |
| microSD | 32 GB A1/Class 10 |

## Netzwerk-Überblick

- `eth0` → Router → Internet (NINA API)
- `wlan0` → Pi als Access Point (SSID: WarnBridge)
- Heltec verbindet sich mit dem Pi-Hotspot und lauscht auf TCP Port 5000

---

## Teil 1: Heltec – Firmware bauen und flashen

Die Standard-MeshCore-Firmware unterstützt kein WiFi. WLAN-SSID und Passwort müssen zur Build-Zeit eincompiliert werden. Dafür wird [meshcomod](https://github.com/ALLFATHER-BV/meshcomod) verwendet.

### 1.1 Voraussetzungen (Mac)

```bash
pip3 install platformio
```

### 1.2 meshcomod klonen

```bash
cd ~/Desktop
git clone https://github.com/ALLFATHER-BV/meshcomod.git
cd meshcomod/MeshCore
```

### 1.3 Firmware bauen

SSID und Passwort müssen mit den Werten aus Teil 3 übereinstimmen:

```bash
export WIFI_SSID="WarnBridge"
export WIFI_PWD="warnbridge2026"
export FIRMWARE_VERSION=v1.15.0
sh build.sh build-firmware Heltec_v3_companion_radio_usb_tcp
```

Der erste Build dauert ca. 10–15 Minuten. Das fertige Binary liegt in `out/`.

> **Hinweis:** Das Binary enthält WLAN-Zugangsdaten – nicht öffentlich teilen.

### 1.4 Heltec flashen

Heltec per USB-C anschließen. Antenne muss vor dem Einschalten angeschlossen sein.

```bash
ls /dev/cu.usb*
pip3 install esptool
esptool.py --chip esp32s3 --port /dev/cu.usbmodem14401 \
  --baud 921600 write_flash 0x0 \
  out/Heltec_v3_companion_radio_usb_tcp-*.bin
```

Alternativ: [flasher.meshcore.io](https://flasher.meshcore.io) (kein Terminal nötig)

---

## Teil 2: Raspberry Pi OS einrichten

### 2.1 Image flashen

[Raspberry Pi Imager](https://www.raspberrypi.com/software/) öffnen:

- Gerät: **Raspberry Pi 4**
- OS: **Raspberry Pi OS Lite (64-bit)**
- Einstellungen vor dem Flashen:
  - Hostname: `warnbridge`
  - Benutzer: `pi` / Passwort nach Wahl
  - WLAN: leer lassen (wird für Hotspot reserviert)
  - SSH: aktivieren
  - Zeitzone: `Europe/Berlin`

### 2.2 Pi starten und verbinden

SD-Karte in den Pi, LAN-Kabel und Strom anschließen. Nach ca. 60 Sekunden:

```bash
ssh pi@warnbridge.local
```

### 2.3 System aktualisieren und Repo klonen

```bash
sudo apt update && sudo apt upgrade -y
sudo apt install -y git
git clone https://github.com/TogeriX-hub/dab-warnings-meshcore.git
cd dab-warnings-meshcore
```

### 2.4 Install-Skript ausführen

```bash
bash install_pi.sh
```

Installiert alle Pakete, RTL-SDR Blacklist, Python-Abhängigkeiten und kompiliert welle-cli (ca. 15 Minuten).

### 2.5 RTL-SDR testen

```bash
rtl_test -t
```

Erwartete Ausgabe: `Found 1 device(s)` – mit `Ctrl+C` abbrechen. Falls Fehler: `sudo reboot` und erneut testen.

---

## Teil 3: WLAN-Hotspot einrichten

### 3.1 Pakete installieren

```bash
sudo apt install -y hostapd dnsmasq
```

### 3.2 Statische IP für wlan0

```bash
echo -e "\ninterface wlan0\nstatic ip_address=192.168.4.1/24\nnohook wpa_supplicant" | sudo tee -a /etc/dhcpcd.conf
```

### 3.3 hostapd konfigurieren

```bash
sudo tee /etc/hostapd/hostapd.conf << 'EOF'
interface=wlan0
driver=nl80211
ssid=WarnBridge
hw_mode=g
channel=6
wmm_enabled=0
macaddr_acl=0
auth_algs=1
ignore_broadcast_ssid=0
wpa=2
wpa_passphrase=warnbridge2026
wpa_key_mgmt=WPA-PSK
wpa_pairwise=TKIP
rsn_pairwise=CCMP
EOF
```

SSID/Passwort muss mit den Werten aus Teil 1 übereinstimmen.

### 3.4 Konfiguration setzen

```bash
sudo sed -i 's|#DAEMON_CONF=""|DAEMON_CONF="/etc/hostapd/hostapd.conf"|' /etc/default/hostapd
sudo mv /etc/dnsmasq.conf /etc/dnsmasq.conf.bak
echo -e "interface=wlan0\ndhcp-range=192.168.4.2,192.168.4.20,255.255.255.0,24h" | sudo tee /etc/dnsmasq.conf
```

### 3.5 Dienste aktivieren

```bash
sudo rfkill unblock wifi
sudo systemctl unmask hostapd
sudo systemctl enable hostapd
sudo systemctl enable dnsmasq
sudo reboot
```

Nach dem Neustart (ca. 2 Minuten) ist das WLAN-Netz „WarnBridge" sichtbar.

---

## Teil 4: WarnBridge starten

### 4.1 Heltec IP herausfinden

```bash
cat /var/lib/misc/dnsmasq.leases
```

### 4.2 config.yaml anpassen

```bash
nano ~/dab-warnings-meshcore/config.yaml
```

```yaml
meshcore:
  host: 192.168.4.2    # IP aus dnsmasq.leases
  port: 5000           # meshcomod TCP-Port
  simulator: false
```

### 4.3 systemd-Service einrichten

```bash
sudo cp ~/dab-warnings-meshcore/warnbridge.service /etc/systemd/system/
sudo systemctl daemon-reload
sudo systemctl enable warnbridge
sudo systemctl start warnbridge
```

### 4.4 Dashboard aufrufen

```
http://warnbridge.local:8080
```

### 4.5 welle-cli starten

Dashboard → Konfiguration → DAB+ → Kanal 9D → **▶ Start**

### 4.6 Broadcasting aktivieren

Dashboard → Monitor → **▶ BROADCASTING AN**

---

## Troubleshooting

**WLAN nicht sichtbar nach Neustart**
Kann 1–2 Minuten dauern bis hostapd startet. Status prüfen:
```bash
sudo systemctl status hostapd
sudo rfkill list
```

**RTL-SDR nicht gefunden**
```bash
sudo rmmod dvb_usb_rtl28xxu 2>/dev/null || true
rtl_test -t
```

**MeshCore Verbindung schlägt fehl**
```bash
cat /var/lib/misc/dnsmasq.leases
nc -zv 192.168.4.2 5000
```
Port in `config.yaml` muss `5000` sein (meshcomod), nicht `4403`.

**welle-cli startet nicht**
```bash
~/dab-warnings-meshcore/welle.io/build/welle-cli -c 9D -C 1 -w 7979
```

---

## Schnell-Referenz

| Aktion | Befehl |
|---|---|
| SSH | `ssh pi@warnbridge.local` |
| WarnBridge Status | `sudo systemctl status warnbridge` |
| Log | `sudo journalctl -u warnbridge -f` |
| Dashboard | `http://warnbridge.local:8080` |
| Dongle testen | `rtl_test -t` |
| Heltec IP | `cat /var/lib/misc/dnsmasq.leases` |
| Repo aktualisieren | `cd ~/dab-warnings-meshcore && git pull && sudo systemctl restart warnbridge` |
