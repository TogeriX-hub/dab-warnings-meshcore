# Änderungen

Alle Versionen von WarnBridge. Die Nummern folgen [Semantic Versioning](https://semver.org/lang/de/): **Major.Minor.Patch**. Patch heißt Fehlerbehebung, Minor heißt neue Funktion, Major heißt, dass sich am Einrichten oder Bedienen etwas grundlegend ändert.

Welche Version bei dir läuft, steht im Dashboard oben neben dem Logo und in der Antwort auf `/help`. Bei Fehlerberichten bitte immer angeben.

Die Versionen bis einschließlich 1.3.0 wurden im November 2026 nachträglich vergeben, als Git-Tags auf den passenden Commits.

---

## Unveröffentlicht

Schon auf `main`, kommt mit der nächsten Version.

**Behoben**
- Dashboard: Die automatische Aktualisierung alle 10 Sekunden klappt aufgeklappte Warnungen nicht mehr zu. Scroll-Position und markierter Text bleiben erhalten, neu gezeichnet wird nur, wenn sich wirklich etwas geändert hat. Dasselbe gilt für das RxLog

## 1.4.0 – Bot-Empfang, neuer Auftritt, Updates per Skript

*6. November 2026*

> **Einmaliger Umstieg:** `config.yaml` ist nicht mehr Teil des Repos. Vorher bitte sichern, siehe [Aktualisieren im README](README.md#aktualisieren). Danach genügt für jedes Update `./update.sh`.

**Neu**
- Bot-Befehle kommen zuverlässig an: WarnBridge holt eingehende Nachrichten selbst vom Node ab. Bei manchen Companions (z.B. Open-Hop) kam vorher nichts an. Danke an DL4FLY (#11)
- Befehle, die während einer mehrteiligen Antwort eintreffen, warten in einer Warteschlange statt verloren zu gehen
- Nach einem Neustart werden alte, im Node gepufferte Befehle nicht mehr beantwortet
- Bot-Befehle lassen sich im Dashboard abschalten
- Neues Logo, neues README, neue Grafiken
- Dashboard mit heller Ansicht, folgt der Systemeinstellung. Schrift liegt lokal und lädt auch ohne Internet
- Versionsanzeige im Dashboard, im Log und bei `/help`
- `update.sh`: holt den neuen Stand, baut welle-cli bei Bedarf neu und startet den Dienst neu
- `config.yaml` wird beim ersten Start aus `config.example.yaml` angelegt und bei Updates nie überschrieben

**Behoben**
- Testbutton im Dashboard tat nichts
- Testwarnung nutzt die eingestellten Kreise statt fest Böblingen/Stuttgart
- Mehrere Testwarnungen hintereinander gehen jetzt alle raus. DAB+-Testwarnungen bleiben dedupliziert, damit sie das Stundenlimit nicht aufbrauchen
- Ein Fehler im Bot trennte die Mesh-Verbindung
- Leere Fläche unten rechts im Monitor

## 1.3.0 – Stabiler Dauerbetrieb mit dem Heltec

*19. Oktober 2026*

**Neu**
- Tote TCP-Verbindung zum Node wird erkannt (Health-Check alle 2 Minuten) und neu aufgebaut
- Node-Uhr wird beim Verbinden und bei Abweichung vom Pi gestellt
- Funk-Hänger: Bestätigt der Node, sendet aber nichts, wird er neu gestartet und die Nachricht nachgeschickt

**Behoben**
- Weltraumwetter: alte Meldungen wurden nach einem Neustart erneut gesendet, Filter für X-Flares fehlte (#8)
- DWD-Stufen pro Ereignis an die offiziellen Warnkriterien angeglichen (#8)
- Broadcast-Dedup lief nie ab und blockierte wiederkehrende Warnungen dauerhaft
- Langlaufende Warnungen wurden alle 24 Stunden neu gesendet

## 1.2.2

*1. Juli 2026*

**Behoben**
- Doppelte Aufhebungen (Cancel) des DWD werden unterdrückt
- Warnungen, deren Ablaufzeit beim Eintreffen schon vorbei war, wurden falsch behandelt

## 1.2.1

*29. Juni 2026*

**Behoben**
- Warnungen anderer Bots, die mit `/warnings` enden, lösen keine Bot-Antwort mehr aus (#9)

## 1.2.0 – Weniger Gewitter-Spam

*22. Juni 2026*

**Neu**
- DWD-Warnungen werden nur noch neu gesendet, wenn sich die Warnstufe ändert. Eine Gewitterlage mit 13 Einzelmeldungen ergab vorher 13 Mesh-Nachrichten, jetzt etwa 6

## 1.1.1

*20. Juni 2026*

**Behoben**
- Ein `/` im Nutzernamen wurde als Befehl erkannt (#7)

## 1.1.0 – Weltraumwetter

*6. Juni 2026*

**Neu**
- Warnungen vor Sonnenstürmen und Funkausfällen von der NOAA, Befehl `/swdetails`

**Behoben**
- Bot-Befehle werden aus dem MeshCore-Nachrichtenformat (`Name: /befehl`) richtig erkannt

## 1.0.0 – Dauerbetrieb auf dem Raspberry Pi

*4. Juni 2026*

**Neu**
- `install_pi.sh` richtet den Pi komplett ein
- systemd-Dienst mit Watchdog, Logrotation
- Broadcasting-Schalter bleibt über Neustarts erhalten
- Mesh-Ausgang auch im Produktivbetrieb im Dashboard sichtbar
- Dokumentation für Installation und Konfiguration

**Behoben**
- DWD-Spam durch Updates und Aufhebungen
- Rate-Limit-Zähler

## 0.5.0 – ASA mit Standort

*23. April 2026*

- ASA-Standortcode (Geocode) wird ausgewertet, Warnungen nur für den eigenen Standort
- ASA bleibt nach dem Ende kurz aktiv, damit der Warntext noch ankommt
- ASA-Meldungen werden dedupliziert, Stufe wird auf Schweregrad abgebildet
- Journaline-Dienst wird zuverlässig gefunden
- Rate-Limit zählt nur echte Mesh-Nachrichten
- Watchdog startet welle-cli auch bei Hängern neu
- Dedup-Cache im Dashboard leerbar

## 0.4.0 – welle-cli aus dem Dashboard

*20. April 2026*

- welle-cli starten, stoppen und Kanal wechseln im Dashboard
- welle.io ist Teil des Repos, `install.sh` für die Einrichtung am Mac
- Fehler im Journaline-Decoder (NML) behoben

## 0.3.0 – Journaline und ASA

*19. April 2026*

- Journaline-Texte werden dekodiert (Decoder von Fraunhofer IIS)
- ASA-Signal (FIG 0/15 nach ETSI TS 104 089) wird erkannt: Heartbeat, Alarm, Ende

## 0.2.0 – DAB+

*18. April 2026*

- DAB+-Empfang über welle-cli, Status und Signalqualität im Dashboard
- Kürzere Mesh-Nachrichten
- welle-cli wird bei Ausfall neu gestartet

## 0.1.0 – Erste Version

*18. April 2026*

- NINA-Warnungen (MoWaS und DWD) für ein Bundesland, Mesh-Alarm für ausgewählte Kreise
- Bot-Befehle `/details`, `/warnings`, `/status`, `/help`
- Dashboard mit Simulator, Entwicklung am Mac ohne Hardware
