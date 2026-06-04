# Konfiguration – config.yaml

Alle Einstellungen werden in `config.yaml` im Repo-Root gespeichert. Änderungen können im Dashboard (Konfiguration-Tab) vorgenommen werden und werden sofort ohne Neustart übernommen.

---

## meshcore

```yaml
meshcore:
  host: 192.168.4.2       # IP des Heltec im Pi-Hotspot-Netz
  port: 5000              # TCP-Port (meshcomod: 5000, Standard-MeshCore: 4403)
  simulator: false        # true = Simulator-Modus (Mac-Entwicklung)
  channel_idx: 0          # MeshCore-Kanal (0–7)
  scope: 'de-bw-str'      # * = Flood (ganzes Mesh), #Name = regionaler Scope
```

**Simulator-Modus** (`simulator: true`): Alle Nachrichten erscheinen im Dashboard statt ins Mesh gesendet zu werden. Für Mac-Entwicklung ohne Heltec.

**Scope**: Mit MeshCore-Firmware ab v1.10.0 kann ein regionaler Scope gesetzt werden. `*` flutet das gesamte Mesh.

---

## dab

```yaml
dab:
  welle_cli_url: http://localhost:7979   # HTTP-API von welle-cli
  channel: 9D                            # Aktiver DAB+-Kanal (Label, wird per API gesetzt)
  welle_cli_path: ''                     # Leer = auto-detect, oder absoluter Pfad
  asa_geocode: 1257-1533-2371            # Standort-Code für regionale ASA-Filterung
  forward_tests: false                   # [TEST]-Meldungen ins Mesh senden?
  journaline_keywords:                   # Keywords für Warnerkennung im Journaline-Text
    - katastrophenwarnung
    - hochwasserwarnung
    - evakuierung
    - unwetterwarnung
    - sturmwarnung
    - notfallwarnung
    - zivilschutz
    - alarm
```

**ASA Geocode**: Standort-Code nach ETSI TS 104 089 Annex F. Ermittelbar über [asa.radio](https://www.asa.radio). Leer lassen = kein Geocode-Filter (alle ASA-Alerts werden verarbeitet).

Auf Kanal 5C (DLF) läuft dauerhaft ein Testbetrieb mit dem Paris-Code `1253-3513-3668` – mit korrektem Geocode wird dieser ignoriert.

**forward_tests**: Bei `false` werden Meldungen mit `status=test` nicht ins Mesh gesendet (aber gespeichert). Für den Bundesweiten Warntag ggf. auf `true` setzen.

---

## nina

```yaml
nina:
  poll_interval_minutes: 15

  region_state: '08'         # Bundesland (08 = Baden-Württemberg)

  broadcast_districts:       # Nur diese Kreise lösen automatischen Mesh-Alert aus
    - '08115'                # Böblingen (Sindelfingen)
    - '08111'                # Stuttgart

  sources:
    mowas:
      enabled: true
      min_severity: Severe   # Extreme oder Severe

    dwd:
      enabled: true
      events:
        gewitter:
          enabled: true
          min_level: 2       # 1=Vorabinfo, 2=Stark, 3=Sehr stark, 4=Extrem
        sturm_orkan:
          enabled: true
          min_level: 2
        starkregen:
          enabled: true
          min_level: 2
        hochwasser:
          enabled: true
          min_level: 1
        schnee_schneesturm:
          enabled: true
          min_level: 2
        glaette_frost:
          enabled: false
          min_level: 3
        hitze:
          enabled: false
          min_level: 2
        nebel:
          enabled: false
          min_level: 3

    lhp:
      enabled: false         # Hochwasserpegel (länderübergreifendes Portal)
```

**region_state**: Bestimmt welches Bundesland vollständig in die SQLite-DB gespeichert wird. Alle Kreise des Bundeslandes sind per `/warnings <Ort>` abrufbar.

**broadcast_districts**: Nur Warnungen die diese AGS-Schlüssel betreffen lösen automatisch einen Mesh-Alert aus. Andere Kreise werden nur gespeichert.

AGS-Schlüssel (5-stellig): Die ersten zwei Stellen sind der Bundesland-Code (08 = BW), die restlichen drei der Kreis.

---

## dedup

```yaml
dedup:
  id_cache_ttl_hours: 24    # Wie lange eine Meldungs-ID als bekannt gilt
  content_hash: true        # Gleicher Inhalt, andere ID = trotzdem ignorieren
  max_per_hour: 5           # Max. Mesh-Nachrichten pro Stunde (Sicherheitsanker)
```

---

## warnings_db

```yaml
warnings_db:
  ttl_hours: 48    # Warnungen werden 48h in SQLite aufbewahrt
```

---

## broadcasting

```yaml
broadcasting:
  enabled: true    # Broadcasting-Status (wird automatisch gespeichert)
```

Dieser Wert wird automatisch gesetzt wenn der Broadcasting-Schalter im Dashboard umgelegt wird. Beim nächsten Start von WarnBridge wird der gespeicherte Status wiederhergestellt.
