"""
dab_listener.py – WarnBridge Phase 3c/3d/3e
Pollt die welle-cli HTTP-API alle 2 Sekunden.
- /mux.json: ASA-Flag, SNR, ews_ensemble, Location Codes
- /journaline.json: Journaline/TPEG/EPG Pakete

Auswertungslogik:
  ASA aktiv + Geocode-Match + Journaline-Titel enthält Keyword + Objekt neu → NormalizedWarning
  Kein ASA → Objekte zur Baseline hinzufügen, kein Mesh
  service_type=="tpeg"/"epg" → nur RxLog

Geocode-Filter (ETSI TS 104 089 Annex A/F):
  asa_geocode in config.yaml im Präsentationsformat (z.B. 1257-1533-2371).
  Wird beim Start in Zone + Combined Code dekodiert.
  Präfix-Matching: Alert-Code muss Präfix des Receiver-Codes sein.
  Kein Location Code im Alert → gesamtes Ensemble betroffen → immer relevant.

Baseline-Mechanismus:
  Alle Journaline-object_ids die OHNE aktives ASA gesehen werden, landen in _baseline_ids.
  Bei aktivem ASA werden nur Objekte ausgewertet die NICHT in der Baseline sind.

Log-Rotation:
  RxLog wird alle 24h geleert (läuft monatelang durch).
  Baseline wird alle 24h geleert damit sie sich nicht unbegrenzt füllt.
"""

import asyncio
import logging
from datetime import datetime, timezone, timedelta
from typing import Callable, Awaitable, Optional

import aiohttp

from cap_normalizer import NormalizedWarning

logger = logging.getLogger(__name__)

CallbackType = Callable[[NormalizedWarning], Awaitable[None]]

POLL_INTERVAL = 2.0
MAX_ERRORS_BEFORE_WARN = 5
WATCHDOG_ERROR_THRESHOLD = 10
RXLOG_MAX = 50
MAX_JOURNALINE_NEW_PER_ALERT = 10  # Max neue Objekte pro ASA-Session auswerten
LOG_ROTATION_HOURS = 24            # RxLog + Baseline alle 24h leeren

# Spezifische Keywords – nur im TITEL prüfen, nicht im Body
# Zusammengesetzte Begriffe um False-Positives zu vermeiden
DEFAULT_KEYWORDS = [
    "katastrophenwarnung",
    "hochwasserwarnung",
    "evakuierung",
    "unwetterwarnung",
    "sturmwarnung",
    "notfallwarnung",
    "zivilschutz",
    "katastrophenschutz",
    "alarm",           # kurz, aber als Titel-Keyword akzeptabel
    "notfall",
]

REQUEST_TIMEOUT = aiohttp.ClientTimeout(
    total=8,
    connect=3,
    sock_read=5,
)


def _parse_presentation_code(pres: str) -> Optional[tuple[int, int, int]]:
    """
    Dekodiert einen DAB Location Code im Präsentationsformat (z.B. '1257-1533-2371')
    nach ETSI TS 104 089 Annex A.
    Gibt (zone, cc_24bit, num_digits=6) zurück, oder None bei Fehler.
    cc_24bit ist linksbündig in 24 Bit gespeichert.
    """
    try:
        parts = pres.strip().split("-")
        if len(parts) != 3 or not all(len(p) == 4 for p in parts):
            return None
        # Präsentationssymbole 1-8 → Oktal 0-7
        groups = []
        for p in parts:
            oct_str = "".join(str(int(c) - 1) for c in p)
            groups.append(int(oct_str, 8))
        # 36-Bit Integer
        val36 = (groups[0] << 24) | (groups[1] << 12) | groups[2]
        # Checksum validieren (mod 61)
        checksum = val36 & 0x3F
        loc30 = val36 >> 6
        if loc30 % 61 != checksum:
            return None
        zone = (loc30 >> 24) & 0x3F
        cc = loc30 & 0xFFFFFF
        return (zone, cc, 6)
    except Exception:
        return None


def _location_match(receiver_zone: int, receiver_cc: int,
                    alert_zone: int, alert_cc: int, alert_num_digits: int) -> bool:
    """
    Präfix-Matching laut ETSI TS 104 089 Clause 7.5.4.
    Alert-Code ist Präfix des Receiver-Codes wenn:
      - Zone identisch
      - Erste alert_num_digits Hex-Ziffern von links identisch
    """
    if receiver_zone != alert_zone:
        return False
    if alert_num_digits == 0:
        # Kein Digit → gesamte Zone → immer Match
        return True
    # Maske: nur die ersten alert_num_digits Nibbles (linksbündig in 24 Bit)
    shift = (6 - alert_num_digits) * 4
    mask = ((1 << (alert_num_digits * 4)) - 1) << shift
    return (receiver_cc & mask) == (alert_cc & mask)


class DabListener:
    def __init__(self, config: dict, on_warning: CallbackType):
        self.cfg = config
        self.on_warning = on_warning
        self.welle_url = config.get("welle_cli_url", "http://localhost:7979")
        self.forward_tests = config.get("forward_tests", False)
        self.asa_geocode_filter = config.get("asa_geocode", "").strip()
        self.keywords = [k.lower() for k in config.get("journaline_keywords", DEFAULT_KEYWORDS)]

        # Standort-Code parsen (Annex A → Zone + CC)
        self._receiver_zone: Optional[int] = None
        self._receiver_cc: Optional[int] = None
        if self.asa_geocode_filter:
            parsed = _parse_presentation_code(self.asa_geocode_filter)
            if parsed:
                self._receiver_zone, self._receiver_cc, _ = parsed
                logger.info("DAB-Listener: Standort-Code %s → Z%d:0x%06X",
                            self.asa_geocode_filter, self._receiver_zone, self._receiver_cc)
            else:
                logger.warning("DAB-Listener: Ungültiger asa_geocode: '%s' – Geocode-Filter deaktiviert",
                               self.asa_geocode_filter)

        self._last_change: int = -1
        self._active_since: Optional[datetime] = None
        self._running = False
        self._error_count = 0
        self._last_snr: Optional[float] = None
        self._last_poll_ok: bool = False
        self._status: str = "not_started"

        # Journaline-Status
        self._journaline_present: bool = False
        self._journaline_service_type: str = ""

        # ASA-Zustand
        self._asa_active: bool = False
        self._asa_is_test: bool = False
        self._asa_level: int = 0
        self._ews_ensemble: bool = False

        # RxLog: alle empfangenen Pakete (max RXLOG_MAX)
        self._rxlog: list[dict] = []

        # Baseline: object_ids die OHNE aktives ASA gesehen wurden
        # Bei aktivem ASA werden nur Objekte ausgewertet die NICHT hier drin sind
        self._baseline_ids: set = set()

        # Dedup: bereits verarbeitete Journaline-object_ids pro ASA-Session
        self._seen_journaline_ids: set = set()

        # Zähler neue Objekte pro ASA-Session (Limit MAX_JOURNALINE_NEW_PER_ALERT)
        self._new_objects_this_session: int = 0

        # Log-Rotation: Zeitstempel letzter Rotation
        self._last_log_rotation: datetime = datetime.now(timezone.utc)

        self._watchdog_triggered = False

    async def run(self):
        self._running = True
        self._status = "starting"
        logger.info("DAB-Listener gestartet: %s, Kanal %s",
                    self.welle_url, self.cfg.get("channel", "9D"))

        async with aiohttp.ClientSession() as session:
            while self._running:
                try:
                    await self._poll(session)
                    await self._maybe_rotate_logs()
                except asyncio.CancelledError:
                    break
                except asyncio.TimeoutError:
                    self._error_count += 1
                    self._last_poll_ok = False
                    self._status = "error"
                    if self._error_count <= MAX_ERRORS_BEFORE_WARN:
                        logger.warning("DAB-Listener: Timeout bei welle-cli – Versuch %d",
                                       self._error_count)
                    elif self._error_count % 30 == 0:
                        logger.warning("DAB-Listener: %d Timeouts in Folge – welle-cli hängt?",
                                       self._error_count)
                    if self._error_count >= WATCHDOG_ERROR_THRESHOLD and not self._watchdog_triggered:
                        logger.error("DAB-Listener: %d Fehler in Folge – Watchdog ausgelöst",
                                     self._error_count)
                        self._watchdog_triggered = True
                except Exception as e:
                    self._error_count += 1
                    self._last_poll_ok = False
                    self._status = "error"
                    if self._error_count <= MAX_ERRORS_BEFORE_WARN:
                        logger.error("DAB-Listener Fehler: %s", e)
                    elif self._error_count % 30 == 0:
                        logger.warning("DAB-Listener: %d Fehler in Folge", self._error_count)
                    if self._error_count >= WATCHDOG_ERROR_THRESHOLD and not self._watchdog_triggered:
                        logger.error("DAB-Listener: %d Fehler in Folge – Watchdog ausgelöst",
                                     self._error_count)
                        self._watchdog_triggered = True
                await asyncio.sleep(POLL_INTERVAL)

        self._running = False
        self._status = "stopped"
        logger.info("DAB-Listener gestoppt")

    def stop(self):
        self._running = False

    def reset_watchdog(self):
        self._watchdog_triggered = False
        self._error_count = 0
        self._last_change = -1

    @property
    def watchdog_triggered(self) -> bool:
        return self._watchdog_triggered

    async def _maybe_rotate_logs(self):
        """Alle 24h RxLog und Baseline leeren."""
        now = datetime.now(timezone.utc)
        if (now - self._last_log_rotation).total_seconds() >= LOG_ROTATION_HOURS * 3600:
            old_rxlog = len(self._rxlog)
            old_baseline = len(self._baseline_ids)
            self._rxlog.clear()
            self._baseline_ids.clear()
            self._last_log_rotation = now
            logger.info("DAB-Listener Log-Rotation: RxLog (%d Einträge) + Baseline (%d IDs) geleert",
                        old_rxlog, old_baseline)

    async def _poll(self, session: aiohttp.ClientSession):
        """Pollt /mux.json und /journaline.json parallel."""
        mux_url = f"{self.welle_url}/mux.json"
        jl_url = f"{self.welle_url}/journaline.json"

        mux_task = session.get(mux_url, timeout=REQUEST_TIMEOUT)
        jl_task = session.get(jl_url, timeout=REQUEST_TIMEOUT)

        async with mux_task as mux_resp, jl_task as jl_resp:
            if mux_resp.status != 200:
                raise ValueError(f"HTTP {mux_resp.status} von welle-cli (/mux.json)")
            mux_data = await mux_resp.json()

            jl_data = None
            if jl_resp.status == 200:
                jl_data = await jl_resp.json()
            elif jl_resp.status != 404:
                logger.debug("DAB: /journaline.json HTTP %d", jl_resp.status)

        if self._error_count > 0:
            logger.info("DAB-Listener: welle-cli wieder erreichbar nach %d Fehlern",
                        self._error_count)
        self._error_count = 0
        self._watchdog_triggered = False
        self._last_poll_ok = True

        demod = mux_data.get("demodulator", {})
        self._last_snr = demod.get("snr")

        snr = self._last_snr or 0
        if snr < 5.0:
            if self._status != "no_signal":
                logger.warning("DAB+: SNR zu niedrig (%.1f dB)", snr)
            self._status = "no_signal"
            return

        self._status = "ok"

        asa = mux_data.get("asa", {})
        self._ews_ensemble = asa.get("ews_ensemble", False)

        await self._process_asa(asa, mux_data)

        if jl_data is not None:
            await self._process_journaline(jl_data)

    async def _process_asa(self, asa: dict, mux: dict):
        """ASA-Block auswerten, NormalizedWarning erzeugen wenn aktiv."""
        active = asa.get("active", False)
        last_change = asa.get("last_change", 0)
        is_test = asa.get("is_test", False)

        self._asa_active = active
        self._asa_is_test = is_test
        self._asa_level = asa.get("level", 0)

        # ASA beendet
        if not active and self._active_since:
            duration = (datetime.now(timezone.utc) - self._active_since).seconds
            logger.info("DAB+ ASA beendet (Dauer: %ds)", duration)
            self._active_since = None
            self._last_change = -1  # Reset damit nächster Alert erkannt wird
            self._seen_journaline_ids.clear()
            self._new_objects_this_session = 0
            return

        # Nicht aktiv
        if not active:
            if self._last_change == -1:
                self._last_change = last_change  # Initialisierung beim ersten Poll
            return

        # Ab hier: active=True

        # Alert läuft bereits → nicht nochmal senden
        # Verhindert Mehrfach-Alerts durch wiederholte FIG 0/15 Instanzen (Alert-Set)
        if self._active_since is not None:
            self._last_change = last_change
            return

        # Neuer Alert – erste Auslösung
        self._last_change = last_change
        self._active_since = datetime.now(timezone.utc)

        has_region = asa.get("has_region", False)
        alert_zone = asa.get("region_zone", 0)
        alert_cc = asa.get("region_cc", 0)
        alert_num_digits = asa.get("region_num_digits", 0)

        logger.info(
            "🚨 DAB+ ASA AKTIV | test=%s level=%s zone=%s num_digits=%s cc=0x%06X",
            is_test, self._asa_level, alert_zone, alert_num_digits, alert_cc
        )

        # Geocode-Filter
        if self._receiver_zone is not None and has_region and alert_num_digits > 0:
            matched = _location_match(
                self._receiver_zone, self._receiver_cc,
                alert_zone, alert_cc, alert_num_digits
            )
            if not matched:
                logger.info(
                    "ASA: Geocode-Filter – kein Match "
                    "(Alert Z%d:0x%06X/%dD vs Receiver Z%d:0x%06X) – ignoriert",
                    alert_zone, alert_cc, alert_num_digits,
                    self._receiver_zone, self._receiver_cc
                )
                self._active_since = None  # Session nicht starten bei gefiltertem Alert
                return
            else:
                logger.info(
                    "ASA: Geocode-Match! Alert Z%d:0x%06X/%dD trifft Receiver Z%d:0x%06X",
                    alert_zone, alert_cc, alert_num_digits,
                    self._receiver_zone, self._receiver_cc
                )
        elif not has_region or alert_num_digits == 0:
            logger.info("ASA: Kein Location Code → gesamtes Ensemble, Alert relevant")

        w = self._build_asa_warning(asa, mux, journaline_text=None)
        if w:
            await self.on_warning(w)

    async def _process_journaline(self, jl_data: dict):
        """
        Journaline-Daten auswerten.
        - Alle Objekte → RxLog
        - Ohne ASA → object_id zur Baseline hinzufügen
        - Mit ASA → nur neue Objekte (nicht in Baseline) + Keyword im TITEL
        """
        present = jl_data.get("present", False)
        service_type = jl_data.get("service_type", "unknown")
        apptype = jl_data.get("apptype", 0)
        objects = jl_data.get("objects", [])

        self._journaline_present = present
        self._journaline_service_type = service_type

        if not present or not objects:
            return

        now = datetime.now(timezone.utc)

        for obj in objects:
            object_id = obj.get("object_id", 0)
            obj_type = obj.get("type", "")
            title = obj.get("title", "")
            body = obj.get("body", "")

            # RxLog – immer
            log_entry = {
                "timestamp": now.isoformat(),
                "time": now.strftime("%H:%M:%S"),
                "service_type": service_type,
                "apptype": hex(apptype) if isinstance(apptype, int) else str(apptype),
                "object_id": object_id,
                "obj_type": obj_type,
                "title": title,
                "body": body[:500] if body else "",
                "asa_active": self._asa_active,
            }
            self._rxlog.append(log_entry)
            if len(self._rxlog) > RXLOG_MAX:
                self._rxlog.pop(0)

            # Nur Journaline-Objekte (appType 0x44a) weiterverarbeiten
            if service_type != "journaline":
                continue

            if not self._asa_active:
                # Kein ASA → Objekt zur Baseline hinzufügen (= bekannte Routine-Inhalte)
                self._baseline_ids.add(object_id)
                continue

            # ASA aktiv ab hier

            # Limit: max N neue Objekte pro Session
            if self._new_objects_this_session >= MAX_JOURNALINE_NEW_PER_ALERT:
                continue

            # Baseline-Check: bekannte Routine-Objekte ignorieren
            if object_id in self._baseline_ids:
                logger.debug("Journaline object_id=%d in Baseline – ignoriert", object_id)
                continue

            # Dedup innerhalb der Session
            if object_id in self._seen_journaline_ids:
                continue

            # Keyword-Check NUR im Titel (nicht im Body)
            title_lower = title.lower()
            matched = [kw for kw in self.keywords if kw in title_lower]

            if not matched:
                logger.debug("Journaline object_id=%d (neu, kein Keyword im Titel): %s",
                             object_id, title[:60])
                # Trotzdem als gesehen markieren damit wir nicht jedes Mal prüfen
                self._seen_journaline_ids.add(object_id)
                continue

            # Neues Objekt mit Keyword im Titel bei aktivem ASA → Warnung
            self._seen_journaline_ids.add(object_id)
            self._new_objects_this_session += 1

            logger.info("📋 Journaline-Warntext (ASA aktiv, neu, object_id=%d, keywords=%s): %s",
                        object_id, matched, title[:60])
            w = self._build_journaline_warning(title, body, object_id)
            if w:
                await self.on_warning(w)

    def _build_asa_warning(self, asa: dict, mux: dict,
                            journaline_text: Optional[str]) -> Optional[NormalizedWarning]:
        """Baut NormalizedWarning aus ASA-Block."""
        is_test = asa.get("is_test", False)
        status_str = "test" if is_test else "actual"

        if is_test and not self.forward_tests:
            logger.debug("DAB+ Test-Warnung übersprungen (forward_tests: false)")
            return None

        ensemble = mux.get("ensemble", {})
        ensemble_label = ensemble.get("label", {}).get("shortlabel", "DAB+").strip()

        has_region = asa.get("has_region", False)
        region_id = asa.get("region_id", 0)
        cluster_id = asa.get("cluster_id", 0)

        if has_region and region_id:
            area_desc = f"Region {region_id} (Cluster {cluster_id})"
        else:
            area_desc = "Baden-Württemberg (bundesweit)"

        prefix = "[TEST] " if is_test else ""
        headline = f"{prefix}DAB+ Katastrophenwarnung – {ensemble_label}"

        if journaline_text:
            description = journaline_text
        else:
            description = (
                "Offizielle Katastrophenwarnung über DAB+ empfangen. "
                "Vollständiger Warntext wird über den Rundfunk übertragen. "
                "Bitte Radio einschalten: " + ensemble_label + "."
            )

        instruction = "Offizielle Durchsagen im Radio beachten. Anweisungen der Behörden befolgen."
        last_change = asa.get("last_change", 0)
        identifier = f"dab-asa-{'test-' if is_test else ''}{last_change}"
        now = datetime.now(timezone.utc)

        return NormalizedWarning(
            source="dab",
            identifier=identifier,
            status=status_str,
            msg_type="Alert",
            severity="Extreme",
            urgency="Immediate",
            headline=headline,
            description=description,
            instruction=instruction,
            area_desc=area_desc,
            ags_codes=[],
            sent=now,
            effective=now,
            expires=None,
        )

    def _build_journaline_warning(self, title: str, body: str,
                                   object_id: int) -> Optional[NormalizedWarning]:
        """Baut NormalizedWarning aus Journaline-Objekt (ASA muss aktiv sein)."""
        is_test = self._asa_is_test
        status_str = "test" if is_test else "actual"

        if is_test and not self.forward_tests:
            logger.debug("Journaline Test-Warnung übersprungen (forward_tests: false)")
            return None

        now = datetime.now(timezone.utc)
        identifier = f"dab-journaline-{object_id}-{int(now.timestamp())}"

        headline = title if title else "DAB+ Katastrophenwarnung (Journaline)"
        description = body if body else title

        return NormalizedWarning(
            source="dab",
            identifier=identifier,
            status=status_str,
            msg_type="Alert",
            severity="Severe",
            urgency="Immediate",
            headline=headline,
            description=description,
            instruction="Anweisungen der Behörden befolgen.",
            area_desc="DAB+ Journaline",
            ags_codes=[],
            sent=now,
            effective=now,
            expires=None,
        )

    def get_rxlog(self) -> list[dict]:
        """RxLog zurückgeben (neueste zuerst)."""
        return list(reversed(self._rxlog))

    def status(self) -> dict:
        return {
            "status": self._status,
            "snr": self._last_snr,
            "poll_ok": self._last_poll_ok,
            "asa_active": self._active_since is not None,
            "asa_is_test": self._asa_is_test,
            "asa_level": self._asa_level,
            "ews_ensemble": self._ews_ensemble,
            "active_since": self._active_since.isoformat() if self._active_since else None,
            "channel": self.cfg.get("channel", "9D"),
            "welle_url": self.welle_url,
            "error_count": self._error_count,
            "watchdog_triggered": self._watchdog_triggered,
            "journaline_present": self._journaline_present,
            "journaline_service_type": self._journaline_service_type,
            "rxlog_count": len(self._rxlog),
            "baseline_count": len(self._baseline_ids),
            "geocode_filter": self.asa_geocode_filter or None,
            "receiver_zone": self._receiver_zone,
            "receiver_cc": f"0x{self._receiver_cc:06X}" if self._receiver_cc is not None else None,
        }
