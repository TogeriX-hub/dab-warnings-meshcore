"""
dab_listener.py – WarnBridge Phase 3c
Pollt die welle-cli HTTP-API alle 2 Sekunden.
- /mux.json: ASA-Flag, SNR
- /journaline.json: Journaline/TPEG/EPG Pakete

Auswertungslogik:
  ASA aktiv + service_type=="journaline" → NormalizedWarning mit echtem Text
  Kein ASA + service_type=="journaline" + Keywords → nur RxLog, kein Mesh
  service_type=="tpeg"/"epg" → nur RxLog

RxLog: alle empfangenen Pakete mit Typ, immer gespeichert (max 50 Einträge).
"""

import asyncio
import logging
from datetime import datetime, timezone
from typing import Callable, Awaitable, Optional

import aiohttp

from cap_normalizer import NormalizedWarning

logger = logging.getLogger(__name__)

CallbackType = Callable[[NormalizedWarning], Awaitable[None]]

POLL_INTERVAL = 2.0
MAX_ERRORS_BEFORE_WARN = 5
WATCHDOG_ERROR_THRESHOLD = 10
RXLOG_MAX = 50

REQUEST_TIMEOUT = aiohttp.ClientTimeout(
    total=8,
    connect=3,
    sock_read=5,
)

# Standard-Keywords für Journaline-Texte ohne aktives ASA-Flag
DEFAULT_KEYWORDS = [
    "warnung", "gefahr", "evakuierung", "katastrophe",
    "hochwasser", "unwetter", "alarm", "notfall",
]


class DabListener:
    def __init__(self, config: dict, on_warning: CallbackType):
        self.cfg = config
        self.on_warning = on_warning
        self.welle_url = config.get("welle_cli_url", "http://localhost:7979")
        self.forward_tests = config.get("forward_tests", False)
        self.asa_geocode_filter = config.get("asa_geocode", "")
        self.keywords = [k.lower() for k in config.get("journaline_keywords", DEFAULT_KEYWORDS)]

        self._last_change: int = -1
        self._active_since: Optional[datetime] = None
        self._running = False
        self._error_count = 0
        self._last_snr: Optional[float] = None
        self._last_poll_ok: bool = False
        self._status: str = "not_started"

        # Journaline-Status (zuletzt empfangener Zustand)
        self._journaline_present: bool = False
        self._journaline_service_type: str = ""

        # ASA-Zustand (für Journaline-Auswertung)
        self._asa_active: bool = False

        # RxLog: alle empfangenen Pakete (max RXLOG_MAX)
        self._rxlog: list[dict] = []

        # Dedup: bereits verarbeitete Journaline-object_ids pro ASA-Session
        self._seen_journaline_ids: set = set()

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

    async def _poll(self, session: aiohttp.ClientSession):
        """Pollt /mux.json und /journaline.json parallel."""
        mux_url = f"{self.welle_url}/mux.json"
        jl_url = f"{self.welle_url}/journaline.json"

        # Beide Endpunkte parallel abfragen
        mux_task = session.get(mux_url, timeout=REQUEST_TIMEOUT)
        jl_task = session.get(jl_url, timeout=REQUEST_TIMEOUT)

        async with mux_task as mux_resp, jl_task as jl_resp:
            if mux_resp.status != 200:
                raise ValueError(f"HTTP {mux_resp.status} von welle-cli (/mux.json)")
            mux_data = await mux_resp.json()

            # /journaline.json: 404 ist ok (Endpunkt noch nicht im Build)
            jl_data = None
            if jl_resp.status == 200:
                jl_data = await jl_resp.json()
            elif jl_resp.status != 404:
                logger.debug("DAB: /journaline.json HTTP %d", jl_resp.status)

        # Erfolgreicher Poll
        if self._error_count > 0:
            logger.info("DAB-Listener: welle-cli wieder erreichbar nach %d Fehlern",
                        self._error_count)
        self._error_count = 0
        self._watchdog_triggered = False
        self._last_poll_ok = True

        # SNR
        demod = mux_data.get("demodulator", {})
        self._last_snr = demod.get("snr")

        snr = self._last_snr or 0
        if snr < 5.0:
            if self._status != "no_signal":
                logger.warning("DAB+: SNR zu niedrig (%.1f dB)", snr)
            self._status = "no_signal"
            return

        self._status = "ok"

        # ASA auswerten
        asa = mux_data.get("asa", {})
        await self._process_asa(asa, mux_data)

        # Journaline auswerten
        if jl_data is not None:
            await self._process_journaline(jl_data)

    async def _process_asa(self, asa: dict, mux: dict):
        """ASA-Block auswerten, NormalizedWarning erzeugen wenn aktiv."""
        active = asa.get("active", False)
        last_change = asa.get("last_change", 0)

        self._asa_active = active

        # ASA beendet → seen_ids zurücksetzen für nächste Session
        if not active and self._active_since:
            duration = (datetime.now(timezone.utc) - self._active_since).seconds
            logger.info("DAB+ ASA beendet (Dauer: %ds)", duration)
            self._active_since = None
            self._seen_journaline_ids.clear()

        # Keine Änderung
        if last_change == self._last_change:
            return

        prev_change = self._last_change
        self._last_change = last_change

        # Beim ersten Poll ohne aktiven Alarm: nichts tun
        if prev_change == -1 and not active:
            logger.debug("DAB-Listener: Initialer Poll, kein aktiver Alarm")
            return

        if active:
            self._active_since = datetime.now(timezone.utc)
            is_test = asa.get("is_test", False)
            status_str = asa.get("status", "actual")
            region_id = asa.get("region_id", 0)
            has_region = asa.get("has_region", False)
            cluster_id = asa.get("cluster_id", 0)
            emergency = asa.get("emergency_warning", False)

            logger.info(
                "🚨 DAB+ ASA AKTIV | test=%s status=%s region_id=%s has_region=%s cluster=%s emergency=%s",
                is_test, status_str, region_id, has_region, cluster_id, emergency
            )

            # Nur generische Warnung senden wenn noch keine Journaline-Objekte da sind.
            # Wenn Journaline vorhanden, übernimmt _process_journaline die NormalizedWarning.
            # Wir senden hier immer eine initiale Warnung, Journaline-Text ergänzt/ersetzt sie.
            w = self._build_asa_warning(asa, mux, journaline_text=None)
            if w:
                await self.on_warning(w)

    async def _process_journaline(self, jl_data: dict):
        """
        Journaline-Daten auswerten.
        Alles wird in den RxLog geschrieben.
        Nur bei service_type=="journaline" + ASA aktiv → NormalizedWarning.
        Kein ASA + Keywords → nur RxLog.
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

            # RxLog-Eintrag – immer, mit allen Infos
            log_entry = {
                "timestamp": now.isoformat(),
                "time": now.strftime("%H:%M:%S"),
                "service_type": service_type,
                "apptype": hex(apptype) if isinstance(apptype, int) else str(apptype),
                "object_id": object_id,
                "obj_type": obj_type,
                "title": title,
                "body": body[:500] if body else "",  # Max 500 Zeichen im Log
                "asa_active": self._asa_active,
            }
            self._rxlog.append(log_entry)
            if len(self._rxlog) > RXLOG_MAX:
                self._rxlog.pop(0)

            # Nur Journaline-Objekte (appType 0x44a) weiterverarbeiten
            if service_type != "journaline":
                logger.debug("RxLog: %s apptype=%s object_id=%d – nur geloggt",
                             service_type, log_entry["apptype"], object_id)
                continue

            # Dedup: bereits verarbeitetes Objekt?
            if object_id in self._seen_journaline_ids:
                continue

            text_combined = f"{title} {body}".strip()

            if self._asa_active:
                # ASA aktiv + Journaline → NormalizedWarning mit echtem Text
                self._seen_journaline_ids.add(object_id)
                logger.info("📋 Journaline-Warntext empfangen (ASA aktiv, object_id=%d): %s",
                            object_id, title[:60])
                w = self._build_journaline_warning(title, body, object_id)
                if w:
                    await self.on_warning(w)

            else:
                # Kein ASA – Keyword-Check
                text_lower = text_combined.lower()
                matched = [kw for kw in self.keywords if kw in text_lower]
                if matched:
                    logger.warning(
                        "📋 Journaline ohne ASA – Keywords gefunden %s (object_id=%d): %s",
                        matched, object_id, title[:60]
                    )
                    # Nur loggen, NICHT ins Mesh senden
                    # Das Log-Entry ist bereits im RxLog oben gespeichert

    def _build_asa_warning(self, asa: dict, mux: dict,
                            journaline_text: Optional[str]) -> Optional[NormalizedWarning]:
        """Baut NormalizedWarning aus ASA-Block (ohne oder mit Journaline-Text)."""
        is_test = asa.get("is_test", False)
        status_str = "test" if is_test else "actual"

        if is_test and not self.forward_tests:
            logger.debug("DAB+ Test-Warnung übersprungen (forward_tests: false)")
            return None

        ensemble = mux.get("ensemble", {})
        ensemble_label = ensemble.get("label", {}).get("shortlabel", "SWR BW N").strip()

        has_region = asa.get("has_region", False)
        region_id = asa.get("region_id", 0)
        cluster_id = asa.get("cluster_id", 0)
        emergency = asa.get("emergency_warning", False)

        if has_region and region_id:
            area_desc = f"Region {region_id} (Cluster {cluster_id})"
        else:
            area_desc = "Baden-Württemberg (bundesweit)"

        severity = "Extreme" if emergency else "Severe"
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
            severity=severity,
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
        is_test = self.cfg.get("forward_tests", False)  # Wenn Tests weitergeleitet werden
        # is_test aus ASA-Status ableiten wäre besser – nutze letzten bekannten Zustand
        # (ASA is_test wird in _process_asa nicht persistent gespeichert – TODO wenn nötig)

        now = datetime.now(timezone.utc)
        identifier = f"dab-journaline-{object_id}-{int(now.timestamp())}"

        # Headline aus Journaline-Titel, Beschreibung aus Body
        headline = title if title else "DAB+ Katastrophenwarnung (Journaline)"
        description = body if body else title

        return NormalizedWarning(
            source="dab",
            identifier=identifier,
            status="actual",
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
            "active_since": self._active_since.isoformat() if self._active_since else None,
            "channel": self.cfg.get("channel", "9D"),
            "welle_url": self.welle_url,
            "error_count": self._error_count,
            "watchdog_triggered": self._watchdog_triggered,
            "journaline_present": self._journaline_present,
            "journaline_service_type": self._journaline_service_type,
            "rxlog_count": len(self._rxlog),
        }
