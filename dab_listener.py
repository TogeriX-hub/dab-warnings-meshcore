"""
dab_listener.py – WarnBridge Phase 3
Pollt die welle-cli HTTP-API alle 2 Sekunden.
Wertet ASA-Flag aus: active, is_test, status, region_id, has_region, last_change.
Erzeugt NormalizedWarning und ruft on_warning-Callback auf.

Journaline-Volltext ist über die HTTP-API nicht verfügbar.
Die NormalizedWarning enthält daher eine generische Beschreibung.
Am Warntag (10.09.2026) wird der echte Warntext über Radio/Journaline übertragen –
WarnBridge gibt im Mesh den Hinweis aus, Radio einzuschalten.
"""

import asyncio
import logging
from datetime import datetime, timezone
from typing import Callable, Awaitable, Optional

import aiohttp

from cap_normalizer import NormalizedWarning

logger = logging.getLogger(__name__)

# Typ des Callbacks: async Funktion die eine NormalizedWarning entgegennimmt
CallbackType = Callable[[NormalizedWarning], Awaitable[None]]

# Poll-Intervall in Sekunden
POLL_INTERVAL = 2.0

# Maximale Fehler in Folge bevor wir eine Warnung loggen
MAX_ERRORS_BEFORE_WARN = 5

# Nach dieser Anzahl Fehler in Folge → Watchdog-Signal auslösen
WATCHDOG_ERROR_THRESHOLD = 10

# Timeout-Einstellungen (getrennt damit hängende TCP-Verbindungen erkannt werden)
REQUEST_TIMEOUT = aiohttp.ClientTimeout(
    total=8,        # Gesamtlimit inkl. connect + read
    connect=3,      # TCP-Verbindungsaufbau max 3s
    sock_read=5,    # Antwort lesen max 5s
)


class DabListener:
    def __init__(self, config: dict, on_warning: CallbackType):
        """
        config: der dab-Block aus config.yaml
        on_warning: async Callback bei neuer/geänderter ASA-Warnung
        """
        self.cfg = config
        self.on_warning = on_warning
        self.welle_url = config.get("welle_cli_url", "http://localhost:7979")
        self.forward_tests = config.get("forward_tests", False)
        self.asa_geocode_filter = config.get("asa_geocode", "")  # Optional: z.B. "DE-BW"

        # Interner Zustand
        self._last_change: int = -1          # last_change-Timestamp aus letztem Poll
        self._active_since: Optional[datetime] = None
        self._running = False
        self._error_count = 0
        self._last_snr: Optional[float] = None
        self._last_poll_ok: bool = False
        self._status: str = "not_started"    # "not_started", "ok", "no_signal", "error"

        # Watchdog: wird von warnbridge.py abgefragt
        self._watchdog_triggered = False

    async def run(self):
        """Läuft dauerhaft, pollt alle POLL_INTERVAL Sekunden."""
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
                        logger.warning("DAB-Listener: Timeout bei welle-cli (%ds) – Versuch %d",
                                       int(REQUEST_TIMEOUT.total), self._error_count)
                    elif self._error_count % 30 == 0:
                        logger.warning("DAB-Listener: %d Timeouts in Folge – welle-cli hängt?",
                                       self._error_count)
                    # Watchdog auslösen wenn Schwelle erreicht
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
                        logger.warning("DAB-Listener: %d Fehler in Folge – welle-cli erreichbar?",
                                       self._error_count)
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
        """Vom Watchdog in warnbridge.py aufgerufen nachdem welle-cli neu gestartet wurde."""
        self._watchdog_triggered = False
        self._error_count = 0
        self._last_change = -1  # Beim Neustart nochmal Initialzustand annehmen

    @property
    def watchdog_triggered(self) -> bool:
        return self._watchdog_triggered

    async def _poll(self, session: aiohttp.ClientSession):
        """Einmal /mux.json abfragen und ASA-Block auswerten."""
        url = f"{self.welle_url}/mux.json"
        async with session.get(url, timeout=REQUEST_TIMEOUT) as resp:
            if resp.status != 200:
                raise ValueError(f"HTTP {resp.status} von welle-cli")
            data = await resp.json()

        # Erfolgreicher Poll: Zähler und Watchdog zurücksetzen
        if self._error_count > 0:
            logger.info("DAB-Listener: welle-cli wieder erreichbar nach %d Fehlern",
                        self._error_count)
        self._error_count = 0
        self._watchdog_triggered = False
        self._last_poll_ok = True

        # SNR aus demodulator
        demod = data.get("demodulator", {})
        self._last_snr = demod.get("snr")

        # Empfangsqualität prüfen
        snr = self._last_snr or 0
        if snr < 5.0:
            if self._status != "no_signal":
                logger.warning("DAB+: SNR zu niedrig (%.1f dB) – kein stabiler Empfang", snr)
            self._status = "no_signal"
            return

        self._status = "ok"

        # ASA-Block auswerten
        asa = data.get("asa", {})
        active = asa.get("active", False)
        last_change = asa.get("last_change", 0)
        is_test = asa.get("is_test", False)
        status_str = asa.get("status", "actual")
        region_id = asa.get("region_id", 0)
        has_region = asa.get("has_region", False)
        cluster_id = asa.get("cluster_id", 0)
        emergency_warning = asa.get("emergency_warning", False)

        # Keine Änderung seit letztem Poll → nichts tun
        if last_change == self._last_change:
            return

        prev_change = self._last_change
        self._last_change = last_change

        # Beim allerersten Poll (prev_change == -1) keinen Alarm auslösen
        # wenn kein aktiver Alarm vorliegt – verhindert false positives beim Start
        if prev_change == -1 and not active:
            logger.debug("DAB-Listener: Initialer Poll, kein aktiver Alarm")
            return

        if active:
            self._active_since = datetime.now(timezone.utc)
            logger.info(
                "🚨 DAB+ ASA AKTIV | test=%s status=%s region_id=%s has_region=%s cluster=%s emergency=%s",
                is_test, status_str, region_id, has_region, cluster_id, emergency_warning
            )
            w = self._build_warning(asa, data)
            if w:
                await self.on_warning(w)
        else:
            # Alarm wurde beendet
            if self._active_since:
                duration = (datetime.now(timezone.utc) - self._active_since).seconds
                logger.info("DAB+ ASA beendet (Dauer: %ds)", duration)
                self._active_since = None

    def _build_warning(self, asa: dict, mux: dict) -> Optional[NormalizedWarning]:
        """Baut eine NormalizedWarning aus dem ASA-Block."""
        is_test = asa.get("is_test", False)
        status_str = "test" if is_test else "actual"

        # Test-Warnungen: nur weiterleiten wenn forward_tests: true
        if is_test and not self.forward_tests:
            logger.debug("DAB+ Test-Warnung übersprungen (forward_tests: false)")
            return None

        # Ensemble-Name für Kontext
        ensemble = mux.get("ensemble", {})
        ensemble_label = ensemble.get("label", {}).get("shortlabel", "SWR BW N").strip()

        # Region aus ASA-Feldern ableiten
        has_region = asa.get("has_region", False)
        region_id = asa.get("region_id", 0)
        cluster_id = asa.get("cluster_id", 0)

        # Optionaler Geocode-Filter
        if self.asa_geocode_filter and has_region:
            logger.debug("DAB+ ASA region_id=%d, geocode_filter=%s – durchgelassen",
                         region_id, self.asa_geocode_filter)

        # Gebietsbeschreibung
        if has_region and region_id:
            area_desc = f"Region {region_id} (Cluster {cluster_id})"
        else:
            area_desc = "Baden-Württemberg (bundesweit)"

        # Severity: emergency_warning = höchste Stufe
        emergency = asa.get("emergency_warning", False)
        severity = "Extreme" if emergency else "Severe"

        # Headline und Beschreibung
        prefix = "[TEST] " if is_test else ""
        headline = f"{prefix}DAB+ Katastrophenwarnung – {ensemble_label}"

        description = (
            "Offizielle Katastrophenwarnung über DAB+ empfangen. "
            "Vollständiger Warntext wird über den Rundfunk übertragen. "
            "Bitte Radio einschalten: " + ensemble_label + ". "
            "Weitere Details per /details abrufbar sobald verfügbar."
        )

        instruction = (
            "Offizielle Durchsagen im Radio beachten. "
            "Anweisungen der Behörden befolgen."
        )

        # Eindeutige ID aus last_change (verhindert Duplikate bei erneutem Poll)
        last_change = asa.get("last_change", 0)
        identifier = f"dab-asa-{last_change}"
        if is_test:
            identifier = f"dab-asa-test-{last_change}"

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
            ags_codes=[],   # ASA liefert keine AGS-Codes – broadcast_filter greift dann nicht
            sent=now,
            effective=now,
            expires=None,
        )

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
        }
