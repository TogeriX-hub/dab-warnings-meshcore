"""
warnbridge.py – WarnBridge Hauptprogramm
asyncio Event-Loop. Verbindet NinaPoller → DedupCache → WarningsDB → MeshSender.
DAB-Listener kommt in Phase 3 dazu.
"""

import asyncio
import logging
import signal
import subprocess
import sys
from datetime import datetime, timezone
from pathlib import Path

import yaml

from cap_normalizer import NormalizedWarning
from dedup import DedupCache
from warnings_db import WarningsDB
from nina_poller import NinaPoller
from mesh_sender import MeshSender
from bot_handler import BotHandler
from dab_listener import DabListener
import ags_lookup

logger = logging.getLogger(__name__)

# welle-cli Watchdog: wie oft pro Stunde darf neu gestartet werden
WELLE_RESTART_COOLDOWN_SECONDS = 120   # min. 2 Minuten zwischen Neustarts
WELLE_WATCHDOG_CHECK_INTERVAL = 5      # alle 5s prüfen ob Watchdog ausgelöst


def load_config(path: str = "config.yaml") -> dict:
    with open(path, "r", encoding="utf-8") as f:
        return yaml.safe_load(f)


def setup_logging(level: str = "INFO"):
    logging.basicConfig(
        level=getattr(logging, level.upper(), logging.INFO),
        format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
        datefmt="%H:%M:%S",
    )



def _dict_to_warning(entry: dict) -> "NormalizedWarning":
    """Rekonstruiert eine NormalizedWarning aus einem DB-Dict."""
    from datetime import datetime, timezone
    def _dt(s):
        if not s: return None
        try: return datetime.fromisoformat(s)
        except: return None
    return NormalizedWarning(
        source=entry.get("source", ""),
        identifier=entry.get("identifier", ""),
        status=entry.get("status", "actual"),
        msg_type=entry.get("msg_type", "Alert"),
        severity=entry.get("severity", "Minor"),
        urgency=entry.get("urgency", "Expected"),
        headline=entry.get("headline", ""),
        description=entry.get("description", ""),
        instruction=entry.get("instruction", ""),
        area_desc=entry.get("area_desc", ""),
        ags_codes=entry.get("ags_codes", []),
        sent=_dt(entry.get("sent")) or datetime.now(timezone.utc),
        effective=_dt(entry.get("effective")),
        expires=_dt(entry.get("expires")),
        dwd_event_type=entry.get("dwd_event_type"),
        dwd_level=entry.get("dwd_level"),
        content_hash=entry.get("content_hash", ""),
    )

class WarnBridge:
    def __init__(self, config: dict):
        self.cfg = config
        self.start_time = datetime.now(timezone.utc)

        # Module initialisieren
        dedup_cfg = config.get("dedup", {})
        self.dedup = DedupCache(
            ttl_hours=dedup_cfg.get("id_cache_ttl_hours", 24),
            use_content_hash=dedup_cfg.get("content_hash", True),
            max_per_hour=dedup_cfg.get("max_per_hour", 5),
        )

        db_cfg = config.get("warnings_db", {})
        self.db = WarningsDB(ttl_hours=db_cfg.get("ttl_hours", 48))

        self.mesh = MeshSender(config.get("meshcore", {}))
        self.nina = NinaPoller(config.get("nina", {}), on_warning=self.handle_warning)
        self.dab = DabListener(config.get("dab", {}), on_warning=self.handle_warning)
        self.bot = BotHandler(self)

        # web_ui wird in Phase 4 hier eingebunden
        self.web_ui = None

        self._tasks: list[asyncio.Task] = []

        # Broadcasting-Schalter: standardmäßig AUS.
        # Muss im Dashboard manuell aktiviert werden.
        # Wird bei Config-Reload automatisch zurückgesetzt.
        self.broadcasting_enabled: bool = False
        # In-Memory Cache für Broadcast-Dedup (verhindert gleiche Warnung mehrfach senden)
        # Wird bei Config-Reload geleert damit neue Kreise frisch prüfen können
        self._broadcast_sent_hashes: set[str] = set()

        # Zähler für tatsächlich gesendete Mesh-Nachrichten (letzte 60 Minuten)
        # Unabhängig von Dedup – wird nur bei erfolgreichem mesh.send_warning() befüllt
        self._mesh_sent_timestamps: list[datetime] = []

        # welle-cli Watchdog State
        self._welle_last_restart: Optional[datetime] = None
        self._welle_restart_count: int = 0

    async def handle_warning(self, w: NormalizedWarning):
        """
        Zentraler Handler für alle eingehenden Warnungen (NINA + DAB+).
        Dedup → DB speichern → Mesh senden.
        """
        logger.debug("Neue Warnung: [%s] %s", w.source, w.headline)

        # Test-Warnungen: nur weiterleiten wenn forward_tests: true
        if w.status == "test":
            forward_tests = self.cfg.get("dab", {}).get("forward_tests", False)
            if not forward_tests:
                logger.debug("Test-Warnung übersprungen: %s", w.headline)
                return

        # Dedup-Prüfung
        if self.dedup.is_duplicate(w.identifier, w.content_hash):
            logger.debug("Duplikat übersprungen: %s", w.identifier)
            return

        # Broadcast-Filter: nur Auto-Alert wenn Kreis in broadcast_districts
        should_broadcast = self._should_broadcast(w)

        # In DB speichern (immer, für /warnings [Ort] Abfragen)
        db_id = self.db.store(w, broadcast_sent=should_broadcast)

        # Als gesehen markieren
        self.dedup.mark_seen(w.identifier, w.content_hash)

        if should_broadcast:
            w_send = self._prepare_for_broadcast(w)
            # Broadcast-Dedup: gleiche Warnung für gleichen Kreis nur 1x senden
            # Hash aus headline + Minute + area_desc (= konfigurierter Kreis)
            # Hash auf Stunde (nicht Minute) – alle Gewitter-Warnungen innerhalb
            # einer Stunde für denselben Broadcast-Kreis = eine Nachricht
            sent_hour = w.sent.strftime("%Y-%m-%dT%H") if w.sent else ""
            broadcast_hash = f"{w.headline.lower()}|{sent_hour}|{w_send.area_desc}"
            import hashlib as _hl
            b_hash = _hl.md5(broadcast_hash.encode()).hexdigest()
            if self.is_broadcast_duplicate(b_hash):
                logger.debug("Broadcast-Duplikat übersprungen: %s | %s",
                             w.headline, w_send.area_desc)
            elif self.dedup.is_rate_limited():
                logger.warning("Rate-Limit – Warnung nicht gesendet: %s", w.headline)
            else:
                self.mark_broadcast_seen(b_hash)
                logger.info("🚨 WARNUNG [%s] %s → Mesh | Gebiet: %s",
                            w.source.upper(), w.headline, w_send.area_desc)
                success = await self.mesh.send_warning(w_send)
                if success:
                    self.db.mark_broadcast_sent(db_id)
                    self.dedup.mark_sent()  # Rate-Limit-Zähler: nur echte Sends
                    # Zähler für "gesendet 1h" – nur echte Mesh-Sends
                    now_ts = datetime.now(timezone.utc)
                    self._mesh_sent_timestamps.append(now_ts)
                    # Einträge älter als 60 Minuten rauswerfen
                    self._mesh_sent_timestamps = [
                        t for t in self._mesh_sent_timestamps
                        if (now_ts - t).total_seconds() < 3600
                    ]
                delay = 1 if self.mesh.simulator else 10
                await asyncio.sleep(delay)  # Rate-Limiting: 1s Simulator, 10s Mesh
        else:
            logger.info("📦 Warnung gespeichert (kein Broadcast) [%s] %s | Gebiet: %s",
                        w.source, w.headline, w.area_desc)

        # WebSocket-Broadcast ans Dashboard (neue Warnung in DB)
        if self.web_ui and hasattr(self.web_ui, 'broadcast_warning'):
            await self.web_ui.broadcast_warning(w, should_broadcast)

    def _should_broadcast(self, w: NormalizedWarning) -> bool:
        """
        Prüft ob die Warnung automatisch ins Mesh gesendet werden soll.
        Broadcasting muss im Dashboard aktiv sein (globaler Schalter).
        """
        if not self.broadcasting_enabled:
            return False
        return self._passes_broadcast_filters(w)

    def _passes_broadcast_filters(self, w: NormalizedWarning) -> bool:
        """Kreis- + Quell-Filter ohne Broadcasting-Flag-Check (für Nachsende-Logik)."""
        nina_cfg = self.cfg.get("nina", {})
        broadcast_districts = nina_cfg.get("broadcast_districts", [])
        sources_cfg = nina_cfg.get("sources", {})

        # Kreis-Check
        if broadcast_districts:
            if w.ags_codes:
                # AGS-Codes vorhanden: prüfen ob ein betroffener Kreis in broadcast_districts
                kreis_ok = any(
                    ags.startswith(district[:5])
                    for ags in w.ags_codes
                    for district in broadcast_districts
                )
            else:
                # Keine AGS-Codes (Dashboard-Format): nicht senden
                # Lieber eine echte Warnung verpassen als falsch senden
                kreis_ok = False
            if not kreis_ok:
                return False

        # Quell-Filter
        if w.source == "dwd":
            return self._passes_dwd_filter(w, sources_cfg.get("dwd", {}))
        elif w.source == "mowas":
            return self._passes_mowas_filter(w, sources_cfg.get("mowas", {}))
        return True

    def _passes_dwd_filter(self, w: NormalizedWarning, src_cfg: dict) -> bool:
        """DWD: Event-Typ aktiviert + min_level erreicht."""
        events_cfg = src_cfg.get("events", {})
        event_type = w.dwd_event_type
        if not event_type:
            return False
        event_cfg = events_cfg.get(event_type, {})
        if not event_cfg.get("enabled", False):
            return False
        return (w.dwd_level or 1) >= event_cfg.get("min_level", 2)

    def _passes_mowas_filter(self, w: NormalizedWarning, src_cfg: dict) -> bool:
        """MoWaS: min_severity erreicht."""
        severity_order = ["Minor", "Moderate", "Severe", "Extreme"]
        min_sev = src_cfg.get("min_severity", "Severe")
        try:
            return severity_order.index(w.severity) >= severity_order.index(min_sev)
        except ValueError:
            return False
    def is_broadcast_duplicate(self, b_hash: str) -> bool:
        """Prüft ob diese Warnung für diesen Kreis bereits gesendet wurde."""
        return b_hash in self._broadcast_sent_hashes

    def mark_broadcast_seen(self, b_hash: str):
        """Markiert diese Warnung+Kreis-Kombination als gesendet."""
        self._broadcast_sent_hashes.add(b_hash)

    def _prepare_for_broadcast(self, w: NormalizedWarning) -> NormalizedWarning:
        """
        Bereitet eine Warnung für den Broadcast vor:
        - area_desc wird durch den konfigurierten Broadcast-Kreis ersetzt
          (nicht den zufälligen ersten betroffenen Kreis)
        """
        import copy
        import ags_lookup as _ags
        nina_cfg = self.cfg.get("nina", {})
        broadcast_districts = nina_cfg.get("broadcast_districts", [])
        if not broadcast_districts:
            return w

        # Welcher konfigurierte Kreis ist betroffen?
        matched_district = None
        for district in broadcast_districts:
            if w.ags_codes and any(ags.startswith(district[:5]) for ags in w.ags_codes):
                matched_district = district
                break

        if not matched_district:
            return w

        # Kreisnamen aus AGS-Lookup
        district_name = _ags.district_name(matched_district) or matched_district

        # Kopie mit angepasstem area_desc
        w2 = copy.copy(w)
        w2.area_desc = district_name
        return w2



    async def on_broadcasting_enabled(self):
        """
        Wird aufgerufen wenn Broadcasting eingeschaltet wird.
        Sendet Warnungen nach die in der DB gespeichert aber noch nicht gesendet
        wurden und die aktuellen Filter jetzt passieren würden.
        """
        logger.info("Broadcasting AN – prüfe DB auf ausstehende Warnungen...")
        try:
            pending = self.db.get_unsent(hours=48)
        except Exception as e:
            logger.error("DB-Abfrage fehlgeschlagen: %s", e)
            return

        if not pending:
            logger.info("Keine ausstehenden Warnungen.")
            return

        logger.info("%d ausstehende Warnungen – prüfe Filter...", len(pending))
        sent_count = 0

        for entry in pending:
            try:
                w = _dict_to_warning(entry)
            except Exception as e:
                logger.debug("Warnung nicht rekonstruierbar: %s", e)
                continue

            if not self._passes_broadcast_filters(w):
                continue

            w_send = self._prepare_for_broadcast(w)
            logger.info("📤 Nachsenden [%s] %s → Gebiet: %s",
                        w.source.upper(), w.headline, w_send.area_desc)
            success = await self.mesh.send_warning(w_send)
            if success:
                self.db.mark_broadcast_sent(entry["db_id"])
                self.dedup.mark_sent()
                now_ts = datetime.now(timezone.utc)
                self._mesh_sent_timestamps.append(now_ts)
                self._mesh_sent_timestamps = [
                    t for t in self._mesh_sent_timestamps
                    if (now_ts - t).total_seconds() < 3600
                ]
                sent_count += 1
                delay = 1 if self.mesh.simulator else 10
                await asyncio.sleep(delay)  # Rate-Limiting: 1s Simulator, 10s Mesh

        logger.info("Broadcasting-Start: %d Warnungen nachgesendet.", sent_count)

    async def start(self):
        logger.info("WarnBridge startet...")

        # AGS-Lookup laden (einmalig, cached lokal)
        await asyncio.get_event_loop().run_in_executor(None, ags_lookup.load)

        # MeshCore verbinden
        await self.mesh.connect()

        # Web-UI starten (wenn konfiguriert)
        if self.web_ui:
            self._tasks.append(asyncio.create_task(self.web_ui.run(), name="web_ui"))

        # NINA Poller starten
        self._tasks.append(asyncio.create_task(self.nina.run(), name="nina_poller"))

        # DAB-Listener starten
        self._tasks.append(asyncio.create_task(self.dab.run(), name="dab_listener"))

        # Täglicher Cleanup
        self._tasks.append(asyncio.create_task(self._cleanup_loop(), name="cleanup"))

        # Mesh Reconnect-Wächter
        self._tasks.append(asyncio.create_task(self._mesh_reconnect_loop(), name="mesh_reconnect"))

        # welle-cli Watchdog
        self._tasks.append(asyncio.create_task(self._welle_watchdog_loop(), name="welle_watchdog"))

        logger.info("WarnBridge läuft. Simulator: %s",
                    self.cfg.get("meshcore", {}).get("simulator", True))

        try:
            await asyncio.gather(*self._tasks)
        except asyncio.CancelledError:
            logger.info("WarnBridge wird beendet...")

    async def stop(self):
        logger.info("WarnBridge stoppt...")
        for task in self._tasks:
            task.cancel()
        self.nina.stop()
        self.dab.stop()

    async def _cleanup_loop(self):
        """Täglich expired entries löschen."""
        while True:
            await asyncio.sleep(3600 * 6)
            try:
                self.dedup.cleanup_expired()
                self.db.cleanup_expired()
            except Exception as e:
                logger.error("Cleanup Fehler: %s", e)

    async def _mesh_reconnect_loop(self):
        """Alle 30s reconnecten wenn Mesh getrennt und nicht im Simulator-Modus."""
        while True:
            await asyncio.sleep(30)
            try:
                if not self.mesh.simulator and not self.mesh._connected:
                    logger.info("MeshCore getrennt – versuche Reconnect...")
                    await self.mesh.connect()
            except Exception as e:
                logger.error("Mesh Reconnect Fehler: %s", e)

    async def _welle_watchdog_loop(self):
        """
        Überwacht den DAB-Listener. Wenn welle-cli einfriert (Watchdog ausgelöst),
        wird der Prozess beendet und neu gestartet.
        Nur aktiv wenn welle_cli_autostart: true in config.yaml (dab-Block).
        """
        dab_cfg = self.cfg.get("dab", {})
        autostart = dab_cfg.get("welle_cli_autostart", False)

        if not autostart:
            logger.debug("welle-cli Watchdog inaktiv (welle_cli_autostart: false)")
            return

        welle_cmd = dab_cfg.get("welle_cli_cmd", "")
        if not welle_cmd:
            logger.warning("welle-cli Watchdog: welle_cli_cmd nicht konfiguriert – Watchdog inaktiv")
            return

        logger.info("welle-cli Watchdog aktiv (cmd: %s)", welle_cmd)

        while True:
            await asyncio.sleep(WELLE_WATCHDOG_CHECK_INTERVAL)

            if not self.dab.watchdog_triggered:
                continue

            # Cooldown prüfen
            now = datetime.now(timezone.utc)
            if self._welle_last_restart:
                elapsed = (now - self._welle_last_restart).total_seconds()
                if elapsed < WELLE_RESTART_COOLDOWN_SECONDS:
                    remaining = int(WELLE_RESTART_COOLDOWN_SECONDS - elapsed)
                    logger.debug("welle-cli Watchdog: Cooldown aktiv, noch %ds", remaining)
                    continue

            # welle-cli neu starten
            self._welle_restart_count += 1
            self._welle_last_restart = now
            logger.warning("welle-cli Watchdog: Neustart #%d wird durchgeführt...",
                           self._welle_restart_count)

            try:
                # Alten Prozess beenden
                subprocess.run(["pkill", "-f", "welle-cli"], capture_output=True)
                await asyncio.sleep(2)

                # Neu starten (im Hintergrund, non-blocking)
                subprocess.Popen(
                    welle_cmd,
                    shell=True,
                    stdout=subprocess.DEVNULL,
                    stderr=subprocess.DEVNULL,
                )
                logger.info("welle-cli neu gestartet (cmd: %s)", welle_cmd)

                # DAB-Listener Watchdog zurücksetzen
                await asyncio.sleep(3)  # kurz warten bis welle-cli hochfährt
                self.dab.reset_watchdog()
                logger.info("welle-cli Watchdog: DAB-Listener zurückgesetzt")

            except Exception as e:
                logger.error("welle-cli Watchdog: Neustart fehlgeschlagen: %s", e)

    def status(self) -> dict:
        uptime_seconds = int((datetime.now(timezone.utc) - self.start_time).total_seconds())
        # Einträge älter als 60 Minuten aus dem Zähler entfernen
        now_ts = datetime.now(timezone.utc)
        self._mesh_sent_timestamps = [
            t for t in self._mesh_sent_timestamps
            if (now_ts - t).total_seconds() < 3600
        ]
        return {
            "uptime_seconds": uptime_seconds,
            "uptime_human": _format_uptime(uptime_seconds),
            "mesh_sent_1h": len(self._mesh_sent_timestamps),
            "nina": self.nina.status(),
            "mesh": self.mesh.status(),
            "dedup": self.dedup.stats(),
            "db": self.db.stats(),
            "dab": self.dab.status(),
            "welle_watchdog": {
                "restart_count": self._welle_restart_count,
                "last_restart": self._welle_last_restart.isoformat() if self._welle_last_restart else None,
            },
        }


def _format_uptime(seconds: int) -> str:
    h = seconds // 3600
    m = (seconds % 3600) // 60
    s = seconds % 60
    if h > 0:
        return f"{h}h {m}m"
    elif m > 0:
        return f"{m}m {s}s"
    return f"{s}s"


async def main():
    setup_logging("INFO")

    config_path = "config.yaml"
    if not Path(config_path).exists():
        logger.error("config.yaml nicht gefunden!")
        sys.exit(1)

    config = load_config(config_path)
    app = WarnBridge(config)

    # Web-UI einbinden (Phase 4)
    try:
        from web_ui import WebUI
        app.web_ui = WebUI(app, config)
        app.mesh.set_ws_broadcast(app.web_ui.ws_broadcast)
        logger.info("WebUI geladen (Port 8080)")
    except ImportError:
        logger.info("web_ui.py nicht gefunden – ohne Dashboard")

    # Graceful shutdown
    loop = asyncio.get_running_loop()

    def _shutdown():
        logger.info("Signal empfangen, beende...")
        asyncio.create_task(app.stop())

    for sig in (signal.SIGINT, signal.SIGTERM):
        try:
            loop.add_signal_handler(sig, _shutdown)
        except NotImplementedError:
            pass  # Windows

    await app.start()


if __name__ == "__main__":
    asyncio.run(main())
