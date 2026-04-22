"""
web_ui.py – WarnBridge Dashboard Server
aiohttp, Port 8080.
REST-API + WebSocket für Live-Updates und Simulator.
"""

import asyncio
import json
import logging
import shutil
import subprocess
from datetime import datetime, timezone
from pathlib import Path
from typing import Optional, TYPE_CHECKING

import aiohttp
from aiohttp import web

if TYPE_CHECKING:
    from warnbridge import WarnBridge

logger = logging.getLogger(__name__)

DASHBOARD_PATH = Path(__file__).parent / "dashboard.html"


class WebUI:
    def __init__(self, app: "WarnBridge", config: dict):
        self.app = app
        self.config = config
        self.port = 8080
        self._ws_clients: set[web.WebSocketResponse] = set()
        self._runner: Optional[web.AppRunner] = None

    async def run(self):
        aio_app = web.Application()
        aio_app.router.add_get("/api/ags/states", self._api_ags_states)
        aio_app.router.add_get("/api/ags/districts", self._api_ags_districts)
        aio_app.router.add_post("/api/simulator/clear", self._api_sim_clear)
        aio_app.router.add_post("/api/dedup/clear", self._api_dedup_clear)
        aio_app.router.add_get("/", self._handle_index)
        aio_app.router.add_get("/dashboard.html", self._handle_index)
        aio_app.router.add_get("/ws", self._handle_ws)
        aio_app.router.add_get("/api/status", self._api_status)
        aio_app.router.add_get("/api/warnings", self._api_warnings)
        aio_app.router.add_get("/api/sent", self._api_sent)
        aio_app.router.add_get("/api/config", self._api_config_get)
        aio_app.router.add_post("/api/config", self._api_config_post)
        aio_app.router.add_post("/api/simulator/trigger", self._api_sim_trigger)
        aio_app.router.add_get("/api/broadcasting", self._api_broadcasting_get)
        aio_app.router.add_post("/api/broadcasting", self._api_broadcasting_post)
        aio_app.router.add_get("/api/rxlog", self._api_rxlog)
        aio_app.router.add_get("/api/welle/status", self._api_welle_status)
        aio_app.router.add_post("/api/welle/start", self._api_welle_start)
        aio_app.router.add_post("/api/welle/stop", self._api_welle_stop)
        aio_app.router.add_post("/api/welle/channel", self._api_welle_channel)

        self._runner = web.AppRunner(aio_app)
        await self._runner.setup()
        site = web.TCPSite(self._runner, "0.0.0.0", self.port)
        await site.start()
        logger.info("Dashboard: http://localhost:%d", self.port)

        # Läuft ewig
        while True:
            await asyncio.sleep(3600)

    # ------------------------------------------------------------------
    # HTTP Handler
    # ------------------------------------------------------------------

    async def _api_ags_states(self, request: web.Request) -> web.Response:
        """Alle Bundesländer – für Frontend-Dropdown."""
        import ags_lookup
        return web.json_response(ags_lookup.get_all_states())

    async def _api_ags_districts(self, request: web.Request) -> web.Response:
        """Alle Kreise eines Bundeslands – für Frontend-Dropdown."""
        import ags_lookup
        state = request.rel_url.query.get("state", "08")
        return web.json_response(ags_lookup.get_districts_for_state(state))

    async def _handle_index(self, request: web.Request) -> web.Response:
        if not DASHBOARD_PATH.exists():
            return web.Response(text="dashboard.html nicht gefunden", status=404)
        content = DASHBOARD_PATH.read_text(encoding="utf-8")
        return web.Response(text=content, content_type="text/html", charset="utf-8")

    async def _api_status(self, request: web.Request) -> web.Response:
        status = self.app.status()
        return web.json_response(status)

    async def _api_warnings(self, request: web.Request) -> web.Response:
        hours = int(request.rel_url.query.get("hours", 48))
        warnings = self.app.db.get_all_recent(hours=hours)
        return web.json_response(warnings)

    async def _api_sent(self, request: web.Request) -> web.Response:
        """Gesendete Mesh-Nachrichten (Simulator-Log)."""
        sent = self.app.mesh.get_sent_log()
        return web.json_response(sent)

    async def _api_config_get(self, request: web.Request) -> web.Response:
        return web.json_response(self.config)

    async def _api_config_post(self, request: web.Request) -> web.Response:
        """Konfiguration ändern, in config.yaml speichern und live neu laden."""
        try:
            data = await request.json()
            allowed = {"meshcore", "dab", "nina", "dedup", "warnings_db"}
            for key in data:
                if key in allowed:
                    self.config[key] = data[key]

            import yaml
            with open("config.yaml", "w", encoding="utf-8") as f:
                yaml.dump(self.config, f, allow_unicode=True, default_flow_style=False)

            # Live-Reload der laufenden Module
            await self._reload_from_config()

            return web.json_response({"ok": True})
        except Exception as e:
            return web.json_response({"ok": False, "error": str(e)}, status=400)

    async def _reload_from_config(self):
        """Module mit neuer Konfig aktualisieren ohne Neustart."""
        nina_cfg = self.config.get("nina", {})
        mc_cfg = self.config.get("meshcore", {})

        # NinaPoller: region_state + broadcast_districts + poll_interval aktualisieren
        self.app.nina.region_state = str(nina_cfg.get("region_state", "08"))
        self.app.nina.broadcast_districts = nina_cfg.get("broadcast_districts", [])
        self.app.nina.poll_interval = int(nina_cfg.get("poll_interval_minutes", 15)) * 60
        self.app.nina._sources = nina_cfg.get("sources", {})

        # MeshSender: simulator + channel_idx + scope aktualisieren
        was_simulator = self.app.mesh.simulator
        self.app.mesh.simulator = mc_cfg.get("simulator", True)
        self.app.mesh.channel_idx = int(mc_cfg.get("channel_idx", 0))
        self.app.mesh.scope = mc_cfg.get("scope", "*")

        # Simulator wurde deaktiviert → echten Connect versuchen
        # Simulator wurde aktiviert → als verbunden markieren (kein echter Connect nötig)
        if was_simulator and not self.app.mesh.simulator:
            self.app.mesh._connected = False
            asyncio.create_task(self.app.mesh.connect())
        elif not was_simulator and self.app.mesh.simulator:
            self.app.mesh._connected = True
            self.app.mesh._mc = None

        # warnbridge: broadcast_districts aktualisieren
        self.app.cfg = self.config

        # Sofort einen Poll auslösen damit neue Region direkt abgefragt wird
        asyncio.create_task(self.app.nina._poll())

        # Broadcasting bei Config-Änderung zurücksetzen
        self.app.broadcasting_enabled = False
        logger.info("Konfig live neu geladen – Broadcasting zurückgesetzt (AUS) – sofortiger Poll gestartet")

    async def _api_sim_clear(self, request: web.Request) -> web.Response:
        """Simulator-Log und Test-Warnungen aus DB löschen (nur wenn simulator: true)."""
        if not self.config.get("meshcore", {}).get("simulator", True):
            return web.json_response({"ok": False, "error": "Nur im Simulator-Modus"}, status=400)

        # Simulator-Log leeren
        self.app.mesh._sent_log.clear()

        # Test-Warnungen aus DB löschen
        conn = self.app.db._get_conn()
        deleted = conn.execute(
            "DELETE FROM warnings WHERE identifier LIKE 'test-%'"
        ).rowcount
        conn.commit()

        # Dedup-Cache für Test-IDs leeren (damit neue Tests nicht blockiert werden)
        conn2 = self.app.dedup._get_conn()
        conn2.execute("DELETE FROM seen_ids WHERE identifier LIKE 'test-%'")
        # Alle Hashes löschen die zu Test-IDs gehören:
        # seen_hashes hat keinen direkten Verweis auf die ID, daher alle Test-Hashes
        # über den in-memory hour_window ebenfalls resetten
        conn2.execute("DELETE FROM seen_hashes")
        conn2.commit()
        # In-memory Stunden-Fenster leeren (sonst blockiert max_per_hour weitere Tests)
        self.app.dedup._hour_window.clear()

        # WebSocket-Broadcast: Dashboard aktualisieren
        await self.ws_broadcast({"event": "sim_cleared"})

        logger.info("Simulator geleert: %d Test-Warnungen gelöscht", deleted)
        return web.json_response({"ok": True, "deleted": deleted})

    async def _api_dedup_clear(self, request: web.Request) -> web.Response:
        """Dedup-Cache komplett leeren (seen_ids + seen_hashes + hour_window)."""
        conn = self.app.dedup._get_conn()
        c1 = conn.execute("DELETE FROM seen_ids").rowcount
        c2 = conn.execute("DELETE FROM seen_hashes").rowcount
        conn.commit()
        self.app.dedup._hour_window.clear()
        self.app.mesh._sent_log.clear() if hasattr(self.app, 'mesh') else None
        # mesh_sent_timestamps ebenfalls leeren
        self.app._mesh_sent_timestamps.clear()
        logger.info("Dedup-Cache geleert: %d IDs, %d Hashes", c1, c2)
        return web.json_response({"ok": True, "deleted_ids": c1, "deleted_hashes": c2})

    async def _api_broadcasting_get(self, request: web.Request) -> web.Response:
        """Broadcasting-Status abfragen."""
        return web.json_response({"enabled": self.app.broadcasting_enabled})

    async def _api_broadcasting_post(self, request: web.Request) -> web.Response:
        """Broadcasting ein- oder ausschalten."""
        try:
            data = await request.json()
            was_enabled = self.app.broadcasting_enabled
            self.app.broadcasting_enabled = bool(data.get("enabled", False))
            state = "AN" if self.app.broadcasting_enabled else "AUS"
            logger.info("Broadcasting: %s", state)
            # Wenn gerade eingeschaltet: ausstehende Warnungen nachsenden
            if self.app.broadcasting_enabled and not was_enabled:
                asyncio.create_task(self.app.on_broadcasting_enabled())
            return web.json_response({"enabled": self.app.broadcasting_enabled})
        except Exception as e:
            return web.json_response({"ok": False, "error": str(e)}, status=400)

    async def _api_rxlog(self, request: web.Request) -> web.Response:
        """RxLog – alle empfangenen DAB-Pakete (Journaline/TPEG/EPG), neueste zuerst."""
        return web.json_response(self.app.dab.get_rxlog())

    async def _api_sim_trigger(self, request: web.Request) -> web.Response:
        """Testwarnung auslösen (nur wenn simulator: true)."""
        if not self.config.get("meshcore", {}).get("simulator", True):
            return web.json_response({"ok": False, "error": "Nicht im Simulator-Modus"}, status=400)

        try:
            data = await request.json()
            warn_type = data.get("type", "nina")
        except Exception:
            warn_type = "nina"

        await self._trigger_test_warning(warn_type)
        return web.json_response({"ok": True, "type": warn_type})

    async def _trigger_test_warning(self, warn_type: str):
        """Synthetische Testwarnung durch das System schicken."""
        from cap_normalizer import NormalizedWarning

        now = datetime.now(timezone.utc)
        w = NormalizedWarning(
            source=warn_type if warn_type in ("dab", "mowas", "dwd") else "mowas",
            identifier=f"test-{warn_type}-{int(now.timestamp())}",
            status="test",
            msg_type="Alert",
            severity="Severe",
            urgency="Immediate",
            headline=f"[TEST] Hochwasser Neckar – Stufe 2",
            description="Dies ist eine Testwarnung ausgelöst über das WarnBridge-Dashboard. Keine reale Gefahr.",
            instruction="Uferbereich meiden (Test).",
            area_desc="Böblingen, Stuttgart",
            ags_codes=["08115", "08111"],
            sent=now,
            effective=now,
            expires=None,
        )

        # forward_tests temporär überschreiben
        original = self.config.get("dab", {}).get("forward_tests", False)
        if self.config.get("dab"):
            self.config["dab"]["forward_tests"] = True

        await self.app.handle_warning(w)

        if self.config.get("dab"):
            self.config["dab"]["forward_tests"] = original

    # ------------------------------------------------------------------
    # welle-cli Steuerung
    # ------------------------------------------------------------------

    # Vollständige Band III Kanalliste (5A–13F)
    DAB_CHANNELS = [
        "5A","5B","5C","5D",
        "6A","6B","6C","6D",
        "7A","7B","7C","7D",
        "8A","8B","8C","8D",
        "9A","9B","9C","9D",
        "10A","10B","10C","10D","10N",
        "11A","11B","11C","11D","11N",
        "12A","12B","12C","12D","12N",
        "13A","13B","13C","13D","13E","13F",
    ]

    def _find_welle_cli(self) -> Optional[str]:
        """welle-cli Binary finden: config → PATH → bekannte Pfade."""
        # 1. Aus config.yaml
        configured = self.config.get("dab", {}).get("welle_cli_path", "").strip()
        if configured and Path(configured).is_file():
            return configured

        # 2. Im PATH
        found = shutil.which("welle-cli")
        if found:
            return found

        # 3. Bekannte Build-Pfade
        known = [
            Path.home() / "DAB Warnings" / "welle.io" / "build" / "welle-cli",
            Path("/usr/local/bin/welle-cli"),
            Path("/usr/bin/welle-cli"),
            Path("/opt/welle-cli/welle-cli"),
        ]
        for p in known:
            if p.is_file():
                return str(p)

        return None

    def _welle_process(self) -> Optional[subprocess.Popen]:
        """Gibt den laufenden welle-cli Prozess zurück (falls vorhanden)."""
        return getattr(self, "_welle_proc", None)

    def _welle_is_running(self) -> bool:
        proc = self._welle_process()
        return proc is not None and proc.poll() is None

    async def _api_welle_status(self, request: web.Request) -> web.Response:
        """welle-cli Status abfragen."""
        binary = self._find_welle_cli()
        proc = self._welle_process()
        channel = self.config.get("dab", {}).get("channel", "9D")
        return web.json_response({
            "running": self._welle_is_running(),
            "pid": proc.pid if self._welle_is_running() else None,
            "channel": channel,
            "binary": binary,
            "binary_found": binary is not None,
            "channels": self.DAB_CHANNELS,
        })

    async def _api_welle_start(self, request: web.Request) -> web.Response:
        """welle-cli starten."""
        if self._welle_is_running():
            return web.json_response({"ok": False, "error": "welle-cli läuft bereits"})

        binary = self._find_welle_cli()
        if not binary:
            return web.json_response({"ok": False, "error": "welle-cli Binary nicht gefunden. Pfad in config.yaml unter dab.welle_cli_path eintragen."}, status=400)

        channel = self.config.get("dab", {}).get("channel", "9D")
        cmd = [binary, "-c", channel, "-C", "1", "-w", "7979"]

        try:
            self._welle_proc = subprocess.Popen(
                cmd,
                stdout=subprocess.DEVNULL,
                stderr=subprocess.DEVNULL,
            )
            logger.info("welle-cli gestartet: %s (PID %d)", " ".join(cmd), self._welle_proc.pid)
            return web.json_response({"ok": True, "pid": self._welle_proc.pid, "channel": channel})
        except Exception as e:
            return web.json_response({"ok": False, "error": str(e)}, status=500)

    async def _api_welle_stop(self, request: web.Request) -> web.Response:
        """welle-cli beenden."""
        proc = self._welle_process()
        if not proc or not self._welle_is_running():
            return web.json_response({"ok": False, "error": "welle-cli läuft nicht"})

        try:
            proc.terminate()
            try:
                proc.wait(timeout=3)
            except subprocess.TimeoutExpired:
                proc.kill()
            self._welle_proc = None
            logger.info("welle-cli beendet")
            return web.json_response({"ok": True})
        except Exception as e:
            return web.json_response({"ok": False, "error": str(e)}, status=500)

    async def _api_welle_channel(self, request: web.Request) -> web.Response:
        """Kanal wechseln: welle-cli beenden und mit neuem Kanal neu starten."""
        try:
            data = await request.json()
            channel = data.get("channel", "").upper().strip()
        except Exception:
            return web.json_response({"ok": False, "error": "Ungültiges JSON"}, status=400)

        if channel not in self.DAB_CHANNELS:
            return web.json_response({"ok": False, "error": f"Unbekannter Kanal: {channel}"}, status=400)

        binary = self._find_welle_cli()
        if not binary:
            return web.json_response({"ok": False, "error": "welle-cli Binary nicht gefunden"}, status=400)

        # Aktuellen Prozess beenden
        proc = self._welle_process()
        if proc and self._welle_is_running():
            proc.terminate()
            try:
                proc.wait(timeout=3)
            except subprocess.TimeoutExpired:
                proc.kill()
            self._welle_proc = None

        # Kanal in config speichern
        if "dab" not in self.config:
            self.config["dab"] = {}
        self.config["dab"]["channel"] = channel
        import yaml
        with open("config.yaml", "w", encoding="utf-8") as f:
            yaml.dump(self.config, f, allow_unicode=True, default_flow_style=False)

        # Neu starten
        await asyncio.sleep(0.5)
        cmd = [binary, "-c", channel, "-C", "1", "-w", "7979"]
        try:
            self._welle_proc = subprocess.Popen(
                cmd,
                stdout=subprocess.DEVNULL,
                stderr=subprocess.DEVNULL,
            )
            logger.info("welle-cli Kanal gewechselt zu %s (PID %d)", channel, self._welle_proc.pid)
            return web.json_response({"ok": True, "channel": channel, "pid": self._welle_proc.pid})
        except Exception as e:
            return web.json_response({"ok": False, "error": str(e)}, status=500)

    # ------------------------------------------------------------------
    # WebSocket
    # ------------------------------------------------------------------

    async def _handle_ws(self, request: web.Request) -> web.WebSocketResponse:
        ws = web.WebSocketResponse()
        await ws.prepare(request)
        self._ws_clients.add(ws)
        logger.debug("WS Client verbunden. Gesamt: %d", len(self._ws_clients))

        # Initiale Status-Nachricht senden
        await ws.send_json({
            "event": "connected",
            "data": {"simulator": self.config.get("meshcore", {}).get("simulator", True)},
        })

        try:
            async for msg in ws:
                if msg.type == aiohttp.WSMsgType.TEXT:
                    await self._handle_ws_message(ws, msg.data)
                elif msg.type == aiohttp.WSMsgType.ERROR:
                    logger.warning("WS Fehler: %s", ws.exception())
        finally:
            self._ws_clients.discard(ws)
            logger.debug("WS Client getrennt. Gesamt: %d", len(self._ws_clients))

        return ws

    async def _handle_ws_message(self, ws: web.WebSocketResponse, data: str):
        """
        Bot-Befehle (/details, /warnings [Ort], /status) vom Simulator verarbeiten.
        """
        try:
            msg = json.loads(data)
            if msg.get("type") == "bot_command":
                cmd = msg.get("command", "").strip()
                if not cmd:
                    return

                replies = []

                async def send_reply(text: str) -> bool:
                    replies.append(text)
                    entry = {
                        "time": datetime.now().strftime("%H:%M:%S"),  # Lokale Zeit
                        "timestamp": datetime.now(timezone.utc).isoformat(),
                        "type": "reply",
                        "text": text,
                        "length": len(text),
                        "simulated": True,
                    }
                    # Auch in mesh_sender Log schreiben
                    self.app.mesh._sent_log.append(entry)
                    if len(self.app.mesh._sent_log) > 100:
                        self.app.mesh._sent_log.pop(0)
                    # Live ans Dashboard
                    await self.ws_broadcast({"event": "simulator_out", "data": entry})
                    return True

                await self.app.bot.handle(cmd, send_reply)

        except Exception as e:
            logger.debug("WS Nachricht Fehler: %s", e)

    async def ws_broadcast(self, message: dict):
        """Nachricht an alle verbundenen WebSocket-Clients senden."""
        if not self._ws_clients:
            return
        dead = set()
        data = json.dumps(message, ensure_ascii=False)
        for ws in self._ws_clients:
            try:
                await ws.send_str(data)
            except Exception:
                dead.add(ws)
        self._ws_clients -= dead

    async def broadcast_warning(self, w, broadcast_sent: bool):
        """Neue Warnung ans Dashboard broadcasten."""
        await self.ws_broadcast({
            "event": "new_warning",
            "data": {
                "source": w.source,
                "headline": w.headline,
                "area_desc": w.area_desc,
                "severity": w.severity,
                "status": w.status,
                "broadcast_sent": broadcast_sent,
                "time": datetime.now(timezone.utc).strftime("%H:%M:%S"),
            }
        })
