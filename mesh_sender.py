"""
mesh_sender.py – WarnBridge
Sendet Warnmeldungen ins MeshCore-Mesh via TCP.
Im Simulator-Modus (config.yaml: meshcore.simulator: true):
  → Nachrichten werden per WebSocket ans Dashboard gesendet statt ans Mesh.
"""

import asyncio
import logging
from datetime import datetime, timezone
from typing import Optional, Callable, Awaitable

logger = logging.getLogger(__name__)

# MeshCore Python-Library
# pip install meshcore
try:
    from meshcore import MeshCore
    MESHCORE_AVAILABLE = True
except ImportError:
    MESHCORE_AVAILABLE = False
    logger.warning("meshcore Library nicht installiert – nur Simulator-Modus verfügbar")


def _now_str() -> str:
    return datetime.now().strftime("%H:%M:%S")  # Lokale Zeit


def format_alert_message(w) -> str:
    """
    Stufe-1-Nachricht: max 120 Zeichen.
    Format: ⚠ QUELLE | Headline | Gebiet | /details
    """
    source_label = {
        "mowas": "WARNUNG",
        "dwd": "DWD",
        "lhp": "PEGEL",
        "dab": "DAB+",
    }.get(w["source"] if isinstance(w, dict) else w.source, "WARN")

    headline = (w["headline"] if isinstance(w, dict) else w.headline) or ""
    area = (w["area_desc"] if isinstance(w, dict) else w.area_desc) or ""

    # Gebiet kürzen: erste Komponente vor dem Komma
    area_short = area.split(",")[0].strip()
    if len(area_short) > 25:
        area_short = area_short[:24] + "…"

    # Headline kürzen
    headline_max = 60
    if len(headline) > headline_max:
        headline = headline[:headline_max - 1] + "…"

    msg = f"⚠ {source_label} | {headline} | {area_short} | /details"
    if len(msg) > 120:
        # Notfall-Kürzung
        msg = msg[:119] + "…"
    return msg


# Typ für WebSocket-Broadcast-Callback (wird von web_ui.py gesetzt)
WsBroadcastType = Callable[[dict], Awaitable[None]]


class MeshSender:
    def __init__(self, config: dict):
        """
        config: der meshcore-Block aus config.yaml
        """
        self.host = config.get("host", "192.168.4.1")
        self.port = int(config.get("port", 4403))
        self.simulator = config.get("simulator", True)
        self.channel_idx = int(config.get("channel_idx", 0))
        self.scope = config.get("scope", "*")
        self._mc: Optional[object] = None
        self._connected = False
        self._ws_broadcast: Optional[WsBroadcastType] = None
        self._sent_log: list[dict] = []  # Für Dashboard (letzte 100 Nachrichten)

    def set_ws_broadcast(self, fn: WsBroadcastType):
        """WebSocket-Broadcast-Funktion von web_ui.py registrieren."""
        self._ws_broadcast = fn

    async def connect(self):
        """Verbindung zum MeshCore-Node aufbauen."""
        if self.simulator:
            logger.info("MeshSender: Simulator-Modus aktiv")
            self._connected = True
            return

        if not MESHCORE_AVAILABLE:
            logger.error("meshcore Library nicht verfügbar und kein Simulator-Modus")
            return

        try:
            self._mc = MeshCore()
            await self._mc.connect_tcp(self.host, self.port)
            self._connected = True
            logger.info("MeshCore verbunden: %s:%d", self.host, self.port)
        except Exception as e:
            self._connected = False
            logger.error("MeshCore Verbindung fehlgeschlagen: %s", e)

    async def send_warning(self, w) -> bool:
        """
        Sendet eine Stufe-1-Alarmnachricht.
        w kann NormalizedWarning oder dict sein.
        Gibt True zurück wenn erfolgreich (oder simuliert).
        """
        msg = format_alert_message(w)
        return await self._send(msg, msg_type="alert", warning=w)

    async def send_text(self, text: str, msg_type: str = "reply") -> bool:
        """Freitext senden (für Bot-Antworten in Phase 2)."""
        return await self._send(text, msg_type=msg_type)

    async def _send(self, text: str, msg_type: str = "alert", warning=None) -> bool:
        entry = {
            "time": _now_str(),
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "type": msg_type,
            "text": text,
            "length": len(text),
            "simulated": self.simulator,
        }

        if self.simulator:
            logger.info("[SIMULATOR OUT] %s", text)
            self._sent_log.append(entry)
            if len(self._sent_log) > 100:
                self._sent_log.pop(0)

            # WebSocket-Broadcast ans Dashboard
            if self._ws_broadcast:
                await self._ws_broadcast({
                    "event": "simulator_out",
                    "data": entry,
                })
            return True

        # Echter MeshCore-Versand
        if not self._connected or self._mc is None:
            logger.warning("MeshCore nicht verbunden – versuche Reconnect")
            await self.connect()
            if not self._connected:
                return False

        try:
            # meshcore Library: send_msg an konfigurierten Channel
            # channel_idx aus config.yaml (Standard: 0)
            await self._mc.commands.send_msg(None, text, channel_idx=self.channel_idx)
            self._sent_log.append(entry)
            if len(self._sent_log) > 100:
                self._sent_log.pop(0)
            logger.info("[MESH OUT] %s", text)
            return True
        except Exception as e:
            logger.error("MeshCore send Fehler: %s", e)
            self._connected = False
            return False

    def get_sent_log(self) -> list[dict]:
        """Letzte gesendete Nachrichten – für Dashboard."""
        return list(reversed(self._sent_log))

    def status(self) -> dict:
        return {
            "simulator": self.simulator,
            "connected": self._connected,
            "host": self.host if not self.simulator else None,
            "port": self.port if not self.simulator else None,
            "channel_idx": self.channel_idx,
            "scope": self.scope,
            "sent_count": len(self._sent_log),
        }
