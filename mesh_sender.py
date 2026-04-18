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

try:
    from meshcore import MeshCore
    MESHCORE_AVAILABLE = True
except ImportError:
    MESHCORE_AVAILABLE = False
    logger.warning("meshcore Library nicht installiert – nur Simulator-Modus verfügbar")

MSG_LIMIT = 120


def _trunc(text: str, max_len: int) -> str:
    """Text auf max_len Zeichen kürzen, mit … wenn nötig."""
    if len(text) <= max_len:
        return text
    return text[:max_len - 1] + "…"


def format_alert_message(w) -> str:
    """
    Stufe-1-Nachricht: max 120 Zeichen, reines ASCII-Prefix [!].
    Format: [!] QUELLE | Headline | Area | /details
    Dynamisches Budget: Headline max 60 Zeichen, Area max 20 Zeichen.
    Restbudget von kurzer Headline geht an Area.
    """
    source = w["source"] if isinstance(w, dict) else w.source
    headline = (w["headline"] if isinstance(w, dict) else w.headline) or ""
    area = (w["area_desc"] if isinstance(w, dict) else w.area_desc) or ""
    status = (w["status"] if isinstance(w, dict) else w.status) or "actual"
    is_test = status == "test"

    source_label = {
        "mowas": "WARNUNG",
        "dwd":   "DWD",
        "lhp":   "PEGEL",
        "dab":   "DAB+",
    }.get(source, "WARN")

    prefix = f"[TEST] {source_label}" if is_test else f"[!] {source_label}"
    suffix = " | /details"

    # Budget für headline + " | " + area
    overhead = len(prefix) + len(" | ") + len(" | ") + len(suffix)
    budget = MSG_LIMIT - overhead  # verfügbare Zeichen für headline + area

    # Area: erste Komponente, max 20 Zeichen
    area_short = area.split(",")[0].strip()
    area_max = min(20, budget - 20 - 3)  # mind. 20 für headline
    area_short = _trunc(area_short, max(10, area_max))

    # Headline bekommt den Rest, max 60
    headline_budget = min(60, budget - len(area_short) - 3)
    headline_short = _trunc(headline, max(10, headline_budget))

    msg = f"{prefix} | {headline_short} | {area_short}{suffix}"

    # Hartes Sicherheitsnetz
    if len(msg) > MSG_LIMIT:
        msg = msg[:MSG_LIMIT - 1] + "…"

    return msg


def _now_str() -> str:
    return datetime.now().strftime("%H:%M:%S")


WsBroadcastType = Callable[[dict], Awaitable[None]]


class MeshSender:
    def __init__(self, config: dict):
        self.host = config.get("host", "192.168.4.1")
        self.port = int(config.get("port", 4403))
        self.simulator = config.get("simulator", True)
        self.channel_idx = int(config.get("channel_idx", 0))
        self.scope = config.get("scope", "*")
        self._mc: Optional[object] = None
        self._connected = False
        self._ws_broadcast: Optional[WsBroadcastType] = None
        self._sent_log: list[dict] = []

    def set_ws_broadcast(self, fn: WsBroadcastType):
        self._ws_broadcast = fn

    async def connect(self):
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
        msg = format_alert_message(w)
        return await self._send(msg, msg_type="alert", warning=w)

    async def send_text(self, text: str, msg_type: str = "reply") -> bool:
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
            if self._ws_broadcast:
                await self._ws_broadcast({"event": "simulator_out", "data": entry})
            return True

        if not self._connected or self._mc is None:
            logger.warning("MeshCore nicht verbunden – versuche Reconnect")
            await self.connect()
            if not self._connected:
                return False

        try:
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
