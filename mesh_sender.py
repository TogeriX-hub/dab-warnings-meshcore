"""
mesh_sender.py – WarnBridge
Sendet Warnmeldungen ins MeshCore-Mesh via TCP.
Im Simulator-Modus (config.yaml: meshcore.simulator: true):
  → Nachrichten werden per WebSocket ans Dashboard gesendet statt ans Mesh.
Im Produktivmodus:
  → Nachrichten gehen ans Mesh UND werden per WebSocket ans Dashboard gespiegelt.
"""

import asyncio
import logging
import time
from datetime import datetime, timezone
from typing import Optional, Callable, Awaitable

logger = logging.getLogger(__name__)

try:
    from meshcore import MeshCore, EventType
    MESHCORE_AVAILABLE = True
except ImportError:
    MESHCORE_AVAILABLE = False
    logger.warning("meshcore Library nicht installiert – nur Simulator-Modus verfügbar")

MSG_LIMIT = 120

# Health check: ask the node for its clock regularly. A dead TCP link
# (Heltec rebooted / WLAN blip without FIN) is otherwise never noticed,
# because send_chan_msg() returns an ERROR event instead of raising.
HEALTH_CHECK_INTERVAL = 120     # seconds between checks
HEALTH_CHECK_TIMEOUT = 10       # seconds to wait for the node's answer
LISTEN_TIMEOUT = 60             # wake up the receive loop regularly
# The ESP32 has no battery-backed clock: after a reboot it starts at a wrong
# time and every channel message carries that timestamp (apps sort them into
# the past). WarnBridge sets the node clock from the Pi on connect and
# corrects it during health checks when it drifts more than this.
CLOCK_MAX_DRIFT = 30            # seconds

# TX hang detection: the node can keep answering over TCP while its LoRa
# radio no longer transmits (send_chan_msg still returns OK). After every
# send we watch the node's own "sent" packet counter; if it does not move
# within TX_VERIFY_TIMEOUT the radio is considered stuck and the node gets
# a reboot command (at most once per REBOOT_COOLDOWN).
TX_VERIFY_TIMEOUT = 45          # seconds (flood TX can be delayed a bit)
TX_VERIFY_POLL = 5              # seconds between counter reads
REBOOT_COOLDOWN = 30 * 60       # seconds


def _trunc(text: str, max_len: int) -> str:
    """Text auf max_len Zeichen kürzen, mit … wenn nötig."""
    if len(text) <= max_len:
        return text
    return text[:max_len - 1] + "…"


def format_alert_message(w) -> str:
    """
    Stufe-1-Nachricht: max 120 Zeichen, reines ASCII-Prefix [!].
    Format: [!] QUELLE | Headline | Area | /details
    Space-Weather-Warnungen nutzen [SW] statt [!].
    """
    source = w["source"] if isinstance(w, dict) else w.source
    headline = (w["headline"] if isinstance(w, dict) else w.headline) or ""
    area = (w["area_desc"] if isinstance(w, dict) else w.area_desc) or ""
    status = (w["status"] if isinstance(w, dict) else w.status) or "actual"
    is_test = status == "test"

    source_label = {
        "mowas":         "WARNUNG",
        "dwd":           "DWD",
        "lhp":           "PEGEL",
        "dab":           "DAB+",
        "space_weather": "WELTRAUM",
    }.get(source, "WARN")

    if source == "space_weather":
        prefix = "[SW]"
    elif is_test:
        prefix = f"[TEST] {source_label}"
    else:
        prefix = f"[!] {source_label}"

    suffix = " | /details"

    # Budget für headline + " | " + area
    overhead = len(prefix) + len(" | ") + len(" | ") + len(suffix)
    budget = MSG_LIMIT - overhead

    # Area: erste Komponente, max 20 Zeichen (bei space_weather weglassen – klar global)
    if source == "space_weather":
        area_short = ""
        headline_budget = min(80, budget)
        headline_short = _trunc(headline, max(10, headline_budget))
        msg = f"{prefix} | {headline_short}{suffix}"
    else:
        area_short = area.split(",")[0].strip()
        area_max = min(20, budget - 20 - 3)
        area_short = _trunc(area_short, max(10, area_max))
        headline_budget = min(60, budget - len(area_short) - 3)
        headline_short = _trunc(headline, max(10, headline_budget))
        msg = f"{prefix} | {headline_short} | {area_short}{suffix}"

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
        self._connected_since: Optional[str] = None
        self._last_ok: Optional[str] = None
        self._last_error: Optional[str] = None
        self._disconnect_count = 0
        self._ws_broadcast: Optional[WsBroadcastType] = None
        self._sent_log: list[dict] = []
        self._listen_task: Optional[asyncio.Task] = None
        self._health_task: Optional[asyncio.Task] = None
        self._connect_lock = asyncio.Lock()
        self._tx_hang_count = 0
        self._last_reboot: Optional[float] = None
        self._last_reboot_reason: Optional[str] = None
        self._verify_tasks: set[asyncio.Task] = set()
        # messages lost to a TX hang, sent once more after the node is back
        self._resend_queue: list[tuple[str, str]] = []

    def set_ws_broadcast(self, fn: WsBroadcastType):
        self._ws_broadcast = fn

    # ── connection handling ──────────────────────────────────────────

    def _mark_ok(self):
        self._last_ok = datetime.now(timezone.utc).isoformat()

    def _mark_disconnected(self, reason: str):
        """Flag the link as dead so _mesh_reconnect_loop picks it up."""
        if self._connected:
            self._disconnect_count += 1
            logger.warning("MeshCore Verbindung verloren (%s) – Reconnect folgt", reason)
        self._connected = False
        self._last_error = f"{_now_str()} {reason}"

    async def _close_old(self):
        """Close a previous MeshCore instance so no half-dead socket lingers."""
        old = self._mc
        self._mc = None
        if old is None:
            return
        try:
            await old.disconnect()
        except Exception:
            pass
        # disconnect() skips closing the socket once the library already
        # considers itself disconnected – close the transport explicitly.
        try:
            cm = getattr(old, "connection_manager", None)
            cx = getattr(cm, "connection", None)
            if cx is not None:
                await cx.disconnect()
        except Exception:
            pass

    async def _on_lib_disconnect(self, event):
        reason = ""
        try:
            reason = (event.payload or {}).get("reason", "")
        except Exception:
            pass
        self._mark_disconnected(f"library: {reason or 'disconnected'}")

    async def connect(self):
        if self.simulator:
            logger.info("MeshSender: Simulator-Modus aktiv")
            self._connected = True
            return

        if not MESHCORE_AVAILABLE:
            logger.error("meshcore Library nicht verfügbar und kein Simulator-Modus")
            return

        async with self._connect_lock:
            await self._close_old()
            try:
                mc = await MeshCore.create_tcp(self.host, self.port)
            except Exception as e:
                self._mark_disconnected(f"connect: {e}")
                logger.error("MeshCore Verbindung fehlgeschlagen: %s", e)
                return

            if mc is None:
                self._mark_disconnected("connect: keine Antwort vom Node")
                logger.error("MeshCore Verbindung fehlgeschlagen: keine Antwort von %s:%d",
                             self.host, self.port)
                return

            self._mc = mc
            try:
                mc.subscribe(EventType.DISCONNECTED, self._on_lib_disconnect)
            except Exception as e:
                logger.debug("DISCONNECTED-Subscription fehlgeschlagen: %s", e)

            self._connected = True
            self._connected_since = datetime.now(timezone.utc).isoformat()
            self._mark_ok()
            logger.info("MeshCore verbunden: %s:%d", self.host, self.port)
            await self._sync_clock(force=True)
            await self._set_scope()
            if self._resend_queue:
                task = asyncio.create_task(self._flush_resend_queue(), name="mesh_resend")
                self._verify_tasks.add(task)
                task.add_done_callback(self._verify_tasks.discard)

    async def _sync_clock(self, force: bool = False, node_time: Optional[int] = None):
        """Set the node clock to the Pi's time (force, or when drift is too big)."""
        if self._mc is None:
            return
        now = int(time.time())
        if not force and node_time is not None and abs(node_time - now) <= CLOCK_MAX_DRIFT:
            return
        try:
            res = await self._mc.commands.set_time(now)
            if res is not None and res.type != EventType.ERROR:
                if node_time is not None:
                    logger.info("MeshCore Uhr korrigiert (Abweichung %ds)", node_time - now)
                else:
                    logger.info("MeshCore Uhr gesetzt")
            else:
                logger.warning("MeshCore Uhr setzen fehlgeschlagen: %s",
                               getattr(res, "payload", None))
        except Exception as e:
            logger.warning("MeshCore Uhr setzen fehlgeschlagen: %s", e)

    async def _set_scope(self):
        if self._mc is None:
            return
        try:
            await self._mc.commands.set_flood_scope(self.scope)
            logger.info("MeshCore Flood-Scope gesetzt: %s", self.scope)
        except Exception as e:
            logger.warning("MeshCore set_flood_scope fehlgeschlagen: %s", e)

    async def health_check(self) -> bool:
        """Ask the node for its time. True = node answers over TCP."""
        if self.simulator:
            return True
        if not self._connected or self._mc is None:
            return False
        try:
            if hasattr(self._mc, "is_connected") and not self._mc.is_connected:
                self._mark_disconnected("library meldet getrennt")
                return False
            res = await asyncio.wait_for(self._mc.commands.get_time(),
                                         timeout=HEALTH_CHECK_TIMEOUT + 5)
        except Exception as e:
            self._mark_disconnected(f"health check: {e}")
            return False

        if res is None or res.type == EventType.ERROR:
            payload = getattr(res, "payload", None)
            self._mark_disconnected(f"health check: keine Antwort ({payload})")
            return False

        self._mark_ok()
        try:
            node_time = int((res.payload or {}).get("time", 0))
            await self._sync_clock(node_time=node_time)
        except Exception as e:
            logger.debug("Uhr-Check fehlgeschlagen: %s", e)
        return True

    # ── TX hang detection ────────────────────────────────────────────

    async def _get_sent_count(self) -> Optional[int]:
        """Node's own counter of transmitted packets, None if unavailable."""
        if self._mc is None:
            return None
        try:
            res = await asyncio.wait_for(self._mc.commands.get_stats_packets(),
                                         timeout=HEALTH_CHECK_TIMEOUT)
        except Exception:
            return None
        if res is None or res.type == EventType.ERROR:
            return None
        try:
            return int((res.payload or {}).get("sent"))
        except (TypeError, ValueError):
            return None

    def _start_tx_verify(self, baseline: int, text: str):
        task = asyncio.create_task(self._verify_tx(baseline, text), name="mesh_tx_verify")
        self._verify_tasks.add(task)
        task.add_done_callback(self._verify_tasks.discard)

    async def _verify_tx(self, baseline: int, text: str):
        waited = 0
        while waited < TX_VERIFY_TIMEOUT:
            await asyncio.sleep(TX_VERIFY_POLL)
            waited += TX_VERIFY_POLL
            if not self._connected:
                return  # connection handling takes over
            count = await self._get_sent_count()
            if count is None:
                return  # stats not readable – can't judge
            if count > baseline or count < baseline:
                # increased = sent; decreased = node rebooted meanwhile
                return
        self._tx_hang_count += 1
        logger.error("[MESH TX HANG] Node hat bestätigt, aber nichts gefunkt "
                     "(sent-Zähler bleibt bei %d): %s", baseline, text)
        if len(self._resend_queue) < 10:
            self._resend_queue.append((text, "resend"))
        await self.reboot_node("Funkmodul sendet nicht")

    async def _flush_resend_queue(self):
        """Re-send messages lost to a TX hang (once, without new verify loop)."""
        await asyncio.sleep(10)  # let the node settle after boot
        queue, self._resend_queue = self._resend_queue, []
        for text, _ in queue:
            if not self._connected or self._mc is None:
                self._resend_queue.append((text, "resend"))
                continue
            try:
                res = await self._mc.commands.send_chan_msg(self.channel_idx, text)
                if res is not None and res.type != EventType.ERROR:
                    logger.info("[MESH OUT] (erneut nach Neustart) %s", text)
                else:
                    logger.error("[MESH FAIL] Erneutes Senden fehlgeschlagen: %s", text)
            except Exception as e:
                logger.error("[MESH FAIL] Erneutes Senden fehlgeschlagen (%s): %s", e, text)
            await asyncio.sleep(10)  # same pacing as warnbridge rate limit

    async def reboot_node(self, reason: str) -> bool:
        """Send a reboot command to the node (rate-limited)."""
        now = time.time()
        if self._last_reboot and now - self._last_reboot < REBOOT_COOLDOWN:
            logger.warning("MeshCore Neustart übersprungen (%s) – letzter Neustart vor %d min",
                           reason, int((now - self._last_reboot) / 60))
            return False
        if self._mc is None:
            return False
        self._last_reboot = now
        self._last_reboot_reason = f"{_now_str()} {reason}"
        logger.warning("MeshCore Node wird neu gestartet: %s", reason)
        try:
            await asyncio.wait_for(self._mc.commands.reboot(), timeout=HEALTH_CHECK_TIMEOUT)
        except Exception as e:
            logger.debug("Reboot-Befehl: %s", e)
        self._mark_disconnected(f"Neustart: {reason}")
        # reconnect is done by warnbridge._mesh_reconnect_loop once the node is back
        return True

    async def _health_loop(self):
        while True:
            await asyncio.sleep(HEALTH_CHECK_INTERVAL)
            try:
                if self._connected and not self.simulator:
                    await self.health_check()
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error("MeshCore health loop Fehler: %s", e)

    async def update_config(self, host: str, port: int, channel_idx: int,
                            scope: str, simulator: bool):
        scope_changed = scope != self.scope
        self.host = host
        self.port = port
        self.channel_idx = channel_idx
        self.scope = scope
        self.simulator = simulator

        if not simulator and self._connected and scope_changed:
            await self._set_scope()

    # ── receive ──────────────────────────────────────────────────────

    def start_listen_loop(self, on_command: Callable[[str], Awaitable[None]]):
        """Startet Listen-Loop und Health-Check als asyncio Tasks."""
        if not (self._listen_task and not self._listen_task.done()):
            self._listen_task = asyncio.create_task(
                self.listen_loop(on_command), name="mesh_listen"
            )
        if not (self._health_task and not self._health_task.done()):
            self._health_task = asyncio.create_task(
                self._health_loop(), name="mesh_health"
            )

    async def listen_loop(self, on_command: Callable[[str], Awaitable[None]]):
        """
        Empfängt eingehende MeshCore-Nachrichten und leitet Bot-Befehle weiter.
        wait_for_event() bekommt ein Timeout, damit der Loop nach einem Reconnect
        auf die neue MeshCore-Instanz wechselt statt ewig auf der alten zu warten.
        Im Simulator-Modus: schläft ewig – Befehle kommen vom Dashboard per WebSocket.
        """
        if self.simulator:
            await asyncio.Event().wait()
            return

        logger.info("MeshCore listen_loop gestartet (channel_idx=%d)", self.channel_idx)

        while True:
            try:
                mc = self._mc
                if not self._connected or mc is None:
                    await asyncio.sleep(5)
                    continue

                event = await mc.wait_for_event(EventType.CHANNEL_MSG_RECV,
                                                timeout=LISTEN_TIMEOUT)
                if event is None:
                    continue

                self._mark_ok()
                text = event.payload.get("text", "").strip()
                channel = event.payload.get("channel_idx", -1)

                if channel != self.channel_idx:
                    continue

                # Ein Befehl ist nur gültig wenn der Marker am Anfang der Nachricht
                # steht (direkt oder nach kurzem Absender-Präfix "Name: /cmd").
                # Damit werden Warnmeldungen ignoriert, die zufällig mit "/warnings"
                # enden (z.B. weil ein anderer Bot das als Suffix anhängt).
                cmd = None
                stripped = text.strip()
                lower_stripped = stripped.lower()
                for marker in ("/swdetails", "/details", "/warnings", "/status", "/help"):
                    if lower_stripped.startswith(marker):
                        # Direkt als Befehl: "/warnings" oder "/warnings Rosenheim"
                        cmd = stripped
                        break
                    # "NodeName: /befehl" – Absenderpräfix max. 50 Zeichen
                    colon_pos = lower_stripped.find(": " + marker)
                    if colon_pos != -1 and colon_pos <= 50:
                        cmd = stripped[colon_pos + 2:].strip()
                        break

                if cmd:
                    logger.info("MeshCore Befehl empfangen: %s", cmd)
                    await on_command(cmd)

            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error("MeshCore receive Fehler: %s", e)
                self._mark_disconnected(f"receive: {e}")
                await asyncio.sleep(10)
                # reconnect is done by warnbridge._mesh_reconnect_loop

    # ── send ─────────────────────────────────────────────────────────

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
            if not self._connected or self._mc is None:
                return False

        baseline = await self._get_sent_count()

        try:
            res = await self._mc.commands.send_chan_msg(self.channel_idx, text)
        except Exception as e:
            logger.error("MeshCore send Fehler: %s", e)
            self._mark_disconnected(f"send: {e}")
            return False

        # send_chan_msg() does not raise on failure – it returns an ERROR
        # event (e.g. after the 15s timeout when the node is gone).
        if res is None or res.type == EventType.ERROR:
            payload = getattr(res, "payload", None)
            logger.error("[MESH FAIL] Node hat nicht bestätigt (%s): %s", payload, text)
            self._mark_disconnected(f"send: keine Bestätigung ({payload})")
            return False

        self._mark_ok()
        self._sent_log.append(entry)
        if len(self._sent_log) > 100:
            self._sent_log.pop(0)
        logger.info("[MESH OUT] %s", text)
        if baseline is not None:
            self._start_tx_verify(baseline, text)
        if self._ws_broadcast:
            await self._ws_broadcast({"event": "simulator_out", "data": entry})
        return True

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
            "connected_since": self._connected_since,
            "last_ok": self._last_ok,
            "last_error": self._last_error,
            "disconnect_count": self._disconnect_count,
            "tx_hang_count": self._tx_hang_count,
            "last_reboot": self._last_reboot_reason,
        }
