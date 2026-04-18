"""
bot_handler.py – WarnBridge
Verarbeitet Bot-Befehle aus dem MeshCore-Mesh (oder Dashboard-Simulator).
Befehle: /details, /warnings [Ort], /status, /help

Nachrichtenlogik:
- /help    → 1 Nachricht
- /status  → 1 Nachricht
- /warnings → max 2 Nachrichten (Header + Liste)
- /details → max 3 Nachrichten (Header + Gebiet + Beschreibung), alle mit Fallback-Kürzung
"""

import asyncio
import logging
from typing import Callable, Awaitable, TYPE_CHECKING
import ags_lookup

if TYPE_CHECKING:
    from warnbridge import WarnBridge

logger = logging.getLogger(__name__)

MSG_LIMIT = 120

MSG_DELAY_SIMULATOR = 1.0
MSG_DELAY_MESH = 10.0

SendFn = Callable[[str], Awaitable[bool]]


def _trunc(text: str, max_len: int = MSG_LIMIT) -> str:
    """Text auf max_len Zeichen kürzen, mit … wenn nötig."""
    if len(text) <= max_len:
        return text
    return text[:max_len - 1] + "…"


def _area_short(area_desc: str, max_len: int = 60) -> str:
    """
    Gebietsbezeichnung kürzen.
    Bei mehreren Kreisen: ersten + "u.a." statt alles aufzulisten.
    """
    if not area_desc:
        return "–"
    parts = [p.strip() for p in area_desc.split(",")]
    if len(parts) == 1:
        return _trunc(parts[0], max_len)
    first = _trunc(parts[0], max_len - 5)
    candidate = f"{first} u.a."
    if len(candidate) <= max_len:
        return candidate
    return _trunc(parts[0], max_len)


class BotHandler:
    def __init__(self, app: "WarnBridge"):
        self.app = app

    def _delay(self) -> float:
        if self.app.cfg.get("meshcore", {}).get("simulator", True):
            return MSG_DELAY_SIMULATOR
        return MSG_DELAY_MESH

    async def handle(self, command: str, send: SendFn):
        cmd = command.strip()
        lower = cmd.lower()

        if lower == "/details" or lower.startswith("/details "):
            await self._cmd_details(cmd, send)
        elif lower.startswith("/warnings"):
            parts = cmd.split(None, 1)
            ort = parts[1].strip() if len(parts) > 1 else ""
            await self._cmd_warnings(ort, send)
        elif lower == "/status":
            await self._cmd_status(send)
        elif lower == "/help":
            await self._cmd_help(send)
        else:
            await send(_trunc(f"Unbekannt: {cmd} | /help"))

    async def _send_sequence(self, messages: list[str], send: SendFn):
        delay = self._delay()
        for i, msg in enumerate(messages):
            await send(msg)
            if i < len(messages) - 1:
                await asyncio.sleep(delay)

    # ------------------------------------------------------------------
    # /details – max 3 Nachrichten
    # ------------------------------------------------------------------
    async def _cmd_details(self, cmd: str, send: SendFn):
        parts = cmd.split(None, 1)
        n = 1
        if len(parts) > 1:
            try:
                n = max(1, int(parts[1].strip()))
            except ValueError:
                pass

        warnings = self.app.db.get_latest(n=n)
        if not warnings:
            await send("Keine Warnungen in den letzten 48h.")
            return

        w = warnings[-1]
        total = len(self.app.db.get_all_recent(hours=48))
        msgs = _format_details(w, n, total)
        await self._send_sequence(msgs, send)

    # ------------------------------------------------------------------
    # /warnings – max 2 Nachrichten
    # ------------------------------------------------------------------
    async def _cmd_warnings(self, ort: str, send: SendFn):
        if ort:
            district_ags = ags_lookup.find_district(ort)
            if district_ags:
                warnings = self.app.db.get_active(district_filter=[district_ags])
                label = ort
            else:
                warnings = self.app.db.get_by_place(ort)
                label = ort
            if not warnings:
                await send(_trunc(f"Keine Warnungen fuer '{ort}' (48h)."))
                return
        else:
            districts = self.app.cfg.get("nina", {}).get("broadcast_districts", [])
            warnings = self.app.db.get_active(district_filter=districts)
            if not warnings:
                await send("Keine aktiven Warnungen fuer deine Region.")
                return
            label = "deine Region"

        # Nachricht 1: Header
        header = _trunc(f"{len(warnings)} Warnung(en) fuer {label}:")

        # Nachricht 2: kompakte Liste, alles in eine Nachricht
        lines = []
        for i, w in enumerate(warnings[:4], 1):
            sev = (w.get("severity") or "?")[:3].upper()
            src = (w.get("source") or "?").upper()
            hl = w.get("headline") or "–"
            area = _area_short(w.get("area_desc") or "–", max_len=20)
            line = f"{i}.[{src}/{sev}] {hl[:35]} {area}"
            lines.append(_trunc(line))

        extra = len(warnings) - 4
        list_msg = " | ".join(lines)
        if extra > 0:
            list_msg = _trunc(list_msg, MSG_LIMIT - 20) + f" +{extra} /details"
        list_msg = _trunc(list_msg)

        await self._send_sequence([header, list_msg], send)

    # ------------------------------------------------------------------
    # /status – 1 Nachricht
    # ------------------------------------------------------------------
    async def _cmd_status(self, send: SendFn):
        status = self.app.status()
        nina = status.get("nina", {})
        mesh = status.get("mesh", {})
        dab = status.get("dab", {})
        db = status.get("db", {})

        nina_ok = "OK" if not nina.get("error") and nina.get("running") else "ERR"

        dab_status = dab.get("status", "–")
        snr = dab.get("snr")
        if dab_status == "not_started":
            dab_str = "P3"
        elif dab_status == "ok":
            dab_str = f"OK{f'/{snr:.0f}dB' if snr else ''}"
        elif dab_status == "no_signal":
            dab_str = "NOSIG"
        else:
            dab_str = "ERR"

        mesh_str = "SIM" if mesh.get("simulator") else ("OK" if mesh.get("connected") else "ERR")
        uptime = status.get("uptime_human", "–")
        warn_count = db.get("active_warnings", 0)
        last_poll = nina.get("last_poll")
        poll_str = last_poll[11:16] if last_poll else "–"

        msg = f"DAB+:{dab_str} NINA:{nina_ok} {poll_str} Mesh:{mesh_str} Up:{uptime} Warn:{warn_count}"
        await send(_trunc(msg))

    # ------------------------------------------------------------------
    # /help – 1 Nachricht
    # ------------------------------------------------------------------
    async def _cmd_help(self, send: SendFn):
        await send("/details [n] /warnings [Ort] /status – WarnBridge")


# ------------------------------------------------------------------
# Hilfsfunktion: /details Nachrichten aufbauen (max 3)
# ------------------------------------------------------------------
def _format_details(w: dict, index: int, total: int) -> list[str]:
    """
    Baut max. 3 Nachrichten für /details.
    1. Header: Index + Quelle + Severity + Headline
    2. Gebiet
    3. Beschreibung (max 120 Zeichen, gekürzt)
    Instruction wird weggelassen.
    """
    msgs = []

    sev = w.get("severity", "?")
    src = (w.get("source") or "?").upper()
    headline = w.get("headline") or "–"
    status = w.get("status", "actual")
    test_prefix = "[TEST] " if status == "test" else ""

    # Nachricht 1: Header
    header = _trunc(f"[{index}/{total}] {test_prefix}{src} {sev} | {headline}")
    msgs.append(header)

    # Nachricht 2: Gebiet
    area = _area_short(w.get("area_desc") or "–", max_len=MSG_LIMIT - len("Gebiet: "))
    msgs.append(_trunc(f"Gebiet: {area}"))

    # Nachricht 3: Beschreibung (nur wenn vorhanden)
    desc = (w.get("description") or "").strip()
    if desc:
        msgs.append(_trunc(desc))

    return msgs
