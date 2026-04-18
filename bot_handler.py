"""
bot_handler.py – WarnBridge
Verarbeitet Bot-Befehle aus dem MeshCore-Mesh (oder Dashboard-Simulator).
Befehle: /details, /warnings [Ort], /status
"""

import asyncio
import logging
from typing import Callable, Awaitable, TYPE_CHECKING
import ags_lookup

if TYPE_CHECKING:
    from warnbridge import WarnBridge

logger = logging.getLogger(__name__)

# Max Zeichen pro Mesh-Nachricht
MSG_LIMIT = 120

# Verzögerung zwischen Nachrichten
# Simulator: kurz damit es im Dashboard flüssig aussieht
# Mesh: 10s Abstand damit der Kanal nicht überflutet wird
MSG_DELAY_SIMULATOR = 1.0
MSG_DELAY_MESH = 10.0

SendFn = Callable[[str], Awaitable[bool]]


def _chunks(text: str, size: int = MSG_LIMIT) -> list[str]:
    """Text in Blöcke à max. `size` Zeichen aufteilen."""
    words = text.split()
    chunks = []
    current = ""
    for word in words:
        if not current:
            current = word
        elif len(current) + 1 + len(word) <= size:
            current += " " + word
        else:
            chunks.append(current)
            current = word
    if current:
        chunks.append(current)
    return chunks


def _format_warning_detail(w: dict, index: int, total: int) -> list[str]:
    """
    Formatiert eine Warnung als Liste von Mesh-Nachrichten (je max. 120 Zeichen).
    """
    lines = []

    # Kopfzeile
    sev = w.get("severity", "?")
    src = (w.get("source") or "?").upper()
    headline = w.get("headline", "–")
    area = (w.get("area_desc") or "–").split(",")[0].strip()
    status = " [TEST]" if w.get("status") == "test" else ""

    header = f"[{index}/{total}]{status} {src} {sev} | {headline}"
    if len(header) > MSG_LIMIT:
        header = header[:MSG_LIMIT - 1] + "…"
    lines.append(header)

    # Gebiet
    area_line = f"Gebiet: {w.get('area_desc', '–')}"
    if len(area_line) > MSG_LIMIT:
        area_line = area_line[:MSG_LIMIT - 1] + "…"
    lines.append(area_line)

    # Beschreibung aufteilen
    desc = w.get("description", "").strip()
    if desc:
        for chunk in _chunks(desc, MSG_LIMIT):
            lines.append(chunk)

    # Verhaltensempfehlung
    instr = w.get("instruction", "").strip()
    if instr:
        instr_header = "► " + instr
        for chunk in _chunks(instr_header, MSG_LIMIT):
            lines.append(chunk)

    return lines


class BotHandler:
    def __init__(self, app: "WarnBridge"):
        self.app = app

    def _delay(self) -> float:
        """Verzögerung je nach Modus."""
        if self.app.cfg.get("meshcore", {}).get("simulator", True):
            return MSG_DELAY_SIMULATOR
        return MSG_DELAY_MESH

    async def handle(self, command: str, send: SendFn):
        """
        Parst und verarbeitet einen Bot-Befehl.
        send: async Funktion die eine Textnachricht verschickt.
        """
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
            await send(f"Unbekannter Befehl: {cmd} | Hilfe: /help")

    async def _send_sequence(self, messages: list[str], send: SendFn):
        """Nachrichten mit Verzögerung nacheinander senden."""
        delay = self._delay()
        for i, msg in enumerate(messages):
            await send(msg)
            if i < len(messages) - 1:
                await asyncio.sleep(delay)

    async def _cmd_details(self, cmd: str, send: SendFn):
        """
        /details – letzte Warnung aus DB, vollständig aufgeteilt.
        /details 2 – zweitletzte Warnung.
        """
        # Optionale Zahl: /details 2
        parts = cmd.split(None, 1)
        n = 1
        if len(parts) > 1:
            try:
                n = max(1, int(parts[1].strip()))
            except ValueError:
                pass

        warnings = self.app.db.get_latest(n=n)
        if not warnings:
            await send("Keine Warnungen in den letzten 48h gespeichert.")
            return

        w = warnings[-1]
        total = len(self.app.db.get_all_recent(hours=48))
        msgs = _format_warning_detail(w, n, total)
        await self._send_sequence(msgs, send)

    async def _cmd_warnings(self, ort: str, send: SendFn):
        """
        /warnings – alle aktiven Warnungen für broadcast_districts
        /warnings Karlsruhe – Warnungen für den genannten Ort
        """
        if ort:
            # AGS-Lookup: Gemeinde/Kreis → district_ags → DB-Abfrage
            district_ags = ags_lookup.find_district(ort)
            if district_ags:
                warnings = self.app.db.get_active(district_filter=[district_ags])
                label = f"{ort} ({ags_lookup.district_name(district_ags)})"
            else:
                # Fallback: Textsuche in area_desc
                warnings = self.app.db.get_by_place(ort)
                label = ort
            if not warnings:
                await send(f"Keine Warnungen für '{ort}' in den letzten 48h.")
                return
        else:
            # Ohne Ort: broadcast_districts
            districts = self.app.cfg.get("nina", {}).get("broadcast_districts", [])
            warnings = self.app.db.get_active(district_filter=districts)
            if not warnings:
                await send("Keine aktiven Warnungen für deine Region.")
                return
            label = "deine Region"

        # Kompakte Übersicht: eine Zeile pro Warnung
        msgs = [f"{len(warnings)} Warnung(en) für {label}:"]
        for i, w in enumerate(warnings[:5], 1):  # max 5
            sev = w.get("severity", "?")[:3].upper()
            src = (w.get("source") or "?").upper()
            headline = w.get("headline", "–")
            area = (w.get("area_desc") or "–").split(",")[0].strip()
            line = f"{i}. [{src}/{sev}] {headline} | {area}"
            if len(line) > MSG_LIMIT:
                line = line[:MSG_LIMIT - 1] + "…"
            msgs.append(line)

        if len(warnings) > 5:
            msgs.append(f"... und {len(warnings) - 5} weitere. /details für Volltext.")

        await self._send_sequence(msgs, send)

    async def _cmd_status(self, send: SendFn):
        """/status – Systemstatus kompakt."""
        status = self.app.status()

        nina = status.get("nina", {})
        mesh = status.get("mesh", {})
        dab = status.get("dab", {})
        db = status.get("db", {})

        nina_ok = "OK" if not nina.get("error") and nina.get("running") else "ERR"
        dab_status = dab.get("status", "–")
        if dab_status == "not_started":
            dab_str = "Phase 3"
        elif dab_status == "ok":
            dab_str = "OK"
        else:
            dab_str = dab_status or "–"

        mesh_str = "SIM" if mesh.get("simulator") else ("OK" if mesh.get("connected") else "ERR")
        uptime = status.get("uptime_human", "–")
        warnings_count = db.get("active_warnings", 0)
        last_poll = nina.get("last_poll")
        poll_str = last_poll[11:16] if last_poll else "–"  # HH:MM aus ISO

        line1 = f"WarnBridge | DAB+:{dab_str} NINA:{nina_ok} Mesh:{mesh_str}"
        line2 = f"Uptime:{uptime} | Warnungen:{warnings_count} | Poll:{poll_str}"

        if len(line1) > MSG_LIMIT:
            line1 = line1[:MSG_LIMIT - 1] + "…"
        if len(line2) > MSG_LIMIT:
            line2 = line2[:MSG_LIMIT - 1] + "…"

        await self._send_sequence([line1, line2], send)

    async def _cmd_help(self, send: SendFn):
        """/help – verfügbare Befehle."""
        await self._send_sequence([
            "/details – Letzte Warnung vollständig",
            "/warnings [Ort] – Warnungen für Ort oder deine Region",
            "/status – Systemstatus",
        ], send)
