"""
nina_poller.py – WarnBridge
Fragt die NINA API alle N Minuten ab.
Kein Inhaltsfilter hier – alle Warnungen des konfigurierten Bundeslands
werden normalisiert und weitergegeben. Die Entscheidung ob ins Mesh
gesendet wird trifft _should_broadcast() in warnbridge.py.
Großflächige Warnungen behalten alle betroffenen AGS-Codes.
"""

import asyncio
import aiohttp
import logging
from typing import Callable, Awaitable, Optional
from datetime import datetime, timezone

from cap_normalizer import NormalizedWarning, normalize_mowas, normalize_dwd
import ags_lookup

logger = logging.getLogger(__name__)

NINA_BASE = "https://warnung.bund.de/api31"

# Stadtstaaten: AGS-Lookup liefert hier nichts, deshalb Fallback
_STADTSTAATEN = {
    "02": ["02000"],           # Hamburg
    "04": ["04011", "04012"],  # Bremen, Bremerhaven
    "11": ["11000"],           # Berlin
}

# BW-Fallback falls AGS-Lookup komplett ausfällt
_BW_FALLBACK = [
    "08111","08115","08116","08117","08118","08119",
    "08121","08125","08126","08127","08128","08135","08136",
    "08211","08212","08215","08216","08221","08222","08225","08226",
    "08231","08235","08236","08237",
    "08311","08315","08316","08317",
    "08325","08326","08327","08335","08336","08337",
    "08415","08416","08417","08421","08425","08426","08435","08436","08437",
]

_DWD_SOURCE_PREFIXES = ("dwd.",)
_MOWAS_SOURCE_PREFIXES = ("mow.", "mowas.")
_LHP_SOURCE_PREFIXES = ("lhp.",)

CallbackType = Callable[[NormalizedWarning], Awaitable[None]]


def _dwd_group_key(w: NormalizedWarning) -> str:
    """
    Gruppierschlüssel für DWD-Warnungen.
    DWD sendet ein Ereignis als viele Meldungen mit verschiedenen UUIDs
    aber gleichem Timestamp in der ID (z.B. 1776600300000).
    Wir extrahieren diesen Timestamp um gleiche Ereignisse zusammenzuführen.
    Format: dwd.2.49.0.0.276.0.DWD.PVW.{TIMESTAMP}.{UUID}.MUL
    """
    if w.source != "dwd":
        return w.identifier
    parts = w.identifier.split(".")
    # Timestamp ist Teil 9 (0-indexed) wenn Format stimmt
    if len(parts) >= 10:
        ts = parts[9]  # z.B. "1776600300000"
        return f"dwd|{ts}|{w.headline.lower()}"
    # Fallback: headline + sent-Minute
    sent_min = w.sent.strftime("%Y-%m-%dT%H:%M") if w.sent else ""
    return f"dwd|{sent_min}|{w.headline.lower()}"




def _merge_dwd_warnings(warnings: list) -> list:
    """
    Führt DWD-Warnungen zusammen die dasselbe Ereignis beschreiben
    (gleicher DWD-Timestamp + gleiche Headline = gleiche Warnung).
    Alle AGS-Codes werden kombiniert, die erste Meldung bleibt als Basis.
    """
    from collections import OrderedDict
    groups: OrderedDict = OrderedDict()

    for w in warnings:
        key = _dwd_group_key(w)
        if key not in groups:
            groups[key] = w
        else:
            # AGS-Codes der Folgemeldung zur Gruppe hinzufügen
            existing = groups[key]
            for ags in w.ags_codes:
                if ags not in existing.ags_codes:
                    existing.ags_codes.append(ags)

    # area_desc für zusammengeführte Warnungen aktualisieren
    for key, w in groups.items():
        if w.source == "dwd" and len(w.ags_codes) > 1:
            names = []
            for ags in w.ags_codes[:5]:  # max 5 Namen
                name = ags_lookup.district_name(ags)
                if name and name not in names:
                    names.append(name)
            if names:
                suffix = f" (+{len(w.ags_codes)-5} weitere)" if len(w.ags_codes) > 5 else ""
                w.area_desc = ", ".join(names) + suffix

    merged = list(groups.values())
    if len(merged) < len(warnings):
        logger.info("DWD-Merge: %d → %d Warnungen zusammengeführt",
                    len(warnings), len(merged))
    return merged


class NinaPoller:
    def __init__(self, config: dict, on_warning: CallbackType):
        self.cfg = config
        self.on_warning = on_warning
        self.region_state = str(config.get("region_state", "08"))
        self.poll_interval = int(config.get("poll_interval_minutes", 15)) * 60
        self.broadcast_districts: list[str] = config.get("broadcast_districts", [])
        self._sources = config.get("sources", {})
        self._last_poll: Optional[datetime] = None
        self._poll_error: Optional[str] = None
        self._running = False

    async def run(self):
        self._running = True
        logger.info("NinaPoller gestartet (Interval: %ds, Bundesland: %s)",
                    self.poll_interval, self.region_state)
        while self._running:
            try:
                await self._poll()
            except Exception as e:
                self._poll_error = str(e)
                logger.error("NinaPoller Fehler: %s", e)
            await asyncio.sleep(self.poll_interval)

    def stop(self):
        self._running = False

    async def _poll(self):
        # Kein Poll wenn kein Bundesland konfiguriert (Onboarding noch nicht abgeschlossen)
        if not self.region_state:
            logger.debug("NinaPoller: kein region_state – Poll übersprungen (Onboarding ausstehend)")
            return
        async with aiohttp.ClientSession() as session:
            warnings = await self._fetch_warnings(session)
        self._last_poll = datetime.now(timezone.utc)
        self._poll_error = None
        logger.info("NINA Poll: %d Meldungen (Bundesland: %s)",
                    len(warnings), self.region_state)
        for w in warnings:
            await self.on_warning(w)

    def _get_districts(self) -> list[str]:
        """Alle Kreise des konfigurierten Bundeslands."""
        # 1. AGS-Lookup (Destatis, cached)
        districts = ags_lookup.get_state_districts(self.region_state)
        if districts:
            return districts
        # 2. Stadtstaaten-Fallback
        if self.region_state in _STADTSTAATEN:
            return _STADTSTAATEN[self.region_state]
        # 3. broadcast_districts als Notfallfall
        if self.broadcast_districts:
            logger.warning("AGS-Lookup für %s nicht verfügbar – nutze broadcast_districts",
                           self.region_state)
            return self.broadcast_districts
        # 4. BW-Hardcode als letzter Ausweg
        logger.warning("AGS-Lookup fehlgeschlagen – BW-Fallback")
        return _BW_FALLBACK

    async def _fetch_warnings(self, session: aiohttp.ClientSession) -> list[NormalizedWarning]:
        """
        Holt ALLE Warnungen aller Kreise des Bundeslands.
        Großflächige Warnungen (für viele Kreise gleichzeitig) werden dank
        seen_ids nur einmal normalisiert, sammeln dabei aber alle AGS-Codes auf.
        Kein Severity- oder Event-Filter – läuft in warnbridge._should_broadcast().
        """
        results: list[NormalizedWarning] = []
        seen_ids: set[str] = set()
        # Schnelle Suche: item_id → Index in results
        id_to_index: dict[str, int] = {}

        all_districts = self._get_districts()
        logger.debug("NINA: frage %d Kreise ab", len(all_districts))

        for district in all_districts:
            ags12 = district.ljust(12, "0")
            url = f"{NINA_BASE}/dashboard/{ags12}.json"
            try:
                async with session.get(url, timeout=aiohttp.ClientTimeout(total=30)) as resp:
                    if resp.status != 200:
                        continue
                    items = await resp.json()
            except aiohttp.ClientError as e:
                logger.debug("NINA nicht erreichbar (AGS %s): %s", district, e)
                continue
            except Exception as e:
                logger.error("NINA fetch Fehler (AGS %s): %s", district, e)
                continue

            for item in items:
                item_id = item.get("id", "")
                if not item_id:
                    continue

                if item_id in seen_ids:
                    # Warnung schon verarbeitet – aktuellen Kreis als weiteres
                    # betroffenes Gebiet hinzufügen (großflächige Warnung)
                    idx = id_to_index.get(item_id)
                    if idx is not None and district not in results[idx].ags_codes:
                        results[idx].ags_codes.append(district)
                    continue

                seen_ids.add(item_id)
                detail = await self._fetch_detail(session, item_id, item)
                if not detail:
                    continue

                normalized = self._process_item(detail, district)
                if normalized:
                    id_to_index[item_id] = len(results)
                    results.append(normalized)

        # DWD-Gruppierung: gleiche Ereignisse zusammenführen
        # (DWD sendet ein Ereignis als N Meldungen mit verschiedenen UUIDs)
        results = _merge_dwd_warnings(results)

        return results

    async def _fetch_detail(self, session: aiohttp.ClientSession,
                             item_id: str, item: dict) -> Optional[dict]:
        """Detail-Endpunkt abrufen für vollständige CAP-Daten."""
        if item_id.startswith("mow."):
            source_path = "mowas"
        elif item_id.startswith("dwd."):
            source_path = "dwd"
        elif item_id.startswith("lhp."):
            source_path = "lhp"
        else:
            t = item.get("payload", {}).get("type", "").lower()
            if "mowas" in t:
                source_path = "mowas"
            elif "dwd" in t:
                source_path = "dwd"
            else:
                return item

        url = f"{NINA_BASE}/{source_path}/{item_id}.json"
        try:
            async with session.get(url, timeout=aiohttp.ClientTimeout(total=15)) as resp:
                if resp.status == 200:
                    detail = await resp.json()
                    return {"id": item_id, "payload": {"data": detail, "type": source_path.upper()}}
                return item
        except Exception:
            return item

    def _process_item(self, item: dict, fallback_district: str) -> Optional[NormalizedWarning]:
        """
        Normalisieren + Regionscheck.
        Kein Severity- oder Event-Filter – das ist Aufgabe von _should_broadcast().
        Einziger Filter: Quelle muss enabled:true sein (z.B. lhp.enabled:false).
        """
        item_id = item.get("id", "")

        # Quelle bestimmen
        if any(item_id.startswith(p) for p in _MOWAS_SOURCE_PREFIXES):
            source = "mowas"
        elif any(item_id.startswith(p) for p in _DWD_SOURCE_PREFIXES):
            source = "dwd"
        elif any(item_id.startswith(p) for p in _LHP_SOURCE_PREFIXES):
            source = "lhp"
        else:
            payload_type = item.get("payload", {}).get("type", "").lower()
            if "mowas" in payload_type:
                source = "mowas"
            elif "dwd" in payload_type:
                source = "dwd"
            elif "lhp" in payload_type:
                source = "lhp"
            else:
                logger.debug("Unbekannte NINA-Quelle für ID: %s", item_id)
                return None

        # Quelle grundsätzlich aktiviert? (enabled:false = komplett ignorieren)
        src_cfg = self._sources.get(source, {})
        if not src_cfg.get("enabled", False):
            return None

        # Normalisieren
        if source == "mowas":
            w = normalize_mowas(item)
        elif source == "dwd":
            w = normalize_dwd(item)
        else:
            return None  # LHP vorerst deaktiviert

        if w is None:
            return None

        # AGS-Fallback: Dashboard-Format liefert keine AGS-Codes
        if not w.ags_codes:
            w.ags_codes = [fallback_district]

        # area_desc-Fallback
        if not w.area_desc or w.area_desc.startswith("Gebiet ("):
            w.area_desc = ags_lookup.district_name(fallback_district) or fallback_district

        # Regionscheck: mindestens ein AGS-Code muss zum Bundesland passen
        if not any(ags.startswith(self.region_state) for ags in w.ags_codes):
            return None

        return w

    def status(self) -> dict:
        return {
            "last_poll": self._last_poll.isoformat() if self._last_poll else None,
            "poll_interval_minutes": self.poll_interval // 60,
            "region_state": self.region_state,
            "error": self._poll_error,
            "running": self._running,
        }
