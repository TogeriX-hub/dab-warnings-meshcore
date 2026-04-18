"""
nina_poller.py – WarnBridge
Fragt die NINA API alle N Minuten ab.
Filter: MoWaS (Severe+) + DWD (per-event + level aus config.yaml).
Liefert NormalizedWarning-Objekte an einen asyncio-Callback.
"""

import asyncio
import aiohttp
import logging
from typing import Callable, Awaitable
from datetime import datetime, timezone

from cap_normalizer import NormalizedWarning, normalize_mowas, normalize_dwd
import ags_lookup

logger = logging.getLogger(__name__)

# NINA API Basis-URL
NINA_BASE = "https://warnung.bund.de/api31"

# Alle 44 Kreise Baden-Württembergs (AGS 5-stellig)
# Quelle: Statistisches Landesamt BW
BW_DISTRICTS = [
    "08111",  # Stuttgart
    "08115",  # Böblingen
    "08116",  # Esslingen
    "08117",  # Göppingen
    "08118",  # Ludwigsburg
    "08119",  # Rems-Murr-Kreis
    "08121",  # Heilbronn Stadt
    "08125",  # Heilbronn Kreis
    "08126",  # Hohenlohekreis
    "08127",  # Schwäbisch Hall
    "08128",  # Main-Tauber-Kreis
    "08135",  # Heidenheim
    "08136",  # Ostalbkreis
    "08211",  # Baden-Baden
    "08212",  # Karlsruhe Stadt
    "08215",  # Karlsruhe Kreis
    "08216",  # Rastatt
    "08221",  # Heidelberg
    "08222",  # Mannheim
    "08225",  # Neckar-Odenwald-Kreis
    "08226",  # Rhein-Neckar-Kreis
    "08231",  # Pforzheim
    "08235",  # Calw
    "08236",  # Enzkreis
    "08237",  # Freudenstadt
    "08311",  # Freiburg
    "08315",  # Breisgau-Hochschwarzwald
    "08316",  # Emmendingen
    "08317",  # Ortenaukreis
    "08325",  # Rottweil
    "08326",  # Schwarzwald-Baar-Kreis
    "08327",  # Tuttlingen
    "08335",  # Konstanz
    "08336",  # Lörrach
    "08337",  # Waldshut
    "08415",  # Reutlingen
    "08416",  # Tübingen
    "08417",  # Zollernalbkreis
    "08421",  # Ulm
    "08425",  # Alb-Donau-Kreis
    "08426",  # Biberach
    "08435",  # Bodenseekreis
    "08436",  # Ravensburg
    "08437",  # Sigmaringen
]

# Mapping Bundesland-Code → Kreisliste
STATE_DISTRICTS = {
    "08": BW_DISTRICTS,
    # weitere Bundesländer können hier ergänzt werden
}

# DWD event-Typ → NINA eventCode Präfixe
_DWD_SOURCE_PREFIXES = ("dwd.",)
_MOWAS_SOURCE_PREFIXES = ("mow.", "mowas.")
_LHP_SOURCE_PREFIXES = ("lhp.",)

CallbackType = Callable[[NormalizedWarning], Awaitable[None]]


class NinaPoller:
    def __init__(self, config: dict, on_warning: CallbackType):
        """
        config: der nina-Block aus config.yaml
        on_warning: async Callback der bei neuer Warnung aufgerufen wird
        """
        self.cfg = config
        self.on_warning = on_warning
        self.region_state = str(config.get("region_state", "08"))
        self.poll_interval = int(config.get("poll_interval_minutes", 15)) * 60
        self.broadcast_districts: list[str] = config.get("broadcast_districts", [])
        self._sources = config.get("sources", {})
        self._last_poll: datetime | None = None
        self._poll_error: str | None = None
        self._running = False

    async def run(self):
        """Läuft dauerhaft, pollt alle poll_interval Sekunden."""
        self._running = True
        logger.info("NinaPoller gestartet (Interval: %ds, Region: BW/%s)",
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
        """Einmal abfragen."""
        async with aiohttp.ClientSession() as session:
            warnings = await self._fetch_warnings(session)
        self._last_poll = datetime.now(timezone.utc)
        self._poll_error = None
        logger.info("NINA Poll: %d Meldungen gefunden", len(warnings))
        for w in warnings:
            await self.on_warning(w)

    async def _fetch_warnings(self, session: aiohttp.ClientSession) -> list[NormalizedWarning]:
        """
        Holt Warnungen für alle Kreise des konfigurierten Bundeslands.
        DB-Scope: ganzes Bundesland (region_state: '08' = alle 44 BW-Kreise).
        Broadcast-Filter läuft später in warnbridge.py (_should_broadcast).
        """
        results = []
        seen_ids: set[str] = set()

        # Alle Kreise des Bundeslands aus AGS-Lookup
        all_districts = ags_lookup.get_state_districts(self.region_state)
        if not all_districts:
            # Fallback: hardcodierte BW-Liste
            all_districts = BW_DISTRICTS if self.region_state == "08" else self.broadcast_districts
            logger.warning("AGS-Lookup nicht verfügbar – nutze Fallback-Liste (%d Kreise)", len(all_districts))

        logger.debug("NINA: frage %d Kreise ab", len(all_districts))

        for district in all_districts:
            ags12 = district.ljust(12, "0")
            url = f"{NINA_BASE}/dashboard/{ags12}.json"
            try:
                async with session.get(url, timeout=aiohttp.ClientTimeout(total=30)) as resp:
                    if resp.status != 200:
                        logger.debug("NINA HTTP %d für AGS %s", resp.status, district)
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
                if item_id in seen_ids:
                    continue
                seen_ids.add(item_id)

                detail = await self._fetch_detail(session, item_id, item)
                if detail:
                    normalized = self._process_item(detail)
                    if normalized:
                        # AGS aus dem abgefragten Kreis eintragen falls noch leer
                        # (Dashboard-Format liefert keine AGS-Codes)
                        if not normalized.ags_codes:
                            normalized.ags_codes = [district]
                        if not normalized.area_desc or normalized.area_desc.startswith("Gebiet ("):
                            import ags_lookup as _ags
                            normalized.area_desc = _ags.district_name(district)
                        results.append(normalized)

        return results

    async def _fetch_detail(self, session: aiohttp.ClientSession,
                             item_id: str, item: dict) -> dict | None:
        """
        Ruft den Detail-Endpunkt ab um vollständige CAP-Daten zu bekommen.
        /api31/{source}/{id}.json
        """
        # Quelle aus ID ableiten
        if item_id.startswith("mow."):
            source_path = "mowas"
        elif item_id.startswith("dwd."):
            source_path = "dwd"
        elif item_id.startswith("lhp."):
            source_path = "lhp"
        else:
            # Quelle aus payload.type
            t = item.get("payload", {}).get("type", "").lower()
            if "mowas" in t:
                source_path = "mowas"
            elif "dwd" in t:
                source_path = "dwd"
            else:
                # Kein Detail-Endpunkt bekannt – direkt verwenden
                return item

        url = f"{NINA_BASE}/{source_path}/{item_id}.json"
        try:
            async with session.get(url, timeout=aiohttp.ClientTimeout(total=15)) as resp:
                if resp.status == 200:
                    detail = await resp.json()
                    # Detail-Antwort hat andere Struktur – in item-Format bringen
                    return {"id": item_id, "payload": {"data": detail, "type": source_path.upper()}}
                else:
                    # Fallback: Dashboard-Item direkt verwenden
                    return item
        except Exception:
            return item

    def _process_item(self, item: dict) -> NormalizedWarning | None:
        """Ein NINA-Item prüfen, normalisieren und filtern."""
        item_id = item.get("id", "")

        # Quelle bestimmen
        if any(item_id.startswith(p) for p in _MOWAS_SOURCE_PREFIXES):
            source = "mowas"
        elif any(item_id.startswith(p) for p in _DWD_SOURCE_PREFIXES):
            source = "dwd"
        elif any(item_id.startswith(p) for p in _LHP_SOURCE_PREFIXES):
            source = "lhp"
        else:
            # Quelle aus payload.type ableiten
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

        # Quelle aktiviert?
        src_cfg = self._sources.get(source, {})
        if not src_cfg.get("enabled", False):
            return None

        # Normalisieren
        if source == "mowas":
            w = normalize_mowas(item)
        elif source == "dwd":
            w = normalize_dwd(item)
        else:
            # LHP: vorerst übersprungen (lhp.enabled: false default)
            return None

        if w is None:
            return None

        # Regionsfilter: AGS-Präfix 08 (BW)
        if not self._is_bw(w):
            return None

        # Quell-spezifischer Filter
        if source == "mowas":
            if not self._passes_mowas_filter(w, src_cfg):
                return None
        elif source == "dwd":
            if not self._passes_dwd_filter(w, src_cfg):
                return None

        return w

    def _is_bw(self, w: NormalizedWarning) -> bool:
        """Prüft ob die Warnung zum konfigurierten Bundesland gehört."""
        if w.ags_codes:
            return any(ags.startswith(self.region_state) for ags in w.ags_codes)
        # Kein AGS → immer durchlassen (AGS wird danach vom Poller eingetragen)
        return True

    def _passes_mowas_filter(self, w: NormalizedWarning, src_cfg: dict) -> bool:
        """MoWaS: min_severity prüfen."""
        severity_order = ["Minor", "Moderate", "Severe", "Extreme"]
        min_sev = src_cfg.get("min_severity", "Severe")
        try:
            return severity_order.index(w.severity) >= severity_order.index(min_sev)
        except ValueError:
            return False

    def _passes_dwd_filter(self, w: NormalizedWarning, src_cfg: dict) -> bool:
        """DWD: event-Typ + min_level aus config.yaml prüfen."""
        events_cfg = src_cfg.get("events", {})
        event_type = w.dwd_event_type

        if not event_type:
            logger.debug("DWD Warnung ohne erkannten Event-Typ: %s", w.headline)
            return False

        event_cfg = events_cfg.get(event_type, {})
        if not event_cfg.get("enabled", False):
            return False

        min_level = event_cfg.get("min_level", 2)
        dwd_level = w.dwd_level or 1
        return dwd_level >= min_level

    def status(self) -> dict:
        return {
            "last_poll": self._last_poll.isoformat() if self._last_poll else None,
            "poll_interval_minutes": self.poll_interval // 60,
            "error": self._poll_error,
            "running": self._running,
        }
