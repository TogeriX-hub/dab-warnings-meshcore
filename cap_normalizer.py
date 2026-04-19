"""
cap_normalizer.py – WarnBridge
Bringt NINA-API-Meldungen (MoWaS + DWD) auf eine einheitliche interne Struktur.
In Phase 3 wird dab_listener.py dieselbe Struktur erzeugen.
"""

from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Optional
import hashlib
import logging

logger = logging.getLogger(__name__)


@dataclass
class NormalizedWarning:
    source: str                      # "mowas", "dwd", "lhp", "dab"
    identifier: str                  # CAP identifier (für Dedup)
    status: str                      # "actual" oder "test"
    msg_type: str                    # "Alert", "Update", "Cancel"
    severity: str                    # "Extreme", "Severe", "Moderate", "Minor"
    urgency: str                     # "Immediate", "Expected", "Future", "Past"
    headline: str
    description: str
    instruction: str

    area_desc: str                   # Lesbare Gebietsbezeichnung
    ags_codes: list[str]             # AGS-Schlüssel aller betroffenen Kreise

    sent: datetime
    effective: Optional[datetime]
    expires: Optional[datetime]

    dwd_event_type: Optional[str] = None   # "gewitter", "dauerregen", etc.
    dwd_level: Optional[int] = None        # 1–4

    content_hash: str = ""
    received_at: datetime = field(default_factory=lambda: datetime.now(timezone.utc))

    def __post_init__(self):
        if not self.content_hash:
            sent_str = self.sent.isoformat() if self.sent else ""
            self.content_hash = make_hash_with_date(self.headline, sent_str)


def make_hash_with_date(headline: str, sent_date: str) -> str:
    """
    Hash aus headline + vollständigem Timestamp (inkl. Uhrzeit).
    Verschiedene DWD-Meldungen mit gleicher Headline aber verschiedenen
    Sendezeiten (z.B. 37 Gewitterwarnungen für Bayern) werden korrekt
    als separate Warnungen behandelt.
    Echte Duplikate (gleiche CAP-ID) werden vom ID-Cache abgefangen.
    """
    raw = f"{headline.strip().lower()}|{sent_date[:19]}"  # YYYY-MM-DDTHH:MM:SS
    return hashlib.md5(raw.encode()).hexdigest()


def _parse_dt(s: Optional[str]) -> Optional[datetime]:
    if not s:
        return None
    try:
        s = s.replace("Z", "+00:00")
        return datetime.fromisoformat(s).astimezone(timezone.utc)
    except Exception:
        return None


_SEVERITY_MAP = {
    "Extreme": "Extreme", "Severe": "Severe",
    "Moderate": "Moderate", "Minor": "Minor", "Unknown": "Minor",
}
_URGENCY_MAP = {
    "Immediate": "Immediate", "Expected": "Expected",
    "Future": "Future", "Past": "Past", "Unknown": "Future",
}
_DWD_SEVERITY_LEVEL = {"Minor": 1, "Moderate": 2, "Severe": 3, "Extreme": 4}
_LEVEL_TO_SEVERITY = {1: "Minor", 2: "Moderate", 3: "Severe", 4: "Extreme"}


# ---------------------------------------------------------------------------
# MoWaS
# ---------------------------------------------------------------------------

def _normalize_mowas_dashboard(item: dict) -> Optional[NormalizedWarning]:
    try:
        payload = item.get("payload", {})
        data = payload.get("data", {})
        identifier = item.get("id", "unknown")
        valid = data.get("valid", True)
        status = "actual" if valid else "cancelled"
        msg_type = data.get("msgType", "Alert")
        severity = _SEVERITY_MAP.get(data.get("severity", ""), "Minor")
        urgency = _URGENCY_MAP.get(data.get("urgency", ""), "Expected")
        i18n = item.get("i18nTitle", {})
        headline = i18n.get("de") or data.get("headline", "").strip()
        area_data = data.get("area", {})
        area_type = area_data.get("type", "")
        area_desc = f"Gebiet ({area_type})" if area_type else "Unbekanntes Gebiet"
        sent = _parse_dt(item.get("sent")) or datetime.now(timezone.utc)
        return NormalizedWarning(
            source="mowas", identifier=identifier, status=status,
            msg_type=msg_type, severity=severity, urgency=urgency,
            headline=headline, description="", instruction="",
            area_desc=area_desc, ags_codes=[],
            sent=sent, effective=_parse_dt(item.get("effective")),
            expires=_parse_dt(item.get("expires")),
        )
    except Exception as e:
        logger.exception("Fehler MoWaS-Dashboard: %s", e)
        return None


def normalize_mowas(item: dict) -> Optional[NormalizedWarning]:
    try:
        payload = item.get("payload", {})
        data = payload.get("data", {})
        info_list = data.get("info", [])

        if not info_list and data.get("headline"):
            return _normalize_mowas_dashboard(item)

        info = next((i for i in info_list if i.get("language", "").startswith("de")), None)
        if info is None and info_list:
            info = info_list[0]
        if info is None:
            logger.warning("MoWaS item ohne info-Block: %s", item.get("id"))
            return None

        identifier = data.get("identifier") or item.get("id", "unknown")
        status = data.get("status", "actual").lower()
        msg_type = data.get("msgType", "Alert")
        severity = _SEVERITY_MAP.get(info.get("severity", ""), "Minor")
        urgency = _URGENCY_MAP.get(info.get("urgency", ""), "Future")
        headline = info.get("headline", "").strip()
        description = info.get("description", "").strip()
        instruction = info.get("instruction", "").strip()

        area_list = info.get("area", [])
        area_desc = ", ".join(a.get("areaDesc", "") for a in area_list if a.get("areaDesc"))
        ags_codes = []
        for area in area_list:
            for gc in area.get("geocode", []):
                if gc.get("valueName") in ("AGS", "ags"):
                    ags_codes.append(gc.get("value", ""))

        sent = _parse_dt(data.get("sent")) or datetime.now(timezone.utc)
        return NormalizedWarning(
            source="mowas", identifier=identifier, status=status,
            msg_type=msg_type, severity=severity, urgency=urgency,
            headline=headline, description=description, instruction=instruction,
            area_desc=area_desc, ags_codes=ags_codes,
            sent=sent, effective=_parse_dt(info.get("effective")),
            expires=_parse_dt(info.get("expires")),
        )
    except Exception as e:
        logger.exception("Fehler MoWaS: %s", e)
        return None


# ---------------------------------------------------------------------------
# DWD Event-Map
# ---------------------------------------------------------------------------

# Mapping DWD CAP ii-Code (numerisch als String) → interne Kategorie
# Quelle: DWD CAP Profil v2.1.14, Anhang 3.1
# Der ii-Code kommt als eventCode valueName="II" in den CAP-Daten
_DWD_EVENT_MAP_II = {
    # Gewitter (Warnungen)
    "22": "glaette_frost",       # FROST (Stufe 1)
    "24": "glaette_frost",       # GLÄTTE (Stufe 1)
    "31": "gewitter",            # GEWITTER
    "33": "gewitter",            # STARKES GEWITTER
    "34": "gewitter",            # STARKES GEWITTER
    "36": "gewitter",            # STARKES GEWITTER
    "38": "gewitter",            # STARKES GEWITTER
    "40": "gewitter",            # SCHWERES GEWITTER mit ORKANBÖEN
    "41": "gewitter",            # SCHWERES GEWITTER mit EXTREMEN ORKANBÖEN
    "42": "gewitter",            # SCHWERES GEWITTER mit HEFTIGEM STARKREGEN
    "44": "gewitter",            # SCHWERES GEWITTER mit ORKANBÖEN und HEFTIGEM STARKREGEN
    "45": "gewitter",            # SCHWERES GEWITTER mit EXTREMEN ORKANBÖEN und HEFTIGEM STARKREGEN
    "46": "gewitter",            # SCHWERES GEWITTER mit HEFTIGEM STARKREGEN und HAGEL
    "48": "gewitter",            # SCHWERES GEWITTER mit ORKANBÖEN, HEFTIGEM STARKREGEN und HAGEL
    "49": "gewitter",            # SCHWERES GEWITTER mit EXTREMEN ORKANBÖEN, HEFTIGEM STARKREGEN und HAGEL
    # Wind / Sturm
    "51": "sturm_orkan",         # WINDBÖEN
    "52": "sturm_orkan",         # STURMBÖEN
    "53": "sturm_orkan",         # SCHWERE STURMBÖEN
    "54": "sturm_orkan",         # ORKANARTIGE BÖEN
    "55": "sturm_orkan",         # ORKANBÖEN
    "56": "sturm_orkan",         # EXTREME ORKANBÖEN
    # Küstenwarnungen Wind
    "11": "sturm_orkan",         # BÖEN
    "12": "sturm_orkan",         # WIND
    "13": "sturm_orkan",         # STURM
    "14": "sturm_orkan",         # Starkwind
    "15": "sturm_orkan",         # Sturm
    "16": "sturm_orkan",         # schwerer Sturm
    # Regen
    "61": "dauerregen",          # REGEN
    "62": "dauerregen",          # ERGIEBIGER REGEN
    "63": "dauerregen",          # STARK ERGIEBIGER REGEN
    "64": "dauerregen",          # EXTREM ERGIEBIGER REGEN
    "65": "dauerregen",          # EXTREM ERGIEBIGER DAUERREGEN
    "66": "starkregen",          # STARKREGEN
    "67": "starkregen",          # HEFTIGER STARKREGEN
    "68": "starkregen",          # EXTREM HEFTIGER STARKREGEN
    # Schnee
    "70": "schnee_schneesturm",  # LEICHTER SCHNEEFALL
    "71": "schnee_schneesturm",  # SCHNEEFALL
    "72": "schnee_schneesturm",  # STARKER SCHNEEFALL
    "73": "schnee_schneesturm",  # EXTREM STARKER SCHNEEFALL
    "74": "schnee_schneesturm",  # SCHNEEVERWEHUNG
    "75": "schnee_schneesturm",  # STARKE SCHNEEVERWEHUNG
    "76": "schnee_schneesturm",  # SCHNEEFALL und SCHNEEVERWEHUNG
    "77": "schnee_schneesturm",  # STARKER SCHNEEFALL und SCHNEEVERWEHUNG
    "78": "schnee_schneesturm",  # EXTREM STARKER SCHNEEFALL und SCHNEEVERWEHUNG
    # Frost / Glätte
    "81": "glaette_frost",       # FROST
    "82": "glaette_frost",       # STRENGER FROST
    "84": "glaette_frost",       # GLÄTTE
    "85": "glaette_frost",       # GLATTEIS
    "87": "glaette_frost",       # GLATTEIS
    # Tauwetter
    "88": "tauwetter",           # TAUWETTER
    "89": "tauwetter",           # STARKES TAUWETTER
    # Gewitter (Stufe 4)
    "90": "gewitter",            # GEWITTER
    "91": "gewitter",            # STARKES GEWITTER
    "92": "gewitter",            # SCHWERES GEWITTER
    "93": "gewitter",            # EXTREMES GEWITTER
    "95": "gewitter",            # SCHWERES GEWITTER mit EXTREM HEFTIGEM STARKREGEN und HAGEL
    "96": "gewitter",            # EXTREMES GEWITTER mit ORKANBÖEN, EXTREM HEFTIGEM STARKREGEN und HAGEL
    # Hitze
    "247": "hitze",              # STARKE HITZE
    "248": "hitze",              # EXTREME HITZE
    # UV
    "246": "uv",                 # UV-INDEX
    # Vorabinformationen (Stufe 1)
    # ii=40 Vorabinfo Gewitter, 55 Vorabinfo Orkanböen, etc. – auf Stufe 1 gemappt
}

# Fallback: englische event-Strings (kommen vor wenn ii-Code fehlt)
_DWD_EVENT_MAP_TEXT = {
    "THUNDERSTORM": "gewitter", "SEVERE_THUNDERSTORM": "gewitter",
    "WIND": "sturm_orkan", "SQUALL": "sturm_orkan", "STORM_FORCE_WINDS": "sturm_orkan",
    "HURRICANE_FORCE_WINDS": "sturm_orkan", "GALE_FORCE_WINDS": "sturm_orkan",
    "HEAVY_RAIN": "starkregen", "EXTREMELY_HEAVY_RAIN": "starkregen",
    "PERSISTENT_RAIN": "dauerregen", "RAIN": "dauerregen",
    "FLOODING": "hochwasser", "FLASH_FLOODING": "hochwasser",
    "SNOW": "schnee_schneesturm", "HEAVY_SNOW": "schnee_schneesturm", "SNOWSTORM": "schnee_schneesturm",
    "BLACK_ICE": "glaette_frost", "FROST": "glaette_frost", "FREEZING_RAIN": "glaette_frost",
    "HEAT": "hitze", "STRONG_HEAT": "hitze", "EXTREME_HEAT": "hitze",
    "FOG": "nebel", "DENSE_FOG": "nebel",
    "UV": "uv",
}

# Kombinierte Map für schnellen Zugriff
_DWD_EVENT_MAP = {**_DWD_EVENT_MAP_II, **_DWD_EVENT_MAP_TEXT}


def _guess_dwd_event(headline: str, description: str = "") -> Optional[str]:
    text = (headline + " " + description).lower()
    if any(w in text for w in ["gewitter", "blitz", "hagel"]):
        return "gewitter"
    if any(w in text for w in ["orkan", "hurricane"]):
        return "sturm_orkan"
    if any(w in text for w in ["sturmböen", "windböen", "sturm"]):
        return "sturm_orkan"
    if "wind" in text and any(w in text for w in ["stark", "heftig", "böen"]):
        return "sturm_orkan"
    if any(w in text for w in ["dauerregen", "anhaltend", "ergiebig", "langanhaltend"]):
        return "dauerregen"
    if any(w in text for w in ["starkregen", "heftiger regen"]):
        return "starkregen"
    if "regen" in text and any(w in text for w in ["stark", "heftig", "extrem"]):
        return "starkregen"
    if "regen" in text:
        return "dauerregen"
    if any(w in text for w in ["hochwasser", "überschwemmung", "flut"]):
        return "hochwasser"
    if any(w in text for w in ["schneesturm", "blizzard"]):
        return "schnee_schneesturm"
    if any(w in text for w in ["schnee"]):
        return "schnee_schneesturm"
    if any(w in text for w in ["frost", "glätte", "glatteis", "eisregen"]):
        return "glaette_frost"
    if "hitze" in text:
        return "hitze"
    if "nebel" in text:
        return "nebel"
    if any(w in text for w in ["uv-index", "ultraviolett"]):
        return "uv"
    return None


# ---------------------------------------------------------------------------
# DWD
# ---------------------------------------------------------------------------

def _normalize_dwd_dashboard(item: dict) -> Optional[NormalizedWarning]:
    """
    DWD im kompakten Dashboard-Format (kein info-Array).
    Struktur: {id, payload.data: {headline, severity, urgency, msgType, area, valid},
               sent, onset, expires, effective, i18nTitle}
    """
    try:
        payload = item.get("payload", {})
        data = payload.get("data", {})
        identifier = item.get("id", "unknown")

        valid = data.get("valid", True)
        status = "actual" if valid else "cancelled"
        msg_type = data.get("msgType", "Alert")

        severity_str = data.get("severity", "Moderate")
        dwd_level = _DWD_SEVERITY_LEVEL.get(severity_str, 2)
        severity = _LEVEL_TO_SEVERITY.get(dwd_level, "Moderate")
        urgency = _URGENCY_MAP.get(data.get("urgency", ""), "Expected")

        i18n = item.get("i18nTitle", {})
        headline = i18n.get("de") or data.get("headline", "").strip()

        # Event-Typ aus Headline raten (kein eventCode im Dashboard-Format)
        dwd_event_type = _guess_dwd_event(headline)
        if not dwd_event_type:
            logger.debug("DWD Dashboard: Event-Typ unbekannt für '%s'", headline)

        area_data = data.get("area", {})
        area_type = area_data.get("type", "")
        area_desc = f"Gebiet ({area_type})" if area_type else "Unbekanntes Gebiet"

        sent = _parse_dt(item.get("sent")) or datetime.now(timezone.utc)

        return NormalizedWarning(
            source="dwd", identifier=identifier, status=status,
            msg_type=msg_type, severity=severity, urgency=urgency,
            headline=headline, description="", instruction="",
            area_desc=area_desc, ags_codes=[],
            sent=sent, effective=_parse_dt(item.get("effective")),
            expires=_parse_dt(item.get("expires")),
            dwd_event_type=dwd_event_type, dwd_level=dwd_level,
        )
    except Exception as e:
        logger.exception("Fehler DWD-Dashboard: %s", e)
        return None


def normalize_dwd(item: dict) -> Optional[NormalizedWarning]:
    try:
        payload = item.get("payload", {})
        data = payload.get("data", {})
        info_list = data.get("info", [])

        # Dashboard-Format: kein info-Array, aber headline direkt in data
        if not info_list and data.get("headline"):
            return _normalize_dwd_dashboard(item)

        info = next((i for i in info_list if i.get("language", "").startswith("de")), None)
        if info is None and info_list:
            info = info_list[0]
        if info is None:
            # Letzter Versuch: Dashboard-Format ohne headline-Check
            if data.get("severity") or item.get("i18nTitle"):
                return _normalize_dwd_dashboard(item)
            return None

        # Event-Typ: zuerst ii-Code (numerisch), dann englischer event-String
        ii_code = ""
        raw_event_text = ""
        for ec in info.get("eventCode", []):
            vn = ec.get("valueName", "")
            if vn == "II":
                ii_code = str(ec.get("value", "")).strip()
            elif vn == "eventCode":
                raw_event_text = ec.get("value", "").upper()
        if not raw_event_text:
            raw_event_text = info.get("event", "").upper().replace(" ", "_")

        # ii-Code hat Vorrang (offizieller DWD CAP Code)
        dwd_event_type = _DWD_EVENT_MAP_II.get(ii_code)
        if not dwd_event_type:
            dwd_event_type = _DWD_EVENT_MAP_TEXT.get(raw_event_text)
        if not dwd_event_type:
            dwd_event_type = _guess_dwd_event(
                info.get("headline", ""),
                info.get("description", ""),
            )

        severity_str = info.get("severity", "Moderate")
        dwd_level = _DWD_SEVERITY_LEVEL.get(severity_str, 2)
        severity = _LEVEL_TO_SEVERITY.get(dwd_level, "Moderate")

        identifier = data.get("identifier") or item.get("id", "unknown")
        status = data.get("status", "actual").lower()
        msg_type = data.get("msgType", "Alert")
        urgency = _URGENCY_MAP.get(info.get("urgency", ""), "Expected")
        headline = info.get("headline", "").strip()
        description = info.get("description", "").strip()
        instruction = info.get("instruction", "").strip()

        area_list = info.get("area", [])
        area_desc = ", ".join(a.get("areaDesc", "") for a in area_list if a.get("areaDesc"))
        ags_codes = []
        for area in area_list:
            for gc in area.get("geocode", []):
                if gc.get("valueName") in ("AGS", "ags"):
                    ags_codes.append(gc.get("value", ""))

        sent = _parse_dt(data.get("sent")) or datetime.now(timezone.utc)
        return NormalizedWarning(
            source="dwd", identifier=identifier, status=status,
            msg_type=msg_type, severity=severity, urgency=urgency,
            headline=headline, description=description, instruction=instruction,
            area_desc=area_desc, ags_codes=ags_codes,
            sent=sent, effective=_parse_dt(info.get("effective")),
            expires=_parse_dt(info.get("expires")),
            dwd_event_type=dwd_event_type, dwd_level=dwd_level,
        )
    except Exception as e:
        logger.exception("Fehler DWD: %s", e)
        return None
