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
    # Pflichtfelder
    source: str                      # "mowas", "dwd", "lhp", "dab"
    identifier: str                  # CAP identifier (für Dedup)
    status: str                      # "actual" oder "test"
    msg_type: str                    # "Alert", "Update", "Cancel"
    severity: str                    # "Extreme", "Severe", "Moderate", "Minor"
    urgency: str                     # "Immediate", "Expected", "Future", "Past"
    headline: str                    # Kurztitel (max ~80 Zeichen)
    description: str                 # Volltext
    instruction: str                 # Verhaltensempfehlung

    # Geo
    area_desc: str                   # Lesbare Gebietsbezeichnung
    ags_codes: list[str]             # AGS-Schlüssel ["08115", "08111", ...]

    # Zeitstempel
    sent: datetime                   # Wann gesendet (UTC)
    effective: Optional[datetime]    # Ab wann gültig
    expires: Optional[datetime]      # Bis wann gültig

    # DWD-spezifisch
    dwd_event_type: Optional[str] = None   # "gewitter", "sturm_orkan", etc.
    dwd_level: Optional[int] = None        # 1-4

    # Intern
    content_hash: str = ""           # MD5 für Content-Dedup (wird automatisch gesetzt)
    received_at: datetime = field(default_factory=lambda: datetime.now(timezone.utc))

    def __post_init__(self):
        if not self.content_hash:
            sent_str = self.sent.isoformat() if self.sent else ""
            self.content_hash = make_hash_with_date(self.headline, sent_str)


def _make_hash(headline: str, area_desc: str) -> str:
    # headline-only: area_desc variiert bei gleicher Warnung über Kreise hinweg.
    # Wird in NormalizedWarning.__post_init__ aufgerufen – sent-Zeit wird dort
    # als separates Feld gesetzt und kann vom Aufrufer per content_hash überschrieben werden.
    raw = headline.strip().lower()
    return hashlib.md5(raw.encode()).hexdigest()


def make_hash_with_date(headline: str, sent_date: str) -> str:
    """
    Hash aus headline + Datum (nicht Uhrzeit).
    Verhindert dass die gleiche Warnung an verschiedenen Tagen als Duplikat gilt,
    aber 44 Kreis-Instanzen derselben Tageswarnung werden zusammengefasst.
    """
    raw = f"{headline.strip().lower()}|{sent_date[:10]}"  # nur YYYY-MM-DD
    return hashlib.md5(raw.encode()).hexdigest()


def _parse_dt(s: Optional[str]) -> Optional[datetime]:
    """Parst ISO-8601 Zeitstrings der NINA API."""
    if not s:
        return None
    try:
        # NINA liefert z.B. "2026-09-10T11:00:00.000+02:00"
        s = s.replace("Z", "+00:00")
        return datetime.fromisoformat(s).astimezone(timezone.utc)
    except Exception:
        return None


# ---------------------------------------------------------------------------
# MoWaS-Normalisierung
# ---------------------------------------------------------------------------

_SEVERITY_MAP = {
    "Extreme": "Extreme",
    "Severe": "Severe",
    "Moderate": "Moderate",
    "Minor": "Minor",
    "Unknown": "Minor",
}

_URGENCY_MAP = {
    "Immediate": "Immediate",
    "Expected": "Expected",
    "Future": "Future",
    "Past": "Past",
    "Unknown": "Future",
}


def _normalize_mowas_dashboard(item: dict) -> Optional[NormalizedWarning]:
    """
    Normalisiert ein MoWaS-Item im kompakten Dashboard-Format.
    Struktur: {id, payload: {data: {headline, severity, urgency, msgType, area, valid}}, sent, i18nTitle}
    """
    try:
        payload = item.get("payload", {})
        data = payload.get("data", {})
        identifier = item.get("id", "unknown")

        # Status: valid=true → actual, valid=false → expired
        valid = data.get("valid", True)
        status = "actual" if valid else "cancelled"

        msg_type = data.get("msgType", "Alert")
        severity = _SEVERITY_MAP.get(data.get("severity", ""), "Minor")
        urgency = _URGENCY_MAP.get(data.get("urgency", ""), "Expected")

        # Headline: aus i18nTitle DE bevorzugen, sonst data.headline
        i18n = item.get("i18nTitle", {})
        headline = i18n.get("de") or data.get("headline", "").strip()

        # Dashboard-Format hat keine description/instruction – leer lassen
        description = ""
        instruction = ""

        # Gebiet: area.type kann GRID, ZGEM, POLY sein – kein AGS direkt
        # AGS wird aus dem Kreis abgeleitet der dieses Item geliefert hat
        area_data = data.get("area", {})
        area_type = area_data.get("type", "")
        area_desc = f"Gebiet ({area_type})" if area_type else "Unbekanntes Gebiet"
        ags_codes = []  # Dashboard-Format liefert keine AGS-Codes direkt

        sent = _parse_dt(item.get("sent")) or datetime.now(timezone.utc)

        return NormalizedWarning(
            source="mowas",
            identifier=identifier,
            status=status,
            msg_type=msg_type,
            severity=severity,
            urgency=urgency,
            headline=headline,
            description=description,
            instruction=instruction,
            area_desc=area_desc,
            ags_codes=ags_codes,
            sent=sent,
            effective=None,
            expires=None,
        )
    except Exception as e:
        logger.exception("Fehler beim Normalisieren eines MoWaS-Dashboard-Eintrags: %s", e)
        return None


def normalize_mowas(item: dict) -> Optional[NormalizedWarning]:
    """
    Normalisiert einen MoWaS-Eintrag aus der NINA API.
    Unterstützt beide Formate:
    - Dashboard-Format: payload.data direkt (headline, severity, urgency etc.)
    - Vollformat: payload.data.info[] mit CAP-Feldern
    """
    try:
        payload = item.get("payload", {})
        data = payload.get("data", {})
        info_list = data.get("info", [])

        # Dashboard-Format erkennen: kein info-Block, aber headline direkt in data
        if not info_list and data.get("headline"):
            return _normalize_mowas_dashboard(item)

        # Vollformat
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

        # Geo
        area_list = info.get("area", [])
        area_desc = ", ".join(a.get("areaDesc", "") for a in area_list if a.get("areaDesc"))
        ags_codes = []
        for area in area_list:
            for gc in area.get("geocode", []):
                if gc.get("valueName") in ("AGS", "ags"):
                    ags_codes.append(gc.get("value", ""))

        sent = _parse_dt(data.get("sent")) or datetime.now(timezone.utc)
        effective = _parse_dt(info.get("effective"))
        expires = _parse_dt(info.get("expires"))

        return NormalizedWarning(
            source="mowas",
            identifier=identifier,
            status=status,
            msg_type=msg_type,
            severity=severity,
            urgency=urgency,
            headline=headline,
            description=description,
            instruction=instruction,
            area_desc=area_desc,
            ags_codes=ags_codes,
            sent=sent,
            effective=effective,
            expires=expires,
        )

    except Exception as e:
        logger.exception("Fehler beim Normalisieren eines MoWaS-Eintrags: %s", e)
        return None


# ---------------------------------------------------------------------------
# DWD-Normalisierung
# ---------------------------------------------------------------------------

# Mapping DWD event → interne Kategorie
# Quelle: NINA API DWD event_codes
_DWD_EVENT_MAP = {
    # Gewitter
    "THUNDERSTORM": "gewitter",
    "THUNDERSTORM_WIND": "gewitter",
    "THUNDERSTORM_STRONG_WIND": "gewitter",
    "THUNDERSTORM_HEAVY_RAIN": "gewitter",
    "THUNDERSTORM_HAIL": "gewitter",
    "THUNDERSTORM_EXTREMELY_HEAVY_RAIN": "gewitter",
    # Sturm
    "WIND": "sturm_orkan",
    "SQUALL": "sturm_orkan",
    "STORM_FORCE_WINDS": "sturm_orkan",
    "HURRICANE_FORCE_WINDS": "sturm_orkan",
    # Starkregen
    "HEAVY_RAIN": "starkregen",
    "EXTREMELY_HEAVY_RAIN": "starkregen",
    "HEAVY_RAIN_CAUSING_FLOODING": "starkregen",
    # Hochwasser
    "FLOODING": "hochwasser",
    "COASTAL_FLOODING": "hochwasser",
    "FLASH_FLOODING": "hochwasser",
    # Schnee
    "SNOW": "schnee_schneesturm",
    "HEAVY_SNOW": "schnee_schneesturm",
    "SNOWSTORM": "schnee_schneesturm",
    "BLACK_ICE": "glaette_frost",
    "FROST": "glaette_frost",
    "FREEZING_RAIN": "glaette_frost",
    # Hitze
    "HEAT": "hitze",
    "STRONG_HEAT": "hitze",
    # Nebel
    "FOG": "nebel",
    "DENSE_FOG": "nebel",
}

# DWD severity → interner Level (1-4)
_DWD_SEVERITY_LEVEL = {
    "Minor": 1,
    "Moderate": 2,
    "Severe": 3,
    "Extreme": 4,
}

# Level → Severity-String für NormalizedWarning
_LEVEL_TO_SEVERITY = {1: "Minor", 2: "Moderate", 3: "Severe", 4: "Extreme"}


def normalize_dwd(item: dict) -> Optional[NormalizedWarning]:
    """
    Normalisiert einen DWD-Eintrag aus der NINA API.
    """
    try:
        payload = item.get("payload", {})
        data = payload.get("data", {})
        info_list = data.get("info", [])

        info = next((i for i in info_list if i.get("language", "").startswith("de")), None)
        if info is None and info_list:
            info = info_list[0]
        if info is None:
            return None

        # DWD event type aus eventCode oder event-Feld
        raw_event = ""
        for ec in info.get("eventCode", []):
            if ec.get("valueName") in ("II", "eventCode"):
                raw_event = ec.get("value", "").upper()
                break
        if not raw_event:
            raw_event = info.get("event", "").upper().replace(" ", "_")

        dwd_event_type = _DWD_EVENT_MAP.get(raw_event)
        # Fallback: keywords aus headline
        if not dwd_event_type:
            dwd_event_type = _guess_dwd_event(info.get("headline", ""))

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
        effective = _parse_dt(info.get("effective"))
        expires = _parse_dt(info.get("expires"))

        return NormalizedWarning(
            source="dwd",
            identifier=identifier,
            status=status,
            msg_type=msg_type,
            severity=severity,
            urgency=urgency,
            headline=headline,
            description=description,
            instruction=instruction,
            area_desc=area_desc,
            ags_codes=ags_codes,
            sent=sent,
            effective=effective,
            expires=expires,
            dwd_event_type=dwd_event_type,
            dwd_level=dwd_level,
        )

    except Exception as e:
        logger.exception("Fehler beim Normalisieren eines DWD-Eintrags: %s", e)
        return None


def _guess_dwd_event(headline: str) -> Optional[str]:
    """Fallback: DWD-Kategorie aus Headline raten."""
    h = headline.lower()
    if any(w in h for w in ["gewitter", "blitz"]):
        return "gewitter"
    if any(w in h for w in ["sturm", "orkan", "wind"]):
        return "sturm_orkan"
    if any(w in h for w in ["regen", "niederschlag"]):
        return "starkregen"
    if any(w in h for w in ["hochwasser", "überschwemmung"]):
        return "hochwasser"
    if any(w in h for w in ["schnee", "schneesturm"]):
        return "schnee_schneesturm"
    if any(w in h for w in ["frost", "glätte", "eis"]):
        return "glaette_frost"
    if "hitze" in h:
        return "hitze"
    if "nebel" in h:
        return "nebel"
    return None
