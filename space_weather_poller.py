"""
space_weather_poller.py – WarnBridge
Pollt NOAA SWPC API auf relevante Weltraumwetter-Ereignisse.
Sendet NormalizedWarning-Objekte via on_warning-Callback an warnbridge.py.

Quellen:
  - products/noaa-scales.json  → strukturierter G/R/S-Status + 3-Tage-Forecast
  - products/alerts.json       → Freitext-Alerts mit product_id als Schlüssel

Trigger-Logik:
  - Watch G3+ (A50F+)        → Vorlaufwarnung (1–3 Tage)
  - Alert/Warning G3+ (K07+) → Akutwarnung
  - X-Flare R3+ (XX0S)       → optional, default aus
  - S3+ Proton (P13+)        → optional, default aus
  - G1/G2, M-Flares          → immer ignoriert (zu häufig, kein Effekt in DE)
"""

import asyncio
import logging
import hashlib
from datetime import datetime, timezone
from typing import Callable, Awaitable, Optional

import aiohttp

from cap_normalizer import NormalizedWarning

logger = logging.getLogger(__name__)

NOAA_BASE = "https://services.swpc.noaa.gov"
SCALES_URL = f"{NOAA_BASE}/products/noaa-scales.json"
ALERTS_URL = f"{NOAA_BASE}/products/alerts.json"

# product_id → (skala, stufe_int)
# K = geomagnetic, A = watch/forecast, XX = X-flare, P = proton
_PRODUCT_G_LEVEL = {
    "K07W": ("G", 3), "K07A": ("G", 3),
    "K08W": ("G", 4), "K08A": ("G", 4),
    "K09W": ("G", 5), "K09A": ("G", 5),
}
_PRODUCT_WATCH_LEVEL = {
    "A50F": ("G", 3),
    "A70F": ("G", 4),
    "A90F": ("G", 5),
}
_PRODUCT_XFLARE = {"XX0S"}       # X1.0+ → R3+
_PRODUCT_PROTON = {
    "P13A": ("S", 3), "P13W": ("S", 3),
    "P14A": ("S", 4), "P14W": ("S", 4),
    "P15A": ("S", 5), "P15W": ("S", 5),
}

_G_TEXT = {1: "Minor", 2: "Moderate", 3: "Strong", 4: "Severe", 5: "Extreme"}
_S_TEXT = {3: "Strong", 4: "Severe", 5: "Extreme"}


def _sw_hash(identifier: str) -> str:
    return hashlib.md5(identifier.encode()).hexdigest()


def _parse_noaa_dt(s: str) -> Optional[datetime]:
    if not s:
        return None
    try:
        # Format: "2026 Jun 05 1714 UTC"
        return datetime.strptime(s.strip(), "%Y %b %d %H%M UTC").replace(tzinfo=timezone.utc)
    except Exception:
        try:
            return datetime.fromisoformat(s).astimezone(timezone.utc)
        except Exception:
            return None


def _extract_field(message: str, field: str) -> str:
    """Einfacher Zeilenparser für NOAA Alert-Texte."""
    for line in message.splitlines():
        if line.startswith(field + ":"):
            return line.split(":", 1)[1].strip()
    return ""


def _is_cancelled(message: str) -> bool:
    first_line = message.strip().splitlines()[0] if message.strip() else ""
    return "CANCELLED" in first_line.upper()


def _is_extended(message: str) -> bool:
    """EXTENDED WARNING = Verlängerung, keine neue Warnung."""
    for line in message.splitlines():
        if line.strip().startswith("EXTENDED WARNING"):
            return True
    return False


class SpaceWeatherPoller:
    def __init__(self, config: dict, on_warning: Callable[[NormalizedWarning], Awaitable[None]]):
        self.cfg = config
        self.on_warning = on_warning
        self._running = False
        self._seen_serials: set[str] = set()  # bereits gesehene Serial Numbers
        self._last_scales_g: int = 0           # letzter bekannter G-Level aus scales.json
        # Tages-Dedup für G-Level: verhindert dass Watch + Alert für gleichen Level
        # am selben Tag doppelt ins Mesh gehen
        self._sent_g_today: dict[int, str] = {}  # g_level → Datum (YYYY-MM-DD)
        self._session: Optional[aiohttp.ClientSession] = None

        sw = config.get("space_weather", {})
        self.enabled = sw.get("enabled", False)
        self.poll_interval = int(sw.get("poll_interval_minutes", 15)) * 60

        geo = sw.get("geomagnetic", {})
        self.geo_enabled = geo.get("enabled", True)
        self.geo_min_level = int(geo.get("min_level", 3))
        self.geo_watch_enabled = geo.get("watch_enabled", True)

        rad = sw.get("solar_radiation", {})
        self.rad_enabled = rad.get("enabled", False)
        self.rad_min_level = int(rad.get("min_level", 3))

        rbl = sw.get("radio_blackout", {})
        self.rbl_enabled = rbl.get("enabled", False)
        self.rbl_min_level = int(rbl.get("min_level", 3))

        self._status = {
            "running": False,
            "last_poll": None,
            "error": None,
            "current_g": 0,
            "forecast_g": [],
        }

    def update_config(self, config: dict):
        sw = config.get("space_weather", {})
        self.enabled = sw.get("enabled", False)
        self.poll_interval = int(sw.get("poll_interval_minutes", 15)) * 60
        geo = sw.get("geomagnetic", {})
        self.geo_enabled = geo.get("enabled", True)
        self.geo_min_level = int(geo.get("min_level", 3))
        self.geo_watch_enabled = geo.get("watch_enabled", True)
        rad = sw.get("solar_radiation", {})
        self.rad_enabled = rad.get("enabled", False)
        self.rad_min_level = int(rad.get("min_level", 3))
        rbl = sw.get("radio_blackout", {})
        self.rbl_enabled = rbl.get("enabled", False)

    async def run(self):
        if not self.enabled:
            logger.info("SpaceWeatherPoller: deaktiviert (space_weather.enabled: false)")
            return

        self._running = True
        self._status["running"] = True
        logger.info("SpaceWeatherPoller gestartet (Interval: %ds)", self.poll_interval)

        async with aiohttp.ClientSession(
            timeout=aiohttp.ClientTimeout(total=20)
        ) as session:
            self._session = session
            while self._running:
                try:
                    await self._poll(session)
                except asyncio.CancelledError:
                    break
                except Exception as e:
                    logger.error("SpaceWeatherPoller Fehler: %s", e)
                    self._status["error"] = str(e)
                await asyncio.sleep(self.poll_interval)

    def stop(self):
        self._running = False

    def status(self) -> dict:
        return dict(self._status)

    # ------------------------------------------------------------------
    # Poll
    # ------------------------------------------------------------------

    async def _poll(self, session: aiohttp.ClientSession):
        now = datetime.now(timezone.utc)
        self._status["last_poll"] = now.isoformat()
        self._status["error"] = None

        # 1. scales.json – aktueller G/R/S-Level + Forecast
        try:
            async with session.get(SCALES_URL) as resp:
                if resp.status == 200:
                    scales = await resp.json(content_type=None)
                    await self._process_scales(scales)
        except Exception as e:
            logger.warning("SpaceWeather scales.json Fehler: %s", e)

        # 2. alerts.json – neue Alerts parsen
        try:
            async with session.get(ALERTS_URL) as resp:
                if resp.status == 200:
                    alerts = await resp.json(content_type=None)
                    await self._process_alerts(alerts)
        except Exception as e:
            logger.warning("SpaceWeather alerts.json Fehler: %s", e)

    # ------------------------------------------------------------------
    # scales.json verarbeiten
    # ------------------------------------------------------------------

    async def _process_scales(self, scales: dict):
        """Aktuellen G/R/S-Level aus scales.json auslesen und Forecast speichern."""
        current = scales.get("0", {})
        g_scale = int(current.get("G", {}).get("Scale") or 0)
        r_scale = int(current.get("R", {}).get("Scale") or 0)
        s_scale = int(current.get("S", {}).get("Scale") or 0)

        self._status["current_g"] = g_scale
        self._status["current_r"] = r_scale
        self._status["current_s"] = s_scale

        # Forecast für /swdetails
        forecast = []
        for day_key in ["1", "2", "3"]:
            day = scales.get(day_key, {})
            date = day.get("DateStamp", "")
            g = day.get("G", {})
            forecast.append({
                "date": date,
                "g_scale": int(g.get("Scale") or 0),
                "g_text": g.get("Text", "none"),
            })
        self._status["forecast_g"] = forecast

    # ------------------------------------------------------------------
    # alerts.json verarbeiten
    # ------------------------------------------------------------------

    async def _process_alerts(self, alerts: list):
        for item in alerts:
            try:
                await self._process_alert(item)
            except Exception as e:
                logger.debug("Alert-Verarbeitung Fehler: %s", e)

    async def _process_alert(self, item: dict):
        product_id = item.get("product_id", "")
        message = item.get("message", "")
        issue_dt_str = item.get("issue_datetime", "")

        # Serial Number als Dedup-Schlüssel
        serial = _extract_field(message, "Serial Number")
        if not serial:
            return
        dedup_key = f"{product_id}:{serial}"
        if dedup_key in self._seen_serials:
            return

        # CANCELLED: als gesehen markieren, keine Warnung
        if _is_cancelled(message):
            self._seen_serials.add(dedup_key)
            logger.debug("SpaceWeather: CANCELLED Alert ignoriert: %s #%s", product_id, serial)
            return

        # EXTENDED WARNING: keine neue Mesh-Nachricht, nur als gesehen markieren
        if _is_extended(message):
            self._seen_serials.add(dedup_key)
            logger.debug("SpaceWeather: EXTENDED WARNING ignoriert: %s #%s", product_id, serial)
            return

        # Entscheiden ob relevant
        warning = self._build_warning(product_id, message, issue_dt_str)
        if warning is None:
            self._seen_serials.add(dedup_key)
            return

        self._seen_serials.add(dedup_key)
        # Seen-Set begrenzen
        if len(self._seen_serials) > 2000:
            oldest = list(self._seen_serials)[:500]
            for k in oldest:
                self._seen_serials.discard(k)

        # G-Level Tages-Dedup: Watch + Alert für gleichen Level → nur 1 Nachricht/Tag
        # Höherer Level (G4 nach G3) darf trotzdem durch
        if warning.source == "space_weather" and warning.description:
            today = datetime.now(timezone.utc).strftime("%Y-%m-%d")
            # G-Level aus Headline extrahieren (z.B. "G3-Sturm")
            import re
            m = re.search(r'G(\d)', warning.headline)
            if m:
                g_level = int(m.group(1))
                last_sent = self._sent_g_today.get(g_level)
                if last_sent == today:
                    logger.debug("SpaceWeather: G%d bereits heute gesendet – übersprungen", g_level)
                    return
                # Alten Tag bereinigen
                self._sent_g_today = {
                    lvl: d for lvl, d in self._sent_g_today.items() if d == today
                }
                self._sent_g_today[g_level] = today

        logger.info("SpaceWeather: Neue Warnung [%s] %s", product_id, warning.headline)
        await self.on_warning(warning)

    # ------------------------------------------------------------------
    # NormalizedWarning aufbauen
    # ------------------------------------------------------------------

    def _build_warning(self, product_id: str, message: str, issue_dt_str: str) -> Optional[NormalizedWarning]:
        now = datetime.now(timezone.utc)
        issued_dt = now
        if issue_dt_str:
            try:
                issued_dt = datetime.fromisoformat(issue_dt_str.replace(" ", "T")).replace(tzinfo=timezone.utc)
            except Exception:
                pass

        # --- Geomagnetic Alert/Warning G3+ ---
        if product_id in _PRODUCT_G_LEVEL and self.geo_enabled:
            _, level = _PRODUCT_G_LEVEL[product_id]
            if level < self.geo_min_level:
                return None
            return self._make_geo_alert(product_id, message, level, issued_dt, is_watch=False)

        # --- Geomagnetic Watch G3+ (1–3 Tage Vorlauf) ---
        if product_id in _PRODUCT_WATCH_LEVEL and self.geo_enabled and self.geo_watch_enabled:
            _, level = _PRODUCT_WATCH_LEVEL[product_id]
            if level < self.geo_min_level:
                return None
            return self._make_geo_alert(product_id, message, level, issued_dt, is_watch=True)

        # --- X-Flare R3+ ---
        if product_id in _PRODUCT_XFLARE and self.rbl_enabled:
            return self._make_xflare_alert(message, issued_dt)

        # --- Proton Event S3+ ---
        if product_id in _PRODUCT_PROTON and self.rad_enabled:
            _, level = _PRODUCT_PROTON[product_id]
            if level < self.rad_min_level:
                return None
            return self._make_proton_alert(message, level, issued_dt)

        return None

    def _make_geo_alert(self, product_id: str, message: str, level: int,
                        issued_dt: datetime, is_watch: bool) -> NormalizedWarning:
        g_text = _G_TEXT.get(level, "Strong")
        serial = _extract_field(message, "Serial Number")
        identifier = f"sw-geo-{product_id}-{serial}-{issued_dt.strftime('%Y%m%d')}"

        # Gültigkeitszeitraum aus Nachricht
        valid_from_str = _extract_field(message, "Valid From") or _extract_field(message, "Highest Storm Level Predicted by Day")
        valid_to_str = _extract_field(message, "Valid To") or _extract_field(message, "Now Valid Until")

        if is_watch:
            # Watch: Vorlaufmeldung mit Datum
            day_str = ""
            for line in message.splitlines():
                if "Predicted by Day" in line or ("Jun" in line and "G" in line):
                    day_str = line.strip()
                    break
            headline = f"Weltraumwetter: G{level}-Sturm ({g_text}) erwartet"
            desc = f"Vorlaufwarnung 1–3 Tage. GPS + HF-Funk Stoerung moeglich."
            if valid_from_str:
                desc += f" {valid_from_str}"
        else:
            # Alert/Warning: akuter Event
            action_word = "ALERT" if "ALERT" in message[:100] else "WARNING"
            headline = f"Weltraumwetter: G{level}-Sturm {action_word}"
            valid_str = ""
            if valid_from_str:
                valid_str = f" ab {valid_from_str}"
            if valid_to_str:
                valid_str += f" bis {valid_to_str}"
            desc = f"G{level} ({g_text}). GPS + HF-Funk Stoerung moeglich.{valid_str}"

        # Severity für NormalizedWarning
        severity_map = {3: "Severe", 4: "Extreme", 5: "Extreme"}
        severity = severity_map.get(level, "Severe")

        # Forecast für DB/Details
        forecast_lines = []
        for fc in self._status.get("forecast_g", []):
            if fc["g_scale"] > 0:
                forecast_lines.append(f"{fc['date']}: G{fc['g_scale']} ({fc['g_text']})")
        instruction = "Prognose: " + " | ".join(forecast_lines) if forecast_lines else ""

        # content_hash aus headline + Stunde
        sent_str = issued_dt.isoformat()
        from cap_normalizer import make_hash_with_date
        content_hash = make_hash_with_date(headline, sent_str)

        return NormalizedWarning(
            source="space_weather",
            identifier=identifier,
            status="actual",
            msg_type="Alert" if not is_watch else "Update",
            severity=severity,
            urgency="Expected" if is_watch else "Immediate",
            headline=headline,
            description=desc,
            instruction=instruction,
            area_desc="Global / Europa",
            ags_codes=[],
            sent=issued_dt,
            effective=issued_dt,
            expires=None,
            content_hash=content_hash,
        )

    def _make_xflare_alert(self, message: str, issued_dt: datetime) -> NormalizedWarning:
        xray_class = _extract_field(message, "Xray Class") or "X1.0"
        serial = _extract_field(message, "Serial Number")
        begin = _extract_field(message, "Begin Time")
        identifier = f"sw-xflare-{serial}-{issued_dt.strftime('%Y%m%d%H%M')}"

        headline = f"Weltraumwetter: Solar Flare {xray_class} (R3)"
        desc = f"HF-Funk Ausfall ~1h auf Sonnenseite."
        if begin:
            desc += f" Beginn: {begin} UTC"

        from cap_normalizer import make_hash_with_date
        content_hash = make_hash_with_date(headline, issued_dt.isoformat())

        return NormalizedWarning(
            source="space_weather",
            identifier=identifier,
            status="actual",
            msg_type="Alert",
            severity="Severe",
            urgency="Immediate",
            headline=headline,
            description=desc,
            instruction="",
            area_desc="Sonnenseite der Erde",
            ags_codes=[],
            sent=issued_dt,
            effective=issued_dt,
            expires=None,
            content_hash=content_hash,
        )

    def _make_proton_alert(self, message: str, level: int, issued_dt: datetime) -> NormalizedWarning:
        serial = _extract_field(message, "Serial Number")
        identifier = f"sw-proton-s{level}-{serial}-{issued_dt.strftime('%Y%m%d')}"
        s_text = _S_TEXT.get(level, "Strong")

        headline = f"Weltraumwetter: Teilchensturm S{level} ({s_text})"
        desc = f"S{level} Proton-Ereignis. GPS-Satellitenelektronik gefaehrdet."

        from cap_normalizer import make_hash_with_date
        content_hash = make_hash_with_date(headline, issued_dt.isoformat())

        return NormalizedWarning(
            source="space_weather",
            identifier=identifier,
            status="actual",
            msg_type="Alert",
            severity="Severe" if level == 3 else "Extreme",
            urgency="Immediate",
            headline=headline,
            description=desc,
            instruction="",
            area_desc="Global / Polarbereiche",
            ags_codes=[],
            sent=issued_dt,
            effective=issued_dt,
            expires=None,
            content_hash=content_hash,
        )
