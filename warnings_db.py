"""
warnings_db.py – WarnBridge
Persistenter Warnungs-Speicher (SQLite, 48h TTL).
Wird von Phase 2 (bot_handler.py) für /details und /warnings [Ort] abgefragt.
"""

import sqlite3
import json
import logging
from datetime import datetime, timezone, timedelta
from pathlib import Path
from typing import Optional

from cap_normalizer import NormalizedWarning

logger = logging.getLogger(__name__)

DB_PATH = Path("warnbridge.db")


def _now() -> datetime:
    return datetime.now(timezone.utc)


def _to_json(w: NormalizedWarning) -> str:
    d = {
        "source": w.source,
        "identifier": w.identifier,
        "status": w.status,
        "msg_type": w.msg_type,
        "severity": w.severity,
        "urgency": w.urgency,
        "headline": w.headline,
        "description": w.description,
        "instruction": w.instruction,
        "area_desc": w.area_desc,
        "ags_codes": w.ags_codes,
        "sent": w.sent.isoformat() if w.sent else None,
        "effective": w.effective.isoformat() if w.effective else None,
        "expires": w.expires.isoformat() if w.expires else None,
        "dwd_event_type": w.dwd_event_type,
        "dwd_level": w.dwd_level,
        "content_hash": w.content_hash,
        "received_at": w.received_at.isoformat() if w.received_at else None,
    }
    return json.dumps(d, ensure_ascii=False)


def _from_row(row: sqlite3.Row) -> dict:
    d = json.loads(row["payload"])
    d["db_id"] = row["id"]
    d["stored_at"] = row["stored_at"]
    d["broadcast_sent"] = bool(row["broadcast_sent"])
    return d


class WarningsDB:
    def __init__(self, db_path: Path = DB_PATH, ttl_hours: int = 48):
        self.db_path = db_path
        self.ttl_hours = ttl_hours
        self._conn: Optional[sqlite3.Connection] = None
        self._init_db()

    def _get_conn(self) -> sqlite3.Connection:
        if self._conn is None:
            self._conn = sqlite3.connect(self.db_path, check_same_thread=False)
            self._conn.row_factory = sqlite3.Row
        return self._conn

    def _init_db(self):
        conn = self._get_conn()
        conn.execute("""
            CREATE TABLE IF NOT EXISTS warnings (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                identifier TEXT NOT NULL,
                source TEXT NOT NULL,
                severity TEXT NOT NULL,
                area_desc TEXT NOT NULL,
                ags_codes TEXT NOT NULL,      -- JSON array als String
                headline TEXT NOT NULL,
                stored_at TEXT NOT NULL,
                expires_at TEXT,
                broadcast_sent INTEGER DEFAULT 0,  -- 1 wenn Mesh-Alert rausging
                payload TEXT NOT NULL              -- vollständiges JSON (NormalizedWarning)
            )
        """)
        conn.execute("CREATE INDEX IF NOT EXISTS idx_identifier ON warnings(identifier)")
        conn.execute("CREATE INDEX IF NOT EXISTS idx_stored_at ON warnings(stored_at)")
        conn.execute("CREATE INDEX IF NOT EXISTS idx_source ON warnings(source)")
        conn.commit()
        logger.debug("WarningsDB initialisiert: %s", self.db_path)

    def store(self, w: NormalizedWarning, broadcast_sent: bool = False) -> int:
        """Warnung speichern. Gibt die DB-ID zurück."""
        conn = self._get_conn()
        now_str = _now().isoformat()
        expires_str = w.expires.isoformat() if w.expires else None
        cur = conn.execute(
            """INSERT INTO warnings
               (identifier, source, severity, area_desc, ags_codes, headline,
                stored_at, expires_at, broadcast_sent, payload)
               VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
            (
                w.identifier,
                w.source,
                w.severity,
                w.area_desc,
                json.dumps(w.ags_codes),
                w.headline,
                now_str,
                expires_str,
                1 if broadcast_sent else 0,
                _to_json(w),
            )
        )
        conn.commit()
        return cur.lastrowid

    def mark_broadcast_sent(self, db_id: int):
        conn = self._get_conn()
        conn.execute("UPDATE warnings SET broadcast_sent = 1 WHERE id = ?", (db_id,))
        conn.commit()

    def get_active(self, district_filter: Optional[list[str]] = None) -> list[dict]:
        """
        Alle aktiven Warnungen (nicht abgelaufen, innerhalb TTL).
        Optionaler Filter auf AGS-Kreisschlüssel.
        """
        conn = self._get_conn()
        cutoff = (_now() - timedelta(hours=self.ttl_hours)).isoformat()
        rows = conn.execute(
            """SELECT * FROM warnings
               WHERE stored_at > ?
               ORDER BY stored_at DESC""",
            (cutoff,)
        ).fetchall()

        results = []
        for row in rows:
            # Prüfen ob abgelaufen (wenn expires gesetzt)
            if row["expires_at"]:
                try:
                    exp = datetime.fromisoformat(row["expires_at"])
                    if exp < _now():
                        continue
                except Exception:
                    pass

            d = _from_row(row)

            # Optionaler Kreisfilter
            if district_filter:
                ags = json.loads(row["ags_codes"])
                # Prüfen ob einer der Kreise im Eintrag vorkommt
                if not any(
                    any(a.startswith(f) for a in ags)
                    for f in district_filter
                ):
                    continue

            results.append(d)

        return results

    def get_by_place(self, place_name: str) -> list[dict]:
        """
        Warnungen für einen Ortsnamen suchen.
        Sucht in area_desc (Textsuche) und in den AGS-Codes.
        Für Phase 2 (/warnings Karlsruhe).
        """
        conn = self._get_conn()
        cutoff = (_now() - timedelta(hours=self.ttl_hours)).isoformat()
        place_lower = place_name.lower().strip()

        rows = conn.execute(
            """SELECT * FROM warnings
               WHERE stored_at > ?
               ORDER BY stored_at DESC""",
            (cutoff,)
        ).fetchall()

        results = []
        for row in rows:
            if row["expires_at"]:
                try:
                    exp = datetime.fromisoformat(row["expires_at"])
                    if exp < _now():
                        continue
                except Exception:
                    pass

            if place_lower in row["area_desc"].lower():
                results.append(_from_row(row))

        return results

    def get_latest(self, n: int = 1) -> list[dict]:
        """Die n neuesten Warnungen. Für /details in Phase 2."""
        conn = self._get_conn()
        cutoff = (_now() - timedelta(hours=self.ttl_hours)).isoformat()
        rows = conn.execute(
            "SELECT * FROM warnings WHERE stored_at > ? ORDER BY stored_at DESC LIMIT ?",
            (cutoff, n)
        ).fetchall()
        return [_from_row(r) for r in rows]

    def get_all_recent(self, hours: int = 48) -> list[dict]:
        """Alle Warnungen der letzten X Stunden – für Dashboard."""
        conn = self._get_conn()
        cutoff = (_now() - timedelta(hours=hours)).isoformat()
        rows = conn.execute(
            "SELECT * FROM warnings WHERE stored_at > ? ORDER BY stored_at DESC",
            (cutoff,)
        ).fetchall()
        return [_from_row(r) for r in rows]

    def cleanup_expired(self):
        """Abgelaufene Einträge löschen. Täglich aufrufen."""
        conn = self._get_conn()
        cutoff = (_now() - timedelta(hours=self.ttl_hours)).isoformat()
        c = conn.execute("DELETE FROM warnings WHERE stored_at <= ?", (cutoff,)).rowcount
        conn.commit()
        if c:
            logger.info("WarningsDB cleanup: %d Einträge gelöscht", c)

    def stats(self) -> dict:
        conn = self._get_conn()
        cutoff = (_now() - timedelta(hours=self.ttl_hours)).isoformat()
        total = conn.execute("SELECT COUNT(*) FROM warnings WHERE stored_at > ?", (cutoff,)).fetchone()[0]
        by_source = {}
        for row in conn.execute(
            "SELECT source, COUNT(*) as n FROM warnings WHERE stored_at > ? GROUP BY source",
            (cutoff,)
        ).fetchall():
            by_source[row["source"]] = row["n"]
        return {"active_warnings": total, "by_source": by_source}
