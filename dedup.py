"""
dedup.py – WarnBridge
Zweistufiger Dedup-Cache: CAP-ID + Content-Hash, SQLite-basiert.
Verhindert Doppelmeldungen wenn MoWaS-ID über NINA und DAB+ gleichzeitig eintrifft.
"""

import sqlite3
import hashlib
import logging
from datetime import datetime, timezone, timedelta
from pathlib import Path
from typing import Optional
from collections import deque

logger = logging.getLogger(__name__)

DB_PATH = Path("warnbridge.db")


def _now() -> datetime:
    return datetime.now(timezone.utc)


class DedupCache:
    def __init__(self, db_path: Path = DB_PATH, ttl_hours: int = 24,
                 use_content_hash: bool = True, max_per_hour: int = 5):
        self.db_path = db_path
        self.ttl_hours = ttl_hours
        self.use_content_hash = use_content_hash
        self.max_per_hour = max_per_hour
        self._hour_window: deque = deque()  # Timestamps der letzten gesendeten Meldungen
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
            CREATE TABLE IF NOT EXISTS seen_ids (
                identifier TEXT PRIMARY KEY,
                first_seen TEXT NOT NULL
            )
        """)
        conn.execute("""
            CREATE TABLE IF NOT EXISTS seen_hashes (
                content_hash TEXT PRIMARY KEY,
                first_seen TEXT NOT NULL
            )
        """)
        # Broadcast-Level-Dedup: verhindert wiederholte Mesh-Nachrichten für
        # dasselbe andauernde Ereignis (Kreis + Event-Typ), solange sich die
        # Warnstufe (dwd_level) nicht ändert. Verfällt automatisch zum
        # DWD-expires-Zeitpunkt der Warnung – danach gilt ein neuer Alert mit
        # gleichem Level wieder als neues Ereignis.
        # state_key = f"{district}|{event_type}"
        conn.execute("""
            CREATE TABLE IF NOT EXISTS broadcast_level_state (
                state_key TEXT PRIMARY KEY,
                last_level INTEGER NOT NULL,
                expires_at TEXT,
                updated_at TEXT NOT NULL
            )
        """)
        conn.commit()
        logger.debug("DedupCache initialisiert: %s", self.db_path)

    def is_duplicate(self, identifier: str, content_hash: str) -> bool:
        """
        Gibt True zurück wenn die Meldung bereits bekannt ist (nicht weiterleiten).
        Prüft: ID-Cache → Content-Hash-Cache → Stunden-Limit.
        """
        conn = self._get_conn()
        cutoff = (_now() - timedelta(hours=self.ttl_hours)).isoformat()

        # Stufe 1: ID-Check
        row = conn.execute(
            "SELECT first_seen FROM seen_ids WHERE identifier = ? AND first_seen > ?",
            (identifier, cutoff)
        ).fetchone()
        if row:
            logger.debug("Dedup: ID bekannt – %s", identifier)
            return True

        # Stufe 2: Content-Hash
        if self.use_content_hash:
            row = conn.execute(
                "SELECT first_seen FROM seen_hashes WHERE content_hash = ? AND first_seen > ?",
                (content_hash, cutoff)
            ).fetchone()
            if row:
                logger.debug("Dedup: Content-Hash bekannt – %s", content_hash)
                return True

        return False

    def is_rate_limited(self) -> bool:
        """
        Prüft ob das Stunden-Limit für Mesh-Sends erreicht ist.
        Wird in warnbridge.py vor dem Senden aufgerufen – NICHT beim DB-Speichern.
        """
        now = _now()
        hour_ago = now - timedelta(hours=1)
        while self._hour_window and self._hour_window[0] < hour_ago:
            self._hour_window.popleft()
        if len(self._hour_window) >= self.max_per_hour:
            logger.warning("Rate-Limit erreicht (%d/h)", self.max_per_hour)
            return True
        return False

    def mark_seen(self, identifier: str, content_hash: str):
        """Meldung als gesehen markieren. Muss nach erfolgreicher Verarbeitung aufgerufen werden."""
        conn = self._get_conn()
        now_str = _now().isoformat()
        conn.execute(
            "INSERT OR REPLACE INTO seen_ids (identifier, first_seen) VALUES (?, ?)",
            (identifier, now_str)
        )
        if self.use_content_hash:
            conn.execute(
                "INSERT OR REPLACE INTO seen_hashes (content_hash, first_seen) VALUES (?, ?)",
                (content_hash, now_str)
            )
        conn.commit()
        logger.debug("Dedup: Als gesehen markiert – %s", identifier)

    def mark_sent(self):
        """Zählt einen echten Mesh-Send für das Stunden-Rate-Limit.
        Nur aufrufen wenn mesh.send_warning() erfolgreich war."""
        self._hour_window.append(_now())

    def check_broadcast_level(self, state_key: str, level: int, is_cancel: bool,
                               expires_iso: Optional[str]) -> bool:
        """
        Level-basiertes Broadcast-Dedup für andauernde Ereignisse (z.B. DWD-Gewitter).
        Gibt True zurück wenn gesendet werden SOLL (kein Duplikat), False wenn
        unterdrückt werden soll.

        Regeln:
        - Cancel (Aufhebung) geht IMMER durch, State wird danach gelöscht.
        - Sonst: nur senden wenn kein gespeicherter State existiert, der State
          abgelaufen ist (expires_at in der Vergangenheit – d.h. seit dem
          letzten Update kam lange nichts mehr, das Ereignis gilt als vorbei),
          oder sich der Level gegenüber dem gespeicherten Wert geändert hat
          (rauf ODER runter).
        - WICHTIG: DWD setzt expires pro Zellen-Update nur 30-60 Min in die
          Zukunft, nicht fürs Gesamtereignis. Daher wird bei jedem Update mit
          UNVERÄNDERTEM Level der gespeicherte expires_at auf den neuen,
          späteren Wert verlängert (Nachricht bleibt unterdrückt, State lebt
          weiter) – so wird ein andauerndes Gewitter nicht fälschlich als
          neues Ereignis gewertet, nur weil DWD kurze Gültigkeitsfenster
          pro Update verwendet.
        """
        conn = self._get_conn()

        if is_cancel:
            # Aufhebung: immer senden, State danach löschen (Ereignis vorbei)
            conn.execute(
                "DELETE FROM broadcast_level_state WHERE state_key = ?",
                (state_key,)
            )
            conn.commit()
            logger.debug("Broadcast-Level: Cancel – immer senden (%s)", state_key)
            return True

        row = conn.execute(
            "SELECT last_level, expires_at FROM broadcast_level_state WHERE state_key = ?",
            (state_key,)
        ).fetchone()

        if row:
            stored_expires = row["expires_at"]
            expired = False
            if stored_expires:
                try:
                    expired = datetime.fromisoformat(stored_expires) < _now()
                except Exception:
                    expired = False

            if not expired and row["last_level"] == level:
                # Gleicher Level, State noch gültig: Nachricht unterdrücken,
                # aber Gültigkeit auf den neuen (späteren) expires-Wert
                # verlängern, damit das andauernde Ereignis nicht zwischen
                # zwei DWD-Updates "ausläuft".
                if expires_iso and (not stored_expires or expires_iso > stored_expires):
                    conn.execute(
                        "UPDATE broadcast_level_state SET expires_at = ?, updated_at = ? WHERE state_key = ?",
                        (expires_iso, _now().isoformat(), state_key)
                    )
                    conn.commit()
                logger.debug(
                    "Broadcast-Level: Duplikat – gleicher Level %d für %s (State verlängert)",
                    level, state_key
                )
                return False
            # Level geändert ODER alter State wirklich abgelaufen → senden + State neu setzen

        conn.execute(
            """INSERT OR REPLACE INTO broadcast_level_state
               (state_key, last_level, expires_at, updated_at)
               VALUES (?, ?, ?, ?)""",
            (state_key, level, expires_iso, _now().isoformat())
        )
        conn.commit()
        logger.debug("Broadcast-Level: senden, neuer State Level %d für %s", level, state_key)
        return True

    def cleanup_expired(self):
        """Abgelaufene Einträge aus der DB löschen. Täglich aufrufen."""
        conn = self._get_conn()
        cutoff = (_now() - timedelta(hours=self.ttl_hours)).isoformat()
        c1 = conn.execute("DELETE FROM seen_ids WHERE first_seen <= ?", (cutoff,)).rowcount
        c2 = conn.execute("DELETE FROM seen_hashes WHERE first_seen <= ?", (cutoff,)).rowcount
        # Level-States löschen deren expires_at in der Vergangenheit liegt
        # (oder die kein expires_at haben aber > ttl_hours alt sind)
        now_iso = _now().isoformat()
        c3 = conn.execute(
            "DELETE FROM broadcast_level_state WHERE expires_at IS NOT NULL AND expires_at <= ?",
            (now_iso,)
        ).rowcount
        c4 = conn.execute(
            "DELETE FROM broadcast_level_state WHERE expires_at IS NULL AND updated_at <= ?",
            (cutoff,)
        ).rowcount
        conn.commit()
        if c1 or c2 or c3 or c4:
            logger.info(
                "Dedup cleanup: %d IDs, %d Hashes, %d+%d Level-States gelöscht",
                c1, c2, c3, c4
            )

    def stats(self) -> dict:
        conn = self._get_conn()
        n_ids = conn.execute("SELECT COUNT(*) FROM seen_ids").fetchone()[0]
        n_hashes = conn.execute("SELECT COUNT(*) FROM seen_hashes").fetchone()[0]
        # Abgelaufene Einträge aus dem Fenster entfernen bevor gezählt wird –
        # sonst zeigt der Counter veraltete Werte wenn is_rate_limited() nicht
        # aufgerufen wurde (z.B. weil neue Warnungen ausgeblieben sind).
        now = _now()
        hour_ago = now - timedelta(hours=1)
        while self._hour_window and self._hour_window[0] < hour_ago:
            self._hour_window.popleft()
        hour_count = len(self._hour_window)
        return {
            "cached_ids": n_ids,
            "cached_hashes": n_hashes,
            "sent_last_hour": hour_count,
            "max_per_hour": self.max_per_hour,
        }
