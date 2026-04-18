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

        # Stufe 3: Stunden-Limit
        now = _now()
        hour_ago = now - timedelta(hours=1)
        # Alte Einträge rauswerfen
        while self._hour_window and self._hour_window[0] < hour_ago:
            self._hour_window.popleft()
        if len(self._hour_window) >= self.max_per_hour:
            logger.warning("Dedup: Stunden-Limit erreicht (%d/h) – %s", self.max_per_hour, identifier)
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
        self._hour_window.append(_now())
        logger.debug("Dedup: Als gesehen markiert – %s", identifier)

    def cleanup_expired(self):
        """Abgelaufene Einträge aus der DB löschen. Täglich aufrufen."""
        conn = self._get_conn()
        cutoff = (_now() - timedelta(hours=self.ttl_hours)).isoformat()
        c1 = conn.execute("DELETE FROM seen_ids WHERE first_seen <= ?", (cutoff,)).rowcount
        c2 = conn.execute("DELETE FROM seen_hashes WHERE first_seen <= ?", (cutoff,)).rowcount
        conn.commit()
        if c1 or c2:
            logger.info("Dedup cleanup: %d IDs, %d Hashes gelöscht", c1, c2)

    def stats(self) -> dict:
        conn = self._get_conn()
        n_ids = conn.execute("SELECT COUNT(*) FROM seen_ids").fetchone()[0]
        n_hashes = conn.execute("SELECT COUNT(*) FROM seen_hashes").fetchone()[0]
        hour_count = len(self._hour_window)
        return {
            "cached_ids": n_ids,
            "cached_hashes": n_hashes,
            "sent_last_hour": hour_count,
            "max_per_hour": self.max_per_hour,
        }
