import sqlite3
import threading
import logging
import json
import os
from pathlib import Path
from typing import Optional
from src.models import Event

logger = logging.getLogger(__name__)
DEFAULT_DB_PATH = os.environ.get("DEDUP_DB_PATH", "app/data/dedup.db")

class DedupStore:
    def __init__(self, db_path: str = DEFAULT_DB_PATH):
        self.db_path = db_path
        self._lock = threading.Lock()
        self._in_memory = (db_path == ":memory:")

        if self._in_memory:
            self._conn = sqlite3.connect(":memory:", check_same_thread=False)
            self._conn.row_factory = sqlite3.Row
        else:
            self._conn = None
        
        self._init_db()
        logger.info(f"DedupStore initialized with DB path: {db_path}")

    def _get_conn(self) -> sqlite3.Connection:
        if self._in_memory:
            return self._conn
        conn = sqlite3.connect(self.db_path, timeout=30)
        conn.row_factory = sqlite3.Row
        return conn
    
    def _init_db(self) -> None:
        conn = self._get_conn()
        try:
            if not self._in_memory:
                conn.execute("PRAGMA journal_mode=WAL")
                conn.execute("PRAGMA synchronous=NORMAL")
            conn.execute("""
                CREATE TABLE IF NOT EXISTS processed_events(
                    topic TEXT NOT NULL,
                    event_id TEXT NOT NULL,
                    source TEXT,
                    timestamp TEXT,
                    payload TEXT,
                    received_at TEXT DEFAULT (datetime('now')),
                    PRIMARY KEY (topic, event_id)
                )
            """)
            # Stats tracking table to persist cumulative counts across restarts
            conn.execute("""
                CREATE TABLE IF NOT EXISTS stats_checkpoint(
                    id INTEGER PRIMARY KEY CHECK (id = 1),
                    total_received INTEGER DEFAULT 0,
                    total_duplicate_dropped INTEGER DEFAULT 0,
                    last_updated TEXT DEFAULT (datetime('now'))
                )
            """)
            # Ensure one row exists
            conn.execute("INSERT OR IGNORE INTO stats_checkpoint(id) VALUES (1)")
            conn.execute("""
                CREATE INDEX IF NOT EXISTS idx_topic ON processed_events(topic)
            """)
            conn.commit()
        finally:
            if not self._in_memory:
                conn.close()

    def is_duplicate(self, topic: str, event_id: str) -> bool:
            with self._lock:
                conn = self._get_conn()
                try:
                    row = conn.execute(
                        "SELECT 1 FROM processed_events WHERE topic = ? AND event_id = ?",
                        (topic, event_id)
                    ).fetchone()
                    return row is not None
                finally:
                    if not self._in_memory:
                        conn.close()

    def save(self, event: Event) -> bool:
        with self._lock:
            conn = self._get_conn()
            try:
                cursor = conn.execute(
                    """
                    INSERT OR IGNORE INTO processed_events 
                        (topic, event_id, source, timestamp, payload) 
                    VALUES (?, ?, ?, ?, ?)
                    """,
                    (
                        event.topic,
                        event.event_id,
                        event.source,
                        event.timestamp,
                        json.dumps(event.payload),
                    )
                )
                conn.commit()
                inserted = cursor.rowcount > 0
                if not inserted:
                    logger.warning(
                        f"[DEDUP] Duplicate detected - topic={event.topic!r}"
                        f"event_id={event.event_id!r} source={event.source!r}"
                    )
                return inserted
            finally:
                if not self._in_memory:
                    conn.close()
    
    def get_events_by_topic(self, topic: str) -> list[Event]:
        with self._lock:
            conn = self._get_conn()
            try:
                rows = conn.execute(
                    "SELECT * FROM processed_events WHERE topic = ? ORDER BY received_at ASC",
                    (topic,)
                ).fetchall()
            finally:
                if not self._in_memory:
                    conn.close()

        events = []
        for row in rows:
            events.append(Event(
                topic=row["topic"],
                event_id=row["event_id"],
                source=row["source"],
                timestamp=row["timestamp"],
                payload=json.loads(row["payload"] or "{}"),
            ))
        return events
    def get_all_topics(self) -> list[str]:
        with self._lock:
            conn = self._get_conn()
            try:
                rows = conn.execute(
                    "SELECT DISTINCT topic FROM processed_events ORDER BY topic"
                ).fetchall()
            finally:
                if not self._in_memory:
                    conn.close()
        return [r["topic"] for r in rows]
    
    def count_unique(self) -> int:
        with self._lock:
            conn = self._get_conn()
            try:
                return conn.execute(
                    "SELECT COUNT(*) FROM processed_events"
                ).fetchone()[0]
            finally:
                if not self._in_memory:
                    conn.close()

    def get_total_event_count(self) -> int:
        """Total events received (unique_processed + duplicate_dropped) from previous runs"""
        with self._lock:
            conn = self._get_conn()
            try:
                unique = conn.execute("SELECT COUNT(*) FROM processed_events").fetchone()[0]
                duplicates = conn.execute(
                    "SELECT total_duplicate_dropped FROM stats_checkpoint WHERE id = 1"
                ).fetchone()[0]
                return unique + duplicates
            finally:
                if not self._in_memory:
                    conn.close()

    def get_unique_event_count(self) -> int:
        """Total unique events successfully processed"""
        with self._lock:
            conn = self._get_conn()
            try:
                return conn.execute(
                    "SELECT COUNT(*) FROM processed_events"
                ).fetchone()[0]
            finally:
                if not self._in_memory:
                    conn.close()

    def get_duplicate_count(self) -> int:
        """Total duplicates dropped from previous runs"""
        with self._lock:
            conn = self._get_conn()
            try:
                result = conn.execute(
                    "SELECT total_duplicate_dropped FROM stats_checkpoint WHERE id = 1"
                ).fetchone()
                return result[0] if result else 0
            finally:
                if not self._in_memory:
                    conn.close()

    def increment_duplicate_count(self) -> None:
        """Increment duplicate counter when duplicate event is detected"""
        with self._lock:
            conn = self._get_conn()
            try:
                conn.execute(
                    "UPDATE stats_checkpoint SET total_duplicate_dropped = total_duplicate_dropped + 1 WHERE id = 1"
                )
                conn.commit()
            finally:
                if not self._in_memory:
                    conn.close()
    
    def close(self) -> None:
        if self._in_memory and self._conn:
            self._conn.close()
        
        logger.info("DedupStore closed")