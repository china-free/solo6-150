"""Lightweight SQLite-backed time-series storage with automatic compaction and retention.

Acts as a consumer on the event bus: subscribes to metric events,
batches them efficiently, and persists to SQLite.

Also manages its own background maintenance thread for compaction and purging.
"""

import sqlite3
import threading
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Dict, List, Optional, Tuple

from .storage_base import (
    DEFAULT_DB_PATH,
    DEFAULT_RETENTION_DAYS,
    MetricPoint,
    RetentionConfig,
)


AUTO_CHECKPOINT_INTERVAL = 1000  # Checkpoint after this many writes
MAINTENANCE_INITIAL_DELAY = 300  # Wait 5 min before first maintenance
SUBSCRIBER_BATCH_SIZE = 200
SUBSCRIBER_FLUSH_INTERVAL = 2.0  # seconds


@dataclass
class StorageStats:
    """Runtime statistics for the storage layer."""
    raw_rows: int
    compacted_rows: int
    db_size_bytes: int
    wal_size_bytes: int
    last_compaction_ts: int
    maintenance_runs: int


class MetricsStore:
    """SQLite storage for time-series metrics with automatic compaction.

    Schema:
    - metrics: raw 1-second granularity data (purgeable)
    - metrics_compacted: 1-minute granularity (min/max/avg) for older data
    - runs: sampling run metadata
    - metadata: key-value store for compaction tracking

    As a consumer:
    - Subscribe to an event bus via subscribe_to_bus()
    - Batches incoming points with configurable batch size and flush interval
    - Uses WAL mode + MMAP for minimal locking and high write throughput

    As an operator:
    - Queries transparently use both raw and compacted tables
    - Background maintenance thread handles compaction and retention
    """

    def __init__(
        self,
        db_path: Optional[Path] = None,
        retention: Optional[RetentionConfig] = None,
    ):
        self.db_path = Path(db_path) if db_path else DEFAULT_DB_PATH
        self.db_path.parent.mkdir(parents=True, exist_ok=True)
        self._retention = retention or RetentionConfig()
        self._lock = threading.Lock()
        self._write_counter = 0
        self._last_compaction = 0.0

        self._conn = self._create_connection()
        self._init_schema()

        self._maintenance_thread: Optional[threading.Thread] = None
        self._maintenance_stop = threading.Event()
        self._maintenance_runs = 0
        self._subscriber_handles: List = []

    def _create_connection(self) -> sqlite3.Connection:
        """Create a well-configured SQLite connection to minimize locking."""
        conn = sqlite3.connect(
            str(self.db_path),
            timeout=60.0,
            isolation_level=None,  # Use explicit transactions for better control
            check_same_thread=False,  # Allow cross-thread access (protected by our own lock)
        )
        conn.execute("PRAGMA journal_mode=WAL")
        conn.execute("PRAGMA synchronous=NORMAL")
        conn.execute("PRAGMA cache_size=-16384")  # 16MB cache
        conn.execute("PRAGMA temp_store=MEMORY")
        conn.execute("PRAGMA mmap_size=33554432")  # 32MB memory map
        conn.execute("PRAGMA wal_autocheckpoint=1000")
        return conn

    def _init_schema(self) -> None:
        with self._lock:
            cursor = self._conn.cursor()
            cursor.execute("BEGIN IMMEDIATE")
            try:
                cursor.execute(
                    """
                    CREATE TABLE IF NOT EXISTS metrics (
                        timestamp INTEGER NOT NULL,
                        metric TEXT NOT NULL,
                        value REAL NOT NULL,
                        tag TEXT DEFAULT ''
                    )
                    """
                )
                cursor.execute(
                    "CREATE INDEX IF NOT EXISTS idx_metrics_ts_metric_tag "
                    "ON metrics(timestamp, metric, tag)"
                )

                cursor.execute(
                    """
                    CREATE TABLE IF NOT EXISTS metrics_compacted (
                        minute_bucket INTEGER NOT NULL,
                        metric TEXT NOT NULL,
                        tag TEXT DEFAULT '',
                        min_val REAL NOT NULL,
                        max_val REAL NOT NULL,
                        avg_val REAL NOT NULL,
                        count INTEGER NOT NULL,
                        PRIMARY KEY (minute_bucket, metric, tag)
                    )
                    """
                )

                cursor.execute(
                    """
                    CREATE TABLE IF NOT EXISTS runs (
                        id INTEGER PRIMARY KEY AUTOINCREMENT,
                        start_ts INTEGER NOT NULL,
                        end_ts INTEGER,
                        pid INTEGER,
                        status TEXT DEFAULT 'running'
                    )
                    """
                )

                cursor.execute(
                    """
                    CREATE TABLE IF NOT EXISTS metadata (
                        key TEXT PRIMARY KEY,
                        value TEXT
                    )
                    """
                )

                cursor.execute(
                    "INSERT OR IGNORE INTO metadata (key, value) VALUES (?, ?)",
                    ("last_compaction_ts", "0"),
                )
                cursor.execute("COMMIT")
            except Exception:
                cursor.execute("ROLLBACK")
                raise

    # ------------------------------------------------------------------
    # Event bus subscription (consumer side)
    # ------------------------------------------------------------------

    def subscribe_to_bus(
        self,
        bus,
        batch_size: int = SUBSCRIBER_BATCH_SIZE,
        flush_interval: float = SUBSCRIBER_FLUSH_INTERVAL,
    ) -> None:
        """Subscribe to an event bus to receive metric points.

        The store will batch incoming points and flush them periodically.
        This is the primary write path when used in a production setup.

        Args:
            bus: MetricEventBus instance
            batch_size: Max points per batch insert
            flush_interval: Max seconds between flushes
        """
        handle = bus.subscribe(
            name="storage",
            handler=self._handle_batch,
            batch_size=batch_size,
            flush_interval=flush_interval,
        )
        self._subscriber_handles.append(handle)

    def unsubscribe_from_bus(self) -> None:
        """Unsubscribe from all event buses."""
        for handle in self._subscriber_handles:
            try:
                handle.stop()
            except Exception:
                pass
        self._subscriber_handles.clear()

    def _handle_batch(self, points: List[MetricPoint]) -> None:
        """Handler called by the event bus subscriber thread."""
        if not points:
            return
        self.insert_batch(points)

    # ------------------------------------------------------------------
    # Direct write API (for one-off / testing use)
    # ------------------------------------------------------------------

    def insert_batch(self, points: List[MetricPoint]) -> None:
        """Insert a batch of metric points efficiently with batching."""
        if not points:
            return
        with self._lock:
            cursor = self._conn.cursor()
            cursor.execute("BEGIN IMMEDIATE")
            try:
                cursor.executemany(
                    "INSERT INTO metrics (timestamp, metric, value, tag) VALUES (?, ?, ?, ?)",
                    [(p.timestamp, p.metric, p.value, p.tag) for p in points],
                )
                cursor.execute("COMMIT")
                self._write_counter += 1

                if self._write_counter >= AUTO_CHECKPOINT_INTERVAL:
                    cursor.execute("PRAGMA wal_checkpoint(PASSIVE)")
                    self._write_counter = 0
            except Exception:
                cursor.execute("ROLLBACK")
                raise

    # ------------------------------------------------------------------
    # Query API
    # ------------------------------------------------------------------

    def query_range(
        self,
        metric: str,
        start_ts: int,
        end_ts: Optional[int] = None,
        tag: Optional[str] = None,
    ) -> List[Tuple[int, float]]:
        """Query metric values within a time range.

        Automatically uses compacted data for queries in the older-than-compaction range.
        Returns the max_val for each minute bucket from compacted data to preserve spikes.
        """
        end_ts = end_ts or int(time.time())
        compaction_cutoff = int(time.time()) - self._retention.compaction_age_seconds
        results: List[Tuple[int, float]] = []

        with self._lock:
            cursor = self._conn.cursor()

            if end_ts < compaction_cutoff:
                self._query_compacted(cursor, metric, start_ts, end_ts, tag, results)
            elif start_ts >= compaction_cutoff:
                self._query_raw(cursor, metric, start_ts, end_ts, tag, results)
            else:
                self._query_compacted(cursor, metric, start_ts, compaction_cutoff, tag, results)
                self._query_raw(cursor, metric, compaction_cutoff + 1, end_ts, tag, results)

        results.sort(key=lambda x: x[0])
        return results

    def _query_raw(
        self,
        cursor: sqlite3.Cursor,
        metric: str,
        start_ts: int,
        end_ts: int,
        tag: Optional[str],
        results: List[Tuple[int, float]],
    ) -> None:
        if tag:
            cursor.execute(
                "SELECT timestamp, value FROM metrics "
                "WHERE metric = ? AND tag = ? AND timestamp >= ? AND timestamp <= ? "
                "ORDER BY timestamp ASC",
                (metric, tag, start_ts, end_ts),
            )
        else:
            cursor.execute(
                "SELECT timestamp, value FROM metrics "
                "WHERE metric = ? AND timestamp >= ? AND timestamp <= ? "
                "ORDER BY timestamp ASC",
                (metric, start_ts, end_ts),
            )
        results.extend((int(ts), float(val)) for ts, val in cursor.fetchall())

    def _query_compacted(
        self,
        cursor: sqlite3.Cursor,
        metric: str,
        start_ts: int,
        end_ts: int,
        tag: Optional[str],
        results: List[Tuple[int, float]],
    ) -> None:
        start_bucket = (start_ts // 60) * 60
        end_bucket = (end_ts // 60) * 60
        if tag:
            cursor.execute(
                "SELECT minute_bucket, max_val FROM metrics_compacted "
                "WHERE metric = ? AND tag = ? AND minute_bucket >= ? AND minute_bucket <= ? "
                "ORDER BY minute_bucket ASC",
                (metric, tag, start_bucket, end_bucket),
            )
        else:
            cursor.execute(
                "SELECT minute_bucket, max_val FROM metrics_compacted "
                "WHERE metric = ? AND minute_bucket >= ? AND minute_bucket <= ? "
                "AND tag = '' "
                "ORDER BY minute_bucket ASC",
                (metric, start_bucket, end_bucket),
            )
        results.extend((int(ts), float(val)) for ts, val in cursor.fetchall())

    def list_metrics(self) -> List[str]:
        with self._lock:
            cursor = self._conn.cursor()
            cursor.execute(
                "SELECT DISTINCT metric FROM ("
                "  SELECT metric FROM metrics UNION SELECT metric FROM metrics_compacted"
                ") ORDER BY metric"
            )
            return [row[0] for row in cursor.fetchall()]

    def list_tags(self, metric: str) -> List[str]:
        with self._lock:
            cursor = self._conn.cursor()
            cursor.execute(
                "SELECT DISTINCT tag FROM ("
                "  SELECT tag FROM metrics WHERE metric = ? AND tag != '' "
                "  UNION "
                "  SELECT tag FROM metrics_compacted WHERE metric = ? AND tag != ''"
                ") ORDER BY tag",
                (metric, metric),
            )
            return [row[0] for row in cursor.fetchall()]

    # ------------------------------------------------------------------
    # Run tracking
    # ------------------------------------------------------------------

    def start_run(self, pid: int) -> int:
        with self._lock:
            cursor = self._conn.cursor()
            cursor.execute("BEGIN IMMEDIATE")
            try:
                cursor.execute(
                    "INSERT INTO runs (start_ts, pid, status) VALUES (?, ?, 'running')",
                    (int(time.time()), pid),
                )
                cursor.execute("COMMIT")
                return cursor.lastrowid
            except Exception:
                cursor.execute("ROLLBACK")
                raise

    def end_run(self, run_id: int) -> None:
        with self._lock:
            cursor = self._conn.cursor()
            cursor.execute("BEGIN IMMEDIATE")
            try:
                cursor.execute(
                    "UPDATE runs SET end_ts = ?, status = 'stopped' WHERE id = ?",
                    (int(time.time()), run_id),
                )
                cursor.execute("COMMIT")
            except Exception:
                cursor.execute("ROLLBACK")
                raise

    def get_active_run(self) -> Optional[Dict]:
        with self._lock:
            cursor = self._conn.cursor()
            cursor.execute(
                "SELECT id, start_ts, pid, status FROM runs "
                "WHERE status = 'running' ORDER BY id DESC LIMIT 1"
            )
            row = cursor.fetchone()
            if row:
                return {"id": row[0], "start_ts": row[1], "pid": row[2], "status": row[3]}
            return None

    # ------------------------------------------------------------------
    # Compaction and retention
    # ------------------------------------------------------------------

    def purge_older_than(self, days: Optional[int] = None) -> int:
        """Remove raw data older than N days and compacted data older than 2x N days.

        Also triggers a checkpoint and returns the number of rows deleted.
        """
        days = days if days is not None else self._retention.retention_days
        cutoff_raw = int(time.time()) - days * 86400
        cutoff_compacted = int(time.time()) - days * 2 * 86400
        total_deleted = 0

        with self._lock:
            cursor = self._conn.cursor()
            cursor.execute("BEGIN IMMEDIATE")
            try:
                cursor.execute("DELETE FROM metrics WHERE timestamp < ?", (cutoff_raw,))
                total_deleted += cursor.rowcount
                cursor.execute(
                    "DELETE FROM metrics_compacted WHERE minute_bucket < ?",
                    (cutoff_compacted,),
                )
                total_deleted += cursor.rowcount
                cursor.execute(
                    "DELETE FROM runs WHERE end_ts IS NOT NULL AND end_ts < ?",
                    (cutoff_raw,),
                )
                cursor.execute("COMMIT")
                cursor.execute("PRAGMA wal_checkpoint(TRUNCATE)")
            except Exception:
                cursor.execute("ROLLBACK")
                raise

        return total_deleted

    def compact(self, force: bool = False) -> Dict:
        """Compact raw data older than compaction_age into minute-level aggregates.

        This reduces storage by ~60x (60 raw points -> 1 compacted point) while
        preserving min/max/avg for each minute. Runs only once per compaction_interval
        unless force=True. Returns statistics about what was compacted.
        """
        now = time.time()
        if not force and not self._retention.enable_auto_compaction:
            return {"status": "skipped", "reason": "auto_compaction_disabled"}
        if not force and (now - self._last_compaction) < self._retention.compaction_interval_seconds:
            return {"status": "skipped", "reason": "too_recent"}

        self._last_compaction = now
        cutoff = int(now) - self._retention.compaction_age_seconds
        stats = {"status": "ok", "rows_compacted": 0, "rows_inserted": 0, "rows_deleted": 0}

        with self._lock:
            cursor = self._conn.cursor()
            cursor.execute("BEGIN IMMEDIATE")
            try:
                cursor.execute(
                    """
                    INSERT OR REPLACE INTO metrics_compacted
                        (minute_bucket, metric, tag, min_val, max_val, avg_val, count)
                    SELECT
                        (timestamp / 60) * 60 AS minute_bucket,
                        metric,
                        tag,
                        MIN(value) AS min_val,
                        MAX(value) AS max_val,
                        AVG(value) AS avg_val,
                        COUNT(*) AS count
                    FROM metrics
                    WHERE timestamp < ?
                    GROUP BY minute_bucket, metric, tag
                    """,
                    (cutoff,),
                )
                stats["rows_inserted"] = cursor.rowcount

                cursor.execute("SELECT COUNT(*) FROM metrics WHERE timestamp < ?", (cutoff,))
                stats["rows_compacted"] = int(cursor.fetchone()[0])

                cursor.execute("DELETE FROM metrics WHERE timestamp < ?", (cutoff,))
                stats["rows_deleted"] = cursor.rowcount

                cursor.execute(
                    "UPDATE metadata SET value = ? WHERE key = 'last_compaction_ts'",
                    (str(int(now)),),
                )

                cursor.execute("COMMIT")
                cursor.execute("PRAGMA wal_checkpoint(PASSIVE)")
            except Exception:
                cursor.execute("ROLLBACK")
                raise

        self._maintenance_runs += 1
        return stats

    # ------------------------------------------------------------------
    # Background maintenance
    # ------------------------------------------------------------------

    def start_maintenance(self) -> None:
        """Start the background maintenance thread (compaction + purge).

        Called automatically when subscribing to a bus, but can also be
        started manually for standalone use.
        """
        if self._maintenance_thread is not None and self._maintenance_thread.is_alive():
            return
        if not self._retention.enable_auto_compaction:
            return

        self._maintenance_stop.clear()
        self._maintenance_thread = threading.Thread(
            target=self._maintenance_loop,
            name="perf_storage_maintenance",
            daemon=True,
        )
        self._maintenance_thread.start()

    def stop_maintenance(self) -> None:
        """Stop the background maintenance thread."""
        self._maintenance_stop.set()
        if self._maintenance_thread is not None and self._maintenance_thread.is_alive():
            self._maintenance_thread.join(timeout=10.0)
        self._maintenance_thread = None

    def _maintenance_loop(self) -> None:
        """Background thread: periodically compact and purge old data.

        Runs at low priority to avoid interfering with writes.
        """
        next_run = time.monotonic() + MAINTENANCE_INITIAL_DELAY

        while not self._maintenance_stop.is_set():
            try:
                if time.monotonic() >= next_run:
                    try:
                        self.compact()
                    except Exception:
                        pass

                    try:
                        self.purge_older_than()
                    except Exception:
                        pass

                    next_run = time.monotonic() + self._retention.compaction_interval_seconds

                sleep_until = next_run
                while time.monotonic() < sleep_until:
                    if self._maintenance_stop.is_set():
                        return
                    sleep_for = min(1.0, sleep_until - time.monotonic())
                    time.sleep(sleep_for)
            except Exception:
                time.sleep(60)
                next_run = time.monotonic() + self._retention.compaction_interval_seconds

    # ------------------------------------------------------------------
    # Stats and diagnostics
    # ------------------------------------------------------------------

    def get_db_size(self) -> Dict:
        """Get database and WAL file sizes in bytes."""
        sizes = {"db": 0, "wal": 0, "shm": 0, "total": 0}
        for suffix, key in [("", "db"), ("-wal", "wal"), ("-shm", "shm")]:
            f = Path(str(self.db_path) + suffix)
            if f.exists():
                sizes[key] = f.stat().st_size
        sizes["total"] = sizes["db"] + sizes["wal"] + sizes["shm"]
        return sizes

    def get_row_counts(self) -> Dict:
        """Get approximate row counts for all tables."""
        with self._lock:
            cursor = self._conn.cursor()
            cursor.execute("SELECT COUNT(*) FROM metrics")
            raw_rows = int(cursor.fetchone()[0])
            cursor.execute("SELECT COUNT(*) FROM metrics_compacted")
            compacted_rows = int(cursor.fetchone()[0])
        return {"raw_rows": raw_rows, "compacted_rows": compacted_rows}

    def get_stats(self) -> StorageStats:
        """Get comprehensive runtime statistics."""
        counts = self.get_row_counts()
        sizes = self.get_db_size()
        return StorageStats(
            raw_rows=counts["raw_rows"],
            compacted_rows=counts["compacted_rows"],
            db_size_bytes=sizes["db"],
            wal_size_bytes=sizes["wal"],
            last_compaction_ts=int(self._last_compaction),
            maintenance_runs=self._maintenance_runs,
        )

    def vacuum(self) -> None:
        """Reclaim unused database space. Can be slow on large DBs."""
        with self._lock:
            self._conn.execute("VACUUM")

    def close(self) -> None:
        self.stop_maintenance()
        self.unsubscribe_from_bus()
        with self._lock:
            try:
                self._conn.execute("PRAGMA wal_checkpoint(TRUNCATE)")
            except Exception:
                pass
            self._conn.close()

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()
