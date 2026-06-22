"""Lightweight SQLite-backed time-series storage with automatic compaction and retention."""

import sqlite3
import threading
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Dict, List, Optional, Tuple


DEFAULT_DB_PATH = Path.home() / ".perf_cli" / "metrics.db"

DEFAULT_RETENTION_DAYS = 7
COMPACTION_AGE_SECONDS = 86400  # 1 day - data older than this gets compacted
COMPACTION_INTERVAL_SECONDS = 3600  # Run compaction once per hour
AUTO_CHECKPOINT_INTERVAL = 1000  # Checkpoint after this many writes


@dataclass
class MetricPoint:
    """A single metric sample at a point in time."""
    timestamp: int
    metric: str
    value: float
    tag: str = ""


@dataclass
class RetentionConfig:
    """Configuration for data retention and compaction."""
    retention_days: int = DEFAULT_RETENTION_DAYS
    compaction_age_seconds: int = COMPACTION_AGE_SECONDS
    compaction_interval_seconds: int = COMPACTION_INTERVAL_SECONDS
    enable_auto_compaction: bool = True


class MetricsStore:
    """SQLite storage for time-series metrics with automatic compaction.

    Schema:
    - metrics: raw 1-second granularity data (purgeable)
    - metrics_compacted: 1-minute granularity (min/max/avg) for older data
    - runs: sampling run metadata
    - metadata: key-value store for compaction tracking
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

        return stats

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

    def vacuum(self) -> None:
        """Reclaim unused database space. Can be slow on large DBs."""
        with self._lock:
            self._conn.execute("VACUUM")

    def close(self) -> None:
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
