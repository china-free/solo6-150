"""Lightweight SQLite-backed time-series storage for performance metrics."""

import os
import sqlite3
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Dict, List, Optional, Tuple


DEFAULT_DB_PATH = Path.home() / ".perf_cli" / "metrics.db"


@dataclass
class MetricPoint:
    """A single metric sample at a point in time."""
    timestamp: int
    metric: str
    value: float
    tag: str = ""


class MetricsStore:
    """SQLite storage for time-series metrics, optimized for writes and time-range queries."""

    def __init__(self, db_path: Optional[Path] = None):
        self.db_path = Path(db_path) if db_path else DEFAULT_DB_PATH
        self.db_path.parent.mkdir(parents=True, exist_ok=True)
        self._conn = sqlite3.connect(str(self.db_path), timeout=30.0)
        self._conn.execute("PRAGMA journal_mode=WAL")
        self._conn.execute("PRAGMA synchronous=NORMAL")
        self._conn.execute("PRAGMA cache_size=-8192")
        self._init_schema()

    def _init_schema(self) -> None:
        cursor = self._conn.cursor()
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
            "CREATE INDEX IF NOT EXISTS idx_metrics_ts_metric ON metrics(timestamp, metric)"
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
        self._conn.commit()

    def insert_batch(self, points: List[MetricPoint]) -> None:
        """Insert a batch of metric points efficiently."""
        if not points:
            return
        cursor = self._conn.cursor()
        cursor.executemany(
            "INSERT INTO metrics (timestamp, metric, value, tag) VALUES (?, ?, ?, ?)",
            [(p.timestamp, p.metric, p.value, p.tag) for p in points],
        )
        self._conn.commit()

    def query_range(
        self,
        metric: str,
        start_ts: int,
        end_ts: Optional[int] = None,
        tag: Optional[str] = None,
    ) -> List[Tuple[int, float]]:
        """Query metric values within a time range."""
        end_ts = end_ts or int(time.time())
        cursor = self._conn.cursor()
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
        return [(int(ts), float(val)) for ts, val in cursor.fetchall()]

    def list_metrics(self) -> List[str]:
        """List all available metric names."""
        cursor = self._conn.cursor()
        cursor.execute("SELECT DISTINCT metric FROM metrics ORDER BY metric")
        return [row[0] for row in cursor.fetchall()]

    def list_tags(self, metric: str) -> List[str]:
        """List all tags for a given metric."""
        cursor = self._conn.cursor()
        cursor.execute(
            "SELECT DISTINCT tag FROM metrics WHERE metric = ? AND tag != '' ORDER BY tag",
            (metric,),
        )
        return [row[0] for row in cursor.fetchall()]

    def start_run(self, pid: int) -> int:
        """Record a new sampling run."""
        cursor = self._conn.cursor()
        cursor.execute(
            "INSERT INTO runs (start_ts, pid, status) VALUES (?, ?, 'running')",
            (int(time.time()), pid),
        )
        self._conn.commit()
        return cursor.lastrowid

    def end_run(self, run_id: int) -> None:
        """Mark a sampling run as ended."""
        cursor = self._conn.cursor()
        cursor.execute(
            "UPDATE runs SET end_ts = ?, status = 'stopped' WHERE id = ?",
            (int(time.time()), run_id),
        )
        self._conn.commit()

    def get_active_run(self) -> Optional[Dict]:
        """Get the currently active run, if any."""
        cursor = self._conn.cursor()
        cursor.execute(
            "SELECT id, start_ts, pid, status FROM runs WHERE status = 'running' ORDER BY id DESC LIMIT 1"
        )
        row = cursor.fetchone()
        if row:
            return {"id": row[0], "start_ts": row[1], "pid": row[2], "status": row[3]}
        return None

    def purge_older_than(self, days: int = 7) -> int:
        """Remove data older than N days. Returns number of rows deleted."""
        cutoff = int(time.time()) - days * 86400
        cursor = self._conn.cursor()
        cursor.execute("DELETE FROM metrics WHERE timestamp < ?", (cutoff,))
        self._conn.commit()
        return cursor.rowcount

    def vacuum(self) -> None:
        """Reclaim unused database space."""
        self._conn.execute("VACUUM")

    def close(self) -> None:
        self._conn.close()

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()
