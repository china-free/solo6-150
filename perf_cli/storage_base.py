"""Shared data types and constants for the storage layer.

Separated to avoid circular imports between storage <-> sampler <-> event_bus.
"""

from dataclasses import dataclass
from pathlib import Path
from typing import Optional


DEFAULT_DB_PATH = Path.home() / ".perf_cli" / "metrics.db"

DEFAULT_RETENTION_DAYS = 7
COMPACTION_AGE_SECONDS = 86400  # 1 day - data older than this gets compacted
COMPACTION_INTERVAL_SECONDS = 3600  # Run compaction once per hour


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
