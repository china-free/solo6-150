"""Time duration parsing utilities."""

import re
from typing import Optional


_DURATION_RE = re.compile(
    r"^\s*(?:(\d+)\s*d)?\s*(?:(\d+)\s*h)?\s*(?:(\d+)\s*m)?\s*(?:(\d+)\s*s)?\s*$",
    re.IGNORECASE,
)


def parse_duration(text: str) -> Optional[int]:
    """Parse a duration string like '1h30m', '10m', '90s' into seconds.

    Supports: d (days), h (hours), m (minutes), s (seconds).
    Returns None if parsing fails.
    """
    if text.isdigit():
        return int(text)
    m = _DURATION_RE.match(text)
    if not m:
        return None
    days = int(m.group(1) or 0)
    hours = int(m.group(2) or 0)
    minutes = int(m.group(3) or 0)
    seconds = int(m.group(4) or 0)
    total = days * 86400 + hours * 3600 + minutes * 60 + seconds
    return total if total > 0 else None


def format_duration(seconds: int) -> str:
    """Format a duration in seconds into a human-readable string."""
    parts = []
    days, seconds = divmod(seconds, 86400)
    hours, seconds = divmod(seconds, 3600)
    minutes, seconds = divmod(seconds, 60)
    if days:
        parts.append(f"{days}d")
    if hours:
        parts.append(f"{hours}h")
    if minutes:
        parts.append(f"{minutes}m")
    if seconds or not parts:
        parts.append(f"{seconds}s")
    return "".join(parts)
