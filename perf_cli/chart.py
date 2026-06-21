"""Terminal-based ASCII/Unicode line chart renderer."""

import shutil
import sys
import time
from dataclasses import dataclass
from typing import Dict, List, Optional, Tuple


BLOCK_CHARS = " ▁▂▃▄▅▆▇█"
LINE_CHARS = {
    "h": "─",
    "v": "│",
    "tl": "┌",
    "tr": "┐",
    "bl": "└",
    "br": "┘",
    "tick": "┼",
    "ltick": "├",
    "rtick": "┤",
    "ttick": "┬",
    "btick": "┴",
    "point": "●",
    "peak": "▲",
}

SERIES_COLORS = [
    "\033[92m",
    "\033[94m",
    "\033[91m",
    "\033[93m",
    "\033[95m",
    "\033[96m",
    "\033[97m",
]
RESET_COLOR = "\033[0m"


def _supports_color() -> bool:
    if not sys.stdout.isatty():
        return False
    if sys.platform == "win32":
        return "ANSICON" in os.environ or os.environ.get("WT_SESSION") is not None
    return True


import os


@dataclass
class Series:
    """A single data series to plot."""
    name: str
    data: List[Tuple[int, float]]
    color: Optional[str] = None
    unit: str = ""


@dataclass
class ChartConfig:
    title: str = ""
    width: Optional[int] = None
    height: int = 20
    show_legend: bool = True
    show_grid: bool = True
    highlight_peaks: bool = True
    use_unicode: bool = True
    color: Optional[bool] = None


def _downsample(
    data: List[Tuple[int, float]], target_points: int
) -> List[Tuple[int, float]]:
    """Downsample data to approximately target_points using LTTB-ish min/max bucketing."""
    if len(data) <= target_points or target_points <= 0:
        return list(data)
    bucket_size = len(data) / target_points
    result: List[Tuple[int, float]] = []
    for i in range(target_points):
        start = int(i * bucket_size)
        end = int((i + 1) * bucket_size)
        if start >= len(data):
            break
        end = min(end, len(data))
        bucket = data[start:end]
        if not bucket:
            continue
        max_idx = 0
        min_idx = 0
        for j, (_, v) in enumerate(bucket):
            if v > bucket[max_idx][1]:
                max_idx = j
            if v < bucket[min_idx][1]:
                min_idx = j
        if min_idx < max_idx:
            result.append(bucket[min_idx])
            result.append(bucket[max_idx])
        else:
            result.append(bucket[max_idx])
            result.append(bucket[min_idx])
    return result


def _format_value(val: float, unit: str = "") -> str:
    if unit in ("mbps", "MB/s", "mb"):
        if val >= 1024:
            return f"{val / 1024:.2f} GB"
        return f"{val:.2f} MB"
    if unit == "%":
        return f"{val:.1f}%"
    return f"{val:.2f}"


def _format_time(ts: int) -> str:
    return time.strftime("%H:%M:%S", time.localtime(ts))


def _find_peaks(
    data: List[Tuple[int, float]], prominence: float = 0.1
) -> List[int]:
    """Find indices of prominent peaks in the data."""
    if len(data) < 3:
        return []
    peaks: List[int] = []
    vals = [v for _, v in data]
    vmin, vmax = min(vals), max(vals)
    vrange = vmax - vmin
    if vrange <= 0:
        return []
    for i in range(1, len(data) - 1):
        if vals[i] > vals[i - 1] and vals[i] >= vals[i + 1]:
            if (vals[i] - vmin) / vrange >= 1.0 - prominence:
                peaks.append(i)
    return peaks


def render_line_chart(series_list: List[Series], config: Optional[ChartConfig] = None) -> str:
    """Render multiple series as a line chart and return the string."""
    cfg = config or ChartConfig()
    use_color = cfg.color if cfg.color is not None else _supports_color()

    if not series_list:
        return "No data to display.\n"

    term_width = shutil.get_terminal_size((100, 40)).columns
    chart_width = cfg.width or min(term_width - 4, 120)
    chart_width = max(40, chart_width)
    height = max(5, cfg.height)
    plot_height = height - 2

    all_ts: List[int] = []
    all_vals: List[float] = []
    for s in series_list:
        for ts, v in s.data:
            all_ts.append(ts)
            all_vals.append(v)
    if not all_ts:
        return "No data to display.\n"

    ts_min, ts_max = min(all_ts), max(all_ts)
    val_min, val_max = min(all_vals), max(all_vals)
    val_range = val_max - val_min
    if val_range <= 0:
        val_range = max(abs(val_max), 1.0) * 0.1
        val_min -= val_range
        val_max += val_range
        val_range = val_max - val_min

    downsampled_series: List[Series] = []
    for s in series_list:
        ds = Series(
            name=s.name,
            data=_downsample(s.data, chart_width * 2),
            color=s.color,
            unit=s.unit,
        )
        downsampled_series.append(ds)

    def ts_to_x(ts: int) -> int:
        if ts_max == ts_min:
            return 0
        return int((ts - ts_min) / (ts_max - ts_min) * (chart_width - 1))

    def val_to_y(val: float) -> int:
        return int((val_max - val) / val_range * (plot_height - 1))

    grid: List[List[str]] = [
        [" "] * chart_width for _ in range(plot_height)
    ]

    if cfg.show_grid:
        for y in range(plot_height):
            frac = y / max(1, plot_height - 1)
            for x in range(chart_width):
                grid[y][x] = "·" if frac in (0.0, 0.5, 1.0) else " "
        for x in range(0, chart_width, max(1, chart_width // 10)):
            for y in range(plot_height):
                if grid[y][x] == "·":
                    grid[y][x] = "┼"
                elif grid[y][x] == " ":
                    grid[y][x] = "│"

    for si, s in enumerate(downsampled_series):
        color_code = ""
        if use_color:
            color_code = s.color or SERIES_COLORS[si % len(SERIES_COLORS)]
        peak_indices = _find_peaks(s.data) if cfg.highlight_peaks else []
        peak_set = set(peak_indices)

        prev_y: Optional[int] = None
        prev_x: Optional[int] = None

        for idx, (ts, val) in enumerate(s.data):
            x = ts_to_x(ts)
            y = val_to_y(val)
            y = max(0, min(plot_height - 1, y))

            char = "*"
            if idx in peak_set:
                char = LINE_CHARS["peak"] if cfg.use_unicode else "!"
            elif prev_y is not None and prev_x is not None:
                if prev_x == x:
                    if prev_y < y:
                        char = "│" if cfg.use_unicode else "|"
                    else:
                        char = "│" if cfg.use_unicode else "|"
                elif abs(y - prev_y) <= 1:
                    char = "─" if cfg.use_unicode else "-"
                else:
                    char = "·"
            else:
                char = "●" if cfg.use_unicode else "o"

            if color_code:
                grid[y][x] = f"{color_code}{char}{RESET_COLOR}"
            else:
                grid[y][x] = char

            if prev_y is not None and prev_x is not None:
                dx = x - prev_x
                dy = y - prev_y
                if abs(dx) > 1 or abs(dy) > 1:
                    steps = max(abs(dx), abs(dy))
                    for step in range(1, steps):
                        ix = prev_x + int(dx * step / steps)
                        iy = prev_y + int(dy * step / steps)
                        if 0 <= ix < chart_width and 0 <= iy < plot_height:
                            existing = grid[iy][ix]
                            if existing in (" ", "·", "│", "┼") or (
                                use_color and RESET_COLOR in existing and "●" not in existing
                            ):
                                line_char = "·" if cfg.use_unicode else "."
                                if color_code:
                                    grid[iy][ix] = f"{color_code}{line_char}{RESET_COLOR}"
                                else:
                                    grid[iy][ix] = line_char
            prev_x = x
            prev_y = y

    lines: List[str] = []

    if cfg.title:
        pad = (chart_width + 10 - len(cfg.title)) // 2
        lines.append(" " * max(0, pad) + cfg.title)

    y_label_width = 10
    for yi in range(plot_height):
        frac = yi / max(1, plot_height - 1)
        y_val = val_max - frac * val_range
        label = f"{_format_value(y_val, series_list[0].unit):>9}"
        row = "".join(grid[yi])
        vline = "│" if cfg.use_unicode else "|"
        lines.append(f"{label} {vline}{row}")

    x_axis = "─" * chart_width if cfg.use_unicode else "-" * chart_width
    lines.append(" " * y_label_width + ("└" if cfg.use_unicode else "+") + x_axis)

    num_ticks = min(6, chart_width // 15)
    tick_labels: List[str] = []
    tick_positions: List[int] = []
    for i in range(num_ticks):
        frac = i / max(1, num_ticks - 1)
        ts = int(ts_min + frac * (ts_max - ts_min))
        tick_labels.append(_format_time(ts))
        tick_positions.append(int(frac * (chart_width - 1)))

    x_label_line = [" "] * (chart_width + 1)
    for i, (pos, label) in enumerate(zip(tick_positions, tick_labels)):
        start = max(0, pos - len(label) // 2)
        for j, c in enumerate(label):
            if start + j < len(x_label_line):
                x_label_line[start + j] = c
    lines.append(" " * y_label_width + " " + "".join(x_label_line))

    if cfg.show_legend and len(series_list) > 0:
        lines.append("")
        legend_parts = []
        for si, s in enumerate(series_list):
            color_code = ""
            if use_color:
                color_code = s.color or SERIES_COLORS[si % len(SERIES_COLORS)]
            marker = f"{color_code}●{RESET_COLOR}" if use_color else "*"
            display_name = s.name
            if s.data:
                peak_val = max(v for _, v in s.data)
                peak_ts = max(s.data, key=lambda x: x[1])[0]
                display_name += f" (peak: {_format_value(peak_val, s.unit)} @ {_format_time(peak_ts)})"
            legend_parts.append(f"  {marker} {display_name}")
        lines.extend(legend_parts)

    return "\n".join(lines) + "\n"
