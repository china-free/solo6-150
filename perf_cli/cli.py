"""perf-cli command-line interface."""

import argparse
import sys
import time
from pathlib import Path
from typing import List, Optional

from . import __version__
from .chart import ChartConfig, Series, render_line_chart
from .daemon import get_status, start_background, stop_background
from .sampler import MetricsSampler, SamplerConfig
from .storage import MetricsStore
from .utils import format_duration, parse_duration


METRIC_UNITS = {
    "cpu_percent": "%",
    "mem_percent": "%",
    "mem_used_mb": "mb",
    "swap_percent": "%",
    "disk_read_mbps": "mbps",
    "disk_write_mbps": "mbps",
    "net_recv_mbps": "mbps",
    "net_send_mbps": "mbps",
}


def _get_metric_unit(metric: str) -> str:
    return METRIC_UNITS.get(metric, "")


def cmd_collect(args: argparse.Namespace) -> int:
    """Start collecting metrics."""
    disks = args.disk.split(",") if args.disk else None
    nets = args.net.split(",") if args.net else None

    if args.foreground:
        print("perf-cli: Starting foreground sampling (Ctrl+C to stop)...")
        cfg = SamplerConfig(
            interval=args.interval,
            include_per_cpu=args.per_cpu,
            disk_filter=set(disks) if disks else None,
            net_filter=set(nets) if nets else None,
        )
        with MetricsStore() as store:
            run_id = store.start_run(0)
            sampler = MetricsSampler(store, cfg)
            try:
                sampler.run()
            except KeyboardInterrupt:
                pass
            finally:
                store.end_run(run_id)
        print("\nperf-cli: Sampling stopped.")
    else:
        try:
            pid = start_background(
                interval=args.interval,
                disks=disks,
                nets=nets,
            )
            print(f"perf-cli: Started background sampler (PID {pid})")
            print(f"perf-cli: Stop with: perf-cli stop")
            print(f"perf-cli: View report with: perf-cli report --last 10m")
        except RuntimeError as e:
            print(f"perf-cli: {e}", file=sys.stderr)
            return 1
    return 0


def cmd_stop(args: argparse.Namespace) -> int:
    """Stop the background collector."""
    pid = stop_background()
    if pid is None:
        print("perf-cli: No running collector found.")
        return 1
    print(f"perf-cli: Stopped collector (PID {pid})")
    return 0


def cmd_status(args: argparse.Namespace) -> int:
    """Show collector status."""
    status = get_status()
    if status["running"]:
        print(f"perf-cli: Collector is RUNNING (PID {status['pid']})")
        with MetricsStore() as store:
            active = store.get_active_run()
            if active:
                elapsed = int(time.time()) - active["start_ts"]
                print(f"perf-cli: Running for {format_duration(elapsed)}")
    else:
        print("perf-cli: Collector is NOT running")
    return 0


def cmd_list(args: argparse.Namespace) -> int:
    """List available metrics in the database."""
    with MetricsStore() as store:
        metrics = store.list_metrics()
        if not metrics:
            print("perf-cli: No metrics recorded yet. Start collecting first: perf-cli collect")
            return 0
        print("Available metrics:")
        for m in metrics:
            tags = store.list_tags(m)
            tag_str = f"  [tags: {', '.join(tags)}]" if tags else ""
            print(f"  - {m}{tag_str}")
    return 0


def cmd_report(args: argparse.Namespace) -> int:
    """Generate a terminal report with charts."""
    duration = parse_duration(args.last)
    if duration is None:
        print(f"perf-cli: Invalid duration format: {args.last}", file=sys.stderr)
        print("perf-cli: Use formats like 10m, 1h, 1h30m, 90s", file=sys.stderr)
        return 1

    end_ts = int(time.time())
    start_ts = end_ts - duration

    metrics = args.metric.split(",") if args.metric else None

    with MetricsStore() as store:
        available = store.list_metrics()
        if not available:
            print("perf-cli: No metrics recorded yet. Start collecting first: perf-cli collect")
            return 0

        if metrics is None:
            metrics = [m for m in available if m in ("cpu_percent", "mem_percent")]
            if not metrics:
                metrics = available[:2]

        series_list: List[Series] = []
        for metric in metrics:
            if metric not in available:
                print(f"perf-cli: Warning: metric '{metric}' not found. Available: {', '.join(available)}", file=sys.stderr)
                continue
            tags = store.list_tags(metric)
            if not tags:
                data = store.query_range(metric, start_ts, end_ts)
                if data:
                    series_list.append(
                        Series(
                            name=metric,
                            data=data,
                            unit=_get_metric_unit(metric),
                        )
                    )
            else:
                if args.tag:
                    selected_tags = args.tag.split(",")
                else:
                    selected_tags = tags[:2]
                for tag in selected_tags:
                    if tag not in tags:
                        continue
                    data = store.query_range(metric, start_ts, end_ts, tag=tag)
                    if data:
                        series_list.append(
                            Series(
                                name=f"{metric}:{tag}",
                                data=data,
                                unit=_get_metric_unit(metric),
                            )
                        )

    if not series_list:
        print(f"perf-cli: No data found for the requested time range ({args.last}).")
        print("perf-cli: Try: perf-cli collect, then wait a bit.")
        return 1

    title = f"Performance Report — last {args.last}"
    cfg = ChartConfig(title=title, height=args.height, show_grid=not args.no_grid)
    output = render_line_chart(series_list, cfg)
    print(output)
    return 0


def cmd_purge(args: argparse.Namespace) -> int:
    """Purge old data from the database."""
    days = args.days
    with MetricsStore() as store:
        deleted = store.purge_older_than(days)
        if args.vacuum:
            store.vacuum()
    print(f"perf-cli: Deleted {deleted} data points older than {days} days.")
    if args.vacuum:
        print("perf-cli: Database vacuumed.")
    return 0


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="perf-cli",
        description="Lightweight performance monitoring CLI with background sampling and terminal charts.",
    )
    parser.add_argument("--version", action="version", version=f"perf-cli {__version__}")
    parser.add_argument("--db", type=Path, default=None, help="Path to the SQLite database file")

    sub = parser.add_subparsers(dest="command", required=True)

    p_collect = sub.add_parser("collect", help="Start collecting metrics")
    p_collect.add_argument("--interval", "-i", type=float, default=1.0, help="Sampling interval in seconds")
    p_collect.add_argument("--foreground", "-f", action="store_true", help="Run in foreground instead of background")
    p_collect.add_argument("--per-cpu", action="store_true", help="Sample per-core CPU usage")
    p_collect.add_argument("--disk", type=str, default=None, help="Filter disk devices (comma-separated)")
    p_collect.add_argument("--net", type=str, default=None, help="Filter network interfaces (comma-separated)")
    p_collect.set_defaults(func=cmd_collect)

    p_stop = sub.add_parser("stop", help="Stop the background collector")
    p_stop.set_defaults(func=cmd_stop)

    p_status = sub.add_parser("status", help="Show collector status")
    p_status.set_defaults(func=cmd_status)

    p_list = sub.add_parser("list", help="List available metrics in the database")
    p_list.set_defaults(func=cmd_list)

    p_report = sub.add_parser("report", help="Generate a report with charts")
    p_report.add_argument("--last", "-l", type=str, default="10m", help="Time range (e.g. 10m, 1h, 1h30m)")
    p_report.add_argument("--metric", "-m", type=str, default=None, help="Metrics to display (comma-separated)")
    p_report.add_argument("--tag", "-t", type=str, default=None, help="Filter metric tags (comma-separated)")
    p_report.add_argument("--height", type=int, default=20, help="Chart height in lines")
    p_report.add_argument("--no-grid", action="store_true", help="Disable background grid")
    p_report.set_defaults(func=cmd_report)

    p_purge = sub.add_parser("purge", help="Remove old data from the database")
    p_purge.add_argument("--days", type=int, default=7, help="Delete data older than N days")
    p_purge.add_argument("--vacuum", action="store_true", help="Vacuum the database after purging")
    p_purge.set_defaults(func=cmd_purge)

    return parser


def main(argv: Optional[List[str]] = None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)
    try:
        return args.func(args)
    except KeyboardInterrupt:
        print("\nperf-cli: Interrupted.", file=sys.stderr)
        return 130
    except Exception as e:
        print(f"perf-cli: Error: {e}", file=sys.stderr)
        return 1


if __name__ == "__main__":
    sys.exit(main())
