"""Background daemon worker entry point - called by daemon.py via subprocess."""

import json
import os
import signal
import sys
import time

from .daemon import clear_pid, write_pid
from .sampler import MetricsSampler, SamplerConfig
from .storage import MetricsStore, RetentionConfig


def _handler(signum, frame):
    raise SystemExit(0)


def main():
    signal.signal(signal.SIGTERM, _handler)
    if sys.platform != "win32":
        signal.signal(signal.SIGHUP, _handler)

    try:
        if len(sys.argv) < 2:
            config_str = "{}"
        else:
            config_str = sys.argv[1]
        cfg = json.loads(config_str)

        interval = float(cfg.get("interval", 1.0))
        disk_filter = set(cfg["disk_filter"]) if cfg.get("disk_filter") else None
        net_filter = set(cfg["net_filter"]) if cfg.get("net_filter") else None
        db_path = cfg.get("db_path")
        enable_maintenance = bool(cfg.get("enable_maintenance", True))

        retention_cfg = None
        if "retention" in cfg and cfg["retention"]:
            r = cfg["retention"]
            retention_cfg = RetentionConfig(
                retention_days=int(r.get("retention_days", 7)),
                compaction_age_seconds=int(r.get("compaction_age_seconds", 86400)),
                compaction_interval_seconds=int(r.get("compaction_interval_seconds", 3600)),
                enable_auto_compaction=bool(r.get("enable_auto_compaction", True)),
            )

        write_pid(os.getpid())

        store = MetricsStore(db_path, retention=retention_cfg)
        run_id = store.start_run(os.getpid())
        sampler_cfg = SamplerConfig(
            interval=interval,
            disk_filter=disk_filter,
            net_filter=net_filter,
            enable_maintenance=enable_maintenance,
            retention=retention_cfg,
        )
        sampler = MetricsSampler(store, sampler_cfg)
        sampler.run()
    except KeyboardInterrupt:
        pass
    except Exception as e:
        log_path = os.path.join(os.path.expanduser("~"), ".perf_cli", "daemon_error.log")
        try:
            os.makedirs(os.path.dirname(log_path), exist_ok=True)
            with open(log_path, "a") as f:
                import traceback
                f.write(f"[{time.ctime()}] {e}\n")
                f.write(traceback.format_exc())
                f.write("\n---\n")
        except Exception:
            pass
    finally:
        try:
            store.end_run(run_id)
        except Exception:
            pass
        clear_pid()


if __name__ == "__main__":
    main()
