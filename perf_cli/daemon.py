"""Cross-platform background daemon management for the sampler."""

import json
import os
import signal
import subprocess
import sys
import time
from pathlib import Path
from typing import Optional

from .storage import MetricsStore


PID_FILE = Path.home() / ".perf_cli" / "collector.pid"


def _pid_exists(pid: int) -> bool:
    """Check if a process with the given PID is running."""
    try:
        os.kill(pid, 0)
        return True
    except (OSError, ProcessLookupError):
        return False


def write_pid(pid: int) -> None:
    """Write the collector PID to the PID file."""
    PID_FILE.parent.mkdir(parents=True, exist_ok=True)
    with open(PID_FILE, "w") as f:
        f.write(str(pid))


def read_pid() -> Optional[int]:
    """Read the collector PID from the PID file."""
    if not PID_FILE.exists():
        return None
    try:
        with open(PID_FILE, "r") as f:
            pid = int(f.read().strip())
        if _pid_exists(pid):
            return pid
        PID_FILE.unlink()
        return None
    except (ValueError, OSError):
        return None


def clear_pid() -> None:
    """Remove the PID file."""
    if PID_FILE.exists():
        try:
            PID_FILE.unlink()
        except OSError:
            pass


def start_background(
    interval: float = 1.0,
    disks: Optional[list] = None,
    nets: Optional[list] = None,
    db_path: Optional[Path] = None,
) -> int:
    """Start the sampler as a background process. Returns the PID."""
    existing = read_pid()
    if existing is not None:
        raise RuntimeError(f"Collector is already running with PID {existing}")

    config = json.dumps({
        "interval": interval,
        "disk_filter": list(disks) if disks else None,
        "net_filter": list(nets) if nets else None,
        "db_path": str(db_path) if db_path else None,
    })

    project_root = str(Path(__file__).resolve().parent.parent)
    env = os.environ.copy()
    env["PYTHONPATH"] = project_root + os.pathsep + env.get("PYTHONPATH", "")

    cmd = [
        sys.executable,
        "-m", "perf_cli._daemon_worker",
        config,
    ]

    if sys.platform == "win32":
        CREATE_NO_WINDOW = 0x08000000
        CREATE_NEW_PROCESS_GROUP = 0x00000200
        creationflags = CREATE_NO_WINDOW | CREATE_NEW_PROCESS_GROUP
        proc = subprocess.Popen(
            cmd,
            creationflags=creationflags,
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
            stdin=subprocess.DEVNULL,
            env=env,
            cwd=project_root,
        )
    else:
        proc = subprocess.Popen(
            cmd,
            start_new_session=True,
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
            stdin=subprocess.DEVNULL,
            env=env,
            cwd=project_root,
        )

    time.sleep(1.0)
    pid = read_pid()
    if pid is None:
        raise RuntimeError("Failed to start background collector (check ~/.perf_cli/daemon_error.log)")
    return pid


def stop_background() -> Optional[int]:
    """Stop the background sampler. Returns the stopped PID or None."""
    pid = read_pid()
    if pid is None:
        return None

    try:
        if sys.platform == "win32":
            os.kill(pid, signal.SIGTERM)
        else:
            os.kill(pid, signal.SIGTERM)
            for _ in range(50):
                if not _pid_exists(pid):
                    break
                time.sleep(0.1)
            else:
                os.kill(pid, signal.SIGKILL)
    except (OSError, ProcessLookupError):
        pass

    clear_pid()
    return pid


def get_status() -> dict:
    """Get the current collector status."""
    pid = read_pid()
    if pid is None:
        with MetricsStore() as store:
            active_run = store.get_active_run()
            if active_run:
                store.end_run(active_run["id"])
        return {"running": False, "pid": None}
    return {"running": True, "pid": pid}
