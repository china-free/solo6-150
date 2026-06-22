"""Lightweight system metrics sampler using psutil.

Pure producer: collects metrics and publishes them to an event bus.
No knowledge of storage or persistence - that's the consumer's job.
"""

import time
from dataclasses import dataclass, field
from typing import Dict, List, Optional, Set

import psutil

from .event_bus import MetricEventBus
from .storage_base import MetricPoint


@dataclass
class SamplerConfig:
    """Configuration for the metrics sampler."""
    interval: float = 1.0
    include_cpu: bool = True
    include_per_cpu: bool = False
    include_memory: bool = True
    include_swap: bool = True
    include_disk_io: bool = True
    include_net_io: bool = True
    disk_filter: Optional[Set[str]] = None
    net_filter: Optional[Set[str]] = None


@dataclass
class _IOSnapshot:
    """Internal snapshot for computing IO rates."""
    disk_io: Dict[str, "psutil._common.sdiskio"] = field(default_factory=dict)
    net_io: Dict[str, "psutil._common.snetio"] = field(default_factory=dict)
    timestamp: float = 0.0


class MetricsSampler:
    """Samples system metrics and publishes them to an event bus.

    Pure producer role:
    - Collects CPU, memory, disk IO, network IO at configurable intervals
    - Publishes MetricPoint events to the event bus
    - Does NOT know or care who consumes the data
    - Designed to be extremely lightweight and never blocked by slow consumers
    """

    def __init__(
        self,
        bus: MetricEventBus,
        config: Optional[SamplerConfig] = None,
    ):
        self._bus = bus
        self._config = config or SamplerConfig()
        self._last_io = _IOSnapshot()
        self._running = False
        self._sample_count = 0

    def _collect_cpu(self, now_ts: int) -> List[MetricPoint]:
        points = []
        if self._config.include_cpu:
            cpu_pct = psutil.cpu_percent(interval=None)
            points.append(MetricPoint(now_ts, "cpu_percent", float(cpu_pct)))
        if self._config.include_per_cpu:
            per_cpu = psutil.cpu_percent(interval=None, percpu=True)
            for i, pct in enumerate(per_cpu):
                points.append(
                    MetricPoint(now_ts, "cpu_percent", float(pct), tag=f"core{i}")
                )
        return points

    def _collect_memory(self, now_ts: int) -> List[MetricPoint]:
        points = []
        if self._config.include_memory:
            mem = psutil.virtual_memory()
            points.append(MetricPoint(now_ts, "mem_percent", float(mem.percent)))
            points.append(
                MetricPoint(
                    now_ts, "mem_used_mb", float(mem.used) / (1024 * 1024)
                )
            )
        if self._config.include_swap:
            swap = psutil.swap_memory()
            points.append(MetricPoint(now_ts, "swap_percent", float(swap.percent)))
        return points

    def _collect_disk_io(self, now_ts: int) -> List[MetricPoint]:
        if not self._config.include_disk_io:
            return []
        points = []
        current = psutil.disk_io_counters(perdisk=True)
        if self._last_io.disk_io and self._last_io.timestamp > 0:
            dt = time.time() - self._last_io.timestamp
            if dt > 0:
                for name, counters in current.items():
                    if self._config.disk_filter and name not in self._config.disk_filter:
                        continue
                    if name in self._last_io.disk_io:
                        prev = self._last_io.disk_io[name]
                        read_rate = (counters.read_bytes - prev.read_bytes) / dt
                        write_rate = (counters.write_bytes - prev.write_bytes) / dt
                        points.append(
                            MetricPoint(
                                now_ts,
                                "disk_read_mbps",
                                float(read_rate) / (1024 * 1024),
                                tag=name,
                            )
                        )
                        points.append(
                            MetricPoint(
                                now_ts,
                                "disk_write_mbps",
                                float(write_rate) / (1024 * 1024),
                                tag=name,
                            )
                        )
        self._last_io.disk_io = current
        return points

    def _collect_net_io(self, now_ts: int) -> List[MetricPoint]:
        if not self._config.include_net_io:
            return []
        points = []
        current = psutil.net_io_counters(pernic=True)
        if self._last_io.net_io and self._last_io.timestamp > 0:
            dt = time.time() - self._last_io.timestamp
            if dt > 0:
                for name, counters in current.items():
                    if self._config.net_filter and name not in self._config.net_filter:
                        continue
                    if name in self._last_io.net_io:
                        prev = self._last_io.net_io[name]
                        recv_rate = (counters.bytes_recv - prev.bytes_recv) / dt
                        send_rate = (counters.bytes_sent - prev.bytes_sent) / dt
                        points.append(
                            MetricPoint(
                                now_ts,
                                "net_recv_mbps",
                                float(recv_rate) / (1024 * 1024),
                                tag=name,
                            )
                        )
                        points.append(
                            MetricPoint(
                                now_ts,
                                "net_send_mbps",
                                float(send_rate) / (1024 * 1024),
                                tag=name,
                            )
                        )
        self._last_io.net_io = current
        return points

    def sample_once(self) -> List[MetricPoint]:
        """Collect one sample of all configured metrics and publish to the bus.

        Returns the list of points collected (for debugging/testing).
        """
        now_ts = int(time.time())
        points: List[MetricPoint] = []

        points.extend(self._collect_cpu(now_ts))
        points.extend(self._collect_memory(now_ts))
        points.extend(self._collect_disk_io(now_ts))
        points.extend(self._collect_net_io(now_ts))

        self._last_io.timestamp = time.time()

        if points:
            self._bus.publish(points)
            self._sample_count += 1

        return points

    def run(self, stop_flag=None) -> None:
        """Run the sampling loop until stop_flag is set or KeyboardInterrupt.

        stop_flag: optional callable returning bool, checked each iteration.

        The sampler never blocks on consumers. If a subscriber's queue is
        full, data for that subscriber is silently dropped - we prioritize
        sampling accuracy over delivery guarantees.
        """
        self._running = True
        self.sample_once()

        try:
            while self._running:
                start = time.monotonic()
                self.sample_once()

                elapsed = time.monotonic() - start
                sleep_for = max(0.0, self._config.interval - elapsed)
                if sleep_for > 0:
                    end_deadline = time.monotonic() + sleep_for
                    while time.monotonic() < end_deadline:
                        if stop_flag is not None and stop_flag():
                            self._running = False
                            break
                        time.sleep(min(0.05, end_deadline - time.monotonic()))
                if stop_flag is not None and stop_flag():
                    self._running = False
        except KeyboardInterrupt:
            pass
        finally:
            self._running = False

    def stop(self) -> None:
        self._running = False

    @property
    def sample_count(self) -> int:
        return self._sample_count
