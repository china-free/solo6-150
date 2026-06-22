"""Event bus and buffered queue for pub/sub decoupling between producers and consumers.

Producer: sampler -> publishes MetricPoint events
Consumer: storage subscriber -> batches and persists to SQLite
"""

import queue
import threading
import time
from dataclasses import dataclass, field
from typing import Any, Callable, Dict, List, Optional

from .storage_base import MetricPoint


DEFAULT_BATCH_SIZE = 100
DEFAULT_FLUSH_INTERVAL = 1.0  # seconds
DEFAULT_QUEUE_CAPACITY = 10000


@dataclass
class SubscriberStats:
    """Statistics for a subscriber."""
    name: str
    received: int = 0
    published: int = 0
    dropped: int = 0
    batches_flushed: int = 0
    avg_batch_size: float = 0.0
    queue_size: int = 0


class MetricEventBus:
    """In-process event bus for metric data points.

    Supports multiple subscribers. Each subscriber gets its own queue.
    Producers call publish() and never block unless all subscriber queues
    are full and block_on_full=True.

    This completely decouples metric producers (samplers) from consumers
    (storage, real-time monitors, alerting, etc.).
    """

    def __init__(
        self,
        queue_capacity: int = DEFAULT_QUEUE_CAPACITY,
        block_on_full: bool = False,
    ):
        self._queue_capacity = queue_capacity
        self._block_on_full = block_on_full
        self._subscribers: Dict[str, "SubscriberHandle"] = {}
        self._lock = threading.Lock()
        self._running = True

    def subscribe(
        self,
        name: str,
        handler: Callable[[List[MetricPoint]], None],
        batch_size: int = DEFAULT_BATCH_SIZE,
        flush_interval: float = DEFAULT_FLUSH_INTERVAL,
    ) -> "SubscriberHandle":
        """Register a subscriber that receives batches of metric points.

        Args:
            name: Unique subscriber name
            handler: Callback that receives a list of MetricPoint
            batch_size: Max points per batch before calling handler
            flush_interval: Max time (seconds) to wait before flushing a
                partial batch

        Returns:
            SubscriberHandle - can be used to unsubscribe or get stats
        """
        handle = SubscriberHandle(
            name=name,
            handler=handler,
            batch_size=batch_size,
            flush_interval=flush_interval,
            queue_capacity=self._queue_capacity,
        )
        with self._lock:
            if name in self._subscribers:
                self._subscribers[name].stop()
            self._subscribers[name] = handle
        handle.start()
        return handle

    def unsubscribe(self, name: str) -> None:
        """Unregister a subscriber by name."""
        with self._lock:
            handle = self._subscribers.pop(name, None)
        if handle:
            handle.stop()

    def publish(self, points: List[MetricPoint]) -> int:
        """Publish a list of metric points to all subscribers.

        Returns the number of subscribers that received the points.

        This is non-blocking by default (block_on_full=False), meaning
        data is dropped if a subscriber's queue is full. This prevents
        the sampling loop from being blocked by slow consumers.
        """
        if not points:
            return 0
        count = 0
        with self._lock:
            handles = list(self._subscribers.values())
        for handle in handles:
            try:
                handle.enqueue(points)
                count += 1
            except queue.Full:
                pass
        return count

    def get_stats(self) -> Dict[str, SubscriberStats]:
        """Get statistics for all subscribers."""
        stats = {}
        with self._lock:
            handles = list(self._subscribers.items())
        for name, handle in handles:
            stats[name] = handle.get_stats()
        return stats

    def flush_all(self, timeout: float = 5.0) -> None:
        """Flush all subscribers (drain queues, call handlers one last time)."""
        with self._lock:
            handles = list(self._subscribers.values())
        for handle in handles:
            handle.flush(timeout=timeout)

    def close(self) -> None:
        """Stop all subscribers and clean up."""
        self._running = False
        with self._lock:
            handles = list(self._subscribers.values())
            self._subscribers.clear()
        for handle in handles:
            handle.stop()

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()


class SubscriberHandle:
    """Handle for a single subscriber.

    Manages the input queue, batching logic, and consumer thread.
    The consumer thread calls the handler periodically with batches.
    """

    def __init__(
        self,
        name: str,
        handler: Callable[[List[MetricPoint]], None],
        batch_size: int,
        flush_interval: float,
        queue_capacity: int,
    ):
        self.name = name
        self._handler = handler
        self._batch_size = max(1, batch_size)
        self._flush_interval = max(0.01, flush_interval)
        self._queue: "queue.Queue[List[MetricPoint]]" = queue.Queue(
            maxsize=queue_capacity
        )
        self._thread: Optional[threading.Thread] = None
        self._stop_event = threading.Event()
        self._flush_event = threading.Event()

        self._stats_lock = threading.Lock()
        self._received = 0
        self._published = 0
        self._dropped = 0
        self._batches_flushed = 0
        self._total_batch_size = 0

    def start(self) -> None:
        """Start the consumer thread."""
        if self._thread is not None and self._thread.is_alive():
            return
        self._stop_event.clear()
        self._thread = threading.Thread(
            target=self._consume_loop,
            name=f"perf_bus_{self.name}",
            daemon=True,
        )
        self._thread.start()

    def stop(self) -> None:
        """Stop the consumer thread gracefully.

        Flushes remaining items before stopping.
        """
        self._stop_event.set()
        self._flush_event.set()
        if self._thread is not None and self._thread.is_alive():
            self._thread.join(timeout=10.0)
        self._thread = None

    def enqueue(self, points: List[MetricPoint]) -> None:
        """Put a batch of points into the subscriber's queue.

        Raises queue.Full if the queue is full and point is dropped.
        """
        if self._stop_event.is_set():
            raise queue.Full("Subscriber stopped")
        try:
            self._queue.put_nowait(list(points))
            with self._stats_lock:
                self._received += len(points)
        except queue.Full:
            with self._stats_lock:
                self._dropped += len(points)
            raise

    def flush(self, timeout: float = 5.0) -> None:
        """Trigger an immediate flush."""
        self._flush_event.set()

    def get_stats(self) -> SubscriberStats:
        with self._stats_lock:
            avg = (
                self._total_batch_size / self._batches_flushed
                if self._batches_flushed > 0
                else 0.0
            )
            return SubscriberStats(
                name=self.name,
                received=self._received,
                published=self._published,
                dropped=self._dropped,
                batches_flushed=self._batches_flushed,
                avg_batch_size=avg,
                queue_size=self._queue.qsize(),
            )

    def _consume_loop(self) -> None:
        """Consumer thread: accumulate points, call handler in batches."""
        buffer: List[MetricPoint] = []
        last_flush = time.monotonic()

        while not self._stop_event.is_set():
            try:
                wait_for = max(0.01, self._flush_interval - (time.monotonic() - last_flush))
                try:
                    batch = self._queue.get(timeout=wait_for)
                    buffer.extend(batch)
                except queue.Empty:
                    pass

                should_flush = (
                    len(buffer) >= self._batch_size
                    or (time.monotonic() - last_flush >= self._flush_interval and buffer)
                    or self._flush_event.is_set()
                    or (self._stop_event.is_set() and buffer)
                )

                if should_flush and buffer:
                    self._flush_event.clear()
                    batch_to_send = buffer
                    buffer = []
                    last_flush = time.monotonic()

                    try:
                        self._handler(batch_to_send)
                        with self._stats_lock:
                            self._published += len(batch_to_send)
                            self._batches_flushed += 1
                            self._total_batch_size += len(batch_to_send)
                    except Exception:
                        with self._stats_lock:
                            self._dropped += len(batch_to_send)
                        raise

            except Exception:
                if self._stop_event.is_set():
                    break
                time.sleep(0.1)

        if buffer:
            try:
                self._handler(buffer)
                with self._stats_lock:
                    self._published += len(buffer)
                    self._batches_flushed += 1
                    self._total_batch_size += len(buffer)
            except Exception:
                with self._stats_lock:
                    self._dropped += len(buffer)
