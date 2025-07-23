# src/lagoon_sync/controller.py
"""
Automatic concurrency controller and dynamic semaphore.

This module implements the AIMD-based congestion controller to dynamically
adjust the number of concurrent transfers based on real-time network
performance signals.
"""

import asyncio
import logging
from collections import deque
from dataclasses import dataclass
from typing import Deque, List, Optional

import numpy as np

from lagoon_sync.config import AppConfig

logger: logging.Logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class TransferMetrics:
    """
    A record of a single object transfer's performance.

    Attributes:
        bytes (int): Size of the transferred object in bytes.
        duration_s (float): Total duration of the transfer attempt in seconds.
        err_tag (str, optional): The exception type name if the transfer failed,
            else None.
        rtt_ms (float, optional): Time-to-first-byte for the S3 GET request
            in milliseconds.
    """

    bytes: int
    duration_s: float
    err_tag: Optional[str] = None
    rtt_ms: Optional[float] = None


class DynamicSemaphore(asyncio.Semaphore):
    """
    An `asyncio.Semaphore` that allows its limit to be changed at runtime.
    """

    def __init__(self, initial_value: int, min_value: int, max_value: int) -> None:
        """
        Initialize the dynamic semaphore.

        Args:
            initial_value (int): The starting concurrency limit.
            min_value (int): The absolute minimum concurrency limit.
            max_value (int): The absolute maximum concurrency limit.
        """
        super().__init__(initial_value)
        self._limit: int = initial_value
        self._min_value: int = min_value
        self._max_value: int = max_value
        self._shrink_pending: int = 0

    @property
    def limit(self) -> int:
        """
        Get the current concurrency limit.

        Returns:
            int: The current concurrent limit value.
        """
        return self._limit

    def set_limit(self, new_limit: int) -> None:
        """
        Adjusts the semaphore's limit, clamping min/max values.

        If the limit is reduced, it doesn't affect tasks that have already
        acquired the semaphore. Instead, it defers the reduction until
        enough tasks have been released.

        Args:
            new_limit (int): The new desired concurrency limit.
        """
        new_limit: int = max(self._min_value, min(self._max_value, new_limit))
        if new_limit == self._limit:
            return

        diff: int = new_limit - self._limit
        self._limit = new_limit

        if diff > 0:
            # Increase limit: release the lock for new waiters
            for _ in range(diff):
                try:
                    super().release()
                except ValueError:
                    # This can happen if the semaphore is already at its max
                    # internal capacity, which if fine
                    pass
        elif diff < 0:
            # Decrease limit: track pending shrinks
            self._shrink_pending += -diff

    def release(self) -> None:
        """
        Releases the semaphore, accounting for any pending shrinks.
        """
        if self._shrink_pending > 0:
            # "Burn" the release to satisfy a pending shrink request
            self._shrink_pending -= 1
        else:
            super().release()


class CongestionController:
    """
    Monitors transfer metrics and adjusts concurrency using an AIMD-like strategy.
    """

    def __init__(
        self,
        semaphore: DynamicSemaphore,
        app_config: AppConfig,
        shutdown_event: asyncio.Event,
    ) -> None:
        """
        Initialize the congestion controller.

        Args:
            semaphore (DynamicSemaphore): The dynamic semaphore to control.
            app_config (AppConfig): The application configuration.
            shutdown_event (asyncio.Event): Event to signal graceful shutdown.
        """
        self._semaphore: DynamicSemaphore = semaphore
        self._config: AppConfig = app_config
        self._shutdown_event: asyncio.Event = shutdown_event
        self.metrics_queue: asyncio.Queue[TransferMetrics] = asyncio.Queue()
        self._task: Optional[asyncio.Task[None]] = None
        self._last_goodput: float = 0.0
        self._last_rtt_ms: float = 0.0

    async def run(self) -> None:
        """Starts the controller's background adjustment loop."""
        logger.info(
            "Congestion controller started. Adjusting concurrency every "
            f"{self._config.controller_interval_s}s."
        )
        self._task = asyncio.create_task(self._control_loop())
        await self._task

    async def stop(self) -> None:
        """Stops the controller's background task."""
        if self._task and not self._task.done():
            self._task.cancel()
            try:
                await self._task
            except asyncio.CancelledError:
                pass
        logger.info("Congestion controller stopped.")

    async def _control_loop(self) -> None:
        """The main loop that periodically analyzes metrics and adjusts."""
        metrics_this_interval: Deque[TransferMetrics] = deque()
        while not self._shutdown_event.is_set():
            try:
                # Wait for the interval, but wake up if shutdown is signaled.
                await asyncio.wait_for(
                    self._shutdown_event.wait(),
                    timeout=self._config.controller_interval_s,
                )
                break  # Shutdown event was set
            except asyncio.TimeoutError:
                pass  # This is the normal path
            except asyncio.CancelledError:
                break

            # Drain the queue of all metrics collected during the interval
            while not self.metrics_queue.empty():
                metrics_this_interval.append(self.metrics_queue.get_nowait())

            if not metrics_this_interval:
                logger.debug("Controller: no metrics this interval, holding steady.")
                continue

            self._adjust_concurrency(list(metrics_this_interval))
            metrics_this_interval.clear()

    def _adjust_concurrency(self, metrics: List[TransferMetrics]) -> None:
        """
        Applies AIMD logic based on collected metrics.

        Args:
            metrics (List[TransferMetrics]): A list of transfer metrics from
                the last interval.
        """
        num_samples: int = len(metrics)
        successes: List[TransferMetrics] = [m for m in metrics if m.err_tag is None]
        failures: List[TransferMetrics] = [m for m in metrics if m.err_tag is not None]

        error_rate: float = len(failures) / num_samples
        current_limit: int = self._semaphore.limit

        # Calculate stats from successful transfers
        if successes:
            total_bytes: int = sum(m.bytes for m in successes)
            goodput_mbps: float = (total_bytes * 8) / (
                self._config.controller_interval_s * 1_000_000
            )
            rtts: List[float] = [m.rtt_ms for m in successes if m.rtt_ms is not None]
            median_rtt: float = float(np.median(rtts)) if rtts else self._last_rtt_ms
        else:
            goodput_mbps: float = 0.0
            median_rtt: float = self._last_rtt_ms

        logger.debug(
            f"Controller state: limit={current_limit}, "
            f"goodput={goodput_mbps:.2f} Mbps, "
            f"error_rate={error_rate:.2%}, median_rtt={median_rtt:.0f}ms"
        )

        # AIMD logic
        new_limit: int = current_limit
        if error_rate > 0.1 or (
            self._last_rtt_ms > 0 and median_rtt > 1.5 * self._last_rtt_ms
        ):
            # Multiplicative Decrease: High errors or sharp RTT spike
            new_limit = max(
                self._config.min_concurrency,
                int(current_limit * self._config.controller_decrease_factor),
            )
            logger.warning(
                f"High error rate or RTT spike. Decreasing concurrency: "
                f"{current_limit} -> {new_limit}"
            )
        elif goodput_mbps >= self._last_goodput * 0.95:
            # Additive Increase: Performance is stable or improving
            new_limit = current_limit + self._config.controller_increase_amount
        else:
            # Throughput dropped without high errors, maybe saturation. Hold.
            logger.info(
                f"Throughput dropped, holding concurrency steady at {current_limit}."
            )

        self._semaphore.set_limit(new_limit)
        self._last_goodput = goodput_mbps
        self._last_rtt_ms = median_rtt
