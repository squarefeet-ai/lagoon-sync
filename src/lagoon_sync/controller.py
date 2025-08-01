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
        self.metrics_queue: asyncio.Queue[TransferMetrics] = asyncio.Queue(
            maxsize=25000
        )
        self._task: Optional[asyncio.Task[None]] = None
        self._slow_start_threshold: Optional[int] = None
        self._smoothed_rtt_ms: Optional[float] = None
        self._rtt_smoothing_factor: float = 0.25
        self._adjustment_cycles: int = 0
        self._warmup_intervals: int = 3
        self._logged_max_concurrency: bool = False

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
        Applies AIMD logic based on collected metrics, incorporating a slow-start
        threshold to stabilize concurrency near the saturation point.

        Args:
            metrics (List[TransferMetrics]): A list of transfer metrics from
                the last interval.
        """
        self._adjustment_cycles += 1
        num_samples: int = len(metrics)
        successes: List[TransferMetrics] = [m for m in metrics if m.err_tag is None]
        failures: List[TransferMetrics] = [m for m in metrics if m.err_tag is not None]

        error_rate: float = len(failures) / num_samples
        current_limit: int = self._semaphore.limit

        ## Calculate stats from successful transfers
        if successes:
            total_bytes: int = sum(m.bytes for m in successes)
            goodput_mbps: float = (total_bytes * 8) / (
                self._config.controller_interval_s * 1_000_000
            )
            rtts: List[float] = [m.rtt_ms for m in successes if m.rtt_ms is not None]
            median_rtt: float = float(np.median(rtts)) if rtts else 0.0
            p90_rtt: float = float(np.percentile(rtts, 90)) if rtts else 0.0
        else:
            goodput_mbps: float = 0.0
            median_rtt: float = 0.0
            p90_rtt: float = 0.0

        # Update the smoothed RTT using an Exponential Moving Average (EMA).
        # This provides a more stable baseline than just the last interval's RTT.
        if median_rtt > 0:
            if self._smoothed_rtt_ms is None:
                self._smoothed_rtt_ms = median_rtt
            else:
                self._smoothed_rtt_ms = (self._rtt_smoothing_factor * median_rtt) + (
                    1 - self._rtt_smoothing_factor
                ) * self._smoothed_rtt_ms

        logger.debug(
            f"Controller state: limit={current_limit}, "
            f"goodput={goodput_mbps:.2f} Mbps, "
            f"error_rate={error_rate:.2%}, p90_rtt={p90_rtt:.0f}ms, "
            f"smoothed_rtt={self._smoothed_rtt_ms or 0:.0f}ms, "
            f"ssthresh={self._slow_start_threshold}"
        )

        high_error_rate: bool = (
            error_rate > self._config.controller_error_rate_threshold
        )
        # Only check for RTT spikes after a warm-up period
        rtt_spike: bool = (
            self._adjustment_cycles > self._warmup_intervals
            and self._smoothed_rtt_ms is not None
            and p90_rtt
            > self._config.controller_rtt_spike_factor * self._smoothed_rtt_ms
        )

        new_limit: int
        if high_error_rate or rtt_spike:
            # Congestion detected. Multiplicatively decrease the limit and set
            # this new, lower limit as the slow-start threshold.
            reason: str = "error rate" if high_error_rate else "RTT spike"
            new_limit = max(
                self._config.min_concurrency,
                int(current_limit * self._config.controller_decrease_factor),
            )
            self._slow_start_threshold = new_limit
            logger.warning(
                f"High {reason} detected. Decreasing concurrency: "
                f"{current_limit} -> {new_limit}."
            )
            self._logged_max_concurrency = False

        elif current_limit < self._config.max_concurrency:
            # No congestion, so we can increase.
            increase_amount: int
            if (
                self._slow_start_threshold is not None
                and current_limit >= self._slow_start_threshold
            ):
                # Congestion avoidance: increase linearly.
                increase_amount = 1
                logger.info(
                    "Congestion avoidance: probing gently. "
                    f"Concurrency: {current_limit} -> {current_limit + increase_amount}"
                )
            else:
                # Slow start: increase additively.
                increase_amount = self._config.controller_increase_amount
                logger.info(
                    f"Slow start: increasing concurrency: "
                    f"{current_limit} -> {current_limit + increase_amount}"
                )
            new_limit = current_limit + increase_amount
            self._logged_max_concurrency = False
        else:
            # At the hard-coded maximum limit.
            if not self._logged_max_concurrency:
                logger.info(f"At max concurrency limit of {current_limit}. Holding.")
                self._logged_max_concurrency = True
            else:
                logger.debug(f"At max concurrency limit of {current_limit}. Holding.")
            new_limit = current_limit

        self._semaphore.set_limit(new_limit)
        self._last_rtt_ms = median_rtt
