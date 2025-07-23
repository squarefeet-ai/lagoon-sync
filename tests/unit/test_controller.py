# tests/unit/test_controller.py
"""
Unit tests for the concurrency control components.

These tests validate the logic of the `DynamicSemaphore` and
the `CongestionController`. The tests for `DynamicSemaphore` verify
its state machine and resizing. The tests for `CongestionController`
focus on its core AIMD like algorithm by directly calling the adjustment
method with controlled inputs, avoiding the need to mock timers
or the asyncio event loop.
"""

import asyncio
from typing import List

import pytest

from lagoon_sync.config import AppConfig
from lagoon_sync.controller import (
    CongestionController,
    DynamicSemaphore,
    TransferMetrics,
)


# --- DynamicSemaphore Tests ---
@pytest.mark.asyncio
async def test_semaphore_initialization() -> None:
    """
    Tests that the semaphore initializes with the correct limit.
    """
    semaphore: DynamicSemaphore = DynamicSemaphore(
        initial_value=10, min_value=1, max_value=100
    )
    assert semaphore.limit == 10
    assert semaphore._value == 10


@pytest.mark.asyncio
async def test_semaphore_increase_limit() -> None:
    """
    Tests that increasing the limit allows more tasks to acquire it.
    """
    semaphore: DynamicSemaphore = DynamicSemaphore(
        initial_value=1, min_value=1, max_value=10
    )
    await semaphore.acquire()  # Use the only available slot

    # A new task would block here
    waiter: asyncio.Task[bool] = asyncio.create_task(semaphore.acquire())
    await asyncio.sleep(0.01)
    assert not waiter.done()

    # Increase the limit, which should unblock the waiter
    semaphore.set_limit(2)
    await asyncio.sleep(0.01)
    assert waiter.done()

    waiter.cancel()


@pytest.mark.asyncio
async def test_semaphore_decrease_limit() -> None:
    """
    Tests that decreasing the limit defers the reduction until releases occur.
    """
    semaphore: DynamicSemaphore = DynamicSemaphore(
        initial_value=5, min_value=1, max_value=10
    )
    # Acquire all 5 slots
    for _ in range(5):
        await semaphore.acquire()

    assert semaphore._value == 0

    # Decrease the limit. This shouldn't affect current holders.
    semaphore.set_limit(2)
    assert semaphore.limit == 2
    assert semaphore._shrink_pending == 3
    assert semaphore._value == 0  # Still no slots available

    # Release one. It should be "burned" to satisfy the shrink.
    semaphore.release()
    assert semaphore._shrink_pending == 2
    assert semaphore._value == 0  # Still blocked

    # Release two more
    semaphore.release()
    semaphore.release()
    assert semaphore._shrink_pending == 0
    assert semaphore._value == 0  # Still blocked

    # The next release should finally free up a slot
    semaphore.release()
    assert semaphore._value == 1


@pytest.mark.asyncio
async def test_semaphore_clamping() -> None:
    """
    Tests that `set_limit` respects the min and max values.
    """
    semaphore: DynamicSemaphore = DynamicSemaphore(
        initial_value=10, min_value=5, max_value=10
    )

    semaphore.set_limit(100)
    assert semaphore.limit == 10

    semaphore.set_limit(1)
    assert semaphore.limit == 5


# --- CongestionController Tests ---


def create_metrics(
    count: int,
    error_rate: float,
    rtt_ms: float,
    size_bytes: int = 1024,
) -> List[TransferMetrics]:
    """
    Helper to generate a list of transfer metrics for testing.

    Args:
        count (int): The current number of transfers.
        error_rate (float): The current error rate.
        rtt_ms (float): The current rtt.
        size_bytes (int): The current size in bytes.
    """
    metrics: List[TransferMetrics] = []
    num_errors: int = int(count * error_rate)
    for i in range(count):
        is_error: bool = i < num_errors
        metrics.append(
            TransferMetrics(
                bytes=0 if is_error else size_bytes,
                duration_s=1.0,
                err_tag="SomeError" if is_error else None,
                rtt_ms=rtt_ms,
            )
        )
    return metrics


@pytest.mark.parametrize(
    "current_limit, last_goodput, last_rtt, metrics, expected_limit, reason",
    [
        (
            50,
            0.16,
            100.0,
            create_metrics(count=100, error_rate=0.0, rtt_ms=100),
            52,
            "Additive Increase: Good performance",
        ),
        (
            50,
            10.0,
            100.0,
            create_metrics(count=100, error_rate=0.15, rtt_ms=100),
            37,
            "Multiplicative Decrease: High error rate",
        ),
        (
            50,
            10.0,
            100.0,
            create_metrics(count=100, error_rate=0.0, rtt_ms=160),
            37,
            "Multiplicative Decrease: RTT spike",
        ),
        (
            50,
            0.1,
            100.0,
            create_metrics(count=100, error_rate=0.0, rtt_ms=100, size_bytes=512),
            50,
            "Hold Steady: Throughput dropped without errors",
        ),
        (
            10,
            10.0,
            100.0,
            create_metrics(count=100, error_rate=0.2, rtt_ms=100),
            7,
            "Multiplicative Decrease: Clamped to min_concurrency",
        ),
    ],
)
def test_controller_adjust_concurrency(
    current_limit: int,
    last_goodput: float,
    last_rtt: float,
    metrics: List[TransferMetrics],
    expected_limit: int,
    reason: str,
) -> None:
    """
    Tests the AIMD logic of the controller under various scenarios.

    This test is synchronous and calls the private _adjust_concurrency method
    directly to validate the core algorithm without involving asyncio scheduling.

    Args:
        current_limit (int): The current concurrency limit.
        last_goodput (float): The last goodput value.
        last_rtt (float): The last rtt value.
        metrics (List[TransferMetrics]): The list of the last `TransferMetrics`.
        expected_limit (int): The current expected limit.
        reason (str): The reason of the test.
    """
    # Arrange
    min_concurrency: int = 7
    app_config: AppConfig = AppConfig(
        min_concurrency=min_concurrency, controller_interval_s=5
    )
    semaphore: DynamicSemaphore = DynamicSemaphore(current_limit, min_concurrency, 1000)
    controller: CongestionController = CongestionController(
        semaphore, app_config, asyncio.Event()
    )
    controller._last_goodput = last_goodput
    controller._last_rtt_ms = last_rtt

    # Act
    controller._adjust_concurrency(metrics)

    # Assert
    assert semaphore.limit == expected_limit, f"Failed on scenario: {reason}"
