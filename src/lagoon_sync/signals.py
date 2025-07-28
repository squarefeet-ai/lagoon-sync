# src/lagoon_sync/signals.py
"""
Helpers for graceful shutdown of asyncio applications.

This module provides a context manager to capture OS signals (SIGINT, SIGTERM)
and translate them into `asyncio.Event`, allowing for a clean and
controlled application shutdown sequence.
"""

import asyncio
import logging
import os
import signal
from types import FrameType
from typing import Any, Callable, Dict, Optional, Set

logger: logging.Logger = logging.getLogger(__name__)

_SignalHandler = Callable[[int, Optional[FrameType]], None]


class GracefulShutdown:
    """
    An async context manager that captures POSIX signals for graceful shutdown.

    This utility translates SIGINT and SIGTERM into an `asyncio.Event`.
    Application components can then wait on this event to know when to
    terminate gracefully. It correctly restores previous signal handlers on exit.

    The first received signal triggers a graceful shutdown. A second signal
    triggers and immediate, forceful exit.
    """

    def __init__(self) -> None:
        """Initialize the shutdown manager."""
        self._event: asyncio.Event = asyncio.Event()
        self._old_handlers: Dict[signal.Signals, _SignalHandler] = {}

    async def __aenter__(self) -> asyncio.Event:
        """
        Registers signal handlers and returns the shutdown event.

        Returns:
            asyncio.Event: The event that will be set when a handled signal
                is received.
        """
        loop: asyncio.AbstractEventLoop = asyncio.get_running_loop()
        signals_to_handle: Set[signal.Signals] = {
            signal.SIGINT,
            signal.SIGTERM,
        }

        def _handler(sig: int, _: Optional[FrameType]) -> None:
            """
            Handles shutdown signals.

            The first signal sets the asyncio event for a graceful shutdown.
            Any subsequent signal triggers an immediate, forceful exit.
            """
            if self._event.is_set():
                # This is the second signal. The user wants to exit NOW
                logger.critical(
                    "Received second shutdown signal. Forcing immediate exit."
                )
                # Use os._exit for a hard exit that doesn't run cleanup,
                # which might be what's hanging
                os._exit(1)
            else:
                # This is the first signal. Initiate graceful shutdown
                logger.warning(
                    f"Received shutdown signal: {signal.strsignal(sig)}. "
                    "Initiating graceful shutdown..."
                )
                loop.call_soon_threadsafe(self._event.set)

        for sig in signals_to_handle:
            try:
                # signal.signal must be called from the main thread
                self._old_handlers[sig] = signal.signal(sig, _handler)
            except (ValueError, OSError) as e:
                # This can fail if not in the main thread, e.g. in tests
                logger.warning(f"Could not set handler for {sig.name}: {e}")

        return self._event

    async def __aexit__(self, *args: Any) -> None:
        """Restores original signal handlers."""
        for sig, handler in self._old_handlers.items():
            try:
                signal.signal(sig, handler)
            except (ValueError, OSError) as e:
                logger.warning(f"Could not restore handler for {sig.name}: {e}")
        self._old_handlers.clear()
