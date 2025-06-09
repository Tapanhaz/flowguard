
from contextlib import contextmanager
from typing import Optional, Dict, Any, Awaitable

class RateLimiter:
    def __init__(
        self,
        sec: Optional[int] = None,
        min: Optional[int] = None,
        hour: Optional[int] = None,
        day: Optional[int] = None,
        sec_window: Optional[int] = None,
        min_window: Optional[int] = None,
        hour_window: Optional[int] = None,
        day_window: Optional[int] = None,
        max_burst: Optional[int] = None,
        blocking: bool = True
    ) -> None:
        """
        Initialize a RateLimiter with specified rate limits and time windows.

        Args:
            sec: Maximum number of operations allowed per second window.
            min: Maximum number of operations allowed per minute window.
            hour: Maximum number of operations allowed per hour window.
            day: Maximum number of operations allowed per day window.
            sec_window: Duration of the second window in seconds (default: 1).
            min_window: Duration of the minute window in minutes (default: 1).
            hour_window: Duration of the hour window in hours (default: 1).
            day_window: Duration of the day window in days (default: 1).
            max_burst: Maximum number of operations allowed in a burst.
            blocking: If True, blocks until a permit is acquired; if False, returns immediately.
        """
        ...

    def acquire(self) -> bool:
        """
        Attempt to acquire a permit from the rate limiter.
        This should only be called in non-blocking mode.

        Returns:
            None if no permit is available.

        """
        ...

    def release(self) -> None:
        """
        Release a previously acquired permit for the burst limit.

        This should only be called in non-blocking mode and if 'max_burst' limit is set.
        Call this after a successful `acquire()` to release the burst permit once work is complete.
        """
        ...

    def wait_time(self) -> Optional[float]:
        """
        Calculate the time until the next permit becomes available.

        Returns:
            The time in seconds until the next permit is available, or None if a permit
            is immediately available.
        """
        ...

    def get_remaining(self) -> Dict[str, int]:
        """
        Retrieve the number of remaining permits for each configured time window.

        Returns:
            A dictionary mapping limit types (e.g., 'second', 'minute', 'hour', 'day', 'burst')
            to the number of remaining permits before the next reset.
        """
        ...

    def __enter__(self) -> None:
        """
        Enter a context manager, acquiring a permit.

        In non-blocking mode, this raises a ValueError if no permit is available. In blocking
        mode, it waits until a permit is acquired.

        """
        ...

    def __exit__(self, exc_type: Optional[Any], exc_value: Optional[Any], traceback: Optional[Any]) -> bool:
        """
        Exit a context manager,

        """
        ...


class AsyncRateLimiter:
    def __init__(
        self,
        sec: Optional[int] = None,
        min: Optional[int] = None,
        hour: Optional[int] = None,
        day: Optional[int] = None,
        sec_window: Optional[int] = None,
        min_window: Optional[int] = None,
        hour_window: Optional[int] = None,
        day_window: Optional[int] = None,
        max_burst: Optional[int] = None,
        blocking: bool = True
    ) -> None:
        """
        Initialize an AsyncRateLimiter with specified rate limits and time windows.

        Args:
            sec: Maximum number of operations allowed per second window.
            min: Maximum number of operations allowed per minute window.
            hour: Maximum number of operations allowed per hour window.
            day: Maximum number of operations allowed per day window.
            sec_window: Duration of the second window in seconds (default: 1).
            min_window: Duration of the minute window in minutes (default: 1).
            hour_window: Duration of the hour window in hours (default: 1).
            day_window: Duration of the day window in days (default: 1).
            max_burst: Maximum number of operations allowed in a burst.
            blocking: If True, blocks until a permit is acquired; if False, returns immediately.
        """
        ...

    def acquire(self) -> Awaitable[bool]:
        """
        Attempt to acquire a permit from the rate limiter.
        This should be called in explicit mode only.

        Returns:
            True if a permit was acquired, False if no permit is available.

        """
        ...

    def release(self) -> Awaitable[None]:
        """
        Release a previously acquired permit for the burst limit.

        This should only be called in explicit mode and if 'max_burst' limit is set.
        Call this after a successful `acquire()` to release the burst permit once work is complete.
        """
        ...

    def wait_time(self) -> Awaitable[Optional[float]]:
        """
        Calculate the time until the next permit becomes available.

        Returns:
            The time in seconds until the next permit is available, or None if a permit
            is immediately available.
        """
        ...

    def get_remaining(self) -> Awaitable[Dict[str, int]]:
        """
        Retrieve the number of remaining permits for each configured time window.

        Returns:
            A dictionary mapping limit types (e.g., 'second', 'minute', 'hour', 'day', 'burst')
            to the number of remaining permits before the next reset.
        """
        ...

    async def __aenter__(self) -> None:
        """
        Enter an async context manager, acquiring a permit.

        In non-blocking mode, this raises a ValueError if no permit is available. In blocking
        mode, it waits until a permit is acquired.

        """
        ...

    async def __aexit__(self, exc_type: Optional[Any], exc_value: Optional[Any], traceback: Optional[Any]) -> bool:
        """
        Exit an async context manager.

        """
        ...