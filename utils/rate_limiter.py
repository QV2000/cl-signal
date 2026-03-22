"""
Rate limiter for Hyperliquid REST API.

Limit: 1,200 weight per rolling 60-second window.
"""
import time
import asyncio
from collections import deque
from dataclasses import dataclass
from typing import Deque, Tuple
import logging

from config import HL_WEIGHT_LIMIT_PER_MINUTE

logger = logging.getLogger(__name__)


@dataclass
class RateLimiter:
    """
    Track API request weights over a rolling window.
    Thread-safe for use with asyncio.
    """
    window_seconds: int = 60
    max_weight: int = HL_WEIGHT_LIMIT_PER_MINUTE
    _requests: Deque[Tuple[float, int]] = None  # (timestamp, weight)
    _lock: asyncio.Lock = None

    def __post_init__(self):
        self._requests = deque()
        self._lock = asyncio.Lock()

    def _cleanup_old_requests(self) -> None:
        """Remove requests older than the window."""
        cutoff = time.time() - self.window_seconds
        while self._requests and self._requests[0][0] < cutoff:
            self._requests.popleft()

    def current_weight(self) -> int:
        """Get total weight in the current window."""
        self._cleanup_old_requests()
        return sum(w for _, w in self._requests)

    def headroom(self) -> int:
        """Get remaining weight capacity."""
        return self.max_weight - self.current_weight()

    def can_request(self, weight: int) -> bool:
        """Check if a request with given weight can be made."""
        return self.headroom() >= weight

    async def acquire(self, weight: int, timeout: float = 60.0) -> bool:
        """
        Wait until there's capacity for a request with given weight.

        Args:
            weight: The weight of the request.
            timeout: Maximum seconds to wait.

        Returns:
            True if acquired, False if timeout.
        """
        start = time.time()

        while True:
            async with self._lock:
                self._cleanup_old_requests()
                if self.can_request(weight):
                    self._requests.append((time.time(), weight))
                    return True

            if time.time() - start > timeout:
                logger.warning(f"Rate limiter timeout waiting for {weight} weight")
                return False

            # Wait a bit before checking again
            await asyncio.sleep(0.1)

    def record(self, weight: int) -> None:
        """
        Record a request weight (synchronous version for simple cases).
        Use acquire() for async code that needs to wait.
        """
        self._requests.append((time.time(), weight))

    def stats(self) -> dict:
        """Get current rate limiter statistics."""
        self._cleanup_old_requests()
        return {
            "current_weight": self.current_weight(),
            "max_weight": self.max_weight,
            "headroom": self.headroom(),
            "requests_in_window": len(self._requests),
            "utilization_pct": round(100 * self.current_weight() / self.max_weight, 1)
        }


# Global rate limiter instance
_rate_limiter = None


def get_rate_limiter() -> RateLimiter:
    """Get or create the global rate limiter."""
    global _rate_limiter
    if _rate_limiter is None:
        _rate_limiter = RateLimiter()
    return _rate_limiter


if __name__ == "__main__":
    import asyncio

    async def test():
        rl = RateLimiter(max_weight=100, window_seconds=5)

        # Make some requests
        for i in range(5):
            await rl.acquire(20)
            print(f"Request {i+1}: {rl.stats()}")

        print("Waiting for window to clear...")
        await asyncio.sleep(6)
        print(f"After wait: {rl.stats()}")

    asyncio.run(test())
