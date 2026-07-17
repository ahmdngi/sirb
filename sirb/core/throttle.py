"""Token bucket rate limiter for worker API resources."""

from __future__ import annotations

import threading
import time


class TokenBucket:
    """Simple thread-safe token bucket rate limiter.

    Each resource (e.g. "equasis", "shodan") has its own bucket with a
    configurable capacity (max tokens) and refill rate (tokens per minute).

    Workers declare their limits via ``rate_limits()``. The pool blocks
    on ``acquire()`` before calling a rate-limited API.
    """

    def __init__(self, capacity: int, refill_per_min: int = 0):
        self._capacity = capacity
        self._tokens = float(capacity)
        self._refill_rate = refill_per_min / 60.0  # tokens per second
        self._last_refill = time.time()
        self._lock = threading.Lock()

    def acquire(self, tokens: int = 1, block: bool = True) -> bool:
        """Acquire tokens from the bucket.

        Args:
            tokens: Number of tokens to consume.
            block: If True, blocks until tokens are available.
                   If False, returns False immediately if insufficient.

        Returns:
            True if tokens were acquired.
        """
        while True:
            with self._lock:
                self._refill()
                if self._tokens >= tokens:
                    self._tokens -= tokens
                    return True

            if not block:
                return False

            # Wait for next refill tick
            time.sleep(0.25)

    def acquire_nowait(self, tokens: int = 1) -> bool:
        """Non-blocking token acquire. Returns True if successful."""
        return self.acquire(tokens, block=False)

    @property
    def available(self) -> float:
        with self._lock:
            self._refill()
            return self._tokens

    def _refill(self):
        now = time.time()
        elapsed = now - self._last_refill
        self._tokens = min(
            self._capacity,
            self._tokens + elapsed * self._refill_rate,
        )
        self._last_refill = now


class TokenBucketPool:
    """Manages a pool of token buckets keyed by resource name."""

    def __init__(self):
        self._buckets: dict[str, TokenBucket] = {}
        self._lock = threading.Lock()

    def register(self, resource: str, max_per_minute: int,
                 burst_capacity: int = None):
        """Register a rate-limited resource.

        Args:
            resource: Name like "equasis", "shodan".
            max_per_minute: Maximum requests per minute.
            burst_capacity: Initial burst size (defaults to max_per_minute).
        """
        cap = burst_capacity or max_per_minute
        with self._lock:
            self._buckets[resource] = TokenBucket(
                capacity=cap,
                refill_per_min=max_per_minute,
            )

    def register_worker(self, worker_name: str, limits: dict[str, int]):
        """Register all rate limits for a worker."""
        for resource, max_per_min in limits.items():
            qualified = f"{worker_name}:{resource}"
            self.register(qualified, max_per_min)

    def acquire(self, worker_name: str, resource: str,
                block: bool = True) -> bool:
        """Acquire a token for a specific worker+resource."""
        qualified = f"{worker_name}:{resource}"
        with self._lock:
            bucket = self._buckets.get(qualified)
        if bucket is None:
            return True  # no limit registered = unlimited
        return bucket.acquire(1, block=block)

    def can_claim(self, worker_name: str) -> bool:
        """Check if this worker has any budget left for its APIs.

        A worker can claim if all its rate-limited resources have available
        tokens. If any bucket is empty, the worker should wait.
        """
        with self._lock:
            for qualified, bucket in self._buckets.items():
                w_name = qualified.split(":")[0]
                if w_name == worker_name and bucket.available < 1:
                    return False
        return True

    def status(self) -> dict[str, float]:
        """Return current token levels for all buckets."""
        with self._lock:
            return {
                name: bucket.available
                for name, bucket in self._buckets.items()
            }
