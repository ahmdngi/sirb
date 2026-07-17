"""Tests for token bucket rate limiter."""

import time
import threading

import pytest

from sirb.core import TokenBucket, TokenBucketPool


class TestTokenBucket:
    def test_init_full(self):
        tb = TokenBucket(10, 60)
        assert tb.available == 10

    def test_acquire_reduces_tokens(self):
        tb = TokenBucket(10, 60)
        tb.acquire(3)
        assert tb.available == pytest.approx(7, abs=0.1)

    def test_acquire_nowait_fails_when_empty(self):
        tb = TokenBucket(1, 0)  # no refill
        tb.acquire()
        assert tb.acquire_nowait() is False

    def test_refill_over_time(self):
        tb = TokenBucket(5, 60)  # 1 token/sec
        tb.acquire(5)  # drain
        assert tb.available == pytest.approx(0, abs=0.1)
        time.sleep(1.1)
        assert tb.available > 0.5

    def test_no_negative_tokens(self):
        tb = TokenBucket(3, 0)
        tb.acquire(3)
        assert tb.available >= 0

    def test_capacity_ceiling(self):
        tb = TokenBucket(5, 60)
        time.sleep(2)
        assert tb.available <= 5  # should not exceed capacity


class TestTokenBucketPool:
    def test_register_and_acquire(self):
        pool = TokenBucketPool()
        pool.register("equasis", 4)
        pool.register("shodan", 30)
        assert pool.acquire("worker", "equasis") is True

    def test_register_worker(self):
        pool = TokenBucketPool()
        pool.register_worker("shipcrawler", {"equasis": 4, "shodan": 30})
        assert pool.acquire("shipcrawler", "equasis") is True

    def test_unlimited_resource_returns_true(self):
        pool = TokenBucketPool()
        assert pool.acquire("any", "unregistered") is True

    def test_can_claim_true_when_budget_exists(self):
        pool = TokenBucketPool()
        pool.register_worker("w", {"api": 60})
        assert pool.can_claim("w") is True

    def test_can_claim_false_when_depleted(self):
        pool = TokenBucketPool()
        pool.register_worker("w", {"api": 0})
        assert pool.can_claim("w") is False

    def test_status(self):
        pool = TokenBucketPool()
        pool.register("my_api", 60, burst_capacity=10)
        status = pool.status()
        assert "my_api" in status
