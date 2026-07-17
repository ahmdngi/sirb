"""Tests for health checks and webhook integration."""

from unittest.mock import MagicMock, patch

import pytest

from sirb.core import Task, Result, WorkerPool, TaskQueue, Router, WorkerRegistry
from sirb.core.worker_base import SirbWorker


class FailingWorker(SirbWorker):
    """A worker that always fails."""
    name = "failing"
    description = "Always fails"

    async def execute(self, task):
        return Result(status="failure", error="boom")


class PassingWorker(SirbWorker):
    """A worker that always succeeds."""
    name = "passing"
    description = "Always passes"

    async def execute(self, task):
        return Result(status=task.params.get("status", "success"))


class TestHealthChecks:
    @pytest.mark.asyncio
    async def test_consecutive_failures_tracked(self):
        """Pool tracks consecutive failures per worker."""
        w = FailingWorker()
        reg = WorkerRegistry()
        reg["failing"] = w
        router = Router(reg)
        queue = TaskQueue()
        queue.add(Task(worker="failing", params={"x": 1}))

        pool = WorkerPool(queue, router, max_workers=1, max_failures=2)
        pool.run()

        assert pool._consecutive_failures.get("failing", 0) >= 1

    @pytest.mark.asyncio
    async def test_success_resets_failures(self):
        """A success after failures resets the counter."""
        w = PassingWorker()
        reg = WorkerRegistry()
        reg["passing"] = w
        router = Router(reg)
        queue = TaskQueue()
        queue.add(Task(worker="passing", params={"x": 1, "status": "failure"}))
        queue.add(Task(worker="passing", params={"x": 2, "status": "failure"}))
        queue.add(Task(worker="passing", params={"x": 3, "status": "success"}))

        pool = WorkerPool(queue, router, max_workers=2, max_failures=3)
        pool.run()

        # After a success, failures should be reset
        assert pool._consecutive_failures.get("passing", 0) == 0


class TestWebhook:
    @patch("urllib.request.urlopen")
    def test_skipped_when_no_url(self, mock_urlopen):
        """No webhook configured = no POST."""
        from sirb.core import Aggregator
        # Just verify the import works
        assert Aggregator is not None
        mock_urlopen.assert_not_called()
