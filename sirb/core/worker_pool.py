"""Worker pool — ThreadPoolExecutor wrapper with Sirb lifecycle."""

from __future__ import annotations

import asyncio
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import Callable, Optional

from .models import Task, Result, TaskStatus
from .task_queue import TaskQueue
from .router import Router
from .worker_base import SirbWorker
from .throttle import TokenBucketPool


class WorkerPool:
    """Manages concurrent task execution against a ThreadPoolExecutor.

    Workers claim tasks from the queue, route them to the correct
    ``SirbWorker.execute()``, write results, and update task state.
    """

    def __init__(
        self,
        queue: TaskQueue,
        router: Router,
        max_workers: int = 10,
        task_timeout: Optional[float] = None,
        on_complete: Optional[Callable[[Task, Result], None]] = None,
        throttle_pool: Optional[TokenBucketPool] = None,
        max_failures: int = 3,
    ):
        self._queue = queue
        self._router = router
        self._max_workers = max_workers
        self._task_timeout = task_timeout
        self._on_complete = on_complete
        self._throttle = throttle_pool or TokenBucketPool()
        self._max_failures = max_failures
        self._consecutive_failures: dict[str, int] = {}

    def run(self, timeout: Optional[float] = None) -> int:
        """Claim and execute all available tasks.

        Args:
            timeout: Max seconds for the entire pool run. Each task still
                has its own ``task_timeout`` per-task ceiling.

        Returns:
            Number of tasks completed.
        """
        completed = 0

        with ThreadPoolExecutor(max_workers=self._max_workers) as executor:
            futures = {}

            while True:
                # Submit more tasks until pool is full
                while len(futures) < self._max_workers:
                    task = self._queue.claim("pool")
                    if task is None:
                        break  # no more work

                    worker = self._router.route(task)
                    if worker is None:
                        self._queue.fail(
                            task.id,
                            f"unknown worker '{task.worker}'",
                            task.version,
                        )
                        continue

                    if not self._queue.start(task.id, task.version):
                        continue  # version mismatch, skip

                    future = executor.submit(
                        self._execute_wrapper, worker, task
                    )
                    futures[future] = task

                if not futures:
                    break  # no work and nothing running

                # Wait for at least one to complete
                for future in as_completed(
                    futures,
                    timeout=self._task_timeout if self._task_timeout else None,
                ):
                    task = futures.pop(future)
                    try:
                        result = future.result(timeout=5)
                        self._handle_result(task, result)
                        if self._on_complete:
                            self._on_complete(task, result)
                        completed += 1
                    except Exception as e:
                        self._queue.fail(
                            task.id,
                            f"worker exception: {e}",
                            task.version,
                        )

        return completed

    async def run_async(self) -> int:
        """Async wrapper around run() for use in asyncio contexts."""
        return await asyncio.get_event_loop().run_in_executor(None, self.run)

    # ── internal ────────────────────────────────────────────────────────

    def _execute_wrapper(self, worker: SirbWorker, task: Task) -> Result:
        """Execute a single task with throttle and timeout enforcement."""
        # Throttle
        self._enforce_throttle(worker)

        try:
            if self._task_timeout:
                result = asyncio.run(
                    asyncio.wait_for(
                        worker.execute(task),
                        timeout=self._task_timeout,
                    )
                )
            else:
                result = asyncio.run(worker.execute(task))

            result.task_id = task.id
            result.worker = worker.name
            return result

        except asyncio.TimeoutError:
            return Result(
                task_id=task.id,
                worker=worker.name,
                status="failure",
                error=f"task exceeded {self._task_timeout}s timeout",
            )
        except Exception as e:
            return Result(
                task_id=task.id,
                worker=worker.name,
                status="failure",
                error=str(e),
            )

    def _handle_result(self, task: Task, result: Result):
        """Route a result back into the queue and track failures."""
        # Track consecutive failures per worker
        worker_name = task.worker
        if result.status == "failure":
            self._consecutive_failures[worker_name] = \
                self._consecutive_failures.get(worker_name, 0) + 1
            if self._consecutive_failures[worker_name] >= self._max_failures:
                print(f"[sirb] WARN: worker '{worker_name}' has "
                      f"{self._consecutive_failures[worker_name]} consecutive "
                      f"failures — pausing")
        else:
            self._consecutive_failures.pop(worker_name, None)

        # Validate — handle both async and sync validate() methods
        worker = self._router.route(task)
        if worker and hasattr(worker, "validate"):
            try:
                if asyncio.iscoroutinefunction(worker.validate):
                    valid = asyncio.run(worker.validate(result))
                else:
                    valid = worker.validate(result)
            except Exception:
                valid = True
            if not valid:
                self._queue.fail(
                    task.id, f"validation rejected: {result.error}", task.version
                )
                return

        # Complete
        if result.status in ("success", "partial"):
            self._queue.complete(task.id, task.version)
        else:
            self._queue.fail(
                task.id, result.error or "execution failed", task.version
            )

    def _enforce_throttle(self, worker: SirbWorker):
        """Apply token bucket rate limits for this worker."""
        limits = worker.rate_limits()
        for resource in limits:
            self._throttle.acquire(worker.name, resource, block=True)
