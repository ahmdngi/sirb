"""Thread-safe task queue with optimistic concurrency."""

from __future__ import annotations

import threading
import time
from typing import Optional

from .models import Task, TaskStatus


class TaskQueue:
    """Thread-safe ordered task queue.

    Designed after the swarms (kyegomez) TaskQueue pattern:
    - Thread-safe dict of tasks protected by a ``threading.Lock``
    - Version-based optimistic concurrency on every state transition
    - Priority + FIFO ordering on claim()
    - Dependency tracking (task won't run until depends_on are COMPLETED)
    - Built-in retry with configurable max_retries
    """

    def __init__(self):
        self._tasks: dict[str, Task] = {}
        self._lock = threading.Lock()

    # ── mutations ──────────────────────────────────────────────────────

    def add(self, task: Task) -> str:
        """Add a single task. Returns the task ID."""
        with self._lock:
            self._tasks[task.id] = task
        return task.id

    def add_many(self, tasks: list[Task]) -> list[str]:
        """Bulk-add tasks. Returns list of task IDs."""
        ids = []
        with self._lock:
            for t in tasks:
                self._tasks[t.id] = t
                ids.append(t.id)
        return ids

    def claim(self, worker_name: str) -> Optional[Task]:
        """Atomically claim the highest-priority available task.

        A task is claimable if:
        1. status == PENDING
        2. All depends_on task IDs are COMPLETED

        Returns None if no tasks are available.
        """
        with self._lock:
            completed_ids = {
                tid for tid, t in self._tasks.items()
                if t.status == TaskStatus.COMPLETED
            }

            available = []
            for task in self._tasks.values():
                if task.status != TaskStatus.PENDING:
                    continue
                if all(dep in completed_ids for dep in task.depends_on):
                    available.append(task)

            if not available:
                return None

            # Highest priority first, then oldest first (lower priority value = higher)
            available.sort(key=lambda t: (t.priority, t.created_at))

            chosen = available[0]
            chosen.status = TaskStatus.CLAIMED
            chosen.assigned_worker = worker_name
            chosen.version += 1

            # Return a frozen copy so subsequent state transitions
            # don't mutate the caller's version reference.
            return Task.from_dict(chosen.to_dict())

    def start(self, task_id: str, expected_version: int) -> bool:
        """Mark a claimed task as RUNNING. Returns False on version mismatch."""
        return self._transition(
            task_id, expected_version,
            {TaskStatus.CLAIMED},
            TaskStatus.RUNNING,
        )

    def complete(self, task_id: str, expected_version: int) -> bool:
        """Mark a running task as COMPLETED."""
        return self._transition(
            task_id, expected_version,
            {TaskStatus.RUNNING},
            TaskStatus.COMPLETED,
        )

    def fail(self, task_id: str, error: str, expected_version: int) -> bool:
        """Mark a running task as FAILED, or reset to PENDING if retries remain."""
        with self._lock:
            task = self._tasks.get(task_id)
            if task is None:
                return False
            if task.version != expected_version:
                return False
            if task.status != TaskStatus.RUNNING:
                return False

            task.retries += 1
            task.error = error

            if task.retries <= task.max_retries:
                task.status = TaskStatus.PENDING
                task.assigned_worker = ""
            else:
                task.status = TaskStatus.FAILED
                task.error = error

            task.version += 1
            return True

    def cancel(self, task_id: str) -> bool:
        """Cancel a task if not yet completed or already cancelled."""
        with self._lock:
            task = self._tasks.get(task_id)
            if task is None:
                return False
            if task.status in (TaskStatus.COMPLETED, TaskStatus.CANCELLED):
                return False
            task.status = TaskStatus.CANCELLED
            task.version += 1
            return True

    def clear(self) -> int:
        """Remove all tasks. Returns the count removed."""
        with self._lock:
            count = len(self._tasks)
            self._tasks.clear()
        return count

    def clear_non_terminal(self) -> int:
        """Remove pending/claimed/running tasks, preserving completed/failed.

        Returns the number of tasks removed.
        """
        terminal = {TaskStatus.COMPLETED, TaskStatus.FAILED, TaskStatus.CANCELLED}
        with self._lock:
            to_remove = [tid for tid, t in self._tasks.items() if t.status not in terminal]
            for tid in to_remove:
                del self._tasks[tid]
        return len(to_remove)

    # ── reads ───────────────────────────────────────────────────────────

    def get(self, task_id: str) -> Optional[Task]:
        """Get a read-only copy of a task."""
        with self._lock:
            task = self._tasks.get(task_id)
            return task if task is None else Task.from_dict(task.to_dict())

    def all(self) -> list[Task]:
        """Get read-only copies of all tasks."""
        with self._lock:
            return [Task.from_dict(t.to_dict()) for t in self._tasks.values()]

    def count(self, status: TaskStatus | None = None) -> int:
        """Count tasks, optionally filtered by status."""
        with self._lock:
            if status is None:
                return len(self._tasks)
            return sum(1 for t in self._tasks.values() if t.status == status)

    def get_status(self) -> dict:
        """Returns a snapshot of queue state for reporting."""
        with self._lock:
            counts = {}
            total = 0
            for t in self._tasks.values():
                counts[t.status.value] = counts.get(t.status.value, 0) + 1
                total += 1

            return {
                "total": total,
                "status_counts": counts,
                "progress": f"{counts.get('completed', 0)}/{total}" if total else "0/0",
            }

    def to_dict(self) -> dict:
        """Serialise entire queue for checkpoint."""
        with self._lock:
            return {
                "tasks": {tid: t.to_dict() for tid, t in self._tasks.items()},
            }

    @classmethod
    def from_dict(cls, d: dict) -> TaskQueue:
        """Deserialise a checkpointed queue."""
        q = cls()
        for tid, tdata in d.get("tasks", {}).items():
            q._tasks[tid] = Task.from_dict(tdata)
        return q

    # ── helpers ─────────────────────────────────────────────────────────

    def _transition(
        self,
        task_id: str,
        expected_version: int,
        valid_statuses: set[TaskStatus],
        new_status: TaskStatus,
    ) -> bool:
        with self._lock:
            task = self._tasks.get(task_id)
            if task is None:
                return False
            if task.version != expected_version:
                return False
            if task.status not in valid_statuses:
                return False
            task.status = new_status
            task.version += 1
            return True
