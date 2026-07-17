"""Thread-safe task queue with optimistic concurrency and deduplication."""

from __future__ import annotations

import threading
import time
from typing import Optional

from .models import Task, TaskStatus


class TaskQueue:
    """Thread-safe ordered task queue.

    - Thread-safe dict of tasks protected by a ``threading.Lock``
    - Version-based optimistic concurrency on every state transition
    - Priority + FIFO ordering on claim()
    - Dependency tracking (task won't run until depends_on are COMPLETED)
    - Built-in retry with configurable max_retries
    - Content-hash deduplication — same (worker + params) rejected
      while the first is still PENDING/CLAIMED/RUNNING
    """

    def __init__(self):
        self._tasks: dict[str, Task] = {}
        self._seen_hashes: dict[str, str] = {}  # content_hash → task_id
        self._lock = threading.Lock()

    # ── mutations ──────────────────────────────────────────────────────

    def add(self, task: Task, dedup: bool = True) -> Optional[str]:
        """Add a single task.

        Args:
            task: The task to add.
            dedup: If True, reject if same (worker + params) already queued
                   and still active (PENDING/CLAIMED/RUNNING).

        Returns:
            Task ID if added, None if rejected as duplicate.
        """
        with self._lock:
            if dedup:
                h = task.content_hash()
                existing = self._seen_hashes.get(h)
                if existing:
                    existing_task = self._tasks.get(existing)
                    if existing_task and existing_task.status in (
                        TaskStatus.PENDING, TaskStatus.CLAIMED, TaskStatus.RUNNING
                    ):
                        return None
                    # Completed/Failed duplicates are fine (re-run)
                    del self._seen_hashes[h]

            self._tasks[task.id] = task
            if dedup:
                self._seen_hashes[task.content_hash()] = task.id
        return task.id

    def add_many(self, tasks: list[Task], dedup: bool = True) -> list[str]:
        """Bulk-add tasks. Returns list of added task IDs (excludes dupes)."""
        ids = []
        with self._lock:
            for t in tasks:
                if dedup:
                    h = t.content_hash()
                    existing = self._seen_hashes.get(h)
                    if existing:
                        existing_task = self._tasks.get(existing)
                        if existing_task and existing_task.status in (
                            TaskStatus.PENDING, TaskStatus.CLAIMED,
                            TaskStatus.RUNNING,
                        ):
                            continue
                        del self._seen_hashes[h]

                self._tasks[t.id] = t
                if dedup:
                    self._seen_hashes[t.content_hash()] = t.id
                ids.append(t.id)
        return ids

    def claim(self, worker_name: str) -> Optional[Task]:
        """Atomically claim the highest-priority available task.

        A task is claimable if:
        1. status == PENDING
        2. All depends_on task IDs are COMPLETED

        Returns a frozen copy of the claimed task, or None.
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

            available.sort(key=lambda t: (t.priority, t.created_at))
            chosen = available[0]
            chosen.status = TaskStatus.CLAIMED
            chosen.assigned_worker = worker_name
            chosen.version += 1

            return Task.from_dict(chosen.to_dict())

    def start(self, task_id: str, expected_version: int) -> bool:
        """Mark a claimed task as RUNNING. Returns False on version mismatch."""
        return self._transition(
            task_id, expected_version,
            {TaskStatus.CLAIMED}, TaskStatus.RUNNING,
        )

    def complete(self, task_id: str, expected_version: int) -> bool:
        """Mark a running task as COMPLETED. Removes seen hash on success."""
        with self._lock:
            task = self._tasks.get(task_id)
            if task is None or task.version != expected_version:
                return False
            if task.status != TaskStatus.RUNNING:
                return False
            task.status = TaskStatus.COMPLETED
            task.version += 1
            self._seen_hashes.pop(task.content_hash(), None)
            return True

    def fail(self, task_id: str, error: str, expected_version: int) -> bool:
        """Fail a running task.

        If retries remain: resets to PENDING.
        If retries exhausted: marks FAILED and removes seen hash.
        """
        with self._lock:
            task = self._tasks.get(task_id)
            if task is None or task.version != expected_version:
                return False
            if task.status != TaskStatus.RUNNING:
                return False
            task.error = error
            if task.retries < task.max_retries:
                task.retries += 1
                task.status = TaskStatus.PENDING
                task.assigned_worker = ""
                task.version += 1
            else:
                task.status = TaskStatus.FAILED
                task.version += 1
                self._seen_hashes.pop(task.content_hash(), None)
            return True

    def cancel(self, task_id: str) -> bool:
        """Cancel a task. Removes seen hash on success."""
        with self._lock:
            task = self._tasks.get(task_id)
            if task is None:
                return False
            if task.status in (TaskStatus.COMPLETED, TaskStatus.CANCELLED):
                return False
            task.status = TaskStatus.CANCELLED
            task.version += 1
            self._seen_hashes.pop(task.content_hash(), None)
            return True

    def clear(self) -> int:
        """Remove all tasks and seen hashes. Returns the count removed."""
        with self._lock:
            count = len(self._tasks)
            self._tasks.clear()
            self._seen_hashes.clear()
        return count

    def clear_non_terminal(self) -> int:
        """Remove pending/claimed/running tasks, preserve terminal ones.

        Also cleans their seen hashes.
        """
        terminal = {TaskStatus.COMPLETED, TaskStatus.FAILED, TaskStatus.CANCELLED}
        with self._lock:
            to_remove = [tid for tid, t in self._tasks.items()
                         if t.status not in terminal]
            for tid in to_remove:
                task = self._tasks[tid]
                self._seen_hashes.pop(task.content_hash(), None)
                del self._tasks[tid]
        return len(to_remove)

    # ── reads ───────────────────────────────────────────────────────────

    def get(self, task_id: str) -> Optional[Task]:
        """Get a read-only copy of a task."""
        with self._lock:
            task = self._tasks.get(task_id)
            return task if task is None else Task.from_dict(task.to_dict())

    def count(self, status: Optional[TaskStatus] = None) -> int:
        """Count tasks, optionally filtered by status."""
        with self._lock:
            if status is None:
                return len(self._tasks)
            return sum(1 for t in self._tasks.values() if t.status == status)

    def get_status(self) -> dict:
        """Returns a snapshot of queue state for reporting."""
        with self._lock:
            counts = {}
            total = len(self._tasks)
            for t in self._tasks.values():
                counts[t.status.value] = counts.get(t.status.value, 0) + 1
            return {
                "total": total,
                "status_counts": counts,
                "progress": f"{counts.get('completed', 0)}/{total}" if total else "0/0",
            }

    # ── serialization ──────────────────────────────────────────────────

    def to_dict(self) -> dict:
        """Serialise entire queue for checkpoint."""
        with self._lock:
            return {
                "tasks": {tid: t.to_dict() for tid, t in self._tasks.items()},
                "seen_hashes": dict(self._seen_hashes),
            }

    @classmethod
    def from_dict(cls, d: dict) -> TaskQueue:
        """Deserialise a checkpointed queue."""
        q = cls()
        q._tasks = {tid: Task.from_dict(td) for tid, td in d.get("tasks", {}).items()}
        q._seen_hashes = dict(d.get("seen_hashes", {}))
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
