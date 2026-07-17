"""Router — dispatches tasks to the correct worker."""

from __future__ import annotations

from typing import Optional

from .models import Task, Result, TaskStatus
from .registry import WorkerRegistry
from .worker_base import SirbWorker


class Router:
    """Routes tasks to workers by matching ``Task.worker`` to ``SirbWorker.name``."""

    def __init__(self, registry: WorkerRegistry):
        self._registry = registry

    def route(self, task: Task) -> Optional[SirbWorker]:
        """Resolve a task to its worker instance.

        Returns None if no worker is registered for ``task.worker``.
        """
        if not task.worker:
            return None
        return self._registry.get(task.worker)

    def validate_task(self, task: Task) -> str | None:
        """Check that a task can be routed. Returns error string or None."""
        if not task.worker:
            return "task.worker is empty — no target worker specified"
        if task.worker not in self._registry:
            return f"no worker registered for '{task.worker}'"
        if not task.type:
            return "task.type is empty"
        return None

    def available_workers(self) -> list[str]:
        """List all registered worker names."""
        return list(self._registry.keys())
