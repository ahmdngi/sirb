"""Abstract base class for all Sirb workers."""

from __future__ import annotations

from abc import ABC, abstractmethod
from typing import Any

from .models import Task, Result


class SirbWorker(ABC):
    """Interface every Sirb worker must implement.

    Subclass this, register via the worker registry, and implement at minimum
    the ``execute()`` method.

    Workers are discovered and instantiated by the registry. Each worker
    instance handles one task at a time from the pool — stateless by design,
    but can hold config that was loaded at registration time.
    """

    # Unique identifier. Must match ``Task.worker`` values for routing.
    name: str = ""

    # Human-readable description shown in --list-workers
    description: str = ""

    async def discover(self) -> list[Task]:
        """Return tasks that Sirb should queue.

        Called by the discoverer component. If a worker has no autonomous
        discovery capability (e.g. it only processes injected tasks), return
        an empty list.
        """
        return []

    @abstractmethod
    async def execute(self, task: Task) -> Result:
        """Execute one task and return the result.

        This is the only required method. It receives a claimed task and
        must return a Result with findings, artifacts, and status.

        The worker should NOT persist anything to disk directly — return
        all data via Result so Sirb's blackboard and persistence layer
        can manage it consistently.
        """
        ...

    async def validate(self, result: Result) -> bool:
        """Validate a result before it is written to the blackboard.

        Return False to reject the result (task will be retried).
        Default implementation returns True for any non-error result.
        """
        return result.status != "failure"

    def rate_limits(self) -> dict[str, int]:
        """Declare rate limits this worker needs.

        Returns a dict of ``{resource_name: max_calls_per_minute}``.
        Sirb enforces these automatically with token buckets.

        Example::

            {"equasis": 4, "shodan": 30}

        Return an empty dict (default) if this worker has no rate limits.
        """
        return {}

    @property
    def config_schema(self) -> dict[str, Any] | None:
        """Optional JSON Schema for this worker's config section in sirb.yml.

        When set, the registry validates the worker's config against this
        schema at startup.
        """
        return None
