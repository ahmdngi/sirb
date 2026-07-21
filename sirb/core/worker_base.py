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

    def input_schema(self) -> dict:
        """Declare the dashboard input form schema for this worker.

        Returns a dict with:
        - ``fields``: list of field definitions, each with:
          - ``name``: param key passed to Task.params
          - ``label``: display label
          - ``type``: "text" | "textarea" | "select" | "number"
          - ``placeholder`` (optional): input placeholder
          - ``options`` (optional): list of {value, label} for select type
          - ``default`` (optional): default value
        - ``parse``: bool — if True, raw input is parsed via parse_targets()
          and a preview is shown before running

        Example::

            {
                "fields": [
                    {"name": "targets", "label": "Vessels (IMO/MMSI)",
                     "type": "textarea", "placeholder": "9297890, IMO 9299678, ...",
                     "parse": True},
                    {"name": "mode", "label": "Mode", "type": "select",
                     "options": [{"value": "fast", "label": "Fast (~2min)"},
                                 {"value": "deep", "label": "Deep (~10min)"}],
                     "default": "deep"},
                ],
                "parse": True
            }

        Return empty dict (default) if the worker doesn't support
        dashboard input.
        """
        return {}

    def parse_targets(self, raw_input: str) -> list[dict]:
        """Parse raw user input into structured targets.

        Called by the dashboard when the user clicks "Parse" before running.
        Returns a list of target dicts, each with at minimum:
        - ``target``: the target identifier (IMO, MMSI, name, email, etc.)
        - ``type``: target type label for display

        Example::

            parse_targets("9297890, IMO 9299678")
            → [{"target": "9297890", "type": "IMO"},
               {"target": "9299678", "type": "IMO"}]

        Return empty list (default) if parsing is not supported.
        """
        return []

    def stats_schema(self) -> list[dict]:
        """Declare the stats bar fields this worker produces.

        Returns a list of stat definitions, each with:
        - ``key``: stat key in the stats dict returned by extract_stats()
        - ``icon``: emoji icon for the stats bar
        - ``label``: display label

        Example::

            [
                {"key": "tool_calls", "icon": "⚙️", "label": "Tool Calls"},
                {"key": "shodan", "icon": "🛰️", "label": "Shodan"},
            ]

        The dashboard renders these dynamically. If empty (default),
        the dashboard falls back to a basic stats bar.

        Common agnostic stats (always available): tool_calls, duration, model.
        Worker-specific stats (shodan, phases, etc.) are defined here.
        """
        return [
            {"key": "tool_calls", "icon": "⚙️", "label": "Tool Calls"},
            {"key": "duration", "icon": "⏱", "label": "Duration"},
            {"key": "model", "icon": "🧠", "label": "Model"},
        ]

    def report_tabs(self, run_dir: str) -> list[dict]:
        """Declare report tabs for a completed run.

        Called by the dashboard when a run is selected. Returns a list
        of tab definitions, each with:
        - ``id``: unique tab identifier (used in switchTab)
        - ``label``: display label (shown on the tab button)
        - ``icon``: emoji icon
        - ``type``: "file" (serves a file) or "per_target" (one sub-tab per target)
        - ``path``: for type="file", path relative to run_dir (e.g. "swarm-report.md")
        - ``endpoint``: for type="per_target", API path to list targets

        Example::

            [
                {"id": "swarm", "label": "Swarm", "icon": "📋", "type": "file",
                 "path": "swarm-report.md"},
                {"id": "connections", "label": "Connections", "icon": "🔗", "type": "file",
                 "path": "connections.md"},
                {"id": "vessel", "label": "Vessels", "icon": "🚢", "type": "per_target",
                 "endpoint": "/run/{rid}/targets"},
            ]

        The dashboard renders these as tab buttons. For per_target tabs,
        sub-tabs are created for each target returned by the endpoint.

        Return empty list (default) if the worker doesn't produce report tabs.
        """
        return []

    def extract_stats(self, log_path: str, report_dir: str) -> dict:
        """Extract stats from agent log and report files.

        Called by the dashboard after a run completes. Returns a dict
        mapping stat keys (from stats_schema()) to values.

        Args:
            log_path: path to the agent's log file (full hermes output)
            report_dir: path to the agent's report directory

        Returns:
            Dict of stat values, e.g. {"tool_calls": 45, "shodan": 4, ...}

        Default implementation extracts generic stats from the hermes
        session summary. Override to add worker-specific stats.
        """
        import re
        from pathlib import Path

        stats = {}
        log = Path(log_path)
        text = log.read_text(errors="replace") if log.exists() else ""

        # tool_calls from session summary
        m = re.search(r"Messages:\s*\d+\s*\([^)]*?(\d+)\s+tool calls?\)", text)
        stats["tool_calls"] = int(m.group(1)) if m else 0

        # duration
        m = re.search(r"Duration:\s*(\d+m\s+\d+s|\d+s)", text)
        stats["duration"] = m.group(1) if m else "—"

        # model
        m = re.search(r"Model:\s*(\S+)", text)
        stats["model"] = m.group(1) if m else "—"

        return stats

    @property
    def config_schema(self) -> dict[str, Any] | None:
        """Optional JSON Schema for this worker's config section in sirb.yml.

        When set, the registry validates the worker's config against this
        schema at startup.
        """
        return None
