"""File persistence — checkpoint/resume for queue + blackboard."""

from __future__ import annotations

import json
import os
from pathlib import Path
from typing import Optional

from .models import Task
from .task_queue import TaskQueue
from .blackboard import Blackboard


class Checkpointer:
    """Save/load Sirb state to disk.

    Checkpoints the task queue (and optionally the blackboard) as JSON so
    that a crash mid-swarm can resume without losing completed work.
    """

    def __init__(self, output_dir: str, checkpoint_interval: int = 5):
        self._output_dir = Path(output_dir).expanduser().resolve()
        self._checkpoint_interval = checkpoint_interval
        self._runs_dir = self._output_dir / "runs"
        self._runs_dir.mkdir(parents=True, exist_ok=True)

    # ── checkpoint path helpers ─────────────────────────────────────────

    def _queue_path(self, run_id: str) -> Path:
        return self._runs_dir / run_id / "task_queue.json"

    def _blackboard_path(self, run_id: str) -> Path:
        return self._runs_dir / run_id / "blackboard.json"

    # ── save ────────────────────────────────────────────────────────────

    def save_queue(self, run_id: str, queue: TaskQueue):
        """Checkpoint the task queue."""
        path = self._queue_path(run_id)
        path.parent.mkdir(parents=True, exist_ok=True)
        with open(path, "w") as f:
            json.dump(queue.to_dict(), f, indent=2)

    def save_blackboard(self, run_id: str, blackboard: Blackboard):
        """Checkpoint the blackboard."""
        path = self._blackboard_path(run_id)
        path.parent.mkdir(parents=True, exist_ok=True)
        with open(path, "w") as f:
            json.dump(blackboard.to_dict(), f, indent=2)

    def save_all(self, run_id: str, queue: TaskQueue,
                 blackboard: Optional[Blackboard] = None):
        """Checkpoint both."""
        self.save_queue(run_id, queue)
        if blackboard:
            self.save_blackboard(run_id, blackboard)

    # ── load ────────────────────────────────────────────────────────────

    def load_queue(self, run_id: str) -> Optional[TaskQueue]:
        """Load a checkpointed task queue. Returns None if not found."""
        path = self._queue_path(run_id)
        if not path.exists():
            return None
        with open(path) as f:
            return TaskQueue.from_dict(json.load(f))

    def load_blackboard(self, run_id: str) -> Optional[Blackboard]:
        """Load a checkpointed blackboard. Returns None if not found."""
        path = self._blackboard_path(run_id)
        if not path.exists():
            return None
        with open(path) as f:
            return Blackboard.from_dict(json.load(f))

    # ── listing ─────────────────────────────────────────────────────────

    def list_runs(self) -> list[dict]:
        """List completed/in-progress runs with metadata."""
        runs = []
        if not self._runs_dir.exists():
            return runs

        for run_dir in sorted(self._runs_dir.iterdir()):
            if not run_dir.is_dir():
                continue
            qp = run_dir / "task_queue.json"
            bp = run_dir / "blackboard.json"
            runs.append({
                "run_id": run_dir.name,
                "path": str(run_dir),
                "has_queue": qp.exists(),
                "has_blackboard": bp.exists(),
            })

        return runs

    def should_checkpoint(self, completed_count: int) -> bool:
        """Returns True if enough tasks have completed since last checkpoint."""
        return completed_count > 0 and completed_count % self._checkpoint_interval == 0
