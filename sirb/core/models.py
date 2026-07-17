"""Agnostic data models for Sirb swarm framework."""

from __future__ import annotations

import uuid
from dataclasses import dataclass, field
from enum import Enum
from typing import Any


class TaskStatus(str, Enum):
    PENDING = "pending"
    CLAIMED = "claimed"
    RUNNING = "running"
    COMPLETED = "completed"
    FAILED = "failed"
    CANCELLED = "cancelled"


@dataclass
class Task:
    """A unit of work for a SirbWorker to execute."""

    id: str = field(default_factory=lambda: uuid.uuid4().hex[:12])
    type: str = ""           # e.g. "scan", "recon", "monitor"
    worker: str = ""         # routes to SirbWorker.name
    params: dict = field(default_factory=dict)
    priority: int = 1        # 0 = highest
    depends_on: list[str] = field(default_factory=list)
    created_at: float = 0.0
    max_retries: int = 3

    # Internal state (managed by TaskQueue)
    status: TaskStatus = TaskStatus.PENDING
    version: int = 0
    assigned_worker: str = ""
    retries: int = 0
    error: str = ""

    def to_dict(self) -> dict:
        return {
            "id": self.id,
            "type": self.type,
            "worker": self.worker,
            "params": self.params,
            "priority": self.priority,
            "depends_on": self.depends_on,
            "created_at": self.created_at,
            "max_retries": self.max_retries,
            "status": self.status.value,
            "version": self.version,
            "assigned_worker": self.assigned_worker,
            "retries": self.retries,
            "error": self.error,
        }

    @classmethod
    def from_dict(cls, d: dict) -> Task:
        t = cls(
            id=d["id"],
            type=d.get("type", ""),
            worker=d.get("worker", ""),
            params=d.get("params", {}),
            priority=d.get("priority", 1),
            depends_on=d.get("depends_on", []),
            created_at=d.get("created_at", 0.0),
            max_retries=d.get("max_retries", 3),
        )
        t.status = TaskStatus(d.get("status", "pending"))
        t.version = d.get("version", 0)
        t.assigned_worker = d.get("assigned_worker", "")
        t.retries = d.get("retries", 0)
        t.error = d.get("error", "")
        return t


@dataclass
class Finding:
    """A structured finding written to the blackboard."""

    id: str = field(default_factory=lambda: uuid.uuid4().hex[:12])
    target_id: str = ""
    target_type: str = ""
    finding_type: str = ""
    severity: str = "medium"  # critical | high | medium | low | info
    weight: float = 1.0
    detail: dict = field(default_factory=dict)
    source: str = ""
    worker: str = ""
    created_at: float = 0.0

    def to_dict(self) -> dict:
        return {
            "id": self.id,
            "target_id": self.target_id,
            "target_type": self.target_type,
            "finding_type": self.finding_type,
            "severity": self.severity,
            "weight": self.weight,
            "detail": self.detail,
            "source": self.source,
            "worker": self.worker,
            "created_at": self.created_at,
        }

    @classmethod
    def from_dict(cls, d: dict) -> Finding:
        return cls(
            id=d.get("id", ""),
            target_id=d.get("target_id", ""),
            target_type=d.get("target_type", ""),
            finding_type=d.get("finding_type", ""),
            severity=d.get("severity", "medium"),
            weight=d.get("weight", 1.0),
            detail=d.get("detail", {}),
            source=d.get("source", ""),
            worker=d.get("worker", ""),
            created_at=d.get("created_at", 0.0),
        )


@dataclass
class Result:
    """Output from a single task execution."""

    task_id: str = ""
    worker: str = ""
    status: str = "success"  # success | partial | failure
    findings: list[Finding] = field(default_factory=list)
    artifacts: list[str] = field(default_factory=list)
    raw: dict = field(default_factory=dict)
    error: str = ""

    def to_dict(self) -> dict:
        return {
            "task_id": self.task_id,
            "worker": self.worker,
            "status": self.status,
            "findings": [f.to_dict() for f in self.findings],
            "artifacts": self.artifacts,
            "raw": self.raw,
            "error": self.error,
        }

    @classmethod
    def from_dict(cls, d: dict) -> Result:
        return cls(
            task_id=d.get("task_id", ""),
            worker=d.get("worker", ""),
            status=d.get("status", "success"),
            findings=[Finding.from_dict(f) for f in d.get("findings", [])],
            artifacts=d.get("artifacts", []),
            raw=d.get("raw", {}),
            error=d.get("error", ""),
        )
