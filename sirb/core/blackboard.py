"""Blackboard — shared findings store with pheromone decay."""

from __future__ import annotations

import json
import threading
import time
from typing import Optional

from .models import Finding


class Blackboard:
    """Thread-safe shared findings store.

    Workers write findings here. The aggregator reads from here.
    Pheromone decay prevents stale findings from dominating attention.
    """

    def __init__(self, decay_rate: float = 0.9, check_interval: int = 60):
        self._findings: dict[str, Finding] = {}
        self._triggers: list[dict] = []
        self._lock = threading.Lock()
        self._decay_rate = decay_rate
        self._check_interval = check_interval

    def add(self, finding: Finding):
        """Add a finding to the blackboard."""
        with self._lock:
            self._findings[finding.id] = finding

    def add_many(self, findings: list[Finding]):
        """Bulk-add findings."""
        with self._lock:
            for f in findings:
                self._findings[f.id] = f

    def get(self, finding_id: str) -> Optional[Finding]:
        """Get a finding by ID."""
        with self._lock:
            f = self._findings.get(finding_id)
            return Finding.from_dict(f.to_dict()) if f else None

    def query(self, target_type: str = None, target_id: str = None,
              finding_type: str = None, severity: str = None,
              worker: str = None, limit: int = 100) -> list[Finding]:
        """Query findings with optional filters."""
        with self._lock:
            results = []
            for f in self._findings.values():
                if target_type and f.target_type != target_type:
                    continue
                if target_id and f.target_id != target_id:
                    continue
                if finding_type and f.finding_type != finding_type:
                    continue
                if severity and f.severity != severity:
                    continue
                if worker and f.worker != worker:
                    continue
                results.append(Finding.from_dict(f.to_dict()))
                if len(results) >= limit:
                    break
            return results

    def all(self) -> list[Finding]:
        """Return all findings."""
        with self._lock:
            return [Finding.from_dict(f.to_dict()) for f in self._findings.values()]

    def count(self) -> int:
        """Total number of findings."""
        with self._lock:
            return len(self._findings)

    def decay(self):
        """Apply pheromone decay to all findings.

        Called periodically by the decay runner. Findings whose weight drops
        below 0.1 are pruned.
        """
        with self._lock:
            to_remove = []
            for fid, f in self._findings.items():
                f.weight *= self._decay_rate
                if f.weight < 0.1:
                    to_remove.append(fid)
            for fid in to_remove:
                del self._findings[fid]

    def register_trigger(self, predicate: dict, action: str):
        """Register a trigger predicate.

        Args:
            predicate: Filter dict like ``{"severity": "critical", "source": "shodan"}``
            action: Action identifier like ``"wake_aggregator"``
        """
        with self._lock:
            self._triggers.append({"predicate": predicate, "action": action})

    def check_triggers(self, finding: Finding) -> list[str]:
        """Check a finding against registered triggers. Returns matching actions."""
        actions = []
        with self._lock:
            for trigger in self._triggers:
                p = trigger["predicate"]
                matches = all(
                    getattr(finding, k, None) == v
                    for k, v in p.items()
                )
                if matches:
                    actions.append(trigger["action"])
        return actions

    def to_dict(self) -> dict:
        """Serialise for checkpoint."""
        with self._lock:
            return {
                "findings": {fid: f.to_dict() for fid, f in self._findings.items()},
                "decay_rate": self._decay_rate,
                "triggers": self._triggers,
            }

    @classmethod
    def from_dict(cls, d: dict) -> Blackboard:
        """Deserialise from checkpoint."""
        bb = cls(
            decay_rate=d.get("decay_rate", 0.9),
        )
        for fid, fdata in d.get("findings", {}).items():
            bb._findings[fid] = Finding.from_dict(fdata)
        bb._triggers = d.get("triggers", [])
        return bb
