"""Cross-finding correlation engine.

Groups findings by shared attributes to surface patterns across targets.
Completely agnostic — works on any Finding regardless of domain.
"""

from __future__ import annotations

from collections import Counter, defaultdict
from typing import Any

from .models import Finding


class CorrelationEngine:
    """Correlate findings across targets to identify shared patterns.

    Usage::

        engine = CorrelationEngine()
        engine.ingest(findings)

        # Count by severity
        engine.count_by("severity")
        # => {"critical": 5, "high": 3, "info": 10}

        # Group by any detail key across all findings
        engine.group_by_detail_key("flag")
        # => [{"value": "palau", "targets": ["A","B"], "count": 2}]
    """

    def __init__(self):
        self._findings: list[Finding] = []

    def ingest(self, findings: list[Finding]):
        """Load findings for correlation analysis."""
        self._findings = findings

    def add(self, finding: Finding):
        self._findings.append(finding)

    # ── generic correlations ────────────────────────────────────────────

    def count_by(self, field: str = "severity") -> dict[str, int]:
        """Count findings grouped by a field value.

        Args:
            field: ``"severity"``, ``"finding_type"``, ``"source"``, etc.

        Returns:
            ``{"critical": 5, "high": 3, ...}``
        """
        counts: dict[str, int] = Counter()
        for f in self._findings:
            val = getattr(f, field, None)
            if val:
                counts[str(val)] += 1
        return dict(counts)

    def count_by_type(self) -> dict[str, int]:
        """Shorthand for count_by('finding_type')."""
        return self.count_by("finding_type")

    def count_by_severity(self) -> dict[str, int]:
        """Shorthand for count_by('severity')."""
        return self.count_by("severity")

    def unique_targets(self) -> int:
        """Number of unique target IDs in the findings."""
        return len({f.target_id for f in self._findings if f.target_id})

    def group_by_detail_key(self, key: str) -> list[dict]:
        """Group findings that share the same value in ``detail[key]``.

        Args:
            key: Key inside each finding's ``detail`` dict, e.g. ``\"flag\"``,
                 ``\"destination\"``, ``\"product\"``.

        Returns:
            List of ``{\"value\": ..., \"targets\": [...], \"count\": N}``
            Only groups with **more than one target** are returned.
        """
        groups: dict[str, set[str]] = defaultdict(set)

        for f in self._findings:
            val = f.detail.get(key)
            if val and f.target_id:
                groups[str(val)].add(f.target_id)

        return [
            {"value": val, "targets": sorted(ids), "count": len(ids)}
            for val, ids in groups.items()
            if len(ids) > 1
        ]

    def group_by_field(self, field: str) -> list[dict]:
        """Group findings that share the same value for a field.

        Args:
            field: ``\"source\"``, ``\"target_type\"``, etc.

        Returns:
            List of ``{\"value\": ..., \"targets\": [...], \"count\": N}``
        """
        groups: dict[str, set[str]] = defaultdict(set)

        for f in self._findings:
            val = getattr(f, field, None)
            if val and f.target_id:
                groups[str(val)].add(f.target_id)

        return [
            {"value": val, "targets": sorted(ids), "count": len(ids)}
            for val, ids in groups.items()
            if len(ids) > 1
        ]

    def risk_tiers(self, severity_field: str = "severity") -> dict[str, int]:
        """Count targets by their highest severity finding.

        Args:
            severity_field: The field name to use for severity ordering.
                            Must match the field on Finding.

        Returns:
            ``{\"critical\": 2, \"high\": 1, \"info\": 3}``
        """
        severity_order = {"critical": 4, "high": 3, "medium": 2,
                          "low": 1, "info": 0}
        tiers: dict[str, str] = {}

        for f in self._findings:
            tid = f.target_id
            sev = str(getattr(f, severity_field, "info"))
            if tid not in tiers or severity_order.get(sev, 0) > severity_order.get(tiers[tid], 0):
                tiers[tid] = sev

        result: dict[str, int] = Counter()
        for t in tiers.values():
            result[t] += 1
        return dict(result)

    def all_correlations(self) -> dict[str, Any]:
        """Run all generic correlations and return a combined report."""
        return {
            "unique_targets": self.unique_targets(),
            "severity": self.count_by_severity(),
            "finding_types": self.count_by_type(),
            "risk_tiers": self.risk_tiers(),
            "shared_sources": self.group_by_field("source"),
            "shared_target_types": self.group_by_field("target_type"),
        }

    def clear(self):
        self._findings.clear()
