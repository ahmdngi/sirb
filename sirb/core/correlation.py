"""Cross-vessel correlation engine.

Analyses findings across multiple vessels to surface patterns that no
single-vessel report would reveal — shared managers, same VSAT provider,
fleet-wide vulnerabilities, shadow fleet clusters.
"""

from __future__ import annotations

from collections import Counter, defaultdict
from typing import Any

from .models import Finding


class CorrelationEngine:
    """Correlate findings across vessels to identify fleet/port patterns."""

    def __init__(self):
        self._findings: list[Finding] = []

    def ingest(self, findings: list[Finding]):
        """Load findings for correlation analysis."""
        self._findings = findings

    def add(self, finding: Finding):
        self._findings.append(finding)

    # ── correlations ────────────────────────────────────────────────────

    def shared_attributes(self, attr: str = "flag") -> list[dict]:
        """Find vessels sharing the same attribute value.

        E.g. ``shared_attributes("flag")`` → vessels sharing a flag.
        E.g. ``shared_attributes("destination")`` → same destination port.
        """
        groups: dict[str, list[str]] = defaultdict(list)

        for f in self._findings:
            # Check detail dict for the attribute
            val = f.detail.get(attr)
            if val:
                groups[str(val)].append(f.target_id)

        return [
            {"value": val, "vessels": list(set(ids)), "count": len(set(ids))}
            for val, ids in groups.items()
            if len(set(ids)) > 1
        ]

    def shared_management(self) -> list[dict]:
        """Find vessels under the same manager/owner."""
        managers: dict[str, set[str]] = defaultdict(set)

        for f in self._findings:
            if f.finding_type == "ownership":
                manager = f.detail.get("manager", f.detail.get("owner", ""))
                if manager:
                    managers[manager].add(f.target_id)

        return [
            {"manager": mgr, "vessels": sorted(vs), "count": len(vs)}
            for mgr, vs in managers.items()
            if len(vs) > 1
        ]

    def same_shadow_fleet_indicators(self) -> list[dict]:
        """Cluster vessels with shadow fleet indicators."""
        clusters: dict[str, list[str]] = defaultdict(list)

        for f in self._findings:
            if f.finding_type in ("shadow_fleet_flag", "no_pi_insurance"):
                key = f.finding_type
                clusters[key].append(f.target_id)

        return [
            {"indicator": key, "vessels": list(set(ids)), "count": len(set(ids))}
            for key, ids in clusters.items()
        ]

    def shared_vsat(self) -> list[dict]:
        """Find vessels with the same VSAT provider/model exposed on Shodan."""
        vsat: dict[str, set[str]] = defaultdict(set)

        for f in self._findings:
            if f.finding_type == "exposed_service":
                products = f.detail.get("products", [])
                for p in products:
                    if any(kw in p.upper() for kw in ["VSAT", "SAILOR", "KVH",
                                                        "COBHAM", "INTELLIAN"]):
                        vsat[p].add(f.target_id)

        return [
            {"product": prod, "vessels": sorted(vs), "count": len(vs)}
            for prod, vs in vsat.items()
            if len(vs) > 1
        ]

    def severity_summary(self) -> dict[str, int]:
        """Count findings by severity across all vessels."""
        counts: dict[str, int] = Counter()
        for f in self._findings:
            counts[f.severity] += 1
        return dict(counts)

    def finding_type_summary(self) -> dict[str, int]:
        """Count findings by type."""
        counts: dict[str, int] = Counter()
        for f in self._findings:
            counts[f.finding_type] += 1
        return dict(counts)

    def vessel_count(self) -> int:
        """Number of unique vessels in the findings."""
        return len({f.target_id for f in self._findings if f.target_id})

    def risk_tiers(self) -> dict[str, int]:
        """Count vessels by highest severity finding per vessel."""
        tiers: dict[str, str] = {}

        for f in self._findings:
            tid = f.target_id
            severity = f.severity
            priority = {"critical": 4, "high": 3, "medium": 2, "low": 1, "info": 0}

            if tid not in tiers or priority.get(severity, 0) > priority.get(tiers.get(tid, "info"), 0):
                tiers[tid] = severity

        result: dict[str, int] = Counter()
        for t in tiers.values():
            result[t] += 1
        return dict(result)

    def all_correlations(self) -> dict[str, Any]:
        """Run all correlations and return a combined report."""
        return {
            "vessel_count": self.vessel_count(),
            "severity_summary": self.severity_summary(),
            "risk_tiers": self.risk_tiers(),
            "finding_type_summary": self.finding_type_summary(),
            "shared_attributes": {
                "flag": self.shared_attributes("flag"),
                "destination": self.shared_attributes("destination"),
            },
            "shared_management": self.shared_management(),
            "shared_vsat": self.shared_vsat(),
            "shadow_fleet_clusters": self.same_shadow_fleet_indicators(),
        }

    def clear(self):
        self._findings.clear()
