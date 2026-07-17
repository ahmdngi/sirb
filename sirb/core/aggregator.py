"""Aggregator — generates port/fleet assessment from findings + correlations."""

from __future__ import annotations

import time
from typing import Any, Optional

from .blackboard import Blackboard
from .correlation import CorrelationEngine
from .models import Finding


class Aggregator:
    """Generates a structured assessment from the blackboard and correlations.

    Can produce per-type assessments (port risk, fleet intelligence) depending
    on the ``target_type`` of findings in the blackboard.
    """

    def __init__(self, output_dir: str = ""):
        self._output_dir = output_dir
        self._correlation = CorrelationEngine()

    def assess(self, blackboard: Blackboard,
               target_type: str = "vessel") -> dict[str, Any]:
        """Run a full assessment on findings in the blackboard.

        Args:
            blackboard: The findings store.
            target_type: Filter to one target type ("vessel", "person", "port").

        Returns:
            Dict with assessment data, ready to be rendered as markdown.
        """
        findings = blackboard.query(target_type=target_type)
        self._correlation.ingest(findings)
        correlations = self._correlation.all_correlations()

        return {
            "generated_at": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
            "target_type": target_type,
            **correlations,
            "exposure_rate": self._exposure_rate(findings, correlations),
            "top_findings": self._top_findings(findings, limit=10),
        }

    def render_markdown(self, assessment: dict[str, Any],
                        title: str = "Port Assessment") -> str:
        """Render an assessment dict as markdown."""
        lines = [
            f"# {title}",
            f"",
            f"**Generated:** {assessment['generated_at']}  ",
            f"**Target type:** {assessment['target_type']}  ",
            f"**Vessels analysed:** {assessment['vessel_count']}  ",
            f"",
            f"---",
            f"",
            f"## Overview",
            f"",
            f"| Metric | Value |",
            f"|--------|-------|",
        ]

        vc = assessment["vessel_count"]
        lines.append(f"| Vessels analysed | {vc} |")

        # Risk tiers
        tiers = assessment.get("risk_tiers", {})
        for sev in ["critical", "high", "medium", "low", "info"]:
            n = tiers.get(sev, 0)
            if n > 0:
                pct = round(n / vc * 100) if vc else 0
                lines.append(f"| {sev.capitalize()} risk vessels | {n} ({pct}%) |")

        # Exposure rate
        exp = assessment.get("exposure_rate", {})
        if exp.get("exposed", 0) > 0:
            lines.append(f"| Vessels with Shodan exposure | {exp['exposed']} ({exp.get('rate', 0)}%) |")

        lines += [
            f"",
            f"## Severity Distribution",
            f"",
            f"| Severity | Count |",
            f"|----------|-------|",
        ]
        for sev, cnt in assessment.get("severity_summary", {}).items():
            lines.append(f"| {sev.capitalize()} | {cnt} |")

        lines += [
            f"",
            f"## Shadow Fleet Indicators",
            f"",
        ]
        shadow = assessment.get("shadow_fleet_clusters", [])
        if shadow:
            for c in shadow:
                lines.append(
                    f"- **{c['indicator']}**: {c['count']} vessels — "
                    f"{', '.join(c['vessels'][:5])}"
                )
        else:
            lines.append("No shadow fleet indicators detected.")

        lines += [
            f"",
            f"## Shared Infrastructure",
            f"",
        ]
        vsat = assessment.get("shared_vsat", [])
        if vsat:
            for v in vsat:
                lines.append(
                    f"- **{v['product']}**: {v['count']} vessels — "
                    f"{', '.join(v['vessels'][:5])}"
                )
        else:
            lines.append("No shared VSAT infrastructure detected.")

        mgmt = assessment.get("shared_management", [])
        if mgmt:
            lines += [
                f"",
                f"## Shared Management",
                f"",
            ]
            for m in mgmt:
                lines.append(
                    f"- **{m['manager']}**: {m['count']} vessels"
                )

        lines += [
            f"",
            f"## Top Findings",
            f"",
            f"| Target | Type | Severity | Detail |",
            f"|--------|------|----------|--------|",
        ]
        for f in assessment.get("top_findings", []):
            detail = str(f.get("detail", {}))[:60]
            lines.append(
                f"| {f.get('target_id', '')[:15]} "
                f"| {f.get('finding_type', '')} "
                f"| {f.get('severity', '')} "
                f"| {detail} |"
            )

        lines += [
            f"",
            f"---",
            f"",
            f"## Shared Flag Analysis",
            f"",
        ]
        flags = assessment.get("shared_attributes", {}).get("flag", [])
        if flags:
            for f in flags:
                lines.append(
                    f"- **{f['value']}**: {f['count']} vessels"
                )
        else:
            lines.append("No shared flags among analysed vessels.")

        return "\n".join(lines)

    # ── internal ────────────────────────────────────────────────────────

    def _exposure_rate(self, findings: list[Finding],
                       correlations: dict) -> dict:
        """Calculate what percentage of vessels have Shodan exposure."""
        exposed = set()
        all_vessels = set()

        for f in findings:
            if f.target_id:
                all_vessels.add(f.target_id)
                if f.finding_type == "shodan_exposure":
                    exposed.add(f.target_id)

        total = len(all_vessels)
        return {
            "exposed": len(exposed),
            "total": total,
            "rate": round(len(exposed) / total * 100) if total else 0,
        }

    def _top_findings(self, findings: list[Finding],
                      limit: int = 10) -> list[dict]:
        """Return the highest-severity findings."""
        severity_order = {"critical": 0, "high": 1, "medium": 2, "low": 3, "info": 4}
        sorted_f = sorted(
            findings,
            key=lambda f: severity_order.get(f.severity, 99),
        )
        return [f.to_dict() for f in sorted_f[:limit]]
