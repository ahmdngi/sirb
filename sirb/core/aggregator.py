"""Aggregator — generates assessment markdown from findings + correlations.

Completely agnostic — works on any target type (device, service, person, etc.).
Uses generic labels; domain-specific context is the user's responsibility.
"""

from __future__ import annotations

import time
from pathlib import Path
from typing import Any

from .blackboard import Blackboard
from .correlation import CorrelationEngine
from .models import Finding

try:
    from jinja2 import Environment, FileSystemLoader
    _JINJA_AVAILABLE = True
except ImportError:
    _JINJA_AVAILABLE = False

_TEMPLATES_DIR = Path(__file__).parent.parent / "templates"


class Aggregator:
    """Generates a structured assessment from the blackboard and correlations.

    Can produce assessments for any target type depending on what findings
    exist in the blackboard.
    """

    def __init__(self):
        self._correlation = CorrelationEngine()

    def assess(self, blackboard: Blackboard,
               target_type: str = "") -> dict[str, Any]:
        """Run a full assessment on findings in the blackboard.

        Args:
            blackboard: The findings store.
            target_type: Optional filter — only assess findings for this
                         target type. Empty string = all types.

        Returns:
            Dict with assessment data, ready to be rendered as markdown.
        """
        if target_type:
            findings = blackboard.query(target_type=target_type)
        else:
            findings = blackboard.all()

        self._correlation.ingest(findings)
        correlations = self._correlation.all_correlations()

        return {
            "generated_at": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
            "target_type": target_type or "all",
            **correlations,
            "top_findings": self._top_findings(findings, limit=10),
            "exposure_stats": self._exposure_stats(findings),
        }

    def render_markdown(self, assessment: dict[str, Any],
                        title: str = "Assessment") -> str:
        """Render an assessment dict as markdown with generic labels.

        Uses a Jinja2 template (templates/assessment.j2) when available for
        consistent formatting. Falls back to the inline string builder if
        jinja2 is not installed.
        """
        if _JINJA_AVAILABLE and (_TEMPLATES_DIR / "assessment.j2").exists():
            env = Environment(
                loader=FileSystemLoader(str(_TEMPLATES_DIR)),
                autoescape=False,
                trim_blocks=True,
                lstrip_blocks=True,
                keep_trailing_newline=True,
            )
            template = env.get_template("assessment.j2")
            return template.render(assessment=assessment, title=title)

        # Fallback: inline string builder (no jinja2 dependency)
        return self._render_markdown_inline(assessment, title)

    def _render_markdown_inline(self, assessment: dict[str, Any],
                                title: str = "Assessment") -> str:
        """Inline markdown renderer — fallback when jinja2 is unavailable."""
        lines = [
            f"# {title}",
            f"",
            f"**Generated:** {assessment['generated_at']}  ",
            f"**Target type:** {assessment['target_type']}  ",
            f"**Unique targets:** {assessment['unique_targets']}  ",
            f"",
            f"---",
            f"",
            f"## Overview",
            f"",
            f"| Metric | Value |",
            f"|--------|-------|",
        ]

        ut = assessment["unique_targets"]
        lines.append(f"| Unique targets | {ut} |")

        # Risk tiers
        tiers = assessment.get("risk_tiers", {})
        for sev in ["critical", "high", "medium", "low", "info"]:
            n = tiers.get(sev, 0)
            if n > 0:
                pct = round(n / ut * 100) if ut else 0
                lines.append(f"| {sev.capitalize()} severity targets | {n} ({pct}%) |")

        lines += [
            f"",
            f"## Severity Distribution",
            f"",
            f"| Severity | Count |",
            f"|----------|-------|",
        ]
        for sev, cnt in assessment.get("severity", {}).items():
            lines.append(f"| {sev.capitalize()} | {cnt} |")

        lines += [
            f"",
            f"## Finding Types",
            f"",
            f"| Type | Count |",
            f"|------|-------|",
        ]
        for ftype, cnt in assessment.get("finding_types", {}).items():
            lines.append(f"| {ftype} | {cnt} |")

        # Shared attributes (if any)
        shared_sources = assessment.get("shared_sources", [])
        if shared_sources:
            lines += [
                f"",
                f"## Shared Sources",
                f"",
            ]
            for s in shared_sources:
                lines.append(
                    f"- **{s['value']}**: {s['count']} targets — "
                    f"{', '.join(s['targets'][:5])}"
                )

        shared_types = assessment.get("shared_target_types", [])
        if shared_types:
            lines += [
                f"",
                f"## Shared Target Types",
                f"",
            ]
            for t in shared_types:
                lines.append(
                    f"- **{t['value']}**: {t['count']} targets"
                )

        lines += [
            f"",
            f"## Top Findings by Severity",
            f"",
            f"| Target | Type | Severity | Detail |",
            f"|--------|------|----------|--------|",
        ]
        for f in assessment.get("top_findings", []):
            detail = str(f.get("detail", {}))[:60]
            lines.append(
                f"| {f.get('target_id', '')[:20]} "
                f"| {f.get('finding_type', '')} "
                f"| {f.get('severity', '')} "
                f"| {detail} |"
            )

        return "\n".join(lines)

    # ── internal ────────────────────────────────────────────────────────

    def _exposure_stats(self, findings: list[Finding]) -> dict:
        """Generic count of positive vs informational findings per target."""
        positives: set[str] = set()
        all_targets: set[str] = set()

        for f in findings:
            if f.target_id:
                all_targets.add(f.target_id)
                if f.severity in ("critical", "high", "medium"):
                    positives.add(f.target_id)

        total = len(all_targets)
        return {
            "positive_findings": len(positives),
            "total_targets": total,
            "rate": round(len(positives) / total * 100) if total else 0,
        }

    def _top_findings(self, findings: list[Finding],
                      limit: int = 10) -> list[dict]:
        """Return the highest-severity findings."""
        severity_order = {"critical": 0, "high": 1, "medium": 2,
                          "low": 3, "info": 4}
        sorted_f = sorted(
            findings,
            key=lambda f: severity_order.get(f.severity, 99),
        )
        return [f.to_dict() for f in sorted_f[:limit]]
