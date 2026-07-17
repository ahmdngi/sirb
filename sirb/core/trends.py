"""Multi-run trend tracking — compare assessments across runs."""

from __future__ import annotations

import json
import os
import time
from pathlib import Path
from typing import Any, Optional


class TrendTracker:
    """Persist and compare assessment summaries across runs.

    Each run's assessment summary is saved as ``assessment-summary.json``
    alongside the existing ``assessment.md``. The tracker can then compare
    the latest run against previous runs to show trends.

    Usage::

        tracker = TrendTracker(runs_dir="~/.sirb/runs")
        tracker.save_summary(run_id, assessment)
        prev_summaries = tracker.previous_summaries(run_id)
        delta = tracker.delta(latest, prev_summaries[0])  # if any
    """

    SUMMARY_FILENAME = "assessment-summary.json"

    def __init__(self, runs_dir: str):
        self._runs_dir = Path(runs_dir).expanduser().resolve()

    # ── save ─────────────────────────────────────────────────────────────

    def save_summary(self, run_id: str, assessment: dict[str, Any]):
        """Save a compact summary of an assessment for comparison."""
        summary = self._compress(assessment)
        summary["run_id"] = run_id  # override with actual run directory name
        path = self._runs_dir / run_id / self.SUMMARY_FILENAME
        path.parent.mkdir(parents=True, exist_ok=True)
        with open(path, "w") as f:
            json.dump(summary, f, indent=2, default=str)

    # ── read ─────────────────────────────────────────────────────────────

    def load_summary(self, run_id: str) -> Optional[dict[str, Any]]:
        """Load a saved summary by run ID."""
        path = self._runs_dir / run_id / self.SUMMARY_FILENAME
        if not path.exists():
            return None
        with open(path) as f:
            return json.load(f)

    def load_all_summaries(self) -> list[dict[str, Any]]:
        """Load all run summaries sorted oldest-first."""
        if not self._runs_dir.exists():
            return []

        summaries = []
        for run_dir in sorted(self._runs_dir.iterdir()):
            if not run_dir.is_dir():
                continue
            summary = self.load_summary(run_dir.name)
            if summary:
                summaries.append(summary)
        return summaries

    def previous_summaries(self, current_run_id: str) -> list[dict[str, Any]]:
        """Return summaries of runs before the current one, newest-first."""
        all_s = sorted(self.load_all_summaries(),
                       key=lambda s: s.get("run_id", ""))
        current_index = None
        for i, s in enumerate(all_s):
            if s.get("run_id") == current_run_id:
                current_index = i
                break

        if current_index is None:
            return []

        # Return runs before current, newest-first
        return list(reversed(all_s[:current_index]))

    # ── delta ────────────────────────────────────────────────────────────

    def delta(self, latest: dict[str, Any],
              previous: dict[str, Any]) -> dict[str, Any]:
        """Compare latest assessment against a previous one.

        Returns a dict with:
            - ``severity_deltas``: {severity: delta_count}
            - ``finding_type_deltas``: {finding_type: delta_count}
            - ``target_count_change``: int
            - ``new_severity_tiers``: {severity: count_in_latest_only}
            - ``summary``: human-readable string
        """
        sev_latest = latest.get("severity", {})
        sev_prev = previous.get("severity", {})

        severity_deltas = {}
        for k in set(list(sev_latest.keys()) + list(sev_prev.keys())):
            diff = sev_latest.get(k, 0) - sev_prev.get(k, 0)
            if diff != 0:
                severity_deltas[k] = diff

        ft_latest = latest.get("finding_types", {})
        ft_prev = previous.get("finding_types", {})

        finding_type_deltas = {}
        for k in set(list(ft_latest.keys()) + list(ft_prev.keys())):
            diff = ft_latest.get(k, 0) - ft_prev.get(k, 0)
            if diff != 0:
                finding_type_deltas[k] = diff

        latest_count = latest.get("unique_targets", 0)
        prev_count = previous.get("unique_targets", 0)
        target_count_change = latest_count - prev_count

        new_severity_tiers = {}
        rt_latest = latest.get("risk_tiers", {})
        rt_prev = previous.get("risk_tiers", {})
        for k, v in rt_latest.items():
            new_severity_tiers[k] = max(0, v - rt_prev.get(k, 0))

        return {
            "severity_deltas": severity_deltas,
            "finding_type_deltas": finding_type_deltas,
            "target_count_change": target_count_change,
            "new_severity_tiers": new_severity_tiers,
            "total_targets": latest_count,
            "has_change": bool(severity_deltas or finding_type_deltas
                               or target_count_change),
        }

    def render_delta_markdown(self, delta: dict[str, Any],
                              run_id: str) -> str:
        """Render a delta as a compact markdown summary."""
        if not delta.get("has_change"):
            return f"_No changes since previous run ({run_id})._"

        lines = [f"### Trends vs previous run",
                 f""]

        tc = delta.get("target_count_change", 0)
        if tc > 0:
            lines.append(f"- **+{tc}** new targets this run")
        elif tc < 0:
            lines.append(f"- **{tc}** fewer targets this run")

        sev_deltas = delta.get("severity_deltas", {})
        for sev in ["critical", "high", "medium", "low", "info"]:
            d = sev_deltas.get(sev, 0)
            if d > 0:
                lines.append(f"- **+{d}** {sev} severity findings")
            elif d < 0:
                lines.append(f"- **{d}** {sev} severity findings")

        ft_deltas = delta.get("finding_type_deltas", {})
        if ft_deltas:
            lines.append("")
            lines.append("| Finding Type | Change |")
            lines.append("|--------------|--------|")
            for ft, d in sorted(ft_deltas.items()):
                sign = "+" if d > 0 else ""
                lines.append(f"| {ft} | {sign}{d} |")

        return "\n".join(lines)

    # ── internal ─────────────────────────────────────────────────────────

    def _compress(self, assessment: dict[str, Any]) -> dict[str, Any]:
        """Extract only the fields needed for trend comparison."""
        return {
            "run_id": assessment.get("generated_at", str(time.time())),
            "generated_at": assessment.get("generated_at", ""),
            "unique_targets": assessment.get("unique_targets", 0),
            "severity": assessment.get("severity", {}),
            "finding_types": assessment.get("finding_types", {}),
            "risk_tiers": assessment.get("risk_tiers", {}),
        }
