"""Tests for correlation engine and aggregator."""

import pytest

from sirb.core import (
    CorrelationEngine, Aggregator, Blackboard, Finding,
)


# ── Fixtures ────────────────────────────────────────────────────────────

@pytest.fixture
def sample_findings():
    """Various findings across 4 vessels for correlation testing."""
    return [
        # Vessel A — critical: shadow flag, exposed VSAT
        Finding(target_id="VESSEL_A", target_type="vessel",
                finding_type="shadow_fleet_flag", severity="critical",
                detail={"flag": "palau"}, source="equasis"),
        Finding(target_id="VESSEL_A", target_type="vessel",
                finding_type="exposed_service", severity="high",
                detail={"products": ["SAILOR 900 VSAT"]}, source="shodan"),
        Finding(target_id="VESSEL_A", target_type="vessel",
                finding_type="no_pi_insurance", severity="high",
                detail={"pi": "Unknown"}, source="equasis"),
        Finding(target_id="VESSEL_A", target_type="vessel",
                finding_type="current_position", severity="info",
                detail={"destination": "TALLINN", "flag": "palau"}),

        # Vessel B — shadow flag, same VSAT, also heading to TALLINN
        Finding(target_id="VESSEL_B", target_type="vessel",
                finding_type="shadow_fleet_flag", severity="critical",
                detail={"flag": "palau"}, source="equasis"),
        Finding(target_id="VESSEL_B", target_type="vessel",
                finding_type="exposed_service", severity="high",
                detail={"products": ["SAILOR 900 VSAT"]}, source="shodan"),
        Finding(target_id="VESSEL_B", target_type="vessel",
                finding_type="no_pi_insurance", severity="high",
                detail={"pi": "Unknown"}, source="equasis"),
        Finding(target_id="VESSEL_B", target_type="vessel",
                finding_type="current_position", severity="info",
                detail={"destination": "TALLINN", "flag": "palau"}),

        # Vessel C — clean, no exposure
        Finding(target_id="VESSEL_C", target_type="vessel",
                finding_type="no_exposure", severity="info",
                detail={"flag": "liberia"}, source="shodan"),
        Finding(target_id="VESSEL_C", target_type="vessel",
                finding_type="current_position", severity="info",
                detail={"destination": "GDANSK", "flag": "liberia"}),

        # Vessel D — different VSAT, no shadow indicators
        Finding(target_id="VESSEL_D", target_type="vessel",
                finding_type="exposed_service", severity="high",
                detail={"products": ["KVH TracPhone V7"]}, source="shodan"),
        Finding(target_id="VESSEL_D", target_type="vessel",
                finding_type="current_position", severity="info",
                detail={"destination": "GDANSK", "flag": "netherlands"}),
    ]


# ── CorrelationEngine ───────────────────────────────────────────────────

class TestCorrelationEngine:
    def test_shared_attributes_flag(self, sample_findings):
        eng = CorrelationEngine()
        eng.ingest(sample_findings)
        flags = eng.shared_attributes("flag")
        flag_values = {f["value"] for f in flags}
        assert "palau" in flag_values  # shared by A + B

    def test_shared_attributes_destination(self, sample_findings):
        eng = CorrelationEngine()
        eng.ingest(sample_findings)
        dests = eng.shared_attributes("destination")
        dest_values = {d["value"] for d in dests}
        assert "TALLINN" in dest_values
        assert "GDANSK" in dest_values

    def test_same_shadow_fleet_indicators(self, sample_findings):
        eng = CorrelationEngine()
        eng.ingest(sample_findings)
        clusters = eng.same_shadow_fleet_indicators()
        cluster_types = {c["indicator"] for c in clusters}
        assert "shadow_fleet_flag" in cluster_types
        assert "no_pi_insurance" in cluster_types

    def test_shared_vsat(self, sample_findings):
        eng = CorrelationEngine()
        eng.ingest(sample_findings)
        vsat = eng.shared_vsat()
        products = [v["product"] for v in vsat]
        assert "SAILOR 900 VSAT" in products
        assert "KVH TracPhone V7" not in [v["product"] for v in vsat
                                            if v["count"] < 2]

    def test_severity_summary(self, sample_findings):
        eng = CorrelationEngine()
        eng.ingest(sample_findings)
        summary = eng.severity_summary()
        assert summary.get("critical", 0) > 0
        assert summary.get("info", 0) > 0

    def test_risk_tiers(self, sample_findings):
        eng = CorrelationEngine()
        eng.ingest(sample_findings)
        tiers = eng.risk_tiers()
        assert tiers.get("critical", 0) >= 1  # A and/or B
        assert tiers.get("info", 0) >= 1       # C

    def test_vessel_count(self, sample_findings):
        eng = CorrelationEngine()
        eng.ingest(sample_findings)
        assert eng.vessel_count() == 4

    def test_all_correlations(self, sample_findings):
        eng = CorrelationEngine()
        eng.ingest(sample_findings)
        report = eng.all_correlations()
        assert report["vessel_count"] == 4
        assert "severity_summary" in report
        assert "shadow_fleet_clusters" in report
        assert "shared_vsat" in report

    def test_clear(self, sample_findings):
        eng = CorrelationEngine()
        eng.ingest(sample_findings)
        eng.clear()
        assert eng.vessel_count() == 0


# ── Aggregator ──────────────────────────────────────────────────────────

class TestAggregator:
    def test_assess_from_blackboard(self, sample_findings):
        bb = Blackboard()
        for f in sample_findings:
            bb.add(f)

        agg = Aggregator()
        assessment = agg.assess(bb)

        assert assessment["vessel_count"] == 4
        assert "severity_summary" in assessment
        assert "risk_tiers" in assessment
        assert "exposure_rate" in assessment
        assert "top_findings" in assessment

    def test_exposure_rate(self):
        """Vessels with shodan_exposure vs total."""
        bb = Blackboard()
        bb.add(Finding(target_id="A", target_type="vessel",
                       finding_type="shodan_exposure", severity="high"))
        bb.add(Finding(target_id="A", target_type="vessel",
                       finding_type="current_position", severity="info"))
        bb.add(Finding(target_id="B", target_type="vessel",
                       finding_type="no_exposure", severity="info"))
        bb.add(Finding(target_id="C", target_type="vessel",
                       finding_type="no_exposure", severity="info"))

        agg = Aggregator()
        assessment = agg.assess(bb)
        rate = assessment["exposure_rate"]
        assert rate["exposed"] == 1
        assert rate["total"] == 3
        assert rate["rate"] == 33

    def test_render_markdown(self, sample_findings):
        bb = Blackboard()
        for f in sample_findings:
            bb.add(f)

        agg = Aggregator()
        assessment = agg.assess(bb)
        md = agg.render_markdown(assessment, title="Port of Tallinn")

        assert "# Port of Tallinn" in md
        assert "Vessels analysed" in md
        assert "Severity Distribution" in md
        assert "Shadow Fleet Indicators" in md
        assert "Shared Infrastructure" in md

    def test_empty_blackboard(self):
        """Assessment on empty blackboard should not crash."""
        bb = Blackboard()
        agg = Aggregator()
        assessment = agg.assess(bb)
        assert assessment["vessel_count"] == 0
        assert assessment["exposure_rate"]["rate"] == 0

        md = agg.render_markdown(assessment)
        assert "Vessels analysed" in md
