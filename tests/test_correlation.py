"""Tests for agnostic correlation engine and aggregator."""

import pytest

from sirb.core import (
    CorrelationEngine, Aggregator, Blackboard, Finding,
)


# ── Fixtures ────────────────────────────────────────────────────────────

@pytest.fixture
def sample_findings():
    """Various findings across 4 targets for correlation testing.

    Uses generic finding types — no domain-specific references.
    """
    return [
        # Target A — critical severity
        Finding(target_id="TARGET_A", target_type="device",
                finding_type="vuln_critical", severity="critical",
                detail={"type": "router", "vendor": "Cisco", "flag": "AA"},
                source="scanner_a"),
        Finding(target_id="TARGET_A", target_type="device",
                finding_type="info", severity="info",
                detail={"flag": "AA", "location": "tallinn"},
                source="scanner_b"),

        # Target B — shares flag AA with A, shares source scanner_b with C
        Finding(target_id="TARGET_B", target_type="device",
                finding_type="vuln_high", severity="high",
                detail={"type": "switch", "vendor": "Cisco", "flag": "AA"},
                source="scanner_a"),
        Finding(target_id="TARGET_B", target_type="device",
                finding_type="info", severity="info",
                detail={"flag": "AA", "location": "tallinn"},
                source="scanner_c"),

        # Target C — shares source scanner_b with A
        Finding(target_id="TARGET_C", target_type="service",
                finding_type="info", severity="info",
                detail={"location": "helsinki"},
                source="scanner_b"),

        # Target D — different flag, different source
        Finding(target_id="TARGET_D", target_type="device",
                finding_type="info", severity="info",
                detail={"flag": "BB", "location": "gdansk"},
                source="scanner_d"),
    ]


# ── CorrelationEngine ───────────────────────────────────────────────────

class TestCorrelationEngine:
    def test_count_by_severity(self, sample_findings):
        eng = CorrelationEngine()
        eng.ingest(sample_findings)
        summary = eng.count_by_severity()
        assert summary.get("critical", 0) > 0
        assert summary.get("info", 0) > 0

    def test_count_by_type(self, sample_findings):
        eng = CorrelationEngine()
        eng.ingest(sample_findings)
        types = eng.count_by_type()
        assert "vuln_critical" in types

    def test_group_by_detail_key_flag(self, sample_findings):
        eng = CorrelationEngine()
        eng.ingest(sample_findings)
        groups = eng.group_by_detail_key("flag")
        values = {g["value"] for g in groups}
        assert "AA" in values  # shared by A + B
        assert "BB" not in {g["value"] for g in groups}  # only 1 target

    def test_group_by_detail_key_location(self, sample_findings):
        eng = CorrelationEngine()
        eng.ingest(sample_findings)
        groups = eng.group_by_detail_key("location")
        values = {g["value"] for g in groups}
        assert "tallinn" in values  # shared by A + B

    def test_group_by_field_source(self, sample_findings):
        eng = CorrelationEngine()
        eng.ingest(sample_findings)
        groups = eng.group_by_field("source")
        sources = {g["value"] for g in groups}
        assert "scanner_a" in sources  # shared by A + B
        # scanner_b shared by A + C
        assert any(g["value"] == "scanner_b" and g["count"] == 2
                   for g in groups)

    def test_group_by_field_target_type(self, sample_findings):
        eng = CorrelationEngine()
        eng.ingest(sample_findings)
        groups = eng.group_by_field("target_type")
        types = {g["value"] for g in groups}
        assert "device" in types  # shared by A, B, D

    def test_risk_tiers(self, sample_findings):
        eng = CorrelationEngine()
        eng.ingest(sample_findings)
        tiers = eng.risk_tiers()
        assert tiers.get("critical", 0) >= 1  # TARGET_A
        assert tiers.get("high", 0) >= 1       # TARGET_B

    def test_unique_targets(self, sample_findings):
        eng = CorrelationEngine()
        eng.ingest(sample_findings)
        assert eng.unique_targets() == 4

    def test_all_correlations(self, sample_findings):
        eng = CorrelationEngine()
        eng.ingest(sample_findings)
        report = eng.all_correlations()
        assert report["unique_targets"] == 4
        assert "severity" in report
        assert "finding_types" in report
        assert "shared_sources" in report

    def test_clear(self, sample_findings):
        eng = CorrelationEngine()
        eng.ingest(sample_findings)
        eng.clear()
        assert eng.unique_targets() == 0


# ── Aggregator ──────────────────────────────────────────────────────────

class TestAggregator:
    def test_assess_from_blackboard(self, sample_findings):
        bb = Blackboard()
        for f in sample_findings:
            bb.add(f)

        agg = Aggregator()
        assessment = agg.assess(bb)

        assert assessment["unique_targets"] == 4
        assert "severity" in assessment
        assert "risk_tiers" in assessment
        assert "top_findings" in assessment

    def test_assess_filtered_by_type(self, sample_findings):
        bb = Blackboard()
        for f in sample_findings:
            bb.add(f)

        agg = Aggregator()
        assessment = agg.assess(bb, target_type="service")

        assert assessment["unique_targets"] == 1  # only TARGET_C

    def test_exposure_stats(self):
        """Targets with positive findings vs total."""
        bb = Blackboard()
        bb.add(Finding(target_id="A", target_type="device",
                       finding_type="vuln", severity="high"))
        bb.add(Finding(target_id="A", target_type="device",
                       finding_type="info", severity="info"))
        bb.add(Finding(target_id="B", target_type="device",
                       finding_type="info", severity="info"))
        bb.add(Finding(target_id="C", target_type="device",
                       finding_type="info", severity="info"))

        agg = Aggregator()
        assessment = agg.assess(bb)
        stats = assessment["exposure_stats"]
        assert stats["positive_findings"] == 1
        assert stats["total_targets"] == 3
        assert stats["rate"] == 33

    def test_render_markdown(self, sample_findings):
        bb = Blackboard()
        for f in sample_findings:
            bb.add(f)

        agg = Aggregator()
        assessment = agg.assess(bb)
        md = agg.render_markdown(assessment, title="Test Assessment")

        assert "# Test Assessment" in md
        assert "Unique targets" in md
        assert "Severity Distribution" in md
        assert "Finding Types" in md
        assert "Top Findings by Severity" in md

    def test_empty_blackboard(self):
        """Assessment on empty blackboard should not crash."""
        bb = Blackboard()
        agg = Aggregator()
        assessment = agg.assess(bb)
        assert assessment["unique_targets"] == 0

        md = agg.render_markdown(assessment)
        assert "Unique targets" in md
