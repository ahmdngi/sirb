"""Tests for multi-run trend tracking."""

import json
import time
from pathlib import Path

import pytest

from sirb.core import TrendTracker


@pytest.fixture
def tracker(tmp_path) -> TrendTracker:
    return TrendTracker(str(tmp_path))


@pytest.fixture
def sample_assessment():
    return {
        "generated_at": "2026-07-17T10:00:00Z",
        "unique_targets": 5,
        "severity": {"critical": 2, "high": 3, "info": 10},
        "finding_types": {"vuln": 5, "info": 10},
        "risk_tiers": {"critical": 1, "high": 2, "info": 2},
    }


@pytest.fixture
def older_assessment():
    return {
        "generated_at": "2026-07-16T10:00:00Z",
        "unique_targets": 3,
        "severity": {"critical": 1, "high": 2, "info": 5},
        "finding_types": {"vuln": 3, "info": 5},
        "risk_tiers": {"critical": 1, "high": 1, "info": 1},
    }


class TestTrendTracker:
    def test_save_and_load(self, tracker, sample_assessment):
        tracker.save_summary("test-run", sample_assessment)
        loaded = tracker.load_summary("test-run")
        assert loaded is not None
        assert loaded["unique_targets"] == 5
        assert loaded["severity"]["critical"] == 2

    def test_load_all_summaries(self, tracker, sample_assessment,
                                older_assessment):
        tracker.save_summary("run-001", older_assessment)
        tracker.save_summary("run-002", sample_assessment)

        all_s = tracker.load_all_summaries()
        assert len(all_s) == 2
        # Oldest first
        assert all_s[0]["unique_targets"] == 3

    def test_previous_summaries_returns_before_current(
            self, tracker, sample_assessment, older_assessment):
        tracker.save_summary("run-001", older_assessment)
        tracker.save_summary("run-002", sample_assessment)

        prev = tracker.previous_summaries("run-002")
        assert len(prev) == 1
        assert prev[0]["unique_targets"] == 3

    def test_delta_detects_increases(self, tracker, sample_assessment,
                                     older_assessment):
        d = tracker.delta(sample_assessment, older_assessment)
        assert d["has_change"] is True
        assert d["severity_deltas"]["critical"] == 1   # 2 - 1
        assert d["severity_deltas"]["high"] == 1       # 3 - 2
        assert d["target_count_change"] == 2           # 5 - 3

    def test_delta_returns_empty_when_no_change(self, tracker):
        a = {"unique_targets": 3, "severity": {"info": 5},
             "finding_types": {"x": 3}, "risk_tiers": {}, "generated_at": "T1"}
        b = {"unique_targets": 3, "severity": {"info": 5},
             "finding_types": {"x": 3}, "risk_tiers": {}, "generated_at": "T2"}
        d = tracker.delta(a, b)
        assert d["has_change"] is False

    def test_render_markdown_shows_changes(self, tracker, sample_assessment,
                                           older_assessment):
        d = tracker.delta(sample_assessment, older_assessment)
        md = tracker.render_delta_markdown(d, "run-001")
        assert "Trends" in md
        assert "+2 new targets" in md or "+2" in md

    def test_render_markdown_empty_when_no_change(self, tracker):
        a = {"unique_targets": 3, "severity": {"info": 5},
             "finding_types": {"x": 3}, "risk_tiers": {},
             "generated_at": "T1"}
        d = tracker.delta(a, a)
        md = tracker.render_delta_markdown(d, "prev-run")
        assert "No changes" in md
