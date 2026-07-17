"""Tests for blackboard triggers and integration."""

import pytest

from sirb.core import Blackboard, Finding


class TestTriggers:
    def test_critical_shodan_triggers_alert(self):
        bb = Blackboard()
        bb.register_trigger(
            {"severity": "critical", "source": "shodan"},
            "alert_aggregator",
        )

        f = Finding(finding_type="shodan_exposure", severity="critical",
                    source="shodan", target_id="VESSEL_A")
        actions = bb.check_triggers(f)
        assert "alert_aggregator" in actions

    def test_critical_but_wrong_source_no_trigger(self):
        bb = Blackboard()
        bb.register_trigger(
            {"severity": "critical", "source": "shodan"},
            "alert_aggregator",
        )

        f = Finding(finding_type="no_pi_insurance", severity="critical",
                    source="equasis", target_id="VESSEL_A")
        actions = bb.check_triggers(f)
        assert "alert_aggregator" not in actions

    def test_shadow_flag_triggers(self):
        bb = Blackboard()
        bb.register_trigger(
            {"finding_type": "shadow_fleet_flag"},
            "alert_aggregator",
        )

        f = Finding(finding_type="shadow_fleet_flag", severity="critical",
                    target_id="VESSEL_A", detail={"flag": "palau"})
        actions = bb.check_triggers(f)
        assert "alert_aggregator" in actions

    def test_non_matching_returns_empty(self):
        bb = Blackboard()
        bb.register_trigger(
            {"severity": "critical"},
            "alert_aggregator",
        )

        f = Finding(finding_type="no_exposure", severity="info")
        actions = bb.check_triggers(f)
        assert actions == []

    def test_multiple_triggers_can_match(self):
        """A finding matching multiple triggers returns all actions."""
        bb = Blackboard()
        bb.register_trigger({"severity": "critical"}, "alert")
        bb.register_trigger({"source": "shodan"}, "log")

        f = Finding(severity="critical", source="shodan",
                    finding_type="exposed_service")
        actions = bb.check_triggers(f)
        assert "alert" in actions
        assert "log" in actions

    def test_no_triggers_registered(self):
        bb = Blackboard()
        f = Finding(severity="critical")
        assert bb.check_triggers(f) == []
