"""Tests for Sirb discovery module."""

import json
import os
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from sirb.discovery import PortConfig, PortDefinition, PortScanner


# ── PortConfig ──────────────────────────────────────────────────────────

class TestPortConfig:
    def test_default_ports(self):
        cfg = PortConfig()
        assert "tallinn" in cfg.keys()
        assert "helsinki" in cfg.keys()
        assert cfg.get("tallinn").name == "Port of Tallinn"
        assert cfg.get("tallinn").slug == "EE-TLL"

    def test_custom_config_overrides(self):
        cfg = PortConfig({
            "tallinn": {"name": "Custom Tallinn"},
            "newport": {"name": "New Port", "slug": "XX-YYY"},
        })
        assert cfg.get("tallinn").name == "Custom Tallinn"
        assert cfg.get("newport").name == "New Port"
        assert cfg.get("newport").slug == "XX-YYY"
        # Helsinki should still exist from defaults
        assert cfg.get("helsinki") is not None

    def test_unknown_port(self):
        cfg = PortConfig()
        assert cfg.get("nonexistent") is None


# ── PortScanner ─────────────────────────────────────────────────────────

class TestPortScanner:
    def test_init(self):
        port = PortDefinition(name="Test", slug="TST", lat_min=0, lat_max=1,
                              lon_min=0, lon_max=1)
        scanner = PortScanner(port)
        assert scanner is not None

    @pytest.mark.asyncio
    async def test_scan_no_browser_and_no_url(self):
        """Port without vessel_finder_url returns empty."""
        port = PortDefinition(name="No URL", slug="NONE",
                              lat_min=0, lat_max=1, lon_min=0, lon_max=1)
        scanner = PortScanner(port)
        result = await scanner.scan()
        assert result == []

    @pytest.mark.asyncio
    async def test_scan_no_browser_fallback(self):
        """When no Playwright/CloakBrowser available, returns empty."""
        port = PortDefinition(name="Test", slug="TST",
                              lat_min=0, lat_max=1, lon_min=0, lon_max=1,
                              vessel_finder_url="https://example.com/port")
        scanner = PortScanner(port)

        # Mock the imports to fail
        with patch.dict("sys.modules", {
            "cloakbrowser": None,
            "playwright": None,
            "playwright.sync_api": None,
        }):
            result = await scanner.scan()
            assert result == []


# ── Integration: ShipCrawler discover() ────────────────────────────────

class TestShipCrawlerPortDiscovery:
    @pytest.mark.asyncio
    async def test_discover_with_port_config(self):
        """ShipCrawlerWorker.discover() creates tasks from port config."""
        from sirb.workers.shipcrawler_worker import ShipCrawlerWorker

        worker = ShipCrawlerWorker(config={
            "ports": {
                "tallinn": {"name": "Port of Tallinn", "slug": "EE-TLL",
                            "vessel_finder_url": "https://example.com/port"}
            }
        })

        # Mock the scanner to return sample vessels
        with patch("sirb.discovery.ais_port_scanner.PortScanner.scan",
                   new=AsyncMock(return_value=[
                       {"mmsi": "273342890", "name": "YAZ"},
                       {"mmsi": "311000987", "name": "BOREALIS"},
                   ])):
            tasks = await worker.discover()

        # Should have 2 tasks from port scan
        port_tasks = [t for t in tasks if t.params.get("port") == "tallinn"]
        assert len(port_tasks) == 2
        assert port_tasks[0].type == "vessel_osint"
        assert port_tasks[0].params["mmsi"] == "273342890"
        assert port_tasks[0].priority == 0  # live vessels = highest

    @pytest.mark.asyncio
    async def test_discover_combines_static_and_port(self, tmp_path):
        """discover() combines both static file and port scan tasks."""
        from sirb.workers.shipcrawler_worker import ShipCrawlerWorker

        # Write static vessels file
        data_dir = tmp_path / "sirb-data"
        data_dir.mkdir()
        with open(data_dir / "vessels.json", "w") as f:
            json.dump([{"mmsi": "123456789"}], f)

        worker = ShipCrawlerWorker(config={
            "ports": {
                "tallinn": {"name": "Test", "slug": "TST",
                            "vessel_finder_url": "https://example.com/port"}
            }
        })

        with patch.dict(os.environ, {"SIRB_WORKER_DATA": str(data_dir)}):
            with patch("sirb.discovery.ais_port_scanner.PortScanner.scan",
                       new=AsyncMock(return_value=[
                           {"mmsi": "273342890", "name": "YAZ"},
                       ])):
                tasks = await worker.discover()

        assert len(tasks) == 2
