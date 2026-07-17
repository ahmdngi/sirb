"""Tests for ShipCrawlerWorker."""

import json
import os
import sys
from pathlib import Path
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from sirb.core import Task, Result
from sirb.workers.shipcrawler_worker import ShipCrawlerWorker


# ── Fixtures ────────────────────────────────────────────────────────────

@pytest.fixture
def worker():
    return ShipCrawlerWorker()


@pytest.fixture
def mock_vessel_data():
    """Simulated merged-vessel.json output from orchestrate.py."""
    return {
        "vessels": 1,
        "elapsed_seconds": 45.0,
        "results": [
            {
                "mmsi": "273342890",
                "extracted_at": "2026-07-16T12:00:00Z",
                "agents": {
                    "equasis": "ok",
                    "ais": "ok",
                    "shodan_web": "ok",
                },
                "identity": {
                    "vessel_name": "YAZ",
                    "imo": "9735323",
                    "mmsi": "273342890",
                    "flag": "Russia",
                    "call_sign": "UBVQ5",
                    "type": "Crude Oil Tanker",
                    "gt": "22886",
                    "dwt": "36903",
                    "year_built": "2017",
                    "status": "In Service",
                },
                "position": {
                    "latitude": 59.5,
                    "longitude": 24.5,
                    "destination": "TALLINN",
                    "speed": "0.0",
                    "course": "0.0",
                    "status": "Moored",
                },
                "compliance": {
                    "detention_rate_36m": "0.00%",
                    "pi": "Unknown",
                },
                "attack_surface": {
                    "shodan": {
                        "imo 9735323": {"total": 0},
                        "port:3000 or port:8080": {"total": 0},
                        '"YAZ" vessel': {"total": 0},
                    },
                    "summary": {"total_hits": 0},
                    "web_osint": {},
                },
                "ownership": {"note": "Not available"},
                "geographical": [],
            }
        ],
    }


@pytest.fixture
def fake_orchestrate(tmp_path):
    """Create a dummy orchestrate.py for tests that need it."""
    p = tmp_path / "scripts" / "orchestrate.py"
    p.parent.mkdir(parents=True, exist_ok=True)
    p.write_text("#!/usr/bin/env python3\nprint('fake')")
    return p


# ── Worker registration ────────────────────────────────────────────────

class TestShipCrawlerWorkerRegistration:
    def test_name_and_description(self, worker):
        assert worker.name == "shipcrawler"
        assert worker.description
        assert "vessel" in worker.description.lower()

    def test_is_sirb_worker(self, worker):
        from sirb.core import SirbWorker
        assert isinstance(worker, SirbWorker)

    def test_has_required_methods(self, worker):
        import inspect
        assert inspect.iscoroutinefunction(worker.execute)
        assert inspect.iscoroutinefunction(worker.discover)
        assert inspect.iscoroutinefunction(worker.validate)

    def test_rate_limits(self, worker):
        limits = worker.rate_limits()
        assert "equasis" in limits
        assert "shodan" in limits
        assert limits["equasis"] > 0


# ── Auto-discover ───────────────────────────────────────────────────────

class TestAutoDiscover:
    def test_discover_from_package(self):
        """Verify the worker is discoverable via the sirb.workers package."""
        from sirb.core import WorkerRegistry
        registry = WorkerRegistry()
        registry.discover_package("sirb.workers")

        assert "shipcrawler" in registry
        assert registry["shipcrawler"].name == "shipcrawler"

    def test_list_workers_after_discovery(self):
        from sirb.core import WorkerRegistry
        registry = WorkerRegistry()
        registry.discover_package("sirb.workers")
        info = registry.list_workers()
        names = [w["name"] for w in info]
        assert "shipcrawler" in names


# ── Execute ─────────────────────────────────────────────────────────────

class TestExecute:
    @patch("sirb.workers.shipcrawler_worker._ORCHESTRATE",
           Path("/nonexistent/path/orchestrate.py"))
    @pytest.mark.asyncio
    async def test_missing_mmsi(self, worker):
        """Task without mmsi should return failure before any file check."""
        result = await worker.execute(Task(type="vessel", worker="shipcrawler"))
        assert result.status == "failure"
        assert "mmsi" in result.error.lower()

    @patch("sirb.workers.shipcrawler_worker._ORCHESTRATE",
           Path("/definitely/missing/orchestrate.py"))
    @pytest.mark.asyncio
    async def test_missing_scripts(self, worker):
        """Missing orchestrate.py should return failure."""
        result = await worker.execute(Task(
            type="vessel", worker="shipcrawler",
            params={"mmsi": "273342890"},
        ))
        assert result.status == "failure"
        assert "not found" in result.error.lower()

    @pytest.mark.asyncio
    async def test_successful_execution(
        self, worker, mock_vessel_data, fake_orchestrate, tmp_path
    ):
        """Simulate a successful ShipCrawler run."""
        with patch("sirb.workers.shipcrawler_worker._ORCHESTRATE",
                   fake_orchestrate):
            with patch("sirb.workers.shipcrawler_worker.subprocess.run") as mock_run:
                mock_proc = MagicMock()
                mock_proc.returncode = 0
                mock_proc.stdout = json.dumps(mock_vessel_data)
                mock_proc.stderr = ""
                mock_run.return_value = mock_proc

                result = await worker.execute(Task(
                    type="vessel", worker="shipcrawler",
                    params={"mmsi": "273342890", "imo": "9735323"},
                ))

        assert result.status == "success"
        assert len(result.findings) > 0
        assert result.raw is not None

    @pytest.mark.asyncio
    async def test_finding_extraction(
        self, worker, mock_vessel_data, fake_orchestrate
    ):
        """Verify findings are properly extracted from vessel data."""
        with patch("sirb.workers.shipcrawler_worker._ORCHESTRATE",
                   fake_orchestrate):
            with patch("sirb.workers.shipcrawler_worker.subprocess.run") as mock_run:
                mock_proc = MagicMock()
                mock_proc.returncode = 0
                mock_proc.stdout = json.dumps(mock_vessel_data)
                mock_proc.stderr = ""
                mock_run.return_value = mock_proc

                result = await worker.execute(Task(
                    type="vessel", worker="shipcrawler",
                    params={"mmsi": "273342890"},
                ))

        # Should have findings: no_exposure, no_pi_insurance
        finding_types = {f.finding_type for f in result.findings}
        assert "no_exposure" in finding_types
        assert "no_pi_insurance" in finding_types

    @pytest.mark.asyncio
    async def test_shadow_flag_detected(
        self, worker, mock_vessel_data, fake_orchestrate
    ):
        """Findings should flag shadow fleet flags."""
        mock_vessel_data["results"][0]["identity"]["flag"] = "Palau"
        mock_vessel_data["results"][0]["compliance"]["pi"] = "Unknown"

        with patch("sirb.workers.shipcrawler_worker._ORCHESTRATE",
                   fake_orchestrate):
            with patch("sirb.workers.shipcrawler_worker.subprocess.run") as mock_run:
                mock_proc = MagicMock()
                mock_proc.returncode = 0
                mock_proc.stdout = json.dumps(mock_vessel_data)
                mock_proc.stderr = ""
                mock_run.return_value = mock_proc

                result = await worker.execute(Task(
                    type="vessel", worker="shipcrawler",
                    params={"mmsi": "273342890"},
                ))

        finding_types = {f.finding_type for f in result.findings}
        assert "shadow_fleet_flag" in finding_types


# ── Discover ────────────────────────────────────────────────────────────

class TestDiscover:
    @pytest.mark.asyncio
    async def test_discover_empty_when_no_file(self, worker):
        """No vessels file = empty discovery."""
        with patch.dict(os.environ, {"SIRB_WORKER_DATA": "/tmp/sirb-test-data-nonexistent"}):
            tasks = await worker.discover()
        assert tasks == []

    @pytest.mark.asyncio
    async def test_discover_from_json(self, worker, tmp_path):
        """Vessels from JSON file should be discovered."""
        vessels = [
            {"mmsi": "273342890", "imo": "9735323", "name": "YAZ"},
            {"mmsi": "311000987", "name": "BOREALIS"},
        ]
        data_dir = tmp_path / "sirb-data"
        data_dir.mkdir()
        vessels_file = data_dir / "vessels.json"
        with open(vessels_file, "w") as f:
            json.dump(vessels, f)

        with patch.dict(os.environ, {"SIRB_WORKER_DATA": str(data_dir)}):
            tasks = await worker.discover()

        assert len(tasks) == 2
        assert tasks[0].type == "vessel_osint"
        assert tasks[0].worker == "shipcrawler"
        assert tasks[0].params["mmsi"] == "273342890"
        assert tasks[1].params["mmsi"] == "311000987"


# ── Validate ────────────────────────────────────────────────────────────

class TestValidate:
    @pytest.mark.asyncio
    async def test_accepts_success(self, worker):
        assert await worker.validate(Result(status="success")) is True

    @pytest.mark.asyncio
    async def test_accepts_partial(self, worker):
        assert await worker.validate(Result(status="partial")) is True

    @pytest.mark.asyncio
    async def test_rejects_failure(self, worker):
        assert await worker.validate(Result(status="failure")) is False
