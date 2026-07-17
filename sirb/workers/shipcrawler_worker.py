"""Sirb worker: ShipCrawler — vessel OSINT via the shipcrawler-parallel pipeline."""

from __future__ import annotations

import asyncio
import json
import os
import subprocess
import sys
import time
from pathlib import Path
from typing import Optional

from sirb.core import SirbWorker, Task, Result, Finding


# Path to the shipcrawler-parallel orchestrate.py
_SC_SCRIPTS = Path(
    os.path.expanduser("~/.hermes/skills/research/shipcrawler-parallel/scripts")
)
_ORCHESTRATE = _SC_SCRIPTS / "orchestrate.py"
_REPORT_BASE = Path(os.path.expanduser("~/hermes-vault/osint-reports"))


class ShipCrawlerWorker(SirbWorker):
    """Execute vessel OSINT investigations via the ShipCrawler pipeline.

    Each task targets one vessel (identified by MMSI). The worker runs
    all three ShipCrawler phases (Equasis, AIS, Shodan/Web) and returns
    structured findings to the Sirb blackboard.

    Discovery mode:
    - ``discover()`` reads vessel lists from ``SIRB_WORKER_DATA`` / ``vessels.json``
      or from a ``--port`` config. Future: live AIS port geofence.
    """

    name = "shipcrawler"
    description = "Vessel OSINT via Equasis + AIS + Shodan/Web pipeline"

    # ── config (loaded at registration time) ────────────────────────────

    _port_config: dict = {}       # port_name → {lat_min, lat_max, lon_min, lon_max}

    def __init__(self, config: Optional[dict] = None):
        super().__init__()
        config = config or {}
        self._port_config = config.get("ports", {})

    # ── required: execute one vessel ────────────────────────────────────

    async def execute(self, task: Task) -> Result:
        """Run ShipCrawler on one vessel.

        Task params:
            - ``mmsi`` (required): vessel MMSI
            - ``imo`` (optional): IMO number
            - ``name`` (optional): vessel name hint
            - ``mode`` (optional): ``"fast"`` (parallel scripts, default)
                                   or ``"deep"`` (hermes agent-driven, ~10 min)
        """
        mmsi = task.params.get("mmsi", "")
        imo = task.params.get("imo", "")
        name = task.params.get("name", task.params.get("vessel_name", ""))
        mode = task.params.get("mode", "fast")

        if not mmsi:
            return Result(
                task_id=task.id, worker=self.name, status="failure",
                error="Task.params missing 'mmsi'",
            )

        if mode == "deep":
            return await self._execute_deep(task, mmsi, imo, name)

        return await self._execute_fast(task, mmsi, imo, name)

    async def _execute_fast(self, task: Task, mmsi: str,
                            imo: str, name: str) -> Result:
        """Fast mode: run shipcrawler-parallel scripts as subprocess."""
        if not _ORCHESTRATE.exists():
            return Result(
                task_id=task.id, worker=self.name, status="failure",
                error=f"ShipCrawler scripts not found at {_ORCHESTRATE}",
            )

        cmd = [
            sys.executable, str(_ORCHESTRATE),
            "--mmsi", mmsi,
            "--parallel",
            "--report",
            "--quiet",
        ]
        if imo:
            cmd.extend(["--imo", imo])
        if name:
            cmd.extend(["--name", name])

        try:
            loop = asyncio.get_event_loop()
            proc = await asyncio.wait_for(
                loop.run_in_executor(
                    None,
                    lambda: subprocess.run(
                        cmd, capture_output=True, text=True, timeout=300,
                    ),
                ),
                timeout=310,
            )
        except asyncio.TimeoutError:
            return Result(
                task_id=task.id, worker=self.name, status="failure",
                error="ShipCrawler pipeline timed out after 300s",
            )

        if proc.returncode != 0:
            return Result(
                task_id=task.id, worker=self.name, status="failure",
                error=f"ShipCrawler exited {proc.returncode}: "
                      f"{proc.stderr[:500]}",
            )

        # Parse the merged JSON from orchestrate.py stdout
        try:
            output = json.loads(proc.stdout)
        except json.JSONDecodeError as e:
            return Result(
                task_id=task.id, worker=self.name, status="failure",
                error=f"Invalid JSON from ShipCrawler: {e}\n"
                      f"stdout: {proc.stdout[:500]}",
            )

        results = output.get("results", [])
        if not results:
            return Result(
                task_id=task.id, worker=self.name, status="failure",
                error="ShipCrawler returned empty results",
            )

        vessel = results[0]
        identity = vessel.get("identity", {})
        vessel_name = identity.get("vessel_name", name or mmsi)

        # Extract findings
        findings = self._extract_findings(vessel, mmsi)

        # Find report path
        safe_name = vessel_name.replace(" ", "-").lower()
        report_dir = _REPORT_BASE / f"{safe_name}-report"

        artifacts = []
        for fname in [
            "merged-vessel.json",
            "analyst-report.md",
            "red-team-playbook.md",
            "indicators-and-detection.md",
        ]:
            p = report_dir / fname
            if p.exists():
                artifacts.append(str(p))

        agent_errors = []
        for agent, status_val in vessel.get("agents", {}).items():
            if status_val != "ok":
                agent_errors.append(f"{agent}: {status_val}")

        result_status = "success"
        if agent_errors and not findings:
            result_status = "failure"
        elif agent_errors:
            result_status = "partial"

        return Result(
            task_id=task.id,
            worker=self.name,
            status=result_status,
            findings=findings,
            artifacts=artifacts,
            raw=vessel,
            error="; ".join(agent_errors) if agent_errors else "",
        )

    async def _execute_deep(self, task: Task, mmsi: str,
                            imo: str, name: str) -> Result:
        """Deep mode: run agent-driven ShipCrawler via hermes chat.

        This is slower but more thorough. Falls back to fast mode if
        Hermes CLI is unavailable.
        """
        hermes = self._find_hermes()
        if not hermes:
            return await self._execute_fast(task, mmsi, imo, name)

        query = f"Run ShipCrawler on MMSI {mmsi}"
        if imo:
            query += f" IMO {imo}"
        if name:
            query += f" ({name})"

        cmd = [hermes, "chat", "-q", "--skills", "shipcrawler", "--", query]

        try:
            loop = asyncio.get_event_loop()
            proc = await asyncio.wait_for(
                loop.run_in_executor(
                    None,
                    lambda: subprocess.run(
                        cmd, capture_output=True, text=True, timeout=600,
                    ),
                ),
                timeout=610,
            )
        except asyncio.TimeoutError:
            return Result(
                task_id=task.id, worker=self.name, status="failure",
                error="Deep ShipCrawler timed out after 600s",
            )

        return Result(
            task_id=task.id,
            worker=self.name,
            status="success",
            findings=[],
            artifacts=[],
            raw={"stdout": proc.stdout[:2000], "stderr": proc.stderr[:500]},
            error="",
        )

    # ── optional: discover targets ─────────────────────────────────────

    async def discover(self) -> list[Task]:
        """Discover vessels from configured sources.

        Sources (checked in order):
        1. ``$SIRB_WORKER_DATA/vessels.json`` — static vessel list
        2. Config ``workers.shipcrawler.ports`` — live AIS port scans
           via VesselFinder port pages.
        """
        tasks = []

        # 1. Static vessel file
        data_dir = os.environ.get(
            "SIRB_WORKER_DATA",
            os.path.expanduser("~/hermes-vault/sirb-data"),
        )
        vessels_file = Path(data_dir) / "vessels.json"
        if vessels_file.exists():
            try:
                with open(vessels_file) as f:
                    vessels = json.load(f)
                for v in vessels:
                    mmsi = v.get("mmsi", v.get("MMSI", ""))
                    if mmsi:
                        tasks.append(Task(
                            type="vessel_osint",
                            worker=self.name,
                            params=v,
                            priority=v.get("priority", 1),
                        ))
            except Exception as e:
                print(f"[shipcrawler] WARN: failed to read {vessels_file}: {e}")

        # 2. Live AIS port scan
        if self._port_config:
            from sirb.discovery import PortConfig, PortScanner

            port_cfg = PortConfig(self._port_config)
            for key in self._port_config.keys():
                port_def = port_cfg.get(key)
                if not port_def:
                    print(f"[shipcrawler] WARN: unknown port '{key}' in config")
                    continue

                print(f"[shipcrawler] Scanning {port_def.name}...")
                scanner = PortScanner(port_def)
                vessels = await scanner.scan()
                print(f"[shipcrawler]   found {len(vessels)} vessels in "
                      f"{port_def.name}")

                for v in vessels:
                    mmsi = v.get("mmsi", "")
                    if mmsi:
                        tasks.append(Task(
                            type="vessel_osint",
                            worker=self.name,
                            params={
                                "mmsi": mmsi,
                                "name": v.get("name", ""),
                                "port": key,
                            },
                            priority=0,  # live vessels = highest priority
                        ))

        return tasks

    # ── optional: validate ─────────────────────────────────────────────

    async def validate(self, result: Result) -> bool:
        """Accept results that have at least some findings."""
        if result.status == "failure":
            return False
        # Partial results (some agents failed) are still useful
        return True

    # ── optional: rate limits ──────────────────────────────────────────

    def rate_limits(self) -> dict[str, int]:
        """Declare API rate limits for ShipCrawler sources."""
        return {
            "equasis": 4,    # ~4 requests/minute
            "shodan": 30,    # free tier: 100/month; adjust down on paid
            "vesselfinder": 30,
        }

    # ── helpers ────────────────────────────────────────────────────────

    def _extract_findings(self, vessel: dict, mmsi: str) -> list[Finding]:
        """Convert a ShipCrawler merged vessel record into Sirb findings."""
        findings = []
        identity = vessel.get("identity", {})
        attack = vessel.get("attack_surface", {})
        shodan_summary = attack.get("summary", {})
        shodan_data = attack.get("shodan", {})
        web_osint = attack.get("web_osint", {})
        compliance = vessel.get("compliance", {})
        ownership = vessel.get("ownership", {})
        position = vessel.get("position", {})

        vessel_name = identity.get("vessel_name", "unknown")
        now = time.time()

        # ── Shodan exposure findings ──
        total_hits = shodan_summary.get("total_hits", -1)
        if total_hits > 0:
            findings.append(Finding(
                target_id=mmsi,
                target_type="vessel",
                finding_type="shodan_exposure",
                severity="critical" if total_hits > 10 else "high",
                weight=min(1.0, total_hits / 20),
                detail={
                    "total_hits": total_hits,
                    "queries": list(shodan_data.keys()),
                },
                source="shodan",
                worker=self.name,
                created_at=now,
            ))

            # Per-query findings
            for query, result in shodan_data.items():
                q_hits = result.get("total", 0)
                if q_hits > 0:
                    matches = result.get("matches", [])
                    products = list(set(
                        m.get("product", "") for m in matches
                        if m.get("product")
                    ))
                    ips = [m.get("ip", "") for m in matches[:3]]

                    findings.append(Finding(
                        target_id=mmsi,
                        target_type="vessel",
                        finding_type="exposed_service",
                        severity="high" if any(
                            p for p in products
                            if "VSAT" in p or "SAILOR" in p or "KVH" in p
                        ) else "medium",
                        weight=min(1.0, q_hits / 10),
                        detail={
                            "query": query,
                            "hits": q_hits,
                            "products": products,
                            "sample_ips": ips,
                        },
                        source="shodan",
                        worker=self.name,
                        created_at=now,
                    ))

        elif total_hits == 0:
            findings.append(Finding(
                target_id=mmsi,
                target_type="vessel",
                finding_type="no_exposure",
                severity="info",
                weight=0.2,
                detail={"note": "No Shodan-visible services detected"},
                source="shodan",
                worker=self.name,
                created_at=now,
            ))

        # ── Compliance findings ──
        detention_rate = compliance.get("detention_rate_36m", "")
        if detention_rate and detention_rate != "0.00%":
            findings.append(Finding(
                target_id=mmsi,
                target_type="vessel",
                finding_type="psc_detention",
                severity="high" if float(detention_rate.strip("%")) > 10 else "medium",
                weight=min(1.0, float(detention_rate.strip("%")) / 50),
                detail={"detention_rate_36m": detention_rate},
                source="equasis",
                worker=self.name,
                created_at=now,
            ))

        # ── Ownership / shadow fleet indicators ──
        pi_value = compliance.get("pi", "")
        if not pi_value or "unknown" in pi_value.lower():
            findings.append(Finding(
                target_id=mmsi,
                target_type="vessel",
                finding_type="no_pi_insurance",
                severity="high",
                weight=0.9,
                detail={"pi_club": pi_value},
                source="equasis",
                worker=self.name,
                created_at=now,
            ))

        # ── Flag / shadow fleet ──
        # Flags associated with shadow fleet activity
        shadow_flags = {"palau", "togo", "comoros", "tanzania", "cameroon",
                        "sierra leone", "cook islands", "dominica"}
        flag = (identity.get("flag") or "").lower()
        if flag in shadow_flags:
            findings.append(Finding(
                target_id=mmsi,
                target_type="vessel",
                finding_type="shadow_fleet_flag",
                severity="critical",
                weight=0.95,
                detail={"flag": flag, "vessel_name": vessel_name},
                source="equasis",
                worker=self.name,
                created_at=now,
            ))

        # ── Web OSINT findings ──
        for category, hits in web_osint.items():
            if hits:
                findings.append(Finding(
                    target_id=mmsi,
                    target_type="vessel",
                    finding_type="web_osint",
                    severity="medium",
                    weight=0.6,
                    detail={"category": category, "hits": hits[:5]},
                    source="web",
                    worker=self.name,
                    created_at=now,
                ))

        # ── AIS position ──
        if position and position.get("latitude"):
            findings.append(Finding(
                target_id=mmsi,
                target_type="vessel",
                finding_type="current_position",
                severity="info",
                weight=0.3,
                detail={
                    "lat": position.get("latitude"),
                    "lon": position.get("longitude"),
                    "destination": position.get("destination", ""),
                    "speed": position.get("speed", ""),
                },
                source="ais",
                worker=self.name,
                created_at=now,
            ))

        return findings

    def _find_hermes(self) -> Optional[str]:
        """Locate the Hermes CLI binary."""
        for p in ["hermes", "/usr/local/bin/hermes",
                  "/usr/local/lib/hermes-agent/venv/bin/hermes"]:
            if os.path.isfile(p) and os.access(p, os.X_OK):
                return p
        # Check PATH
        for p in os.environ.get("PATH", "").split(":"):
            candidate = os.path.join(p, "hermes")
            if os.path.isfile(candidate) and os.access(candidate, os.X_OK):
                return candidate
        return None
